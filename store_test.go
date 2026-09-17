package cas

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newTestNode(t testing.TB, cfg Config, peers ...Peer) *Node {
	t.Helper()
	n, err := OpenNode(t.TempDir(), cfg, NodeOptions{Peers: peers})
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func mkData(t testing.TB, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 128})
	for _, size := range []int{0, 1, 127, 128, 129, 128 * 10, 128*10 + 7} {
		data := mkData(t, size)
		id, err := n.PutBytes(ctx, data)
		if err != nil {
			t.Fatalf("size=%d put: %v", size, err)
		}
		got, err := n.GetBytes(ctx, id)
		if err != nil {
			t.Fatalf("size=%d get: %v", size, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("size=%d mismatch", size)
		}
	}
}

func TestContentIDStableAcrossNodes(t *testing.T) {
	ctx := context.Background()
	data := mkData(t, 4096)
	n1 := newTestNode(t, Config{ChunkSize: 256})
	n2 := newTestNode(t, Config{ChunkSize: 256})
	id1, err := n1.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := n2.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("content id not stable: %s vs %s", id1, id2)
	}
	if !IsValidContentID(string(id1)) {
		t.Fatalf("bad content id: %s", id1)
	}
}

func TestDeduplication(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 64})
	data := bytes.Repeat([]byte("A"), 1600) // 全相同字节，所有分块一致
	id1, err := n.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := n.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatal("same content different id")
	}
	st := n.Stats()
	// 去重：相同分块只保留一份。逐字节计算唯一分块数。
	uniq := map[string]bool{}
	cs := 64
	for i := 0; i < len(data); i += cs {
		end := i + cs
		if end > len(data) {
			end = len(data)
		}
		uniq[string(chunkIDFromBytes(data[i:end]))] = true
	}
	if st.Chunks != len(uniq) {
		t.Fatalf("chunks=%d want unique=%d", st.Chunks, len(uniq))
	}
	if st.Manifests != 1 {
		t.Fatalf("manifests=%d want 1", st.Manifests)
	}
	// 磁盘上每个分块也只有一个文件。
	count := 0
	_ = filepath.Walk(filepath.Join(n.store.dir, chunksDir), func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			count++
		}
		return nil
	})
	if count != st.Chunks {
		t.Fatalf("disk chunk files=%d index=%d", count, st.Chunks)
	}
}

func TestConcurrentPutSameContent(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 100})
	data := mkData(t, 2500)
	const writers = 24
	var wg sync.WaitGroup
	ids := make([]ContentID, writers)
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = n.PutBytes(ctx, data)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("writer %d id=%s want %s", i, ids[i], ids[0])
		}
	}
	got, err := n.GetBytes(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data corrupted after concurrent puts")
	}
	st := n.Stats()
	if st.Manifests != 1 {
		t.Fatalf("manifests=%d, duplicate copies produced", st.Manifests)
	}
}

func TestConcurrentGetMissingContent(t *testing.T) {
	// 多个并发获取同一缺失内容：只应产生一次对端拉取 / 一份副本。
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 200})
	data := mkData(t, 3000)
	cid, err := src.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	probe := &countingPeer{Peer: NewLocalPeer("src", src)}
	dst := newTestNode(t, Config{ChunkSize: 200}, probe)

	const readers = 16
	var wg sync.WaitGroup
	errs := make([]error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b, gerr := dst.GetBytes(ctx, cid)
			if gerr != nil {
				errs[i] = gerr
				return
			}
			if !bytes.Equal(b, data) {
				errs[i] = fmt.Errorf("reader %d mismatch", i)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("reader %d: %v", i, err)
		}
	}
	if probe.manifestGets() > 1 {
		t.Fatalf("manifest fetched %d times, want single-flight merge", probe.manifestGets())
	}
	if probe.chunkMaxGets() > 1 {
		t.Fatalf("some chunk fetched %d times, want single-flight merge", probe.chunkMaxGets())
	}
	if st := dst.Stats(); st.Manifests != 1 || st.Chunks != src.Stats().Chunks {
		t.Fatalf("dst stats=%+v src=%+v", st, src.Stats())
	}
}

type countingPeer struct {
	Peer
	mu        sync.Mutex
	chunks    map[string]int
	manifestN int
}

func (c *countingPeer) GetChunk(ctx context.Context, id ChunkID) ([]byte, error) {
	c.mu.Lock()
	if c.chunks == nil {
		c.chunks = map[string]int{}
	}
	c.chunks[string(id)]++
	c.mu.Unlock()
	return c.Peer.GetChunk(ctx, id)
}

func (c *countingPeer) GetManifest(ctx context.Context, id ContentID) ([]byte, error) {
	c.mu.Lock()
	c.manifestN++
	c.mu.Unlock()
	return c.Peer.GetManifest(ctx, id)
}

func (c *countingPeer) manifestGets() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.manifestN
}

func (c *countingPeer) chunkMaxGets() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := 0
	for _, v := range c.chunks {
		if v > m {
			m = v
		}
	}
	return m
}

func TestPinsAndResolve(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 64})
	data := mkData(t, 500)
	id, err := n.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Pin(ctx, "images/logo.png", id); err != nil {
		t.Fatal(err)
	}
	got, err := n.Resolve("images/logo.png")
	if err != nil || got != id {
		t.Fatalf("resolve=%s,%v want %s", got, err, id)
	}
	if err := n.CreatePin(ctx, "images/logo.png", id); !errors.Is(err, ErrRefExists) {
		t.Fatalf("want ErrRefExists got %v", err)
	}
	if err := n.Unpin("images/logo.png"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Resolve("images/logo.png"); !errors.Is(err, ErrRefNotFound) {
		t.Fatalf("want ErrRefNotFound got %v", err)
	}
	// 路径穿越等非法引用名必须拒绝。
	for _, bad := range []string{"../x", "a/../b", "..", strings.Repeat("a", 300), "/abs", "x?y"} {
		if err := n.Pin(ctx, bad, id); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("name %q: want ErrInvalidArgument got %v", bad, err)
		}
	}
}

func TestCorruptChunkIsRejectedAndRepaired(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 100})
	data := mkData(t, 500)
	id, err := src.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	dst := newTestNode(t, Config{ChunkSize: 100}, NewLocalPeer("src", src))
	if _, err := dst.GetBytes(ctx, id); err != nil {
		t.Fatal(err)
	}

	// 篡改本地中间分块：读取必须报错或自愈，绝不返回错误内容。
	m, _, err := dst.store.ReadManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Chunks) < 2 {
		t.Fatal("need multi-chunk content")
	}
	victim := m.Chunks[1].ID
	path := dst.store.chunkPath(victim)
	if err := os.WriteFile(path, bytes.Repeat([]byte("Z"), int(m.Chunks[1].Size)), 0o644); err != nil {
		t.Fatal(err)
	}

	// 直接读篡改分块：类型化损坏错误。
	if err := dst.Verify(ctx, id); !errors.Is(err, ErrCorruptChunk) {
		t.Fatalf("Verify want ErrCorruptChunk got %v", err)
	}
	// Get 自愈：隔离坏分块并从对端重取，返回正确内容。
	got, err := dst.GetBytes(ctx, id)
	if err != nil {
		t.Fatalf("Get after corruption should repair: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("repaired content mismatch")
	}
}

func TestCorruptManifestIsRejectedAndRepaired(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 100})
	data := mkData(t, 500)
	id, err := src.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	dst := newTestNode(t, Config{ChunkSize: 100}, NewLocalPeer("src", src))
	if _, err := dst.GetBytes(ctx, id); err != nil {
		t.Fatal(err)
	}
	// 直接覆写清单文件（破坏摘要）。
	path := dst.store.manifestPath(id)
	if err := os.WriteFile(path, []byte("{\"format\":\"cas-manifest-v1\",\"manifest\":{\"version\":1,\"hash\":\"sha256\",\"chunkSize\":100,\"length\":1,\"chunks\":[]}}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.GetBytes(ctx, id); err != nil {
		t.Fatalf("corrupt manifest should be refetched: %v", err)
	}
	if err := dst.Verify(ctx, id); err != nil {
		t.Fatalf("verify after repair: %v", err)
	}
}

func TestMissingChunkFetchedFromPeer(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 100})
	data := mkData(t, 600)
	id, err := src.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	dst := newTestNode(t, Config{ChunkSize: 100}, NewLocalPeer("src", src))
	if _, err := dst.GetBytes(ctx, id); err != nil {
		t.Fatal(err)
	}
	m, _, err := dst.store.ReadManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	// 删除一个分块文件：应得到可区分的缺失错误（Verify），Get 自动补齐。
	victim := m.Chunks[0].ID
	if err := os.Remove(dst.store.chunkPath(victim)); err != nil {
		t.Fatal(err)
	}
	if err := dst.Verify(ctx, id); !errors.Is(err, ErrMissingChunk) {
		t.Fatalf("want ErrMissingChunk got %v", err)
	}
	got, err := dst.GetBytes(ctx, id)
	if err != nil {
		t.Fatalf("missing chunk should be refetched: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("refetched content mismatch")
	}
}

func TestContentNotFoundEverywhere(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{})
	dst := newTestNode(t, Config{}, NewLocalPeer("src", src))
	fake := ContentID("cas1-" + strings.Repeat("00", 32))
	if _, err := dst.GetBytes(ctx, fake); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound got %v", err)
	}
}

func TestTamperedPeerRejected(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 100})
	data := mkData(t, 300)
	id, err := src.PutBytes(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	// 恶意对端：清单给真的，分块给假的。
	evil := &evilPeer{inner: NewLocalPeer("src", src)}
	dst := newTestNode(t, Config{ChunkSize: 100}, evil)
	_, err = dst.GetBytes(ctx, id)
	if !errors.Is(err, ErrCorruptChunk) {
		t.Fatalf("want ErrCorruptChunk from evil peer, got %v", err)
	}
	// 坏分块绝不能落盘：节点上不应出现任何已提交分块。
	if st := dst.Stats(); st.Chunks != 0 {
		t.Fatalf("tampered chunks must never be committed, got %d chunks", st.Chunks)
	}
	// 再次读取仍应失败（坏对端无法提供真实数据），且不会 panic/返回错误内容。
	if b, err := dst.GetBytes(ctx, id); err == nil {
		t.Fatalf("unexpected success with %d bytes", len(b))
	}
}

type evilPeer struct{ inner Peer }

func (e *evilPeer) Name() string { return "evil" }
func (e *evilPeer) GetManifest(ctx context.Context, id ContentID) ([]byte, error) {
	return e.inner.GetManifest(ctx, id)
}
func (e *evilPeer) GetChunk(ctx context.Context, id ChunkID) ([]byte, error) {
	return []byte("not-the-real-chunk-at-all-but-32b"), nil
}
func (e *evilPeer) HasChunk(ctx context.Context, id ChunkID) (bool, error) { return true, nil }
func (e *evilPeer) HasManifest(ctx context.Context, id ContentID) (bool, error) {
	return true, nil
}

var _ = io.EOF
