package cas

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func testStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 64 // 小分块，强制产生多分块内容
	}
	if cfg.MaxObjectSize == 0 {
		cfg.MaxObjectSize = 1 << 30
	}
	s, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func payload(n int) []byte { return seq(n, 7, 3) }

// seq 生成确定性字节序列 (i*mul+add)%251；不同 (mul,add) 的序列
// 不可能有整段 16B 分块完全相同，用于构造无法跨内容去重的测试数据。
func seq(n, mul, add int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*mul + add) % 251)
	}
	return b
}

func TestPutGetRoundTrip(t *testing.T) {
	s := testStore(t, Config{})
	ctx := context.Background()
	for _, n := range []int{0, 1, 63, 64, 65, 1000} {
		content := payload(n)
		cid, err := s.Put(content)
		if err != nil {
			t.Fatalf("put n=%d: %v", n, err)
		}
		got, err := s.Get(ctx, cid)
		if err != nil {
			t.Fatalf("get n=%d: %v", n, err)
		}
		if !bytes.Equal(got, content) {
			t.Fatalf("content mismatch n=%d", n)
		}
	}
}

func TestCIDStableAndContentDedup(t *testing.T) {
	s := testStore(t, Config{})
	content := payload(500)
	a1, err := s.Put(content)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := s.Put(append([]byte(nil), content...))
	if err != nil {
		t.Fatal(err)
	}
	if a1 != a2 {
		t.Fatalf("identical content produced different CIDs: %s vs %s", a1, a2)
	}
	// 手工构造预期 CID，验证其可复现。
	refs, blocks, _ := BuildChunks(content, 64)
	_, _, expected, _ := BuildManifest(refs)
	if a1 != expected {
		t.Fatalf("CID not reproducible: %s vs %s", a1, expected)
	}
	if len(blocks) != 8 || blocks[7] == nil {
		t.Fatalf("expected 8 chunks")
	}
	st, _ := s.Stats()
	// 只应存在 8 个去重分块。
	if got := countFiles(t, filepath.Join(s.Root(), dirChunks)); got != 8 {
		t.Fatalf("expected 8 chunk files, got %d (usage=%d)", got, st.UsageBytes)
	}
}

func TestConcurrentPutSameContentNoDuplicates(t *testing.T) {
	s := testStore(t, Config{})
	content := payload(2000)
	var wg sync.WaitGroup
	cids := make([]Addr, 16)
	errs := make([]error, 16)
	for i := range cids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 每个 goroutine 使用独立 slice，模拟不同写入方。
			buf := append([]byte(nil), content...)
			cids[i], errs[i] = s.Put(buf)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if cids[i] != cids[0] {
			t.Fatalf("cid mismatch: %s vs %s", cids[i], cids[0])
		}
	}
	chunks := countFiles(t, filepath.Join(s.Root(), dirChunks))
	manifests := countFiles(t, filepath.Join(s.Root(), dirManifests))
	if chunks != 32 { // ceil(2000/64)=32
		t.Fatalf("expected 32 deduped chunk files, got %d", chunks)
	}
	if manifests != 1 {
		t.Fatalf("expected 1 manifest, got %d", manifests)
	}
}

func TestReadBackReopen(t *testing.T) {
	dir := t.TempDir()
	content := payload(700)
	var cid Addr
	{
		s, err := Open(dir, Config{ChunkSize: 64})
		if err != nil {
			t.Fatal(err)
		}
		cid, _ = s.Put(content)
		_ = s.Close()
	}
	// 历史数据无需重建：重新打开即可读。
	s2, err := Open(dir, Config{ChunkSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s2.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("reopen get: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("reopen content mismatch")
	}
}

func TestBadAddressRejected(t *testing.T) {
	s := testStore(t, Config{})
	if _, err := s.Get(context.Background(), "sha256-deadbeef"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}

func countFiles(t *testing.T, root string) int {
	t.Helper()
	n := 0
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		n++
		return nil
	})
	return n
}

// ---- 损坏与缺失：可区分的失败原因 + 自愈 ----

// corruptFile flips a byte in the file at path.
func corruptFile(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(b) == 0 {
		b = []byte{0}
	} else {
		b[0] ^= 0xFF
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptChunkQuarantinedAndFetched(t *testing.T) {
	s := testStore(t, Config{})
	content := payload(500)
	cid, _ := s.Put(content)

	// 找到一个分块文件并破坏它。
	var chunkFile string
	_ = filepath.Walk(filepath.Join(s.Root(), dirChunks), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			chunkFile = p
		}
		return nil
	})
	if chunkFile == "" {
		t.Fatal("no chunk file")
	}
	corruptFile(t, chunkFile)

	// 没有 Fetcher 时：必须报 ErrCorruptChunk，而不是返回坏数据。
	_, err := s.Get(context.Background(), cid)
	if !errors.Is(err, ErrCorruptChunk) {
		t.Fatalf("want ErrCorruptChunk, got %v", err)
	}
	// 坏文件已被隔离，原位置不存在。
	if _, statErr := os.Stat(chunkFile); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("corrupt chunk not removed: %v", statErr)
	}
	if n := countFiles(t, filepath.Join(s.Root(), dirQuarantine)); n != 1 {
		t.Fatalf("expected 1 quarantined file, got %d", n)
	}

	// 配置一个内存对端（由一份干净存储支撑），再次访问必须自愈成功。
	good := testStore(t, Config{})
	gcid, _ := good.Put(content)
	if gcid != cid {
		t.Fatal("cids differ across stores")
	}
	s.SetFetcher(storeFetcher(t, good))
	got, err := s.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("heal get: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("healed content mismatch")
	}
}

func TestMissingChunkRecoveredFromPeer(t *testing.T) {
	s := testStore(t, Config{})
	content := payload(500)
	cid, _ := s.Put(content)

	// 删除一个分块。
	var chunkFile string
	_ = filepath.Walk(filepath.Join(s.Root(), dirChunks), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && chunkFile == "" {
			chunkFile = p
		}
		return nil
	})
	if err := os.Remove(chunkFile); err != nil {
		t.Fatal(err)
	}
	_, err := s.Get(context.Background(), cid)
	if !errors.Is(err, ErrMissingChunk) {
		t.Fatalf("want ErrMissingChunk, got %v", err)
	}

	good := testStore(t, Config{})
	_, _ = good.Put(content)
	s.SetFetcher(storeFetcher(t, good))
	got, err := s.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("recovered content mismatch")
	}
}

func TestCorruptManifestFetched(t *testing.T) {
	s := testStore(t, Config{})
	content := payload(300)
	cid, _ := s.Put(content)
	mp := s.manifestPath(cid)
	corruptFile(t, mp)
	_, err := s.Get(context.Background(), cid)
	if !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("want ErrCorruptManifest, got %v", err)
	}
	good := testStore(t, Config{})
	_, _ = good.Put(content)
	s.SetFetcher(storeFetcher(t, good))
	got, err := s.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("heal manifest: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch after manifest heal")
	}
}

func TestContentNotFoundAnywhere(t *testing.T) {
	s := testStore(t, Config{})
	good := testStore(t, Config{})
	s.SetFetcher(storeFetcher(t, good))
	missing := AddressBytes([]byte("nobody has this"))
	_, err := s.Get(context.Background(), missing)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestTamperedPeerRejected(t *testing.T) {
	s := testStore(t, Config{})
	content := payload(200)
	cid, _ := s.Put(content)
	// 删除本地全部数据，强制走对端。
	_ = os.RemoveAll(filepath.Join(s.Root(), dirChunks))
	_ = os.RemoveAll(filepath.Join(s.Root(), dirManifests))

	s.SetFetcher(NewFetcher(func(ctx context.Context, kind string, addr Addr) ([]byte, error) {
		// 恶意对端返回看似存在但哈希错误的内容。
		return []byte("tampered payload bytes that do not match"), nil
	}))
	_, err := s.Get(context.Background(), cid)
	if !errors.Is(err, ErrCorruptManifest) && !errors.Is(err, ErrCorruptChunk) {
		t.Fatalf("expected corrupt error, got %v", err)
	}
}

func TestManifestTamperedChunkListRejected(t *testing.T) {
	// 直接构造一个声明虚假分块地址的清单，保证它无法被解码为有效内容。
	fake := &Manifest{Version: ManifestVersion, Size: 3, Chunks: []ChunkRef{
		{Addr: AddressBytes([]byte("x")), Size: 3},
	}}
	raw := marshalManifest(fake)
	cid := AddressBytes(raw) // 真实地址只对应这份“坏布局”清单
	if _, err := DecodeManifest(raw, cid); err != nil {
		t.Fatalf("structurally valid manifest should decode: %v", err)
	}
	// 但用另一个内容的地址去解码必须失败：
	other := AddressBytes([]byte("different"))
	if _, err := DecodeManifest(raw, other); !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("want corrupt manifest for mismatched address, got %v", err)
	}
}

func TestErrorKindSentinels(t *testing.T) {
	e := fail(KindCapacity, "put", "sha256-aa", "full", errors.New("disk full"))
	if !errors.Is(e, ErrCapacity) {
		t.Fatal("errors.Is kind match failed")
	}
	if errors.Is(e, ErrIO) {
		t.Fatal("should not match unrelated kind")
	}
	if !strings.Contains(e.Error(), "capacity") {
		t.Fatalf("error text missing kind: %v", e)
	}
}
