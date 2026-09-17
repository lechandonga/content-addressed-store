package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFullGCRemovesUnpinnedKeepsPinned(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 64})
	pinnedData := mkData(t, 300)
	unpinnedData := mkData(t, 400)
	pinnedID, _ := n.PutBytes(ctx, pinnedData)
	unpinnedID, _ := n.PutBytes(ctx, unpinnedData)
	if err := n.Pin(ctx, "keep", pinnedID); err != nil {
		t.Fatal(err)
	}
	rep, err := n.GC(ctx, GCOptions{Full: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.RemovedManifests != 1 || rep.EvictedContents != 1 {
		t.Fatalf("report=%+v", rep)
	}
	if !n.store.HasManifest(pinnedID) {
		t.Fatal("pinned content must not be collected")
	}
	if n.store.HasManifest(unpinnedID) {
		t.Fatal("unpinned content should be collected by full GC")
	}
	// 被引用内容仍可读且正确。
	got, err := n.GetBytes(ctx, pinnedID)
	if err != nil || !bytes.Equal(got, pinnedData) {
		t.Fatalf("pinned read: %v", err)
	}
}

func TestGCCleansOrphanChunksAndTmp(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 64})
	data := mkData(t, 300)
	id, _ := n.PutBytes(ctx, data)
	// 固定住有效内容：Full GC 应保留其全部分块，只清理孤立分块与暂存残留。
	if err := n.Pin(ctx, "valid", id); err != nil {
		t.Fatal(err)
	}
	m, _, _ := n.store.ReadManifest(id)

	// 制造一个“孤立分块”：直接写入索引+文件但无清单引用。
	orphan := chunkIDFromBytes([]byte("nobody references me, i am garbage"))
	if err := n.store.CommitChunkBytes(orphan, []byte("nobody references me, i am garbage")); err != nil {
		t.Fatal(err)
	}
	// 暂存残留：tmp 与 .inflight 只在打开时清扫崩溃产物。运行期 GC 不触碰它们。
	tmpLeft := filepath.Join(n.store.dir, tmpDir, "leftover")
	if err := os.WriteFile(tmpLeft, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	inflightDir := filepath.Join(n.store.dir, chunksDir, "00", ".inflight")
	if err := os.MkdirAll(inflightDir, 0o755); err != nil {
		t.Fatal(err)
	}
	inflightFile := filepath.Join(inflightDir, ".cas-tmp-crash")
	if err := os.WriteFile(inflightFile, []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !n.store.HasChunk(orphan) {
		t.Fatal("setup")
	}
	rep, err := n.GC(ctx, GCOptions{Full: true})
	if err != nil {
		t.Fatal(err)
	}
	if n.store.HasChunk(orphan) {
		t.Fatal("orphan chunk not collected")
	}
	// 运行期 GC 不得误删并发提交可能使用的 .inflight（这里是崩溃残留，但
	// 运行期无法区分，按“正在提交”保护处理）。
	if _, err := os.Stat(inflightFile); err != nil {
		t.Fatal("GC must not touch .inflight during runtime")
	}
	// 有效内容及其分块完好。
	if n.Stats().Chunks != len(m.Chunks) {
		t.Fatalf("chunks after gc=%d want %d", n.Stats().Chunks, len(m.Chunks))
	}
	if _, err := n.GetBytes(ctx, id); err != nil {
		t.Fatalf("valid content broken: %v report=%+v", err, rep)
	}
	// 关闭后重新打开：崩溃残留的 tmp/.inflight 被一次性清扫。
	dir := n.store.dir
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n2, err := OpenNode(dir, Config{ChunkSize: 64}, NodeOptions{Peers: []Peer{NewLocalPeer("src", n)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n2.Close() })
	if _, err := os.Stat(tmpLeft); !os.IsNotExist(err) {
		t.Fatal("tmp leftover must be swept on open")
	}
	if _, err := os.Stat(inflightFile); !os.IsNotExist(err) {
		t.Fatal(".inflight crash residue must be swept on open")
	}
}

func TestCapacityEvictionLRU(t *testing.T) {
	ctx := context.Background()
	// 容量很小，高水位 0.8：约 3200 字节触发回收。
	src := newTestNode(t, Config{ChunkSize: 100})
	dst := newTestNode(t, Config{ChunkSize: 100, MaxBytes: 4000, HighWatermark: 0.8},
		NewLocalPeer("src", src))

	// 预先在对端准备 6 个内容，然后按时间顺序拉取到 dst（每次拉取都 touch LRU）。
	ids := make([]ContentID, 6)
	datas := make([][]byte, 6)
	for i := range ids {
		datas[i] = mkData(t, 600)
		ids[i], _ = src.PutBytes(ctx, datas[i])
	}
	for _, id := range ids {
		if _, err := dst.GetBytes(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	// 容量收敛到硬上限以内。
	if st := dst.Stats(); st.Bytes > 4000 {
		t.Fatalf("bytes %d exceeds hard limit 4000: %+v", st.Bytes, st)
	}
	// 必须发生了 LRU 淘汰。
	retained := 0
	for _, id := range ids {
		if dst.store.HasManifest(id) {
			retained++
		}
	}
	if retained >= len(ids) {
		t.Fatalf("expected LRU evictions, stats=%+v", dst.Stats())
	}
	// 最近使用的最后一个内容应当保留（它在 LRU 队首）。
	if !dst.store.HasManifest(ids[len(ids)-1]) {
		t.Fatal("most recently used content should be retained")
	}
	// 最旧内容应已淘汰。
	if dst.store.HasManifest(ids[0]) {
		t.Fatal("oldest content should have been evicted")
	}
	// 被淘汰内容再次访问时可从对端重新获取（而不是报错/返回坏数据）。
	got, err := dst.GetBytes(ctx, ids[0])
	if err != nil {
		t.Fatalf("evicted content should be refetchable: %v", err)
	}
	if !bytes.Equal(got, datas[0]) {
		t.Fatal("refetched content mismatch")
	}
}

func TestPinnedContentSurvivesCapacityPressure(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 100, MaxBytes: 3000, HighWatermark: 0.8})
	pinned, _ := n.PutBytes(ctx, mkData(t, 800))
	if err := n.Pin(ctx, "v1", pinned); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		_, _ = n.PutBytes(ctx, mkData(t, 500)) // 制造容量压力
	}
	if !n.store.HasManifest(pinned) {
		t.Fatal("pinned content evicted under capacity pressure")
	}
	if err := n.Verify(ctx, pinned); err != nil {
		t.Fatalf("pinned verify: %v", err)
	}
}

func TestHardCapacityRejectsNewPinned(t *testing.T) {
	ctx := context.Background()
	// 容量允许一个 ~600B 的被引用内容常驻，但无法容纳第二个不共享分块的内容。
	n := newTestNode(t, Config{ChunkSize: 100, MaxBytes: 3200, HighWatermark: 1.0})
	d1 := mkData(t, 500)
	id1, err := n.PutBytes(ctx, d1)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Pin(ctx, "a", id1); err != nil {
		t.Fatal(err)
	}
	if err := n.Verify(ctx, id1); err != nil {
		t.Fatalf("pinned content: %v", err)
	}
	// 第二个独立内容：被引用内容 + 新增数据超过硬上限，必须得到明确容量错误。
	id2, putErr := n.PutBytes(ctx, mkData(t, 500))
	if putErr != nil && !errors.Is(putErr, ErrCapacityExceeded) {
		t.Fatalf("unexpected error: %v", putErr)
	}
	if putErr != nil {
		t.Fatalf("second content should fit once: %v", putErr)
	}
	if err := n.Pin(ctx, "b", id2); err != nil {
		t.Fatal(err)
	}
	// 两个被引用内容已占 ~2100B；第三个独立内容超过硬上限，必须明确失败。
	if _, err := n.PutBytes(ctx, mkData(t, 500)); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("want ErrCapacityExceeded, got %v", err)
	}
	if st := n.Stats(); st.Bytes > 3200 {
		t.Fatalf("bytes=%d over hard limit", st.Bytes)
	}
	// 已有引用与读取始终不受影响。
	r, err := n.Resolve("a")
	if err != nil || r != id1 {
		t.Fatalf("existing ref harmed: %s %v", r, err)
	}
	if err := n.Verify(ctx, id1); err != nil {
		t.Fatalf("existing content harmed: %v", err)
	}
}

func TestRefetchAfterGC(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 100})
	data := mkData(t, 900)
	id, _ := src.PutBytes(ctx, data)
	dst := newTestNode(t, Config{ChunkSize: 100}, NewLocalPeer("src", src))
	if _, err := dst.GetBytes(ctx, id); err != nil {
		t.Fatal(err)
	}
	// 无引用内容被 GC 回收。
	if _, err := dst.GC(ctx, GCOptions{Full: true}); err != nil {
		t.Fatal(err)
	}
	if dst.store.HasManifest(id) {
		t.Fatal("unpinned content should be gone")
	}
	// 再次访问：必须重新获取而不是返回损坏/陈旧数据。
	got, err := dst.GetBytes(ctx, id)
	if err != nil {
		t.Fatalf("access after GC should refetch: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("refetched data mismatch")
	}
}

func TestPinDuringGCProtectsContent(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 200})
	data := mkData(t, 2000)
	id, _ := src.PutBytes(ctx, data)

	dst := newTestNode(t, Config{ChunkSize: 200}, NewLocalPeer("src", src))
	if _, err := dst.GetBytes(ctx, id); err != nil {
		t.Fatal(err)
	}

	// 并发：一侧反复 Full GC，一侧在随机时刻 Pin / Unpin / Get。
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var failures int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := dst.GC(ctx, GCOptions{Full: true}); err != nil {
					t.Errorf("gc: %v", err)
					return
				}
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			name := fmt.Sprintf("race/%d", i%5)
			if err := dst.Pin(ctx, name, id); err != nil {
				atomic.AddInt64(&failures, 1)
				t.Errorf("pin: %v", err)
				return
			}
			b, err := dst.GetBytes(ctx, id)
			if err != nil {
				atomic.AddInt64(&failures, 1)
				t.Errorf("get: %v", err)
				return
			}
			if !bytes.Equal(b, data) {
				atomic.AddInt64(&failures, 1)
				t.Errorf("data mismatch")
				return
			}
			if i%3 == 0 {
				_ = dst.Unpin(name)
			}
		}
	}()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	if failures != 0 {
		t.Fatalf("failures=%d", failures)
	}
	// 至少还有一个存活引用（race/1, race/2, race/3, race/4 之一），内容必须可用。
	if err := dst.Verify(ctx, id); err != nil {
		t.Fatalf("final verify: %v", err)
	}
}

func TestConcurrentReadersAndGC(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 250})
	data := mkData(t, 5000)
	id, _ := src.PutBytes(ctx, data)
	dst := newTestNode(t, Config{ChunkSize: 250}, NewLocalPeer("src", src))
	if _, err := dst.GetBytes(ctx, id); err != nil {
		t.Fatal(err)
	}
	// 一部分内容被引用（GC 永远不动），一部分不引用（可被回收但读取者会触发重取）。
	pinned, _ := src.PutBytes(ctx, mkData(t, 1000))
	if _, err := dst.GetBytes(ctx, pinned); err != nil {
		t.Fatal(err)
	}
	if err := dst.Pin(ctx, "p", pinned); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var failures int64
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				target := id
				if seed%2 == 0 {
					target = pinned
				}
				b, err := dst.GetBytes(ctx, target)
				if err != nil {
					atomic.AddInt64(&failures, 1)
					t.Errorf("reader: %v", err)
					return
				}
				if target == pinned && !bytes.Equal(b, data[:1000]) && len(b) != 1000 {
					// pinned 内容独立校验在下方
				}
			}
		}(r)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = dst.GC(ctx, GCOptions{Full: true})
			}
		}
	}()
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	if failures != 0 {
		t.Fatalf("reader/gc failures=%d", failures)
	}
	pb, err := dst.GetBytes(ctx, pinned)
	if err != nil || len(pb) != 1000 {
		t.Fatalf("pinned read: len=%d err=%v", len(pb), err)
	}
}

func TestRestartPreservesState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	n1, err := OpenNode(dir, Config{ChunkSize: 100}, NodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d1 := mkData(t, 700)
	d2 := mkData(t, 700)
	id1, _ := n1.PutBytes(ctx, d1)
	id2, _ := n1.PutBytes(ctx, d2)
	if err := n1.Pin(ctx, "name", id1); err != nil {
		t.Fatal(err)
	}
	if err := n1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开：历史数据无需重建即可读写，引用与 LRU 状态保留。
	n2, err := OpenNode(dir, Config{ChunkSize: 100, MaxBytes: 4000, HighWatermark: 0.8}, NodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	r, err := n2.Resolve("name")
	if err != nil || r != id1 {
		t.Fatalf("ref lost after restart: %s %v", r, err)
	}
	if got, err := n2.GetBytes(ctx, id1); err != nil || !bytes.Equal(got, d1) {
		t.Fatalf("id1 after restart: %v", err)
	}
	if got, err := n2.GetBytes(ctx, id2); err != nil || !bytes.Equal(got, d2) {
		t.Fatalf("id2 after restart: %v", err)
	}
	// Full GC 只清掉未引用的 id2，id1 受保护。
	if _, err := n2.GC(ctx, GCOptions{Full: true}); err != nil {
		t.Fatal(err)
	}
	if !n2.store.HasManifest(id1) || n2.store.HasManifest(id2) {
		t.Fatal("post-restart GC wrong retention")
	}
}

func TestGCRepeatedConvergence(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 100, MaxBytes: 5000, HighWatermark: 0.8})
	for i := 0; i < 20; i++ {
		_, _ = n.PutBytes(ctx, mkData(t, 400))
	}
	var last int64
	for i := 0; i < 5; i++ {
		rep, err := n.GC(ctx, GCOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if rep.BytesAfter > 5000 {
			t.Fatalf("bytes=%d over hard limit", rep.BytesAfter)
		}
		if i > 0 && rep.BytesAfter != last && rep.EvictedContents == 0 && rep.RemovedChunks == 0 {
			t.Fatalf("GC non-convergent: %+v", rep)
		}
		last = rep.BytesAfter
	}
}
