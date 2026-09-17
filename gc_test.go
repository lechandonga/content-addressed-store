package cas

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func TestPinnedContentNeverCollected(t *testing.T) {
	// 默认 64B 分块；900B 内容在盘上约 1.5KiB（分块+清单）。
	s := testStore(t, Config{ChunkSize: 64, MaxBytes: 20000, HighWatermark: 0.5, LowWatermark: 0.2})
	ctx := context.Background()
	c1, err := s.Put(payload(1000))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Pin(ctx, "keep", c1); err != nil {
		t.Fatal(err)
	}
	// 写更多内容把容量推过高水位；写满硬上限后允许 Put 报容量错误。
	var sawCapacity bool
	for i := 0; i < 20; i++ {
		if _, perr := s.Put(payload(900 + i)); perr != nil {
			if !errors.Is(perr, ErrCapacity) {
				t.Fatalf("unexpected put error: %v", perr)
			}
			sawCapacity = true
			break
		}
	}
	_ = sawCapacity
	rep, err := s.Collect(ctx, GCConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.EvictedManifests == 0 {
		t.Fatalf("expected some evictions, got %+v", rep)
	}
	// 被引用的内容必须完好。
	got, err := s.Get(ctx, c1)
	if err != nil {
		t.Fatalf("pinned content lost: %v", err)
	}
	if !bytes.Equal(got, payload(1000)) {
		t.Fatal("pinned content corrupted")
	}
	pins, _ := s.Pins()
	if len(pins) != 1 || pins[0].Addr != c1 {
		t.Fatalf("pin table wrong: %+v", pins)
	}
}

func TestUnpinnedEvictedByLRUAndRefetch(t *testing.T) {
	cfg := Config{ChunkSize: 16, MaxBytes: 1000, HighWatermark: 0.95, LowWatermark: 0.5,
		QuarantineTTL: -1}
	s := testStore(t, cfg)
	ctx := context.Background()

	victim, _ := s.Put(payload(100))
	keeper, _ := s.Put(payload(40))
	// 访问 keeper 让其在 LRU 上更新；victim 更久未用。
	if _, err := s.Get(ctx, keeper); err != nil {
		t.Fatal(err)
	}

	// 把干净副本放到对端，以便驱逐后重新获取。
	peer := testStore(t, Config{ChunkSize: 16})
	if _, err := peer.Put(payload(100)); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Put(payload(40)); err != nil {
		t.Fatal(err)
	}
	s.SetFetcher(storeFetcher(t, peer))

	// 强制写入一个大对象触发自动回收（通过 Put 内部 makeRoom）。
	big := make([]byte, 120)
	for i := range big {
		big[i] = byte(i)
	}
	if _, err := s.Put(big); err != nil {
		t.Fatalf("put big with auto-GC: %v", err)
	}

	// victim 是 LRU 最旧，应当已被驱逐（本地不存在），但重新 Get 必须能从对端取回并校验。
	if s.Has(victim) {
		t.Fatal("LRU victim should have been evicted")
	}
	got, err := s.Get(ctx, victim)
	if err != nil {
		t.Fatalf("refetch after eviction: %v", err)
	}
	if !bytes.Equal(got, payload(100)) {
		t.Fatal("refetched content mismatch")
	}
}

func TestSharedChunksRefcount(t *testing.T) {
	s := testStore(t, Config{ChunkSize: 16, MaxBytes: 100000,
		HighWatermark: 0.1, LowWatermark: 0.05})
	ctx := context.Background()
	base := payload(500)
	c1, _ := s.Put(base)
	// 与 base 高度重叠的内容（前缀相同），共享大量分块。
	c2, _ := s.Put(append(append([]byte{}, base...), 1, 2, 3, 4))

	// pin c1，不 pin c2；高水位极低，GC 会驱逐 c2。
	if err := s.Pin(ctx, "base", c1); err != nil {
		t.Fatal(err)
	}
	rep, err := s.Collect(ctx, GCConfig{EvictAll: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.EvictedManifests != 1 {
		t.Fatalf("expected exactly 1 manifest evicted, got %+v", rep)
	}
	// c1 必须仍可完整读取：共享分块不能被误删。
	got, err := s.Get(ctx, c1)
	if err != nil {
		t.Fatalf("shared chunks wrongly removed: %v", err)
	}
	if !bytes.Equal(got, base) {
		t.Fatal("base content mismatch")
	}
	// c2 的清单已不在，但其独有分块被回收。
	if s.Has(c2) {
		t.Fatal("c2 manifest should be gone")
	}
}

func TestHardCapacityRejectsWhenPinnedFills(t *testing.T) {
	s := testStore(t, Config{ChunkSize: 16, MaxBytes: 3000,
		HighWatermark: 0.95, LowWatermark: 0.9})
	ctx := context.Background()
	// 两份内容使用不同的字节序列，保证没有任何 16B 分块地址重合（无法去重）。
	pinned, err := s.Put(seq(260, 7, 3))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Pin(ctx, "full", pinned); err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(seq(700, 3, 1)) // 无法腾挪：唯一内容被引用
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("want ErrCapacity, got %v", err)
	}
}

func TestManualGCReclaimsOrphansAndTmp(t *testing.T) {
	s := testStore(t, Config{})
	cid, _ := s.Put(payload(300))
	_ = cid

	// 伪造一个孤儿分块（地址正确但无清单引用）。
	orphan := []byte("orphan block")
	oa := AddressBytes(orphan)
	op := s.chunkPath(oa)
	if err := os.MkdirAll(filepath.Dir(op), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(op, orphan, 0o644); err != nil {
		t.Fatal(err)
	}

	// 伪造一个过期 tmp 残留。
	tp := filepath.Join(s.Root(), dirTmp, "w-stale")
	if err := os.WriteFile(tp, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := s.now().Add(-2 * staleTmpGrace)
	_ = os.Chtimes(tp, old, old)

	rep, err := s.Collect(context.Background(), GCConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrphanChunks != 1 {
		t.Fatalf("expected 1 orphan, got %+v", rep)
	}
	if rep.StageTmpFiles != 1 {
		t.Fatalf("expected 1 stale tmp removed, got %+v", rep)
	}
	if _, err := os.Stat(op); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("orphan not removed")
	}
}

func TestGCCorruptFilesQuarantined(t *testing.T) {
	s := testStore(t, Config{ChunkSize: 16})
	cid, _ := s.Put(payload(200))
	_ = cid
	// 破坏一个分块。
	var cp string
	_ = filepath.Walk(filepath.Join(s.Root(), dirChunks), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && cp == "" {
			cp = p
		}
		return nil
	})
	corruptFile(t, cp)
	rep, err := s.Collect(context.Background(), GCConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Quarantined == 0 {
		t.Fatalf("corrupt chunk should be quarantined by GC: %+v", rep)
	}
	if _, err := os.Stat(cp); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupt chunk still in place after GC")
	}
}

func TestPinUnpinResolve(t *testing.T) {
	s := testStore(t, Config{})
	ctx := context.Background()
	cid, _ := s.Put(payload(100))
	if err := s.Pin(ctx, "a", cid); err != nil {
		t.Fatal(err)
	}
	if err := s.Pin(ctx, "a", cid); err != nil {
		t.Fatalf("idempotent pin failed: %v", err)
	}
	other, _ := s.Put(payload(120))
	if err := s.Pin(ctx, "a", other); !errors.Is(err, ErrRefConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	got, err := s.Resolve("a")
	if err != nil || got != cid {
		t.Fatalf("resolve: %v %v", got, err)
	}
	if _, err := s.Resolve("nope"); !errors.Is(err, ErrRefNotFound) {
		t.Fatalf("want ref not found, got %v", err)
	}
	if err := s.Unpin("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Resolve("a"); !errors.Is(err, ErrRefNotFound) {
		t.Fatalf("unpin failed: %v", err)
	}
}

func TestPinFetchesMissingContent(t *testing.T) {
	s := testStore(t, Config{ChunkSize: 16})
	peer := testStore(t, Config{ChunkSize: 16})
	cid, _ := peer.Put(payload(400))
	s.SetFetcher(storeFetcher(t, peer))
	if err := s.Pin(context.Background(), "remote", cid); err != nil {
		t.Fatalf("pin should fetch+verify: %v", err)
	}
	if !s.Has(cid) {
		t.Fatal("pinned content not materialized locally")
	}
}

// ---- 并发：读取 x 回收 x 新增引用 ----

func TestConcurrentReadAndGC(t *testing.T) {
	s := testStore(t, Config{ChunkSize: 8, MaxBytes: 100000,
		HighWatermark: 0.0001, LowWatermark: 0.00001})
	ctx := context.Background()
	var cids []Addr
	for i := 0; i < 6; i++ {
		c, _ := s.Put(payload(200 + i))
		cids = append(cids, c)
	}
	// pin 一半，另一半可被回收；提供对端使被回收内容总能重新获取并校验。
	peer := testStore(t, Config{ChunkSize: 8})
	for i := 0; i < 6; i++ {
		if _, err := peer.Put(payload(200 + i)); err != nil {
			t.Fatal(err)
		}
	}
	s.SetFetcher(storeFetcher(t, peer))
	for i := 0; i < 3; i++ {
		_ = s.Pin(ctx, string(rune('a'+i)), cids[i])
	}

	stop := make(chan struct{})
	var readerWG, workersWG sync.WaitGroup

	// 读方：反复读取所有内容。未 pin 的内容可能被回收，
	// 但必须能从对端重新获取；任何情况下都不得返回损坏数据。
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for i, cid := range cids {
				b, err := s.Get(ctx, cid)
				if err != nil {
					t.Errorf("reader cid %d unexpected error (should self-heal): %v", i, err)
					return
				}
				if !bytes.Equal(b, payload(200+i)) {
					t.Errorf("reader got corrupted content for cid %d", i)
					return
				}
			}
			runtime_Gosched()
		}
	}()

	// GC 方：持续强制回收。
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		for i := 0; i < 50; i++ {
			if _, err := s.Collect(ctx, GCConfig{EvictAll: true}); err != nil {
				t.Errorf("gc: %v", err)
				return
			}
		}
	}()

	// Pin/Unpin 方：持续增删引用，验证被引用内容不会被误删。
	workersWG.Add(1)
	go func() {
		defer workersWG.Done()
		for i := 0; i < 50; i++ {
			idx := 3 + (i % 3)
			cid := cids[idx]
			name := "dyn"
			if err := s.Pin(ctx, name, cid); err != nil {
				t.Errorf("concurrent pin: %v", err)
				return
			}
			b, err := s.Get(ctx, cid)
			if err != nil {
				t.Errorf("read under pin failed: %v", err)
				return
			}
			if !bytes.Equal(b, payload(200+idx)) {
				t.Error("read under pin returned wrong bytes")
				return
			}
			if err := s.Unpin(name); err != nil && !errors.Is(err, ErrRefNotFound) {
				t.Errorf("unpin: %v", err)
				return
			}
		}
	}()

	workersWG.Wait()
	close(stop)
	readerWG.Wait()
}

func TestConcurrentFetchSingleflight(t *testing.T) {
	peer := testStore(t, Config{ChunkSize: 8})
	cid, _ := peer.Put(payload(500))

	var fetchMu sync.Mutex
	fetchCount := map[string]int{}
	counting := NewFetcher(func(ctx context.Context, kind string, addr Addr) ([]byte, error) {
		fetchMu.Lock()
		fetchCount[kind+":"+string(addr)]++
		fetchMu.Unlock()
		if kind == "manifest" {
			return peer.OpenManifest(addr)
		}
		return peer.OpenChunk(addr)
	})

	s := testStore(t, Config{ChunkSize: 8})
	s.SetFetcher(counting)

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			b, err := s.Get(context.Background(), cid)
			if err == nil && !bytes.Equal(b, payload(500)) {
				errs[i] = errors.New("bytes mismatch")
			} else {
				errs[i] = err
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent getter %d: %v", i, err)
		}
	}
	// 清单必须恰好拉取一次（singleflight）；分块也应每个至多一次。
	fetchMu.Lock()
	mf := fetchCount["manifest:"+string(cid)]
	fetchMu.Unlock()
	if mf != 1 {
		t.Fatalf("manifest fetched %d times, expected 1", mf)
	}
	// 落盘只有一份内容。
	chunks := countFiles(t, filepath.Join(s.Root(), dirChunks))
	if chunks != 63 { // ceil(500/8)
		t.Fatalf("expected 63 chunk files, got %d", chunks)
	}
}

func TestConcurrentPutDistinctContents(t *testing.T) {
	s := testStore(t, Config{ChunkSize: 32, MaxBytes: 1 << 30})
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := payload(1000 + i*17)
			cid, err := s.Put(c)
			if err != nil {
				t.Errorf("put %d: %v", i, err)
				return
			}
			b, err := s.Get(context.Background(), cid)
			if err != nil || !bytes.Equal(b, c) {
				t.Errorf("verify %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	// GC 后用量记账与磁盘一致，无不可回收残留。
	if _, err := s.Collect(context.Background(), GCConfig{}); err != nil {
		t.Fatal(err)
	}
	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	var onDisk int64
	for _, d := range []string{dirChunks, dirManifests} {
		_ = filepath.Walk(filepath.Join(s.Root(), d), func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				onDisk += info.Size()
			}
			return nil
		})
	}
	if onDisk != st.UsageBytes {
		t.Fatalf("usage accounting drift: stats=%d disk=%d", st.UsageBytes, onDisk)
	}
}

// runtime_Gosched 让出 CPU，避免热循环在竞态测试中饿死其他 goroutine。
func runtime_Gosched() { runtime.Gosched() }

func TestFetchRespectsCapacityAndSelfHeals(t *testing.T) {
	// 小容量节点从对端获取超出容量的内容：普通 Get 应在回收后仍失败时给出
	// 明确的 ErrCapacity，而不是落盘超额数据或返回损坏内容。
	src := testStore(t, Config{ChunkSize: 16})
	cid, _ := src.Put(seq(2000, 7, 3))

	dst := testStore(t, Config{ChunkSize: 16, MaxBytes: 500,
		HighWatermark: 0.9, LowWatermark: 0.5})
	dst.SetFetcher(storeFetcher(t, src))
	_, err := dst.Get(context.Background(), cid)
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("want ErrCapacity on over-cap fetch, got %v", err)
	}

	// 放大容量后再次获取应成功并通过完整校验。
	dir := dst.Root()
	_ = dst.Close()
	big, err := Open(dir, Config{ChunkSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	big.SetFetcher(storeFetcher(t, src))
	got, err := big.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("refetch after capacity relief: %v", err)
	}
	if !bytes.Equal(got, seq(2000, 7, 3)) {
		t.Fatal("fetched content mismatch")
	}
}

func TestGCReportUsageConsistency(t *testing.T) {
	s := testStore(t, Config{ChunkSize: 32})
	for i := 0; i < 5; i++ {
		if _, err := s.Put(seq(300+i, 7+i, 3)); err != nil {
			t.Fatal(err)
		}
	}
	rep, err := s.Collect(context.Background(), GCConfig{EvictAll: true})
	if err != nil {
		t.Fatal(err)
	}
	// 无引用内容全部驱逐后，chunks/manifests 用量应归零且记账一致。
	if rep.UsageAfter != 0 {
		t.Fatalf("expected zero usage after evict-all, got %d (report=%+v)", rep.UsageAfter, rep)
	}
	st, _ := s.Stats()
	if st.UsageBytes != 0 || st.ManifestCount != 0 {
		t.Fatalf("post-gc stats inconsistent: %+v", st)
	}
}
