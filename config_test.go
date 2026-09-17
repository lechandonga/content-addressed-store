package cas

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

func TestConfigNormalization(t *testing.T) {
	c := Config{}.normalized()
	if c.ChunkSize != DefaultChunkSize {
		t.Fatalf("default chunk size=%d", c.ChunkSize)
	}
	if c.HighWatermark != 0.9 {
		t.Fatalf("default watermark=%v", c.HighWatermark)
	}
	c2 := Config{ChunkSize: 1, HighWatermark: 5}.normalized()
	if c2.ChunkSize != minChunkSize {
		t.Fatalf("clamped chunk size=%d", c2.ChunkSize)
	}
	if c2.HighWatermark != 1 {
		t.Fatalf("clamped watermark=%v", c2.HighWatermark)
	}
	c3 := Config{ChunkSize: 1 << 40}.normalized()
	if c3.ChunkSize != maxChunkSize {
		t.Fatalf("clamped large chunk size=%d", c3.ChunkSize)
	}
}

func TestEmptyContent(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{})
	id, err := n.PutBytes(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !IsValidContentID(string(id)) {
		t.Fatalf("bad empty content id %s", id)
	}
	got, err := n.GetBytes(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("empty content got %d bytes", len(got))
	}
	if err := n.Verify(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidIDsRejected(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{})
	for _, bad := range []ContentID{"", "x", "cas1-zz", ContentID("cas1-" + strings.Repeat("0", 63)), "sha256:abc"} {
		if _, _, err := n.Get(ctx, bad); err == nil {
			t.Fatalf("accepted bad id %q", bad)
		}
	}
}

func TestRestartHealsCorruption(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src, err := OpenNode(t.TempDir(), Config{ChunkSize: 100}, NodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data := mkData(t, 500)
	id, _ := src.PutBytes(ctx, data)

	n1, err := OpenNode(dir, Config{ChunkSize: 100}, NodeOptions{Peers: []Peer{NewLocalPeer("src", src)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n1.GetBytes(ctx, id); err != nil {
		t.Fatal(err)
	}
	m, _, _ := n1.Store().ReadManifest(id)
	// 关闭后在磁盘上损坏一个分块。
	if err := n1.Close(); err != nil {
		t.Fatal(err)
	}
	path := n1.Store().chunkPath(m.Chunks[2].ID)
	if err := os.WriteFile(path, bytes.Repeat([]byte("K"), int(m.Chunks[2].Size)), 0o644); err != nil {
		t.Fatal(err)
	}
	// 重新打开：历史数据可读；损坏分块在访问时被隔离并从对端自愈。
	n2, err := OpenNode(dir, Config{ChunkSize: 100}, NodeOptions{Peers: []Peer{NewLocalPeer("src", src)}})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	got, err := n2.GetBytes(ctx, id)
	if err != nil {
		t.Fatalf("heal after restart: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("healed data mismatch")
	}
}

func TestRefsSnapshotAndOverwrite(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 64})
	d1, _ := n.PutBytes(ctx, mkData(t, 200))
	d2, _ := n.PutBytes(ctx, mkData(t, 200))
	if err := n.Pin(ctx, "r", d1); err != nil {
		t.Fatal(err)
	}
	// 覆盖引用：d1 失去引用转入 LRU，d2 受保护。
	if err := n.Pin(ctx, "r", d2); err != nil {
		t.Fatal(err)
	}
	if r, _ := n.Resolve("r"); r != d2 {
		t.Fatalf("overwrite failed: %s", r)
	}
	if len(n.Store().Refs()) != 1 {
		t.Fatal("refs snapshot wrong size")
	}
	rep, err := n.GC(ctx, GCOptions{Full: true})
	if err != nil {
		t.Fatal(err)
	}
	if !n.Store().HasManifest(d2) {
		t.Fatal("newly pinned content evicted")
	}
	// d1 应已被回收（无引用）。
	if n.Store().HasManifest(d1) && rep.EvictedContents == 0 {
		t.Fatal("old content after ref overwrite should be evictable")
	}
}

func TestSetRefRollbackOnWriteFailure(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 64})
	id, _ := n.PutBytes(ctx, mkData(t, 200))

	// 在 refs 目录下放置一个普通文件占位，使引用名与其冲突导致建目录失败。
	blocker := "blocker"
	path := n.Store().Dir() + "/refs/" + blocker
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// blocker 是文件，blocker/sub 无法创建：SetRef 必须失败且不留内存引用。
	err := n.Store().SetRef(blocker+"/sub", id)
	if err == nil {
		t.Fatal("expected SetRef failure")
	}
	if _, rerr := n.Resolve(blocker + "/sub"); rerr == nil {
		t.Fatal("failed SetRef must not leave an in-memory reference")
	}
	// 内容仍在，且作为无引用缓存可读。
	if _, err := n.GetBytes(ctx, id); err != nil {
		t.Fatalf("content should remain readable: %v", err)
	}
}
