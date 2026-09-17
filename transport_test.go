package cas

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPPeerFetch(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 128})
	data := mkData(t, 2000)
	id, _ := src.PutBytes(ctx, data)
	srv := httptest.NewServer(HTTPHandler(src))
	defer srv.Close()

	// HEAD 存在性。
	peer := NewHTTPPeer("src", srv.URL)
	ok, err := peer.HasManifest(ctx, id)
	if err != nil || !ok {
		t.Fatalf("HasManifest=%v,%v", ok, err)
	}

	dst := newTestNode(t, Config{ChunkSize: 128}, peer)
	got, err := dst.GetBytes(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("http fetch mismatch")
	}

	// 不存在的对象：404 → ErrNotFound。
	fake := ContentID("cas1-" + strings.Repeat("ab", 32))
	if _, err := dst.GetBytes(ctx, fake); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want not found got %v", err)
	}
}

func TestHTTPPeerBadID(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{})
	srv := httptest.NewServer(HTTPHandler(src))
	defer srv.Close()

	resp, err := http.Get(srv.URL + httpRoutePrefix + "chunks/not-an-id")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}

	peer := NewHTTPPeer("src", srv.URL)
	if _, err := peer.GetChunk(ctx, ChunkID("garbage")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want invalid argument got %v", err)
	}
}

func TestHTTPPeerTamperedChunkRejected(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 100})
	data := mkData(t, 1000)
	id, _ := src.PutBytes(ctx, data)

	// 中间人代理：篡改其中一个分块响应。
	upstream := httptest.NewServer(HTTPHandler(src))
	defer upstream.Close()
	m, _, _ := src.store.ReadManifest(id)
	victim := string(m.Chunks[1].ID)
	mitm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if bytes.Contains([]byte(r.URL.Path), []byte(victim)) {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, string(bytes.Repeat([]byte("X"), 100)))
			return
		}
		httputilNewRequest(r, upstream, w)
	}))
	defer mitm.Close()

	dst := newTestNode(t, Config{ChunkSize: 100}, NewHTTPPeer("mitm", mitm.URL))
	_, err := dst.GetBytes(ctx, id)
	if !errors.Is(err, ErrCorruptChunk) {
		t.Fatalf("want corrupt chunk rejected, got %v", err)
	}
	// 被篡改的受害者分块绝不能落盘（其他分块可正常缓存）。
	if dst.store.HasChunk(ChunkID(victim)) {
		t.Fatal("tampered victim chunk must never be committed")
	}
	// 整个内容不得作为有效内容返回：清单即使缓存，Verify 也必须失败。
	if err := dst.Verify(ctx, id); err == nil {
		t.Fatal("content with a tampered chunk must not verify")
	}
}

// httputilNewRequest 反向代理单个请求（避免引入额外依赖）。
func httputilNewRequest(r *http.Request, upstream *httptest.Server, w http.ResponseWriter) {
	// 代理的是 GET/HEAD（无请求体），直接转发即可。
	req, _ := http.NewRequest(r.Method, upstream.URL+r.URL.Path, nil)
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func TestHTTPServerProtectsChunkDuringTransfer(t *testing.T) {
	// 服务端在整个响应期间持有分块保护：即便本地 Full GC 并发运行，
	// 客户端也必须收到完整正确字节。
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 256})
	data := mkData(t, 20000)
	id, _ := src.PutBytes(ctx, data)
	if err := src.Pin(ctx, "held", id); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(HTTPHandler(src))
	defer srv.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var gcErr int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := src.GC(ctx, GCOptions{Full: true}); err != nil {
					atomic.StoreInt64(&gcErr, 1)
					return
				}
			}
		}
	}()

	peer := NewHTTPPeer("src", srv.URL, WithHTTPClient(&http.Client{Timeout: 10 * time.Second}))
	for i := 0; i < 20; i++ {
		raw, err := peer.GetManifest(ctx, id)
		if err != nil {
			t.Fatalf("manifest transfer interrupted by GC: %v", err)
		}
		if _, err := verifyManifestBytes(raw, id); err != nil {
			t.Fatalf("served corrupt manifest: %v", err)
		}
		m, err := verifyManifestBytes(raw, id)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range m.Chunks {
			b, err := peer.GetChunk(ctx, c.ID)
			if err != nil {
				t.Fatalf("chunk transfer interrupted by GC: %v", err)
			}
			if err := verifyChunk(c.ID, b); err != nil {
				t.Fatalf("served corrupt chunk: %v", err)
			}
		}
	}
	close(stop)
	wg.Wait()
	if atomic.LoadInt64(&gcErr) != 0 {
		t.Fatal("GC errored during serving")
	}
}

func TestCorruptChunkOnDiskRemovedByGC(t *testing.T) {
	ctx := context.Background()
	n := newTestNode(t, Config{ChunkSize: 100})
	data := mkData(t, 300)
	id, _ := n.PutBytes(ctx, data)
	if err := n.Pin(ctx, "keep", id); err != nil {
		t.Fatal(err)
	}
	m, _, _ := n.store.ReadManifest(id)
	// 直接在索引存在的情况下篡改分块字节。
	path := n.store.chunkPath(m.Chunks[0].ID)
	if err := os.WriteFile(path, bytes.Repeat([]byte("Q"), int(m.Chunks[0].Size)), 0o644); err != nil {
		t.Fatal(err)
	}
	// 无对端：Get 必须返回损坏错误，且绝不返回错误内容；坏副本被隔离。
	if _, err := n.GetBytes(ctx, id); !errors.Is(err, ErrCorruptChunk) {
		t.Fatalf("want ErrCorruptChunk got %v", err)
	}
	if n.store.HasChunk(m.Chunks[0].ID) {
		// ensureChunk 已隔离坏副本；索引应已剔除。
		t.Fatal("corrupt chunk should be quarantined from index")
	}
}
