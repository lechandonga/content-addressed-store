package cashttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lechandonga/content-addressed-store"
)

func newNode(t *testing.T, chunkSize int) *cas.Store {
	t.Helper()
	s, err := cas.Open(t.TempDir(), cas.Config{ChunkSize: chunkSize})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func startServer(t *testing.T, s *cas.Store) string {
	t.Helper()
	srv := httptest.NewServer(NewServer(s))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestServerServesAndClientFetches(t *testing.T) {
	content := bytes.Repeat([]byte("abcdefgh-12345678-"), 200)
	src := newNode(t, 16)
	cid, err := src.Put(content)
	if err != nil {
		t.Fatal(err)
	}
	url := startServer(t, src)

	dst := newNode(t, 16)
	dst.SetFetcher(NewClient([]string{url}))

	got, err := dst.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("distributed get: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("distributed content mismatch")
	}
}

func TestServerStatusCodes(t *testing.T) {
	src := newNode(t, 16)
	cid, _ := src.Put([]byte("hello"))
	url := startServer(t, src)

	resp, err := http.Get(url + "/cas/v1/manifests/" + string(cid))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}

	// HEAD 探测：200 且无响应体。
	req, _ := http.NewRequest(http.MethodHead, url+"/cas/v1/manifests/"+string(cid), nil)
	hresp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	hresp.Body.Close()
	if hresp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD want 200, got %d", hresp.StatusCode)
	}

	// 不存在的分块 -> 404 missing_chunk。
	missing := cas.AddressBytes([]byte("missing"))
	resp2, err := http.Get(url + "/cas/v1/chunks/" + string(missing))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp2.StatusCode)
	}
	var eb errorBody
	_ = json.NewDecoder(resp2.Body).Decode(&eb)
	if eb.Error != "missing_chunk" {
		t.Fatalf("want missing_chunk body, got %+v", eb)
	}

	// 非法地址/穿越尝试 -> 400。编码后的斜杠能到达服务端，
	// 服务端必须拒绝任何包含路径分隔符的地址。
	for _, bad := range []string{
		url + "/cas/v1/chunks/not-an-address",
		url + "/cas/v1/chunks/sha256-aaaa%2f..%2f..%2fetc%2fpasswd",
		url + "/cas/v1/manifests/sha256-aaaa?evil",
	} {
		r, err := http.Get(bad)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusBadRequest {
			t.Fatalf("want 400 for %s, got %d", bad, r.StatusCode)
		}
	}
}

func TestMultiPeerFallback(t *testing.T) {
	content := bytes.Repeat([]byte("xy"), 100)
	src := newNode(t, 8)
	cid, _ := src.Put(content)
	url := startServer(t, src)
	emptyURL := startServer(t, newNode(t, 8))

	// 第一个对端为空，客户端必须回退到第二个对端。
	dst := newNode(t, 8)
	dst.SetFetcher(NewClient([]string{emptyURL, url}))
	got, err := dst.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("peer fallback failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("fallback content mismatch")
	}
}

// tamperingHandler 对任何 blob 请求都返回 200 + 错误字节。
type tamperingHandler struct{}

func newTamperingHandler() http.Handler { return tamperingHandler{} }

func (tamperingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, "/manifests/") || strings.Contains(r.URL.Path, "/chunks/") {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("malicious-and-incorrect-bytes"))
		return
	}
	http.NotFound(w, r)
}

func TestMaliciousPeerRejected(t *testing.T) {
	content := []byte("content that will be served wrongly")
	src := newNode(t, 8)
	cid, _ := src.Put(content)
	goodURL := startServer(t, src)

	malSrv := httptest.NewServer(newTamperingHandler())
	t.Cleanup(malSrv.Close)

	dst := newNode(t, 8)
	// 恶意对端排在前面：其内容必须被本地哈希校验拒绝后回退到正常对端。
	dst.SetFetcher(NewClient([]string{malSrv.URL, goodURL}))
	got, err := dst.Get(context.Background(), cid)
	if err != nil {
		t.Fatalf("should fall back past malicious peer: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("content from good peer mismatch")
	}
}

// countingHandler 统计 manifest 请求次数，验证并发获取被 singleflight 合并。
type countingHandler struct {
	inner       http.Handler
	manifestHit int64
	mu          sync.Mutex
}

func (h *countingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, "/manifests/") {
		atomic.AddInt64(&h.manifestHit, 1)
	}
	h.inner.ServeHTTP(w, r)
}

func TestConcurrentDistributedGetDedup(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 50)
	src := newNode(t, 10)
	cid, _ := src.Put(content)

	h := &countingHandler{inner: NewServer(src)}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	dst := newNode(t, 10)
	dst.SetFetcher(NewClient([]string{srv.URL}))

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			b, err := dst.Get(context.Background(), cid)
			if err == nil && !bytes.Equal(b, content) {
				err = errors.New("content mismatch")
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("getter %d: %v", i, err)
		}
	}
	if got := atomic.LoadInt64(&h.manifestHit); got != 1 {
		t.Fatalf("manifest requested %d times, expected exactly 1", got)
	}
}
