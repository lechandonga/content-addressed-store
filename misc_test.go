package cas

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPHeadAndMethodNotAllowed(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 128})
	id, _ := src.PutBytes(ctx, []byte("head-check"))
	srv := httptest.NewServer(HTTPHandler(src))
	defer srv.Close()

	m, _, _ := src.Store().ReadManifest(id)
	curl := srv.URL + httpRoutePrefix + "chunks/" + string(m.Chunks[0].ID)

	// HEAD：200 且无响应体。
	resp, err := http.Head(curl)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status=%d", resp.StatusCode)
	}
	if resp.Request.Method != http.MethodHead {
		t.Fatal("method mismatch")
	}

	// POST：405。
	req, _ := http.NewRequest(http.MethodPost, curl, strings.NewReader("x"))
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status=%d want 405", resp2.StatusCode)
	}

	// HEAD 不存在：404。
	missing := srv.URL + httpRoutePrefix + "chunks/" + "cas1c-" + strings.Repeat("0", 64)
	resp3, err := http.Head(missing)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusNotFound {
		t.Fatalf("missing HEAD status=%d want 404", resp3.StatusCode)
	}
}

func TestHTTPPeerHasMethods(t *testing.T) {
	ctx := context.Background()
	src := newTestNode(t, Config{ChunkSize: 64})
	id, _ := src.PutBytes(ctx, []byte("has-methods-data"))
	srv := httptest.NewServer(HTTPHandler(src))
	defer srv.Close()
	p := NewHTTPPeer("src", srv.URL)

	ok, err := p.HasManifest(ctx, id)
	if err != nil || !ok {
		t.Fatalf("HasManifest: %v %v", ok, err)
	}
	m, _, _ := src.Store().ReadManifest(id)
	ok, err = p.HasChunk(ctx, m.Chunks[0].ID)
	if err != nil || !ok {
		t.Fatalf("HasChunk: %v %v", ok, err)
	}
	ok, err = p.HasManifest(ctx, ContentID("cas1-"+strings.Repeat("f", 64)))
	if err != nil || ok {
		t.Fatalf("HasManifest missing: %v %v", ok, err)
	}
}

func TestAddPeerDedup(t *testing.T) {
	n := newTestNode(t, Config{})
	p1 := NewHTTPPeer("same", "http://127.0.0.1:1")
	n.AddPeer(p1)
	n.AddPeer(p1)
	n.AddPeer(nil)
	if got := len(n.Peers()); got != 1 {
		t.Fatalf("peers=%d want 1", got)
	}
}

func TestGuardianActiveUsers(t *testing.T) {
	g := newGuardian()
	if g.ActiveUsers("x") != 0 {
		t.Fatal("fresh key users!=0")
	}
	r1 := g.Acquire("x")
	if g.ActiveUsers("x") != 1 {
		t.Fatal("users!=1")
	}
	r2 := g.Acquire("x")
	_ = r2
	if g.ActiveUsers("x") != 2 {
		t.Fatal("users!=2")
	}
	r1()
	if g.ActiveUsers("x") != 1 {
		t.Fatal("users!=1 after release")
	}
	// 独占等待共享排空。
	done := make(chan struct{})
	go func() { g.GCLock("x"); close(done); g.GCUnlock("x") }()
	select {
	case <-done:
		t.Fatal("GCLock must wait for shared users")
	default:
	}
	// 标记时活跃键被跳过。
	if marked := g.MarkCandidates([]string{"x"}); len(marked) != 0 {
		t.Fatalf("active key must not be marked, got %v", marked)
	}
	r2()
	<-done
}

func TestIDValidation(t *testing.T) {
	for _, okID := range []string{"cas1c-" + strings.Repeat("a", 64), "cas1-" + strings.Repeat("0", 64)} {
		if strings.HasPrefix(okID, "cas1c") != IsValidChunkID(okID) {
			t.Fatalf("valid id rejected: %s", okID)
		}
	}
	for _, bad := range []string{"", "cas1c-xyz", "cas1-" + strings.Repeat("A", 64), "cas2-" + strings.Repeat("0", 64)} {
		if IsValidChunkID(bad) || IsValidContentID(bad) {
			t.Fatalf("bad id accepted: %q", bad)
		}
	}
	if s, err := shard("cas1-" + strings.Repeat("ab", 32)); err != nil || s != "ab" {
		t.Fatalf("shard=%s,%v", s, err)
	}
}
