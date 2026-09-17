package cas

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTP 协议（v1），全部为无状态 GET，便于缓存与反代：
//
//	GET /cas/v1/manifests/<ContentID>  返回清单字节（application/octet-stream）
//	GET /cas/v1/chunks/<ChunkID>       返回分块字节
//	HEAD 同上                            存在性探测（200/404）
//
// 错误响应：
//
//	404 不存在；400 ID 非法；409 摘要不一致（服务端发现自身副本损坏时主动上报）；
//	500 其他内部错误。响应头 X-Cas-Error 携带类型化错误类别。
const (
	httpRoutePrefix = "/cas/v1/"
	hdrCasError     = "X-Cas-Error"
)

// HTTPHandler 把节点暴露为 CAS HTTP 服务。返回的 http.Handler 可挂到任意 mux。
//
// 服务端在流式发送过程中同样持有 guardian 保护：分块/清单在整个响应期间
// 不会被本地 GC 删除，保证对端收到的是完整一致的字节；客户端仍独立复核摘要。
func HTTPHandler(n *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(httpRoutePrefix+"manifests/", func(w http.ResponseWriter, r *http.Request) {
		id := ContentID(strings.TrimPrefix(r.URL.Path, httpRoutePrefix+"manifests/"))
		serveObject(w, r, string(id), IsValidContentID, func() (io.ReadCloser, int64, func(), error) {
			// 清单体积小：整读校验后再以内存字节发送，绝不分发损坏/被篡改清单。
			rel := n.store.guard.Acquire(string(id))
			_, raw, err := n.store.ReadManifest(id)
			if err != nil {
				rel()
				return nil, 0, nil, err
			}
			return io.NopCloser(bytes.NewReader(raw)), int64(len(raw)), rel, nil
		})
	})
	mux.HandleFunc(httpRoutePrefix+"chunks/", func(w http.ResponseWriter, r *http.Request) {
		id := ChunkID(strings.TrimPrefix(r.URL.Path, httpRoutePrefix+"chunks/"))
		serveObject(w, r, string(id), IsValidChunkID, func() (io.ReadCloser, int64, func(), error) {
			rel := n.store.guard.Acquire(string(id))
			rc, size, err := n.store.OpenChunk(id)
			if err != nil {
				rel()
				return nil, 0, nil, err
			}
			return rc, size, rel, nil
		})
	})
	return mux
}

type idValidator func(string) bool

func serveObject(w http.ResponseWriter, r *http.Request, id string,
	valid idValidator,
	openStream func() (io.ReadCloser, int64, func(), error)) {

	if !valid(id) {
		writeCasStatus(w, http.StatusBadRequest, "invalid-id")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeCasStatus(w, http.StatusMethodNotAllowed, "method-not-allowed")
		return
	}

	rc, size, rel, err := openStream()
	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			writeCasStatus(w, http.StatusNotFound, "not-found")
		case errors.Is(err, ErrCorruptChunk), errors.Is(err, ErrCorruptManifest):
			writeCasStatus(w, http.StatusConflict, "corrupt")
		default:
			writeCasStatus(w, http.StatusInternalServerError, "internal")
		}
		return
	}
	defer rc.Close()
	defer rel()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, rc); err != nil {
		// 连接中断等：响应已开始，无法改状态，仅结束（客户端会自行复核摘要）。
		return
	}
}

func writeCasStatus(w http.ResponseWriter, code int, marker string) {
	w.Header().Set(hdrCasError, marker)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": marker})
}

// HTTPPeer 是通过 HTTP 访问其他节点的 Peer 实现。
type HTTPPeer struct {
	name    string
	baseURL string
	client  *http.Client
}

// PeerOption 配置 HTTPPeer。
type PeerOption func(*HTTPPeer)

// WithHTTPClient 自定义 HTTP Client（超时、传输等）。
func WithHTTPClient(c *http.Client) PeerOption {
	return func(p *HTTPPeer) { p.client = c }
}

// NewHTTPPeer 创建一个 HTTP 对端。baseURL 形如 http://host:port/。
func NewHTTPPeer(name, baseURL string, opts ...PeerOption) *HTTPPeer {
	p := &HTTPPeer{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func (p *HTTPPeer) Name() string { return p.name }

func (p *HTTPPeer) urlFor(kind, id string) (string, error) {
	if strings.ContainsAny(id, "?#") {
		return "", opError(KindInvalidArgument, "http.url", id, nil)
	}
	return p.baseURL + httpRoutePrefix + kind + "/" + id, nil
}

func (p *HTTPPeer) doProbe(ctx context.Context, kind, id string) (bool, error) {
	u, err := p.urlFor(kind, id)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u, nil)
	if err != nil {
		return false, opError(KindNetwork, "http.head", id, err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return false, opError(KindNetwork, "http.head", id, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, httpStatusErr(resp, id)
	}
}

func (p *HTTPPeer) doGet(ctx context.Context, kind, id string) ([]byte, error) {
	u, err := p.urlFor(kind, id)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, opError(KindNetwork, "http.get", id, err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, opError(KindNetwork, "http.get", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, httpStatusErr(resp, id)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPObjectSize+1))
	if err != nil {
		return nil, opError(KindNetwork, "http.get", id, err)
	}
	if int64(len(data)) > maxHTTPObjectSize {
		return nil, opError(KindNetwork, "http.get", id,
			fmt.Errorf("object exceeds %d bytes", maxHTTPObjectSize))
	}
	return data, nil
}

// maxHTTPObjectSize 限制单次 HTTP 响应大小（分块默认最大 64MiB，清单很小），
// 防止恶意对端造成无界内存占用。
const maxHTTPObjectSize = 128 * 1024 * 1024

func httpStatusErr(resp *http.Response, id string) error {
	switch resp.StatusCode {
	case http.StatusNotFound:
		return opError(KindNotFound, "http.get", id, nil)
	case http.StatusConflict:
		return opError(KindCorruptChunk, "http.get", id,
			fmt.Errorf("peer reports corrupt local copy"))
	case http.StatusBadRequest:
		return opError(KindInvalidArgument, "http.get", id, nil)
	default:
		return opError(KindNetwork, "http.get", id,
			fmt.Errorf("unexpected status %d (%s)", resp.StatusCode, resp.Header.Get(hdrCasError)))
	}
}

func (p *HTTPPeer) GetChunk(ctx context.Context, id ChunkID) ([]byte, error) {
	return p.doGet(ctx, "chunks", string(id))
}

func (p *HTTPPeer) GetManifest(ctx context.Context, id ContentID) ([]byte, error) {
	return p.doGet(ctx, "manifests", string(id))
}

func (p *HTTPPeer) HasChunk(ctx context.Context, id ChunkID) (bool, error) {
	return p.doProbe(ctx, "chunks", string(id))
}

func (p *HTTPPeer) HasManifest(ctx context.Context, id ContentID) (bool, error) {
	return p.doProbe(ctx, "manifests", string(id))
}
