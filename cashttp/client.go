package cashttp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lechandonga/content-addressed-store"
)

// Client 是 cashttp 服务端的客户端，并实现 cas.Fetcher，
// 使本地 Store 可以从一组对等节点按需补齐缺失的分块/清单。
type Client struct {
	// Peers 为对等节点的基础地址，如 "http://10.0.0.2:8080"。
	// 请求按顺序尝试，直到某个对端返回有效内容；全部失败才报错。
	Peers []string
	// HTTPClient 可自定义（超时、Transport 等）；为 nil 时使用带默认超时的客户端。
	HTTPClient *http.Client
	// MaxBlobBytes 限制单个 blob 的响应体大小，0 表示用默认 64MiB。
	MaxBlobBytes int64
	// VerifyResponse 为 true（默认）时对响应体做内容哈希校验，
	// 即便对端返回了被篡改的内容也会被本地拒绝。
	VerifyResponse bool
}

// NewClient 创建一个从给定对端拉取的客户端。
func NewClient(peers []string) *Client {
	return &Client{
		Peers:          peers,
		HTTPClient:     &http.Client{Timeout: 30 * time.Second},
		MaxBlobBytes:   64 << 20,
		VerifyResponse: true,
	}
}

// FetchManifest 实现 cas.Fetcher。
func (c *Client) FetchManifest(ctx context.Context, addr cas.Addr) ([]byte, error) {
	return c.fetch(ctx, "manifest", addr)
}

// FetchChunk 实现 cas.Fetcher。
func (c *Client) FetchChunk(ctx context.Context, addr cas.Addr) ([]byte, error) {
	return c.fetch(ctx, "chunk", addr)
}

func (c *Client) fetch(ctx context.Context, kind string, addr cas.Addr) ([]byte, error) {
	if len(c.Peers) == 0 {
		return nil, cas.ErrNotFound // 无对端：按“内容缺失”处理，触发上层明确失败。
	}
	limit := c.MaxBlobBytes
	if limit <= 0 {
		limit = 64 << 20
	}
	var lastErr error
	for _, peer := range c.Peers {
		raw, status, err := c.getFromPeer(ctx, peer, kind, addr, limit)
		if err == nil {
			if c.VerifyResponse && cas.AddressBytes(raw) != addr {
				// 对端给的内容与地址不符：判定该对端数据损坏，继续尝试下一个。
				lastErr = c.classify(kind, addr, http.StatusUnprocessableEntity,
					"peer returned content that fails address verification")
				continue
			}
			return raw, nil
		}
		if status == http.StatusNotFound {
			// 该对端明确没有；继续尝试其他对端。
			lastErr = c.classify(kind, addr, status, "peer does not have content")
			continue
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = cas.ErrNotFound
	}
	return nil, lastErr
}

func (c *Client) getFromPeer(ctx context.Context, peer, kind string, addr cas.Addr, limit int64) ([]byte, int, error) {
	base := strings.TrimRight(peer, "/")
	u, err := url.Parse(base)
	if err != nil {
		return nil, 0, failClient(kind, addr, "bad peer url %q: %v", peer, err)
	}
	u.Path = fmt.Sprintf("/cas/v1/%ss/%s", kind, url.PathEscape(string(addr)))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, failClient(kind, addr, "build request: %v", err)
	}
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, failClient(kind, addr, "request %s: %v", peer, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, c.classify(kind, addr, resp.StatusCode, readErrMsg(resp))
	}
	// 多读 1 字节用于发现超限。
	body := io.LimitReader(resp.Body, limit+1)
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, resp.StatusCode, failClient(kind, addr, "read body: %v", err)
	}
	if int64(len(data)) > limit {
		return nil, resp.StatusCode, &cas.Error{
			Kind: cas.KindCapacity, Op: "fetch-" + kind, Addr: string(addr),
			Msg: "response blob exceeds MaxBlobBytes",
		}
	}
	return data, resp.StatusCode, nil
}

func (c *Client) classify(kind string, addr cas.Addr, status int, msg string) *cas.Error {
	var k cas.FailureKind
	switch status {
	case http.StatusNotFound:
		if kind == "chunk" {
			k = cas.KindMissingChunk
		} else {
			k = cas.KindNotFound
		}
	case http.StatusUnprocessableEntity:
		if kind == "chunk" {
			k = cas.KindCorruptChunk
		} else {
			k = cas.KindCorruptManifest
		}
	case http.StatusBadRequest:
		k = cas.KindInvalid
	case http.StatusServiceUnavailable:
		k = cas.KindCapacity
	default:
		k = cas.KindIO
	}
	return &cas.Error{Kind: k, Op: "fetch-" + kind, Addr: string(addr), Msg: msg}
}

func failClient(kind string, addr cas.Addr, format string, args ...any) *cas.Error {
	return &cas.Error{
		Kind: cas.KindIO, Op: "fetch-" + kind, Addr: string(addr),
		Msg: fmt.Sprintf(format, args...),
	}
}

func readErrMsg(resp *http.Response) string {
	var eb errorBody
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&eb); err == nil && eb.Error != "" {
		return eb.Error
	}
	return http.StatusText(resp.StatusCode)
}
