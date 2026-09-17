// Package cashttp 基于仓库既有网络能力（标准库 net/http）对外提供
// 内容寻址数据的按需分发，并提供对应的客户端 Fetcher 实现。
//
// 协议（v1，详见 docs/http-protocol.md）：
//
//	GET /cas/v1/manifests/{addr}  读取清单（已校验）
//	GET /cas/v1/chunks/{addr}     读取分块（已校验）
//	HEAD 同路径                    探测存在性
//
// 服务端只会返回通过内容哈希校验的字节；损坏/缺失通过明确的 HTTP 状态码区分。
package cashttp

import (
	"errors"
	"net/http"
	"strings"

	"github.com/lechandonga/content-addressed-store"
)

// APIPrefix 是所有 CAS HTTP 路由的公共前缀。
const APIPrefix = "/cas/v1/"

// BlobSource 是服务端读取本地内容所需的最小接口，*cas.Store 天然满足。
type BlobSource interface {
	OpenManifest(addr cas.Addr) ([]byte, error)
	OpenChunk(addr cas.Addr) ([]byte, error)
}

// Server 是一个 http.Handler，对外暴露只读的内容分发接口。
type Server struct {
	src BlobSource
}

// NewServer 创建分发服务端。
func NewServer(src BlobSource) *Server {
	return &Server{src: src}
}

// Handler 返回可挂载到自定义 mux 的路由处理器。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(APIPrefix+"manifests/", s.handleManifest)
	mux.HandleFunc(APIPrefix+"chunks/", s.handleChunk)
	mux.HandleFunc(APIPrefix, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == APIPrefix {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte("cas v1\n"))
			return
		}
		writeError(w, http.StatusNotFound, "not_found", "unknown route")
	})
	return mux
}

// ServeHTTP 使 Server 直接实现 http.Handler。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.Handler().ServeHTTP(w, r)
}

func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	s.serve(w, r, "manifest")
}

func (s *Server) handleChunk(w http.ResponseWriter, r *http.Request) {
	s.serve(w, r, "chunk")
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request, kind string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET or HEAD")
		return
	}
	addr, ok := addrFromPath(r.URL.Path, kind)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_address", "bad content address")
		return
	}

	var (
		data []byte
		err  error
	)
	if kind == "manifest" {
		data, err = s.src.OpenManifest(addr)
	} else {
		data, err = s.src.OpenChunk(addr)
	}
	if err != nil {
		writeCasError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-CAS-Addr", string(addr))
	w.Header().Set("X-CAS-Kind", kind)
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func addrFromPath(path, kind string) (cas.Addr, bool) {
	prefix := APIPrefix + kind + "s/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}
	raw := strings.TrimPrefix(path, prefix)
	// 拒绝任何路径式后缀，杜绝目录穿越尝试。
	if raw == "" || strings.ContainsAny(raw, "/\\?#") {
		return "", false
	}
	a, err := cas.ParseAddr(raw)
	if err != nil {
		return "", false
	}
	return a, true
}

// writeCasError 将 cas 错误类别映射为明确的 HTTP 状态码与错误体。
func writeCasError(w http.ResponseWriter, err error) {
	var ce *cas.Error
	if errors.As(err, &ce) {
		switch ce.Kind {
		case cas.KindNotFound, cas.KindRefNotFound:
			writeError(w, http.StatusNotFound, "not_found", ce.Msg)
			return
		case cas.KindMissingChunk:
			writeError(w, http.StatusNotFound, "missing_chunk", ce.Msg)
			return
		case cas.KindCorruptChunk, cas.KindCorruptManifest:
			// 损坏内容不对外分发：以 422 表达“存在但不可用/未通过校验”，
			// 客户端会转而尝试其他对端。
			writeError(w, http.StatusUnprocessableEntity, "corrupt", ce.Msg)
			return
		case cas.KindInvalid:
			writeError(w, http.StatusBadRequest, "invalid", ce.Msg)
			return
		case cas.KindCapacity:
			writeError(w, http.StatusServiceUnavailable, "capacity", ce.Msg)
			return
		}
	}
	writeError(w, http.StatusInternalServerError, "io_error", "internal error")
}
