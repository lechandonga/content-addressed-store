package cas

import (
	"context"
)

// Fetcher 抽象“从其他节点按需获取数据”的网络能力。
// cashttp.Client 是其默认实现；测试中可注入内存实现。
//
// 契约：
//   - 内容确实不存在时返回可被 errors.Is(.., cas.ErrNotFound) 识别的错误；
//   - 返回的字节必须是完整的分块/清单字节，本地会再次做哈希校验，
//     对端返回的任何被篡改内容都会被判为 ErrCorruptChunk/ErrCorruptManifest；
//   - ctx 取消时尽快返回。
type Fetcher interface {
	// FetchChunk 从对端获取指定分块。
	FetchChunk(ctx context.Context, addr Addr) ([]byte, error)
	// FetchManifest 从对端获取指定清单。
	FetchManifest(ctx context.Context, addr Addr) ([]byte, error)
}

// FetchFunc 是函数式 Fetcher，便于在不关心清单/分块区别时快速构造（例如测试）。
type FetchFunc func(ctx context.Context, kind string, addr Addr) ([]byte, error)

// funcFetcher 把 FetchFunc 适配为 Fetcher。
type funcFetcher struct{ f FetchFunc }

// NewFetcher 用一个统一函数构造 Fetcher。
// kind 为 "chunk" 或 "manifest"。
func NewFetcher(f FetchFunc) Fetcher { return &funcFetcher{f: f} }

func (f *funcFetcher) FetchChunk(ctx context.Context, addr Addr) ([]byte, error) {
	return f.f(ctx, "chunk", addr)
}

func (f *funcFetcher) FetchManifest(ctx context.Context, addr Addr) ([]byte, error) {
	return f.f(ctx, "manifest", addr)
}
