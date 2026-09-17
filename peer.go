package cas

import (
	"context"
)

// Peer 是节点可获取内容的远端来源（HTTP 对端、其他本地节点、测试替身等）。
// 实现必须保证：返回的字节在调用方校验失败时可被丢弃，不得污染本地存储。
type Peer interface {
	// Name 返回对端标识（日志/错误诊断用）。
	Name() string
	// GetChunk 拉取分块完整字节。本地不存在时返回包装了 ErrNotFound 的错误。
	GetChunk(ctx context.Context, id ChunkID) ([]byte, error)
	// GetManifest 拉取清单字节。
	GetManifest(ctx context.Context, id ContentID) ([]byte, error)
	// HasChunk/HasManifest 报告对端是否持有对应数据（用于选择来源）。
	HasChunk(ctx context.Context, id ChunkID) (bool, error)
	HasManifest(ctx context.Context, id ContentID) (bool, error)
}

// PeerSource 是在构造时提供的对端来源（可选，用于动态对端列表）。
type PeerSource interface {
	Peers() []Peer
}
