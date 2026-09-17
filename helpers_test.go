package cas

import (
	"context"
	"testing"
)

// storeFetcher 把另一个 Store 适配为 Fetcher（内存/进程内“对端”）。
// 它通过 OpenChunk/OpenManifest 读取，只暴露已校验字节。
func storeFetcher(t *testing.T, peer *Store) Fetcher {
	t.Helper()
	return NewFetcher(func(ctx context.Context, kind string, addr Addr) ([]byte, error) {
		if kind == "manifest" {
			b, err := peer.OpenManifest(addr)
			if err != nil {
				return nil, err
			}
			return b, nil
		}
		return peer.OpenChunk(addr)
	})
}
