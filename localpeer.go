package cas

import (
	"context"
	"errors"
	"io"
)

// localPeer 把另一个 *Node 直接作为 Peer（进程内/同机互联、测试用），
// 语义与 HTTP 对端一致：读取前先校验，坏数据以类型化错误上报。
type localPeer struct {
	peerName string
	n        *Node
}

// NewLocalPeer 包装一个节点作为对端。
func NewLocalPeer(name string, n *Node) Peer { return &localPeer{peerName: name, n: n} }

func (p *localPeer) Name() string { return p.peerName }

func (p *localPeer) GetChunk(ctx context.Context, id ChunkID) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rel := p.n.store.guard.Acquire(string(id))
	defer rel()
	rc, _, err := p.n.store.OpenChunk(id)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		if errors.Is(err, ErrCorruptChunk) {
			return nil, err
		}
		return nil, opError(KindCorruptChunk, "localPeer.getChunk", string(id), err)
	}
	if err := verifyChunk(id, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (p *localPeer) GetManifest(ctx context.Context, id ContentID) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rel := p.n.store.guard.Acquire(string(id))
	defer rel()
	_, raw, err := p.n.store.ReadManifest(id)
	return raw, err
}

func (p *localPeer) HasChunk(ctx context.Context, id ChunkID) (bool, error) {
	return p.n.store.HasChunk(id), nil
}

func (p *localPeer) HasManifest(ctx context.Context, id ContentID) (bool, error) {
	return p.n.store.HasManifest(id), nil
}
