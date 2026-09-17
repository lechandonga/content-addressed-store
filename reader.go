package cas

import (
	"context"
	"fmt"
	"io"
)

// contentReader 是 Node.Get 返回的流式读取器：按清单顺序逐分块暴露内容。
// 每个分块在 openChunkVerified 中完成“确保可用 + 整分块校验”，并以已校验
// 字节提供；分块的 guardian 共享保护一直持有到该分块交付结束，因此 GC
// 无法在读取中途回收它。全部分块读完后做整体长度收口校验。
type contentReader struct {
	node *Node
	ctx  context.Context
	id   ContentID
	m    *Manifest

	idx    int       // 当前分块序号
	cur    io.Reader // 当前分块（已校验字节）
	curRel func()    // 当前分块的 guardian 释放
	closed bool
	offset int64 // 已输出字节数（最终必须等于 m.Length）
}

func (r *contentReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("cas: content reader already closed")
	}
	if err := r.ctx.Err(); err != nil {
		_ = r.closeCurrent()
		return 0, err
	}
	if r.idx >= len(r.m.Chunks) {
		if r.offset != r.m.Length {
			return 0, opError(KindContentMismatch, "content.read", string(r.id),
				fmt.Errorf("delivered %d bytes, manifest declares %d", r.offset, r.m.Length))
		}
		return 0, io.EOF
	}
	if r.cur == nil {
		if err := r.openNext(); err != nil {
			return 0, err
		}
	}
	n, err := r.cur.Read(p)
	r.offset += int64(n)
	if err == io.EOF {
		_ = r.closeCurrent()
		r.idx++
		if n > 0 {
			return n, nil
		}
		return r.Read(p) // 进入下一分块
	}
	return n, err
}

// openNext 在单个 guardian 保护窗口内确保分块可用并取得已校验字节；
// 本地缺失/损坏在该窗口内（隔离后）从对端补齐，GC 无法在读取中途回收它。
func (r *contentReader) openNext() error {
	ref := r.m.Chunks[r.idx]
	rc, _, rel, err := r.node.openChunkVerified(r.ctx, ref.ID, ref.Size)
	if err != nil {
		return err
	}
	r.cur = rc
	r.curRel = rel
	return nil
}

func (r *contentReader) closeCurrent() error {
	var err error
	if c, ok := r.cur.(io.Closer); ok && r.cur != nil {
		err = c.Close()
	}
	r.cur = nil
	if r.curRel != nil {
		r.curRel()
		r.curRel = nil
	}
	return err
}

// Close 释放当前分块（以及尚未读取的后续分块不会被打开）。
func (r *contentReader) Close() error {
	r.closed = true
	return r.closeCurrent()
}
