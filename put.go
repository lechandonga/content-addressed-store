package cas

import (
	"context"
	"errors"
	"os"
)

// Put 将一段内容写入本地存储，返回其内容标识（清单地址，即 CID）。
//
// 保证：
//   - 内容被分块并逐块哈希，清单自身也按内容寻址，可被任何持有 CID 的人完整验证；
//   - 重复内容（相同字节、或与已有内容共享分块）只落盘新增的那一份分块；
//   - 多个并发 Put 相同内容不会产生重复文件或相互覆盖（去重提交由 commitMu 串行化）；
//   - 容量不足时先尝试回收；回收后仍放不下返回 ErrCapacity，已写入的孤儿分块
//     会在后续 GC 中被安全清理，不会遗留不可回收残留。
func (s *Store) Put(content []byte) (Addr, error) {
	if err := s.checkClosed(); err != nil {
		return "", err
	}
	if int64(len(content)) > s.cfg.MaxObjectSize {
		return "", fail(KindCapacity, "put", "",
			guardf("object size %d exceeds MaxObjectSize %d", len(content), s.cfg.MaxObjectSize), nil)
	}

	refs, blocks, err := BuildChunks(content, s.cfg.ChunkSize)
	if err != nil {
		return "", err
	}
	_, mraw, cid, err := BuildManifest(refs)
	if err != nil {
		return "", err
	}

	// 整内容级去重：并发 Put 同一内容只做一次提交工作。
	_, _, perr := s.getFlight.do("put:"+string(cid), func() (any, error) {
		return nil, s.putCommit(cid, refs, blocks, mraw)
	})
	if perr != nil {
		return "", perr
	}
	return cid, nil
}

// putCommit 执行实际的去重提交与容量判定。
func (s *Store) putCommit(cid Addr, refs []ChunkRef, blocks [][]byte, mraw []byte) error {
	for attempt := 0; ; attempt++ {
		s.beginRead()
		s.commitMu.Lock()
		// 清单已存在 => 整条内容此前已写入，直接复用。
		if s.hasLocked(bManifest, cid) {
			s.touchLocked(cid)
			s.commitMu.Unlock()
			s.endRead()
			return nil
		}
		// 统计尚需写入的新字节（分块可能已作为其他内容的一部分存在）。
		var need int64
		missingChunks := make([]ChunkRef, 0, len(refs))
		missingBlocks := make([][]byte, 0, len(refs))
		for i, r := range refs {
			if !s.hasLocked(bChunk, r.Addr) {
				need += r.Size
				missingChunks = append(missingChunks, r)
				missingBlocks = append(missingBlocks, blocks[i])
			}
		}
		need += int64(len(mraw))

		if s.cfg.MaxBytes > 0 && s.usageCommitted+need > s.cfg.MaxBytes {
			s.commitMu.Unlock()
			s.endRead()
			if attempt >= 1 {
				return fail(KindCapacity, "put", string(cid),
					guardf("need %d more bytes but capacity %d is exhausted after GC",
						need, s.cfg.MaxBytes), nil)
			}
			// 容量紧张：在锁外执行回收，然后重新评估。
			if _, gerr := s.Collect(context.Background(), GCConfig{MakeRoom: need}); gerr != nil {
				return gerr
			}
			continue
		}

		// 提交所有缺失分块（commitIfAbsent 逐个再确认一次存在性，防御并发）。
		for i, r := range missingChunks {
			rel := dirChunks + "/" + shardOf(r.Addr) + "/" + string(r.Addr)
			committed, cerr := s.commitIfAbsentLocked(rel, missingBlocks[i])
			if cerr != nil {
				s.commitMu.Unlock()
				s.endRead()
				return cerr
			}
			_ = committed
		}
		// 最后提交清单：清单可见即表示整条内容完整（其分块均已就位）。
		rel := dirManifests + "/" + shardOf(cid) + "/" + string(cid)
		if _, cerr := s.commitIfAbsentLocked(rel, mraw); cerr != nil {
			s.commitMu.Unlock()
			s.endRead()
			return cerr
		}
		s.touchLocked(cid)
		s.commitMu.Unlock()
		s.endRead()
		return nil
	}
}

// touchLocked 更新清单的访问时间，作为无引用内容 LRU 回收的依据。
// 调用方需持有 commitMu。
func (s *Store) touchLocked(a Addr) {
	path := s.manifestPath(a)
	now := s.now()
	if err := os.Chtimes(path, now, now); err != nil && !errors.Is(err, os.ErrNotExist) {
		// 时间戳仅影响回收顺序，失败不影响正确性。
		return
	}
}
