package cas

import (
	"context"
)

// Get 按内容标识（CID，即清单地址）取回完整内容。
//
// 验证与恢复保证：
//   - 先读取并哈希校验清单本身；再按清单顺序逐块校验分块。
//     任何清单被篡改、分块损坏/缺失且无法从对端补齐的情况，
//     都不会返回拼接结果，而是返回可区分的 *cas.Error
//     （ErrCorruptManifest / ErrCorruptChunk / ErrMissingChunk / ErrNotFound）。
//   - 本地缺失的分块/清单通过配置的 Fetcher 从其他节点按需补齐；
//     多个并发 Get 同一内容由 singleflight 合并，只拉取一次、只落盘一份。
//   - 回收后再次访问：若本地已被清理，则自动重新获取并校验后返回，
//     绝不会把残留的损坏数据当有效内容。
func (s *Store) Get(ctx context.Context, cid Addr) ([]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if !cid.Valid() {
		return nil, fail(KindInvalid, "get", string(cid), "bad content address", nil)
	}

	v, _, err := s.getFlight.do("get:"+string(cid), func() (any, error) {
		return s.getAssemble(ctx, cid, nil)
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// getProtected 取回内容，并在“内容已完整落盘、gcMu 读锁仍持有”的状态下
// 调用 protected。protected 执行期间 GC 被读锁阻塞，因此它写入引用后，
// 被引用内容从那一刻起就受引用保护，整条“获取 -> 保留”链路不存在回收窗口。
//
// 注意：网络获取在更早阶段（锁外）完成；只有最后的校验/提交与 protected
// 运行在读锁临界区，慢对端不会长时间阻塞回收。
func (s *Store) getProtected(ctx context.Context, cid Addr, protected func() error) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	// 使用独立飞行键：Pin 与普通 Get 合并“获取”的意义不大，
	// 且避免两者返回值形态不同造成耦合。内容落盘后仍由 commitIfAbsent 去重，
	// 不会产生重复副本。
	_, _, err := s.getFlight.do("pin:"+string(cid), func() (any, error) {
		_, gerr := s.getAssemble(ctx, cid, protected)
		return nil, gerr
	})
	return err
}

// getAssemble 执行“清单 -> 逐块 -> 拼接”的完整校验流程。
//
// 落盘原子性：当需要从对端补齐时，所有慢网络获取都在 gcMu 之外完成；
// 随后在一次 gcMu 读锁临界区内按“先所有分块、最后清单”的顺序提交。
// 因此磁盘上始终维持“清单可见 ⇒ 全部分块就位”的不变量，
// 并发 GC 不可能观察到半条内容，也不会误隔离正在装配的清单。
func (s *Store) getAssemble(ctx context.Context, cid Addr, protected func() error) ([]byte, error) {
	const maxAttempts = 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, fail(KindIO, "get", string(cid), "context cancelled", err)
		}

		// 快速路径：清单与全部分块本地齐全时，逐块校验并拼接。
		// tryLocalAssemble 在同一个读锁临界区内完成“校验 + 受保护动作”，
		// 杜绝两者之间被 GC 插入回收。
		assembled, fast, ferr := s.tryLocalAssemble(cid, protected)
		if fast {
			if ferr != nil {
				return nil, ferr
			}
			s.touch(cid)
			return assembled, nil
		} else if ferr != nil {
			if !isMissingOrCorrupt(ferr) {
				return nil, ferr
			}
			// 本地处于损坏/缺失状态且没有任何对端：这是“定论”，
			// 立即返回可区分原因，避免隔离后重试把 corrupt 掩盖成 missing。
			if s.fetcher == nil {
				return nil, ferr
			}
			// 分块缺失（例如外部删除）而清单本身仍在：把这份“不完整”的本地
			// 清单隔离掉，慢路径将从对端重新取回完整版本，避免反复命中坏状态。
			if missing, _ := s.localManifestIncomplete(cid); missing {
				_ = s.quarantine(s.manifestPath(cid), "incomplete-manifest")
			}
		}

		// 慢路径：先在锁外取到“已哈希校验”的清单字节（不立即落盘）。
		mraw, err := s.fetchVerified(ctx, bManifest, cid)
		if err != nil {
			return nil, err
		}
		m, err := DecodeManifest(mraw, cid)
		if err != nil {
			lastErr = err
			if attempt < maxAttempts {
				continue
			}
			return nil, err
		}
		if m.Size > s.cfg.MaxObjectSize {
			return nil, fail(KindCapacity, "get", string(cid),
				guardf("object size %d exceeds MaxObjectSize %d", m.Size, s.cfg.MaxObjectSize), nil)
		}

		// 锁外逐块获取已校验字节，并记录哪些本地尚缺（需要落盘）。
		blocks := make([][]byte, len(m.Chunks))
		missingIdx := make([]int, 0, len(m.Chunks))
		fetchFailed := false
		for i, ref := range m.Chunks {
			b, lerr := s.loadVerified(bChunk, ref.Addr) // 读锁内完成
			if lerr == nil {
				blocks[i] = b
				continue
			}
			if !isMissingOrCorrupt(lerr) {
				return nil, lerr
			}
			raw, ferr := s.fetchVerified(ctx, bChunk, ref.Addr)
			if ferr != nil {
				lastErr = ferr
				fetchFailed = true
				break
			}
			if int64(len(raw)) != ref.Size {
				return nil, fail(KindCorruptChunk, "assemble", string(ref.Addr),
					guardf("chunk %d size %d != manifest size %d", i, len(raw), ref.Size), nil)
			}
			blocks[i] = raw
			missingIdx = append(missingIdx, i)
		}
		if fetchFailed {
			if s.fetcher == nil {
				return nil, lastErr // 无对端：定论
			}
			if attempt < maxAttempts {
				continue
			}
			return nil, lastErr
		}

		// 原子提交临界区：GC 无法在分块与清单提交之间插入。
		// 容量预评估：计算尚需落盘的字节，超限则先在锁外回收（普通 Get）。
		// 受保护获取（Pin）不允许驱逐任何内容——包括正在建立的这条——
		// 因为其调用语义要求“获取即可保留”；放不下时直接返回 ErrCapacity。
		var need int64
		for _, i := range missingIdx {
			need += int64(len(blocks[i]))
		}
		need += int64(len(mraw))
		if s.cfg.MaxBytes > 0 {
			s.beginRead()
			s.commitMu.Lock()
			over := s.usageCommitted+need > s.cfg.MaxBytes
			s.commitMu.Unlock()
			s.endRead()
			if over {
				if protected != nil {
					return nil, fail(KindCapacity, "get", string(cid),
						guardf("need %d bytes but capacity %d exhausted and content must be retained",
							need, s.cfg.MaxBytes), nil)
				}
				if attempt >= maxAttempts-1 {
					return nil, fail(KindCapacity, "get", string(cid),
						guardf("need %d bytes but capacity %d exhausted after GC",
							need, s.cfg.MaxBytes), nil)
				}
				if _, gerr := s.Collect(ctx, GCConfig{MakeRoom: need}); gerr != nil {
					return nil, gerr
				}
				continue
			}
		}

		s.beginRead()
		cerr := func() error {
			s.commitMu.Lock()
			defer s.commitMu.Unlock()
			for _, i := range missingIdx {
				ref := m.Chunks[i]
				rel := dirChunks + "/" + shardOf(ref.Addr) + "/" + string(ref.Addr)
				if _, err := s.commitIfAbsentLocked(rel, blocks[i]); err != nil {
					return err
				}
			}
			rel := dirManifests + "/" + shardOf(cid) + "/" + string(cid)
			if _, err := s.commitIfAbsentLocked(rel, mraw); err != nil {
				return err
			}
			return nil
		}()
		if cerr == nil {
			// 回读复核（读锁内，文件不会被回收）。
			if data, rerr := s.loadVerified(bManifest, cid); rerr == nil {
				if _, derr := DecodeManifest(data, cid); derr != nil {
					cerr = derr
				}
			} else {
				cerr = rerr
			}
		}
		// 提交并复核成功后，仍在读锁内执行受保护动作（写引用），
		// 保证“内容可见”与“引用落盘”对 GC 而言是一个不可分割的受保护区间。
		if cerr == nil && protected != nil {
			cerr = protected()
		}
		s.endRead()
		if cerr != nil {
			lastErr = cerr
			if attempt < maxAttempts {
				continue
			}
			return nil, cerr
		}

		out := make([]byte, 0, m.Size)
		for _, b := range blocks {
			out = append(out, b...)
		}
		if int64(len(out)) != m.Size {
			return nil, fail(KindCorruptManifest, "assemble", "",
				guardf("assembled size %d != manifest size %d", len(out), m.Size), nil)
		}
		s.touch(cid)
		return out, nil
	}
	return nil, lastErr
}

// localManifestIncomplete 判断本地清单存在但其某个分块缺失（区别于清单自身缺失）。
func (s *Store) localManifestIncomplete(cid Addr) (bool, error) {
	s.beginRead()
	defer s.endRead()
	raw, err := s.loadVerified(bManifest, cid)
	if err != nil {
		return false, err
	}
	m, err := DecodeManifest(raw, cid)
	if err != nil {
		return false, err
	}
	for _, c := range m.Chunks {
		if !s.hasLocked(bChunk, c.Addr) {
			return true, nil
		}
	}
	return false, nil
}

// tryLocalAssemble 尝试纯本地装配。fast=false 表示至少有一个 blob 缺失/损坏，
// 调用方应转入“网络补齐 + 原子提交”慢路径。
//
// 全部校验通过后，protected（若非空）会在仍持有 gcMu 读锁时执行，
// 使写引用与内容存在性处于同一个 GC 不可分割的保护区间。
func (s *Store) tryLocalAssemble(cid Addr, protected func() error) (out []byte, fast bool, err error) {
	s.beginRead()
	defer s.endRead()
	mraw, lerr := s.loadVerified(bManifest, cid)
	if lerr != nil {
		return nil, false, lerr
	}
	m, derr := DecodeManifest(mraw, cid)
	if derr != nil {
		return nil, false, derr
	}
	out = make([]byte, 0, m.Size)
	for _, ref := range m.Chunks {
		b, cerr := s.loadVerified(bChunk, ref.Addr)
		if cerr != nil {
			return nil, false, cerr
		}
		if int64(len(b)) != ref.Size {
			return nil, true, fail(KindCorruptChunk, "assemble", string(ref.Addr),
				guardf("chunk size %d != manifest size %d", len(b), ref.Size), nil)
		}
		out = append(out, b...)
	}
	if int64(len(out)) != m.Size {
		return nil, true, fail(KindCorruptManifest, "assemble", "",
			guardf("assembled size %d != manifest size %d", len(out), m.Size), nil)
	}
	if protected != nil {
		if perr := protected(); perr != nil {
			return nil, true, perr
		}
	}
	return out, true, nil
}

// touch 更新 LRU 访问时间（尽力而为，失败不影响正确性）。
func (s *Store) touch(cid Addr) {
	s.beginRead()
	s.commitMu.Lock()
	s.touchLocked(cid)
	s.commitMu.Unlock()
	s.endRead()
}

// OpenChunk 以原始字节方式打开一个已校验的分块，供分发服务端直接读取。
// 不存在返回 ErrMissingChunk，损坏返回 ErrCorruptChunk。
func (s *Store) OpenChunk(addr Addr) ([]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if !addr.Valid() {
		return nil, fail(KindInvalid, "open-chunk", string(addr), "bad address", nil)
	}
	s.beginRead()
	defer s.endRead()
	return s.loadVerified(bChunk, addr)
}

// OpenManifest 以原始字节方式打开一个已校验的清单，供分发服务端使用。
func (s *Store) OpenManifest(addr Addr) ([]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if !addr.Valid() {
		return nil, fail(KindInvalid, "open-manifest", string(addr), "bad address", nil)
	}
	s.beginRead()
	defer s.endRead()
	data, err := s.loadVerified(bManifest, addr)
	if err != nil {
		return nil, err
	}
	// 服务端也只分发结构合法的清单。
	if _, derr := DecodeManifest(data, addr); derr != nil {
		return nil, derr
	}
	return data, nil
}

// Has 报告某内容（清单）当前是否完整保存在本地（存在即认为可直接服务）。
func (s *Store) Has(cid Addr) bool {
	s.beginRead()
	defer s.endRead()
	return s.hasLocked(bManifest, cid)
}

// Info 描述一条本地内容的状态。
type Info struct {
	CID     Addr
	Size    int64 // 内容总字节数（清单声明）
	Chunks  int
	Pinned  bool
	ModTime string // 最近访问时间（RFC3339），供 LRU 可观测
}

// Stat 返回本地一条内容的信息；不存在返回 ErrNotFound，损坏返回 ErrCorruptManifest。
func (s *Store) Stat(cid Addr) (*Info, error) {
	if !cid.Valid() {
		return nil, fail(KindInvalid, "stat", string(cid), "bad address", nil)
	}
	s.beginRead()
	defer s.endRead()
	data, err := s.loadVerified(bManifest, cid)
	if err != nil {
		return nil, err
	}
	m, err := DecodeManifest(data, cid)
	if err != nil {
		return nil, err
	}
	mt := ""
	if fi, statErr := statNoFollow(s.manifestPath(cid)); statErr == nil {
		mt = fi.ModTime().Format(timeLayoutRFC3339)
	}
	return &Info{
		CID: cid, Size: m.Size, Chunks: len(m.Chunks),
		Pinned: s.isPinnedAnyLocked(cid), ModTime: mt,
	}, nil
}

const timeLayoutRFC3339 = "2006-01-02T15:04:05Z07:00"
