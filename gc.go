package cas

import (
	"context"
	"os"
	"path/filepath"
	"sort"
)

// GCOptions 控制一次回收的行为。
type GCOptions struct {
	// Full 为 true 时回收所有无命名引用的内容（严格模式 / 配置了
	// EvictUnreferenced 时的默认行为）；为 false 时只做必要工作：
	// 清理损坏与不可达垃圾，并在超过容量高水位时按 LRU 淘汰无引用内容。
	Full bool
}

// GCReport 汇报一次回收的结果。
type GCReport struct {
	EvictedContents  int   // 被 LRU 淘汰的无引用内容数
	RemovedManifests int   // 删除的清单数（含淘汰、损坏、不可达）
	RemovedChunks    int   // 删除的分块数（不可达/孤立）
	FreedBytes       int64 // 释放字节数
	CorruptManifests int   // 发现并隔离的损坏/被篡改清单数
	BytesAfter       int64 // 回收后占用字节
	SkippedActive    int   // 因并发访问而跳过、留待下轮的条目数
}

// GC 执行一次回收。全库同一时刻只运行一个 GC；并发调用会等待当前 GC 完成后
// 直接返回其报告（不会叠加执行，避免与写入相互踩踏）。
//
// 回收安全保证：
//   - 被命名引用（Pin）的内容绝不回收；
//   - 标记阶段处于活跃（读/写/拉取）的条目跳过，绝不删除正在使用的数据；
//   - 每个条目的实际删除在 guardian 独占下进行，与访问者互斥；
//   - 被删内容上的新访问会在 guardian 处等待后得到 doomed 信号并重新获取，
//     不会读到半截数据；
//   - 孤立分块、损坏清单、暂存残留与非预期文件一律清理，不残留不可回收垃圾。
func (s *Store) GC(ctx context.Context, opt GCOptions) (GCReport, error) {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	return s.gcLocked(ctx, opt)
}

// strayItem 是需要清理但又要走 guardian 活跃保护的“合法命名、未入索引”条目
// （典型：打开时校验失败的损坏清单；或异常状态下索引缺失的数据文件）。
type strayItem struct {
	key  string // guardian 键（内容/分块 ID）
	path string // 磁盘路径
	size int64
	kind uint8 // 1=清单 2=分块
}

func (s *Store) gcLocked(ctx context.Context, opt GCOptions) (GCReport, error) {
	var rep GCReport

	// 1) 快照索引。
	s.mu.RLock()
	chunks := make(map[ChunkID]*chunkRec, len(s.chunks))
	for id, rec := range s.chunks {
		chunks[id] = rec
	}
	manifests := make(map[ContentID]*manifestRec, len(s.manifests))
	for id, rec := range s.manifests {
		manifests[id] = rec
	}
	pinned := s.pinnedSetLocked()
	bytesNow := s.bytes
	maxBytes := s.cfg.MaxBytes
	waterBytes := s.watermarkBytesLocked()
	full := opt.Full || s.cfg.EvictUnreferenced
	lruOrder := append([]string(nil), s.lru.ordered()...) // 从旧到新
	s.mu.RUnlock()

	// 2) 枚举损坏/非预期磁盘条目。
	corruptM, strayValid, junk := s.scanStray(chunks, manifests)
	rep.CorruptManifests = len(corruptM)

	// 3) 确定“候选淘汰清单”（无引用），容量驱动时按 LRU 从旧到新选取。
	var evictManifests []ContentID
	switch {
	case full:
		for id := range manifests {
			if _, isPinned := pinned[id]; !isPinned {
				evictManifests = append(evictManifests, id)
			}
		}
		sortContentIDs(evictManifests)
	default:
		if maxBytes > 0 && bytesNow > waterBytes {
			projected := bytesNow
			for _, idStr := range lruOrder {
				if projected <= waterBytes {
					break
				}
				cid := ContentID(idStr)
				m, ok := manifests[cid]
				if !ok {
					continue
				}
				if _, isPinned := pinned[cid]; isPinned {
					continue
				}
				evictManifests = append(evictManifests, cid)
				projected -= m.rawLen
			}
		}
	}
	evictCand := make(map[ContentID]struct{}, len(evictManifests))
	for _, id := range evictManifests {
		evictCand[id] = struct{}{}
	}

	// 4) 第一阶段标记：仅标记清单（损坏清单 + 候选淘汰清单）。
	//    标记时处于活跃（读/写/建立引用中）的清单会被跳过——这一点必须先于
	//    分块存活分析，否则“跳过保留的清单”的分块可能被误判为不可达。
	deadManifestKeys := make([]string, 0, len(corruptM)+len(evictManifests))
	for _, it := range corruptM {
		deadManifestKeys = append(deadManifestKeys, it.key)
	}
	for _, id := range evictManifests {
		deadManifestKeys = append(deadManifestKeys, string(id))
	}
	markedM := s.guard.MarkCandidates(deadManifestKeys)
	markedMSet := make(map[string]struct{}, len(markedM))
	for _, k := range markedM {
		markedMSet[k] = struct{}{}
	}
	defer s.guard.Commit(deadManifestKeys)

	// 5) 基于“本轮真正会删除的清单”计算分块存活集：
	//    被跳过（活跃）的候选清单仍视为存活，其分块一律保留。
	actuallyEvicted := make(map[ContentID]struct{}, len(markedM))
	for _, k := range markedM {
		if _, isCandidate := evictCand[ContentID(k)]; isCandidate {
			actuallyEvicted[ContentID(k)] = struct{}{}
		}
	}
	keptChunks := make(map[ChunkID]struct{})
	for cid, mrec := range manifests {
		if _, gone := actuallyEvicted[cid]; gone {
			continue
		}
		for _, c := range mrec.chunks {
			keptChunks[c.ID] = struct{}{}
		}
	}
	var deadChunks []ChunkID
	for id := range chunks {
		if _, live := keptChunks[id]; !live {
			deadChunks = append(deadChunks, id)
		}
	}
	sortChunkIDs(deadChunks)

	// 6) 第二阶段标记：分块（含合法命名但未入索引的异常残留）。
	deadChunkKeys := make([]string, 0, len(deadChunks)+len(strayValid))
	for _, id := range deadChunks {
		deadChunkKeys = append(deadChunkKeys, string(id))
	}
	for _, it := range strayValid {
		deadChunkKeys = append(deadChunkKeys, it.key)
	}
	markedC := s.guard.MarkCandidates(deadChunkKeys)
	rep.SkippedActive = len(deadManifestKeys) + len(deadChunkKeys) - len(markedM) - len(markedC)
	markedCSet := make(map[string]struct{}, len(markedC))
	for _, k := range markedC {
		markedCSet[k] = struct{}{}
	}
	defer s.guard.Commit(deadChunkKeys)

	// 7) 删除清单（先删绑定，后删分块）。
	corruptByKey := make(map[string]strayItem, len(corruptM))
	for _, it := range corruptM {
		corruptByKey[it.key] = it
	}
	var removedM []ContentID
	for _, key := range markedM {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		cid := ContentID(key)
		s.guard.GCLock(key)
		ok := s.removeManifestFile(cid)
		s.guard.GCUnlock(key)
		if ok {
			removedM = append(removedM, cid)
			if rec := manifests[cid]; rec != nil {
				rep.FreedBytes += rec.rawLen
			} else if it, isCorrupt := corruptByKey[key]; isCorrupt {
				rep.FreedBytes += it.size
			}
		}
	}
	rep.RemovedManifests = len(removedM)
	rep.EvictedContents = countContent(removedM, evictCand)

	// 8) 删除分块。
	strayByKey := make(map[string]strayItem, len(strayValid))
	for _, it := range strayValid {
		strayByKey[it.key] = it
	}
	var removedC []ChunkID
	for _, key := range markedC {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		id := ChunkID(key)
		s.guard.GCLock(key)
		ok := s.removeChunkFile(id, false)
		s.guard.GCUnlock(key)
		if ok {
			removedC = append(removedC, id)
			if rec := chunks[id]; rec != nil {
				rep.FreedBytes += rec.size
			} else if it, isStray := strayByKey[key]; isStray {
				rep.FreedBytes += it.size
			}
		}
	}
	rep.RemovedChunks = len(removedC)

	// 10) 非法命名垃圾直接删（不可能与任何正常读写竞争）。
	for _, p := range junk {
		if info, err := os.Stat(p); err == nil {
			if err := os.Remove(p); err == nil {
				rep.FreedBytes += info.Size()
			}
		}
	}

	// 11) 持久化 LRU 并汇报。
	s.mu.Lock()
	rep.BytesAfter = s.bytes
	perr := s.persistLRULocked()
	s.mu.Unlock()
	return rep, perr
}

// scanStray 枚举三类磁盘条目：
//   - corruptM：文件名是合法 ContentID 但字节损坏/被篡改（或未被索引接受）；
//   - strayValid：文件名是合法 ChunkID 但不在索引（异常状态残留；仍走活跃保护）；
//   - junk：文件名非法、不可能属于任何读写，直接清理。
func (s *Store) scanStray(knownChunks map[ChunkID]*chunkRec,
	knownManifests map[ContentID]*manifestRec) (corruptM, strayValid []strayItem, junk []string) {

	mRoot := filepath.Join(s.dir, manifestsDir)
	_ = walkDataFiles(mRoot, func(path string, d os.DirEntry) {
		name := d.Name()
		if !IsValidContentID(name) {
			junk = append(junk, path)
			return
		}
		if _, known := knownManifests[ContentID(name)]; !known {
			var size int64
			if info, err := d.Info(); err == nil {
				size = info.Size()
			}
			corruptM = append(corruptM, strayItem{key: name, path: path, size: size, kind: 1})
		}
	})

	cRoot := filepath.Join(s.dir, chunksDir)
	_ = walkDataFiles(cRoot, func(path string, d os.DirEntry) {
		name := d.Name()
		if !IsValidChunkID(name) {
			junk = append(junk, path)
			return
		}
		if _, known := knownChunks[ChunkID(name)]; !known {
			var size int64
			if info, err := d.Info(); err == nil {
				size = info.Size()
			}
			strayValid = append(strayValid, strayItem{key: name, path: path, size: size, kind: 2})
		}
	})
	return corruptM, strayValid, junk
}

// walkDataFiles 只遍历“直接数据文件”，并跳过 .inflight 暂存子目录
// （并发提交中的临时文件绝不能被当作垃圾清理），也不进入任何子目录。
func walkDataFiles(root string, fn func(path string, d os.DirEntry)) error {
	shards, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, sh := range shards {
		if !sh.IsDir() {
			continue // 顶层杂项文件不处理（正常不会出现）
		}
		entries, err := os.ReadDir(filepath.Join(root, sh.Name()))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue // 含 .inflight：一律跳过，不进入也不删除
			}
			fn(filepath.Join(root, sh.Name(), e.Name()), e)
		}
	}
	return nil
}

func sortChunkIDs(ids []ChunkID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
}

func sortContentIDs(ids []ContentID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
}

func countContent(removed []ContentID, set map[ContentID]struct{}) int {
	n := 0
	for _, id := range removed {
		if _, ok := set[id]; ok {
			n++
		}
	}
	return n
}
