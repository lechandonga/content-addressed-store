package cas

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// GCConfig 控制一次回收的触发条件。
type GCConfig struct {
	// MakeRoom>0 时，回收目标是在遵守 MaxBytes 的前提下，至少腾出该数字节
	// （用于 Put 前的容量预留）；为 0 时仅在用量超过高水位时才驱逐无引用内容。
	MakeRoom int64
	// EvictAll 强制驱逐所有“无引用”内容（不考虑水位）。被引用内容仍受保护。
	// 主要用于测试与运维“彻底回收缓存”。
	EvictAll bool
	// MaxObjects 限制本次最多扫描/处理的清单数量，0 表示不限制（保留参数）。
	MaxObjects int
}

// GCReport 汇报一次回收的结果，便于观测与测试断言。
type GCReport struct {
	EvictedManifests  int   // 被驱逐的无引用清单数
	RemovedChunks     int   // 因引用计数归零而删除的分块数
	OrphanChunks      int   // 不属于任何现存清单的游离分块数
	StageTmpFiles     int   // 清理的崩溃残留临时文件数
	Quarantined       int   // 隔离的损坏/未知文件数
	ExpiredQuarantine int   // 过期后删除的隔离文件数
	ReclaimedBytes    int64 // 实际释放的字节数
	UsageBefore       int64
	UsageAfter        int64
}

// Collect 执行一次安全回收。
//
// 安全保证：
//   - 持有 gcMu 写锁（排斥所有读/写），因此回收绝不会与任何并发读或新增引用交错，
//     不会误删正在读取的内容，也不会遗留“引用了已删分块”的半状态；
//   - 标记阶段以 refs 目录中的引用为根：被引用的清单及其（传递）分块一律保留；
//   - 无引用内容仅在容量超过高水位（或 MakeRoom/EvictAll 要求）时，
//     按 LRU（最近访问时间）驱逐；
//   - 分块采用引用计数：只有当引用它的所有清单都被移除后才删除，
//     因此被多个内容共享的去重分块不会被误回收；
//   - 损坏的清单/分块被隔离而非删除；崩溃残留 tmp 与过期隔离件被清理；
//   - 回收后再次 Get 会自动重新获取并校验，绝不返回损坏残留。
func (s *Store) Collect(ctx context.Context, cfg GCConfig) (*GCReport, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	// GC 独占：与所有读/写互斥，从锁结构上消除竞争窗口。
	s.gcMu.Lock()
	s.commitMu.Lock()
	defer func() {
		s.commitMu.Unlock()
		s.gcMu.Unlock()
	}()

	rep := &GCReport{UsageBefore: s.usageCommitted}

	// ---------- 标记（mark） ----------
	pinnedAddrs := make(map[Addr]string) // cid -> 引用名（取其一）
	if err := walkRefs(s.root, func(name string, rec pinRecord) bool {
		if a, perr := ParseAddr(rec.Addr); perr == nil {
			pinnedAddrs[a] = name
		}
		return true
	}); err != nil {
		return nil, err
	}

	type manEntry struct {
		addr Addr
		m    *Manifest
		mt   time.Time
		size int64 // 清单独占字节数
	}
	all := make([]manEntry, 0)
	manChunks := make(map[Addr][]ChunkRef)

	manDir := filepath.Join(s.root, dirManifests)
	manErr := filepath.WalkDir(manDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a, perr := ParseAddr(d.Name())
		if perr != nil {
			// 文件名就不合法：隔离未知文件。
			if qerr := s.quarantineLocked(path, "unknown-manifest"); qerr == nil {
				rep.Quarantined++
			}
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		m, derr := DecodeManifest(raw, a)
		if derr != nil {
			// 清单损坏（哈希不符或结构非法）：隔离，待下次访问时重新获取。
			if qerr := s.quarantineLocked(path, "corrupt-manifest"); qerr == nil {
				rep.Quarantined++
			}
			return nil
		}
		all = append(all, manEntry{addr: a, m: m, mt: info.ModTime(), size: info.Size()})
		manChunks[a] = m.Chunks
		return nil
	})
	if manErr != nil && !errors.Is(manErr, context.Canceled) {
		return nil, fail(KindIO, "gc-scan-manifests", "", "", manErr)
	}
	if manErr != nil {
		return rep, fail(KindIO, "gc", "", "cancelled", manErr)
	}

	// 分块引用计数（仅统计属于现存、合法清单的分块）。
	chunkRefs := make(map[Addr]int)
	chunkSizes := make(map[Addr]int64)
	for _, e := range all {
		for _, c := range e.m.Chunks {
			chunkRefs[c.Addr]++
			chunkSizes[c.Addr] = c.Size
		}
	}

	// ---------- 选择要驱逐的无引用清单（sweep 计划） ----------
	evict := make(map[Addr]bool)
	pinnedSet := make(map[Addr]bool, len(pinnedAddrs))
	unpinned := make([]manEntry, 0, len(all))
	for _, e := range all {
		if _, pinned := pinnedAddrs[e.addr]; pinned {
			pinnedSet[e.addr] = true
			continue
		}
		unpinned = append(unpinned, e)
	}

	needEviction := cfg.EvictAll
	var targetFree int64
	if s.cfg.MaxBytes > 0 {
		hw := int64(s.cfg.HighWatermark * float64(s.cfg.MaxBytes))
		lw := int64(s.cfg.LowWatermark * float64(s.cfg.MaxBytes))
		if cfg.MakeRoom > 0 {
			// 需要为预留字节腾出空间：目标使 usage-MakeRoom <= 低水位。
			if s.usageCommitted+cfg.MakeRoom > s.cfg.MaxBytes ||
				s.usageCommitted > hw {
				needEviction = true
				targetFree = s.usageCommitted + cfg.MakeRoom - lw
			}
		} else if s.usageCommitted > hw {
			needEviction = true
			targetFree = s.usageCommitted - lw
		}
	} else if cfg.MakeRoom > 0 {
		// 无容量上限时仍尊重 MakeRoom（理论上不会触发，保留语义）。
		needEviction = true
		targetFree = cfg.MakeRoom
	}

	if needEviction {
		// LRU：最久未访问的先驱逐。
		sort.Slice(unpinned, func(i, j int) bool { return unpinned[i].mt.Before(unpinned[j].mt) })
		var freed int64
		for _, e := range unpinned {
			var payload int64
			for _, c := range e.m.Chunks {
				// 只计本清单独占、会被释放的分块大小。
				if chunkRefs[c.Addr] <= 1 {
					payload += c.Size
				}
			}
			evict[e.addr] = true
			freed += payload + e.size
			// EvictAll 不提前停止；否则达到释放目标即停。
			if !cfg.EvictAll && freed >= targetFree {
				break
			}
		}
	}

	// ---------- 执行清单驱逐 ----------
	for a := range evict {
		path := s.manifestPath(a)
		if err := s.removeFileLocked(path); err != nil {
			return rep, err
		}
		rep.EvictedManifests++
		info, _ := os.Stat(path)
		rep.ReclaimedBytes += manifestOwnSize(a, manChunks, chunkRefs)
		if info != nil {
			rep.ReclaimedBytes += info.Size()
		}
		for _, c := range manChunks[a] {
			chunkRefs[c.Addr]--
		}
	}

	// ---------- 清理引用计数归零 / 游离的分块 ----------
	chunkDir := filepath.Join(s.root, dirChunks)
	chunkErr := filepath.WalkDir(chunkDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a, perr := ParseAddr(d.Name())
		if perr != nil {
			if qerr := s.quarantineLocked(path, "unknown-chunk"); qerr == nil {
				rep.Quarantined++
			}
			return nil
		}
		refn := chunkRefs[a]
		info, _ := d.Info()
		// 无论被引用与否，都做完整性校验：任何哈希不符的分块一律隔离，
		// 绝不允许损坏字节继续占用其内容地址、伪装成有效内容。
		data, rerr := os.ReadFile(path)
		if rerr == nil && AddressBytes(data) != a {
			if qerr := s.quarantineLocked(path, "corrupt-chunk"); qerr == nil {
				rep.Quarantined++
			}
			// 引用了该损坏分块的所有清单同样不可信：一并隔离，
			// 下次访问时整体重新获取，而不是留下“清单在但内容坏”的半状态。
			for mAddr, refs := range manChunks {
				for _, c := range refs {
					if c.Addr == a {
						mp := s.manifestPath(mAddr)
						if qerr := s.quarantineLocked(mp, "corrupt-manifest"); qerr == nil {
							rep.Quarantined++
						}
						delete(manChunks, mAddr)
						break
					}
				}
			}
			return nil
		}
		if refn <= 0 {
			// 游离（且哈希正确）的分块属于可安全回收的孤儿/去重残留。
			if err := s.removeFileLocked(path); err != nil {
				return err
			}
			if info != nil {
				rep.ReclaimedBytes += info.Size()
			}
			if _, wasReferenced := chunkSizes[a]; wasReferenced {
				rep.RemovedChunks++
			} else {
				rep.OrphanChunks++
			}
		}
		return nil
	})
	if chunkErr != nil && !errors.Is(chunkErr, context.Canceled) {
		return nil, fail(KindIO, "gc-sweep-chunks", "", "", chunkErr)
	}

	// ---------- 一致性补漏：隔离缺失分块的清单 ----------
	// 分块可能在标记之后、本轮清理之前被外部删除（例如并发的运维操作）。
	// 若仍保留引用了缺失分块的清单，会留下“清单在但永远读不出”的残留；
	// 这里做一次存在性补扫，把这种清单也隔离，使其下次访问时整体重新获取。
	manDir2 := filepath.Join(s.root, dirManifests)
	if err := filepath.WalkDir(manDir2, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		a, perr := ParseAddr(d.Name())
		if perr != nil {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		m, derr := DecodeManifest(raw, a)
		if derr != nil {
			return nil // 已在标记阶段隔离
		}
		for _, c := range m.Chunks {
			if _, serr := os.Stat(s.chunkPath(c.Addr)); errors.Is(serr, fs.ErrNotExist) {
				if qerr := s.quarantineLocked(path, "incomplete-manifest"); qerr == nil {
					rep.Quarantined++
				}
				break
			}
		}
		return nil
	}); err != nil {
		return nil, fail(KindIO, "gc-consistency", "", "", err)
	}

	// ---------- 清理崩溃残留 tmp（超过宽限期） ----------
	now := s.now()
	tmpDir := filepath.Join(s.root, dirTmp)
	if entries, err := os.ReadDir(tmpDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			info, ierr := e.Info()
			if ierr != nil {
				continue
			}
			if now.Sub(info.ModTime()) > staleTmpGrace {
				if rerr := os.Remove(filepath.Join(tmpDir, e.Name())); rerr == nil {
					rep.StageTmpFiles++
				}
			}
		}
	}

	// ---------- 清理过期隔离件 ----------
	if s.cfg.QuarantineTTL > 0 {
		qDir := filepath.Join(s.root, dirQuarantine)
		if entries, err := os.ReadDir(qDir); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				info, ierr := e.Info()
				if ierr != nil {
					continue
				}
				if now.Sub(info.ModTime()) > s.cfg.QuarantineTTL {
					if rerr := os.Remove(filepath.Join(qDir, e.Name())); rerr == nil {
						rep.ExpiredQuarantine++
					}
				}
			}
		}
	}

	// 回收后用量以扫描校准，保证记账与磁盘一致。
	if err := s.rescanUsageLocked(); err != nil {
		return rep, err
	}
	rep.UsageAfter = s.usageCommitted
	return rep, nil
}

// manifestOwnSize 估算被驱逐清单独占（将随之释放）的字节数：
// 清单自身大小 + 引用计数降为 0 的分块大小。此处清单在 evict 后 refs 已减。
func manifestOwnSize(a Addr, manChunks map[Addr][]ChunkRef, chunkRefs map[Addr]int) int64 {
	var n int64
	for _, c := range manChunks[a] {
		// 调用前先计算；evict 循环在删除后才 decrement，故此处读到的是删除前计数。
		if chunkRefs[c.Addr] <= 1 {
			n += c.Size
		}
	}
	return n
}

// rescanUsageLocked 以 chunks/manifests 目录扫描结果校准用量。调用方持 commitMu。
func (s *Store) rescanUsageLocked() error {
	var total int64
	for _, d := range []string{dirChunks, dirManifests} {
		err := filepath.WalkDir(filepath.Join(s.root, d), func(path string, de fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if de.IsDir() {
				return nil
			}
			info, ierr := de.Info()
			if ierr != nil {
				return ierr
			}
			total += info.Size()
			return nil
		})
		if err != nil {
			return fail(KindIO, "gc-rescan", "", "", err)
		}
	}
	s.usageCommitted = total
	return nil
}

// ensure encoding/json retained for potential metadata extension.
var _ = json.Marshal
