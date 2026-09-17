package cas

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Stats 是节点当前可观测状态的快照。
type Stats struct {
	UsageBytes      int64 // 已提交分块总字节（与磁盘扫描校准）
	MaxBytes        int64 // 容量上限（0 表示不限）
	ManifestCount   int   // 现存合法内容数
	PinnedCount     int   // 引用数
	QuarantineFiles int   // 隔离区文件数
	QuarantineSize  int64 // 隔离区总字节
}

// Stats 返回当前状态快照。
func (s *Store) Stats() (Stats, error) {
	if err := s.checkClosed(); err != nil {
		return Stats{}, err
	}
	s.beginRead()
	defer s.endRead()

	var st Stats
	s.commitMu.Lock()
	st.UsageBytes = s.usageCommitted
	s.commitMu.Unlock()
	st.MaxBytes = s.cfg.MaxBytes

	count := 0
	_ = walkFilesNamed(filepath.Join(s.root, dirManifests), func(_ string) { count++ })
	st.ManifestCount = count

	if pins, err := s.Pins(); err == nil {
		st.PinnedCount = len(pins)
	}

	_ = filepath.WalkDir(filepath.Join(s.root, dirQuarantine),
		func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			info, ierr := d.Info()
			if ierr != nil {
				return nil
			}
			st.QuarantineFiles++
			st.QuarantineSize += info.Size()
			return nil
		})
	return st, nil
}

// walkFilesNamed 遍历目录下所有“文件名是合法地址”的文件。
func walkFilesNamed(dir string, fn func(name string)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, shard := range entries {
		if !shard.IsDir() {
			continue
		}
		sub, err := os.ReadDir(filepath.Join(dir, shard.Name()))
		if err != nil {
			continue
		}
		for _, e := range sub {
			if e.IsDir() {
				continue
			}
			if _, perr := ParseAddr(e.Name()); perr == nil {
				fn(e.Name())
			}
		}
	}
	return nil
}
