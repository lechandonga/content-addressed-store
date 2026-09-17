package cas

import (
	"os"
	"time"
)

// statNoFollow 对路径做 lstat，避免意外跟随符号链接。
func statNoFollow(path string) (os.FileInfo, error) {
	return os.Lstat(path)
}

// isPinnedAnyLocked 在 refs 目录中查找是否存在指向给定内容的引用。
// 仅用于可观测信息；调用方持有 gcMu 读锁即可。
func (s *Store) isPinnedAnyLocked(cid Addr) bool {
	var found bool
	_ = walkRefs(s.root, func(_ string, rec pinRecord) bool {
		if Addr(rec.Addr) == cid {
			found = true
			return false
		}
		return true
	})
	return found
}

var _ = time.Now
