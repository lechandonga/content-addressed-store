package cas

import (
	"context"
	"errors"
	"io/fs"
	"os"
)

// blobKind 区分清单与分块，用于错误归类、路径与隔离标签。
type blobKind int

const (
	bManifest blobKind = iota
	bChunk
)

func (k blobKind) name() string {
	if k == bManifest {
		return "manifest"
	}
	return "chunk"
}

func (k blobKind) path(s *Store, a Addr) string {
	if k == bManifest {
		return s.manifestPath(a)
	}
	return s.chunkPath(a)
}

func (k blobKind) missingErr(a Addr) *Error {
	if k == bManifest {
		return fail(KindNotFound, "load-"+k.name(), string(a), "not found locally", nil)
	}
	return fail(KindMissingChunk, "load-"+k.name(), string(a), "missing chunk", nil)
}

func (k blobKind) corruptErr(a Addr, reason error) *Error {
	if k == bManifest {
		return fail(KindCorruptManifest, "load-"+k.name(), string(a),
			"manifest hash mismatch", reason)
	}
	return fail(KindCorruptChunk, "load-"+k.name(), string(a),
		"chunk hash mismatch", reason)
}

func (s *Store) fetch(ctx context.Context, k blobKind, a Addr) ([]byte, error) {
	if s.fetcher == nil {
		return nil, fail(k.missingKind(), "fetch", string(a),
			"no peers configured (fetcher is nil)", nil)
	}
	if k == bManifest {
		return s.fetcher.FetchManifest(ctx, a)
	}
	return s.fetcher.FetchChunk(ctx, a)
}

func (k blobKind) missingKind() FailureKind {
	if k == bManifest {
		return KindNotFound
	}
	return KindMissingChunk
}

// loadVerified 读取并校验一个 blob（清单或分块）。
//
// 保证：返回给调用方的字节一定与期望地址一致；
// 任何损坏（哈希不符）的本地文件都会被移入隔离区并返回 corrupt 错误，
// 缺失文件返回对应 missing 错误。调用方持有 gcMu 读锁期间，文件不会被 GC 删除。
func (s *Store) loadVerified(k blobKind, a Addr) ([]byte, error) {
	path := k.path(s, a)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, k.missingErr(a)
		}
		return nil, fail(KindIO, "read-"+k.name(), string(a), "", err)
	}
	if AddressBytes(data) != a {
		// 本地文件被篡改或静默损坏：立即隔离，绝不返回其字节。
		if qerr := s.quarantine(path, "corrupt-"+k.name()); qerr != nil {
			return nil, qerr
		}
		return nil, k.corruptErr(a, nil)
	}
	return data, nil
}

// hasLocked 判断 blob 是否已在本地落盘（仅判断存在性，GC 判定用）。
func (s *Store) hasLocked(k blobKind, a Addr) bool {
	if _, err := os.Stat(k.path(s, a)); err == nil {
		return true
	}
	return false
}

// fetchVerified 返回一个已通过内容哈希校验的 blob 字节，但不负责落盘。
//
// 它先查本地（可能此前已被并发获取并提交）；本地缺失/损坏时通过 singleflight
// 合并所有并发远程获取：同一 blob 只发一次网络请求，天然不会产生重复副本。
// 网络 IO 不持有 gcMu，因此慢对端不会阻塞回收。
func (s *Store) fetchVerified(ctx context.Context, k blobKind, a Addr) ([]byte, error) {
	if s.fetcher == nil {
		// 无对端：直接返回本地观测到的可区分原因（损坏/缺失），定论。
		s.beginRead()
		_, lerr := s.loadVerified(k, a)
		s.endRead()
		if lerr == nil {
			// 理论不会进入这里（本地有数据就不会被调用）；防御处理。
			return nil, nil
		}
		return nil, lerr
	}

	v, _, ferr := s.blobFlight.do(k.name()+":"+string(a), func() (any, error) {
		// 拿到 singleflight 后先复查本地：前一个持有者可能已完成提交。
		s.beginRead()
		data, lerr := s.loadVerified(k, a)
		s.endRead()
		if lerr == nil {
			return data, nil
		}
		if !isMissingOrCorrupt(lerr) {
			return nil, lerr
		}
		corruptSeen := isCorruptErr(lerr)

		raw, fetchErr := s.fetch(ctx, k, a)
		if fetchErr != nil {
			var ce *Error
			if errors.As(fetchErr, &ce) && (ce.Kind == KindNotFound || ce.Kind == KindMissingChunk) {
				// 对端也没有：若本地处于损坏状态，保持“损坏”这一可区分原因。
				if corruptSeen {
					return nil, k.corruptErr(a, nil)
				}
				return nil, k.missingErr(a)
			}
			return nil, classify("fetch-"+k.name(), fetchErr)
		}
		if AddressBytes(raw) != a {
			// 对端返回被篡改/错误内容：拒绝并归类为损坏，调用方可尝试其他对端。
			return nil, k.corruptErr(a, nil)
		}
		return raw, nil
	})
	if ferr != nil {
		return nil, ferr
	}
	return v.([]byte), nil
}

func isMissingOrCorrupt(err error) bool {
	var ce *Error
	if !errors.As(err, &ce) {
		return false
	}
	switch ce.Kind {
	case KindNotFound, KindMissingChunk, KindCorruptChunk, KindCorruptManifest:
		return true
	}
	return false
}

func isCorruptErr(err error) bool {
	var ce *Error
	if !errors.As(err, &ce) {
		return false
	}
	return ce.Kind == KindCorruptChunk || ce.Kind == KindCorruptManifest
}
