package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
)

// DefaultChunkSize 是新写入内容使用的默认分块大小（256 KiB）。
const DefaultChunkSize = 256 * 1024

// min/max 分块大小边界，防止误配置导致大量极小文件或单文件过大。
const (
	minChunkSize = 16
	maxChunkSize = 64 * 1024 * 1024
)

func normalizeChunkSize(n int) int {
	switch {
	case n == 0:
		return DefaultChunkSize
	case n < minChunkSize:
		return minChunkSize
	case n > maxChunkSize:
		return maxChunkSize
	default:
		return n
	}
}

// chunkIDFromBytes 计算字节的分块标识。
func chunkIDFromBytes(p []byte) ChunkID {
	sum := sha256.Sum256(p)
	return ChunkID(chunkIDPrefix + hex.EncodeToString(sum[:]))
}

func newSHA256() hash.Hash { return sha256.New() }
