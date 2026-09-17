package cas

import (
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
)

// ID 版本与格式：
//   - 分块标识：cas1c-<64 个十六进制字符>（SHA-256，裸字节）
//   - 内容标识：cas1-<64 个十六进制字符>（SHA-256，规范化的 v1 清单编码字节）
//
// 两类前缀不同，永远不会互相混淆。ID 自描述其哈希算法与版本，
// 未来升级哈希算法时可引入新版本前缀而不破坏历史数据。
const (
	idVersionPrefix   = "cas1"   // 内容标识前缀
	chunkIDPrefix     = "cas1c-" // 分块标识前缀
	contentIDPrefix   = "cas1-"  // 内容标识前缀
	sha256HexLen      = 64
	hashAlgoSHA256    = "sha256"
	manifestVersionV1 = 1
)

var (
	chunkIDPattern   = regexp.MustCompile(`^cas1c-[0-9a-f]{64}$`)
	contentIDPattern = regexp.MustCompile(`^cas1-[0-9a-f]{64}$`)
)

// IsValidChunkID 判断是否为合法分块标识。
func IsValidChunkID(id string) bool { return chunkIDPattern.MatchString(id) }

// IsValidContentID 判断是否为合法内容标识。
func IsValidContentID(id string) bool { return contentIDPattern.MatchString(id) }

// hashPart 返回 ID 中的十六进制摘要部分，非法时返回空串。
func hashPart(id string) string {
	var p string
	switch {
	case IsValidChunkID(id):
		p = id[len(chunkIDPrefix):]
	case IsValidContentID(id):
		p = id[len(contentIDPrefix):]
	default:
		return ""
	}
	if len(p) != sha256HexLen {
		return ""
	}
	if _, err := hex.DecodeString(p); err != nil {
		return ""
	}
	return p
}

// shard 返回标识在磁盘上的两位分片目录名（摘要前两位），用于限制单目录条目数。
func shard(id string) (string, error) {
	h := hashPart(id)
	if h == "" {
		return "", fmt.Errorf("%w: %q", ErrInvalidArgument, id)
	}
	return h[:2], nil
}

var errBadID = errors.New("malformed cas id")

// mustShard 用于调用方已保证 ID 合法的场景。
func mustShard(id string) string {
	s, err := shard(id)
	if err != nil {
		panic(err)
	}
	return s
}
