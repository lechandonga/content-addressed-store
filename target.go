package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
)

// AddrAlgo 是当前支持的内容地址算法标识。
const AddrAlgo = "sha256"

// Addr 是一个内容地址，形如 "sha256-<64 位十六进制摘要>"。
// 它同时也是分块与清单的唯一标识；清单的地址即为整条内容的内容标识（CID）。
type Addr string

// String 返回地址的规范文本。
func (a Addr) String() string { return string(a) }

// Valid 检查地址的格式（算法前缀 + 长度正确的十六进制摘要）。
func (a Addr) Valid() bool {
	s := string(a)
	prefix := AddrAlgo + "-"
	if !strings.HasPrefix(s, prefix) {
		return false
	}
	hexPart := s[len(prefix):]
	if len(hexPart) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(hexPart)
	return err == nil
}

// ParseAddr 解析并校验一个地址字符串。
func ParseAddr(s string) (Addr, error) {
	a := Addr(s)
	if !a.Valid() {
		return "", fail(KindInvalid, "parse-addr", s, "bad content address", nil)
	}
	return a, nil
}

// AddressBytes 计算一段字节的内容地址。
func AddressBytes(data []byte) Addr {
	sum := sha256.Sum256(data)
	return Addr(AddrAlgo + "-" + hex.EncodeToString(sum[:]))
}

// ChunkRef 是清单中对单个分块的引用。
type ChunkRef struct {
	Addr Addr  `json:"addr"` // 分块内容地址
	Size int64 `json:"size"` // 分块原始字节数
}

// Manifest 描述一条完整内容的分块布局。
// 内容标识（CID）= 清单规范序列化字节的 sha256 地址，
// 因此任何对分块列表、大小等信息的篡改都会使 CID 失效。
type Manifest struct {
	// Version 为清单格式版本，当前固定为 1。
	Version int `json:"version"`
	// Size 是完整内容的总字节数（各分块大小之和）。
	Size int64 `json:"size"`
	// Chunks 按内容顺序排列的分块引用。
	Chunks []ChunkRef `json:"chunks"`
}

// ManifestVersion 是当前支持的清单格式版本。
const ManifestVersion = 1

// marshalManifest 返回清单的规范序列化字节。
// Go 的 encoding/json 对结构体按字段声明顺序、确定性地输出，
// 因此相同逻辑清单始终得到相同字节，CID 计算稳定。
func marshalManifest(m *Manifest) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		// 当前 Manifest 结构不可能产生 JSON 序列化错误。
		panic(fmt.Sprintf("cas: internal: marshal manifest: %v", err))
	}
	return b
}

// validateManifest 校验清单内部一致性，不做任何磁盘/网络假设。
func validateManifest(m *Manifest) error {
	if m == nil {
		return fail(KindCorruptManifest, "validate-manifest", "", "nil manifest", nil)
	}
	if m.Version != ManifestVersion {
		return fail(KindInvalid, "validate-manifest", "",
			fmt.Sprintf("unsupported manifest version %d", m.Version), nil)
	}
	if m.Size < 0 {
		return fail(KindCorruptManifest, "validate-manifest", "", "negative total size", nil)
	}
	if len(m.Chunks) == 0 {
		// 空内容也是合法内容（单分块，长度为 0），因此无分块一定是损坏/伪造。
		return fail(KindCorruptManifest, "validate-manifest", "", "manifest has no chunks", nil)
	}
	var sum int64
	seen := make(map[Addr]struct{}, len(m.Chunks))
	for i, c := range m.Chunks {
		if !c.Addr.Valid() {
			return fail(KindCorruptManifest, "validate-manifest", "",
				fmt.Sprintf("chunk %d has invalid address", i), nil)
		}
		if c.Size < 0 {
			return fail(KindCorruptManifest, "validate-manifest", string(c.Addr),
				fmt.Sprintf("chunk %d has negative size", i), nil)
		}
		// 注意：同一分块地址在清单中可合法出现多次（重复内容会产生相同分块），
		// 引用计数按出现次数累加，因此这里不做“去重/查重”。
		sum += c.Size
		seen[c.Addr] = struct{}{}
	}
	_ = seen
	if sum != m.Size {
		return fail(KindCorruptManifest, "validate-manifest", "",
			fmt.Sprintf("chunk sizes sum %d != total size %d", sum, m.Size), nil)
	}
	return nil
}

// DecodeManifest 解析清单字节、校验格式并验证其内容地址。
// 任何被篡改或截断的清单都会返回 KindCorruptManifest，且不会被当作有效数据使用。
func DecodeManifest(raw []byte, claimed Addr) (*Manifest, error) {
	if claimed.Valid() && AddressBytes(raw) != claimed {
		return nil, fail(KindCorruptManifest, "decode-manifest", string(claimed),
			"manifest bytes do not match content address", nil)
	}
	var m Manifest
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fail(KindCorruptManifest, "decode-manifest", string(claimed),
			"manifest is not valid JSON", err)
	}
	if err := validateManifest(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// BuildChunks 把内容按固定大小切分并逐块计算内容地址。
// 返回每个分块的原始字节（顺序与引用一致）与对应清单。
// chunkSize 必须为正数。
func BuildChunks(content []byte, chunkSize int) ([]ChunkRef, [][]byte, error) {
	if chunkSize <= 0 {
		return nil, nil, fail(KindInvalid, "build-chunks", "", "chunk size must be positive", nil)
	}
	if content == nil {
		content = []byte{}
	}
	refs := make([]ChunkRef, 0, len(content)/chunkSize+1)
	blocks := make([][]byte, 0, cap(refs))
	for off := 0; off < len(content); off += chunkSize {
		end := off + chunkSize
		if end > len(content) {
			end = len(content)
		}
		block := content[off:end]
		refs = append(refs, ChunkRef{Addr: AddressBytes(block), Size: int64(len(block))})
		blocks = append(blocks, block)
	}
	if len(refs) == 0 {
		// 空内容：单个零长度分块。
		zero := []byte{}
		refs = append(refs, ChunkRef{Addr: AddressBytes(zero), Size: 0})
		blocks = append(blocks, zero)
	}
	return refs, blocks, nil
}

// BuildManifest 依据分块引用构造清单并返回清单字节与内容地址。
func BuildManifest(refs []ChunkRef) (*Manifest, []byte, Addr, error) {
	m := &Manifest{Version: ManifestVersion, Chunks: refs}
	var total int64
	for _, c := range refs {
		total += c.Size
	}
	m.Size = total
	if err := validateManifest(m); err != nil {
		return nil, nil, "", err
	}
	raw := marshalManifest(m)
	return m, raw, AddressBytes(raw), nil
}

// ChunkReader 在哈希数据的同时把字节透传出去，用于流式校验。
type ChunkReader struct {
	r    io.Reader
	h    hash.Hash
	addr Addr
	n    int64
}

// NewChunkReader 包装一个分块字节流，读取结束后可通过 Verified 校验地址。
func NewChunkReader(r io.Reader, expected Addr) *ChunkReader {
	return &ChunkReader{r: r, addr: expected, h: sha256.New()}
}

func (c *ChunkReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.h.Write(p[:n])
		c.n += int64(n)
	}
	return n, err
}

// Verified 在流读完后判断哈希是否与期望地址一致。
func (c *ChunkReader) Verified() bool {
	return Addr(AddrAlgo+"-"+hex.EncodeToString(c.h.Sum(nil))) == c.addr
}

// ensure package used errors import retained for interface compatibility helpers.
var _ = errors.Is
