package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// Manifest 是内容的整体绑定结构：它按顺序记录每个分块的标识与大小，
// 以及内容总长度。Manifest 本身的字节摘要即为内容标识（ContentID）。
//
// 因此验证链条为：
//
//	ContentID  == SHA256(规范化的 Manifest 编码)   —— 证明清单未被篡改
//	每个 ChunkID == SHA256(分块字节)              —— 证明分块未被篡改
//	Manifest 中 ChunkID 顺序 + Length              —— 证明重组内容完整且顺序正确
type Manifest struct {
	Version   int        `json:"version"`
	Hash      string     `json:"hash"`
	ChunkSize int        `json:"chunkSize"`
	Length    int64      `json:"length"`
	Chunks    []ChunkRef `json:"chunks"`
}

// ChunkRef 描述内容中的一个分块。
type ChunkRef struct {
	ID   ChunkID `json:"id"`
	Size int64   `json:"size"`
}

// ContentID 是整体内容标识（cas1-...）。
type ContentID string

// ChunkID 是分块标识（cas1c-...）。
type ChunkID string

func (c ContentID) String() string { return string(c) }
func (c ChunkID) String() string   { return string(c) }

// manifestEnvelope 是落盘/网络传输时的外层包装，带魔数以便识别格式版本。
type manifestEnvelope struct {
	Format   string   `json:"format"`
	Manifest Manifest `json:"manifest"`
}

const manifestFormatV1 = "cas-manifest-v1"

// encodeManifest 返回清单的规范化编码字节。使用紧凑 JSON（无多余空白），
// 字段顺序由 struct 定义固定；同一清单在任何节点上编码结果字节一致，
// 从而保证 ContentID 跨节点一致、可验证。
func encodeManifest(m *Manifest) ([]byte, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	env := manifestEnvelope{Format: manifestFormatV1, Manifest: *m}
	raw, err = json.Marshal(&env)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// decodeManifest 解析清单字节，但不做摘要校验（调用方负责结合期望的 ContentID 校验）。
func decodeManifest(raw []byte) (*Manifest, error) {
	var env manifestEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	if env.Format != manifestFormatV1 {
		return nil, fmt.Errorf("%w: unknown format %q", ErrInvalidManifest, env.Format)
	}
	return &env.Manifest, nil
}

// computeContentID 对规范化清单编码取 SHA-256，生成内容标识。
func computeContentID(raw []byte) ContentID {
	sum := sha256.Sum256(raw)
	return ContentID(contentIDPrefix + hex.EncodeToString(sum[:]))
}

// validateManifest 校验清单结构自洽：版本、哈希、长度、分块边界与 ID 合法。
// 不检查分块是否实际存在（那是存储层的职责）。
func validateManifest(m *Manifest) error {
	if m == nil {
		return fmt.Errorf("%w: nil", ErrInvalidManifest)
	}
	if m.Version != manifestVersionV1 {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidManifest, m.Version)
	}
	if m.Hash != hashAlgoSHA256 {
		return fmt.Errorf("%w: unsupported hash %q", ErrInvalidManifest, m.Hash)
	}
	if m.ChunkSize <= 0 {
		return fmt.Errorf("%w: non-positive chunk size", ErrInvalidManifest)
	}
	if m.Length < 0 {
		return fmt.Errorf("%w: negative length", ErrInvalidManifest)
	}
	if len(m.Chunks) == 0 {
		// 空内容允许：长度必须为 0。
		if m.Length != 0 {
			return fmt.Errorf("%w: no chunks but length=%d", ErrInvalidManifest, m.Length)
		}
		return nil
	}
	var sum int64
	for i, c := range m.Chunks {
		if !IsValidChunkID(string(c.ID)) {
			return fmt.Errorf("%w: chunk %d has bad id %q", ErrInvalidManifest, i, c.ID)
		}
		if c.Size <= 0 {
			return fmt.Errorf("%w: chunk %d non-positive size", ErrInvalidManifest, i)
		}
		// 除最后一个分块外，其余分块必须恰好等于 ChunkSize。
		if i < len(m.Chunks)-1 && c.Size != int64(m.ChunkSize) {
			return fmt.Errorf("%w: chunk %d size %d != chunk size %d",
				ErrInvalidManifest, i, c.Size, m.ChunkSize)
		}
		if c.Size > int64(m.ChunkSize) {
			return fmt.Errorf("%w: chunk %d size %d > chunk size", ErrInvalidManifest, i, c.Size)
		}
		sum += c.Size
	}
	if sum != m.Length {
		return fmt.Errorf("%w: chunk sizes sum %d != length %d", ErrInvalidManifest, sum, m.Length)
	}
	return nil
}

// verifyManifestBytes 验证清单字节与期望内容标识一致，并返回解析后的 Manifest。
// wantID 为空字符串时只做结构校验。
func verifyManifestBytes(raw []byte, wantID ContentID) (*Manifest, error) {
	m, err := decodeManifest(raw)
	if err != nil {
		if errors.Is(err, ErrInvalidManifest) {
			return nil, err
		}
		return nil, opError(KindInvalidManifest, "manifest.decode", "", err)
	}
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	if wantID != "" {
		gotID := computeContentID(raw)
		if gotID != wantID {
			return nil, opError(KindCorruptManifest, "manifest.verify", string(wantID),
				fmt.Errorf("computed %s", gotID))
		}
	}
	return m, nil
}

// verifyChunk 校验分块字节与标识一致。
func verifyChunk(id ChunkID, data []byte) error {
	if !IsValidChunkID(string(id)) {
		return opError(KindInvalidArgument, "chunk.verify", string(id), errBadID)
	}
	sum := sha256.Sum256(data)
	got := chunkIDPrefix + hex.EncodeToString(sum[:])
	if got != string(id) {
		return opError(KindCorruptChunk, "chunk.verify", string(id),
			fmt.Errorf("computed %s", got))
	}
	return nil
}

// emptyContentID 是空内容的标识（空内容也有 0 个分块的清单）。
var emptyManifestRaw []byte
var emptyContentID ContentID

func init() {
	m := &Manifest{
		Version:   manifestVersionV1,
		Hash:      hashAlgoSHA256,
		ChunkSize: DefaultChunkSize,
		Length:    0,
		Chunks:    nil,
	}
	raw, err := encodeManifest(m)
	if err != nil {
		panic(err)
	}
	emptyManifestRaw = raw
	emptyContentID = computeContentID(raw)
}
