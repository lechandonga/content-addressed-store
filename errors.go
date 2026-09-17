package cas

import "strings"

// ErrorKind 是类型化的失败原因，调用方可据此区分“损坏 / 缺失 / 篡改 / 容量受限”等情况。
type ErrorKind int

const (
	KindUnknown          ErrorKind = iota
	KindNotFound                   // 本地与所有已配置对端均无该内容
	KindMissingChunk               // 清单引用的分块在本地与所有对端均不存在
	KindCorruptChunk               // 分块字节摘要与 ChunkID 不一致（损坏或被篡改）
	KindCorruptManifest            // 清单字节摘要与 ContentID 不一致（损坏或被篡改）
	KindInvalidManifest            // 清单结构非法（长度/分块边界/ID 等不自洽）
	KindContentMismatch            // 读取时分块序列与清单整体绑定不一致
	KindRefNotFound                // 命名引用不存在
	KindRefExists                  // 命名引用已存在（Create 模式下冲突）
	KindCapacityExceeded           // 容量上限无法满足（连仅保留被引用内容都放不下）
	KindNetwork                    // 对端交互的网络/协议层失败
	KindInvalidArgument            // 参数非法
)

// Error 是 cas 包返回的统一错误类型，携带失败类别、操作与相关标识，
// 便于调用方精确区分失败原因并据此决策（重试 / 换对端 / 报警 / 扩容）。
type Error struct {
	Kind ErrorKind
	Op   string // 失败发生时的逻辑操作，如 "node.get"、"store.commitChunk"
	ID   string // 相关内容或分块标识（可能为空）
	Err  error  // 被包装的底层错误
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("cas: ")
	if e.Op != "" {
		b.WriteString(e.Op)
		b.WriteString(": ")
	}
	b.WriteString(e.Kind.name())
	if e.ID != "" {
		b.WriteString(" ")
		b.WriteString(e.ID)
	}
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.Err }

// Is 仅按 Kind 判定，因此可以直接用 errors.Is(err, cas.ErrCorruptChunk)
// 而不要求具体错误是同一个指针。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	if t.Kind == KindUnknown {
		return false
	}
	return e.Kind == t.Kind
}

func (k ErrorKind) name() string {
	switch k {
	case KindNotFound:
		return "content not found"
	case KindMissingChunk:
		return "referenced chunk is missing"
	case KindCorruptChunk:
		return "chunk hash mismatch (corrupt or tampered)"
	case KindCorruptManifest:
		return "manifest hash mismatch (corrupt or tampered)"
	case KindInvalidManifest:
		return "invalid manifest"
	case KindContentMismatch:
		return "content does not match content id"
	case KindRefNotFound:
		return "reference not found"
	case KindRefExists:
		return "reference already exists"
	case KindCapacityExceeded:
		return "capacity limit exceeded"
	case KindNetwork:
		return "network error"
	case KindInvalidArgument:
		return "invalid argument"
	default:
		return "unknown error"
	}
}

// 哨兵错误：可直接用于 errors.Is 判定，也可作为包装的内层错误。
var (
	ErrNotFound         = &Error{Kind: KindNotFound}
	ErrMissingChunk     = &Error{Kind: KindMissingChunk}
	ErrCorruptChunk     = &Error{Kind: KindCorruptChunk}
	ErrCorruptManifest  = &Error{Kind: KindCorruptManifest}
	ErrInvalidManifest  = &Error{Kind: KindInvalidManifest}
	ErrContentMismatch  = &Error{Kind: KindContentMismatch}
	ErrRefNotFound      = &Error{Kind: KindRefNotFound}
	ErrRefExists        = &Error{Kind: KindRefExists}
	ErrCapacityExceeded = &Error{Kind: KindCapacityExceeded}
	ErrNetwork          = &Error{Kind: KindNetwork}
	ErrInvalidArgument  = &Error{Kind: KindInvalidArgument}
)

func opError(kind ErrorKind, op, id string, err error) *Error {
	return &Error{Kind: kind, Op: op, ID: id, Err: err}
}
