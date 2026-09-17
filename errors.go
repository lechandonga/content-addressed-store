package cas

import (
	"errors"
	"fmt"
)

// FailureKind 描述一次操作失败的类别。调用方可以据此区分
// “数据损坏”“内容缺失”“容量不足”等不同失败原因，
// 而不必解析错误字符串。
type FailureKind string

const (
	// KindNotFound 清单/分块地址在本地与所有对端均不存在。
	KindNotFound FailureKind = "not_found"
	// KindMissingChunk 清单存在，但其所引用的某个分块无法获取。
	KindMissingChunk FailureKind = "missing_chunk"
	// KindCorruptChunk 分块字节与其内容地址不一致，或无法从损坏状态恢复。
	KindCorruptChunk FailureKind = "corrupt_chunk"
	// KindCorruptManifest 清单字节与其声明地址不一致、无法解析或内部引用不匹配。
	KindCorruptManifest FailureKind = "corrupt_manifest"
	// KindCapacity 达到容量上限且回收后仍无法腾出足够空间。
	KindCapacity FailureKind = "capacity"
	// KindRefNotFound 引用（pin）不存在。
	KindRefNotFound FailureKind = "ref_not_found"
	// KindRefConflict 引用已经指向另一个不兼容的内容。
	KindRefConflict FailureKind = "ref_conflict"
	// KindInvalid 参数不合法。
	KindInvalid FailureKind = "invalid"
	// KindIO 底层 IO 错误（并非内容层面的损坏）。
	KindIO FailureKind = "io"
)

// 哨兵错误，便于 errors.Is 判断类别。
var (
	ErrNotFound        = &Error{Kind: KindNotFound}
	ErrMissingChunk    = &Error{Kind: KindMissingChunk}
	ErrCorruptChunk    = &Error{Kind: KindCorruptChunk}
	ErrCorruptManifest = &Error{Kind: KindCorruptManifest}
	ErrCapacity        = &Error{Kind: KindCapacity}
	ErrRefNotFound     = &Error{Kind: KindRefNotFound}
	ErrRefConflict     = &Error{Kind: KindRefConflict}
	ErrInvalid         = &Error{Kind: KindInvalid}
	ErrIO              = &Error{Kind: KindIO}
)

// Error 是本包所有失败的统一错误类型。
// 它携带可机读的 Kind、相关地址（可选）以及底层原因。
type Error struct {
	Kind   FailureKind
	Addr   string // 可选：与失败相关的内容/分块地址
	Op     string // 可选：失败发生时的操作名
	Reason error  // 可选：被包装的底层错误
	Msg    string // 可选：补充说明
}

func (e *Error) Error() string {
	s := "cas: " + string(e.Kind)
	if e.Op != "" {
		s += " during " + e.Op
	}
	if e.Addr != "" {
		s += " [" + e.Addr + "]"
	}
	if e.Msg != "" {
		s += ": " + e.Msg
	}
	if e.Reason != nil {
		s += ": " + e.Reason.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.Reason }

// Is 使得任何同 Kind 的 Error 都能匹配对应哨兵，
// 例如 errors.Is(wrapErr, cas.ErrCapacity) 为 true。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return t.Kind == e.Kind
}

// fail 构造一个带细节的分类错误。
func fail(kind FailureKind, op, addr, msg string, reason error) *Error {
	return &Error{Kind: kind, Op: op, Addr: addr, Msg: msg, Reason: reason}
}

// classify 将普通错误归一化为 *Error；已经是 *Error 的原样返回。
func classify(op string, err error) *Error {
	if err == nil {
		return nil
	}
	var ce *Error
	if errors.As(err, &ce) {
		return ce
	}
	return fail(KindIO, op, "", "", err)
}

// guardf 保证 panic 文本不会泄漏到非分类错误中（测试辅助）。
func guardf(format string, args ...any) string { return fmt.Sprintf(format, args...) }
