package cas

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"io"
)

// 磁盘布局版本与子目录名（详见 docs/storage-format.md）。
const (
	formatVersion = 1

	dirChunks     = "chunks"
	dirManifests  = "manifests"
	dirRefs       = "refs"
	dirTmp        = "tmp"
	dirQuarantine = "quarantine"

	formatFile = "format.json"
)

// 默认配置。
const (
	DefaultChunkSize     = 1 << 20  // 1 MiB
	DefaultMaxObjectSize = 64 << 20 // 64 MiB
	// 默认高水位：已用量达到容量的 90% 时触发回收。
	DefaultHighWatermark = 0.90
	// 默认低水位：回收后尽量降到 70% 以下。
	DefaultLowWatermark = 0.70
	// DefaultQuarantineTTL 是损坏文件在隔离区中保留的默认时长，便于事后取证。
	DefaultQuarantineTTL = 24 * time.Hour
	// staleTmpGrace：tmp 中超过该时长的文件视为崩溃残留，可被回收。
	// 取一个远大于正常写入耗时、默认 MaxObjectSize 下也足够宽松的值。
	staleTmpGrace = time.Hour
)

// Config 配置 Store 的容量与行为。零值配合 DefaultConfig 使用。
type Config struct {
	// ChunkSize 为新写入内容的分块大小（字节），必须为正。默认 1 MiB。
	ChunkSize int
	// MaxObjectSize 限制单次 Put/Get 内容的最大字节数，防止内存膨胀。默认 64 MiB。
	MaxObjectSize int64
	// MaxBytes 为本地提交的分块+清单的总字节上限。0 表示不限制。
	MaxBytes int64
	// HighWatermark/LowWatermark 取值 (0,1]，在 MaxBytes>0 时生效。
	HighWatermark float64
	LowWatermark  float64
	// QuarantineTTL 为隔离区文件保留时长；<0 表示永久保留；0 用默认值。
	QuarantineTTL time.Duration
	// Clock 用于测试注入时间；为 nil 时使用系统时钟。
	Clock func() time.Time
}

// DefaultConfig 返回带默认值的配置。
func DefaultConfig() Config {
	return Config{
		ChunkSize:     DefaultChunkSize,
		MaxObjectSize: DefaultMaxObjectSize,
		HighWatermark: DefaultHighWatermark,
		LowWatermark:  DefaultLowWatermark,
		QuarantineTTL: DefaultQuarantineTTL,
		Clock:         time.Now,
	}
}

func (c *Config) withDefaults() error {
	d := DefaultConfig()
	if c.ChunkSize == 0 {
		c.ChunkSize = d.ChunkSize
	}
	if c.ChunkSize <= 0 {
		return fail(KindInvalid, "config", "", "ChunkSize must be positive", nil)
	}
	if c.MaxObjectSize == 0 {
		c.MaxObjectSize = d.MaxObjectSize
	}
	if c.MaxObjectSize < 0 {
		return fail(KindInvalid, "config", "", "MaxObjectSize must be >= 0", nil)
	}
	if c.HighWatermark == 0 {
		c.HighWatermark = d.HighWatermark
	}
	if c.LowWatermark == 0 {
		c.LowWatermark = d.LowWatermark
	}
	if c.MaxBytes > 0 {
		if c.HighWatermark <= 0 || c.HighWatermark > 1 {
			return fail(KindInvalid, "config", "", "HighWatermark must be in (0,1]", nil)
		}
		if c.LowWatermark <= 0 || c.LowWatermark > 1 || c.LowWatermark > c.HighWatermark {
			return fail(KindInvalid, "config", "", "LowWatermark must be in (0,1] and <= HighWatermark", nil)
		}
	}
	if c.QuarantineTTL == 0 {
		c.QuarantineTTL = d.QuarantineTTL
	}
	if c.Clock == nil {
		c.Clock = d.Clock
	}
	return nil
}

// formatJSON 是 format.json 的内容。
type formatJSON struct {
	Version int `json:"version"`
}

// Store 是一个本地内容寻址存储节点。
//
// 并发模型（锁序：gcMu -> commitMu，不可反向）：
//   - gcMu 为 RWMutex：所有读写内容的操作持有读锁；GC 独占写锁，
//     因此回收绝不会与任何读/写中途交错，从根本上避免“读到一半被删”。
//   - commitMu 串行化“去重提交/删除文件/用量记账”等结构性变更，
//     保证并发 Put/获取同一分块只落盘一份、用量不重不漏。
type Store struct {
	root string
	cfg  Config

	gcMu     sync.RWMutex
	commitMu sync.Mutex

	usageCommitted int64

	// getFlight 合并并发的整内容获取（只放一份到网络、只解析一次）。
	getFlight flightGroup
	// blobFlight 合并单个分块/清单的远程获取，避免重复网络副本。
	blobFlight flightGroup

	fetcher Fetcher

	closed  bool
	closeMu sync.Mutex
}

// Open 打开（或初始化）位于 root 的存储。已存在的历史数据无需迁移即可继续使用。
func Open(root string, cfg Config) (*Store, error) {
	if err := cfg.withDefaults(); err != nil {
		return nil, err
	}
	if root == "" {
		return nil, fail(KindInvalid, "open", "", "empty root path", nil)
	}
	s := &Store{root: root, cfg: cfg}
	for _, d := range []string{dirChunks, dirManifests, dirRefs, dirTmp, dirQuarantine} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, fail(KindIO, "open", root, "create layout dir", err)
		}
	}
	fp := filepath.Join(root, formatFile)
	if data, err := os.ReadFile(fp); errors.Is(err, fs.ErrNotExist) {
		data, err := json.Marshal(formatJSON{Version: formatVersion})
		if err != nil {
			return nil, fail(KindIO, "open", root, "marshal format", err)
		}
		if err := writeFileAtomic(root, formatFile, data); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fail(KindIO, "open", root, "read format", err)
	} else {
		var f formatJSON
		if err := json.Unmarshal(data, &f); err != nil || f.Version != formatVersion {
			return nil, fail(KindInvalid, "open", root,
				fmt.Sprintf("incompatible storage format version %q", strings.TrimSpace(string(data))), err)
		}
	}
	if err := s.reconcileUsage(); err != nil {
		return nil, err
	}
	return s, nil
}

// SetFetcher 注入跨节点获取能力（通常是 cashttp.Client）。可在 Open 之后调用。
func (s *Store) SetFetcher(f Fetcher) {
	s.fetcher = f
}

// Root 返回存储根目录（主要供测试与运维检查使用）。
func (s *Store) Root() string { return s.root }

// now 返回当前时间（可被测试注入）。
func (s *Store) now() time.Time { return s.cfg.Clock() }

// ---- 路径与布局 ----

func shardOf(a Addr) string {
	// sha256-<hex>，取摘要前 2 个十六进制字符做分片（256 个分目录）。
	str := string(a)
	return str[len(AddrAlgo)+1 : len(AddrAlgo)+3]
}

func (s *Store) chunkPath(a Addr) string {
	return filepath.Join(s.root, dirChunks, shardOf(a), string(a))
}

func (s *Store) manifestPath(a Addr) string {
	return filepath.Join(s.root, dirManifests, shardOf(a), string(a))
}

func (s *Store) refPath(name string) string {
	return filepath.Join(s.root, dirRefs, urlSafeName(name)+".json")
}

// urlSafeName 防止引用名中的路径分隔符逃逸 refs 目录。
func urlSafeName(name string) string {
	// 采用十六进制编码，彻底消除路径穿越与非法字符。
	return hex.EncodeToString([]byte(name))
}

// ---- 文件级原语 ----

// writeFileAtomic 先写 tmp 再 rename，且只有目标不存在时才提交，
// 从而保证多个并发写同一地址只产生一份文件、绝不互相覆盖。
// 返回 committed==true 表示本次调用实际写入。
//
// 调用方必须持有 commitMu（文件结构与用量变更串行化）。
func (s *Store) commitIfAbsentLocked(rel string, data []byte) (committed bool, err error) {
	dst := filepath.Join(s.root, filepath.FromSlash(rel))
	if _, statErr := os.Stat(dst); statErr == nil {
		return false, nil // 已存在：内容寻址下同名即同内容，直接复用。
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return false, fail(KindIO, "stat", rel, "", statErr)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return false, fail(KindIO, "mkdir", rel, "", err)
	}
	tmp, err := s.newTempFileLocked(data)
	if err != nil {
		return false, err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return false, fail(KindIO, "rename", rel, "", err)
	}
	syncDir(filepath.Dir(dst))
	// 分块与清单都计入容量（内容寻址元数据开销很小且真实占盘）。
	if strings.HasPrefix(filepath.ToSlash(rel), dirChunks+"/") ||
		strings.HasPrefix(filepath.ToSlash(rel), dirManifests+"/") {
		s.usageCommitted += int64(len(data))
	}
	return true, nil
}

// newTempFileLocked 在 tmp 目录写入一个随机命名的临时文件。
func (s *Store) newTempFileLocked(data []byte) (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fail(KindIO, "temp", "", "read random", err)
	}
	name := fmt.Sprintf("w-%d-%s", s.now().UnixNano(), hex.EncodeToString(buf[:]))
	full := filepath.Join(s.root, dirTmp, name)
	if err := os.WriteFile(full, data, 0o644); err != nil {
		return "", fail(KindIO, "temp", dirTmp, "", err)
	}
	return full, nil
}

// writeFileAtomic 用于非内容寻址的小文件（format.json、refs/*.json）：总是覆盖提交。
func writeFileAtomic(root, rel string, data []byte) error {
	dst := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fail(KindIO, "mkdir", rel, "", err)
	}
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fail(KindIO, "temp", rel, "read random", err)
	}
	tmp := filepath.Join(root, dirTmp, fmt.Sprintf("w-%s", hex.EncodeToString(buf[:])))
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fail(KindIO, "temp", rel, "", err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fail(KindIO, "rename", rel, "", err)
	}
	syncDir(filepath.Dir(dst))
	return nil
}

// removeFileLocked 删除 chunks/manifests 下的一个文件并核减用量；
// 文件不存在视为成功。调用方持 commitMu。
func (s *Store) removeFileLocked(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fail(KindIO, "stat", path, "", err)
	}
	if err := os.Remove(path); err != nil {
		return fail(KindIO, "remove", path, "", err)
	}
	if inContentDir(s.root, path) {
		s.usageCommitted -= info.Size()
		if s.usageCommitted < 0 {
			// 与扫描口径校准，防止历史异常造成负值漂移。
			s.usageCommitted = 0
		}
	}
	return nil
}

// inContentDir 判断路径是否位于计入容量的 chunks/manifests 目录。
func inContentDir(root, path string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	for _, d := range []string{dirChunks, dirManifests} {
		prefix, err := filepath.Abs(filepath.Join(root, d))
		if err == nil && strings.HasPrefix(abs, prefix+string(os.PathSeparator)) {
			return true
		}
	}
	return false
}

// quarantine 串行化地把可疑文件移动到隔离区而不是直接删除，
// 损坏文件因此不会再被当作有效内容返回，同时保留事后排查的证据。
// 命名冲突时追加随机后缀。可在持有 gcMu 读锁时调用（锁序 gcMu->commitMu）。
func (s *Store) quarantine(path, kind string) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	return s.quarantineLocked(path, kind)
}

// quarantineLocked 为内部实现：调用方必须持有 commitMu。
func (s *Store) quarantineLocked(path, kind string) error {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fail(KindIO, "quarantine-stat", path, "", err)
	}
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	base := fmt.Sprintf("%s-%d-%s-%s", kind, s.now().UnixNano(),
		hex.EncodeToString(buf[:]), filepath.Base(path))
	dst := filepath.Join(s.root, dirQuarantine, base)
	if err := os.Rename(path, dst); err != nil {
		// 跨设备兜底：复制后删除。
		if copyErr := copyFile(path, dst); copyErr != nil {
			return copyErr
		}
		_ = os.Remove(path)
		dst = ""
	} else {
		syncDir(filepath.Join(s.root, dirQuarantine))
	}
	if info.IsDir() {
		return nil
	}
	// 分块/清单被隔离意味着用量减少（后续访问时按需重新获取）。
	if inContentDir(s.root, path) {
		s.usageCommitted -= info.Size()
		if s.usageCommitted < 0 {
			s.usageCommitted = 0
		}
	}
	if dst != "" {
		syncDir(filepath.Dir(path))
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fail(KindIO, "copy-open", src, "", err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return fail(KindIO, "copy-create", dst, "", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return fail(KindIO, "copy", src, "", err)
	}
	if err := out.Close(); err != nil {
		return fail(KindIO, "copy-close", dst, "", err)
	}
	return nil
}

// syncDir 对目录做一次 fsync，保证 rename/删除在崩溃后仍可见。
// 在不支持目录 fsync 的平台上静默忽略。
func syncDir(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = f.Close()
}

// reconcileUsage 通过扫描 chunks 目录重建已用量，并顺手记录启动时的状态。
func (s *Store) reconcileUsage() error {
	var total int64
	for _, d := range []string{dirChunks, dirManifests} {
		err := filepath.WalkDir(filepath.Join(s.root, d), func(path string, de fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if de.IsDir() {
				return nil
			}
			info, err := de.Info()
			if err != nil {
				return err
			}
			total += info.Size()
			return nil
		})
		if err != nil {
			return fail(KindIO, "reconcile", s.root, "scan "+d, err)
		}
	}
	s.commitMu.Lock()
	s.usageCommitted = total
	s.commitMu.Unlock()
	return nil
}

// beginRead 获取读侧共享锁。
func (s *Store) beginRead() { s.gcMu.RLock() }

func (s *Store) endRead() { s.gcMu.RUnlock() }

// closedErr 报告节点是否已关闭。
func (s *Store) checkClosed() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return fail(KindIO, "store", "", "store is closed", nil)
	}
	return nil
}

// Close 关闭节点。幂等。
func (s *Store) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.closed = true
	return nil
}
