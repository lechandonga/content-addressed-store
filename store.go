package cas

import (
	"context"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// 磁盘布局目录名（v1）。
const (
	chunksDir    = "chunks"
	manifestsDir = "manifests"
	refsDir      = "refs"
	tmpDir       = "tmp"
	metaDir      = "meta"
	lruFile      = "lru.json"
)

// Config 控制存储行为与回收策略，全部可配置；零值可用（见 normalize）。
type Config struct {
	// ChunkSize 新写入内容的分块大小，0 表示 DefaultChunkSize。
	ChunkSize int

	// MaxBytes 容量上限（字节，0 表示不限）。容量压力下仅回收“无命名引用”的内容，
	// 被 Pin 的内容受保护；若连仅保留被引用内容都超过上限，写入返回 ErrCapacityExceeded。
	MaxBytes int64

	// HighWatermark 高水位比例（(0,1]，0 表示默认 0.9）。
	// 已用容量达到 MaxBytes*HighWatermark 后，写入后触发按 LRU 的容量回收，
	// 回收到高水位以下为止。
	HighWatermark float64

	// SyncWrites 为 true 时，分块/清单/引用落盘执行 fsync 并同步父目录，
	// 崩溃后更强保证；代价是写入吞吐降低。
	SyncWrites bool

	// EvictUnreferenced 为 true 时，每次 GC 都会主动回收所有无命名引用的内容
	// （严格保留模式）；默认 false：无引用内容作为缓存按 LRU 保留，仅在容量
	// 压力下回收。无论该选项如何，损坏数据与不可达垃圾始终被清理。
	EvictUnreferenced bool
}

func (c Config) normalized() Config {
	c.ChunkSize = normalizeChunkSize(c.ChunkSize)
	if c.HighWatermark <= 0 {
		c.HighWatermark = 0.9
	}
	if c.HighWatermark > 1 {
		c.HighWatermark = 1
	}
	return c
}

// refNamePattern 限制命名引用字符集，杜绝路径穿越等问题。
var refNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/@:-]{0,254}$`)

func validRefName(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	return refNamePattern.MatchString(name)
}

// Stats 是存储占用与保留状态的快照。
type Stats struct {
	Bytes          int64 // 本地分块+清单总字节
	ChunkBytes     int64
	ManifestBytes  int64
	Chunks         int   // 去重后的分块数
	Manifests      int   // 内容（清单）数
	PinnedContents int   // 被命名引用的内容数
	PinnedBytes    int64 // 归属于被引用内容的字节（被其引用的共享分块计入）
	MaxBytes       int64
	HighWatermark  float64
	HighWaterBytes int64
}

type chunkRec struct {
	size int64
}

type manifestRec struct {
	rawLen int64
	chunks []ChunkRef
}

// Store 是本地内容寻址存储：负责分块/清单/引用的磁盘格式、去重、索引与容量核算。
// 它本身不做网络分发，网络能力由 Node 在其上组合。
type Store struct {
	dir string
	cfg Config

	guard *guardian

	mu        sync.RWMutex
	chunks    map[ChunkID]*chunkRec
	manifests map[ContentID]*manifestRec
	refs      map[string]ContentID
	lru       *lru
	bytes     int64 // chunks+manifests 字节合计

	gcMu sync.Mutex // 全局串行化 GC

	closed bool
}

// Open 打开（或初始化）dir 下的存储。历史 v1 数据直接加载，无需重建；
// 索引在打开时根据磁盘现状重建，因此进程崩溃/外部拷贝后状态依然一致。
func Open(dir string, cfg Config) (*Store, error) {
	cfg = cfg.normalized()
	for _, sub := range []string{chunksDir, manifestsDir, refsDir, tmpDir, metaDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, opError(KindUnknown, "store.open", dir, err)
		}
	}
	s := &Store{
		dir:       dir,
		cfg:       cfg,
		guard:     newGuardian(),
		chunks:    make(map[ChunkID]*chunkRec),
		manifests: make(map[ContentID]*manifestRec),
		refs:      make(map[string]ContentID),
		lru:       newLRU(),
	}
	if err := s.sweepTmp(); err != nil {
		return nil, err
	}
	if err := s.sweepInflight(); err != nil {
		return nil, err
	}
	if err := s.loadChunks(); err != nil {
		return nil, err
	}
	if err := s.loadManifests(); err != nil {
		return nil, err
	}
	if err := s.loadRefs(); err != nil {
		return nil, err
	}
	s.loadLRUState()
	return s, nil
}

// Dir 返回存储根目录。
func (s *Store) Dir() string { return s.dir }

// Close 持久化 LRU 状态。关闭后不应再使用。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.persistLRULocked()
}

func (s *Store) chunkPath(id ChunkID) string {
	return filepath.Join(s.dir, chunksDir, mustShard(string(id)), string(id))
}

func (s *Store) manifestPath(id ContentID) string {
	return filepath.Join(s.dir, manifestsDir, mustShard(string(id)), string(id))
}

func (s *Store) refPath(name string) string {
	return filepath.Join(s.dir, refsDir, name)
}

// sweepTmp 清理暂存目录：任何遗留临时文件都是上次崩溃的未提交产物，绝不可见。
func (s *Store) sweepTmp() error {
	tmp := filepath.Join(s.dir, tmpDir)
	entries, err := os.ReadDir(tmp)
	if err != nil {
		return opError(KindUnknown, "store.sweepTmp", tmp, err)
	}
	for _, e := range entries {
		_ = os.RemoveAll(filepath.Join(tmp, e.Name()))
	}
	return nil
}

// sweepInflight 清理崩溃遗留的 .inflight 暂存目录（只在打开时执行一次，
// 运行期间不再清理，因此不会与并发提交竞争）。
func (s *Store) sweepInflight() error {
	roots := []string{
		filepath.Join(s.dir, chunksDir),
		filepath.Join(s.dir, manifestsDir),
		filepath.Join(s.dir, refsDir),
		filepath.Join(s.dir, metaDir),
	}
	for _, root := range roots {
		_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() && d.Name() == inflightDir {
				_ = os.RemoveAll(path)
				return filepath.SkipDir
			}
			return nil
		})
	}
	return nil
}

// writeAtomic 通过“目标目录内临时文件 + rename”原子落盘。
// 临时文件与最终文件位于同一目录，rename 是原子操作；不使用公共 tmp 目录，
// 避免与 GC 的 tmp 清扫并发时把别人正在提交的临时文件误删。
// 同一目标的并发写以同一内容寻址（字节必然相同），rename 互换等价。
func (s *Store) writeAtomic(targetDir, targetPath string, data []byte) error {
	return s.writeAtomicInDir(targetPath, data)
}

// inflightDir 是目标分片目录内的隐藏暂存子目录。GC 只枚举分片目录的直接
// 数据文件并跳过目录，因此并发提交中的临时文件绝不会被回收扫描误删；
// rename 仍发生在同一文件系统内（分片目录的直接父级），保持原子性。
const inflightDir = ".inflight"

// writeAtomicInDir 在“目标目录的 .inflight 子目录”中创建临时文件再 rename：
// 重命名与目标同文件系统，原子且不会被 GC 的数据文件扫描当作垃圾清理。
func (s *Store) writeAtomicInDir(targetPath string, data []byte) error {
	dir := filepath.Dir(targetPath)
	tmpParent := filepath.Join(dir, inflightDir)
	if err := os.MkdirAll(tmpParent, 0o755); err != nil {
		return opError(KindUnknown, "store.mkdir", tmpParent, err)
	}
	f, err := os.CreateTemp(tmpParent, ".cas-tmp-*")
	if err != nil {
		return opError(KindUnknown, "store.tmp", targetPath, err)
	}
	tmpName := f.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := f.Write(data); err != nil {
		f.Close()
		cleanup()
		return opError(KindUnknown, "store.write", targetPath, err)
	}
	if s.cfg.SyncWrites {
		if err := f.Sync(); err != nil {
			f.Close()
			cleanup()
			return opError(KindUnknown, "store.fsync", targetPath, err)
		}
	}
	if err := f.Close(); err != nil {
		cleanup()
		return opError(KindUnknown, "store.close", targetPath, err)
	}
	if err := os.Rename(tmpName, targetPath); err != nil {
		cleanup()
		return opError(KindUnknown, "store.rename", targetPath, err)
	}
	if s.cfg.SyncWrites {
		_ = syncDir(dir)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

func (s *Store) loadChunks() error {
	root := filepath.Join(s.dir, chunksDir)
	shards, err := os.ReadDir(root)
	if err != nil {
		return opError(KindUnknown, "store.loadChunks", root, err)
	}
	for _, sh := range shards {
		if !sh.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(root, sh.Name()))
		if err != nil {
			return opError(KindUnknown, "store.loadChunks", sh.Name(), err)
		}
		for _, f := range files {
			name := f.Name()
			if f.IsDir() || !IsValidChunkID(name) {
				// 非预期文件：留给 GC 清理（sweep unreachable 不认识即删）。
				continue
			}
			info, err := f.Info()
			if err != nil {
				return opError(KindUnknown, "store.stat", name, err)
			}
			id := ChunkID(name)
			s.chunks[id] = &chunkRec{size: info.Size()}
			s.bytes += info.Size()
		}
	}
	return nil
}

func (s *Store) loadManifests() error {
	root := filepath.Join(s.dir, manifestsDir)
	shards, err := os.ReadDir(root)
	if err != nil {
		return opError(KindUnknown, "store.loadManifests", root, err)
	}
	for _, sh := range shards {
		if !sh.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(root, sh.Name()))
		if err != nil {
			return opError(KindUnknown, "store.loadManifests", sh.Name(), err)
		}
		for _, f := range files {
			if f.IsDir() || !IsValidContentID(f.Name()) {
				continue
			}
			id := ContentID(f.Name())
			raw, err := os.ReadFile(filepath.Join(root, sh.Name(), f.Name()))
			if err != nil {
				return opError(KindUnknown, "store.readManifest", f.Name(), err)
			}
			// 打开时即做全量校验：损坏/被篡改的清单不进入索引，
			// 随后由 GC 作为不可达垃圾隔离删除；节点再次访问时会重新从对端获取。
			m, verr := verifyManifestBytes(raw, id)
			if verr != nil {
				continue
			}
			s.manifests[id] = &manifestRec{rawLen: int64(len(raw)), chunks: m.Chunks}
			s.bytes += int64(len(raw))
		}
	}
	return nil
}

func (s *Store) loadRefs() error {
	root := filepath.Join(s.dir, refsDir)
	var walkErr error
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		name := filepath.ToSlash(rel)
		if !validRefName(name) {
			walkErr = opError(KindInvalidManifest, "store.loadRefs", name,
				fmt.Errorf("invalid ref file name"))
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		id := ContentID(strings.TrimSpace(string(raw)))
		if !IsValidContentID(string(id)) {
			// 引用文件损坏：不加载，GC 不负责删除 refs，这里仅忽略并暴露给诊断。
			return nil
		}
		s.refs[name] = id
		return nil
	})
	if err != nil {
		return opError(KindUnknown, "store.loadRefs", root, err)
	}
	return walkErr
}

func (s *Store) loadLRUState() {
	raw, _ := os.ReadFile(filepath.Join(s.dir, metaDir, lruFile))
	saved := loadRawLRU(raw)
	pinned := make(map[ContentID]struct{}, len(s.refs))
	for _, id := range s.refs {
		pinned[id] = struct{}{}
	}
	// 与磁盘现状对齐：剔除悬空/已引用条目，再确定性地补入缺失的无引用清单。
	s.lru = newLRU()
	if saved != nil {
		// 持久化顺序为“从旧到新”：先加载已存在且未引用的条目。
		for _, id := range saved {
			cid := ContentID(id)
			if _, ok := s.manifests[cid]; !ok {
				continue
			}
			if _, isPinned := pinned[cid]; isPinned {
				continue
			}
			s.lru.touch(id) // 依次 touch：结果顺序与持久化一致（旧→新）
		}
	}
	missing := make([]string, 0)
	for id := range s.manifests {
		if _, isPinned := pinned[id]; isPinned {
			continue
		}
		if _, ok := s.lru.elems[string(id)]; !ok {
			missing = append(missing, string(id))
		}
	}
	sort.Strings(missing)
	for _, id := range missing {
		s.lru.touch(id) // 磁盘有但 LRU 状态缺失：视为最近使用，置队首
	}
}

func loadRawLRU(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var ids []string
	if json.Unmarshal(raw, &ids) != nil {
		return nil
	}
	return ids
}

func (s *Store) persistLRULocked() error {
	if s.closed {
		return nil
	}
	ids := s.lru.ordered() // 从旧到新
	if ids == nil {
		ids = []string{}
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, metaDir, lruFile)
	return s.writeAtomicInDir(path, raw)
}

// ---- 分块读取 ----

type verifyingReadCloser struct {
	rc       io.ReadCloser
	id       ChunkID
	wantSize int64
	h        hash.Hash
	got      int64
	verified bool
	failed   bool
}

func (v *verifyingReadCloser) Read(p []byte) (int, error) {
	if v.failed {
		return 0, opError(KindCorruptChunk, "chunk.read", string(v.id), nil)
	}
	n, err := v.rc.Read(p)
	if n > 0 {
		v.h.Write(p[:n])
		v.got += int64(n)
	}
	if err != nil && !v.verified {
		v.verified = true
		sum := v.h.Sum(nil)
		got := chunkIDPrefix + fmt.Sprintf("%x", sum)
		if err == io.EOF && v.got == v.wantSize && got == string(v.id) {
			return n, err
		}
		v.failed = true
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return n, opError(KindCorruptChunk, "chunk.read", string(v.id),
				fmt.Errorf("expected size=%d hash=%s, got size=%d hash=%s",
					v.wantSize, v.id, v.got, got))
		}
		return n, err
	}
	return n, err
}

func (v *verifyingReadCloser) Close() error { return v.rc.Close() }

// OpenChunk 打开分块并返回流式校验读取器：读到 EOF 时自动比对长度与 SHA-256，
// 任何不一致表现为 KindCorruptChunk。返回的 size 是索引记录的分块大小。
// 调用方必须持有相应的生命周期保护（Node 层通过 guardian 获取）。
func (s *Store) OpenChunk(id ChunkID) (io.ReadCloser, int64, error) {
	s.mu.RLock()
	rec := s.chunks[id]
	s.mu.RUnlock()
	if rec == nil {
		return nil, 0, opError(KindNotFound, "store.openChunk", string(id), nil)
	}
	path := s.chunkPath(id)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// 索引与磁盘不一致（外部删除）：归类为分块缺失，交由上层从对端重新获取。
			s.mu.Lock()
			if cur := s.chunks[id]; cur != nil {
				delete(s.chunks, id)
				s.bytes -= cur.size
			}
			s.mu.Unlock()
			return nil, 0, opError(KindMissingChunk, "store.openChunk", string(id), err)
		}
		return nil, 0, opError(KindUnknown, "store.openChunk", string(id), err)
	}
	return &verifyingReadCloser{
		rc:       f,
		id:       id,
		wantSize: rec.size,
		h:        newSHA256(),
	}, rec.size, nil
}

// HasChunk 报告分块是否在本地存在（以索引为准，GC 期间保持一致）。
func (s *Store) HasChunk(id ChunkID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.chunks[id]
	return ok
}

// HasManifest 报告内容清单是否在本地存在。
func (s *Store) HasManifest(id ContentID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.manifests[id]
	return ok
}

// ReadManifest 读取并校验清单，wantID 为空时以文件字节重算内容标识。
func (s *Store) ReadManifest(id ContentID) (*Manifest, []byte, error) {
	if !IsValidContentID(string(id)) {
		return nil, nil, opError(KindInvalidArgument, "store.readManifest", string(id), nil)
	}
	path := s.manifestPath(id)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, opError(KindNotFound, "store.readManifest", string(id), nil)
		}
		return nil, nil, opError(KindUnknown, "store.readManifest", string(id), err)
	}
	m, err := verifyManifestBytes(raw, id)
	if err != nil {
		return nil, raw, err
	}
	return m, raw, nil
}

// CommitChunkBytes 以拉取/修复路径写入一个已自带标识的分块；
// 字节会先做摘要校验，不一致绝不落盘。重复内容只保留一份（去重）。
// 调用方应已通过 guardian 取得生命周期保护。
func (s *Store) CommitChunkBytes(id ChunkID, p []byte) error {
	if err := verifyChunk(id, p); err != nil {
		return err
	}
	return s.commitChunk(id, p)
}

// commitChunkLocked 落盘分块（幂等、去重）。已存在则直接复用，不重写。
func (s *Store) commitChunk(id ChunkID, p []byte) error {
	path := s.chunkPath(id)
	s.mu.Lock()
	rec, exists := s.chunks[id]
	s.mu.Unlock()
	if exists {
		// 内容寻址 + 读取时全量校验：已存在副本若损坏会在读取/Verify 时被发现并修复，
		// 此处无需重复读盘。
		_ = rec
		return nil
	}
	if _, err := os.Stat(path); err == nil {
		// 磁盘有文件但索引缺失（例如异常状态恢复）：补索引即可。
		info, statErr := os.Stat(path)
		size := int64(len(p))
		if statErr == nil {
			size = info.Size()
		}
		s.mu.Lock()
		if _, ok := s.chunks[id]; !ok {
			s.chunks[id] = &chunkRec{size: size}
			s.bytes += size
		}
		s.mu.Unlock()
		return nil
	}
	if err := s.writeAtomic(filepath.Dir(path), path, p); err != nil {
		return err
	}
	s.mu.Lock()
	if _, ok := s.chunks[id]; !ok {
		s.chunks[id] = &chunkRec{size: int64(len(p))}
		s.bytes += int64(len(p))
	}
	s.mu.Unlock()
	return nil
}

// CommitManifestBytes 以拉取路径写入清单字节：先验证字节摘要与内容标识一致，
// 再落盘。返回其内容标识。调用方负责保证引用分块可获取（本地或对端）。
func (s *Store) CommitManifestBytes(raw []byte) (ContentID, error) {
	cid := computeContentID(raw)
	m, err := verifyManifestBytes(raw, cid)
	if err != nil {
		return "", err
	}
	_ = m
	if err := s.commitManifest(cid, raw); err != nil {
		return "", err
	}
	return cid, nil
}

// commitManifestLocked 幂等落盘清单并登记索引；无引用内容进入 LRU 尾部。
func (s *Store) commitManifest(cid ContentID, raw []byte) error {
	path := s.manifestPath(cid)
	m, err := verifyManifestBytes(raw, cid)
	if err != nil {
		return err
	}
	s.mu.Lock()
	_, exists := s.manifests[cid]
	_, isPinned := s.refsValueLocked(cid)
	s.mu.Unlock()
	if !exists {
		if _, statErr := os.Stat(path); statErr != nil {
			if err := s.writeAtomic(filepath.Dir(path), path, raw); err != nil {
				return err
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.manifests[cid]; !ok {
		s.manifests[cid] = &manifestRec{rawLen: int64(len(raw)), chunks: append([]ChunkRef(nil), m.Chunks...)}
		s.bytes += int64(len(raw))
	}
	if !isPinned {
		s.lru.touch(string(cid))
	}
	return nil
}

// removeChunkFile 从磁盘与索引删除分块（GC / 损坏隔离使用）。
// 调用方必须已通过 guardian 取得该分块的独占权。
func (s *Store) removeChunkFile(id ChunkID, corrupt bool) bool {
	path := s.chunkPath(id)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return false
	}
	s.mu.Lock()
	if rec := s.chunks[id]; rec != nil {
		delete(s.chunks, id)
		s.bytes -= rec.size
	}
	s.mu.Unlock()
	_ = corrupt
	return true
}

// removeManifestFile 从磁盘与索引删除清单。调用方须持 guardian 独占权。
func (s *Store) removeManifestFile(id ContentID) bool {
	path := s.manifestPath(id)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return false
	}
	s.mu.Lock()
	if rec := s.manifests[id]; rec != nil {
		delete(s.manifests, id)
		s.bytes -= rec.rawLen
	}
	s.lru.remove(string(id))
	s.mu.Unlock()
	return true
}

// quarantineChunk 隔离删除已确认损坏/被篡改的本地分块，以便随后从对端重新获取；
// 损坏数据绝不保留为有效副本。调用方不得持有该分块的普通访问权。
func (s *Store) quarantineChunk(id ChunkID) bool {
	s.guard.GCLock(string(id))
	ok := s.removeChunkFile(id, true)
	s.guard.GCUnlock(string(id))
	return ok
}

// Put 写入 r 的全部内容：分块、摘要、组装清单、去重落盘，返回内容标识。
// 并发写入相同内容只会产生一份磁盘副本（分块与清单均幂等）。
func (s *Store) Put(ctx context.Context, r io.Reader) (ContentID, error) {
	return s.putWithChunkSize(ctx, r, s.cfg.ChunkSize)
}

// PutWithChunkSize 使用指定分块大小写入（主要用于测试）。
func (s *Store) PutWithChunkSize(ctx context.Context, r io.Reader, chunkSize int) (ContentID, error) {
	return s.putWithChunkSize(ctx, r, normalizeChunkSize(chunkSize))
}

type heldKey struct {
	key     string
	release func()
}

func (s *Store) putWithChunkSize(ctx context.Context, r io.Reader, chunkSize int) (ContentID, error) {
	buf := make([]byte, chunkSize)
	var refs []ChunkRef
	var total int64
	var holds []heldKey
	var newChunks []ChunkID
	newChunkBytes := int64(0)

	releaseHolds := func() {
		for i := len(holds) - 1; i >= 0; i-- {
			holds[i].release()
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			releaseHolds()
			return "", opError(KindUnknown, "store.put", "", err)
		}
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			p := buf[:n]
			id := chunkIDFromBytes(p)
			rel := s.guard.Acquire(string(id))
			holds = append(holds, heldKey{string(id), rel})
			existed := s.HasChunk(id)
			if err := s.commitChunk(id, p); err != nil {
				releaseHolds()
				return "", err
			}
			if !existed {
				newChunks = append(newChunks, id)
				newChunkBytes += int64(n)
			}
			refs = append(refs, ChunkRef{ID: id, Size: int64(n)})
			total += int64(n)
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			releaseHolds()
			return "", opError(KindUnknown, "store.put", "", readErr)
		}
	}

	m := &Manifest{
		Version:   manifestVersionV1,
		Hash:      hashAlgoSHA256,
		ChunkSize: chunkSize,
		Length:    total,
		Chunks:    refs,
	}
	if err := validateManifest(m); err != nil {
		releaseHolds()
		return "", err
	}
	raw, err := encodeManifest(m)
	if err != nil {
		releaseHolds()
		return "", err
	}
	cid := computeContentID(raw)

	// 容量预留：在仍持有本次全部分块保护的情况下做 LRU 回收腾位。
	// 本批新分块受 guardian 保护，绝不会被这里的回收误删。
	if err := s.reserveCapacity(ctx, int64(len(raw)), newChunkBytes, newChunks); err != nil {
		releaseHolds()
		s.rollbackNewChunks(newChunks)
		return "", err
	}

	relM := s.guard.Acquire(string(cid))
	holds = append(holds, heldKey{string(cid), relM})
	if err := s.commitManifest(cid, raw); err != nil {
		releaseHolds()
		return "", err
	}
	releaseHolds()

	s.mu.Lock()
	over := s.cfg.MaxBytes > 0 && s.bytes > s.watermarkBytesLocked()
	s.mu.Unlock()
	if over {
		_, _ = s.GC(ctx, GCOptions{})
	}
	return cid, nil
}

// rollbackNewChunks 在容量不足导致写入失败时，回收本次新引入的分块副本。
// 必须在释放全部守卫之后调用：取独占锁后再次确认该分块未被任何清单引用，
// 从而在并发写入相同内容的情况下绝不会删掉另一个已提交内容的共享分块。
func (s *Store) rollbackNewChunks(ids []ChunkID) {
	for _, id := range ids {
		s.guard.GCLock(string(id))
		if !s.chunkReferencedByAnyManifest(id) {
			s.removeChunkFile(id, false)
		}
		s.guard.GCUnlock(string(id))
	}
}

// chunkReferencedByAnyManifest 报告分块当前是否被任何已登记清单引用。
func (s *Store) chunkReferencedByAnyManifest(id ChunkID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.manifests {
		for _, c := range m.chunks {
			if c.ID == id {
				return true
			}
		}
	}
	return false
}

func (s *Store) watermarkBytesLocked() int64 {
	if s.cfg.MaxBytes <= 0 {
		return 0
	}
	return int64(float64(s.cfg.MaxBytes) * s.cfg.HighWatermark)
}

// reserveCapacity 保证提交后占用不超过 MaxBytes：先按 LRU 回收无引用内容腾位；
// 若连“仅保留被引用内容 + 本次真正新增的字节”都超过硬上限，则失败，
// 调用方回滚本次新分块。已有读取与已建立的引用不受影响。
func (s *Store) reserveCapacity(ctx context.Context, manifestBytes, newChunkBytes int64, newChunks []ChunkID) error {
	if s.cfg.MaxBytes <= 0 {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.RLock()
		// 本次新分块中与已存在的被引用内容共享的部分，pinnedBytes 已计入，
		// 不应重复累加：扣除这些共享新分块的大小。
		shared := int64(0)
		for _, id := range newChunks {
			if rec, ok := s.chunks[id]; ok {
				if s.chunkReferencedByPinnedLocked(id) {
					shared += rec.size
				}
			}
		}
		pinned := s.pinnedBytesLocked()
		need := s.bytes + manifestBytes + newChunkBytes
		hardFloor := pinned + manifestBytes + newChunkBytes - shared
		water := s.watermarkBytesLocked()
		s.mu.RUnlock()

		if need <= water {
			return nil
		}
		rep, err := s.GC(ctx, GCOptions{})
		if err != nil {
			return err
		}
		if hardFloor > s.cfg.MaxBytes {
			return opError(KindCapacityExceeded, "store.reserve", "",
				fmt.Errorf("pinned content %d + new data %d exceeds MaxBytes %d",
					pinned, manifestBytes+newChunkBytes-shared, s.cfg.MaxBytes))
		}
		if rep.EvictedContents == 0 && rep.RemovedChunks == 0 && rep.CorruptManifests == 0 {
			if need > s.cfg.MaxBytes {
				return opError(KindCapacityExceeded, "store.reserve", "",
					fmt.Errorf("need %d > MaxBytes %d and nothing evictable", need, s.cfg.MaxBytes))
			}
			return nil // 处于高水位与硬上限之间：放行，提交后再回收
		}
	}
}

// chunkReferencedByPinnedLocked 判断分块是否被某个被引用内容引用（调用方持锁）。
func (s *Store) chunkReferencedByPinnedLocked(id ChunkID) bool {
	for cid := range s.pinnedSetLocked() {
		if m, ok := s.manifests[cid]; ok {
			for _, c := range m.chunks {
				if c.ID == id {
					return true
				}
			}
		}
	}
	return false
}

// ---- 命名引用（保留策略的“根”）----

// Resolve 返回命名引用指向的内容标识。
func (s *Store) Resolve(name string) (ContentID, error) {
	if !validRefName(name) {
		return "", opError(KindInvalidArgument, "store.resolve", name, nil)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.refs[name]
	if !ok {
		return "", opError(KindRefNotFound, "store.resolve", name, nil)
	}
	return id, nil
}

// Refs 返回全部命名引用的副本。
func (s *Store) Refs() map[string]ContentID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]ContentID, len(s.refs))
	for k, v := range s.refs {
		out[k] = v
	}
	return out
}

// SetRef 创建或覆盖命名引用；内容必须本地存在（或由 Node 层保证可获取）。
func (s *Store) SetRef(name string, id ContentID) error {
	if !validRefName(name) {
		return opError(KindInvalidArgument, "store.setRef", name, nil)
	}
	if !IsValidContentID(string(id)) {
		return opError(KindInvalidArgument, "store.setRef", string(id), nil)
	}
	// 先取得内容的生命周期保护：保证与 GC 并发时，引用建立完成前内容不被删除。
	rel := s.guard.Acquire(string(id))
	defer rel()
	s.mu.RLock()
	_, exists := s.manifests[id]
	s.mu.RUnlock()
	if !exists {
		return opError(KindNotFound, "store.setRef", string(id), nil)
	}
	// 先在内存中提交引用（GC 快照以内存为准，立即把该内容视为受保护），
	// 再原子落盘；落盘失败则回滚内存。这样“文件已写但 GC 尚未看到引用”
	// 的窗口不会导致内容被误回收。
	s.mu.Lock()
	old, hadOld := s.refs[name]
	s.refs[name] = id
	s.lru.remove(string(id)) // 被引用 → 受容量回收保护
	var oldLRU []ContentID
	if hadOld && old != id {
		if _, ok := s.manifests[old]; ok {
			if _, pinned := s.refsValueLocked(old); !pinned {
				s.lru.touch(string(old))
			}
		}
	}
	_ = oldLRU
	s.mu.Unlock()

	if err := s.writeRefFile(name, id); err != nil {
		// 回滚内存到覆盖前状态，保持“内存 = 磁盘”。
		s.mu.Lock()
		if hadOld {
			s.refs[name] = old
			s.lru.remove(string(old))
		} else {
			delete(s.refs, name)
		}
		s.lru.touch(string(id))
		s.mu.Unlock()
		return err
	}

	s.mu.Lock()
	perr := s.persistLRULocked()
	s.mu.Unlock()
	return perr
}

// CreateRef 仅在引用不存在时创建，已存在返回 ErrRefExists。
func (s *Store) CreateRef(name string, id ContentID) error {
	if !validRefName(name) {
		return opError(KindInvalidArgument, "store.createRef", name, nil)
	}
	s.mu.RLock()
	_, exists := s.refs[name]
	s.mu.RUnlock()
	if exists {
		return opError(KindRefExists, "store.createRef", name, nil)
	}
	return s.SetRef(name, id)
}

// DeleteRef 删除命名引用（解除保留）；内容本身不立即删除，由 GC 按 LRU 回收。
func (s *Store) DeleteRef(name string) error {
	if !validRefName(name) {
		return opError(KindInvalidArgument, "store.deleteRef", name, nil)
	}
	s.mu.Lock()
	old, ok := s.refs[name]
	if !ok {
		s.mu.Unlock()
		return opError(KindRefNotFound, "store.deleteRef", name, nil)
	}
	delete(s.refs, name)
	s.mu.Unlock()

	if err := os.Remove(s.refPath(name)); err != nil && !os.IsNotExist(err) {
		// 回滚内存状态，保持与磁盘一致。
		s.mu.Lock()
		s.refs[name] = old
		s.mu.Unlock()
		return opError(KindUnknown, "store.deleteRef", name, err)
	}
	s.mu.Lock()
	if _, still := s.manifests[old]; still {
		if _, pinned := s.refsValueLocked(old); !pinned {
			s.lru.touch(string(old))
		}
	}
	perr := s.persistLRULocked()
	s.mu.Unlock()
	return perr
}

func (s *Store) writeRefFile(name string, id ContentID) error {
	path := s.refPath(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return opError(KindUnknown, "store.mkdirRef", name, err)
	}
	return s.writeAtomicInDir(path, []byte(string(id)))
}

func (s *Store) refsValueLocked(cid ContentID) (string, bool) {
	for name, id := range s.refs {
		if id == cid {
			return name, true
		}
	}
	return "", false
}

// pinnedContentsLocked 返回被引用的内容集合（调用方持锁）。
func (s *Store) pinnedSetLocked() map[ContentID]struct{} {
	out := make(map[ContentID]struct{}, len(s.refs))
	for _, id := range s.refs {
		out[id] = struct{}{}
	}
	return out
}

// pinnedBytesLocked 计算归属于被引用内容的字节：被任一被引用清单使用的分块
// 计入一次，加上被引用清单自身字节。
func (s *Store) pinnedBytesLocked() int64 {
	var n int64
	liveChunks := make(map[ChunkID]struct{})
	for cid := range s.pinnedSetLocked() {
		if m, ok := s.manifests[cid]; ok {
			n += m.rawLen
			for _, c := range m.chunks {
				if rec, ok := s.chunks[c.ID]; ok {
					if _, seen := liveChunks[c.ID]; !seen {
						liveChunks[c.ID] = struct{}{}
						n += rec.size
					}
				}
			}
		}
	}
	return n
}

// Stats 返回占用快照。
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var chunkBytes, manifestBytes int64
	for _, c := range s.chunks {
		chunkBytes += c.size
	}
	for _, m := range s.manifests {
		manifestBytes += m.rawLen
	}
	return Stats{
		Bytes:          s.bytes,
		ChunkBytes:     chunkBytes,
		ManifestBytes:  manifestBytes,
		Chunks:         len(s.chunks),
		Manifests:      len(s.manifests),
		PinnedContents: len(s.pinnedSetLocked()),
		PinnedBytes:    s.pinnedBytesLocked(),
		MaxBytes:       s.cfg.MaxBytes,
		HighWatermark:  s.cfg.HighWatermark,
		HighWaterBytes: s.watermarkBytesLocked(),
	}
}
