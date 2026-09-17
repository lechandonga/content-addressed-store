package cas

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// pinRecord 是 refs/<name>.json 的存储格式（v1）。
// 引用是“保留策略”的核心：被引用的内容在任何情况下都不会被回收。
type pinRecord struct {
	Version int    `json:"version"`
	Name    string `json:"name"`
	Addr    string `json:"addr"`
	Created string `json:"created"` // RFC3339
}

const pinVersion = 1

// PinInfo 是对外暴露的引用信息。
type PinInfo struct {
	Name    string
	Addr    Addr
	Created time.Time
}

// Pin 创建一个指向 cid 的命名引用（保留标记）。
//
//   - 同一名字重复 Pin 相同内容是幂等的；
//   - 同一名字 Pin 到不同内容返回 ErrRefConflict；
//   - Pin 时若内容不在本地，会像 Get 一样从对端按需获取并完整校验；
//   - “获取内容”与“写入引用”在同一个 gcMu 读锁临界区内收尾：
//     GC 要么看到内容尚未引用（但此刻引用也正在同临界区落盘），
//     要么看到引用已存在；绝不存在“内容已取回却被 GC 在引用落盘前删掉”的窗口。
func (s *Store) Pin(ctx context.Context, name string, cid Addr) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if name == "" {
		return fail(KindInvalid, "pin", "", "empty pin name", nil)
	}
	if !cid.Valid() {
		return fail(KindInvalid, "pin", string(cid), "bad content address", nil)
	}

	// 在获取前先做一次同名冲突快检，避免为注定冲突的 Pin 发起网络传输。
	if err := s.checkRefConflict(name, cid); err != nil {
		return err
	}

	// 获取 + 校验 + 写引用在同一个受 GC 读锁保护的临界区中完成。
	return s.getProtected(ctx, cid, func() error {
		s.commitMu.Lock()
		defer s.commitMu.Unlock()
		if !s.hasLocked(bManifest, cid) {
			// 处于 getProtected 的读锁保护临界区，理论不可能发生。
			return fail(KindNotFound, "pin", string(cid),
				"content vanished before pin", nil)
		}
		return s.writeRefLocked(name, cid)
	})
}

// checkRefConflict 在不获取内容的前提下检查同名引用是否指向不同内容。
func (s *Store) checkRefConflict(name string, cid Addr) error {
	s.beginRead()
	defer s.endRead()
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if existing, err := os.ReadFile(s.refPath(name)); err == nil {
		var rec pinRecord
		if json.Unmarshal(existing, &rec) == nil && rec.Addr != string(cid) {
			return fail(KindRefConflict, "pin", string(cid),
				guardf("pin %q already points to %s", name, rec.Addr), nil)
		}
	}
	return nil
}

// writeRefLocked 原子写入引用；若存在同名冲突则报错。调用方持 commitMu。
func (s *Store) writeRefLocked(name string, cid Addr) error {
	path := s.refPath(name)
	if existing, err := os.ReadFile(path); err == nil {
		var rec pinRecord
		if jerr := json.Unmarshal(existing, &rec); jerr == nil {
			if rec.Addr == string(cid) {
				return nil // 幂等
			}
			return fail(KindRefConflict, "pin", string(cid),
				guardf("pin %q already points to %s", name, rec.Addr), nil)
		}
		// 损坏的引用文件：隔离后覆盖重建，避免残留卡死引用。
		if qerr := s.quarantineLocked(path, "corrupt-ref"); qerr != nil {
			return qerr
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fail(KindIO, "pin", name, "read ref", err)
	}
	rec := pinRecord{
		Version: pinVersion,
		Name:    name,
		Addr:    string(cid),
		Created: s.now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.MarshalIndent(&rec, "", "  ")
	if err != nil {
		return fail(KindIO, "pin", name, "marshal ref", err)
	}
	if err := writeFileAtomic(s.root, filepath.Join(dirRefs, filepath.Base(path)), data); err != nil {
		return err
	}
	return nil
}

// Unpin 删除一个引用。删除后内容并不会立即消失，而是在下一次回收时
// 按容量策略与 LRU 顺序处理；名字不存在返回 ErrRefNotFound（幂等语义上也可忽略）。
func (s *Store) Unpin(name string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if name == "" {
		return fail(KindInvalid, "unpin", "", "empty pin name", nil)
	}
	s.beginRead()
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	defer s.endRead()

	path := s.refPath(name)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fail(KindRefNotFound, "unpin", name, "no such pin", nil)
		}
		return fail(KindIO, "unpin", name, "", err)
	}
	syncDir(filepath.Dir(path))
	return nil
}

// Resolve 返回引用名当前指向的内容地址；不存在返回 ErrRefNotFound。
// 引用文件损坏时会被隔离并报告损坏，避免返回不可信映射。
func (s *Store) Resolve(name string) (Addr, error) {
	if name == "" {
		return "", fail(KindInvalid, "resolve", "", "empty pin name", nil)
	}
	s.beginRead()
	defer s.endRead()
	rec, err := s.readRef(name)
	if err != nil {
		return "", err
	}
	a, err := ParseAddr(rec.Addr)
	if err != nil {
		s.commitMu.Lock()
		_ = s.quarantineLocked(s.refPath(name), "corrupt-ref")
		s.commitMu.Unlock()
		return "", fail(KindCorruptManifest, "resolve", rec.Addr,
			"pin record contains invalid address", nil)
	}
	return a, nil
}

func (s *Store) readRef(name string) (pinRecord, error) {
	var rec pinRecord
	data, err := os.ReadFile(s.refPath(name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return rec, fail(KindRefNotFound, "resolve", name, "no such pin", nil)
		}
		return rec, fail(KindIO, "resolve", name, "", err)
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		s.commitMu.Lock()
		_ = s.quarantineLocked(s.refPath(name), "corrupt-ref")
		s.commitMu.Unlock()
		return rec, fail(KindCorruptManifest, "resolve", name, "corrupt pin record", err)
	}
	return rec, nil
}

// Pins 返回全部引用，按名字排序。
func (s *Store) Pins() ([]PinInfo, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	s.beginRead()
	defer s.endRead()
	out := make([]PinInfo, 0)
	err := walkRefs(s.root, func(name string, rec pinRecord) bool {
		a, perr := ParseAddr(rec.Addr)
		if perr != nil {
			return true // 单个坏引用不影响列表；Resolve 时会隔离。
		}
		t, _ := time.Parse(time.RFC3339Nano, rec.Created)
		out = append(out, PinInfo{Name: name, Addr: a, Created: t})
		return true
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// walkRefs 遍历 refs 目录。cb 返回 false 可提前停止。
func walkRefs(root string, cb func(name string, rec pinRecord) bool) error {
	dir := filepath.Join(root, dirRefs)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fail(KindIO, "walk-refs", dir, "", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var rec pinRecord
		if err := json.Unmarshal(data, &rec); err != nil || rec.Name == "" {
			continue
		}
		if !cb(rec.Name, rec) {
			return nil
		}
	}
	return nil
}
