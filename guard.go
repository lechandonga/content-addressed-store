package cas

import "sync"

// guardian 协调按逻辑键（ChunkID / ContentID）的并发访问者与回收器：
//
//   - 普通访问（读/写/拉取）为“共享”模式，多 goroutine 可同时持有；
//   - GC 删除为“独占”模式，等待全部共享访问结束后进行；
//   - GC 标记候选键（MarkCandidates）后，新访问者阻塞等待本轮提交：
//     被实际删除时调用方随后读盘会得到“本地缺失”并自动重新获取；
//     因当时活跃被 GC 跳过的情况则直接继续访问；
//   - 等待者在等待期间也计入共享占用，GC 提交时不会在它们等待期间删数据。
//
// 本实现不使用 sync.RWMutex：多个共享访问者可能需要等待彼此完成的同 key
// single-flight 拉取（网络 I/O），RWMutex 下等待者持锁会自锁。这里约定：
// 需要在“已持共享访问”的代码中继续调用下层时，使用 *Held 变体（同 goroutine
// 不得对同一键重复 Acquire）。
type guardian struct {
	mu      sync.Mutex
	entries map[string]*guarded
}

type guarded struct {
	cond   *sync.Cond // Locker=&guardian.mu
	users  int        // 共享访问者数；-1 表示 GC 独占
	wait   int        // 等待 doomed 结束的访问者数
	doomed bool       // 本轮 GC 已标记待删
}

func newGuardian() *guardian {
	return &guardian{entries: make(map[string]*guarded)}
}

func (g *guardian) getLocked(key string) *guarded {
	e := g.entries[key]
	if e == nil {
		e = &guarded{}
		e.cond = sync.NewCond(&g.mu)
		g.entries[key] = e
	}
	return e
}

func (g *guardian) cleanupLocked(key string, e *guarded) {
	if e.users == 0 && e.wait == 0 && !e.doomed {
		delete(g.entries, key)
	}
}

// Acquire 获取一个键的共享访问权，返回必须调用的释放函数。
// 同 goroutine 不得对同一键重复 Acquire（应使用带 Held 的内部变体）。
func (g *guardian) Acquire(key string) func() {
	g.mu.Lock()
	e := g.getLocked(key)
	if e.doomed {
		e.wait++
		for e.doomed {
			e.cond.Wait()
		}
		e.wait--
	}
	e.users++
	g.mu.Unlock()

	return func() {
		g.mu.Lock()
		e.users--
		if e.users == 0 {
			e.cond.Broadcast()
		}
		g.cleanupLocked(key, e)
		g.mu.Unlock()
	}
}

// MarkCandidates 标记候选键进入本轮 GC 待删状态。返回实际可处理的键：
// 标记时仍有共享访问者的键被跳过（留待下轮），绝不删除正在使用的数据。
func (g *guardian) MarkCandidates(keys []string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		e := g.getLocked(k)
		if e.users > 0 {
			continue
		}
		if !e.doomed {
			e.doomed = true
			out = append(out, k)
		}
	}
	return out
}

// GCLock 独占一个键：等待全部共享访问结束。不可与同 goroutine 已持有的共享
// 访问嵌套（GC/隔离流程从不嵌套）。
func (g *guardian) GCLock(key string) {
	g.mu.Lock()
	e := g.entries[key]
	if e == nil {
		g.mu.Unlock()
		return
	}
	for e.users > 0 {
		e.cond.Wait()
	}
	e.users = -1 // 独占态
	g.mu.Unlock()
}

// GCUnlock 释放独占。
func (g *guardian) GCUnlock(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.entries[key]
	if e == nil || e.users != -1 {
		return
	}
	e.users = 0
	e.cond.Broadcast()
	g.cleanupLocked(key, e)
}

// Commit 结束本轮 GC：清除 doomed 标记并唤醒等待者。
func (g *guardian) Commit(keys []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, k := range keys {
		e := g.entries[k]
		if e == nil {
			continue
		}
		e.doomed = false
		g.cleanupLocked(k, e)
		e.cond.Broadcast()
	}
}

// ActiveUsers 返回某键当前共享访问者数（测试/诊断用）。
func (g *guardian) ActiveUsers(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e := g.entries[key]; e != nil && e.users > 0 {
		return e.users
	}
	return 0
}
