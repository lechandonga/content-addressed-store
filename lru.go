package cas

import "container/list"

// lru 是按 ContentID 的最近最少使用顺序表（仅跟踪“可回收”的无引用清单）。
// 队首为最近使用，队尾为最久未使用；GC 需要腾容量时从队尾淘汰。
//
// 被 Pin 的内容立即移出 LRU（受保护，不参与容量淘汰）；Unpin 后重新加入。
type lru struct {
	ll    *list.List // *list.Value 存 string(ContentID)
	elems map[string]*list.Element
}

func newLRU() *lru {
	return &lru{ll: list.New(), elems: make(map[string]*list.Element)}
}

func (l *lru) touch(id string) {
	if e, ok := l.elems[id]; ok {
		l.ll.MoveToFront(e)
		return
	}
	e := l.ll.PushFront(id)
	l.elems[id] = e
}

func (l *lru) remove(id string) {
	if e, ok := l.elems[id]; ok {
		l.ll.Remove(e)
		delete(l.elems, id)
	}
}

func (l *lru) len() int { return l.ll.Len() }

// oldest 返回最久未使用的标识；空表返回空串。
func (l *lru) oldest() string {
	e := l.ll.Back()
	if e == nil {
		return ""
	}
	return e.Value.(string)
}

// ordered 返回从旧到新的全部标识（用于持久化与 GC 枚举）。
func (l *lru) ordered() []string {
	out := make([]string, 0, l.ll.Len())
	for e := l.ll.Back(); e != nil; e = e.Prev() {
		out = append(out, e.Value.(string))
	}
	return out
}
