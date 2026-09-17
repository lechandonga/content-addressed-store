package cas

import "sync"

// flightCall 表示一次正在进行的共享调用。
type flightCall struct {
	wg  sync.WaitGroup
	val any
	err error
}

// flightGroup 是一个极简 singleflight：同一时刻相同 key 的并发调用只会执行一次，
// 其余调用共享同一份结果。这样多个并发获取同一内容只会产生一次网络请求与一次落盘，
// 不会出现重复副本或相互覆盖。
type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

// do 执行 fn；若 key 已有进行中的调用，则等待并复用其结果。
// shared=true 表示结果来自另一个并发调用。
func (g *flightGroup) do(key string, fn func() (any, error)) (any, bool, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*flightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, true, c.err
	}
	c := &flightCall{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.val, false, c.err
}
