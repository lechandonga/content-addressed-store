package cas

import "sync"

// flightGroup 是标准库风格的 single-flight：同一时刻对同一个 key 只会有一个
// fn 在执行，其余并发调用共享其结果。用于合并多个并发的网络拉取/写入，
// 避免重复下载、重复落盘或相互覆盖。
type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

type flightCall struct {
	done chan struct{}
	val  interface{}
	err  error
}

func newFlightGroup() *flightGroup {
	return &flightGroup{m: make(map[string]*flightCall)}
}

func (g *flightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*flightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		<-c.done
		return c.val, c.err
	}
	c := &flightCall{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	close(c.done)

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.val, c.err
}
