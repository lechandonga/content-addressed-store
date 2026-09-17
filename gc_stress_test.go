package cas

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStressPinRace(t *testing.T) {
	for iter := 0; iter < 4; iter++ {
		s := testStore(t, Config{ChunkSize: 8, MaxBytes: 100000,
			HighWatermark: 0.0001, LowWatermark: 0.00001})
		ctx := context.Background()
		var cids []Addr
		for i := 0; i < 6; i++ {
			c, _ := s.Put(payload(200 + i))
			cids = append(cids, c)
		}
		peer := testStore(t, Config{ChunkSize: 8})
		for i := 0; i < 6; i++ {
			_, _ = peer.Put(payload(200 + i))
		}
		s.SetFetcher(storeFetcher(t, peer))
		for i := 0; i < 3; i++ {
			_ = s.Pin(ctx, string(rune('a'+i)), cids[i])
		}
		stop := make(chan struct{})
		var rwg, wg sync.WaitGroup
		var failErr error
		var mu sync.Mutex
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i, cid := range cids {
					b, err := s.Get(ctx, cid)
					if err == nil && !bytes.Equal(b, payload(200+i)) {
						mu.Lock()
						failErr = errors.New("corrupt read")
						mu.Unlock()
						return
					}
				}
				runtime_Gosched()
			}
		}()
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				s.Collect(ctx, GCConfig{EvictAll: true})
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				idx := 3 + (i % 3)
				if err := s.Pin(ctx, "dyn", cids[idx]); err != nil {
					mu.Lock()
					failErr = err
					mu.Unlock()
					return
				}
				if b, err := s.Get(ctx, cids[idx]); err != nil || !bytes.Equal(b, payload(200+idx)) {
					mu.Lock()
					failErr = err
					mu.Unlock()
					return
				}
				s.Unpin("dyn")
			}
		}()
		wg.Wait()
		close(stop)
		rwg.Wait()
		mu.Lock()
		fe := failErr
		mu.Unlock()
		if fe != nil {
			quar := []string{}
			filepath.Walk(filepath.Join(s.Root(), dirQuarantine), func(p string, fi os.FileInfo, e error) error {
				if e == nil && !fi.IsDir() {
					quar = append(quar, filepath.Base(p))
				}
				return nil
			})
			t.Fatalf("iter %d: %v; quarantine=%v", iter, fe, quar)
		}
		s.Close()
	}
}
