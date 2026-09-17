// casdemo 是一个最小可运行示例：在两个本地节点间演示
// 内容写入、HTTP 分发、按需获取、引用保留与容量回收。
//
// 运行： go run ./cmd/casdemo
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/content-addressed-store"
	"github.com/lechandonga/content-addressed-store/cashttp"
)

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "casdemo-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(base)

	// 节点 A：内容生产者，并启动 HTTP 分发服务。
	dirA := filepath.Join(base, "nodeA")
	nodeA, err := cas.Open(dirA, cas.Config{ChunkSize: 1 << 20})
	must(err)
	defer nodeA.Close()

	srv := &http.Server{
		Addr:    "127.0.0.1:18080",
		Handler: cashttp.NewServer(nodeA),
	}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()
	time.Sleep(200 * time.Millisecond)

	content := bytes.Repeat([]byte("the quick brown fox "), 200000) // ~4.4 MiB
	cid, err := nodeA.Put(content)
	must(err)
	must(nodeA.Pin(ctx, "datasets/animals", cid))
	fmt.Printf("nodeA put content, cid=%s size=%d\n", cid, len(content))

	// 节点 B：消费者，配置较小容量与 A 为对端。
	dirB := filepath.Join(base, "nodeB")
	nodeB, err := cas.Open(dirB, cas.Config{
		ChunkSize:     1 << 20,
		MaxBytes:      8 << 20,
		HighWatermark: 0.8,
		LowWatermark:  0.5,
	})
	must(err)
	defer nodeB.Close()
	nodeB.SetFetcher(cashttp.NewClient([]string{"http://127.0.0.1:18080"}))

	// 本地缺失：按 CID 从 A 按需补齐并逐块校验。
	got, err := nodeB.Get(ctx, cid)
	must(err)
	if !bytes.Equal(got, content) {
		log.Fatal("content verification failed")
	}
	fmt.Println("nodeB fetched and verified content from nodeA")

	// 容量压力：写入更多对象触发自动 LRU 回收；被 pin 的内容在 A 上永不删除，
	// B 上被回收的内容也可随时再次从 A 重新获取。
	for i := 0; i < 3; i++ {
		_, err = nodeB.Put(bytes.Repeat([]byte{byte('A' + i)}, 500000))
		if err != nil {
			// 容量被无法驱逐的内容占满时得到明确的容量错误。
			fmt.Printf("put %d capacity result: %v\n", i, err)
		}
	}
	again, err := nodeB.Get(ctx, cid)
	must(err)
	fmt.Printf("nodeB re-fetch after GC verified=%v\n", bytes.Equal(again, content))

	st, _ := nodeB.Stats()
	fmt.Printf("nodeB stats: usage=%d max=%d manifests=%d pins=%d\n",
		st.UsageBytes, st.MaxBytes, st.ManifestCount, st.PinnedCount)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
