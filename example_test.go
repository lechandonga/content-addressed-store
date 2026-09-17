package cas_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	cas "github.com/lechandonga/content-addressed-store"
)

// 演示：写入、钉住、跨节点按需获取、容量回收与损坏可区分错误。
func Example() {
	ctx := context.Background()

	dirA, err := os.MkdirTemp("", "cas-a-")
	if err != nil {
		log.Fatal(err)
	}
	dirB, err := os.MkdirTemp("", "cas-b-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dirA)
	defer os.RemoveAll(dirB)

	// 节点 A（数据源）。
	nodeA, err := cas.OpenNode(dirA, cas.Config{ChunkSize: 64, MaxBytes: 1 << 20}, cas.NodeOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer nodeA.Close()

	content := []byte("the quick brown fox jumps over the lazy dog. content-addressed!")
	id, err := nodeA.PutBytes(ctx, content)
	if err != nil {
		log.Fatal(err)
	}
	if err := nodeA.Pin(ctx, "demo/v1", id); err != nil {
		log.Fatal(err)
	}

	// 节点 B 以进程内对端从 A 按需获取（生产中可换成 cas.NewHTTPPeer）。
	nodeB, err := cas.OpenNode(dirB, cas.Config{ChunkSize: 64, MaxBytes: 4096, HighWatermark: 0.8},
		cas.NodeOptions{Peers: []cas.Peer{cas.NewLocalPeer("a", nodeA)}})
	if err != nil {
		log.Fatal(err)
	}
	defer nodeB.Close()

	got, err := nodeB.GetBytes(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("fetched=%v\n", string(got) == string(content))

	// 不存在的内容返回可区分的 ErrNotFound。
	_, err = nodeB.GetBytes(ctx, cas.ContentID("cas1-0000000000000000000000000000000000000000000000000000000000000000"))
	fmt.Printf("missing=%v\n", errors.Is(err, cas.ErrNotFound))

	// 主动回收：无引用内容被清理，被 Pin 的内容保留。
	if _, err := nodeB.GC(ctx, cas.GCOptions{Full: true}); err != nil {
		log.Fatal(err)
	}
	// nodeB 从未 Pin 过该内容，因此已回收；再次访问会自动重新从 A 获取。
	again, err := nodeB.GetBytes(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("refetched=%v\n", string(again) == string(content))

	// Output:
	// fetched=true
	// missing=true
	// refetched=true
}
