package cas_test

import (
	"context"
	"fmt"
	"log"

	"github.com/lechandonga/content-addressed-store"
)

// Example 演示内容的写入、按 CID 校验读取、引用保留与回收。
func Example() {
	ctx := context.Background()

	// 1) 打开一个带容量上限的节点（目录不存在会自动按 v1 布局初始化）。
	s, err := cas.Open("/tmp/cas-example", cas.Config{
		ChunkSize:     4096,     // 4 KiB 分块
		MaxBytes:      64 << 20, // 本地最多保留 64 MiB 已提交字节
		HighWatermark: 0.90,     // 用量达 90% 时自动触发回收
		LowWatermark:  0.70,     // 回收后尽量降到 70% 以下
	})
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 2) 写入内容，得到其内容标识（CID = 清单字节的 sha256）。
	content := []byte("hello, content-addressed world")
	cid, err := s.Put(content)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("cid:", cid.Valid())

	// 3) 读取时逐块 + 整体校验；任何损坏都会得到可区分的错误类别。
	got, err := s.Get(ctx, cid)
	if err != nil {
		// 可通过 errors.Is(err, cas.ErrCorruptChunk) 等区分失败原因：
		// ErrNotFound / ErrMissingChunk / ErrCorruptChunk /
		// ErrCorruptManifest / ErrCapacity。
		log.Fatal(err)
	}
	_ = got

	// 4) 用命名引用（pin）保留关键内容：被引用的内容不会被回收。
	if err := s.Pin(ctx, "greeting/latest", cid); err != nil {
		log.Fatal(err)
	}
	resolved, err := s.Resolve("greeting/latest")
	if err != nil || resolved != cid {
		log.Fatalf("resolve mismatch")
	}

	// 5) 不再需要保留时解除引用；内容会在下次容量回收时按 LRU 处理。
	if err := s.Unpin("greeting/latest"); err != nil {
		log.Fatal(err)
	}

	// 6) 也可以手动触发一次回收（通常 Put 在容量压力下会自动触发）。
	if _, err := s.Collect(ctx, cas.GCConfig{}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok")
	// Output:
	// cid: true
	// ok
}
