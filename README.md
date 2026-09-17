# 内容寻址存储（Content-Addressed Store）

一个支持**内容分块与可验证标识、跨节点按需分发、引用保留与容量回收**的本地存储库。
节点在并发读写、部分数据损坏或容量受限的情况下，仍能保证：

- **内容可验证**：每个分块与整条内容都由其字节哈希（SHA-256）标识；读取时逐块校验，
  任何损坏、缺失、被篡改的数据都不会作为有效内容返回，并给出可区分的失败原因。
- **内容可恢复**：本地缺失/损坏的分块可通过 HTTP 从其他节点按需补齐；回收后再次访问
  会自动重新获取并重新校验，而不是返回损坏残留。
- **内容可安全回收**：被引用（pin）的内容绝不回收；分块按引用计数去重删除；
  回收与并发读取/新增引用之间不存在竞争窗口。

## 安装与要求

- Go 1.25+（仅使用标准库 + Go 工具链，无第三方运行时依赖）
- 模块路径：`github.com/lechandonga/content-addressed-store`

```bash
go build ./...
go test ./...            # 单元与并发测试
CGO_ENABLED=1 go test -race ./...   # 竞态检测
go run ./cmd/casdemo     # 双节点分发 + 回收演示
```

## 快速上手

```go
s, _ := cas.Open("/var/lib/cas", cas.Config{
    ChunkSize:     1 << 20, // 1 MiB 分块
    MaxBytes:      64 << 20,
    HighWatermark: 0.90,
    LowWatermark:  0.70,
})
defer s.Close()

cid, _ := s.Put(content)                       // 写入，得到内容标识 CID
data, err := s.Get(ctx, cid)                   // 读取（逐块校验，缺失自动补齐）

s.Pin(ctx, "report/2026-09.pdf", cid)          // 引用保留：被引用内容不被回收
addr, _ := s.Resolve("report/2026-09.pdf")     // 通过名字解析
s.Unpin("report/2026-09.pdf")                  // 解除引用（之后按 LRU 回收）

s.Collect(ctx, cas.GCConfig{})                 // 也可手动回收
```

跨节点分发基于标准库 `net/http`（仓库既有网络能力），无需额外服务框架：

```go
// 节点 A：暴露只读分发接口
http.ListenAndServe(":8080", cashttp.NewServer(nodeA))

// 节点 B：把 A 配置为对端，本地缺失时自动按需获取
nodeB.SetFetcher(cashttp.NewClient([]string{"http://node-a:8080"}))
```

## 文档索引

- [内容标识与校验规则](docs/content-verification.md)：分块、清单、CID 与失败分类
- [存储格式](docs/storage-format.md)：磁盘布局、原子写、隔离区与历史兼容
- [分发协议](docs/http-protocol.md)：HTTP v1 接口、状态码与对端回退
- [保留与回收](docs/retention-and-gc.md)：引用、引用计数、LRU、水位与并发安全
- [运维与验证](docs/operations.md)：配置项、容量行为、故障排查与测试说明

## 失败原因分类

所有错误均为 `*cas.Error`，可通过 `errors.Is` 判断类别：

| 哨兵 | 含义 |
| --- | --- |
| `cas.ErrNotFound` | 内容（清单）本地及所有对端均不存在 |
| `cas.ErrMissingChunk` | 清单存在，但某个分块无法获取 |
| `cas.ErrCorruptChunk` | 分块字节与其内容地址不一致（损坏/篡改） |
| `cas.ErrCorruptManifest` | 清单字节与地址不一致、无法解析或内部引用不匹配 |
| `cas.ErrCapacity` | 达到容量上限且回收后仍无法腾出足够空间 |
| `cas.ErrRefNotFound` / `cas.ErrRefConflict` | 引用不存在 / 同名引用指向不同内容 |
| `cas.ErrInvalid` / `cas.ErrIO` | 参数非法 / 底层 IO 错误 |

## 兼容性

- 存储目录带 `format.json` 版本标记；v1 布局下的历史数据**无需重建**即可继续读写。
- 所有提交均通过“临时文件 + 原子 rename”完成，崩溃不会留下半写入对象。
- 分块大小是写入参数：不同分块大小会产生不同的清单 CID，互操作的节点应约定同一 `ChunkSize`。
