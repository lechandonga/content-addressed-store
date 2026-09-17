# 内容寻址存储（Content-Addressed Store）

零外部依赖（仅 Go 标准库）的内容寻址数据分发与保留库。节点在**并发读写、部分数据
损坏/被篡改、容量受限**的条件下，仍保证内容**可验证、可恢复、可安全回收**。

## 1. 数据模型与内容标识

- 内容按固定大小**分块**（默认 256 KiB，可配置，范围 16B～64MiB）。
- 每个分块以其字节的 SHA-256 摘要作为标识：

  ```
  cas1c-<64 位十六进制 SHA-256>
  ```

- 内容由一份**清单（Manifest）**绑定：按顺序记录每个分块的标识与大小、内容总长度、
  分块大小、哈希算法与格式版本。清单以规范化紧凑 JSON 编码（外层信封格式
  `cas-manifest-v1`），其字节的 SHA-256 即**内容标识**：

  ```
  cas1-<64 位十六进制 SHA-256>
  ```

内容标识是自描述的（带版本与算法前缀），分块标识与内容标识不会互相混淆；
将来升级哈希算法可引入新版本前缀而不破坏历史数据。

## 2. 校验规则（任何坏数据都不会作为有效内容返回）

验证链条自上而下三层，缺一不可：

1. **内容标识 → 清单**：读取清单时重算 `SHA256(规范化清单编码)`，必须等于 ContentID。
2. **清单 → 分块序列**：校验清单结构自洽（版本、哈希、长度等于各分块大小之和、
   非末块大小必须等于 ChunkSize、每个 ChunkID 合法）。
3. **分块标识 → 分块字节**：每个分块在**交付前整读校验**长度与 SHA-256，必须等于 ChunkID。

分块在交付给调用方之前就完成整分块校验并以已校验字节提供，因此调用方永远不会先收到
正确前缀、后半截才发现损坏。

失败原因通过类型化错误 `cas.Error.Kind` 区分，可直接用 `errors.Is` 判定：

| 哨兵错误 | 含义 |
| --- | --- |
| `cas.ErrCorruptChunk` | 分块摘要不符（损坏/被篡改） |
| `cas.ErrCorruptManifest` | 清单摘要不符（损坏/被篡改） |
| `cas.ErrInvalidManifest` | 清单结构非法、不自洽 |
| `cas.ErrMissingChunk` | 清单引用的分块本地与所有对端都没有 |
| `cas.ErrNotFound` | 内容本地与所有对端都没有 |
| `cas.ErrContentMismatch` | 重组内容与清单整体绑定不一致 |
| `cas.ErrRefExists` / `cas.ErrRefNotFound` | 命名引用冲突 / 不存在 |
| `cas.ErrCapacityExceeded` | 容量硬上限无法满足 |
| `cas.ErrNetwork` | 对端网络/协议层失败 |

## 3. 磁盘格式（v1，历史数据无需重建）

打开目录时根据磁盘现状**重建内存索引**，因此崩溃、拷贝目录、进程异常退出后状态仍然
一致；旧版本数据可直接继续读写。

```
<dir>/
├── chunks/<xx>/cas1c-<sha256>     内容分块（xx 为摘要前两位分片）
├── manifests/<xx>/cas1-<sha256>   清单字节（文件内容的 SHA-256 即 ContentID）
├── refs/<name>                    命名引用（Pin），文件内容为 ContentID
├── meta/lru.json                  LRU 顺序（从旧到新），崩溃后仅影响淘汰次序
└── **/.inflight/.cas-tmp-*        原子写暂存（目标分片目录内隐藏子目录）
```

- 所有写入采用“目标目录内 `.inflight` 暂存文件 + 同目录 `rename`”原子落盘，
  并发写同一内容寻址对象（字节必然相同）幂等去重，磁盘上只保留一份。
- `.inflight` 仅在打开时清扫崩溃残留；运行期间 GC 只枚举分片目录的直接数据文件，
  不会误删并发提交中的暂存文件。

## 4. 跨节点分发

- 节点通过 `cas.Peer` 接口按需获取：本地缺失的清单/分块自动从对端顺序拉取，
  首个校验通过的副本被幂等落盘；任何对端给的字节都要在本地重算摘要，**篡改数据
  绝不写入**。
- 标准库自带 HTTP 传输：`cas.HTTPHandler(node)` 暴露无状态 GET/HEAD 服务，
  `cas.NewHTTPPeer(name, baseURL)` 作为对端；服务端在整个响应期间持有对象的
  生命周期保护，并发 GC 不会截断正在传输的响应。
- **single-flight 合并**：多个并发获取同一清单/分块只产生一次实际拉取，共享结果，
  不会产生重复副本或相互覆盖。等待者同样持有该对象的共享保护，因此拉取进行中
  GC 不会回收它。
- 回收后的再次访问：清单/分块已被回收时，读取链路自动重新从对端获取，
  而不是返回陈旧或损坏数据。

## 5. 保留与回收（Pin / GC）

- **命名引用（Pin）是保留的根**：`Pin/CreatePin/Unpin`。被任一引用指向的内容
  及其（含与其他内容共享的）分块绝不回收；引用以原子文件持久化。
- GC 为**标记—清除**，且全库同一时刻只运行一个 GC：
  1. 快照索引与引用集合，枚举损坏清单、孤立分块与非法垃圾文件；
  2. **两阶段标记**：先标记候选淘汰清单，活跃（读/写/拉取/建立引用中）的清单跳过；
     仅基于“本轮真正会删除的清单”计算分块存活集——被跳过清单引用的分块一律保留；
     再标记待删分块，活跃分块跳过；
  3. 每个对象的实际删除在 guardian 独占下进行，与普通访问互斥；
  4. 新访问若撞上本轮回收，在 guardian 处等待本轮提交，随后以“本地缺失→自动重取”
     的路径继续，不会观察到半截数据。
- 孤立分块（无任何清单引用）、损坏清单、暂存残留与非预期文件每次 GC 都会清理，
  不残留不可回收垃圾。
- 无引用内容默认作为 **LRU 缓存**保留；`GC(GCOptions{Full:true})` 或配置
  `EvictUnreferenced:true` 时主动回收全部无引用内容。

## 6. 容量配置

`cas.Config`：

| 字段 | 说明 |
| --- | --- |
| `ChunkSize` | 新内容分块大小；0 用默认 256 KiB |
| `MaxBytes` | 容量硬上限（字节），0 表示不限 |
| `HighWatermark` | 高水位比例 `(0,1]`，默认 0.9；超过 `MaxBytes*比例` 后写入/拉取触发 LRU 回收 |
| `SyncWrites` | 落盘是否 fsync + 同步父目录（更强崩溃一致性，吞吐更低） |
| `EvictUnreferenced` | true 时每次 GC 都回收全部无引用内容（严格保留模式） |

容量行为：

- 超水位时按 LRU 从最旧开始淘汰**无引用**内容，直至回落到水位以下；被 Pin 的
  内容即使最旧也受保护。
- 若“仅保留被引用内容 + 本次真正新增的字节”都超过 `MaxBytes`，写入返回
  `cas.ErrCapacityExceeded`，本次新增分块回滚，已有引用与读取不受影响。
- 写入与拉取路径都会在提交后自动做容量回收；容量压力下对外读取行为保持可用与一致。

## 7. 快速开始

```go
import "github.com/lechandonga/content-addressed-store"

func main() {
    ctx := context.Background()

    // 节点 A：容量 1 GiB、高水位 0.9
    a, _ := cas.OpenNode("./data-a", cas.Config{MaxBytes: 1 << 30}, cas.NodeOptions{})
    defer a.Close()

    id, _ := a.PutBytes(ctx, []byte("hello content-addressed world"))

    // 建立命名引用：被引用内容不会被回收
    _ = a.Pin(ctx, "greetings/v1", id)

    // 节点 B：通过 HTTP 从 A 按需获取
    // （A 侧：http.ListenAndServe(addr, cas.HTTPHandler(a))）
    b, _ := cas.OpenNode("./data-b", cas.Config{},
        cas.NodeOptions{Peers: []cas.Peer{cas.NewHTTPPeer("a", "http://10.0.0.1:8080")}})
    defer b.Close()

    got, err := b.GetBytes(ctx, id) // 本地缺失自动拉取，逐层校验
    _, _ = got, err

    // 主动回收（容量超水位会自动触发）
    report, _ := b.GC(ctx, cas.GCOptions{})
    _ = report
}
```

## 8. 验证方法

```bash
go test ./...                 # 功能测试
go test -race ./...           # 竞态检测（需 CGO）
go test -race -count=10 ./... # 高重复度压测并发回收/读取/拉取竞争
go vet ./...
```

测试覆盖（见 `*_test.go`）：

- **损坏与篡改**：分块/清单字节被篡改、对端投毒、长度不符；断言类型化错误、
  坏数据不落盘、坏副本被隔离并从健康对端自愈；
- **缺失与分发**：删除分块、清空本地、对端 404；断言自动补取与可区分的缺失原因；
- **并发获取**：多 goroutine 同时拉取同一内容，single-flight 合并、无重复副本；
- **并发读写**：多 goroutine 并发写同一内容，内容标识一致且仅一份；
- **回收竞争**：Full GC 与并发 Get/Pin/Unpin 长时间交错（race 检测），
  被引用内容不丢、读者永远拿到正确字节、回收后再次访问自动重取；
- **容量受限**：超水位 LRU 淘汰、被 Pin 内容在容量压力下存活、硬上限拒绝新写入、
  反复 GC 收敛且不超上限；
- **持久化与兼容**：重启后引用/数据/LRU 状态保留；重启前损坏的分块在重启后自愈；
  空内容、非法 ID、非法引用名等边界。
