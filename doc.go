// Package cas 实现了一套内容寻址（content-addressed）的数据分发与保留机制。
//
// 核心能力：
//
//   - 内容按固定大小分块，每个分块以其 SHA-256 摘要作为标识（ChunkID）；
//     整体内容由清单（Manifest）绑定分块顺序与总长度，清单摘要即内容标识（ContentID）。
//   - 读取链路逐层校验：ContentID 校验清单字节、清单绑定分块 ID、每个分块读取时
//     重新计算摘要，任何损坏、缺失、篡改都不会作为有效内容返回，而是返回可区分的
//     类型化错误（见 ErrorKind）。
//   - 本地缺失的分块/清单可通过 Peer 接口（含 HTTP 传输实现）按需从其他节点拉取，
//     并发拉取同一内容通过 single-flight 合并，内容寻址天然去重，磁盘上只保留一份。
//   - 命名引用（Pin）保证被引用内容不被回收；GC 与并发读取、写入、新增引用之间通过
//     守卫（Guardian）与索引锁协作，不会误删活跃内容，也不会残留不可回收垃圾。
//   - 容量上限（MaxBytes）与高水位（HighWatermark）可配置；容量压力下按 LRU 回收
//     无引用内容，被引用（Pin）内容受保护；回收后的再次访问会自动重新从对端获取。
//
// 磁盘布局（v1）：
//
//	chunks/xx/<chunkID>      内容分块（xx 为 ID 前两位分片）
//	manifests/xx/<contentID> 清单字节（文件内容的 SHA-256 即 ContentID）
//	refs/<name>              命名引用，文件内容为 ContentID
//	tmp/                     暂存与原子重命名临时文件
//	meta/lru.json            LRU 顺序持久化
//
// 典型用法见 OpenNode、Node.Put、Node.Get、Node.Pin、Node.GC。
package cas
