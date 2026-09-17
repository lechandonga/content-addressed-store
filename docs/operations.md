# 运维、配置与验证

## 配置项（cas.Config）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `ChunkSize` | 1 MiB | 新写入内容的分块字节数；互操作节点应使用相同值 |
| `MaxObjectSize` | 64 MiB | 单次 Put/Get 内容大小上限 |
| `MaxBytes` | 0（不限） | chunks+manifests 已提交字节上限 |
| `HighWatermark` | 0.90 | 用量达到该比例触发回收 |
| `LowWatermark` | 0.70 | 回收目标水位（须 ≤ 高水位） |
| `QuarantineTTL` | 24h | 隔离文件保留时长；负数永久保留 |
| `Clock` | time.Now | 注入时钟（测试用） |

## 常用 API

- `Open(root, cfg)` / `Close()`
- `Put([]byte) (cid, error)`
- `Get(ctx, cid) ([]byte, error)`
- `Pin / Unpin / Resolve / Pins`
- `Has(cid)`、`Stat(cid)`、`Stats()`
- `Collect(ctx, GCConfig{MakeRoom, EvictAll}) (GCReport, error)`
- `SetFetcher(cashttp.NewClient(peers))`

## 可观测性

- `Stats()`：`UsageBytes / MaxBytes / ManifestCount / PinnedCount / Quarantine*`。
- 每次回收返回 `GCReport`，建议记录 `ReclaimedBytes`、`Quarantined` 与水位变化。
- `quarantine/` 中的文件名带时间戳、损坏类型（corrupt-chunk / corrupt-manifest /
  incomplete-manifest / unknown-* / corrupt-ref），可用于定位静默损坏来源。

## 故障排查

| 现象 | 可能原因 | 处理 |
| --- | --- | --- |
| `corrupt_chunk` | 磁盘静默损坏/对端给错字节 | 文件已隔离；配置健康对端后重试即可自愈 |
| `missing_chunk` | 分块本地缺失且对端也没有 | 补齐持有该内容的对端，或从备份恢复 |
| `corrupt_manifest` | 清单被篡改/布局不一致 | 清单已隔离；从可信对端重新获取 |
| `capacity` | 达到上限且无法回收（被 pin 占满） | 提高 `MaxBytes`、Unpin 或扩容 |
| `ref_conflict` | 同名引用改指向不同内容 | 设计为显式失败；先 Unpin 再重新 Pin |

## 自动化测试与验证方法

```bash
go test ./...                                   # 全量单元测试
CGO_ENABLED=1 go test -race ./...               # 竞态检测
go test -race -count=N ./...                    # 重复运行暴露偶发竞争
go test -run TestCorrupt ./...                  # 仅损坏/自愈相关
go test -run TestConcurrent ./...               # 并发获取/读写竞争
go run ./cmd/casdemo                            # 端到端双节点演示
```

测试覆盖（对应需求）：

- **数据损坏与缺失**
  - `TestCorruptChunkQuarantinedAndFetched`：分块篡改 → 隔离 + 对端自愈；
  - `TestMissingChunkRecoveredFromPeer`：删除分块 → 明确缺失 + 对端补齐；
  - `TestCorruptManifestFetched`：清单篡改 → 隔离 + 重新获取；
  - `TestTamperedPeerRejected` / `TestMaliciousPeerRejected`：恶意/错误对端内容被本地哈希拒绝；
  - `TestContentNotFoundAnywhere`：本地与对端都没有 → `ErrNotFound`；
  - `TestErrorKindSentinels`：失败类别可机读区分。
- **并发读取与回收竞争**
  - `TestConcurrentReadAndGC`：读 / 强制 GC / Pin-Unpin 三方高并发，断言无损坏读取、被引用内容不丢；
  - `TestStressPinRace`：多轮高强度重复，验证“获取→引用”无回收窗口（建议配合 `-race -count`）；
  - `TestConcurrentFetchSingleflight`：并发获取同一内容，清单恰好拉取一次、落盘仅一份；
  - `TestConcurrentPutSameContentNoDuplicates` / `TestConcurrentPutDistinctContents`：并发写去重与记账一致。
- **容量受限回收**
  - `TestPinnedContentNeverCollected`：高水位触发回收但 pin 内容完好；
  - `TestUnpinnedEvictedByLRUAndRefetch`：LRU 驱逐最旧内容并可重新获取；
  - `TestSharedChunksRefcount`：共享分块引用计数，不误删；
  - `TestHardCapacityRejectsWhenPinnedFills`：被 pin 占满时明确 `ErrCapacity`；
  - `TestFetchRespectsCapacityAndSelfHeals`：获取也遵守容量，扩容后可成功；
  - `TestManualGCReclaimsOrphansAndTmp`、`TestGCCorruptFilesQuarantined`、
    `TestGCReportUsageConsistency`：孤儿/tmp/损坏清理与用量一致性。
- **分发协议**（`cashttp` 包）：正常服务、状态码、对端回退、恶意对端拒绝、
  路径穿越拒绝、并发获取去重。

## 历史数据

无需任何迁移：v1 目录可直接由当前版本 `Open` 挂载，启动时扫描校准用量。
