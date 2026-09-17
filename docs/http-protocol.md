# 跨节点分发协议（HTTP v1）

基于仓库既有网络能力（Go 标准库 `net/http`），无额外依赖。

## 路由

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/cas/v1/manifests/{addr}` | 读取清单原始字节（已校验） |
| GET | `/cas/v1/chunks/{addr}` | 读取分块原始字节（已校验） |
| HEAD | 同上 | 探测存在性，返回 200 + `Content-Length`，无响应体 |
| 任意 | `/cas/v1/` | 版本探针，返回 `cas v1` |

- `{addr}` 必须是完整的 `sha256-<hex>`；服务端拒绝任何含 `/ \ ? #` 的形式，
  加上 http.Client 默认会规整 `/../`，共同杜绝目录穿越。
- 成功响应头：`X-CAS-Addr`、`X-CAS-Kind`、`Content-Type: application/octet-stream`。
- 服务端通过 `OpenManifest/OpenChunk` 读取，**只对外分发哈希校验通过的字节**；
  损坏内容不会流出本节点。

## 错误响应

统一 JSON：`{"error": "<code>", "message": "..."}`

| HTTP | error code | 对应 cas 错误 | 客户端行为 |
| --- | --- | --- | --- |
| 404 | `not_found` | `ErrNotFound` | 尝试下一个对端；全部没有则失败 |
| 404 | `missing_chunk` | `ErrMissingChunk` | 尝试下一个对端 |
| 422 | `corrupt` | `ErrCorrupt*` | 该对端内容不可信，尝试下一个对端 |
| 400 | `invalid` | `ErrInvalid` | 参数问题，终止 |
| 503 | `capacity` | `ErrCapacity` | 可重试/换对端 |
| 500 | `io_error` | `ErrIO` | 可重试 |

## 客户端（cashttp.Client 实现 cas.Fetcher）

- `NewClient(peers)` 按顺序尝试对端，直到取到有效内容；全部失败才报错。
- 对**每个响应体独立做 SHA-256 校验**：即便 TLS/对端可信，只要字节与地址不符
  就视为损坏并回退到下一个对端（防御对端静默损坏或被篡改）。
- `MaxBlobBytes`（默认 64 MiB）限制单个 blob 响应大小，防止内存膨胀。
- 可自定义 `*http.Client`（超时、连接复用、TLS 等）。

## 按需获取与并发去重

本地 `Get` 的获取路径：

1. 本地快速路径：清单与全部分块齐全 ⇒ 逐块校验拼接，零网络。
2. 否则进入慢路径：清单、分块逐个从对端获取（网络 IO 在 GC 锁外），
   本地对每个字节做哈希复核。
3. 取齐后在**同一个 gcMu 读锁临界区**内“先分块、最后清单”原子提交。
4. 多个并发 `Get/Pin` 同一内容：
   - 整内容由 singleflight 合并为一次装配；
   - 单个分块/清单的远程获取由 blob singleflight 合并为**一次网络请求**；
   - 落盘由 `commitIfAbsent` 去重。
   因此并发获取同一内容不会产生重复副本，也不会相互覆盖。

## 挂载示例

```go
mux := http.NewServeMux()
mux.Handle("/cas/v1/", cashttp.NewServer(store))
http.ListenAndServe(":8080", mux)
```

`NewServer` 返回的对象本身也实现了 `http.Handler`，可直接 `http.ListenAndServe(":8080", cashttp.NewServer(store))`。
