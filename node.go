package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// Node 在本地 Store 之上组合按需分发（Peer 拉取）、single-flight 合并、
// 全链路校验与损坏自愈，是对外的主要使用入口。
type Node struct {
	store *Store

	peersMu sync.RWMutex
	peers   []Peer

	chunkFlight    *flightGroup
	manifestFlight *flightGroup
	putFlight      *flightGroup
}

// NodeOptions 用于 OpenNode 的附加选项。
type NodeOptions struct {
	// Peers 初始对端集合，可后续通过 AddPeer 动态扩展。
	Peers []Peer
}

// OpenNode 打开目录并构建节点。
func OpenNode(dir string, cfg Config, opts NodeOptions) (*Node, error) {
	st, err := Open(dir, cfg)
	if err != nil {
		return nil, err
	}
	n := &Node{
		store:          st,
		peers:          append([]Peer(nil), opts.Peers...),
		chunkFlight:    newFlightGroup(),
		manifestFlight: newFlightGroup(),
		putFlight:      newFlightGroup(),
	}
	return n, nil
}

// NewNode 在已有 Store 上构建节点（便于测试/嵌入）。
func NewNode(st *Store, peers ...Peer) *Node {
	return &Node{
		store:          st,
		peers:          append([]Peer(nil), peers...),
		chunkFlight:    newFlightGroup(),
		manifestFlight: newFlightGroup(),
		putFlight:      newFlightGroup(),
	}
}

// Store 返回底层存储（高级用法：直接 GC、统计、引用管理等）。
func (n *Node) Store() *Store { return n.store }

// Close 关闭底层存储。
func (n *Node) Close() error { return n.store.Close() }

// AddPeer 动态注册一个对端（重复注册按名字去重）。
func (n *Node) AddPeer(p Peer) {
	if p == nil {
		return
	}
	n.peersMu.Lock()
	defer n.peersMu.Unlock()
	for _, x := range n.peers {
		if x.Name() == p.Name() {
			return
		}
	}
	n.peers = append(n.peers, p)
}

// Peers 返回当前对端集合的快照。
func (n *Node) Peers() []Peer {
	n.peersMu.RLock()
	defer n.peersMu.RUnlock()
	return append([]Peer(nil), n.peers...)
}

// Put 写入内容并返回内容标识（直接落本地；相同内容并发写入经 single-flight
// 合并，保证只有一个写入流程在跑，分块/清单本身也幂等去重）。
func (n *Node) Put(ctx context.Context, r io.Reader) (ContentID, error) {
	// single-flight 需要按内容 key，但写入前不知道内容 ID：
	// 先完整读取以计算 ID（内容在内存中），相同 ID 的并发提交合并。
	data, err := io.ReadAll(r)
	if err != nil {
		return "", opError(KindUnknown, "node.put", "", err)
	}
	cid := previewContentID(data, n.store.cfg.ChunkSize)
	v, err := n.putFlight.Do(string(cid), func() (interface{}, error) {
		id, perr := n.store.Put(ctx, bytes.NewReader(data))
		return id, perr
	})
	if err != nil {
		return "", err
	}
	return v.(ContentID), nil
}

// PutBytes 写入字节切片。
func (n *Node) PutBytes(ctx context.Context, p []byte) (ContentID, error) {
	return n.Put(ctx, bytes.NewReader(p))
}

// previewContentID 仅用于 single-flight 去重键：按与 Store 相同的分块/编码
// 规则预演一遍得到内容标识。
func previewContentID(data []byte, chunkSize int) ContentID {
	cs := normalizeChunkSize(chunkSize)
	buf := make([]byte, cs)
	rd := bytes.NewReader(data)
	var refs []ChunkRef
	var total int64
	for {
		nr, err := rd.Read(buf)
		if nr > 0 {
			refs = append(refs, ChunkRef{ID: chunkIDFromBytes(buf[:nr]), Size: int64(nr)})
			total += int64(nr)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
	}
	m := &Manifest{Version: manifestVersionV1, Hash: hashAlgoSHA256,
		ChunkSize: cs, Length: total, Chunks: refs}
	raw, _ := encodeManifest(m)
	return computeContentID(raw)
}

// Get 返回内容的流式读取器与元数据。读取过程逐分块校验：
//   - 清单按 ContentID 校验（缺失/损坏时自动从对端重新获取）；
//   - 每个分块读取到 EOF 时校验长度与 SHA-256，损坏分块被隔离并重新获取一次；
//   - 任何无法恢复的损坏/缺失都会以类型化错误返回，绝不返回被污染的内容。
//
// 多个并发 Get 同一内容时，缺失部分的拉取经 single-flight 合并，
// 不会产生重复副本或相互覆盖。
func (n *Node) Get(ctx context.Context, id ContentID) (io.ReadCloser, *Manifest, error) {
	if !IsValidContentID(string(id)) {
		return nil, nil, opError(KindInvalidArgument, "node.get", string(id), nil)
	}
	m, err := n.loadManifest(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	// 清单存在即记录一次使用（影响 LRU 顺序；已引用内容不在 LRU 中）。
	n.touchLRU(id)
	cr := &contentReader{
		node: n,
		ctx:  ctx,
		id:   id,
		m:    m,
	}
	return cr, m, nil
}

// GetBytes 读取并重组全部内容（便捷方法，同样走全量校验）。
func (n *Node) GetBytes(ctx context.Context, id ContentID) ([]byte, error) {
	rc, _, err := n.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (n *Node) touchLRU(id ContentID) {
	n.store.mu.Lock()
	if _, ok := n.store.manifests[id]; ok {
		if _, pinned := n.store.refsValueLocked(id); !pinned {
			n.store.lru.touch(string(id))
			_ = n.store.persistLRULocked()
		}
	}
	n.store.mu.Unlock()
}

// loadManifest 返回已校验的清单：本地缺失/损坏时从对端拉取，
// 相同内容的并发拉取经 single-flight 合并。
func (n *Node) loadManifest(ctx context.Context, id ContentID) (*Manifest, error) {
	v, err := n.manifestFlight.Do(string(id), func() (interface{}, error) {
		return n.loadManifestOnce(ctx, id)
	})
	if err != nil {
		return nil, err
	}
	return v.(*Manifest), nil
}

// loadManifestHeld 要求调用方已持有该清单的共享保护。
func (n *Node) loadManifestHeld(ctx context.Context, id ContentID) (*Manifest, error) {
	if m, _, localErr := n.store.ReadManifest(id); localErr == nil {
		return m, nil // ReadManifest 已读全字节到内存并完成校验
	} else if !errors.Is(localErr, ErrNotFound) &&
		!errors.Is(localErr, ErrCorruptManifest) && !errors.Is(localErr, ErrInvalidManifest) {
		return nil, localErr
	}
	raw, err := n.fetchManifestFromPeers(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := n.store.commitManifest(computeContentID(raw), raw); err != nil {
		return nil, err
	}
	// 拉取同样占用本地容量：超过水位时按 LRU 回收无引用内容。
	n.maybeAutoGC(ctx)
	return verifyManifestBytes(raw, id)
}

func (n *Node) loadManifestOnce(ctx context.Context, id ContentID) (*Manifest, error) {
	// 整个“本地校验 → 对端拉取 → 落盘”过程持有清单的共享保护，
	// 杜绝“释放保护后、提交前清单恰被 GC 回收”的竞争。
	rel := n.store.guard.Acquire(string(id))
	m, _, localErr := n.store.ReadManifest(id)
	if localErr == nil {
		rel()
		return m, nil
	}
	corrupt := errors.Is(localErr, ErrCorruptManifest) || errors.Is(localErr, ErrInvalidManifest)
	if !corrupt && !errors.Is(localErr, ErrNotFound) {
		rel()
		return nil, localErr
	}
	if corrupt {
		// 隔离坏副本需要独占：先释放共享保护，隔离后重新进入保护窗口。
		rel()
		n.store.quarantineManifest(id)
		rel = n.store.guard.Acquire(string(id))
	}
	m, err := n.loadManifestHeld(ctx, id)
	rel()
	return m, err
}

// maybeAutoGC 在已用容量超过高水位时触发一次普通 GC（仅回收无引用内容/垃圾）。
func (n *Node) maybeAutoGC(ctx context.Context) {
	n.store.mu.RLock()
	over := n.store.cfg.MaxBytes > 0 && n.store.bytes > n.store.watermarkBytesLocked()
	n.store.mu.RUnlock()
	if over {
		_, _ = n.store.GC(ctx, GCOptions{})
	}
}

func (n *Node) fetchManifestFromPeers(ctx context.Context, id ContentID) ([]byte, error) {
	var lastErr error = opError(KindNotFound, "node.fetchManifest", string(id), nil)
	for _, p := range n.Peers() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := p.GetManifest(ctx, id)
		if err != nil {
			lastErr = classifyPeerErr("node.fetchManifest", string(id), p.Name(), err)
			continue
		}
		// 强制内容标识校验，篡改的清单一律拒绝。
		if _, verr := verifyManifestBytes(raw, id); verr != nil {
			lastErr = opError(KindCorruptManifest, "node.fetchManifest", string(id),
				fmt.Errorf("peer %s served tampered manifest: %w", p.Name(), verr))
			continue
		}
		return raw, nil
	}
	return nil, lastErr
}

// ensureChunk 保证分块在本地可用且字节正确（供 Verify 等不持守卫的调用方使用）：
// 获取共享保护后在同一窗口内校验/拉取。读取链路请使用 openChunkVerified，
// 它会把保护一直保持到分块交付结束。
func (n *Node) ensureChunk(ctx context.Context, id ChunkID, wantSize int64) error {
	rel := n.store.guard.Acquire(string(id))
	defer rel()
	return n.ensureChunkHeld(ctx, id, wantSize)
}

// ensureChunkHeld 要求调用方已持有该分块的共享保护。
// 本地损坏时先释放由调用方持有的保护无法在此完成，因此损坏隔离路径单独处理：
// 这里仅处理“缺失 → 拉取”；损坏由上层（openChunkVerified/ensureChunk）隔离后进入。
func (n *Node) ensureChunkHeld(ctx context.Context, id ChunkID, wantSize int64) error {
	if n.store.HasChunk(id) {
		err := n.verifyLocalChunkHeld(id, wantSize)
		if err == nil {
			return nil
		}
		// 损坏/大小不符：保留原始错误类别继续尝试对端（对端也没有时仍上报损坏）。
		if !(errors.Is(err, ErrNotFound) || errors.Is(err, ErrMissingChunk)) {
			if ferr := n.fetchChunkHeld(ctx, id, wantSize); ferr == nil {
				return nil
			} else {
				// 对端无法提供：优先返回原始的本地损坏原因。
				return err
			}
		}
	}
	return n.fetchChunkHeld(ctx, id, wantSize)
}

// fetchChunkHeld 在调用方已持有共享保护的前提下，经 single-flight 合并拉取。
// 所有参与者（含 single-flight 等待者）都持有该分块的共享保护，GC 任何时刻
// 都不会把正在拉取的分块标记为可回收。
func (n *Node) fetchChunkHeld(ctx context.Context, id ChunkID, wantSize int64) error {
	_, err := n.chunkFlight.Do(string(id), func() (interface{}, error) {
		// leader 同样是持有共享保护的参与者；双检后真正发起网络拉取。
		if n.store.HasChunk(id) {
			if err := n.verifyLocalChunkHeld(id, wantSize); err == nil {
				return nil, nil
			}
		}
		return nil, n.fetchChunkFromPeers(ctx, id, wantSize)
	})
	return err
}

// fetchChunkFromPeers 顺序尝试各对端，首个校验通过的分块被幂等落盘。
// 调用方（leader）持有该分块的共享保护。
func (n *Node) fetchChunkFromPeers(ctx context.Context, id ChunkID, wantSize int64) error {
	var lastErr error = opError(KindMissingChunk, "node.ensureChunk", string(id), nil)
	for _, p := range n.Peers() {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := p.GetChunk(ctx, id)
		if err != nil {
			lastErr = classifyPeerErr("node.ensureChunk", string(id), p.Name(), err)
			continue
		}
		if wantSize > 0 && int64(len(data)) != wantSize {
			lastErr = opError(KindCorruptChunk, "node.ensureChunk", string(id),
				fmt.Errorf("peer %s size %d != want %d", p.Name(), len(data), wantSize))
			continue
		}
		// 先验证摘要再落盘：被篡改/损坏的字节绝不写入。
		if err := verifyChunk(id, data); err != nil {
			lastErr = err
			continue
		}
		if err := n.store.commitChunk(id, data); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// verifyLocalChunk 读完整分块并校验长度与摘要（自行获取/释放共享保护）。
func (n *Node) verifyLocalChunk(id ChunkID, wantSize int64) error {
	rel := n.store.guard.Acquire(string(id))
	defer rel()
	return n.verifyLocalChunkHeld(id, wantSize)
}

// verifyLocalChunkHeld 要求调用方已持有该分块的共享保护。
func (n *Node) verifyLocalChunkHeld(id ChunkID, wantSize int64) error {
	rc, size, err := n.store.OpenChunk(id)
	if err != nil {
		return err
	}
	defer rc.Close()
	if wantSize > 0 && size != wantSize {
		return opError(KindCorruptChunk, "node.verifyChunk", string(id),
			fmt.Errorf("index size %d != manifest size %d", size, wantSize))
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		return opError(KindCorruptChunk, "node.verifyChunk", string(id), err)
	}
	return verifyChunk(id, data)
}

// openChunkVerified 在单个共享保护窗口内“确保可用、整分块预校验、交付”：
// 本地缺失时在窗口内从对端拉取补齐；本地损坏时释放保护、隔离坏副本，再以新
// 窗口重取并重试一次。返回的读取器由已校验字节支撑；release 在读完后调用，
// 交付期间 GC 无法回收该分块。
func (n *Node) openChunkVerified(ctx context.Context, id ChunkID, wantSize int64) (io.ReadCloser, int64, func(), error) {
	rel := n.store.guard.Acquire(string(id))
	data, verr := n.readAndVerifyHeld(id, wantSize)
	if verr == nil {
		return io.NopCloser(bytes.NewReader(data)), int64(len(data)), rel, nil
	}
	rel()

	if errors.Is(verr, ErrNotFound) || errors.Is(verr, ErrMissingChunk) {
		// 新窗口内拉取后交付（fetchChunkHeld 假设调用方持守卫）。
		rel2 := n.store.guard.Acquire(string(id))
		if ferr := n.fetchChunkHeld(ctx, id, wantSize); ferr != nil {
			rel2()
			return nil, 0, nil, ferr
		}
		data2, verr2 := n.readAndVerifyHeld(id, wantSize)
		if verr2 != nil {
			rel2()
			return nil, 0, nil, verr2
		}
		return io.NopCloser(bytes.NewReader(data2)), int64(len(data2)), rel2, nil
	}
	if !errors.Is(verr, ErrCorruptChunk) {
		return nil, 0, nil, verr
	}
	// 损坏：隔离坏副本后，以新窗口重取并重试一次。
	n.store.quarantineChunk(id)
	rel3 := n.store.guard.Acquire(string(id))
	if ferr := n.fetchChunkHeld(ctx, id, wantSize); ferr != nil {
		rel3()
		// 对端无法补齐：上报原始的“本地副本损坏/被篡改”原因，可明确区分于从未持有。
		return nil, 0, nil, verr
	}
	data3, verr3 := n.readAndVerifyHeld(id, wantSize)
	if verr3 != nil {
		rel3()
		return nil, 0, nil, verr3
	}
	return io.NopCloser(bytes.NewReader(data3)), int64(len(data3)), rel3, nil
}

// readAndVerifyHeld 在已持共享保护下读出整分块并校验长度/摘要。
func (n *Node) readAndVerifyHeld(id ChunkID, wantSize int64) ([]byte, error) {
	rc, _, err := n.store.OpenChunk(id)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, opError(KindCorruptChunk, "node.openChunk", string(id), err)
	}
	if wantSize > 0 && int64(len(data)) != wantSize {
		return nil, opError(KindCorruptChunk, "node.openChunk", string(id),
			fmt.Errorf("size %d != want %d", len(data), wantSize))
	}
	if err := verifyChunk(id, data); err != nil {
		return nil, err
	}
	return data, nil
}

func classifyPeerErr(op, id, peer string, err error) error {
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrMissingChunk) {
		// 该对端没有，保持原始类别由调用方尝试下一对端。
		return opError(KindNotFound, op, id, fmt.Errorf("peer %s: %w", peer, err))
	}
	if errors.Is(err, ErrCorruptChunk) {
		return err
	}
	return opError(KindNetwork, op, id, fmt.Errorf("peer %s: %w", peer, err))
}

// ---- 引用（保留）----

// Pin 将命名引用指向某内容：若本地缺失则先从对端获取（含其清单与分块），
// 引用与 GC 并发时由 guardian 保证内容不会在引用建立前被回收。
func (n *Node) Pin(ctx context.Context, name string, id ContentID) error {
	if !validRefName(name) {
		return opError(KindInvalidArgument, "node.pin", name, nil)
	}
	// 确保清单本地存在（建立保留根之前先可达）。
	if _, err := n.loadManifest(ctx, id); err != nil {
		return err
	}
	rel := n.store.guard.Acquire(string(id))
	err := n.store.SetRef(name, id)
	rel()
	if err != nil {
		return err
	}
	// Pin 改变了保留集合：若容量因此超硬上限，无法通过回收解决，
	// 但已有引用与读取仍可用（这里仅做一次尽力回收）。
	_, _ = n.store.GC(ctx, GCOptions{})
	return nil
}

// CreatePin 仅在引用名未被占用时建立引用。
func (n *Node) CreatePin(ctx context.Context, name string, id ContentID) error {
	if _, err := n.store.Resolve(name); err == nil {
		return opError(KindRefExists, "node.createPin", name, nil)
	} else if !errors.Is(err, ErrRefNotFound) {
		return err
	}
	return n.Pin(ctx, name, id)
}

// Unpin 解除命名引用，内容转入 LRU 缓存，可在容量压力下回收。
func (n *Node) Unpin(name string) error { return n.store.DeleteRef(name) }

// Resolve 解析命名引用。
func (n *Node) Resolve(name string) (ContentID, error) { return n.store.Resolve(name) }

// Verify 对本地内容做全量离线校验（不发起网络请求）：
// 校验清单摘要、每个分块的存在性与 SHA-256。缺失返回 ErrMissingChunk，
// 损坏返回 ErrCorruptChunk/ErrCorruptManifest，全部通过返回 nil。
func (n *Node) Verify(ctx context.Context, id ContentID) error {
	rel := n.store.guard.Acquire(string(id))
	m, _, err := n.store.ReadManifest(id)
	rel()
	if err != nil {
		return err
	}
	for i, c := range m.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !n.store.HasChunk(c.ID) {
			return opError(KindMissingChunk, "node.verify", string(c.ID),
				fmt.Errorf("chunk %d/%d of %s", i, len(m.Chunks), id))
		}
		if err := n.verifyLocalChunk(c.ID, c.Size); err != nil {
			return err
		}
	}
	return nil
}

// GC / Stats 转发到存储层，便于直接从 Node 使用。
func (n *Node) GC(ctx context.Context, opt GCOptions) (GCReport, error) {
	return n.store.GC(ctx, opt)
}

// Stats 返回存储统计。
func (n *Node) Stats() Stats { return n.store.Stats() }

// quarantineManifest 隔离损坏/被篡改的本地清单文件（不在索引中的损坏清单由
// GC 清理，这里主动删除以保证随后的重新获取能落到干净状态）。
func (s *Store) quarantineManifest(id ContentID) {
	s.guard.GCLock(string(id))
	defer s.guard.GCUnlock(string(id))
	if _, err := os.Stat(s.manifestPath(id)); err == nil {
		_ = os.Remove(s.manifestPath(id))
	}
	s.mu.Lock()
	if rec := s.manifests[id]; rec != nil {
		delete(s.manifests, id)
		s.bytes -= rec.rawLen
		s.lru.remove(string(id))
	}
	s.mu.Unlock()
}
