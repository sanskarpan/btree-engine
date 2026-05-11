package engine

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"btree-engine/internal/btree"
	"btree-engine/internal/buffer"
	"btree-engine/internal/disk"
	"btree-engine/internal/events"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"
	"btree-engine/internal/recovery"
	"btree-engine/internal/wal"
)

// StorageEngine is the main entry point for the B+Tree storage engine.
// treeMu is a RWMutex: read ops (Get, Scan) hold RLock; write ops (Insert, Update,
// Delete, Rebalance, Checkpoint) hold the exclusive Lock. MVCC provides logical isolation.
type StorageEngine struct {
	cfg     Config
	dm      *disk.DiskManager
	bufPool *buffer.BufferPool
	walMgr  *wal.WALManager
	tree    *btree.BPlusTree
	tm      *mvcc.TransactionManager
	bus     *events.EventBus
	treeMu  sync.RWMutex // serializes tree write ops; allows concurrent reads
	crashed bool         // true after CrashClose; prevents double-close in Close()

	// Background checkpoint goroutine fields (P1-009).
	checkpointTicker  *time.Ticker
	checkpointDone    chan struct{}
	lastCheckpointLSN atomic.Uint64 // LSN at last successful checkpoint
	lastCheckpointAt  atomic.Int64  // Unix nanoseconds of last checkpoint
}

// OpenEngine opens (or creates) a storage engine at the given configuration.
// On open, ARIES recovery is run if there is a WAL file.
func OpenEngine(cfg Config) (*StorageEngine, error) {
	slog.Info("engine opening",
		"data_file", cfg.DataFile,
		"wal_file", cfg.WALFile,
		"buffer_pool_size", cfg.BufferPoolSize)
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}
	bus := events.NewEventBus()
	tm := mvcc.NewTransactionManager(bus)

	dm, err := disk.Open(cfg.DataFile)
	if err != nil {
		return nil, err
	}
	closeDisk := true

	walMgr, err := wal.Open(cfg.WALFile, cfg.WALBufferSize, cfg.SyncWAL, cfg.WALMaxSegmentSize, cfg.WALArchiveCommand, bus)
	if err != nil {
		_ = dm.Close()
		return nil, err
	}
	closeWAL := true

	cleanup := func() {
		if closeWAL {
			_ = walMgr.Close()
		}
		if closeDisk {
			_ = dm.Close()
		}
	}

	bp := buffer.New(cfg.BufferPoolSize, dm, walMgr, bus)

	meta, err := dm.ReadMeta()
	if err != nil {
		cleanup()
		return nil, err
	}

	tree := btree.New(meta.RootPageID, bp, walMgr, dm, tm, bus)

	// Persist root ID changes to MetaPage. WAL logging is done in createNewRoot directly.
	tree.SetRootChangeCallback(func(newRootID uint32) {
		m, err := dm.ReadMeta()
		if err == nil {
			m.RootPageID = newRootID
			dm.WriteMeta(m) //nolint
		}
	})

	eng := &StorageEngine{
		cfg:     cfg,
		dm:      dm,
		bufPool: bp,
		walMgr:  walMgr,
		tree:    tree,
		tm:      tm,
		bus:     bus,
	}

	// Run ARIES recovery
	rm := recovery.New(walMgr, bp, dm, tm, bus)
	if err := rm.Recover(); err != nil {
		cleanup()
		return nil, err
	}

	// Re-read MetaPage after recovery — root ID may have been updated by redo.
	metaAfter, err := dm.ReadMeta()
	if err != nil {
		cleanup()
		return nil, err
	}
	if metaAfter.RootPageID != meta.RootPageID {
		tree = btree.New(metaAfter.RootPageID, bp, walMgr, dm, tm, bus)
		tree.SetRootChangeCallback(func(newRootID uint32) {
			m, err := dm.ReadMeta()
			if err == nil {
				m.RootPageID = newRootID
				dm.WriteMeta(m) //nolint
			}
		})
		eng.tree = tree
	}

	if cfg.CheckpointInterval > 0 {
		eng.startBackgroundCheckpoint(cfg.CheckpointInterval)
	}
	closeWAL = false
	closeDisk = false

	return eng, nil
}

// startBackgroundCheckpoint launches the background checkpoint goroutine.
func (e *StorageEngine) startBackgroundCheckpoint(interval time.Duration) {
	e.checkpointTicker = time.NewTicker(interval)
	e.checkpointDone = make(chan struct{})
	go func() {
		for {
			select {
			case <-e.checkpointTicker.C:
				e.maybeCheckpoint()
			case <-e.checkpointDone:
				return
			}
		}
	}()
}

// maybeCheckpoint runs a checkpoint if there are dirty pages. Skips if clean.
func (e *StorageEngine) maybeCheckpoint() {
	dpt := e.bufPool.DirtyPageTable()
	if len(dpt) == 0 {
		return // nothing to checkpoint
	}
	start := time.Now()
	lsn, err := e.Checkpoint()
	if err != nil {
		slog.Error("background checkpoint failed", "err", err)
		return
	}
	e.lastCheckpointLSN.Store(lsn)
	e.lastCheckpointAt.Store(time.Now().UnixNano())
	dur := time.Since(start)
	slog.Info("background checkpoint complete",
		"lsn", lsn,
		"dirty_pages_at_ckpt", len(dpt),
		"duration_ms", dur.Milliseconds())
	if e.bus != nil {
		e.bus.Publish(events.Event{
			Type: events.EvtWALCheckpoint,
			Extra: map[string]interface{}{
				"lsn":         lsn,
				"dpt_size":    len(dpt),
				"duration_ns": dur.Nanoseconds(),
			},
		})
	}
}

// LastCheckpointLSN returns the WAL LSN of the most recent successful checkpoint.
func (e *StorageEngine) LastCheckpointLSN() uint64 {
	return e.lastCheckpointLSN.Load()
}

// LastCheckpointAt returns the Unix nanosecond timestamp of the last checkpoint, or 0.
func (e *StorageEngine) LastCheckpointAt() int64 {
	return e.lastCheckpointAt.Load()
}

// Close flushes the WAL, writes a checkpoint, and closes file handles gracefully.
func (e *StorageEngine) Close() error {
	if e.crashed {
		return nil // already crash-closed; nothing to flush
	}

	// Stop background checkpoint before final flush to avoid concurrent access.
	if e.checkpointTicker != nil {
		e.checkpointTicker.Stop()
		close(e.checkpointDone)
	}

	dpt := e.bufPool.DirtyPageTable()
	slog.Info("engine closing gracefully",
		"dirty_pages", len(dpt),
		"active_txns", e.tm.ActiveCount())

	// Write a final checkpoint on clean close.
	if _, err := e.Checkpoint(); err != nil {
		slog.Warn("final checkpoint on close failed", "err", err)
	}

	if err := e.bufPool.FlushDirtyPages(); err != nil {
		_ = e.walMgr.Close()
		_ = e.dm.Close()
		return err
	}
	if err := e.walMgr.Close(); err != nil {
		_ = e.dm.Close()
		return err
	}
	return e.dm.Close()
}

// CrashClose simulates a crash by closing file handles without flushing.
// Only used in tests.
func (e *StorageEngine) CrashClose() {
	slog.Warn("engine crash-close invoked (test/admin only)")
	e.crashed = true
	_ = e.walMgr.CrashClose()
	_ = e.dm.Close()
}

// Checkpoint writes a WAL checkpoint record containing DPT and ATT snapshots.
func (e *StorageEngine) Checkpoint() (uint64, error) {
	e.treeMu.Lock()
	defer e.treeMu.Unlock()

	dpt := e.bufPool.DirtyPageTable()
	att := make(map[uint64]wal.ATTEntry)
	for txnID, info := range e.tm.ActiveTxnSnapshot() {
		att[uint64(txnID)] = wal.ATTEntry{
			Status:  uint8(info.Status),
			LastLSN: info.LastLSN,
		}
	}

	lsn, err := e.walMgr.WriteCheckpoint(dpt, att, uint64(e.tm.NextTxnID()))
	if err != nil {
		return 0, err
	}

	if e.bus != nil {
		e.bus.Publish(events.Event{Type: events.EvtWALCheckpoint,
			Extra: map[string]interface{}{"lsn": lsn, "dpt_size": len(dpt), "att_size": len(att)}})
	}
	return lsn, nil
}

// Begin starts a new transaction.
func (e *StorageEngine) Begin(iso mvcc.IsolationLevel) *mvcc.Transaction {
	txn := e.tm.Begin(iso)
	e.walMgr.LogBeginTxn(uint64(txn.ID))
	return txn
}

// Commit commits a transaction.
// For Serializable transactions a final SSI check is performed before commit;
// if the transaction is the pivot of a write-skew cycle, it is aborted and
// mvcc.ErrSerializationFailure is returned.
func (e *StorageEngine) Commit(txn *mvcc.Transaction) error {
	if txn.IsoLevel == mvcc.Serializable {
		if err := e.tm.SIREADMgr.CheckCommit(txn.ID); err != nil {
			// Abort the transaction so its SIREAD locks are released.
			e.walMgr.Abort(uint64(txn.ID), txn.LastLSN)
			_ = e.tm.Abort(txn)
			return err
		}
	}
	lsn := e.walMgr.CommitGrouped(uint64(txn.ID), txn.LastLSN, e.cfg.GroupCommitDelay)
	txn.LastLSN = lsn
	return e.tm.Commit(txn)
}

// Abort rolls back a transaction.
func (e *StorageEngine) Abort(txn *mvcc.Transaction) error {
	e.walMgr.Abort(uint64(txn.ID), txn.LastLSN)
	return e.tm.Abort(txn)
}

// Savepoint creates a named savepoint in the given transaction.
// Returns an error if a savepoint with that name already exists.
func (e *StorageEngine) Savepoint(txn *mvcc.Transaction, name string) error {
	for _, sp := range txn.Savepoints {
		if sp.Name == name {
			return fmt.Errorf("savepoint %q already exists", name)
		}
	}
	txn.Savepoints = append(txn.Savepoints, mvcc.SavepointEntry{
		Name:        name,
		UndoLogMark: len(txn.UndoLog),
		LastLSN:     txn.LastLSN,
	})
	return nil
}

// RollbackToSavepoint undoes all operations performed after the named savepoint,
// within the same transaction.  The transaction remains active.
// Returns ErrSavepointNotFound if the savepoint does not exist.
func (e *StorageEngine) RollbackToSavepoint(txn *mvcc.Transaction, name string) error {
	e.treeMu.Lock()
	defer e.treeMu.Unlock()

	spIdx := -1
	for i, sp := range txn.Savepoints {
		if sp.Name == name {
			spIdx = i
			break
		}
	}
	if spIdx < 0 {
		return fmt.Errorf("savepoint %q not found", name)
	}
	sp := txn.Savepoints[spIdx]

	// Apply undo in reverse order from the end of the undo log to the savepoint mark.
	for i := len(txn.UndoLog) - 1; i >= sp.UndoLogMark; i-- {
		rec := txn.UndoLog[i]
		var clrLSN uint64
		if e.walMgr != nil && rec.WALSN != 0 {
			if original, err := e.walMgr.FetchRecord(rec.WALSN); err == nil {
				clrPayload := wal.EncodeCLRPayload(rec.WALSN, original.Type, original.Payload)
				clrLSN = e.walMgr.AppendRecord(wal.LogRecord{
					Type:    wal.LogCLR,
					TxnID:   uint64(txn.ID),
					PageID:  rec.PageID,
					PrevLSN: txn.LastLSN,
					Payload: clrPayload,
				})
				txn.LastLSN = clrLSN
			}
		}
		if err := e.tree.ApplyUndoRecord(txn, &rec, clrLSN); err != nil {
			return err
		}
	}

	// Truncate undo log and remove this savepoint and all subsequent ones.
	txn.UndoLog = txn.UndoLog[:sp.UndoLogMark]
	txn.Savepoints = txn.Savepoints[:spIdx]
	return nil
}

// Put inserts or updates a key-value pair.
func (e *StorageEngine) Put(txn *mvcc.Transaction, key, value []byte) error {
	e.treeMu.Lock()
	defer e.treeMu.Unlock()
	if txn.IsoLevel == mvcc.ReadCommitted {
		txn.Snapshot = e.tm.TakeSnapshot(txn.ID)
	}
	err := e.tree.Update(txn, key, value)
	if err == btree.ErrKeyNotFound {
		return e.tree.Insert(txn, key, value)
	}
	return err
}

// Get returns the visible value for a key.
// Under ReadCommitted, the snapshot is refreshed before each read so the
// transaction sees the latest committed data (non-repeatable reads allowed).
func (e *StorageEngine) Get(txn *mvcc.Transaction, key []byte) ([]byte, error) {
	e.treeMu.RLock()
	defer e.treeMu.RUnlock()
	if txn.IsoLevel == mvcc.ReadCommitted {
		txn.Snapshot = e.tm.TakeSnapshot(txn.ID)
	}
	return e.tree.Get(txn, key)
}

// Update explicitly updates a key (must exist).
func (e *StorageEngine) Update(txn *mvcc.Transaction, key, newValue []byte) error {
	e.treeMu.Lock()
	defer e.treeMu.Unlock()
	if txn.IsoLevel == mvcc.ReadCommitted {
		txn.Snapshot = e.tm.TakeSnapshot(txn.ID)
	}
	return e.tree.Update(txn, key, newValue)
}

// Delete deletes a key.
func (e *StorageEngine) Delete(txn *mvcc.Transaction, key []byte) error {
	e.treeMu.Lock()
	defer e.treeMu.Unlock()
	if txn.IsoLevel == mvcc.ReadCommitted {
		txn.Snapshot = e.tm.TakeSnapshot(txn.ID)
	}
	return e.tree.Delete(txn, key)
}

// Scan returns a cursor for range queries [start, end] (both bounds inclusive).
// Note: the cursor holds a pin on the current leaf page; callers must call Close().
// Cursor iteration is not serialized by treeMu; callers must not modify the tree
// concurrently with cursor traversal.
func (e *StorageEngine) Scan(txn *mvcc.Transaction, start, end []byte) (*btree.BTreeCursor, error) {
	e.treeMu.RLock()
	defer e.treeMu.RUnlock()
	if txn.IsoLevel == mvcc.ReadCommitted {
		txn.Snapshot = e.tm.TakeSnapshot(txn.ID)
	}
	return e.tree.Scan(txn, start, end)
}

// EventBus returns the engine's event bus for subscribing to events.
func (e *StorageEngine) EventBus() *events.EventBus {
	return e.bus
}

// TakeSnapshot exposes the transaction manager snapshot builder for gateway inspection endpoints.
func (e *StorageEngine) TakeSnapshot(txnID mvcc.TxnID) *mvcc.Snapshot {
	return e.tm.TakeSnapshot(txnID)
}

// EngineStats is a snapshot of key engine metrics.
type EngineStats struct {
	BufferHitRate float64 `json:"buffer_hit_rate"`
	DirtyPages    int     `json:"dirty_pages"`
	TotalFrames   int     `json:"total_frames"`
	ActiveTxns    int     `json:"active_txns"`
	WALSizeBytes  int64   `json:"wal_size_bytes"`
	WALSegments   int     `json:"wal_segment_count"`
	WALOldestLSN  uint64  `json:"wal_oldest_lsn"`
	WALCurrentLSN uint64  `json:"wal_current_lsn"`
}

// FrameInfo describes a single buffer pool frame.
type FrameInfo struct {
	PageID   uint32 `json:"page_id"`
	Dirty    bool   `json:"dirty"`
	PinCount int32  `json:"pin_count"`
	RecLSN   uint64 `json:"rec_lsn"`
	Empty    bool   `json:"empty"`
}

// TreeNode is a node in the B+Tree structure response.
type TreeNode struct {
	PageID   uint32      `json:"page_id"`
	Type     string      `json:"type"`
	NumSlots int         `json:"num_slots"`
	FillPct  float64     `json:"fill_pct"`
	PrevSib  uint32      `json:"prev_sib,omitempty"`
	NextSib  uint32      `json:"next_sib,omitempty"`
	Keys     []string    `json:"keys,omitempty"`
	Children []*TreeNode `json:"children,omitempty"`
}

// SlotInfo describes a single slot in a page.
type SlotInfo struct {
	SlotIdx    int    `json:"slot_idx"`
	Xmin       uint64 `json:"xmin"`
	Xmax       uint64 `json:"xmax"`
	Key        string `json:"key"`
	Value      string `json:"value"`
	XminStatus string `json:"xmin_status"`
	XmaxStatus string `json:"xmax_status"`
}

// PageContentsResult is the decoded contents of a single page.
type PageContentsResult struct {
	PageID    uint32     `json:"page_id"`
	Type      string     `json:"type"`
	NumSlots  int        `json:"num_slots"`
	FreeSpace int        `json:"free_space"`
	PageLSN   uint64     `json:"page_lsn"`
	PrevSib   uint32     `json:"prev_sib"`
	NextSib   uint32     `json:"next_sib"`
	Slots     []SlotInfo `json:"slots"`
}

// VersionInfo is one MVCC version of a key.
type VersionInfo struct {
	Xmin       uint64 `json:"xmin"`
	Xmax       uint64 `json:"xmax"`
	Value      string `json:"value"`
	XminStatus string `json:"xmin_status"`
	XmaxStatus string `json:"xmax_status"`
}

// VersionVisibilityInfo describes whether a stored tuple version is visible to a transaction.
type VersionVisibilityInfo struct {
	Xmin       uint64 `json:"xmin"`
	Xmax       uint64 `json:"xmax"`
	Value      string `json:"value"`
	XminStatus string `json:"xmin_status"`
	XmaxStatus string `json:"xmax_status"`
	Visible    bool   `json:"visible"`
	Reason     string `json:"reason"`
}

// WALRecordInfo is a summary of a single WAL record.
type WALRecordInfo struct {
	LSN     uint64 `json:"lsn"`
	PrevLSN uint64 `json:"prev_lsn"`
	TxnID   uint64 `json:"txn_id"`
	PageID  uint32 `json:"page_id"`
	Type    string `json:"type"`
}

// Stats returns key engine metrics.
func (e *StorageEngine) Stats() EngineStats {
	dpt := e.bufPool.DirtyPageTable()
	return EngineStats{
		BufferHitRate: e.bufPool.HitRate(),
		DirtyPages:    len(dpt),
		TotalFrames:   e.bufPool.FrameCount(),
		ActiveTxns:    e.tm.ActiveCount(),
		WALSizeBytes:  e.walMgr.WALSizeBytes(),
		WALSegments:   e.walMgr.SegmentCount(),
		WALOldestLSN:  e.walMgr.OldestLSN(),
		WALCurrentLSN: e.walMgr.CurrentLSN(),
	}
}

// Frames returns info about all buffer pool frames.
func (e *StorageEngine) Frames() []FrameInfo {
	infos := make([]FrameInfo, e.bufPool.FrameCount())
	for i, f := range e.bufPool.SnapshotFrames() {
		infos[i] = FrameInfo{
			PageID:   f.PageID,
			Dirty:    f.Dirty,
			PinCount: f.PinCount,
			RecLSN:   f.RecLSN,
			Empty:    f.PageID == 0,
		}
	}
	return infos
}

// WALTail returns the last n WAL records.
func (e *StorageEngine) WALTail(n int) []WALRecordInfo {
	all := make([]WALRecordInfo, 0)
	_ = e.walMgr.ScanFrom(0, func(r wal.LogRecord) {
		all = append(all, walRecordToInfo(r))
	})
	if len(all) > n {
		all = all[len(all)-n:]
	}
	return all
}

func walRecordToInfo(r wal.LogRecord) WALRecordInfo {
	names := map[wal.LogRecordType]string{
		wal.LogBegin: "BEGIN", wal.LogCommit: "COMMIT", wal.LogAbort: "ABORT",
		wal.LogInsert: "INSERT", wal.LogDelete: "DELETE", wal.LogUpdate: "UPDATE",
		wal.LogSplit: "SPLIT", wal.LogMerge: "MERGE", wal.LogCLR: "CLR",
		wal.LogCheckpoint: "CHECKPOINT", wal.LogPageAlloc: "PAGE_ALLOC",
		wal.LogPageFree: "PAGE_FREE", wal.LogRootChange: "ROOT_CHANGE",
	}
	typeName := names[r.Type]
	if typeName == "" {
		typeName = "UNKNOWN"
	}
	return WALRecordInfo{
		LSN:     r.LSN,
		PrevLSN: r.PrevLSN,
		TxnID:   r.TxnID,
		PageID:  r.PageID,
		Type:    typeName,
	}
}

func txnStatusName(s mvcc.TxnStatus) string {
	switch s {
	case mvcc.TxnCommitted:
		return "committed"
	case mvcc.TxnAborted:
		return "aborted"
	default:
		return "active"
	}
}

// TreeStructure returns the tree structure starting from root.
func (e *StorageEngine) TreeStructure() *TreeNode {
	return e.buildTreeNode(e.tree.RootPageID())
}

func (e *StorageEngine) buildTreeNode(pageID uint32) *TreeNode {
	frame, err := e.bufPool.FetchPage(pageID)
	if err != nil {
		return nil
	}
	p := e.bufPool.ReadPageData(frame)
	e.bufPool.UnpinPage(frame.PageID, false)

	hdr := p.DecodeHeader()
	used := int(hdr.FreeStart) - page.HeaderSize
	total := page.PageSize - page.HeaderSize
	if total == 0 {
		total = 1
	}

	node := &TreeNode{
		PageID:   pageID,
		NumSlots: p.NumSlots(),
		FillPct:  float64(used) / float64(total) * 100,
		PrevSib:  hdr.PrevSib,
		NextSib:  hdr.NextSib,
		Keys:     []string{},
		Children: []*TreeNode{},
	}

	if hdr.Type == page.PageTypeInternal {
		node.Type = "internal"
		ip := &btree.InternalPage{Page: p}
		seenChildren := make(map[uint32]struct{})
		addChild := func(childID uint32) {
			if childID == 0 {
				return
			}
			if _, exists := seenChildren[childID]; exists {
				return
			}
			seenChildren[childID] = struct{}{}
			child := e.buildTreeNode(childID)
			if child != nil {
				node.Children = append(node.Children, child)
			}
		}

		// Leftmost child pointer.
		addChild(hdr.PrevSib)
		// Extract separator keys and children
		for i := 0; i < p.NumSlots(); i++ {
			data := p.GetRecord(i)
			if data == nil {
				continue
			}
			rec, err := btree.DecodeInternalRecord(data)
			if err != nil {
				continue
			}
			node.Keys = append(node.Keys, string(rec.Key))
			addChild(rec.ChildID)
		}
		// Rightmost child
		rc := ip.RightmostChild()
		addChild(rc)
	} else {
		node.Type = "leaf"
		// Sample first few keys
		for i := 0; i < p.NumSlots() && i < 5; i++ {
			data := p.GetRecord(i)
			if data == nil {
				continue
			}
			t, err := mvcc.DecodeTuple(data)
			if err != nil {
				continue
			}
			node.Keys = append(node.Keys, string(t.Key))
		}
	}
	return node
}

// PageContents returns the decoded contents of a page.
func (e *StorageEngine) PageContents(pageID uint32) (*PageContentsResult, error) {
	frame, err := e.bufPool.FetchPage(pageID)
	if err != nil {
		return nil, err
	}
	p := e.bufPool.ReadPageData(frame)
	e.bufPool.UnpinPage(frame.PageID, false)

	hdr := p.DecodeHeader()
	var typeStr string
	switch hdr.Type {
	case page.PageTypeInternal:
		typeStr = "internal"
	case page.PageTypeLeaf:
		typeStr = "leaf"
	case page.PageTypeMeta:
		typeStr = "meta"
	default:
		typeStr = "free"
	}

	res := &PageContentsResult{
		PageID:    pageID,
		Type:      typeStr,
		NumSlots:  p.NumSlots(),
		FreeSpace: p.FreeSpace(),
		PageLSN:   hdr.PageLSN,
		PrevSib:   hdr.PrevSib,
		NextSib:   hdr.NextSib,
		Slots:     []SlotInfo{},
	}

	if hdr.Type == page.PageTypeLeaf {
		for i := 0; i < p.NumSlots(); i++ {
			data := p.GetRecord(i)
			if data == nil {
				continue
			}
			t, err := mvcc.DecodeTuple(data)
			if err != nil {
				continue
			}
			xminStat := txnStatusName(e.tm.GetStatus(mvcc.TxnID(t.Xmin)))
			xmaxStat := ""
			if t.Xmax != 0 {
				xmaxStat = txnStatusName(e.tm.GetStatus(mvcc.TxnID(t.Xmax)))
			}
			res.Slots = append(res.Slots, SlotInfo{
				SlotIdx:    i,
				Xmin:       t.Xmin,
				Xmax:       t.Xmax,
				Key:        string(t.Key),
				Value:      string(t.Value),
				XminStatus: xminStat,
				XmaxStatus: xmaxStat,
			})
		}
	} else if hdr.Type == page.PageTypeInternal {
		for i := 0; i < p.NumSlots(); i++ {
			data := p.GetRecord(i)
			if data == nil {
				continue
			}
			rec, err := btree.DecodeInternalRecord(data)
			if err != nil {
				continue
			}
			res.Slots = append(res.Slots, SlotInfo{
				SlotIdx: i,
				Key:     string(rec.Key),
				Value:   fmt.Sprintf("child:%d", rec.ChildID),
			})
		}
	}
	return res, nil
}

// RunVacuum performs a single synchronous vacuum pass over all leaf pages.
// It is exposed for the btree-admin CLI; the background vacuum loop in the
// server uses its own VacuumWorker goroutine.
func (e *StorageEngine) RunVacuum() {
	scanner := btree.NewLeafScanner(e.tree, e.bufPool)
	vcfg := mvcc.VacuumConfig{
		Interval:      0,
		DeadThreshold: e.cfg.VacuumThreshold,
	}
	vw := mvcc.NewVacuumWorker(scanner, e.tm, vcfg, e.bus)
	vw.SetBloomRebuildHook(func(pageID uint32, keys [][]byte) {
		e.tree.BloomRebuild(pageID, keys)
	})
	vw.RunOnce()
}

// MVCCVersions returns all stored versions of a key.
func (e *StorageEngine) MVCCVersions(key []byte) []VersionInfo {
	frame, parents, err := e.tree.FindLeafForInspection(key)
	if err != nil {
		return nil
	}
	for _, pf := range parents {
		e.bufPool.UnpinPage(pf.PageID, false)
	}
	p := e.bufPool.ReadPageData(frame)
	e.bufPool.UnpinPage(frame.PageID, false)

	lp := &btree.LeafPage{Page: p}
	tuples := lp.FindAllVersions(key)

	versions := make([]VersionInfo, 0)
	for _, t := range tuples {
		xmaxStat := ""
		if t.Xmax != 0 {
			xmaxStat = txnStatusName(e.tm.GetStatus(mvcc.TxnID(t.Xmax)))
		}
		versions = append(versions, VersionInfo{
			Xmin:       t.Xmin,
			Xmax:       t.Xmax,
			Value:      string(t.Value),
			XminStatus: txnStatusName(e.tm.GetStatus(mvcc.TxnID(t.Xmin))),
			XmaxStatus: xmaxStat,
		})
	}
	return versions
}

// MVCCVisibilityForTxn evaluates all stored versions of key against txn's current snapshot.
func (e *StorageEngine) MVCCVisibilityForTxn(key []byte, txn *mvcc.Transaction) []VersionVisibilityInfo {
	frame, parents, err := e.tree.FindLeafForInspection(key)
	if err != nil {
		return nil
	}
	for _, pf := range parents {
		e.bufPool.UnpinPage(pf.PageID, false)
	}
	p := e.bufPool.ReadPageData(frame)
	e.bufPool.UnpinPage(frame.PageID, false)

	lp := &btree.LeafPage{Page: p}
	tuples := lp.FindAllVersions(key)

	versions := make([]VersionVisibilityInfo, 0, len(tuples))
	for _, t := range tuples {
		xminStatus := e.tm.GetStatus(mvcc.TxnID(t.Xmin))
		xmaxStatus := mvcc.TxnStatus(0)
		xmaxStatusName := ""
		if t.Xmax != 0 {
			xmaxStatus = e.tm.GetStatus(mvcc.TxnID(t.Xmax))
			xmaxStatusName = txnStatusName(xmaxStatus)
		}
		visible, reason := explainVisibility(txn.Snapshot, xminStatus, xmaxStatus, t)
		versions = append(versions, VersionVisibilityInfo{
			Xmin:       t.Xmin,
			Xmax:       t.Xmax,
			Value:      string(t.Value),
			XminStatus: txnStatusName(xminStatus),
			XmaxStatus: xmaxStatusName,
			Visible:    visible,
			Reason:     reason,
		})
	}
	return versions
}

func explainVisibility(
	snap *mvcc.Snapshot,
	xminStatus mvcc.TxnStatus,
	xmaxStatus mvcc.TxnStatus,
	t *mvcc.MVCCTuple,
) (bool, string) {
	if mvcc.TxnID(t.Xmin) == snap.CreatorTxnID {
		goto checkXmax
	}
	if xminStatus != mvcc.TxnCommitted {
		return false, "creating transaction is not committed"
	}
	if mvcc.TxnID(t.Xmin) >= snap.XmaxSnapshot {
		return false, "version was created after this snapshot"
	}
	for _, xid := range snap.XipList {
		if mvcc.TxnID(t.Xmin) == xid {
			return false, "creating transaction was still in progress at snapshot time"
		}
	}

checkXmax:
	if t.Xmax == 0 {
		return true, "visible: tuple is not deleted"
	}
	if mvcc.TxnID(t.Xmax) == snap.CreatorTxnID {
		return false, "invisible: deleted by this transaction"
	}
	if xmaxStatus == mvcc.TxnAborted {
		return true, "visible: deleting transaction aborted"
	}
	if mvcc.TxnID(t.Xmax) >= snap.XmaxSnapshot {
		return true, "visible: deletion happened after this snapshot"
	}
	for _, xid := range snap.XipList {
		if mvcc.TxnID(t.Xmax) == xid {
			return true, "visible: deleting transaction was still in progress at snapshot time"
		}
	}
	return false, "invisible: deleted before this snapshot"
}
