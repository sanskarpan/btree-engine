package btree

import (
	"errors"

	"btree-engine/internal/buffer"
	"btree-engine/internal/events"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"
	"btree-engine/internal/wal"
)

// ErrKeyDeleted is returned when a key exists but all visible versions are deleted.
var ErrKeyDeleted = errors.New("key deleted")

// ErrWriteConflict is returned when a write-write conflict is detected under
// Snapshot Isolation: another committed transaction already modified the key.
// The caller should abort and retry the transaction.
var ErrWriteConflict = errors.New("write conflict: key modified by a concurrent committed transaction")

// ErrSerializationFailure is re-exported from mvcc for callers that only import btree.
var ErrSerializationFailure = mvcc.ErrSerializationFailure

// DiskAllocator is the subset of the disk manager the B+Tree needs.
type DiskAllocator interface {
	AllocPage() (uint32, error)
	FreePage(pageID uint32) error
}

// BPlusTree is the top-level B+Tree coordinating the buffer pool, WAL, and disk.
type BPlusTree struct {
	rootPageID uint32
	bufPool    *buffer.BufferPool
	walMgr     *wal.WALManager
	disk       DiskAllocator
	tm         *mvcc.TransactionManager
	bus        *events.EventBus
	bloom      *BloomIndex
	// onRootChange is called (if non-nil) whenever the root page ID changes.
	// The engine uses this to persist the new root ID durably.
	onRootChange func(uint32)
}

// New creates a BPlusTree rooted at the given page ID.
func New(rootPageID uint32, bp *buffer.BufferPool, w *wal.WALManager, disk DiskAllocator,
	tm *mvcc.TransactionManager, bus *events.EventBus) *BPlusTree {
	return &BPlusTree{
		rootPageID: rootPageID,
		bufPool:    bp,
		walMgr:     w,
		disk:       disk,
		tm:         tm,
		bus:        bus,
		bloom:      newBloomIndex(),
	}
}

// BloomRebuild rebuilds the bloom filter for a leaf page from an explicit key list.
// Called by the engine's RunVacuum to keep the bloom filter consistent after dead
// tuple removal.
func (bt *BPlusTree) BloomRebuild(pageID uint32, keys [][]byte) {
	bt.bloom.Rebuild(pageID, keys)
}

// rebuildPageBloom rebuilds the bloom filter for a leaf page from its current records.
func (bt *BPlusTree) rebuildPageBloom(pageID uint32, p *page.Page) {
	var keys [][]byte
	for i := 0; i < p.NumSlots(); i++ {
		data := p.GetRecord(i)
		if data == nil {
			continue
		}
		k := mvcc.TupleKeyExtract(data)
		if k != nil {
			keys = append(keys, k)
		}
	}
	bt.bloom.Rebuild(pageID, keys)
}

// SetRootChangeCallback registers a callback invoked whenever the root page changes.
func (bt *BPlusTree) SetRootChangeCallback(fn func(uint32)) {
	bt.onRootChange = fn
}

// RootPageID returns the current root page ID.
func (bt *BPlusTree) RootPageID() uint32 { return bt.rootPageID }

// findLeafWithParents traverses the tree top-down, returning the leaf frame
// and the list of parent frames (root first). The leaf frame is NOT unpinned by this call;
// caller must unpin it. Parent frames are also pinned; caller must unpin them too.
func (bt *BPlusTree) findLeafWithParents(key []byte) (*buffer.Frame, []*buffer.Frame, error) {
	frame, err := bt.bufPool.FetchPage(bt.rootPageID)
	if err != nil {
		return nil, nil, err
	}
	p := bt.bufPool.ReadPageData(frame)

	var parents []*buffer.Frame
	for p.Type() == page.PageTypeInternal {
		ip := &InternalPage{p}
		childID := ip.FindChild(key)
		if childID == 0 {
			childID = ip.RightmostChild()
		}
		parents = append(parents, frame)
		frame, err = bt.bufPool.FetchPage(childID)
		if err != nil {
			// Unpin parents
			for _, pf := range parents {
				bt.bufPool.UnpinPage(pf.PageID, false)
			}
			return nil, nil, err
		}
		p = bt.bufPool.ReadPageData(frame)
	}
	return frame, parents, nil
}

// Get returns the visible value for key in the given transaction.
// Under Serializable isolation a SIREAD lock is acquired on the key so that
// concurrent writers can detect rw-anti-dependencies.
func (bt *BPlusTree) Get(txn *mvcc.Transaction, key []byte) ([]byte, error) {
	frame, parents, err := bt.findLeafWithParents(key)
	if err != nil {
		return nil, err
	}
	defer func() {
		bt.bufPool.UnpinPage(frame.PageID, false)
		for _, p := range parents {
			bt.bufPool.UnpinPage(p.PageID, false)
		}
	}()

	// Bloom filter early-exit: if the filter says the key is definitely absent, skip the scan.
	if !bt.bloom.MayContain(frame.PageID, key) {
		return nil, ErrKeyNotFound
	}

	p := bt.bufPool.ReadPageData(frame)
	lp := &LeafPage{p}

	_, tuple := lp.FindVisibleSlot(key, txn.Snapshot, bt.tm)
	if tuple == nil {
		return nil, ErrKeyNotFound
	}
	// IsVisible already correctly checks Xmax via snapshot rules (Cases 5-9).
	// A visible tuple with Xmax set means the deletion is NOT visible to this snapshot
	// (e.g. deleter committed after our snapshot), so the value is still readable.

	// SSI: acquire SIREAD lock so concurrent writers can detect anti-dependencies.
	if txn.IsoLevel == mvcc.Serializable && bt.tm != nil {
		bt.tm.SIREADMgr.AcquireRead(txn.ID, key)
	}
	return tuple.Value, nil
}

// Insert adds a new key-value tuple under the given transaction.
func (bt *BPlusTree) Insert(txn *mvcc.Transaction, key, value []byte) error {
	// SSI: check for rw-anti-dependencies on the key being written.
	if txn.IsoLevel == mvcc.Serializable && bt.tm != nil {
		if err := bt.tm.SIREADMgr.CheckAndRecordWrite(txn.ID, key); err != nil {
			return err
		}
	}

	frame, parents, err := bt.findLeafWithParents(key)
	if err != nil {
		return err
	}

	tuple := &mvcc.MVCCTuple{
		Xmin:  uint64(txn.ID),
		Xmax:  0,
		Key:   key,
		Value: value,
	}
	encoded := tuple.Encode()

	// WAL: log the insert BEFORE modifying the page
	var lsn uint64
	if bt.walMgr != nil {
		lsn = bt.walMgr.LogInsertRecord(uint64(txn.ID), uint64(frame.PageID), txn.LastLSN, encoded)
		txn.LastLSN = lsn
	}

	p := bt.bufPool.ReadPageData(frame)
	if lsn != 0 {
		p.SetPageLSN(lsn)
	}
	lp := &LeafPage{p}

	slotIdx, insertErr := lp.InsertRecord(encoded, mvcc.TupleKeyExtract)
	if errors.Is(insertErr, page.ErrPageFull) {
		parentIDs := framePageIDs(parents)
		// Copy modified page back and mark dirty before split
		bt.bufPool.WritePageData(frame.PageID, p)
		bt.bufPool.UnpinPage(frame.PageID, true)
		for _, pf := range parents {
			bt.bufPool.UnpinPage(pf.PageID, false)
		}
		return bt.splitLeaf(txn, frame.PageID, parentIDs, key, tuple)
	}
	if insertErr != nil {
		bt.bufPool.UnpinPage(frame.PageID, false)
		for _, pf := range parents {
			bt.bufPool.UnpinPage(pf.PageID, false)
		}
		return insertErr
	}

	bt.bufPool.WritePageData(frame.PageID, p)
	bt.bufPool.UnpinPage(frame.PageID, true)
	for _, pf := range parents {
		bt.bufPool.UnpinPage(pf.PageID, false)
	}

	// Update bloom filter for this leaf page.
	bt.bloom.Add(frame.PageID, key)

	// Record undo entry for savepoint rollback support.
	txn.UndoLog = append(txn.UndoLog, mvcc.UndoRecord{
		PageID:  frame.PageID,
		SlotIdx: slotIdx,
		Op:      mvcc.UndoInsert,
		WALSN:   lsn,
	})

	if bt.bus != nil {
		bt.bus.Publish(events.Event{Type: events.EvtTupleInsert,
			Extra: map[string]interface{}{
				"key": string(key), "txn_id": txn.ID, "lsn": lsn,
			}})
	}
	return nil
}

func framePageIDs(frames []*buffer.Frame) []uint32 {
	ids := make([]uint32, len(frames))
	for i, f := range frames {
		ids[i] = f.PageID
	}
	return ids
}

// Update performs a logical delete of the old version and inserts a new version.
func (bt *BPlusTree) Update(txn *mvcc.Transaction, key, newValue []byte) error {
	// SSI: check for rw-anti-dependencies on the key being written.
	if txn.IsoLevel == mvcc.Serializable && bt.tm != nil {
		if err := bt.tm.SIREADMgr.CheckAndRecordWrite(txn.ID, key); err != nil {
			return err
		}
	}

	// Find and mark old version as deleted
	frame, parents, err := bt.findLeafWithParents(key)
	if err != nil {
		return err
	}
	p := bt.bufPool.ReadPageData(frame)
	lp := &LeafPage{p}

	slotIdx, oldTuple := lp.FindVisibleSlot(key, txn.Snapshot, bt.tm)
	if oldTuple == nil {
		bt.bufPool.UnpinPage(frame.PageID, false)
		for _, pf := range parents {
			bt.bufPool.UnpinPage(pf.PageID, false)
		}
		return ErrKeyNotFound
	}

	// Write-write conflict detection: if the visible tuple already has Xmax set
	// by a *different* committed transaction, we have a lost-update conflict.
	if oldTuple.Xmax != 0 && mvcc.TxnID(oldTuple.Xmax) != txn.ID &&
		bt.tm.GetStatus(mvcc.TxnID(oldTuple.Xmax)) == mvcc.TxnCommitted {
		bt.bufPool.UnpinPage(frame.PageID, false)
		for _, pf := range parents {
			bt.bufPool.UnpinPage(pf.PageID, false)
		}
		return ErrWriteConflict
	}

	// WAL log the delete
	var lsn uint64
	if bt.walMgr != nil {
		lsn = bt.walMgr.LogDeleteRecord(uint64(txn.ID), uint64(frame.PageID), txn.LastLSN, slotIdx, oldTuple.Xmax)
		txn.LastLSN = lsn
		p.SetPageLSN(lsn)
	}

	// Set Xmax in-place
	lp.SetXmax(slotIdx, uint64(txn.ID))

	bt.bufPool.WritePageData(frame.PageID, p)
	bt.bufPool.UnpinPage(frame.PageID, true)
	for _, pf := range parents {
		bt.bufPool.UnpinPage(pf.PageID, false)
	}

	// Record undo entry for the SetXmax operation (undo = clear Xmax).
	txn.UndoLog = append(txn.UndoLog, mvcc.UndoRecord{
		PageID:  frame.PageID,
		SlotIdx: slotIdx,
		OldXmax: oldTuple.Xmax,
		Op:      mvcc.UndoDelete,
		WALSN:   lsn,
	})

	// Insert new version (will add its own UndoInsert entry).
	return bt.Insert(txn, key, newValue)
}

// Delete marks the visible tuple for key as deleted (sets Xmax = txn.ID).
func (bt *BPlusTree) Delete(txn *mvcc.Transaction, key []byte) error {
	// SSI: check for rw-anti-dependencies on the key being written.
	if txn.IsoLevel == mvcc.Serializable && bt.tm != nil {
		if err := bt.tm.SIREADMgr.CheckAndRecordWrite(txn.ID, key); err != nil {
			return err
		}
	}

	frame, parents, err := bt.findLeafWithParents(key)
	if err != nil {
		return err
	}
	p := bt.bufPool.ReadPageData(frame)
	lp := &LeafPage{p}

	slotIdx, visible := lp.FindVisibleSlot(key, txn.Snapshot, bt.tm)
	if visible == nil {
		bt.bufPool.UnpinPage(frame.PageID, false)
		for _, pf := range parents {
			bt.bufPool.UnpinPage(pf.PageID, false)
		}
		return ErrKeyNotFound
	}

	// Write-write conflict detection (same as Update).
	if visible.Xmax != 0 && mvcc.TxnID(visible.Xmax) != txn.ID &&
		bt.tm.GetStatus(mvcc.TxnID(visible.Xmax)) == mvcc.TxnCommitted {
		bt.bufPool.UnpinPage(frame.PageID, false)
		for _, pf := range parents {
			bt.bufPool.UnpinPage(pf.PageID, false)
		}
		return ErrWriteConflict
	}

	var lsn uint64
	if bt.walMgr != nil {
		lsn = bt.walMgr.LogDeleteRecord(uint64(txn.ID), uint64(frame.PageID), txn.LastLSN, slotIdx, visible.Xmax)
		txn.LastLSN = lsn
		p.SetPageLSN(lsn)
	}

	lp.SetXmax(slotIdx, uint64(txn.ID))

	// Record undo entry for the SetXmax operation (undo = clear Xmax).
	txn.UndoLog = append(txn.UndoLog, mvcc.UndoRecord{
		PageID:  frame.PageID,
		SlotIdx: slotIdx,
		OldXmax: visible.Xmax,
		Op:      mvcc.UndoDelete,
		WALSN:   lsn,
	})

	bt.bufPool.WritePageData(frame.PageID, p)
	bt.bufPool.UnpinPage(frame.PageID, true)
	if bt.bus != nil {
		bt.bus.Publish(events.Event{Type: events.EvtTupleDelete,
			Extra: map[string]interface{}{"key": string(key), "txn_id": txn.ID, "lsn": lsn}})
	}
	return bt.handleUnderflow(txn, frame.PageID, parents)
}

// ApplyUndoRecord reverses a single UndoRecord for savepoint rollback.
// UndoInsert: marks the inserted tuple as deleted by setting Xmax = txnID.
// UndoDelete: clears the Xmax on the previously-deleted tuple, restoring visibility.
func (bt *BPlusTree) ApplyUndoRecord(txn *mvcc.Transaction, rec *mvcc.UndoRecord, clrLSN uint64) error {
	frame, err := bt.bufPool.FetchPage(rec.PageID)
	if err != nil {
		return err
	}
	p := bt.bufPool.ReadPageData(frame)
	lp := &LeafPage{p}

	switch rec.Op {
	case mvcc.UndoInsert:
		lp.SetXmax(rec.SlotIdx, uint64(txn.ID))
	case mvcc.UndoDelete:
		lp.SetXmax(rec.SlotIdx, rec.OldXmax)
	}
	if clrLSN != 0 {
		p.SetPageLSN(clrLSN)
	}

	bt.bufPool.WritePageData(frame.PageID, p)
	bt.bufPool.UnpinPage(frame.PageID, true)
	return nil
}

// FindLeafForInspection returns the leaf frame and parent chain for key, for read-only inspection.
// The caller is responsible for unpinning all returned frames.
func (bt *BPlusTree) FindLeafForInspection(key []byte) (*buffer.Frame, []*buffer.Frame, error) {
	return bt.findLeafWithParents(key)
}

// LeafScanner adapts a BPlusTree for use as a mvcc.PageScanner (required by VacuumWorker).
type LeafScanner struct {
	tree    *BPlusTree
	bufPool *buffer.BufferPool
}

// NewLeafScanner creates a LeafScanner that satisfies the mvcc.PageScanner interface.
func NewLeafScanner(bt *BPlusTree, bp *buffer.BufferPool) *LeafScanner {
	return &LeafScanner{tree: bt, bufPool: bp}
}

// FirstLeafPageID returns the leftmost leaf page ID by traversing from the root.
func (ls *LeafScanner) FirstLeafPageID() uint32 {
	rootID := ls.tree.RootPageID()
	frame, err := ls.bufPool.FetchPage(rootID)
	if err != nil {
		return 0
	}
	p := ls.bufPool.ReadPageData(frame)
	for p.Type() == page.PageTypeInternal {
		childID := p.PrevSib()
		ls.bufPool.UnpinPage(frame.PageID, false)
		if childID == 0 {
			return 0
		}
		frame, err = ls.bufPool.FetchPage(childID)
		if err != nil {
			return 0
		}
		p = ls.bufPool.ReadPageData(frame)
	}
	id := frame.PageID
	ls.bufPool.UnpinPage(id, false)
	return id
}

// ReadLeafPage fetches and returns the page for reading/writing.
func (ls *LeafScanner) ReadLeafPage(pageID uint32) *page.Page {
	frame, err := ls.bufPool.FetchPage(pageID)
	if err != nil {
		return nil
	}
	return ls.bufPool.ReadPageData(frame)
}

// WriteLeafPage writes back a modified leaf page.
func (ls *LeafScanner) WriteLeafPage(p *page.Page) {
	ls.bufPool.WritePageData(p.ID, p)
}

// UnpinLeaf releases the pin on a leaf page.
func (ls *LeafScanner) UnpinLeaf(pageID uint32, dirty bool) {
	ls.bufPool.UnpinPage(pageID, dirty)
}
