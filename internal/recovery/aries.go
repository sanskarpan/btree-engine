// Package recovery implements ARIES 3-phase crash recovery.
package recovery

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"log/slog"
	"time"

	"btree-engine/internal/buffer"
	"btree-engine/internal/disk"
	"btree-engine/internal/events"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"
	"btree-engine/internal/wal"
)

// DirtyPageTable maps pageID -> recLSN.
type DirtyPageTable map[uint32]uint64

// ATTEntry is a record in the Active Transaction Table.
type ATTEntry struct {
	Status  mvcc.TxnStatus
	LastLSN uint64
}

// ActiveTxnTable maps txnID -> ATTEntry.
type ActiveTxnTable map[uint64]ATTEntry

type analysisState struct {
	dpt       DirtyPageTable
	att       ActiveTxnTable
	statuses  map[uint64]mvcc.TxnStatus
	nextTxnID uint64
	maxTxnID  uint64
}

// RecoveryManager orchestrates ARIES 3-phase recovery.
type RecoveryManager struct {
	walMgr  *wal.WALManager
	bufPool *buffer.BufferPool
	dm      *disk.DiskManager
	tm      *mvcc.TransactionManager
	bus     *events.EventBus
}

// New creates a RecoveryManager.
func New(w *wal.WALManager, bp *buffer.BufferPool, dm *disk.DiskManager,
	tm *mvcc.TransactionManager, bus *events.EventBus) *RecoveryManager {
	return &RecoveryManager{walMgr: w, bufPool: bp, dm: dm, tm: tm, bus: bus}
}

// Recover runs the full ARIES 3-phase recovery.
func (r *RecoveryManager) Recover() error {
	start := time.Now()
	if r.bus != nil {
		r.bus.Publish(events.Event{Type: events.EvtRecoveryStart})
	}

	checkpointLSN := r.walMgr.FindLastCheckpoint()
	slog.Info("aries recovery starting", "checkpoint_lsn", checkpointLSN)

	if r.bus != nil {
		r.bus.Publish(events.Event{Type: events.EvtRecoveryAnalysis,
			Extra: map[string]interface{}{"from_lsn": checkpointLSN}})
	}
	analysisStart := time.Now()
	analysis := r.Analyze(checkpointLSN)
	dpt, att := analysis.dpt, analysis.att
	slog.Info("aries analysis complete",
		"dpt_entries", len(dpt),
		"att_entries", len(att),
		"duration_ms", time.Since(analysisStart).Milliseconds())

	// Advance TM's nextTxnID past all recovered transactions and treat any
	// older unknown txn IDs as committed unless an explicit status overrides it.
	if r.tm != nil {
		nextTxnID := analysis.nextTxnID
		if minNext := analysis.maxTxnID + 1; minNext > nextTxnID {
			nextTxnID = minNext
		}
		if nextTxnID > 0 {
			r.tm.AdvanceNextTxnID(nextTxnID)
			r.tm.SetRecoveredCommittedFloor(mvcc.TxnID(nextTxnID))
		}
	}

	if r.bus != nil {
		r.bus.Publish(events.Event{Type: events.EvtRecoveryRedo,
			Extra: map[string]interface{}{"dirty_pages": len(dpt)}})
	}
	redoStart := time.Now()
	if err := r.Redo(dpt); err != nil {
		return err
	}
	slog.Info("aries redo complete",
		"dirty_pages", len(dpt),
		"duration_ms", time.Since(redoStart).Milliseconds())

	if r.bus != nil {
		r.bus.Publish(events.Event{Type: events.EvtRecoveryUndo,
			Extra: map[string]interface{}{"active_txns": len(att)}})
	}
	undoStart := time.Now()
	if err := r.Undo(att); err != nil {
		return err
	}
	slog.Info("aries undo complete",
		"active_txns", len(att),
		"duration_ms", time.Since(undoStart).Milliseconds())

	// Restore explicit recovered statuses so visibility checks work correctly.
	if r.tm != nil {
		for txnID, status := range analysis.statuses {
			r.tm.RestoreStatus(mvcc.TxnID(txnID), status)
		}
		for txnID, entry := range att {
			if entry.Status == mvcc.TxnActive {
				r.tm.RestoreStatus(mvcc.TxnID(txnID), mvcc.TxnAborted)
			}
		}
	}

	if r.bus != nil {
		r.bus.Publish(events.Event{Type: events.EvtRecoveryDone,
			Extra: map[string]interface{}{"duration_ns": time.Since(start).Nanoseconds()}})
	}
	slog.Info("aries recovery complete", "total_duration_ms", time.Since(start).Milliseconds())
	return nil
}

// Analyze scans the WAL from checkpointLSN and builds DPT + ATT.
func (r *RecoveryManager) Analyze(checkpointLSN uint64) analysisState {
	state := analysisState{
		dpt:      make(DirtyPageTable),
		att:      make(ActiveTxnTable),
		statuses: make(map[uint64]mvcc.TxnStatus),
	}

	if checkpointLSN != 0 {
		if rec, err := r.walMgr.FetchRecord(checkpointLSN); err == nil && rec.Type == wal.LogCheckpoint {
			if ckpt, err := wal.DecodeCheckpoint(rec.Payload); err == nil {
				for pageID, lsn := range ckpt.DPT {
					state.dpt[pageID] = lsn
				}
				for txnID, entry := range ckpt.ATT {
					state.att[txnID] = ATTEntry{
						Status:  mvcc.TxnStatus(entry.Status),
						LastLSN: entry.LastLSN,
					}
					if txnID > state.maxTxnID {
						state.maxTxnID = txnID
					}
				}
				state.nextTxnID = ckpt.NextTxnID
				if ckpt.NextTxnID > 0 && ckpt.NextTxnID-1 > state.maxTxnID {
					state.maxTxnID = ckpt.NextTxnID - 1
				}
			}
		}
	}

	r.walMgr.ScanFrom(checkpointLSN, func(record wal.LogRecord) { //nolint
		if record.LSN == checkpointLSN && record.Type == wal.LogCheckpoint {
			return
		}
		if record.TxnID > state.maxTxnID {
			state.maxTxnID = record.TxnID
		}
		switch record.Type {
		case wal.LogBegin:
			state.att[record.TxnID] = ATTEntry{Status: mvcc.TxnActive, LastLSN: record.LSN}
		case wal.LogCommit:
			state.att[record.TxnID] = ATTEntry{Status: mvcc.TxnCommitted, LastLSN: record.LSN}
			state.statuses[record.TxnID] = mvcc.TxnCommitted
		case wal.LogAbort:
			state.att[record.TxnID] = ATTEntry{Status: mvcc.TxnAborted, LastLSN: record.LSN}
			state.statuses[record.TxnID] = mvcc.TxnAborted
		case wal.LogInsert, wal.LogDelete, wal.LogUpdate, wal.LogCLR:
			entry := state.att[record.TxnID]
			entry.LastLSN = record.LSN
			if entry.Status == 0 {
				entry.Status = mvcc.TxnActive
			}
			state.att[record.TxnID] = entry
			if record.PageID != 0 {
				if _, exists := state.dpt[record.PageID]; !exists {
					state.dpt[record.PageID] = record.LSN
				}
			}
		case wal.LogSplit:
			entry := state.att[record.TxnID]
			entry.LastLSN = record.LSN
			if entry.Status == 0 {
				entry.Status = mvcc.TxnActive
			}
			state.att[record.TxnID] = entry
			if _, exists := state.dpt[record.PageID]; !exists {
				state.dpt[record.PageID] = record.LSN
			}
			if len(record.Payload) >= 4 {
				rightPageID := binary.LittleEndian.Uint32(record.Payload[0:])
				if rightPageID != 0 {
					if _, exists := state.dpt[rightPageID]; !exists {
						state.dpt[rightPageID] = record.LSN
					}
				}
			}
		case wal.LogMerge:
			if record.TxnID != 0 {
				entry := state.att[record.TxnID]
				entry.LastLSN = record.LSN
				if entry.Status == 0 {
					entry.Status = mvcc.TxnActive
				}
				state.att[record.TxnID] = entry
			}
			if _, exists := state.dpt[record.PageID]; !exists {
				state.dpt[record.PageID] = record.LSN
			}
			if len(record.Payload) >= 8 {
				parentPageID := binary.LittleEndian.Uint32(record.Payload[4:])
				if parentPageID != 0 {
					if _, exists := state.dpt[parentPageID]; !exists {
						state.dpt[parentPageID] = record.LSN
					}
				}
			}
		case wal.LogRootChange:
			if record.TxnID != 0 {
				entry := state.att[record.TxnID]
				entry.LastLSN = record.LSN
				if entry.Status == 0 {
					entry.Status = mvcc.TxnActive
				}
				state.att[record.TxnID] = entry
			}
			if len(record.Payload) >= 4 {
				newRootID := binary.LittleEndian.Uint32(record.Payload[0:])
				if newRootID != 0 {
					if _, exists := state.dpt[newRootID]; !exists {
						state.dpt[newRootID] = record.LSN
					}
				}
			}
		}
	})
	return state
}

// Redo replays WAL records starting at the earliest recLSN in the DPT.
func (r *RecoveryManager) Redo(dpt DirtyPageTable) error {
	if len(dpt) == 0 {
		return nil
	}
	startLSN := minRecLSN(dpt)
	return r.walMgr.ScanFrom(startLSN, func(record wal.LogRecord) {
		switch record.Type {
		case wal.LogRootChange:
			r.applyRedoRootChange(record, dpt)
		case wal.LogSplit:
			r.applyRedoSplit(record, dpt)
		case wal.LogMerge:
			r.applyRedoMerge(record, dpt)
		case wal.LogInsert, wal.LogDelete, wal.LogUpdate:
			// skip if page not in DPT
			recLSN, inDPT := dpt[record.PageID]
			if !inDPT || record.LSN < recLSN {
				return
			}
			frame, err := r.bufPool.FetchPage(record.PageID)
			if err != nil {
				return
			}
			p := r.bufPool.ReadPageData(frame)
			h := p.DecodeHeader()
			if h.PageLSN >= record.LSN {
				r.bufPool.UnpinPage(record.PageID, false)
				return // already applied
			}
			r.applyRedo(frame, record)
			r.bufPool.UnpinPage(record.PageID, true)
		case wal.LogCLR:
			recLSN, inDPT := dpt[record.PageID]
			if !inDPT || record.LSN < recLSN {
				return
			}
			frame, err := r.bufPool.FetchPage(record.PageID)
			if err != nil {
				return
			}
			p := r.bufPool.ReadPageData(frame)
			if p.DecodeHeader().PageLSN >= record.LSN {
				r.bufPool.UnpinPage(record.PageID, false)
				return
			}
			r.applyCLRRedo(frame, record)
			r.bufPool.UnpinPage(record.PageID, true)
		}
	})
}

// applyRedo re-applies a single WAL record to a page.
func (r *RecoveryManager) applyRedo(frame *buffer.Frame, record wal.LogRecord) {
	p := r.bufPool.ReadPageData(frame)
	switch record.Type {
	case wal.LogInsert:
		// Choose the correct key extractor based on page type
		var keyExtract func([]byte) []byte
		if p.Type() == page.PageTypeInternal {
			keyExtract = recoveryInternalKeyExtract
		} else {
			keyExtract = recoveryTupleKeyExtract
		}
		p.InsertRecord(record.Payload, keyExtract) //nolint
		p.SetPageLSN(record.LSN)
		r.bufPool.WritePageData(record.PageID, p)
	case wal.LogDelete:
		if len(record.Payload) >= 4 {
			slotIdx := int(binary.LittleEndian.Uint32(record.Payload[0:]))
			data := p.GetRecord(slotIdx)
			if len(data) >= 16 {
				// Redo the delete: set Xmax to the deleting transaction.
				// The payload stores oldXmax (for undo); the new Xmax is record.TxnID.
				binary.LittleEndian.PutUint64(data[8:], record.TxnID)
			}
			p.SetPageLSN(record.LSN)
			r.bufPool.WritePageData(record.PageID, p)
		}
	}
}

func (r *RecoveryManager) applyCLRRedo(frame *buffer.Frame, record wal.LogRecord) {
	p := r.bufPool.ReadPageData(frame)
	_, originalType, originalPayload := wal.DecodeCLRPayload(record.Payload)
	switch originalType {
	case wal.LogInsert:
		targetKey := recoveryTupleKeyExtract(originalPayload)
		for i := 0; i < p.NumSlots(); i++ {
			data := p.GetRecord(i)
			if len(data) < 8 {
				continue
			}
			xmin := binary.LittleEndian.Uint64(data[0:])
			if xmin != record.TxnID {
				continue
			}
			if targetKey != nil && !bytes.Equal(recoveryTupleKeyExtract(data), targetKey) {
				continue
			}
			p.DeleteRecord(i)
			p.Compact()
			break
		}
	case wal.LogDelete:
		if len(originalPayload) >= 12 {
			slotIdx := int(binary.LittleEndian.Uint32(originalPayload[0:]))
			oldXmax := binary.LittleEndian.Uint64(originalPayload[4:])
			data := p.GetRecord(slotIdx)
			if len(data) >= 16 {
				binary.LittleEndian.PutUint64(data[8:], oldXmax)
			}
		}
	}
	p.SetPageLSN(record.LSN)
	r.bufPool.WritePageData(record.PageID, p)
}

// applyRedoSplit re-executes a leaf split during redo.
// Payload: [rightPageID:4][newTupleLen:4][newTuple]
func (r *RecoveryManager) applyRedoSplit(record wal.LogRecord, dpt DirtyPageTable) {
	if len(record.Payload) < 8 {
		return
	}
	rightPageID := binary.LittleEndian.Uint32(record.Payload[0:])
	newTupleLen := int(binary.LittleEndian.Uint32(record.Payload[4:]))
	if len(record.Payload) < 8+newTupleLen {
		return
	}
	newTuple := record.Payload[8 : 8+newTupleLen]

	// Check DPT for left page
	recLSN, inDPT := dpt[record.PageID]
	if !inDPT || record.LSN < recLSN {
		return
	}

	leftFrame, err := r.bufPool.FetchPage(record.PageID)
	if err != nil {
		return
	}
	leftPage := r.bufPool.ReadPageData(leftFrame)

	// Skip if already applied
	if leftPage.DecodeHeader().PageLSN >= record.LSN {
		r.bufPool.UnpinPage(record.PageID, false)
		return
	}

	rightFrame, err := r.bufPool.FetchPage(rightPageID)
	if err != nil {
		r.bufPool.UnpinPage(record.PageID, false)
		return
	}
	rightPage := r.bufPool.ReadPageData(rightFrame)

	// Initialise right page as leaf, inheriting sibling links
	oldNextSib := leftPage.NextSib()
	rightPage.ID = rightPageID
	rightPage.EncodeHeader(page.PageHeader{
		Type:      page.PageTypeLeaf,
		FreeStart: page.HeaderSize,
		FreeEnd:   page.PageSize,
		PrevSib:   record.PageID,
		NextSib:   oldNextSib,
	})

	// Move top half from left to right
	numSlots := leftPage.NumSlots()
	midSlot := numSlots / 2
	for i := midSlot; i < numSlots; i++ {
		data := leftPage.GetRecord(i)
		if data == nil {
			continue
		}
		rightPage.InsertRecord(data, recoveryTupleKeyExtract) //nolint
		leftPage.DeleteRecord(i)
	}
	leftPage.Compact()

	// Update sibling links
	leftPage.SetNextSib(rightPageID)
	rightPage.SetPrevSib(record.PageID)
	rightPage.SetNextSib(oldNextSib)

	// Insert the new tuple into the correct half
	if len(newTuple) > 0 {
		newKey := recoveryTupleKeyExtract(newTuple)
		var rightMinKey []byte
		if rightPage.NumSlots() > 0 {
			rightMinKey = recoveryTupleKeyExtract(rightPage.GetRecord(0))
		}
		if rightMinKey != nil && bytes.Compare(newKey, rightMinKey) >= 0 {
			rightPage.InsertRecord(newTuple, recoveryTupleKeyExtract) //nolint
		} else {
			leftPage.InsertRecord(newTuple, recoveryTupleKeyExtract) //nolint
		}
	}

	leftPage.SetPageLSN(record.LSN)
	rightPage.SetPageLSN(record.LSN)

	r.bufPool.WritePageData(record.PageID, leftPage)
	r.bufPool.WritePageData(rightPageID, rightPage)
	r.bufPool.UnpinPage(record.PageID, true)
	r.bufPool.UnpinPage(rightPageID, true)

	// Fix the old next sibling's PrevSib pointer
	if oldNextSib != 0 {
		nextFrame, err := r.bufPool.FetchPage(oldNextSib)
		if err == nil {
			nextPage := r.bufPool.ReadPageData(nextFrame)
			nextPage.SetPrevSib(rightPageID)
			r.bufPool.WritePageData(oldNextSib, nextPage)
			r.bufPool.UnpinPage(oldNextSib, true)
		}
	}
}

// applyRedoMerge re-applies a leaf merge by writing the after-images stored in the payload.
// Payload: [freedPageID:4][parentPageID:4][survivingAfterImage:pageSize][parentAfterImage:pageSize]
func (r *RecoveryManager) applyRedoMerge(record wal.LogRecord, dpt DirtyPageTable) {
	const pageSize = page.PageSize
	if len(record.Payload) < 8+pageSize+pageSize {
		return
	}
	parentPageID := binary.LittleEndian.Uint32(record.Payload[4:])
	survivingAfterImage := record.Payload[8 : 8+pageSize]
	parentAfterImage := record.Payload[8+pageSize : 8+2*pageSize]

	// Redo surviving page (record.PageID)
	recLSN, inDPT := dpt[record.PageID]
	if inDPT && record.LSN >= recLSN {
		frame, err := r.bufPool.FetchPage(record.PageID)
		if err == nil {
			p := r.bufPool.ReadPageData(frame)
			if p.DecodeHeader().PageLSN < record.LSN {
				sp := &page.Page{ID: record.PageID}
				copy(sp.Data[:], survivingAfterImage)
				r.bufPool.WritePageData(record.PageID, sp)
				r.bufPool.UnpinPage(record.PageID, true)
			} else {
				r.bufPool.UnpinPage(record.PageID, false)
			}
		}
	}

	// Redo parent page
	if parentPageID != 0 {
		recLSN, inDPT = dpt[parentPageID]
		if inDPT && record.LSN >= recLSN {
			frame, err := r.bufPool.FetchPage(parentPageID)
			if err == nil {
				p := r.bufPool.ReadPageData(frame)
				if p.DecodeHeader().PageLSN < record.LSN {
					pp := &page.Page{ID: parentPageID}
					copy(pp.Data[:], parentAfterImage)
					r.bufPool.WritePageData(parentPageID, pp)
					r.bufPool.UnpinPage(parentPageID, true)
				} else {
					r.bufPool.UnpinPage(parentPageID, false)
				}
			}
		}
	}
}

// applyRedoRootChange updates the MetaPage and reconstructs the new root internal page.
// Payload: [newRootID:4][leftChildID:4][rightChildID:4][sepKeyLen:2][sepKey]
func (r *RecoveryManager) applyRedoRootChange(record wal.LogRecord, dpt DirtyPageTable) {
	if len(record.Payload) < 4 {
		return
	}
	newRootID := binary.LittleEndian.Uint32(record.Payload[0:])

	// Always update MetaPage
	meta, err := r.dm.ReadMeta()
	if err == nil {
		meta.RootPageID = newRootID
		r.dm.WriteMeta(meta) //nolint
	}

	// Check DPT for new root page
	recLSN, inDPT := dpt[newRootID]
	if !inDPT || record.LSN < recLSN {
		return
	}

	frame, err := r.bufPool.FetchPage(newRootID)
	if err != nil {
		return
	}
	p := r.bufPool.ReadPageData(frame)
	if p.DecodeHeader().PageLSN >= record.LSN {
		r.bufPool.UnpinPage(newRootID, false)
		return
	}
	if len(record.Payload) < 14 {
		p.SetFlag(page.FlagIsRoot)
		p.SetPageLSN(record.LSN)
		r.bufPool.WritePageData(newRootID, p)
		r.bufPool.UnpinPage(newRootID, true)
		return
	}
	leftChildID := binary.LittleEndian.Uint32(record.Payload[4:])
	rightChildID := binary.LittleEndian.Uint32(record.Payload[8:])
	sepKeyLen := int(binary.LittleEndian.Uint16(record.Payload[12:]))
	if len(record.Payload) < 14+sepKeyLen {
		r.bufPool.UnpinPage(newRootID, false)
		return
	}
	sepKey := record.Payload[14 : 14+sepKeyLen]

	// Initialise the new root as an internal page
	p.ID = newRootID
	p.Init(page.PageTypeInternal)
	p.SetFlag(page.FlagIsRoot)
	p.SetPrevSib(leftChildID)  // leftmost child pointer
	p.SetNextSib(rightChildID) // rightmost child pointer
	// Insert separator record
	rec := recoveryEncodeInternalRecord(sepKey, rightChildID)
	p.InsertRecord(rec, recoveryInternalKeyExtract) //nolint
	p.SetPageLSN(record.LSN)

	r.bufPool.WritePageData(newRootID, p)
	r.bufPool.UnpinPage(newRootID, true)
}

// recoveryInternalKeyExtract extracts the key from an internal record [KeyLen:2][Key][ChildID:4].
func recoveryInternalKeyExtract(d []byte) []byte {
	if len(d) < 2 {
		return nil
	}
	keyLen := int(d[0]) | int(d[1])<<8
	if len(d) < 2+keyLen {
		return nil
	}
	return d[2 : 2+keyLen]
}

// recoveryTupleKeyExtract extracts the key from an MVCC tuple
// [Xmin:8][Xmax:8][KeyLen:2][ValLen:2][Flags:1][Key][Val].
func recoveryTupleKeyExtract(d []byte) []byte {
	if len(d) < 21 {
		return nil
	}
	keyLen := int(d[16]) | int(d[17])<<8
	if len(d) < 21+keyLen {
		return nil
	}
	return d[21 : 21+keyLen]
}

// recoveryEncodeInternalRecord encodes a separator key + child pointer.
func recoveryEncodeInternalRecord(key []byte, childID uint32) []byte {
	rec := make([]byte, 2+len(key)+4)
	rec[0] = byte(len(key))
	rec[1] = byte(len(key) >> 8)
	copy(rec[2:], key)
	binary.LittleEndian.PutUint32(rec[2+len(key):], childID)
	return rec
}

// Undo rolls back all active (uncommitted) transactions.
func (r *RecoveryManager) Undo(att ActiveTxnTable) error {
	h := &lsnHeap{}
	for txnID, entry := range att {
		if entry.Status == mvcc.TxnActive && entry.LastLSN != 0 {
			heap.Push(h, lsnEntry{lsn: entry.LastLSN, txnID: txnID})
		}
	}

	for h.Len() > 0 {
		top := heap.Pop(h).(lsnEntry)
		lsn, txnID := top.lsn, top.txnID

		record, err := r.walMgr.FetchRecord(lsn)
		if err != nil {
			return err
		}

		if record.Type == wal.LogCLR {
			undoNext := wal.DecodeCLRUndoNext(record.Payload)
			if undoNext != 0 {
				heap.Push(h, lsnEntry{lsn: undoNext, txnID: txnID})
			}
			continue
		}

		// Undo this record
		clrLSN, err := r.applyUndo(record)
		if err != nil {
			return err
		}

		if r.bus != nil {
			r.bus.Publish(events.Event{Type: events.EvtRecoveryUndo,
				Extra: map[string]interface{}{
					"txn_id": txnID, "undone_lsn": lsn, "clr_lsn": clrLSN,
				}})
		}

		if record.PrevLSN != 0 {
			heap.Push(h, lsnEntry{lsn: record.PrevLSN, txnID: txnID})
		}
	}
	return nil
}

// applyUndo reverses a single WAL record and writes a CLR.
func (r *RecoveryManager) applyUndo(record *wal.LogRecord) (uint64, error) {
	switch record.Type {
	case wal.LogInsert:
		clrPayload := wal.EncodeCLRPayload(record.PrevLSN, record.Type, record.Payload)
		clrLSN := r.walMgr.AppendRecord(wal.LogRecord{
			Type:    wal.LogCLR,
			TxnID:   record.TxnID,
			PageID:  record.PageID,
			PrevLSN: record.LSN,
			Payload: clrPayload,
		})
		// Undo insert: physically remove the tuple from the page.
		frame, err := r.bufPool.FetchPage(record.PageID)
		if err != nil {
			return 0, nil
		}
		p := r.bufPool.ReadPageData(frame)
		// Match by both Xmin == record.TxnID AND key, so that when the same
		// transaction inserted multiple tuples on the same page we remove the
		// correct one rather than the first slot whose Xmin matches.
		targetKey := recoveryTupleKeyExtract(record.Payload)
		deleted := false
		for i := 0; i < p.NumSlots(); i++ {
			data := p.GetRecord(i)
			if len(data) < 8 {
				continue
			}
			xmin := binary.LittleEndian.Uint64(data[0:])
			if xmin != record.TxnID {
				continue
			}
			if targetKey != nil && !bytes.Equal(recoveryTupleKeyExtract(data), targetKey) {
				continue
			}
			p.DeleteRecord(i)
			deleted = true
			break
		}
		if deleted {
			p.Compact() // remove tombstone so BinarySearch works correctly
		}
		p.SetPageLSN(clrLSN)
		r.bufPool.WritePageData(record.PageID, p)
		r.bufPool.UnpinPage(record.PageID, true)
		return clrLSN, nil

	case wal.LogDelete:
		// Undo delete: restore the previous Xmax.
		if len(record.Payload) < 12 {
			return 0, nil
		}
		clrPayload := wal.EncodeCLRPayload(record.PrevLSN, record.Type, record.Payload)
		clrLSN := r.walMgr.AppendRecord(wal.LogRecord{
			Type:    wal.LogCLR,
			TxnID:   record.TxnID,
			PageID:  record.PageID,
			PrevLSN: record.LSN,
			Payload: clrPayload,
		})
		slotIdx := int(binary.LittleEndian.Uint32(record.Payload[0:]))
		oldXmax := binary.LittleEndian.Uint64(record.Payload[4:])
		frame, err := r.bufPool.FetchPage(record.PageID)
		if err != nil {
			return 0, nil
		}
		p := r.bufPool.ReadPageData(frame)
		data := p.GetRecord(slotIdx)
		if len(data) >= 16 {
			binary.LittleEndian.PutUint64(data[8:], oldXmax)
		}
		p.SetPageLSN(clrLSN)
		r.bufPool.WritePageData(record.PageID, p)
		r.bufPool.UnpinPage(record.PageID, true)
		return clrLSN, nil
	}
	return 0, nil
}

func minRecLSN(dpt DirtyPageTable) uint64 {
	var min uint64
	for _, lsn := range dpt {
		if min == 0 || lsn < min {
			min = lsn
		}
	}
	return min
}

// lsnEntry is an item in the max-heap.
type lsnEntry struct {
	lsn   uint64
	txnID uint64
}

type lsnHeap []lsnEntry

func (h lsnHeap) Len() int            { return len(h) }
func (h lsnHeap) Less(i, j int) bool  { return h[i].lsn > h[j].lsn } // max-heap
func (h lsnHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *lsnHeap) Push(x interface{}) { *h = append(*h, x.(lsnEntry)) }
func (h *lsnHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
