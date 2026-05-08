package mvcc

import (
	"sync"
	"sync/atomic"
	"time"

	"btree-engine/internal/events"
)

// TxnID is a monotonically increasing transaction identifier. 0 is invalid.
type TxnID uint64

// InvalidTxnID represents a null/unset transaction ID.
const InvalidTxnID TxnID = 0

// TxnStatus is the lifecycle state of a transaction.
type TxnStatus uint8

// TxnStatus values.
const (
	TxnActive    TxnStatus = iota // running
	TxnCommitted                  // committed
	TxnAborted                    // rolled back
)

// IsolationLevel controls when a transaction's snapshot is taken.
type IsolationLevel uint8

// IsolationLevel values.
const (
	ReadCommitted    IsolationLevel = iota // new snapshot per statement
	SnapshotIsolation                      // one snapshot at BEGIN
	Serializable                           // SSI: snapshot + SIREAD locks + rw-anti-dep tracking
)

// UndoOpType identifies the kind of undo required for a single operation.
type UndoOpType uint8

const (
	// UndoInsert marks an inserted tuple as deleted by setting Xmax = txnID.
	UndoInsert UndoOpType = iota
	// UndoDelete restores a deleted tuple by clearing Xmax (setting to 0).
	UndoDelete
)

// UndoRecord captures the information needed to roll back a single operation.
type UndoRecord struct {
	PageID  uint32
	SlotIdx int
	OldData []byte     // retained for future byte-level undo; not used by MVCC undo
	Op      UndoOpType // how to reverse the operation
	WALSN   uint64     // LSN of the WAL record for this operation (used to write CLRs)
}

// SavepointEntry marks a position in the UndoLog for partial rollback.
type SavepointEntry struct {
	Name        string
	UndoLogMark int    // len(txn.UndoLog) at savepoint creation time
	LastLSN     uint64 // txn.LastLSN at savepoint creation time
}

// Transaction is the in-flight state for a single database transaction.
type Transaction struct {
	ID          TxnID
	Status      TxnStatus
	IsoLevel    IsolationLevel
	Snapshot    *Snapshot
	LastLSN     uint64
	UndoLog     []UndoRecord
	Savepoints  []SavepointEntry
	StartedAt   time.Time
	CommittedAt time.Time
}

// ActiveTxnInfo is a lightweight snapshot of an active transaction.
type ActiveTxnInfo struct {
	Status  TxnStatus
	LastLSN uint64
}

// TransactionManager owns all transaction lifecycle operations.
type TransactionManager struct {
	mu          sync.RWMutex
	nextTxnID   atomic.Uint64
	statusTable map[TxnID]TxnStatus
	activeTxns  map[TxnID]*Transaction
	bus         *events.EventBus
	SIREADMgr   *SIREADLockManager  // always non-nil; used only for Serializable txns
	Deadlock    *DeadlockDetector   // always non-nil; detects waits-for cycles
}

// NewTransactionManager creates a TransactionManager with nextTxnID starting at 1.
func NewTransactionManager(bus *events.EventBus) *TransactionManager {
	tm := &TransactionManager{
		statusTable: make(map[TxnID]TxnStatus),
		activeTxns:  make(map[TxnID]*Transaction),
		bus:         bus,
		SIREADMgr:   NewSIREADLockManager(),
		Deadlock:    NewDeadlockDetector(),
	}
	tm.nextTxnID.Store(1)
	return tm
}

// Begin creates and registers a new transaction.
func (tm *TransactionManager) Begin(iso IsolationLevel) *Transaction {
	id := TxnID(tm.nextTxnID.Add(1) - 1)

	txn := &Transaction{
		ID:        id,
		Status:    TxnActive,
		IsoLevel:  iso,
		StartedAt: time.Now(),
	}

	tm.mu.Lock()
	tm.activeTxns[id] = txn
	snap := tm.takeSnapshotLocked(id)
	tm.mu.Unlock()

	txn.Snapshot = snap

	if tm.bus != nil {
		tm.bus.Publish(events.Event{
			Type:  events.EvtTxnBegin,
			Extra: map[string]interface{}{"txn_id": uint64(id)},
		})
	}
	return txn
}

// Commit marks the transaction as committed and removes it from the active set.
// For Serializable transactions the SIREAD lock entry is retained (CommitTxn)
// so concurrent active transactions can still detect dangerous structures;
// it will be pruned by PruneStatusesBefore.
func (tm *TransactionManager) Commit(txn *Transaction) error {
	tm.mu.Lock()
	txn.Status = TxnCommitted
	txn.CommittedAt = time.Now()
	tm.statusTable[txn.ID] = TxnCommitted
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()

	if txn.IsoLevel == Serializable {
		tm.SIREADMgr.CommitTxn(txn.ID)
	}

	if tm.bus != nil {
		tm.bus.Publish(events.Event{
			Type:  events.EvtTxnCommit,
			Extra: map[string]interface{}{"txn_id": uint64(txn.ID)},
		})
	}
	return nil
}

// Abort marks the transaction as aborted and releases its SIREAD locks.
func (tm *TransactionManager) Abort(txn *Transaction) error {
	tm.mu.Lock()
	txn.Status = TxnAborted
	tm.statusTable[txn.ID] = TxnAborted
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()

	if txn.IsoLevel == Serializable {
		tm.SIREADMgr.Release(txn.ID)
	}

	if tm.bus != nil {
		tm.bus.Publish(events.Event{
			Type:  events.EvtTxnAbort,
			Extra: map[string]interface{}{"txn_id": uint64(txn.ID)},
		})
	}
	return nil
}

// GetStatus returns the status of a transaction.
// Unknown transactions are treated as aborted (safe default for visibility).
func (tm *TransactionManager) GetStatus(id TxnID) TxnStatus {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if txn, ok := tm.activeTxns[id]; ok {
		return txn.Status
	}
	if status, ok := tm.statusTable[id]; ok {
		return status
	}
	return TxnAborted // unknown = treat as aborted
}

// OldestActiveTxn returns the lowest TxnID among all active transactions.
// Returns 0 if there are no active transactions.
func (tm *TransactionManager) OldestActiveTxn() TxnID {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	var oldest TxnID
	for id := range tm.activeTxns {
		if oldest == 0 || id < oldest {
			oldest = id
		}
	}
	return oldest
}

// TakeSnapshot builds a visibility snapshot for the given creator transaction.
func (tm *TransactionManager) TakeSnapshot(creator TxnID) *Snapshot {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.takeSnapshotLocked(creator)
}

// takeSnapshotLocked must be called with at least tm.mu.RLock held.
func (tm *TransactionManager) takeSnapshotLocked(creator TxnID) *Snapshot {
	var xmin TxnID
	var xip []TxnID
	for id := range tm.activeTxns {
		if id != creator {
			xip = append(xip, id)
			if xmin == 0 || id < xmin {
				xmin = id
			}
		}
	}
	xmax := TxnID(tm.nextTxnID.Load())
	return &Snapshot{
		XminSnapshot: xmin,
		XmaxSnapshot: xmax,
		XipList:      xip,
		CreatorTxnID: creator,
	}
}

// NextTxnID returns the next TxnID that will be assigned (not yet in use).
func (tm *TransactionManager) NextTxnID() TxnID {
	return TxnID(tm.nextTxnID.Load())
}

// AdvanceNextTxnID ensures nextTxnID >= minID, used after WAL recovery to
// prevent new transactions from colliding with IDs seen before the crash.
func (tm *TransactionManager) AdvanceNextTxnID(minID uint64) {
	for {
		current := tm.nextTxnID.Load()
		if current >= minID {
			return
		}
		if tm.nextTxnID.CompareAndSwap(current, minID) {
			return
		}
	}
}

// RestoreStatus records a transaction's final status from WAL recovery,
// making it visible to GetStatus without it being in activeTxns.
func (tm *TransactionManager) RestoreStatus(id TxnID, status TxnStatus) {
	tm.mu.Lock()
	tm.statusTable[id] = status
	tm.mu.Unlock()
}

// WaitFor records that waiterID is blocked waiting for holderID.
// If this creates a deadlock cycle, it returns (victimID, ErrDeadlockVictim)
// where victimID is the youngest transaction in the cycle.  The caller is
// responsible for aborting the victim.
func (tm *TransactionManager) WaitFor(waiterID, holderID TxnID) (TxnID, error) {
	victimID, err := tm.Deadlock.AddWait(waiterID, holderID)
	if err != nil && tm.bus != nil {
		tm.bus.Publish(events.Event{
			Type: events.EvtDeadlock,
			Extra: map[string]interface{}{
				"waiter_id": uint64(waiterID),
				"holder_id": uint64(holderID),
				"victim_id": uint64(victimID),
			},
		})
	}
	return victimID, err
}

// ReleaseWait removes the wait edge for waiterID (called after the waiter
// acquires the lock or is aborted).
func (tm *TransactionManager) ReleaseWait(waiterID TxnID) {
	tm.Deadlock.RemoveWait(waiterID)
}

// PruneStatusesBefore removes all finalized (non-active) statusTable entries
// with TxnID strictly less than horizon. This bounds the memory used by the
// status table in long-running engines.
// The horizon should be the oldest active TxnID; entries below it can never
// affect visibility checks for any current or future snapshot.
// Also prunes committed SIREAD lock entries below horizon.
func (tm *TransactionManager) PruneStatusesBefore(horizon TxnID) {
	if horizon == 0 {
		return
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for id := range tm.statusTable {
		if id < horizon {
			delete(tm.statusTable, id)
		}
	}
	tm.SIREADMgr.PruneBefore(horizon)
}

// ActiveCount returns the number of currently active transactions.
func (tm *TransactionManager) ActiveCount() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.activeTxns)
}

// ActiveTxnSnapshot returns a copy of active transactions for checkpointing.
func (tm *TransactionManager) ActiveTxnSnapshot() map[TxnID]ActiveTxnInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	out := make(map[TxnID]ActiveTxnInfo, len(tm.activeTxns))
	for id, txn := range tm.activeTxns {
		out[id] = ActiveTxnInfo{Status: txn.Status, LastLSN: txn.LastLSN}
	}
	return out
}
