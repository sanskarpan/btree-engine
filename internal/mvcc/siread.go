// Package mvcc — SIREAD lock manager for Serializable Snapshot Isolation.
//
// The algorithm follows Cahill et al. (2008) "Serializable Isolation for
// Snapshot Databases". Each serializable transaction acquires a SIREAD (predicate)
// lock on every key it reads.  On write, we scan all SIREAD holders of that key
// and record rw-anti-dependency edges.  A transaction with BOTH an incoming and
// outgoing rw-anti-dependency is the "pivot" of a dangerous structure (write-skew
// cycle) and must be aborted.
package mvcc

import (
	"errors"
	"sync"
)

// ErrSerializationFailure is returned when SSI detects a write-skew anomaly.
// The caller should abort the transaction and retry.
var ErrSerializationFailure = errors.New("serialization failure: write skew detected, retry the transaction")

// txnConflictEntry tracks rw-anti-dependency flags for a single transaction.
type txnConflictEntry struct {
	// inConflict is true when some T_r →(rw) this T:
	// another transaction T_r read a key that this transaction later wrote.
	inConflict bool

	// outConflict is true when this T →(rw) some T_w:
	// this transaction read a key that another transaction T_w later wrote.
	outConflict bool

	// committed is true after CommitTxn is called.
	// Committed transactions' SIREAD locks are retained until PruneBefore
	// clears them, so that concurrent active transactions can still detect
	// dangerous structures that span a committed and an active transaction.
	committed bool
}

// SIREADLockManager tracks per-key read sets and rw-anti-dependency edges
// for Serializable Snapshot Isolation.
type SIREADLockManager struct {
	mu sync.Mutex

	// keyReaders: key → set of transaction IDs that have a SIREAD lock on it.
	keyReaders map[string]map[TxnID]struct{}

	// conflicts: per-transaction rw-anti-dependency flags.
	conflicts map[TxnID]*txnConflictEntry
}

// NewSIREADLockManager creates an empty lock manager.
func NewSIREADLockManager() *SIREADLockManager {
	return &SIREADLockManager{
		keyReaders: make(map[string]map[TxnID]struct{}),
		conflicts:  make(map[TxnID]*txnConflictEntry),
	}
}

// getOrCreate returns the conflict entry for id, creating it if absent.
// Must be called with m.mu held.
func (m *SIREADLockManager) getOrCreate(id TxnID) *txnConflictEntry {
	e, ok := m.conflicts[id]
	if !ok {
		e = &txnConflictEntry{}
		m.conflicts[id] = e
	}
	return e
}

// AcquireRead records that txnID has read key.
// Must only be called for Serializable transactions.
func (m *SIREADLockManager) AcquireRead(txnID TxnID, key []byte) {
	k := string(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	readers := m.keyReaders[k]
	if readers == nil {
		readers = make(map[TxnID]struct{})
		m.keyReaders[k] = readers
	}
	readers[txnID] = struct{}{}
	m.getOrCreate(txnID) // ensure entry exists so CheckCommit works
}

// CheckAndRecordWrite is called before writerID writes key.
//
// For every other transaction readerID that holds a SIREAD lock on key, it
// records the rw-anti-dependency readerID →(rw) writerID and checks for a
// dangerous structure.  Returns ErrSerializationFailure if:
//   - writerID is now the pivot (inConflict && outConflict), OR
//   - a concurrent reader has become the pivot (inConflict && outConflict).
func (m *SIREADLockManager) CheckAndRecordWrite(writerID TxnID, key []byte) error {
	k := string(key)
	m.mu.Lock()
	defer m.mu.Unlock()

	readers := m.keyReaders[k]
	if len(readers) == 0 {
		return nil
	}

	we := m.getOrCreate(writerID)

	for readerID := range readers {
		if readerID == writerID {
			continue
		}
		re := m.getOrCreate(readerID)

		// Anti-dependency: readerID →(rw) writerID
		// (readerID read key; writerID overwrites it with a newer version)
		re.outConflict = true
		we.inConflict = true

		// Case 1: writer is the pivot of T_reader →(rw) T_writer →(rw) T_x.
		if we.inConflict && we.outConflict {
			return ErrSerializationFailure
		}

		// Case 2: reader is the pivot of T_y →(rw) T_reader →(rw) T_writer.
		// If the reader is still active, it will be caught at its own commit.
		// If the reader already committed, the dangerous structure is complete
		// and we must abort the writer (the active party).
		if re.inConflict && re.outConflict {
			return ErrSerializationFailure
		}
	}
	return nil
}

// CheckCommit returns ErrSerializationFailure if txnID is the pivot of a
// dangerous structure (has both inConflict and outConflict set).
// Call this immediately before committing a Serializable transaction.
func (m *SIREADLockManager) CheckCommit(txnID TxnID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.conflicts[txnID]
	if !ok {
		return nil
	}
	if e.inConflict && e.outConflict {
		return ErrSerializationFailure
	}
	return nil
}

// CommitTxn marks txnID as committed while retaining its SIREAD locks so
// that concurrent transactions can still detect dangerous structures.
// PruneBefore eventually reclaims this memory.
func (m *SIREADLockManager) CommitTxn(txnID TxnID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getOrCreate(txnID).committed = true
}

// Release immediately removes all SIREAD locks and conflict state for txnID.
// Call this for aborted transactions.
func (m *SIREADLockManager) Release(txnID TxnID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(txnID)
}

// releaseLocked removes txnID's state. Must be called with m.mu held.
func (m *SIREADLockManager) releaseLocked(txnID TxnID) {
	for k, readers := range m.keyReaders {
		delete(readers, txnID)
		if len(readers) == 0 {
			delete(m.keyReaders, k)
		}
	}
	delete(m.conflicts, txnID)
}

// PruneBefore removes committed transactions with ID < horizon.
// Safe when all transactions with ID < horizon are no longer active.
func (m *SIREADLockManager) PruneBefore(horizon TxnID) {
	if horizon == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for txnID, e := range m.conflicts {
		if e.committed && txnID < horizon {
			m.releaseLocked(txnID)
		}
	}
}
