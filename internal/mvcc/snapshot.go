package mvcc

// Snapshot captures the database visibility state at a point in time.
// A tuple is visible iff all visibility rules in IsVisible pass.
type Snapshot struct {
	// XminSnapshot is the lowest active TxnID at snapshot time (used for optimisation).
	XminSnapshot TxnID
	// XmaxSnapshot is the next TxnID to assign at snapshot time.
	// All TxnIDs >= XmaxSnapshot are future transactions and invisible.
	XmaxSnapshot TxnID
	// XipList is the list of in-progress (active, uncommitted) TxnIDs at snapshot time.
	XipList []TxnID
	// CreatorTxnID is the transaction that owns this snapshot.
	// Its own writes are always visible.
	CreatorTxnID TxnID
}

// IsVisible implements the 9-case PostgreSQL-style MVCC visibility check.
//
// CRITICAL CORRECTNESS: any bug here causes dirty reads, lost updates, or phantom reads.
func (snap *Snapshot) IsVisible(t *MVCCTuple, tm *TransactionManager) bool {
	// =========== Check Xmin (creating transaction) ===========

	// Case 1: Creator is this transaction itself — always visible (own writes)
	if TxnID(t.Xmin) == snap.CreatorTxnID {
		goto checkXmax
	}

	// Case 2: Creator not committed — invisible
	if tm.GetStatus(TxnID(t.Xmin)) != TxnCommitted {
		return false
	}

	// Case 3: Creator committed after snapshot — invisible (future transaction)
	if TxnID(t.Xmin) >= snap.XmaxSnapshot {
		return false
	}

	// Case 4: Creator was in-progress when snapshot was taken — invisible
	for _, xid := range snap.XipList {
		if TxnID(t.Xmin) == xid {
			return false
		}
	}
	// Creator committed before snapshot and not in XipList → row was created before our snapshot

checkXmax:
	// =========== Check Xmax (deleting transaction) ===========

	// Case 5: Not deleted — visible
	if t.Xmax == 0 {
		return true
	}

	// Case 6: Deleted by this transaction — invisible (we deleted it)
	if TxnID(t.Xmax) == snap.CreatorTxnID {
		return false
	}

	// Case 7: Deleting transaction aborted — deletion rolled back, row is alive
	if tm.GetStatus(TxnID(t.Xmax)) == TxnAborted {
		return true
	}

	// Case 8: Deleting transaction committed after snapshot — deletion not yet visible to us
	if TxnID(t.Xmax) >= snap.XmaxSnapshot {
		return true
	}

	// Case 9: Deleting transaction was in-progress when snapshot taken — deletion not visible
	for _, xid := range snap.XipList {
		if TxnID(t.Xmax) == xid {
			return true // deleter still running when we took snapshot
		}
	}

	// Deleting transaction committed before our snapshot — row is deleted and invisible
	return false
}
