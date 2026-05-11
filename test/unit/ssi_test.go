package unit

import (
	"errors"
	"testing"

	"btree-engine/internal/btree"
	"btree-engine/internal/buffer"
	"btree-engine/internal/disk"
	"btree-engine/internal/mvcc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openSSITree returns a BPlusTree and TransactionManager suitable for SSI tests.
func openSSITree(t *testing.T) (*btree.BPlusTree, *mvcc.TransactionManager, func()) {
	t.Helper()
	dir := t.TempDir()
	dm, err := disk.Open(dir + "/btree.db")
	require.NoError(t, err)
	meta, err := dm.ReadMeta()
	require.NoError(t, err)
	bp := buffer.New(512, dm, nil, nil)
	tm := mvcc.NewTransactionManager(nil)
	bt := btree.New(meta.RootPageID, bp, nil, dm, tm, nil)
	return bt, tm, func() { _ = dm.Close() }
}

// TestSSI_NoAnomalyOnIndependentKeys: two serializable transactions read/write
// completely disjoint keys — both should commit without error.
func TestSSI_NoAnomalyOnIndependentKeys(t *testing.T) {
	bt, tm, cleanup := openSSITree(t)
	defer cleanup()

	// Seed keys
	setup := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(setup, []byte("k1"), []byte("v1")))
	require.NoError(t, bt.Insert(setup, []byte("k2"), []byte("v2")))
	require.NoError(t, tm.Commit(setup))

	// T1: reads k1, writes k1
	t1 := tm.Begin(mvcc.Serializable)
	_, err := bt.Get(t1, []byte("k1"))
	require.NoError(t, err)
	require.NoError(t, bt.Update(t1, []byte("k1"), []byte("new1")))

	// T2: reads k2, writes k2 — no overlap with T1
	t2 := tm.Begin(mvcc.Serializable)
	_, err = bt.Get(t2, []byte("k2"))
	require.NoError(t, err)
	require.NoError(t, bt.Update(t2, []byte("k2"), []byte("new2")))

	assert.NoError(t, tm.Commit(t1))
	assert.NoError(t, tm.Commit(t2))
}

// TestSSI_WriteSkewDetected: classic write-skew — T1 reads k2 and writes k1;
// T2 reads k1 and writes k2.  At least one must be aborted.
func TestSSI_WriteSkewDetected(t *testing.T) {
	bt, tm, cleanup := openSSITree(t)
	defer cleanup()

	// Seed
	setup := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(setup, []byte("k1"), []byte("a")))
	require.NoError(t, bt.Insert(setup, []byte("k2"), []byte("b")))
	require.NoError(t, tm.Commit(setup))

	t1 := tm.Begin(mvcc.Serializable)
	t2 := tm.Begin(mvcc.Serializable)

	// T1 reads k2
	_, err := bt.Get(t1, []byte("k2"))
	require.NoError(t, err)

	// T2 reads k1
	_, err = bt.Get(t2, []byte("k1"))
	require.NoError(t, err)

	// T1 writes k1
	err = bt.Update(t1, []byte("k1"), []byte("t1"))
	require.NoError(t, err)

	// T2 writes k2 — this should trigger SSI detection
	err = bt.Update(t2, []byte("k2"), []byte("t2"))

	if err != nil {
		// Detected eagerly at write time — correct.
		assert.True(t, errors.Is(err, mvcc.ErrSerializationFailure), "want ErrSerializationFailure, got %v", err)
		_ = tm.Abort(t2)
		// T1 should still commit successfully.
		assert.NoError(t, tm.Commit(t1))
		return
	}

	// Both writes succeeded; detection must happen at commit time.
	err1 := tm.Commit(t1)
	err2 := tm.Commit(t2)

	// At least one must fail with ErrSerializationFailure.
	serialFail := errors.Is(err1, mvcc.ErrSerializationFailure) ||
		errors.Is(err2, mvcc.ErrSerializationFailure)
	assert.True(t, serialFail, "write skew must be detected: t1=%v t2=%v", err1, err2)
}

// TestSSI_ReadOnlyDoesNotConflict: a read-only serializable transaction should
// never be aborted due to SSI (it can't be the pivot of a dangerous structure).
func TestSSI_ReadOnlyDoesNotConflict(t *testing.T) {
	bt, tm, cleanup := openSSITree(t)
	defer cleanup()

	setup := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(setup, []byte("x"), []byte("1")))
	require.NoError(t, tm.Commit(setup))

	ro := tm.Begin(mvcc.Serializable)
	_, err := bt.Get(ro, []byte("x"))
	require.NoError(t, err)

	// Another transaction writes the same key
	rw := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Update(rw, []byte("x"), []byte("2")))
	require.NoError(t, tm.Commit(rw))

	// Read-only serializable transaction has no outConflict → must commit OK.
	assert.NoError(t, tm.Commit(ro))
}

// TestSSI_PruneBefore: after committing a serializable transaction and calling
// PruneBefore, its SIREAD state is cleaned up.
func TestSSI_PruneBefore(t *testing.T) {
	_, tm, cleanup := openSSITree(t)
	defer cleanup()

	mgr := tm.SIREADMgr
	mgr.AcquireRead(mvcc.TxnID(1), []byte("key"))
	mgr.CommitTxn(mvcc.TxnID(1))
	mgr.PruneBefore(mvcc.TxnID(5))

	// After prune, CheckCommit should see no entry (returns nil).
	assert.NoError(t, mgr.CheckCommit(mvcc.TxnID(1)))
}

// TestSSI_SIREADMgr_DirectUnit tests the lock manager in isolation.
func TestSSI_SIREADMgr_DirectUnit(t *testing.T) {
	mgr := mvcc.NewSIREADLockManager()

	// T1 acquires read on "k"
	mgr.AcquireRead(1, []byte("k"))
	// T2 writes "k" → records T1 →(rw) T2; T2 gets inConflict, T1 gets outConflict
	require.NoError(t, mgr.CheckAndRecordWrite(2, []byte("k")))

	// T2 acquires read on "m"
	mgr.AcquireRead(2, []byte("m"))
	// T1 writes "m" → T2 →(rw) T1 is added; T1 gets inConflict
	// Now T1 has inConflict (from T2) AND outConflict (from the earlier step)
	err := mgr.CheckAndRecordWrite(1, []byte("m"))
	assert.True(t, errors.Is(err, mvcc.ErrSerializationFailure), "want ErrSerializationFailure, got %v", err)
}

// TestSSI_NoFalsePositiveOnSameTransaction: a transaction that reads and writes
// the same key should not trigger a false SSI abort.
func TestSSI_NoFalsePositiveOnSameTransaction(t *testing.T) {
	mgr := mvcc.NewSIREADLockManager()

	// T1 reads "k", then writes "k" — no other transaction involved.
	mgr.AcquireRead(1, []byte("k"))
	err := mgr.CheckAndRecordWrite(1, []byte("k"))
	assert.NoError(t, err, "self read-write must not trigger SSI")
}
