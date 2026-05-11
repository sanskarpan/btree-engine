package unit

import (
	"testing"

	"btree-engine/internal/btree"
	"btree-engine/internal/buffer"
	"btree-engine/internal/disk"
	"btree-engine/internal/engine"
	"btree-engine/internal/mvcc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openEngineForSavepoint(t *testing.T) (*engine.StorageEngine, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg := engine.DefaultConfig(dir)
	cfg.CheckpointInterval = 0 // disable background checkpoint
	eng, err := engine.OpenEngine(cfg)
	require.NoError(t, err)
	return eng, func() { _ = eng.Close() }
}

// TestSavepoint_Basic: insert → savepoint → insert → rollback → first key visible, second not.
func TestSavepoint_Basic(t *testing.T) {
	eng, cleanup := openEngineForSavepoint(t)
	defer cleanup()

	txn := eng.Begin(mvcc.SnapshotIsolation)

	require.NoError(t, eng.Put(txn, []byte("k1"), []byte("v1")))
	require.NoError(t, eng.Savepoint(txn, "sp1"))
	require.NoError(t, eng.Put(txn, []byte("k2"), []byte("v2")))

	// Both keys should be visible before rollback.
	_, err := eng.Get(txn, []byte("k1"))
	require.NoError(t, err)
	_, err = eng.Get(txn, []byte("k2"))
	require.NoError(t, err)

	// Roll back to savepoint — k2 should disappear for this transaction.
	require.NoError(t, eng.RollbackToSavepoint(txn, "sp1"))

	_, err = eng.Get(txn, []byte("k1"))
	assert.NoError(t, err, "k1 should still be visible after rollback to savepoint")

	_, err = eng.Get(txn, []byte("k2"))
	assert.ErrorIs(t, err, btree.ErrKeyNotFound, "k2 should be invisible after rollback to savepoint")

	require.NoError(t, eng.Commit(txn))
}

// TestSavepoint_RollbackDelete: delete a key → savepoint rollback → key restored.
func TestSavepoint_RollbackDelete(t *testing.T) {
	eng, cleanup := openEngineForSavepoint(t)
	defer cleanup()

	setup := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(setup, []byte("x"), []byte("original")))
	require.NoError(t, eng.Commit(setup))

	txn := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Savepoint(txn, "before_delete"))
	require.NoError(t, eng.Delete(txn, []byte("x")))

	// Deleted within transaction.
	_, err := eng.Get(txn, []byte("x"))
	assert.ErrorIs(t, err, btree.ErrKeyNotFound)

	// Roll back — x should be visible again.
	require.NoError(t, eng.RollbackToSavepoint(txn, "before_delete"))

	val, err := eng.Get(txn, []byte("x"))
	require.NoError(t, err)
	assert.Equal(t, "original", string(val))

	require.NoError(t, eng.Commit(txn))
}

// TestSavepoint_DuplicateName returns an error.
func TestSavepoint_DuplicateName(t *testing.T) {
	eng, cleanup := openEngineForSavepoint(t)
	defer cleanup()

	txn := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Savepoint(txn, "sp"))
	err := eng.Savepoint(txn, "sp")
	assert.Error(t, err, "duplicate savepoint name must return an error")
	require.NoError(t, eng.Abort(txn))
}

// TestSavepoint_NotFound returns an error.
func TestSavepoint_NotFound(t *testing.T) {
	eng, cleanup := openEngineForSavepoint(t)
	defer cleanup()

	txn := eng.Begin(mvcc.SnapshotIsolation)
	err := eng.RollbackToSavepoint(txn, "ghost")
	assert.Error(t, err, "missing savepoint must return an error")
	require.NoError(t, eng.Abort(txn))
}

// TestSavepoint_NestedSavepoints: nested savepoints roll back correctly.
func TestSavepoint_NestedSavepoints(t *testing.T) {
	eng, cleanup := openEngineForSavepoint(t)
	defer cleanup()

	txn := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(txn, []byte("a"), []byte("1")))
	require.NoError(t, eng.Savepoint(txn, "sp1"))
	require.NoError(t, eng.Put(txn, []byte("b"), []byte("2")))
	require.NoError(t, eng.Savepoint(txn, "sp2"))
	require.NoError(t, eng.Put(txn, []byte("c"), []byte("3")))

	// Roll back to sp1 — b and c should disappear.
	require.NoError(t, eng.RollbackToSavepoint(txn, "sp1"))

	_, err := eng.Get(txn, []byte("a"))
	assert.NoError(t, err, "a visible")
	_, err = eng.Get(txn, []byte("b"))
	assert.ErrorIs(t, err, btree.ErrKeyNotFound, "b gone after rollback to sp1")
	_, err = eng.Get(txn, []byte("c"))
	assert.ErrorIs(t, err, btree.ErrKeyNotFound, "c gone after rollback to sp1")

	// sp2 should also be gone after rolling back past it.
	err = eng.RollbackToSavepoint(txn, "sp2")
	assert.Error(t, err, "sp2 should no longer exist")

	require.NoError(t, eng.Commit(txn))
}

// TestSavepoint_UndoLogPopulated verifies Insert populates the UndoLog.
func TestSavepoint_UndoLogPopulated(t *testing.T) {
	dir := t.TempDir()
	dm, err := disk.Open(dir + "/btree.db")
	require.NoError(t, err)
	defer dm.Close()
	meta, err := dm.ReadMeta()
	require.NoError(t, err)
	bp := buffer.New(512, dm, nil, nil)
	tm := mvcc.NewTransactionManager(nil)
	bt := btree.New(meta.RootPageID, bp, nil, dm, tm, nil)

	txn := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(txn, []byte("key"), []byte("val")))
	assert.Equal(t, 1, len(txn.UndoLog), "UndoLog should have one entry after Insert")
	assert.Equal(t, mvcc.UndoInsert, txn.UndoLog[0].Op)
}
