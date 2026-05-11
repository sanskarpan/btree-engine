package integration

import (
	"fmt"
	"testing"

	"btree-engine/internal/engine"
	"btree-engine/internal/mvcc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openEngine(t *testing.T, dir string) *engine.StorageEngine {
	t.Helper()
	cfg := engine.DefaultConfig(dir)
	cfg.SyncWAL = true
	eng, err := engine.OpenEngine(cfg)
	require.NoError(t, err)
	return eng
}

func TestARIES_UncommittedRolledBack(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write 500 keys without committing, then "crash"
	{
		eng := openEngine(t, dir)
		txn := eng.Begin(mvcc.SnapshotIsolation)
		for i := 0; i < 500; i++ {
			err := eng.Put(txn, []byte(fmt.Sprintf("k%04d", i)), []byte("v"))
			require.NoError(t, err)
		}
		// NO COMMIT — simulate crash
		eng.CrashClose()
	}

	// Phase 2: reopen → ARIES recovery → uncommitted writes must be absent
	{
		eng := openEngine(t, dir)
		defer func() { _ = eng.Close() }()
		txn := eng.Begin(mvcc.SnapshotIsolation)
		for i := 0; i < 500; i++ {
			_, err := eng.Get(txn, []byte(fmt.Sprintf("k%04d", i)))
			assert.Error(t, err, "uncommitted key k%04d must not exist after crash recovery", i)
		}
		eng.Commit(txn) //nolint
	}
}

// TestARIES_PartialCrash commits 300 keys then crashes mid-write of 200 more.
// After recovery exactly 300 keys must be present.
func TestARIES_PartialCrash(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: commit 300 keys, then write 200 more WITHOUT committing, then crash.
	{
		eng := openEngine(t, dir)
		committed := eng.Begin(mvcc.SnapshotIsolation)
		for i := 0; i < 300; i++ {
			require.NoError(t, eng.Put(committed, []byte(fmt.Sprintf("pc%04d", i)), []byte(fmt.Sprintf("v%04d", i))))
		}
		require.NoError(t, eng.Commit(committed))

		uncommitted := eng.Begin(mvcc.SnapshotIsolation)
		for i := 300; i < 500; i++ {
			require.NoError(t, eng.Put(uncommitted, []byte(fmt.Sprintf("pc%04d", i)), []byte("dirty")))
		}
		// NO commit — simulate crash mid-write
		eng.CrashClose()
	}

	// Phase 2: reopen → ARIES recovery → exactly 300 committed keys visible.
	{
		eng := openEngine(t, dir)
		defer func() { _ = eng.Close() }()

		txn := eng.Begin(mvcc.SnapshotIsolation)
		// Committed keys must be present.
		for i := 0; i < 300; i++ {
			val, err := eng.Get(txn, []byte(fmt.Sprintf("pc%04d", i)))
			assert.NoError(t, err, "committed key pc%04d missing after partial crash", i)
			if err == nil {
				assert.Equal(t, []byte(fmt.Sprintf("v%04d", i)), val)
			}
		}
		// Uncommitted keys must be absent.
		for i := 300; i < 500; i++ {
			_, err := eng.Get(txn, []byte(fmt.Sprintf("pc%04d", i)))
			assert.Error(t, err, "uncommitted key pc%04d visible after recovery", i)
		}
		eng.Commit(txn) //nolint
	}
}

func TestARIES_CommitSurvivesCrash(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write and commit 200 keys, then crash
	{
		eng := openEngine(t, dir)
		txn := eng.Begin(mvcc.SnapshotIsolation)
		for i := 0; i < 200; i++ {
			require.NoError(t, eng.Put(txn, []byte(fmt.Sprintf("c%04d", i)), []byte(fmt.Sprintf("v%04d", i))))
		}
		require.NoError(t, eng.Commit(txn))
		eng.CrashClose()
	}

	// Phase 2: reopen → all committed keys must be present
	{
		eng := openEngine(t, dir)
		defer func() { _ = eng.Close() }()
		txn := eng.Begin(mvcc.SnapshotIsolation)
		for i := 0; i < 200; i++ {
			val, err := eng.Get(txn, []byte(fmt.Sprintf("c%04d", i)))
			assert.NoError(t, err, "committed key c%04d must survive crash", i)
			if err == nil {
				assert.Equal(t, []byte(fmt.Sprintf("v%04d", i)), val)
			}
		}
		eng.Commit(txn) //nolint
	}
}

// TestARIES_DeleteSurvivesCrash verifies that a committed delete is durable across a crash.
// Before the ARIES redo fix, the LogDelete redo handler was setting Xmax=oldXmax(0) instead
// of Xmax=txn.ID, making deleted tuples reappear after recovery.
func TestARIES_DeleteSurvivesCrash(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: insert keys, delete some, commit, crash.
	{
		eng := openEngine(t, dir)
		// Insert 50 keys.
		ins := eng.Begin(mvcc.SnapshotIsolation)
		for i := 0; i < 50; i++ {
			require.NoError(t, eng.Put(ins, []byte(fmt.Sprintf("del%04d", i)), []byte("alive")))
		}
		require.NoError(t, eng.Commit(ins))

		// Delete even-numbered keys and commit.
		del := eng.Begin(mvcc.SnapshotIsolation)
		for i := 0; i < 50; i += 2 {
			require.NoError(t, eng.Delete(del, []byte(fmt.Sprintf("del%04d", i))))
		}
		require.NoError(t, eng.Commit(del))

		// Crash without flushing buffer pool pages.
		eng.CrashClose()
	}

	// Phase 2: reopen — ARIES must re-apply the deletions.
	{
		eng := openEngine(t, dir)
		defer func() { _ = eng.Close() }()

		txn := eng.Begin(mvcc.SnapshotIsolation)
		// Even-numbered keys were deleted: must not be visible.
		for i := 0; i < 50; i += 2 {
			_, err := eng.Get(txn, []byte(fmt.Sprintf("del%04d", i)))
			assert.Error(t, err, "deleted key del%04d must not be visible after recovery", i)
		}
		// Odd-numbered keys were not deleted: must still be visible.
		for i := 1; i < 50; i += 2 {
			val, err := eng.Get(txn, []byte(fmt.Sprintf("del%04d", i)))
			assert.NoError(t, err, "non-deleted key del%04d must survive crash", i)
			if err == nil {
				assert.Equal(t, []byte("alive"), val)
			}
		}
		eng.Commit(txn) //nolint
	}
}

func TestARIES_CheckpointPreservesCommittedVisibilityAndNextTxnID(t *testing.T) {
	dir := t.TempDir()
	var setupTxnID uint64

	{
		eng := openEngine(t, dir)
		setup := eng.Begin(mvcc.SnapshotIsolation)
		setupTxnID = uint64(setup.ID)
		require.NoError(t, eng.Put(setup, []byte("checkpoint-key"), []byte("stable")))
		require.NoError(t, eng.Commit(setup))
		_, err := eng.Checkpoint()
		require.NoError(t, err)
		eng.CrashClose()
	}

	{
		eng := openEngine(t, dir)
		defer func() { _ = eng.Close() }()

		reader := eng.Begin(mvcc.SnapshotIsolation)
		val, err := eng.Get(reader, []byte("checkpoint-key"))
		require.NoError(t, err)
		assert.Equal(t, []byte("stable"), val)
		require.NoError(t, eng.Commit(reader))

		next := eng.Begin(mvcc.SnapshotIsolation)
		assert.Greater(t, uint64(next.ID), setupTxnID, "post-recovery txn IDs must advance past checkpointed transactions")
		require.NoError(t, eng.Abort(next))
	}
}
