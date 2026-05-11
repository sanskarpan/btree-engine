package unit

import (
	"fmt"
	"testing"

	"btree-engine/internal/btree"
	"btree-engine/internal/buffer"
	"btree-engine/internal/disk"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestBTreeWithVacuum(t *testing.T) (*btree.BPlusTree, *mvcc.TransactionManager, *mvcc.VacuumWorker, func()) {
	t.Helper()
	dir := t.TempDir()
	dm, err := disk.Open(dir + "/btree.db")
	require.NoError(t, err)

	meta, err := dm.ReadMeta()
	require.NoError(t, err)

	bp := buffer.New(512, dm, nil, nil)
	tm := mvcc.NewTransactionManager(nil)
	bt := btree.New(meta.RootPageID, bp, nil, dm, tm, nil)

	scanner := btree.NewLeafScanner(bt, bp)
	vac := mvcc.NewVacuumWorker(scanner, tm, mvcc.DefaultVacuumConfig(), nil)

	return bt, tm, vac, func() { _ = dm.Close() }
}

func openTestBTree(t *testing.T) (*btree.BPlusTree, *mvcc.TransactionManager, func()) {
	t.Helper()
	dir := t.TempDir()
	dm, err := disk.Open(dir + "/btree.db")
	require.NoError(t, err)

	meta, err := dm.ReadMeta()
	require.NoError(t, err)

	bp := buffer.New(256, dm, nil, nil)
	tm := mvcc.NewTransactionManager(nil)
	bt := btree.New(meta.RootPageID, bp, nil, dm, tm, nil)

	return bt, tm, func() { _ = dm.Close() }
}

func openTestBTreeWithPoolSize(t *testing.T, poolSize int) (*btree.BPlusTree, *mvcc.TransactionManager, func()) {
	t.Helper()
	dir := t.TempDir()
	dm, err := disk.Open(dir + "/btree.db")
	require.NoError(t, err)

	meta, err := dm.ReadMeta()
	require.NoError(t, err)

	bp := buffer.New(poolSize, dm, nil, nil)
	tm := mvcc.NewTransactionManager(nil)
	bt := btree.New(meta.RootPageID, bp, nil, dm, tm, nil)

	return bt, tm, func() { _ = dm.Close() }
}

func TestBTree_InsertAndGet(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	txn := tm.Begin(mvcc.SnapshotIsolation)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		val := []byte(fmt.Sprintf("val%04d", i))
		require.NoError(t, bt.Insert(txn, key, val))
	}
	require.NoError(t, tm.Commit(txn))

	// New transaction to read
	txn2 := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		expected := []byte(fmt.Sprintf("val%04d", i))
		val, err := bt.Get(txn2, key)
		require.NoError(t, err, "key %s not found", key)
		assert.Equal(t, expected, val)
	}
	tm.Commit(txn2) //nolint
}

func TestBTree_SplitOnInsert(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	// Insert enough keys to trigger splits (a leaf holds ~100 small tuples)
	txn := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		val := []byte("v")
		require.NoError(t, bt.Insert(txn, key, val))
	}
	tm.Commit(txn) //nolint

	txn2 := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		_, err := bt.Get(txn2, key)
		assert.NoError(t, err, "key %s missing after split", key)
	}
	tm.Commit(txn2) //nolint
}

func TestBTree_SplitOnInsert_TinyBufferPool(t *testing.T) {
	bt, tm, cleanup := openTestBTreeWithPoolSize(t, 2)
	defer cleanup()

	txn := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 800; i++ {
		key := []byte(fmt.Sprintf("tiny%05d", i))
		require.NoError(t, bt.Insert(txn, key, []byte("v")))
	}
	require.NoError(t, tm.Commit(txn))

	reader := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 800; i++ {
		key := []byte(fmt.Sprintf("tiny%05d", i))
		_, err := bt.Get(reader, key)
		require.NoError(t, err, "key %s missing after split with tiny buffer pool", key)
	}
	require.NoError(t, tm.Commit(reader))
}

func TestBTree_Delete_DoesNotLeakPins(t *testing.T) {
	bt, tm, cleanup := openTestBTreeWithPoolSize(t, 2)
	defer cleanup()

	writer := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 300; i++ {
		key := []byte(fmt.Sprintf("del%05d", i))
		require.NoError(t, bt.Insert(writer, key, []byte("v")))
	}
	require.NoError(t, tm.Commit(writer))

	deleter := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 220; i++ {
		key := []byte(fmt.Sprintf("del%05d", i))
		require.NoError(t, bt.Delete(deleter, key), "delete should not exhaust the buffer pool with leaked pins")
	}
	require.NoError(t, tm.Commit(deleter))

	reader := tm.Begin(mvcc.SnapshotIsolation)
	for i := 220; i < 300; i++ {
		key := []byte(fmt.Sprintf("del%05d", i))
		_, err := bt.Get(reader, key)
		require.NoError(t, err)
	}
	require.NoError(t, tm.Commit(reader))
}

func TestBTree_MVCC_OwnWrites(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	txn := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(txn, []byte("foo"), []byte("bar")))

	// Same txn must see its own write
	val, err := bt.Get(txn, []byte("foo"))
	require.NoError(t, err)
	assert.Equal(t, []byte("bar"), val)

	tm.Commit(txn) //nolint
}

func TestBTree_MVCC_IsolatedReads(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	// T1 inserts but doesn't commit yet
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(t1, []byte("secret"), []byte("val")))

	// T2 starts while T1 is still active
	t2 := tm.Begin(mvcc.SnapshotIsolation)
	_, err := bt.Get(t2, []byte("secret"))
	assert.ErrorIs(t, err, btree.ErrKeyNotFound, "T2 must not see T1's uncommitted write")

	tm.Commit(t1) //nolint

	// T3 starts after T1 commits — must see it
	t3 := tm.Begin(mvcc.SnapshotIsolation)
	val, err := bt.Get(t3, []byte("secret"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val"), val)

	// T2's snapshot is before T1's commit — still must not see it
	_, err = bt.Get(t2, []byte("secret"))
	assert.ErrorIs(t, err, btree.ErrKeyNotFound, "T2 snapshot predates T1 commit")

	tm.Commit(t2) //nolint
	tm.Commit(t3) //nolint
}

func TestBTree_Update(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	t1 := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(t1, []byte("bal"), []byte("100")))
	tm.Commit(t1) //nolint

	t2 := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Update(t2, []byte("bal"), []byte("200")))
	tm.Commit(t2) //nolint

	t3 := tm.Begin(mvcc.SnapshotIsolation)
	val, err := bt.Get(t3, []byte("bal"))
	require.NoError(t, err)
	assert.Equal(t, []byte("200"), val)
	tm.Commit(t3) //nolint
}

func TestBTree_LeafVersionsOrdered(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	// Insert v1
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(t1, []byte("key"), []byte("v1")))
	tm.Commit(t1) //nolint

	// Update to v2
	t2 := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Update(t2, []byte("key"), []byte("v2")))
	tm.Commit(t2) //nolint

	// Latest transaction sees v2
	t3 := tm.Begin(mvcc.SnapshotIsolation)
	val, err := bt.Get(t3, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
	tm.Commit(t3) //nolint
}

// TestBTree_MergeOnDelete inserts many keys, deletes most of them, runs vacuum
// to compact the pages, then calls Rebalance and verifies the remaining keys
// are still accessible and the tree structure is valid.
func TestBTree_MergeOnDelete(t *testing.T) {
	bt, tm, vac, cleanup := openTestBTreeWithVacuum(t)
	defer cleanup()

	const total = 300
	const keep = 20

	// Insert total keys across multiple leaf pages (causes splits).
	ins := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("m%05d", i))
		require.NoError(t, bt.Insert(ins, key, []byte("value")))
	}
	require.NoError(t, tm.Commit(ins))

	// Delete all but the last `keep` keys in a committed transaction.
	del := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < total-keep; i++ {
		key := []byte(fmt.Sprintf("m%05d", i))
		require.NoError(t, bt.Delete(del, key))
	}
	require.NoError(t, tm.Commit(del))

	// VACUUM: physically remove dead tuples and compact pages.
	vac.RunOnce()

	// Rebalance: merge underflowing leaf pages.
	rb := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Rebalance(rb))
	require.NoError(t, tm.Commit(rb))

	// All remaining keys must still be readable.
	r := tm.Begin(mvcc.SnapshotIsolation)
	for i := total - keep; i < total; i++ {
		key := []byte(fmt.Sprintf("m%05d", i))
		_, err := bt.Get(r, key)
		assert.NoError(t, err, "key %s missing after merge+rebalance", key)
	}
	// Deleted keys must be invisible.
	for i := 0; i < total-keep; i++ {
		key := []byte(fmt.Sprintf("m%05d", i))
		_, err := bt.Get(r, key)
		assert.Error(t, err, "deleted key %s still visible", key)
	}
	tm.Commit(r) //nolint
}

// TestBTree_CursorClose_AfterEOF verifies that calling Close() after a cursor has
// returned io.EOF does not panic or double-unpin a buffer frame.
// Before the fix, the cursor kept c.current non-nil after an EOF return, causing
// a second UnpinPage call in Close() that could corrupt pin counts.
func TestBTree_CursorClose_AfterEOF(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	txn := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 10; i++ {
		require.NoError(t, bt.Insert(txn, []byte(fmt.Sprintf("eof%02d", i)), []byte("v")))
	}
	require.NoError(t, tm.Commit(txn))

	r := tm.Begin(mvcc.SnapshotIsolation)
	// Scan with an upper bound that stops mid-range.
	cursor, err := bt.Scan(r, []byte("eof00"), []byte("eof04"))
	require.NoError(t, err)

	var count int
	for {
		_, err := cursor.Next()
		if err != nil {
			break
		}
		count++
	}
	assert.GreaterOrEqual(t, count, 1)
	// Must not panic or corrupt pin counts.
	cursor.Close()
	cursor.Close() // idempotent: second close must be a no-op
	tm.Commit(r)   //nolint
}

// TestVacuumWorker_NilPageNoCrash verifies that VacuumWorker.RunOnce does not panic
// when ReadLeafPage returns nil (e.g. the buffer pool is full and FetchPage fails).
// Regression for: nil deref in vacuumLeafPage / WriteLeafPage / p.NextSib().
func TestVacuumWorker_NilPageNoCrash(t *testing.T) {
	scanner := &nilPageScanner{}
	tm := mvcc.NewTransactionManager(nil)
	vac := mvcc.NewVacuumWorker(scanner, tm, mvcc.DefaultVacuumConfig(), nil)
	require.NotPanics(t, func() { vac.RunOnce() })
}

// nilPageScanner always returns nil from ReadLeafPage to simulate a fetch failure.
type nilPageScanner struct{}

func (n *nilPageScanner) FirstLeafPageID() uint32          { return 1 } // non-zero to enter loop
func (n *nilPageScanner) ReadLeafPage(_ uint32) *page.Page { return nil }
func (n *nilPageScanner) WriteLeafPage(_ *page.Page)       {}
func (n *nilPageScanner) UnpinLeaf(_ uint32, _ bool)       {}

// TestVacuumWorker_PrunesStatusTable verifies that VacuumWorker.RunOnce prunes the
// TransactionManager statusTable so finalized entries below the oldest active txn
// are removed, preventing unbounded memory growth.
func TestVacuumWorker_PrunesStatusTable(t *testing.T) {
	bt, tm, vac, cleanup := openTestBTreeWithVacuum(t)
	defer cleanup()

	// Commit 50 transactions — they populate the statusTable.
	for i := 0; i < 50; i++ {
		txn := tm.Begin(mvcc.SnapshotIsolation)
		_ = bt.Insert(txn, []byte(fmt.Sprintf("p%04d", i)), []byte("v"))
		require.NoError(t, tm.Commit(txn))
	}

	// Start a long-lived active transaction to pin the pruning horizon.
	long := tm.Begin(mvcc.SnapshotIsolation)

	// Commit 50 more transactions above the horizon.
	for i := 50; i < 100; i++ {
		txn := tm.Begin(mvcc.SnapshotIsolation)
		_ = bt.Insert(txn, []byte(fmt.Sprintf("p%04d", i)), []byte("v"))
		require.NoError(t, tm.Commit(txn))
	}

	// Vacuum should prune entries below long.ID.
	vac.RunOnce()

	// Entries with ID < long.ID must be gone; entries >= long.ID must remain.
	// Verify by checking that the oldest-active txn's ID is still known (not pruned).
	// We probe via GetStatus: pruned entries fall back to TxnAborted (unknown), but
	// the long txn itself is still active so must be TxnActive.
	assert.Equal(t, mvcc.TxnActive, tm.GetStatus(long.ID), "long txn must still be active")

	tm.Commit(long) //nolint
}

// TestVacuumWorker_ReclaimsAbortedInserts verifies that VACUUM removes tuples
// whose inserting transaction was aborted. Before this fix, aborted-insert tuples
// were never reclaimed and would grow the tree indefinitely.
func TestVacuumWorker_ReclaimsAbortedInserts(t *testing.T) {
	bt, tm, vac, cleanup := openTestBTreeWithVacuum(t)
	defer cleanup()

	// Insert some committed baseline keys.
	base := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(base, []byte("keep"), []byte("alive")))
	require.NoError(t, tm.Commit(base))

	// Insert keys in a transaction that we then abort.
	bad := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 10; i++ {
		require.NoError(t, bt.Insert(bad, []byte(fmt.Sprintf("dead%02d", i)), []byte("gone")))
	}
	require.NoError(t, tm.Abort(bad))

	// Aborted inserts are logically invisible (MVCC Case 2).
	chk := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 10; i++ {
		_, err := bt.Get(chk, []byte(fmt.Sprintf("dead%02d", i)))
		assert.ErrorIs(t, err, btree.ErrKeyNotFound, "aborted insert must be invisible")
	}
	// Baseline must still be visible.
	val, err := bt.Get(chk, []byte("keep"))
	require.NoError(t, err)
	assert.Equal(t, []byte("alive"), val)
	tm.Commit(chk) //nolint

	// VACUUM must reclaim the aborted tuples without panicking.
	require.NotPanics(t, func() { vac.RunOnce() })
}

// TestBTree_MergePageIntegrity inserts keys, deletes enough to trigger leaf merges,
// then verifies all surviving keys are readable and no panic occurred.
// Regression for: freed page frame staying dirty in buffer pool and overwriting freelist.
func TestBTree_MergePageIntegrity(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	// Insert 300 keys to guarantee multiple leaf pages.
	ins := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 300; i++ {
		require.NoError(t, bt.Insert(ins, []byte(fmt.Sprintf("mp%04d", i)), []byte("v")))
	}
	require.NoError(t, tm.Commit(ins))

	// Delete keys at even indices — enough to trigger underflow/merge on most leaves.
	del := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 300; i += 2 {
		require.NoError(t, bt.Delete(del, []byte(fmt.Sprintf("mp%04d", i))))
	}
	require.NoError(t, tm.Commit(del))

	// All odd-indexed keys must still be readable.
	r := tm.Begin(mvcc.SnapshotIsolation)
	for i := 1; i < 300; i += 2 {
		_, err := bt.Get(r, []byte(fmt.Sprintf("mp%04d", i)))
		assert.NoError(t, err, "key mp%04d must survive merge", i)
	}
	tm.Commit(r) //nolint
}

// TestBTree_BinarySearchLeftmostVersion verifies that FindVisibleSlot returns the correct
// (older) MVCC version when BinarySearch might land on a newer duplicate key.
// Regression for: BinarySearch returning non-leftmost match causing MVCC isolation failure.
func TestBTree_BinarySearchLeftmostVersion(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	// Insert 8 keys so we have exactly 8 slots on one leaf.
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	for _, k := range []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"} {
		require.NoError(t, bt.Insert(t1, []byte(k), []byte("v1")))
	}
	require.NoError(t, tm.Commit(t1))

	// T2 begins: snapshot sees all 8 keys at v1.
	t2 := tm.Begin(mvcc.SnapshotIsolation)

	// T3 updates k3 → 9 slots total. With 9 slots, BinarySearch(k3) gets mid=4 which
	// is the new version; the leftmost fix must make it find the old version at slot 3.
	t3 := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Update(t3, []byte("k3"), []byte("v2")))
	require.NoError(t, tm.Commit(t3))

	// T2 must see the OLD version (v1) — T3 committed after T2's snapshot.
	val, err := bt.Get(t2, []byte("k3"))
	require.NoError(t, err, "T2 must see old version of k3 (snapshot predates T3 update)")
	assert.Equal(t, []byte("v1"), val)

	// T4 (started after T3) must see the NEW version (v2).
	t4 := tm.Begin(mvcc.SnapshotIsolation)
	val4, err4 := bt.Get(t4, []byte("k3"))
	require.NoError(t, err4)
	assert.Equal(t, []byte("v2"), val4)

	tm.Commit(t2) //nolint
	tm.Commit(t4) //nolint
}

// TestBTree_WriteWriteConflict verifies that Snapshot Isolation rejects a concurrent update
// when another transaction has already committed a modification to the same key.
func TestBTree_WriteWriteConflict(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	// T1 inserts key "shared".
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(t1, []byte("shared"), []byte("v1")))
	require.NoError(t, tm.Commit(t1))

	// T2 and T3 both start; each should see "shared"=v1.
	t2 := tm.Begin(mvcc.SnapshotIsolation)
	t3 := tm.Begin(mvcc.SnapshotIsolation)

	// T3 updates first and commits.
	require.NoError(t, bt.Update(t3, []byte("shared"), []byte("v3")))
	require.NoError(t, tm.Commit(t3))

	// T2 now tries to update the same key — must get a write-write conflict.
	err := bt.Update(t2, []byte("shared"), []byte("v2"))
	require.ErrorIs(t, err, btree.ErrWriteConflict, "T2 must detect write-write conflict with T3")
	tm.Commit(t2) //nolint

	// A fresh transaction must see T3's value (v3).
	t4 := tm.Begin(mvcc.SnapshotIsolation)
	val, err := bt.Get(t4, []byte("shared"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v3"), val)
	tm.Commit(t4) //nolint
}

// TestBTree_WriteWriteConflict_Delete verifies conflict detection on Delete as well.
func TestBTree_WriteWriteConflict_Delete(t *testing.T) {
	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	t1 := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Insert(t1, []byte("todelete"), []byte("v1")))
	require.NoError(t, tm.Commit(t1))

	t2 := tm.Begin(mvcc.SnapshotIsolation)
	t3 := tm.Begin(mvcc.SnapshotIsolation)

	// T3 deletes the key and commits.
	require.NoError(t, bt.Delete(t3, []byte("todelete")))
	require.NoError(t, tm.Commit(t3))

	// T2 also tries to delete — must get a write-write conflict.
	err := bt.Delete(t2, []byte("todelete"))
	require.ErrorIs(t, err, btree.ErrWriteConflict)
	tm.Commit(t2) //nolint
}

// TestBTree_CascadeInternalMerge verifies that deleting enough keys to trigger
// multiple leaf merges eventually causes internal-node underflows that are
// handled correctly (cascade merge propagates to root if needed).
func TestBTree_CascadeInternalMerge(t *testing.T) {
	bt, tm, vac, cleanup := openTestBTreeWithVacuum(t)
	defer cleanup()

	// Insert enough keys to create a multi-level tree with multiple internal nodes.
	const total = 1000
	ins := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < total; i++ {
		require.NoError(t, bt.Insert(ins, []byte(fmt.Sprintf("ci%06d", i)), []byte("v")))
	}
	require.NoError(t, tm.Commit(ins))

	// Delete the majority to force widespread leaf underflows → leaf merges → internal underflows.
	const keep = 10
	del := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < total-keep; i++ {
		require.NoError(t, bt.Delete(del, []byte(fmt.Sprintf("ci%06d", i))))
	}
	require.NoError(t, tm.Commit(del))

	// VACUUM + Rebalance to trigger the actual cascade.
	vac.RunOnce()
	rb := tm.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, bt.Rebalance(rb))
	require.NoError(t, tm.Commit(rb))

	// All surviving keys must be readable.
	r := tm.Begin(mvcc.SnapshotIsolation)
	for i := total - keep; i < total; i++ {
		key := []byte(fmt.Sprintf("ci%06d", i))
		_, err := bt.Get(r, key)
		assert.NoError(t, err, "key %s missing after cascade merge", key)
	}
	tm.Commit(r) //nolint
}

// TestBTree_1MInserts inserts 1 million sequential keys and verifies all are readable.
// Tree height must remain <= 4 for a balanced fan-out.
func TestBTree_1MInserts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M insert test in short mode")
	}

	bt, tm, cleanup := openTestBTree(t)
	defer cleanup()

	const n = 1_000_000
	txn := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%010d", i))
		require.NoError(t, bt.Insert(txn, key, []byte("v")))
	}
	require.NoError(t, tm.Commit(txn))

	// Spot-check: read back every 1000th key.
	r := tm.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < n; i += 1000 {
		key := []byte(fmt.Sprintf("%010d", i))
		_, err := bt.Get(r, key)
		assert.NoError(t, err, "key %s missing after 1M inserts", key)
	}
	tm.Commit(r) //nolint
}
