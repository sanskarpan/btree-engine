package unit

import (
	"testing"

	"btree-engine/internal/mvcc"

	"github.com/stretchr/testify/assert"
)

func newTM() *mvcc.TransactionManager {
	return mvcc.NewTransactionManager(nil)
}

func TestVisibility_OwnWrites(t *testing.T) {
	tm := newTM()
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	tuple := &mvcc.MVCCTuple{Xmin: uint64(t1.ID), Xmax: 0, Key: []byte("k1"), Value: []byte("v1")}
	assert.True(t, t1.Snapshot.IsVisible(tuple, tm), "txn must see its own uncommitted writes")
}

func TestVisibility_UncommittedHidden(t *testing.T) {
	tm := newTM()
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	// T2 starts after T1 — T1 is in T2's XipList
	t2 := tm.Begin(mvcc.SnapshotIsolation)

	tuple := &mvcc.MVCCTuple{Xmin: uint64(t1.ID), Xmax: 0, Key: []byte("k1"), Value: []byte("v1")}
	assert.False(t, t2.Snapshot.IsVisible(tuple, tm),
		"dirty read: T2 must not see T1's uncommitted write")
}

func TestVisibility_CommittedBeforeSnapshot(t *testing.T) {
	tm := newTM()
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	tuple := &mvcc.MVCCTuple{Xmin: uint64(t1.ID), Xmax: 0, Key: []byte("k1"), Value: []byte("v1")}
	_ = tm.Commit(t1)

	// T3 starts after T1 committed — must see T1's write
	t3 := tm.Begin(mvcc.SnapshotIsolation)
	assert.True(t, t3.Snapshot.IsVisible(tuple, tm), "T3 must see T1's committed write")
}

func TestVisibility_DirtyRead_Prevented(t *testing.T) {
	tm := newTM()
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	tuple := &mvcc.MVCCTuple{Xmin: uint64(t1.ID), Xmax: 0, Key: []byte("k1"), Value: []byte("v1")}
	t2 := tm.Begin(mvcc.SnapshotIsolation)

	assert.False(t, t2.Snapshot.IsVisible(tuple, tm),
		"dirty read: T2 must not see T1's uncommitted write")

	_ = tm.Commit(t1)

	t3 := tm.Begin(mvcc.SnapshotIsolation)
	assert.True(t, t3.Snapshot.IsVisible(tuple, tm), "T3 must see T1's committed write")

	// T2's snapshot predates T1's commit — still must not see it
	assert.False(t, t2.Snapshot.IsVisible(tuple, tm),
		"snapshot isolation: T2's snapshot predates T1's commit")
}

func TestVisibility_DeletedByCommitted(t *testing.T) {
	tm := newTM()
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	_ = tm.Commit(t1)

	t2 := tm.Begin(mvcc.SnapshotIsolation)
	tuple := &mvcc.MVCCTuple{Xmin: uint64(t1.ID), Xmax: uint64(t2.ID), Key: []byte("k1"), Value: []byte("v1")}
	_ = tm.Commit(t2)

	// T3 begins after T2 committed: should NOT see the deleted tuple
	t3 := tm.Begin(mvcc.SnapshotIsolation)
	assert.False(t, t3.Snapshot.IsVisible(tuple, tm),
		"deleted tuple must be invisible after deleter commits")
}

func TestVisibility_DeletedByActive(t *testing.T) {
	tm := newTM()
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	_ = tm.Commit(t1)

	t2 := tm.Begin(mvcc.SnapshotIsolation)
	// T3 takes snapshot while T2 is still active (deleter in XipList)
	tuple := &mvcc.MVCCTuple{Xmin: uint64(t1.ID), Xmax: uint64(t2.ID), Key: []byte("k1"), Value: []byte("v1")}
	t3 := tm.Begin(mvcc.SnapshotIsolation) // T2 still active

	// T3 must still see the tuple because its deleter (T2) was in-progress
	assert.True(t, t3.Snapshot.IsVisible(tuple, tm),
		"T3 must see row whose deleter is still active")
}

func TestVisibility_DeletedByAborted(t *testing.T) {
	tm := newTM()
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	_ = tm.Commit(t1)

	t2 := tm.Begin(mvcc.SnapshotIsolation)
	tuple := &mvcc.MVCCTuple{Xmin: uint64(t1.ID), Xmax: uint64(t2.ID), Key: []byte("k1"), Value: []byte("v1")}
	_ = tm.Abort(t2) // deletion rolled back

	t3 := tm.Begin(mvcc.SnapshotIsolation)
	assert.True(t, t3.Snapshot.IsVisible(tuple, tm),
		"aborted deleter: tuple is still alive")
}

func TestSnapshot_IsolationLevelDiff(t *testing.T) {
	tm := newTM()

	// Initial committed value
	t0 := tm.Begin(mvcc.SnapshotIsolation)
	v1 := &mvcc.MVCCTuple{Xmin: uint64(t0.ID), Xmax: 0, Key: []byte("bal"), Value: []byte("100")}
	_ = tm.Commit(t0)

	// SI transaction — snapshot taken at Begin
	tSI := tm.Begin(mvcc.SnapshotIsolation)

	// Another transaction updates (commits a v2)
	tUpd := tm.Begin(mvcc.SnapshotIsolation)
	v2 := &mvcc.MVCCTuple{Xmin: uint64(tUpd.ID), Xmax: 0, Key: []byte("bal"), Value: []byte("200")}
	// mark v1 as deleted by tUpd
	v1.Xmax = uint64(tUpd.ID)
	_ = tm.Commit(tUpd)

	// SI must not see v2 (snapshot predates commit of tUpd)
	assert.False(t, tSI.Snapshot.IsVisible(v2, tm),
		"SI: must not see update committed after snapshot")

	// RC: refresh snapshot after tUpd committed
	tRC := tm.Begin(mvcc.ReadCommitted)
	// Refresh snapshot for RC
	tRC.Snapshot = tm.TakeSnapshot(tRC.ID)
	assert.True(t, tRC.Snapshot.IsVisible(v2, tm),
		"RC: must see update that committed before this statement")

	_ = tm.Commit(tSI)
	_ = tm.Commit(tRC)
}
