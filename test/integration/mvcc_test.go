package integration

import (
	"testing"

	"btree-engine/internal/mvcc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMVCC_SnapshotIsolation(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	// Setup: T1 inserts "balance"=100
	t1 := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(t1, []byte("balance"), []byte("100")) //nolint
	eng.Commit(t1)                                //nolint

	// T2 starts snapshot isolation txn
	t2 := eng.Begin(mvcc.SnapshotIsolation)

	// T3 updates "balance" to 200 and commits
	t3 := eng.Begin(mvcc.SnapshotIsolation)
	eng.Update(t3, []byte("balance"), []byte("200")) //nolint
	eng.Commit(t3)                                   //nolint

	// T2 must still see 100 (snapshot taken before T3's update)
	val, err := eng.Get(t2, []byte("balance"))
	require.NoError(t, err)
	assert.Equal(t, []byte("100"), val, "SI: T2 must see snapshot value, not T3's committed update")
	eng.Commit(t2) //nolint

	// T4 starts after T3: must see 200
	t4 := eng.Begin(mvcc.SnapshotIsolation)
	val, err = eng.Get(t4, []byte("balance"))
	require.NoError(t, err)
	assert.Equal(t, []byte("200"), val)
	eng.Commit(t4) //nolint
}

func TestMVCC_WriteSkew_Demonstrable(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	// Setup: two doctors on call
	setup := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(setup, []byte("alice_on_call"), []byte("true")) //nolint
	eng.Put(setup, []byte("bob_on_call"), []byte("true"))   //nolint
	eng.Commit(setup)                                       //nolint

	// T1 reads all on-call doctors: sees both
	t1 := eng.Begin(mvcc.SnapshotIsolation)
	alice, _ := eng.Get(t1, []byte("alice_on_call"))
	bob, _ := eng.Get(t1, []byte("bob_on_call"))
	assert.Equal(t, []byte("true"), alice)
	assert.Equal(t, []byte("true"), bob)

	// T2 also reads: sees both
	t2 := eng.Begin(mvcc.SnapshotIsolation)
	alice2, _ := eng.Get(t2, []byte("alice_on_call"))
	bob2, _ := eng.Get(t2, []byte("bob_on_call"))
	assert.Equal(t, []byte("true"), alice2)
	assert.Equal(t, []byte("true"), bob2)

	// T1 decides to clock out Alice
	eng.Update(t1, []byte("alice_on_call"), []byte("false")) //nolint
	eng.Commit(t1)                                           //nolint — no write-write conflict

	// T2 decides to clock out Bob
	eng.Update(t2, []byte("bob_on_call"), []byte("false")) //nolint
	eng.Commit(t2)                                         //nolint — no write-write conflict with T1

	// Final state: BOTH clocked out — invariant violated!
	t3 := eng.Begin(mvcc.SnapshotIsolation)
	aliceFinal, _ := eng.Get(t3, []byte("alice_on_call"))
	bobFinal, _ := eng.Get(t3, []byte("bob_on_call"))
	assert.Equal(t, []byte("false"), aliceFinal) // both clocked out!
	assert.Equal(t, []byte("false"), bobFinal)   // WRITE SKEW confirmed
	eng.Commit(t3)                               //nolint

	t.Log("Write skew demonstrated: SI allowed both transactions to commit despite invariant violation")
}

func TestMVCC_NoDirtyReads(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	// T1 inserts but doesn't commit
	t1 := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(t1, []byte("dirty"), []byte("secret")) //nolint

	// T2 must not see T1's uncommitted write
	t2 := eng.Begin(mvcc.SnapshotIsolation)
	_, err := eng.Get(t2, []byte("dirty"))
	assert.Error(t, err, "dirty read: T2 must not see T1's uncommitted write")

	eng.Commit(t1) //nolint
	eng.Commit(t2) //nolint
}

// TestMVCC_ReadCommitted_NonRepeatableRead verifies that a ReadCommitted transaction
// sees a new committed value on its second read (non-repeatable read is allowed under RC).
func TestMVCC_ReadCommitted_NonRepeatableRead(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	// Setup: x = v1
	setup := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(setup, []byte("x"), []byte("v1")) //nolint
	eng.Commit(setup)                         //nolint

	// RC transaction reads x = v1
	rcTxn := eng.Begin(mvcc.ReadCommitted)
	v1, err := eng.Get(rcTxn, []byte("x"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), v1, "RC first read must see v1")

	// Another txn updates x to v2 and commits
	upd := eng.Begin(mvcc.SnapshotIsolation)
	eng.Update(upd, []byte("x"), []byte("v2")) //nolint
	eng.Commit(upd)                             //nolint

	// RC transaction reads x again — must see v2 (snapshot refreshed per statement)
	v2, err := eng.Get(rcTxn, []byte("x"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v2, "RC second read must see v2 (non-repeatable read)")
	eng.Commit(rcTxn) //nolint
}

// TestMVCC_SnapshotIso_RepeatableRead verifies that SI gives repeatable reads.
func TestMVCC_SnapshotIso_RepeatableRead(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	// Setup: x = v1
	setup := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(setup, []byte("x"), []byte("v1")) //nolint
	eng.Commit(setup)                         //nolint

	// SI transaction takes snapshot now
	siTxn := eng.Begin(mvcc.SnapshotIsolation)

	// Another txn updates x to v2 and commits
	upd := eng.Begin(mvcc.SnapshotIsolation)
	eng.Update(upd, []byte("x"), []byte("v2")) //nolint
	eng.Commit(upd)                            //nolint

	// SI must still see v1 (snapshot isolation = repeatable read)
	v, err := eng.Get(siTxn, []byte("x"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), v, "SI: same value on second read (repeatable read)")

	eng.Commit(siTxn) //nolint
}

func TestMVCC_ReadCommitted_ScanRefreshesSnapshot(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	setup := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(setup, []byte("a"), []byte("va")))
	require.NoError(t, eng.Commit(setup))

	rcTxn := eng.Begin(mvcc.ReadCommitted)

	upd := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(upd, []byte("b"), []byte("vb")))
	require.NoError(t, eng.Commit(upd))

	cursor, err := eng.Scan(rcTxn, []byte("a"), []byte("z"))
	require.NoError(t, err)
	defer cursor.Close()

	foundB := false
	for {
		tuple, scanErr := cursor.Next()
		if scanErr != nil {
			break
		}
		if string(tuple.Key) == "b" && string(tuple.Value) == "vb" {
			foundB = true
		}
	}
	assert.True(t, foundB, "read committed scan should refresh snapshot and see newly committed rows")
	require.NoError(t, eng.Commit(rcTxn))
}

func TestMVCC_ReadCommitted_UpdateUsesLatestCommittedVersion(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	setup := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(setup, []byte("x"), []byte("v1")))
	require.NoError(t, eng.Commit(setup))

	rcTxn := eng.Begin(mvcc.ReadCommitted)

	upd := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Update(upd, []byte("x"), []byte("v2")))
	require.NoError(t, eng.Commit(upd))

	require.NoError(t, eng.Update(rcTxn, []byte("x"), []byte("v3")))
	require.NoError(t, eng.Commit(rcTxn))

	reader := eng.Begin(mvcc.SnapshotIsolation)
	val, err := eng.Get(reader, []byte("x"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v3"), val)
	require.NoError(t, eng.Commit(reader))
}
