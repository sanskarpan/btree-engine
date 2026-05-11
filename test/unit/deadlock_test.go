package unit

import (
	"testing"

	"btree-engine/internal/mvcc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeadlock_NoDeadlock: T1 waits for T2, no cycle.
func TestDeadlock_NoDeadlock(t *testing.T) {
	d := mvcc.NewDeadlockDetector()
	victim, err := d.AddWait(1, 2)
	assert.NoError(t, err, "no deadlock expected")
	assert.Equal(t, mvcc.TxnID(0), victim)
}

// TestDeadlock_DirectCycle: T1 → T2 → T1 (youngest = T2 is victim).
func TestDeadlock_DirectCycle(t *testing.T) {
	d := mvcc.NewDeadlockDetector()

	// T1 waits for T2 — no cycle yet.
	_, err := d.AddWait(1, 2)
	require.NoError(t, err)

	// T2 waits for T1 — creates cycle T1 → T2 → T1.
	victim, err := d.AddWait(2, 1)
	assert.ErrorIs(t, err, mvcc.ErrDeadlockVictim, "cycle must be detected")
	// Victim is the youngest (highest ID) — T2.
	assert.Equal(t, mvcc.TxnID(2), victim, "youngest transaction (T2) is the victim")

	// After detection, the victim's wait edge should be gone.
	assert.False(t, d.CycleExists(), "cycle should be resolved after detection")
}

// TestDeadlock_ThreeWayCycle: T1 → T2 → T3 → T1, victim = T3.
func TestDeadlock_ThreeWayCycle(t *testing.T) {
	d := mvcc.NewDeadlockDetector()

	_, err := d.AddWait(1, 2)
	require.NoError(t, err)
	_, err = d.AddWait(2, 3)
	require.NoError(t, err)

	victim, err := d.AddWait(3, 1) // closes the cycle
	assert.ErrorIs(t, err, mvcc.ErrDeadlockVictim)
	assert.Equal(t, mvcc.TxnID(3), victim, "T3 is youngest in {1,2,3}")
	assert.False(t, d.CycleExists())
}

// TestDeadlock_RemoveWait: removing a wait edge breaks the deadlock.
func TestDeadlock_RemoveWait(t *testing.T) {
	d := mvcc.NewDeadlockDetector()

	_, err := d.AddWait(1, 2)
	require.NoError(t, err)

	d.RemoveWait(1)

	// Now T2 waits for T1 — should be no cycle since T1's edge was removed.
	_, err = d.AddWait(2, 1)
	assert.NoError(t, err, "no cycle after removing T1's wait")
}

// TestDeadlock_TransactionManager_WaitFor: integration with TransactionManager.
func TestDeadlock_TransactionManager_WaitFor(t *testing.T) {
	tm := mvcc.NewTransactionManager(nil)
	t1 := tm.Begin(mvcc.SnapshotIsolation)
	t2 := tm.Begin(mvcc.SnapshotIsolation)

	// T1 waits for T2 — no deadlock.
	_, err := tm.WaitFor(t1.ID, t2.ID)
	require.NoError(t, err)

	// T2 waits for T1 — deadlock, T2 is victim (younger).
	victim, err := tm.WaitFor(t2.ID, t1.ID)
	assert.ErrorIs(t, err, mvcc.ErrDeadlockVictim)
	assert.Equal(t, t2.ID, victim, "T2 is younger and should be victim")
}

// TestDeadlock_CycleExists detects cycle without side effects.
func TestDeadlock_CycleExists(t *testing.T) {
	d := mvcc.NewDeadlockDetector()
	assert.False(t, d.CycleExists())

	_, err := d.AddWait(1, 2)
	require.NoError(t, err)
	assert.False(t, d.CycleExists())

	// Manually add edge to create cycle without triggering resolution.
	// Use AddWait which will detect and resolve.
	_, err = d.AddWait(2, 1)
	require.Error(t, err) // resolved immediately
	assert.False(t, d.CycleExists(), "resolved by AddWait")
}
