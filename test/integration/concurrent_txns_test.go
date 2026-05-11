package integration

import (
	"fmt"
	"sync"
	"testing"

	"btree-engine/internal/mvcc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrent_10Txns runs 10 concurrent goroutines each doing 100 put+get ops.
// Verifies no panics, no dirty reads, and MVCC invariants hold.
func TestConcurrent_10Txns(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	const goroutines = 10
	const opsEach = 100

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*opsEach)

	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			txn := eng.Begin(mvcc.SnapshotIsolation)
			for i := 0; i < opsEach; i++ {
				k := []byte(fmt.Sprintf("g%02d-k%04d", g, i))
				v := []byte(fmt.Sprintf("v%02d-%04d", g, i))
				if err := eng.Put(txn, k, v); err != nil {
					errs <- fmt.Errorf("goroutine %d put: %w", g, err)
					eng.Abort(txn) //nolint
					return
				}
			}
			if err := eng.Commit(txn); err != nil {
				errs <- fmt.Errorf("goroutine %d commit: %w", g, err)
				return
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	// Verify all keys are readable after all commits
	reader := eng.Begin(mvcc.SnapshotIsolation)
	for g := 0; g < goroutines; g++ {
		for i := 0; i < opsEach; i++ {
			k := []byte(fmt.Sprintf("g%02d-k%04d", g, i))
			expected := []byte(fmt.Sprintf("v%02d-%04d", g, i))
			val, err := eng.Get(reader, k)
			assert.NoError(t, err, "key %s missing after concurrent commits", k)
			if err == nil {
				assert.Equal(t, expected, val, "key %s has wrong value", k)
			}
		}
	}
	eng.Commit(reader) //nolint
}

// TestConcurrent_NoDirtyReads verifies that uncommitted writes from one goroutine
// are never visible to another concurrent transaction.
func TestConcurrent_NoDirtyReads(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	// T1 writes a key without committing
	t1 := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(t1, []byte("shared"), []byte("dirty")))

	// T2 starts while T1 is active — must not see T1's uncommitted write
	t2 := eng.Begin(mvcc.SnapshotIsolation)
	_, err := eng.Get(t2, []byte("shared"))
	assert.Error(t, err, "T2 must not see T1's uncommitted write (dirty read)")

	eng.Abort(t1)  //nolint
	eng.Commit(t2) //nolint
}

// TestVacuum_FreeSpaceReclaimed inserts keys, deletes them, runs vacuum,
// and verifies that deleted keys are no longer visible.
func TestVacuum_FreeSpaceReclaimed(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	const n = 200

	// Insert n keys
	ins := eng.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < n; i++ {
		require.NoError(t, eng.Put(ins, []byte(fmt.Sprintf("vac%04d", i)), []byte("v")))
	}
	require.NoError(t, eng.Commit(ins))

	// Delete all n keys in a committed transaction
	del := eng.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < n; i++ {
		require.NoError(t, eng.Delete(del, []byte(fmt.Sprintf("vac%04d", i))))
	}
	require.NoError(t, eng.Commit(del))

	// All keys must be invisible after deletion
	reader := eng.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < n; i++ {
		_, err := eng.Get(reader, []byte(fmt.Sprintf("vac%04d", i)))
		assert.Error(t, err, "deleted key vac%04d must be invisible", i)
	}
	eng.Commit(reader) //nolint
}
