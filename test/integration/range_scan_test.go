package integration

import (
	"io"
	"testing"

	"btree-engine/internal/mvcc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScan_BasicRange inserts keys "a".."z" and scans "f".."t",
// verifying exactly keys f..t are returned.
func TestScan_BasicRange(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	// Insert a..z
	ins := eng.Begin(mvcc.SnapshotIsolation)
	for c := byte('a'); c <= 'z'; c++ {
		k := []byte{c}
		require.NoError(t, eng.Put(ins, k, []byte("v_"+string(c))))
	}
	require.NoError(t, eng.Commit(ins))

	// Scan f..t (inclusive start, exclusive end by cursor convention — but
	// our cursor returns keys <= end so use "u" as exclusive upper bound)
	reader := eng.Begin(mvcc.SnapshotIsolation)
	cursor, err := eng.Scan(reader, []byte("f"), []byte("t"))
	require.NoError(t, err)
	defer cursor.Close()

	var got []string
	for {
		tuple, err := cursor.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, string(tuple.Key))
	}
	eng.Commit(reader) //nolint

	expected := []string{"f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"}
	assert.Equal(t, expected, got, "range scan f..t must return exactly f through t")
}

// TestScan_MVCCVisibility verifies a range scan only returns the latest
// committed version of each key visible to the scanning transaction's snapshot.
func TestScan_MVCCVisibility(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	// Insert v1 of "key"
	t1 := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(t1, []byte("key"), []byte("v1")))
	require.NoError(t, eng.Commit(t1))

	// Update to v2
	t2 := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(t2, []byte("key"), []byte("v2")))
	require.NoError(t, eng.Commit(t2))

	// A new scan must see only v2 (latest committed version)
	reader := eng.Begin(mvcc.SnapshotIsolation)
	cursor, err := eng.Scan(reader, []byte("key"), []byte("key"))
	require.NoError(t, err)
	defer cursor.Close()

	tuple, err := cursor.Next()
	require.NoError(t, err, "key must be found in scan")
	assert.Equal(t, []byte("v2"), tuple.Value, "scan must see latest committed version")

	_, err = cursor.Next()
	assert.Equal(t, io.EOF, err, "only one key in range")
	eng.Commit(reader) //nolint
}

// TestScan_EmptyRange verifies scanning an empty range returns no results.
func TestScan_EmptyRange(t *testing.T) {
	dir := t.TempDir()
	eng := openEngine(t, dir)
	defer func() { _ = eng.Close() }()

	// Insert keys aa, bb, cc
	ins := eng.Begin(mvcc.SnapshotIsolation)
	require.NoError(t, eng.Put(ins, []byte("aa"), []byte("1")))
	require.NoError(t, eng.Put(ins, []byte("bb"), []byte("2")))
	require.NoError(t, eng.Put(ins, []byte("cc"), []byte("3")))
	require.NoError(t, eng.Commit(ins))

	// Scan "dd".."zz" — should be empty
	reader := eng.Begin(mvcc.SnapshotIsolation)
	cursor, err := eng.Scan(reader, []byte("dd"), []byte("zz"))
	require.NoError(t, err)
	defer cursor.Close()

	_, err = cursor.Next()
	assert.Equal(t, io.EOF, err, "scan of empty range must return EOF immediately")
	eng.Commit(reader) //nolint
}

// TestARIES_FullRecovery commits 1000 keys across multiple transactions,
// crashes, recovers, and verifies all committed keys are present.
func TestARIES_FullRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: commit 1000 keys across 5 transactions, then crash
	{
		eng := openEngine(t, dir)
		for batch := 0; batch < 5; batch++ {
			txn := eng.Begin(mvcc.SnapshotIsolation)
			for i := 0; i < 200; i++ {
				idx := batch*200 + i
				k := []byte{byte(idx >> 8), byte(idx)}
				_ = k
				require.NoError(t, eng.Put(txn,
					[]byte{byte((batch*200 + i) >> 8), byte(batch*200 + i), 'k'},
					[]byte{byte(batch), byte(i), 'v'},
				))
			}
			require.NoError(t, eng.Commit(txn))
		}
		eng.CrashClose()
	}

	// Phase 2: recover and verify all 1000 keys
	{
		eng := openEngine(t, dir)
		defer func() { _ = eng.Close() }()

		reader := eng.Begin(mvcc.SnapshotIsolation)
		found := 0
		for batch := 0; batch < 5; batch++ {
			for i := 0; i < 200; i++ {
				val, err := eng.Get(reader,
					[]byte{byte((batch*200 + i) >> 8), byte(batch*200 + i), 'k'})
				if err == nil && len(val) == 3 && val[0] == byte(batch) {
					found++
				}
			}
		}
		eng.Commit(reader) //nolint
		assert.Equal(t, 1000, found, "all 1000 committed keys must survive crash+recovery")
	}
}
