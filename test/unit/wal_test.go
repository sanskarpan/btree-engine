package unit

import (
	"os"
	"path/filepath"
	"testing"

	"btree-engine/internal/wal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestWAL(t *testing.T) *wal.WALManager {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := wal.Open(path, 65536, false, 0, "", nil) // no fsync, no rotation, no archive
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestWAL_AppendAndScan(t *testing.T) {
	w := openTestWAL(t)

	// Append 100 records
	var lsns []uint64
	for i := 0; i < 100; i++ {
		lsn := w.AppendRecord(wal.LogRecord{
			Type:    wal.LogInsert,
			TxnID:   uint64(i + 1),
			Payload: []byte("payload"),
		})
		lsns = append(lsns, lsn)
	}

	// Scan and collect
	var scanned []wal.LogRecord
	require.NoError(t, w.ScanFrom(0, func(r wal.LogRecord) {
		scanned = append(scanned, r)
	}))

	assert.Equal(t, 100, len(scanned))
	for i, r := range scanned {
		assert.Equal(t, uint64(i+1), r.TxnID)
		assert.Equal(t, lsns[i], r.LSN)
	}
}

func TestWAL_CRCDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.wal")
	w, err := wal.Open(path, 65536, false, 0, "", nil)
	require.NoError(t, err)

	lsn := w.AppendRecord(wal.LogRecord{
		Type:    wal.LogInsert,
		TxnID:   42,
		Payload: []byte("important data"),
	})
	require.NoError(t, w.FlushUpTo(lsn))
	segPath := w.ActiveSegmentPath() // get actual segment path before close
	_ = w.Close()

	// Corrupt one byte in the payload region of the segment file.
	data, err := os.ReadFile(segPath)
	require.NoError(t, err)
	data[40] ^= 0xFF // flip a bit in the payload
	require.NoError(t, os.WriteFile(segPath, data, 0644))

	// Reopen and scan — corrupt record should be skipped.
	w2, err := wal.Open(path, 65536, false, 0, "", nil)
	require.NoError(t, err)
	defer func() { _ = w2.Close() }()

	var count int
	w2.ScanFrom(0, func(r wal.LogRecord) { count++ }) //nolint
	// The corrupt record is skipped (CRC mismatch), so count should be 0
	assert.Equal(t, 0, count, "corrupt record must be skipped")
}

func TestWAL_CommitFlushes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit.wal")
	// Open with syncOnCommit=true
	w, err := wal.Open(path, 65536, true, 0, "", nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	// Write a BEGIN first so Commit's LSN is > 0
	w.LogBeginTxn(1)
	lsn := w.Commit(1, 0)
	assert.Greater(t, lsn, uint64(0))

	// After Commit, flushedLSN must be >= commitLSN
	assert.GreaterOrEqual(t, w.FlushedLSN(), lsn)
}

func TestWAL_FetchRecord(t *testing.T) {
	w := openTestWAL(t)

	lsn1 := w.AppendRecord(wal.LogRecord{Type: wal.LogBegin, TxnID: 1})
	lsn2 := w.AppendRecord(wal.LogRecord{Type: wal.LogInsert, TxnID: 1, PrevLSN: lsn1, Payload: []byte("data")})
	_ = w.FlushUpTo(lsn2)

	r, err := w.FetchRecord(lsn2)
	require.NoError(t, err)
	assert.Equal(t, wal.LogInsert, r.Type)
	assert.Equal(t, uint64(1), r.TxnID)
	assert.Equal(t, []byte("data"), r.Payload)
}
