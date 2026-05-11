package unit

import (
	"testing"

	"btree-engine/internal/disk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisk_ReadWritePage(t *testing.T) {
	dir := t.TempDir()
	dm, err := disk.Open(dir + "/test.db")
	require.NoError(t, err)
	defer func() { _ = dm.Close() }()

	// Write a recognisable pattern to page 5
	var buf [4096]byte
	for i := range buf {
		buf[i] = byte(i % 251)
	}
	require.NoError(t, dm.WritePage(5, buf[:]))

	var got [4096]byte
	require.NoError(t, dm.ReadPage(5, got[:]))
	assert.Equal(t, buf, got)
}

func TestDisk_FreeList(t *testing.T) {
	dir := t.TempDir()
	dm, err := disk.Open(dir + "/test.db")
	require.NoError(t, err)
	defer func() { _ = dm.Close() }()

	// Allocate 10 pages
	ids := make([]uint32, 10)
	for i := range ids {
		id, err := dm.AllocPage()
		require.NoError(t, err)
		ids[i] = id
	}
	// All IDs must be unique
	seen := map[uint32]bool{}
	for _, id := range ids {
		assert.False(t, seen[id], "duplicate page ID %d", id)
		seen[id] = true
	}

	// Free 5 pages
	freed := ids[:5]
	for _, id := range freed {
		require.NoError(t, dm.FreePage(id))
	}

	// Allocate 5 more — they must reuse freed pages
	reused := map[uint32]bool{}
	for _, id := range freed {
		reused[id] = true
	}
	for i := 0; i < 5; i++ {
		id, err := dm.AllocPage()
		require.NoError(t, err)
		assert.True(t, reused[id], "expected reuse of freed page, got new page %d", id)
		delete(reused, id)
	}
}

func TestDisk_MetaRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dm, err := disk.Open(dir + "/test.db")
	require.NoError(t, err)
	defer func() { _ = dm.Close() }()

	meta, err := dm.ReadMeta()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), meta.RootPageID)

	meta.TxnIDCounter = 42
	require.NoError(t, dm.WriteMeta(meta))

	meta2, err := dm.ReadMeta()
	require.NoError(t, err)
	assert.Equal(t, uint64(42), meta2.TxnIDCounter)
}
