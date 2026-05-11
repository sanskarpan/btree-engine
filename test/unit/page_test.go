package unit

import (
	"fmt"
	"testing"

	"btree-engine/internal/page"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPage_InsertAndGet(t *testing.T) {
	p := &page.Page{ID: 1}
	p.EncodeHeader(page.PageHeader{
		Type:      page.PageTypeLeaf,
		FreeStart: page.HeaderSize,
		FreeEnd:   page.PageSize,
	})

	keyExtract := func(d []byte) []byte { return d[:6] } // "keyNN:" prefix

	for i := 0; i < 50; i++ {
		data := []byte(fmt.Sprintf("key%02d:value%02d", i, i))
		slotIdx, err := p.InsertRecord(data, keyExtract)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, slotIdx, 0)
	}
	h := p.DecodeHeader()
	assert.Equal(t, uint16(50), h.NumSlots)

	// Verify all records readable via binary search
	for i := 0; i < 50; i++ {
		expected := fmt.Sprintf("key%02d:value%02d", i, i)
		idx := p.BinarySearch([]byte(expected[:6]), keyExtract)
		require.GreaterOrEqual(t, idx, 0, "key%02d not found", i)
		assert.Equal(t, []byte(expected), p.GetRecord(idx))
	}
}

func TestPage_BinarySearch(t *testing.T) {
	p := &page.Page{ID: 2}
	p.EncodeHeader(page.PageHeader{
		Type:      page.PageTypeLeaf,
		FreeStart: page.HeaderSize,
		FreeEnd:   page.PageSize,
	})

	// Insert 50 keys in random-ish order
	keys := make([]string, 50)
	for i := 0; i < 50; i++ {
		keys[i] = fmt.Sprintf("k%03d", (i*17)%50) // pseudo-random order
	}
	extract := func(d []byte) []byte { return d[:4] }
	inserted := map[string]bool{}
	for _, k := range keys {
		if inserted[k] {
			continue
		}
		inserted[k] = true
		_, err := p.InsertRecord([]byte(k+"_val"), extract)
		require.NoError(t, err)
	}

	// All inserted keys must be findable
	for k := range inserted {
		idx := p.BinarySearch([]byte(k), extract)
		assert.GreaterOrEqual(t, idx, 0, "key %q not found", k)
	}
}

func TestPage_Compact(t *testing.T) {
	p := &page.Page{ID: 3}
	p.EncodeHeader(page.PageHeader{
		Type:      page.PageTypeLeaf,
		FreeStart: page.HeaderSize,
		FreeEnd:   page.PageSize,
	})
	extract := func(d []byte) []byte { return d[:4] }

	for i := 0; i < 20; i++ {
		_, err := p.InsertRecord([]byte(fmt.Sprintf("k%03d:data", i)), extract)
		require.NoError(t, err)
	}

	// Delete 10 records
	for i := 0; i < 10; i++ {
		idx := p.BinarySearch([]byte(fmt.Sprintf("k%03d", i)), extract)
		require.GreaterOrEqual(t, idx, 0)
		p.DeleteRecord(idx)
	}

	fsBefore := p.FreeSpace()
	p.Compact()
	fsAfter := p.FreeSpace()

	h := p.DecodeHeader()
	assert.Equal(t, uint16(10), h.NumSlots, "should have 10 live records after compact")
	assert.Greater(t, fsAfter, fsBefore, "free space must increase after compaction")
}

func TestPage_ChecksumDetectsCorruption(t *testing.T) {
	p := &page.Page{ID: 4}
	p.EncodeHeader(page.PageHeader{
		Type:      page.PageTypeLeaf,
		FreeStart: page.HeaderSize,
		FreeEnd:   page.PageSize,
	})
	assert.True(t, p.VerifyChecksum(), "fresh page must have valid checksum")

	// Corrupt one byte in the DATA area (outside the header) — full-page checksum detects this.
	p.Data[200] ^= 0xFF
	assert.False(t, p.VerifyChecksum(), "full-page checksum must detect data-area corruption")
	p.Data[200] ^= 0xFF // restore

	// Corrupt a header byte.
	p.Data[5] ^= 0xFF // flip a bit in NumSlots
	assert.False(t, p.VerifyChecksum(), "full-page checksum must detect header corruption")
}

// TestPage_BinarySearch_WithTombstones verifies that BinarySearch returns correct
// results even when tombstone slots (Length=0) exist between live records.
// Before the fix, a tombstone mid-search caused BinarySearch to jump lo past live
// records, making Get() return KeyNotFound for existing keys.
func TestPage_BinarySearch_WithTombstones(t *testing.T) {
	extract := func(d []byte) []byte { return d[:4] }

	// Build: ["k000", "k001", "k002", "k003", "k004"]
	p := &page.Page{ID: 10}
	p.EncodeHeader(page.PageHeader{
		Type:      page.PageTypeLeaf,
		FreeStart: page.HeaderSize,
		FreeEnd:   page.PageSize,
	})
	for i := 0; i < 5; i++ {
		_, err := p.InsertRecord([]byte(fmt.Sprintf("k%03d_v", i)), extract)
		require.NoError(t, err)
	}

	// Tombstone k002 (slot 2) — simulates what vacuum does without compaction
	idx := p.BinarySearch([]byte("k002"), extract)
	require.GreaterOrEqual(t, idx, 0)
	p.DeleteRecord(idx)

	// All remaining live keys must still be findable
	for _, k := range []string{"k000", "k001", "k003", "k004"} {
		got := p.BinarySearch([]byte(k), extract)
		assert.GreaterOrEqual(t, got, 0, "key %q not found after tombstoning k002", k)
		if got >= 0 {
			data := p.GetRecord(got)
			require.NotNil(t, data, "GetRecord returned nil for %q", k)
			assert.Equal(t, k, string(data[:4]))
		}
	}

	// Inserting a new key whose sort position is BEFORE the tombstone must not corrupt order
	_, err := p.InsertRecord([]byte("k001_new"), extract) // same key prefix, sorts between k001 and k002
	require.NoError(t, err)
	gotK000 := p.BinarySearch([]byte("k000"), extract)
	assert.GreaterOrEqual(t, gotK000, 0, "k000 must still be findable after insert near tombstone")
}

func TestPage_FreeSpace(t *testing.T) {
	p := &page.Page{ID: 5}
	p.EncodeHeader(page.PageHeader{
		Type:      page.PageTypeLeaf,
		FreeStart: page.HeaderSize,
		FreeEnd:   page.PageSize,
	})
	initial := p.FreeSpace()
	assert.Equal(t, page.PageSize-page.HeaderSize, initial)

	extract := func(d []byte) []byte { return d[:4] }
	data := []byte("key1_somevalue")
	_, err := p.InsertRecord(data, extract)
	require.NoError(t, err)

	after := p.FreeSpace()
	assert.Equal(t, initial-len(data)-page.SlotSize, after)
}
