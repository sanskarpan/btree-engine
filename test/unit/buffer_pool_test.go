package unit

import (
	"sync"
	"testing"

	"btree-engine/internal/buffer"
	"btree-engine/internal/page"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDisk is a simple in-memory disk for testing.
type mockDisk struct {
	mu    sync.Mutex
	pages map[uint32][page.PageSize]byte
}

func newMockDisk() *mockDisk { return &mockDisk{pages: make(map[uint32][page.PageSize]byte)} }

func (d *mockDisk) ReadPage(pageID uint32, buf []byte) error {
	d.mu.Lock()
	p := d.pages[pageID]
	d.mu.Unlock()
	copy(buf, p[:])
	return nil
}

func (d *mockDisk) WritePage(pageID uint32, data []byte) error {
	d.mu.Lock()
	var p [page.PageSize]byte
	copy(p[:], data)
	d.pages[pageID] = p
	d.mu.Unlock()
	return nil
}

// mockWAL tracks FlushUpTo calls for WAL-before-write verification.
type mockWAL struct {
	mu          sync.Mutex
	flushedUpTo []uint64
	currentLSN  uint64
}

func (w *mockWAL) FlushUpTo(lsn uint64) error {
	w.mu.Lock()
	w.flushedUpTo = append(w.flushedUpTo, lsn)
	w.mu.Unlock()
	return nil
}
func (w *mockWAL) CurrentLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLSN
}

func TestBuffer_FetchPin(t *testing.T) {
	disk := newMockDisk()
	bp := buffer.New(8, disk, nil, nil)

	f1, err := bp.FetchPage(10)
	require.NoError(t, err)
	assert.Equal(t, int32(1), f1.PinCount)

	f2, err := bp.FetchPage(10)
	require.NoError(t, err)
	assert.Equal(t, int32(2), f2.PinCount)
	assert.Same(t, f1, f2)

	bp.UnpinPage(10, false)
	assert.Equal(t, int32(1), f1.PinCount)
	bp.UnpinPage(10, false)
	assert.Equal(t, int32(0), f1.PinCount)
}

func TestBuffer_ClockEviction(t *testing.T) {
	disk := newMockDisk()
	bp := buffer.New(4, disk, nil, nil) // small pool

	// Fill all 4 frames
	frames := make([]*buffer.Frame, 4)
	for i := 0; i < 4; i++ {
		f, err := bp.FetchPage(uint32(i + 1))
		require.NoError(t, err)
		frames[i] = f
		bp.UnpinPage(uint32(i+1), false) // unpin so they are evictable
	}

	// Fetch a 5th page — must evict one of the existing ones
	f5, err := bp.FetchPage(5)
	require.NoError(t, err)
	assert.NotNil(t, f5)
	bp.UnpinPage(5, false)
}

func TestBuffer_DirtyNotLostBeforeWAL(t *testing.T) {
	disk := newMockDisk()
	wal := &mockWAL{currentLSN: 100}
	bp := buffer.New(2, disk, wal, nil) // tiny pool to force eviction

	// Fetch page 1, mark dirty
	f1, err := bp.FetchPage(1)
	require.NoError(t, err)
	p := bp.ReadPageData(f1)
	p.Init(page.PageTypeLeaf)
	p.SetPageLSN(125)
	bp.WritePageData(1, p)
	bp.UnpinPage(1, true) // dirty!

	// Fetch two more pages — forces eviction of page 1
	_, err = bp.FetchPage(2)
	require.NoError(t, err)
	bp.UnpinPage(2, false)

	_, err = bp.FetchPage(3)
	require.NoError(t, err)
	bp.UnpinPage(3, false)

	// Now fetch page 4 to force eviction of dirty page 1
	_, err = bp.FetchPage(4)
	require.NoError(t, err)
	bp.UnpinPage(4, false)

	_ = f1 // used to silence linter

	// WAL FlushUpTo must have been called before writing dirty page
	wal.mu.Lock()
	flushCalls := len(wal.flushedUpTo)
	lastFlush := wal.flushedUpTo[len(wal.flushedUpTo)-1]
	wal.mu.Unlock()
	assert.Greater(t, flushCalls, 0, "WAL FlushUpTo must be called before evicting dirty page")
	assert.Equal(t, uint64(125), lastFlush, "eviction must flush WAL through the page LSN")
}

func TestBuffer_HitRate(t *testing.T) {
	disk := newMockDisk()
	bp := buffer.New(8, disk, nil, nil)

	// First fetch = miss
	bp.FetchPage(1) //nolint
	bp.UnpinPage(1, false)
	// Second fetch = hit
	bp.FetchPage(1) //nolint
	bp.UnpinPage(1, false)

	hr := bp.HitRate()
	assert.InDelta(t, 0.5, hr, 0.01)
}

func TestBuffer_UsesPageLSNForRecLSN(t *testing.T) {
	disk := newMockDisk()
	wal := &mockWAL{currentLSN: 999}
	bp := buffer.New(2, disk, wal, nil)

	frame, err := bp.FetchPage(7)
	require.NoError(t, err)
	p := bp.ReadPageData(frame)
	p.Init(page.PageTypeLeaf)
	p.SetPageLSN(42)
	bp.WritePageData(7, p)
	bp.UnpinPage(7, true)

	dpt := bp.DirtyPageTable()
	assert.Equal(t, uint64(42), dpt[7])
}

func TestBuffer_FlushDirtyPages_UsesCurrentPageLSN(t *testing.T) {
	disk := newMockDisk()
	wal := &mockWAL{currentLSN: 50}
	bp := buffer.New(2, disk, wal, nil)

	frame, err := bp.FetchPage(8)
	require.NoError(t, err)
	p := bp.ReadPageData(frame)
	p.Init(page.PageTypeLeaf)
	p.SetPageLSN(42)
	bp.WritePageData(8, p)
	bp.UnpinPage(8, true)

	frame, err = bp.FetchPage(8)
	require.NoError(t, err)
	p = bp.ReadPageData(frame)
	p.SetPageLSN(99)
	bp.WritePageData(8, p)
	bp.UnpinPage(8, false)

	require.NoError(t, bp.FlushDirtyPages())

	wal.mu.Lock()
	defer wal.mu.Unlock()
	require.NotEmpty(t, wal.flushedUpTo)
	assert.Equal(t, uint64(99), wal.flushedUpTo[len(wal.flushedUpTo)-1])
}
