// Package buffer implements the buffer pool manager with Clock eviction.
package buffer

import (
	"btree-engine/internal/page"
)

// Frame is one buffer pool slot holding a single 4096-byte page.
type Frame struct {
	PageID   uint32
	Data     [page.PageSize]byte
	PinCount int32 // access via sync/atomic
	Dirty    bool
	RefBit   bool   // Clock second-chance bit
	recLSN   uint64 // ARIES: LSN of first write that dirtied this frame

	// LRU-K eviction tracking.
	// accessHist[0] = 2nd most recent access tick; accessHist[1] = most recent.
	// accessCount tracks total accesses (capped behaviour after K).
	accessCount uint64
	accessHist  [2]uint64
}

// RecLSN returns the recovery LSN (first dirtying LSN) for ARIES.
func (f *Frame) RecLSN() uint64 { return f.recLSN }

// SetRecLSN sets the recLSN if not already set (first-dirty wins).
func (f *Frame) SetRecLSN(lsn uint64) {
	if f.recLSN == 0 {
		f.recLSN = lsn
	}
}

// Reset clears the frame state (called when a frame is evicted and reused).
func (f *Frame) Reset() {
	f.PageID = 0
	f.Dirty = false
	f.RefBit = false
	f.recLSN = 0
	f.accessCount = 0
	f.accessHist[0] = 0
	f.accessHist[1] = 0
}

// Page returns a *page.Page view of this frame's data buffer.
func (f *Frame) Page(id uint32) *page.Page {
	p := &page.Page{ID: id}
	copy(p.Data[:], f.Data[:])
	return p
}
