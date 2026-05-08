package buffer

import (
	"fmt"
	"sync"
	"sync/atomic"

	"btree-engine/internal/events"
	"btree-engine/internal/page"
)

// DefaultPoolSize is the default number of buffer frames when no size is specified.
const DefaultPoolSize = 256

// WALFlusher is the minimal WAL interface the buffer pool needs.
type WALFlusher interface {
	FlushUpTo(lsn uint64) error
	CurrentLSN() uint64
}

// DiskRW is the minimal disk interface the buffer pool needs.
type DiskRW interface {
	ReadPage(pageID uint32, buf []byte) error
	WritePage(pageID uint32, data []byte) error
}

// BufferPool manages a fixed set of in-memory frames for page caching.
type BufferPool struct {
	mu             sync.Mutex
	frames         []*Frame
	frameIdx       map[*Frame]int    // frame pointer -> index (for policy.OnAccess)
	pageTable      map[uint32]*Frame // pageID -> frame
	dirtyPageTable map[uint32]uint64 // pageID -> recLSN
	hits           atomic.Uint64
	misses         atomic.Uint64
	disk           DiskRW
	wal            WALFlusher
	bus            *events.EventBus
	policy         EvictionPolicy
}

// New creates a BufferPool with poolSize frames backed by disk and wal.
// The default eviction policy is Clock.
func New(poolSize int, disk DiskRW, wal WALFlusher, bus *events.EventBus) *BufferPool {
	frames := make([]*Frame, poolSize)
	idx := make(map[*Frame]int, poolSize)
	for i := range frames {
		frames[i] = &Frame{}
		idx[frames[i]] = i
	}
	return &BufferPool{
		frames:         frames,
		frameIdx:       idx,
		pageTable:      make(map[uint32]*Frame),
		dirtyPageTable: make(map[uint32]uint64),
		disk:           disk,
		wal:            wal,
		bus:            bus,
		policy:         NewClockPolicy(),
	}
}

// SetEvictionPolicy swaps the eviction policy.  Must be called before any
// pages are loaded (i.e., right after New or after all pages are flushed).
func (bp *BufferPool) SetEvictionPolicy(p EvictionPolicy) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.policy = p
}

// FetchPage pins a page into the buffer pool, loading from disk if not cached.
func (bp *BufferPool) FetchPage(pageID uint32) (*Frame, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if frame, ok := bp.pageTable[pageID]; ok {
		atomic.AddInt32(&frame.PinCount, 1)
		bp.policy.OnAccess(bp.frameIdx[frame], bp.frames)
		bp.hits.Add(1)
		if bp.bus != nil {
			bp.bus.Publish(events.Event{Type: events.EvtBufferHit,
				Extra: map[string]interface{}{"page_id": pageID}})
		}
		return frame, nil
	}

	// Cache miss: evict a victim frame
	frame, err := bp.evict()
	if err != nil {
		return nil, err
	}

	// Load the page into the frame
	if err := bp.disk.ReadPage(pageID, frame.Data[:]); err != nil {
		return nil, err
	}
	// Verify full-page checksum. Type==0 means freshly allocated / freed page — skip check.
	{
		var tmp page.Page
		copy(tmp.Data[:], frame.Data[:])
		if tmp.Type() != 0 && !tmp.VerifyChecksum() {
			return nil, fmt.Errorf("page %d: checksum mismatch (possible torn write or corruption)", pageID)
		}
	}
	frame.PageID = pageID
	frame.PinCount = 1
	bp.policy.OnAccess(bp.frameIdx[frame], bp.frames)
	bp.pageTable[pageID] = frame
	bp.misses.Add(1)

	if bp.bus != nil {
		bp.bus.Publish(events.Event{Type: events.EvtBufferMiss,
			Extra: map[string]interface{}{"page_id": pageID}})
	}
	return frame, nil
}

// UnpinPage decrements the pin count. If dirty=true the frame is marked dirty
// and its recLSN is recorded for ARIES.
func (bp *BufferPool) UnpinPage(pageID uint32, dirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, ok := bp.pageTable[pageID]
	if !ok {
		return
	}
	atomic.AddInt32(&frame.PinCount, -1)
	if dirty && !frame.Dirty {
		frame.Dirty = true
		if _, exists := bp.dirtyPageTable[pageID]; !exists {
			var lsn uint64
			if bp.wal != nil {
				lsn = bp.wal.CurrentLSN()
			}
			bp.dirtyPageTable[pageID] = lsn
			frame.SetRecLSN(lsn)
		}
		if bp.bus != nil {
			bp.bus.Publish(events.Event{Type: events.EvtPageDirty,
				Extra: map[string]interface{}{"page_id": pageID}})
		}
	}
}

// WritePageData copies updated page bytes back into the frame (after modifying a page in-place).
func (bp *BufferPool) WritePageData(pageID uint32, p *page.Page) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if frame, ok := bp.pageTable[pageID]; ok {
		copy(frame.Data[:], p.Data[:])
	}
}

// ReadPageData reads the frame's current bytes into a page struct.
func (bp *BufferPool) ReadPageData(frame *Frame) *page.Page {
	p := &page.Page{ID: frame.PageID}
	copy(p.Data[:], frame.Data[:])
	return p
}

// FlushDirtyPages writes all dirty pages to disk (for checkpoint).
func (bp *BufferPool) FlushDirtyPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for pageID, recLSN := range bp.dirtyPageTable {
		frame := bp.pageTable[pageID]
		if frame == nil {
			continue
		}
		if bp.wal != nil {
			if err := bp.wal.FlushUpTo(recLSN); err != nil {
				return err
			}
		}
		if err := bp.disk.WritePage(pageID, frame.Data[:]); err != nil {
			return err
		}
		frame.Dirty = false
		frame.recLSN = 0
	}
	bp.dirtyPageTable = make(map[uint32]uint64)
	return nil
}

// HitRate returns the cache hit ratio (0.0–1.0).
func (bp *BufferPool) HitRate() float64 {
	h := bp.hits.Load()
	m := bp.misses.Load()
	total := h + m
	if total == 0 {
		return 0
	}
	return float64(h) / float64(total)
}

// DirtyPageTable returns a snapshot of dirty pages for ARIES checkpointing.
func (bp *BufferPool) DirtyPageTable() map[uint32]uint64 {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	cp := make(map[uint32]uint64, len(bp.dirtyPageTable))
	for k, v := range bp.dirtyPageTable {
		cp[k] = v
	}
	return cp
}

// InvalidatePage removes a page from the buffer pool without writing it to disk.
// Must be called only after the page has been unpinned (PinCount == 0).
// Used when a page is freed so its stale dirty frame cannot overwrite the
// on-disk freelist data that disk.FreePage already wrote.
func (bp *BufferPool) InvalidatePage(pageID uint32) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	frame, ok := bp.pageTable[pageID]
	if !ok {
		return
	}
	delete(bp.pageTable, pageID)
	delete(bp.dirtyPageTable, pageID)
	frame.Reset()
}

// FrameCount returns the total number of frames in the pool.
func (bp *BufferPool) FrameCount() int { return len(bp.frames) }

// FrameSnapshot is a read-only snapshot of one buffer pool frame.
type FrameSnapshot struct {
	PageID   uint32
	Dirty    bool
	PinCount int32
	RecLSN   uint64
}

// SnapshotFrames returns a point-in-time snapshot of all frames (for inspection).
func (bp *BufferPool) SnapshotFrames() []FrameSnapshot {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	snaps := make([]FrameSnapshot, len(bp.frames))
	for i, f := range bp.frames {
		snaps[i] = FrameSnapshot{
			PageID:   f.PageID,
			Dirty:    f.Dirty,
			PinCount: f.PinCount,
			RecLSN:   f.recLSN,
		}
	}
	return snaps
}

// helper event constructors
func pageWriteEvent(id uint32) events.Event {
	return events.Event{Type: events.EvtPageWrite,
		Extra: map[string]interface{}{"page_id": id}}
}
func pageEvictEvent(id uint32, dirty bool) events.Event {
	return events.Event{Type: events.EvtPageEvict,
		Extra: map[string]interface{}{"page_id": id, "dirty": dirty}}
}
