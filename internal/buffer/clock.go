package buffer

import "errors"

// ErrBufferPoolFull is returned when all frames are pinned and no eviction is possible.
var ErrBufferPoolFull = errors.New("buffer pool full: all frames pinned")

// evict selects a victim frame via the current EvictionPolicy, writes it back
// if dirty, removes it from the page table, and returns the cleared frame.
// Must be called with bp.mu held.
func (bp *BufferPool) evict() (*Frame, error) {
	victimIdx := bp.policy.ChooseVictim(bp.frames)
	if victimIdx < 0 {
		return nil, ErrBufferPoolFull
	}
	frame := bp.frames[victimIdx]

	if frame.Dirty {
		// WAL rule: flush log up to recLSN before writing dirty page to disk.
		if bp.wal != nil {
			if err := bp.wal.FlushUpTo(frame.recLSN); err != nil {
				return nil, err
			}
		}
		if err := bp.disk.WritePage(frame.PageID, frame.Data[:]); err != nil {
			return nil, err
		}
		delete(bp.dirtyPageTable, frame.PageID)
		if bp.bus != nil {
			bp.bus.Publish(pageWriteEvent(frame.PageID))
		}
	}
	delete(bp.pageTable, frame.PageID)
	if bp.bus != nil {
		bp.bus.Publish(pageEvictEvent(frame.PageID, frame.Dirty))
	}
	frame.Reset()
	return frame, nil
}
