package disk

import (
	"encoding/binary"
	"sync"

	"btree-engine/internal/page"
)

// freeListMu serialises alloc/free operations so the meta page stays consistent.
var freeListMu sync.Mutex

// AllocPage returns the next available page ID, reusing freed pages when possible.
func (dm *DiskManager) AllocPage() (uint32, error) {
	freeListMu.Lock()
	defer freeListMu.Unlock()

	meta, err := dm.ReadMeta()
	if err != nil {
		return 0, err
	}

	if meta.FreeListHead != 0 {
		// Reuse the head of the free list
		pageID := meta.FreeListHead
		var buf [page.PageSize]byte
		if err := dm.ReadPage(pageID, buf[:]); err != nil {
			return 0, err
		}
		meta.FreeListHead = binary.LittleEndian.Uint32(buf[0:])
		if err := dm.WriteMeta(meta); err != nil {
			return 0, err
		}
		return pageID, nil
	}

	// Grow the file
	pageID := meta.NextPageID
	meta.NextPageID++
	if err := dm.WriteMeta(meta); err != nil {
		return 0, err
	}
	// Zero-fill the new page
	var blank [page.PageSize]byte
	if err := dm.WritePageRaw(pageID, blank[:]); err != nil {
		return 0, err
	}
	return pageID, nil
}

// FreePage returns pageID to the free list.
func (dm *DiskManager) FreePage(pageID uint32) error {
	freeListMu.Lock()
	defer freeListMu.Unlock()

	meta, err := dm.ReadMeta()
	if err != nil {
		return err
	}

	// Store old head in this page's first 4 bytes
	var buf [page.PageSize]byte
	binary.LittleEndian.PutUint32(buf[0:], meta.FreeListHead)
	if err := dm.WritePageRaw(pageID, buf[:]); err != nil {
		return err
	}

	meta.FreeListHead = pageID
	return dm.WriteMeta(meta)
}
