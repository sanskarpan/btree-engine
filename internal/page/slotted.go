package page

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// ErrPageFull is returned when there is not enough free space to insert a record.
var ErrPageFull = errors.New("page full")

// Slot is the decoded form of a 4-byte slot entry.
type Slot struct {
	Offset uint16
	Length uint16
}

// GetSlot reads the slot at the given index.
func (p *Page) GetSlot(idx int) Slot {
	base := HeaderSize + idx*SlotSize
	return Slot{
		Offset: binary.LittleEndian.Uint16(p.Data[base:]),
		Length: binary.LittleEndian.Uint16(p.Data[base+2:]),
	}
}

// SetSlot writes the slot at the given index (does NOT update the checksum —
// callers do that via EncodeHeader after all header fields are final).
func (p *Page) SetSlot(idx int, offset, length uint16) {
	base := HeaderSize + idx*SlotSize
	binary.LittleEndian.PutUint16(p.Data[base:], offset)
	binary.LittleEndian.PutUint16(p.Data[base+2:], length)
}

// InsertRecord inserts data into the page maintaining key-sorted slot order.
// keyExtract returns the sort key for a record. Returns the slot index of the new entry.
func (p *Page) InsertRecord(data []byte, keyExtract func([]byte) []byte) (slotIdx int, err error) {
	h := p.DecodeHeader()
	needed := len(data) + SlotSize
	if int(h.FreeEnd)-int(h.FreeStart) < needed {
		return 0, ErrPageFull
	}

	// Write data at freeEnd - len(data)
	newFreeEnd := h.FreeEnd - uint16(len(data))
	copy(p.Data[newFreeEnd:], data)

	// Binary search for sorted insertion position
	newKey := keyExtract(data)
	insertPos := p.binarySearchInsertPos(int(h.NumSlots), newKey, keyExtract)

	// Shift slots right to make room
	slotBase := HeaderSize + int(h.NumSlots)*SlotSize
	copy(p.Data[HeaderSize+(insertPos+1)*SlotSize:slotBase+SlotSize],
		p.Data[HeaderSize+insertPos*SlotSize:slotBase])

	// Write new slot
	p.SetSlot(insertPos, newFreeEnd, uint16(len(data)))

	// Update header
	h.NumSlots++
	h.FreeStart += SlotSize
	h.FreeEnd = newFreeEnd
	p.EncodeHeader(h)

	return insertPos, nil
}

// GetRecord returns the raw data bytes for slot idx. Returns nil for tombstones (length=0).
func (p *Page) GetRecord(slotIdx int) []byte {
	s := p.GetSlot(slotIdx)
	if s.Length == 0 {
		return nil // tombstone
	}
	return p.Data[s.Offset : s.Offset+s.Length]
}

// DeleteRecord marks a slot as a tombstone (length = 0) and refreshes the checksum.
func (p *Page) DeleteRecord(slotIdx int) {
	s := p.GetSlot(slotIdx)
	p.SetSlot(slotIdx, s.Offset, 0)
	p.UpdateChecksum()
}

// BinarySearch searches the sorted slot array for key.
// Returns the slot index of the leftmost match if found, or -(insertionPoint)-1 if not found.
//
// Tombstone handling: when a slot has Length=0, its original key is unknown.
// We scan left within the current search range to find a live anchor, then
// use that key to determine which half of the range to continue searching.
func (p *Page) BinarySearch(key []byte, keyExtract func([]byte) []byte) int {
	numSlots := p.NumSlots()
	lo, hi := 0, numSlots-1
	result := -1 // index of leftmost match found so far (-1 = none)
	for lo <= hi {
		mid := (lo + hi) / 2
		slotData := p.GetRecord(mid)
		if slotData == nil {
			// tombstone — find nearest live record in [lo..mid-1] for directional guidance.
			liveIdx, liveKey := p.nearestLiveLeft(lo, mid-1, keyExtract)
			if liveIdx >= 0 {
				cmp := bytes.Compare(liveKey, key)
				if cmp == 0 {
					result = liveIdx
					hi = liveIdx - 1 // keep searching left for an earlier occurrence
				} else if cmp < 0 {
					lo = mid + 1 // live anchor < target → target is to the right
				} else {
					hi = liveIdx - 1 // live anchor > target → target is to the left
				}
			} else {
				lo = mid + 1 // no live records in [lo..mid], go right
			}
			continue
		}
		cmp := bytes.Compare(keyExtract(slotData), key)
		if cmp == 0 {
			result = mid  // found a match; record it
			hi = mid - 1 // keep searching left for an earlier occurrence
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if result >= 0 {
		return result
	}
	return -(lo + 1)
}

// nearestLiveLeft scans from right down to left, returning the rightmost non-tombstone
// slot index and its extracted key. Returns (-1, nil) if all slots are tombstones.
func (p *Page) nearestLiveLeft(left, right int, keyExtract func([]byte) []byte) (int, []byte) {
	for i := right; i >= left; i-- {
		if d := p.GetRecord(i); d != nil {
			return i, keyExtract(d)
		}
	}
	return -1, nil
}

// binarySearchInsertPos finds the sorted insertion index for a new key among numSlots existing slots.
func (p *Page) binarySearchInsertPos(numSlots int, key []byte, keyExtract func([]byte) []byte) int {
	lo, hi := 0, numSlots-1
	for lo <= hi {
		mid := (lo + hi) / 2
		slotData := p.GetRecord(mid)
		if slotData == nil {
			// tombstone — use left-of-mid live anchor to determine direction.
			liveIdx, liveKey := p.nearestLiveLeft(lo, mid-1, keyExtract)
			if liveIdx >= 0 {
				if bytes.Compare(liveKey, key) <= 0 {
					lo = mid + 1
				} else {
					hi = liveIdx - 1
				}
			} else {
				lo = mid + 1
			}
			continue
		}
		if bytes.Compare(keyExtract(slotData), key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return lo
}

// Compact defragments the page: re-packs the data area from non-tombstone slots,
// resets FreeStart/FreeEnd, and updates the header.
func (p *Page) Compact() {
	h := p.DecodeHeader()
	numSlots := int(h.NumSlots)

	// Collect live records
	type liveRec struct {
		data    []byte
		slotIdx int
	}
	live := make([]liveRec, 0, numSlots)
	for i := 0; i < numSlots; i++ {
		d := p.GetRecord(i)
		if d != nil {
			cp := make([]byte, len(d))
			copy(cp, d)
			live = append(live, liveRec{cp, i})
		}
	}

	// Zero everything after the header
	for i := HeaderSize; i < PageSize; i++ {
		p.Data[i] = 0
	}

	// Re-write live records from the bottom of the page upward
	freeEnd := uint16(PageSize)
	freeStart := uint16(HeaderSize)
	newSlot := 0
	for _, rec := range live {
		freeEnd -= uint16(len(rec.data))
		copy(p.Data[freeEnd:], rec.data)
		p.SetSlot(newSlot, freeEnd, uint16(len(rec.data)))
		freeStart += SlotSize
		newSlot++
	}

	h.NumSlots = uint16(len(live))
	h.FreeStart = freeStart
	h.FreeEnd = freeEnd
	p.EncodeHeader(h)
}

// FragmentationRatio returns the fraction of space occupied by tombstone slots
// relative to total data area usage. Returns 0 if no data has been written.
func (p *Page) FragmentationRatio() float64 {
	h := p.DecodeHeader()
	numSlots := int(h.NumSlots)
	tombstoneBytes := 0
	totalBytes := 0
	for i := 0; i < numSlots; i++ {
		s := p.GetSlot(i)
		if s.Length == 0 {
			// We don't know the original size; count as 1 slot overhead
			tombstoneBytes++
		} else {
			totalBytes += int(s.Length)
		}
	}
	if totalBytes == 0 {
		return 0
	}
	return float64(tombstoneBytes) / float64(numSlots)
}
