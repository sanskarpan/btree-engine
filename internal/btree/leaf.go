package btree

import (
	"bytes"

	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"
)

// LeafPage wraps a *page.Page to provide leaf-node MVCC tuple operations.
type LeafPage struct {
	*page.Page
}

// InsertTuple encodes an MVCCTuple and inserts it into the leaf page.
func (lp *LeafPage) InsertTuple(tuple *mvcc.MVCCTuple) error {
	encoded := tuple.Encode()
	_, err := lp.InsertRecord(encoded, mvcc.TupleKeyExtract)
	return err
}

// GetTuple decodes the MVCC tuple at the given slot index.
func (lp *LeafPage) GetTuple(slotIdx int) *mvcc.MVCCTuple {
	data := lp.GetRecord(slotIdx)
	if data == nil {
		return nil
	}
	t, err := mvcc.DecodeTuple(data)
	if err != nil {
		return nil
	}
	return t
}

// FindAllVersions returns all tuple versions for key, scanning forward from the first
// matching slot. Results are in slot order (insertion order for same key).
func (lp *LeafPage) FindAllVersions(key []byte) []*mvcc.MVCCTuple {
	n := lp.NumSlots()

	// Binary search for first slot with this key
	startIdx := lp.BinarySearch(key, mvcc.TupleKeyExtract)
	if startIdx < 0 {
		startIdx = -(startIdx + 1)
	}

	versions := make([]*mvcc.MVCCTuple, 0)
	for i := startIdx; i < n; i++ {
		data := lp.GetRecord(i)
		if data == nil {
			continue
		}
		t, err := mvcc.DecodeTuple(data)
		if err != nil {
			continue
		}
		if !bytes.Equal(t.Key, key) {
			break // moved past this key
		}
		versions = append(versions, t)
	}
	return versions
}

// SetXmax updates the Xmax field of the tuple at slotIdx in-place.
func (lp *LeafPage) SetXmax(slotIdx int, xmax uint64) {
	data := lp.GetRecord(slotIdx)
	if data == nil || len(data) < mvcc.TupleHeaderSize {
		return
	}
	// Xmax is at bytes [8:16] in the tuple wire format
	data[8] = byte(xmax)
	data[9] = byte(xmax >> 8)
	data[10] = byte(xmax >> 16)
	data[11] = byte(xmax >> 24)
	data[12] = byte(xmax >> 32)
	data[13] = byte(xmax >> 40)
	data[14] = byte(xmax >> 48)
	data[15] = byte(xmax >> 56)
}

// FindVisibleSlot returns the slot index and tuple for the first visible version of key.
// Returns (-1, nil) if not found.
func (lp *LeafPage) FindVisibleSlot(key []byte, snap *mvcc.Snapshot, tm *mvcc.TransactionManager) (int, *mvcc.MVCCTuple) {
	n := lp.NumSlots()
	startIdx := lp.BinarySearch(key, mvcc.TupleKeyExtract)
	if startIdx < 0 {
		startIdx = -(startIdx + 1)
	}
	for i := startIdx; i < n; i++ {
		data := lp.GetRecord(i)
		if data == nil {
			continue
		}
		t, err := mvcc.DecodeTuple(data)
		if err != nil {
			continue
		}
		if !bytes.Equal(t.Key, key) {
			break
		}
		if snap.IsVisible(t, tm) {
			return i, t
		}
	}
	return -1, nil
}
