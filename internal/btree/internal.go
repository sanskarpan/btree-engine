// Package btree implements the B+Tree on top of slotted pages.
package btree

import (
	"encoding/binary"
	"errors"

	"btree-engine/internal/page"
)

// ErrKeyNotFound is returned when a key does not exist in the tree.
var ErrKeyNotFound = errors.New("key not found")

// InternalRecord is a separator-key + child-pointer entry stored in an internal node.
//
// Wire format: [KeyLen:2][Key][ChildID:4]
type InternalRecord struct {
	Key     []byte
	ChildID uint32
}

// EncodeInternalRecord serialises an InternalRecord.
func EncodeInternalRecord(r InternalRecord) []byte {
	buf := make([]byte, 2+len(r.Key)+4)
	binary.LittleEndian.PutUint16(buf[0:], uint16(len(r.Key)))
	copy(buf[2:], r.Key)
	binary.LittleEndian.PutUint32(buf[2+len(r.Key):], r.ChildID)
	return buf
}

// DecodeInternalRecord deserialises an InternalRecord.
func DecodeInternalRecord(data []byte) (InternalRecord, error) {
	if len(data) < 2 {
		return InternalRecord{}, errors.New("internal record too short")
	}
	keyLen := int(binary.LittleEndian.Uint16(data[0:]))
	if len(data) < 2+keyLen+4 {
		return InternalRecord{}, errors.New("internal record truncated")
	}
	r := InternalRecord{}
	r.Key = make([]byte, keyLen)
	copy(r.Key, data[2:])
	r.ChildID = binary.LittleEndian.Uint32(data[2+keyLen:])
	return r, nil
}

// InternalKeyExtract returns the key bytes from an encoded InternalRecord.
func InternalKeyExtract(data []byte) []byte {
	if len(data) < 2 {
		return nil
	}
	keyLen := int(binary.LittleEndian.Uint16(data[0:]))
	if len(data) < 2+keyLen {
		return nil
	}
	return data[2 : 2+keyLen]
}

// InternalPage wraps a *page.Page to provide internal-node operations.
type InternalPage struct {
	*page.Page
}

// FindChild performs a binary search to find the child page ID for a given search key.
// Returns the rightmost child ID (stored in header flags area) if key >= all separators.
func (ip *InternalPage) FindChild(key []byte) uint32 {
	n := ip.NumSlots()
	// Binary search: find last separator <= key
	lo, hi := 0, n-1
	result := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		data := ip.GetRecord(mid)
		if data == nil {
			lo = mid + 1
			continue
		}
		sepKey := InternalKeyExtract(data)
		if bytesLessOrEqual(sepKey, key) {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	if result < 0 {
		// key < all separators: follow leftmost child (stored in slot 0's ChildID but
		// the actual leftmost pointer is the ChildID of slot[0] when key < slot[0].Key).
		// By convention the leftmost child pointer is the RightmostChild of a virtual
		// leftmost separator — but here we store it differently:
		// For a standard B+tree: separators s[0]..s[n-1] partition into n+1 children.
		// We store n separators; child for key < s[0] is in the Page's PrevSib field.
		return ip.PrevSib() // leftmost child pointer
	}

	data := ip.GetRecord(result)
	r, _ := DecodeInternalRecord(data)
	return r.ChildID
}

// RightmostChild returns the page ID of the rightmost child (for keys > all separators).
// Stored in the page's NextSib field by convention.
func (ip *InternalPage) RightmostChild() uint32 {
	return ip.NextSib()
}

// SetRightmostChild stores the rightmost child page ID in the NextSib field.
func (ip *InternalPage) SetRightmostChild(childID uint32) {
	ip.SetNextSib(childID)
}

// SetLeftmostChild stores the leftmost child in the PrevSib field.
func (ip *InternalPage) SetLeftmostChild(childID uint32) {
	ip.SetPrevSib(childID)
}

// LeftmostChild returns the leftmost child page ID (stored in PrevSib).
func (ip *InternalPage) LeftmostChild() uint32 {
	return ip.PrevSib()
}

// InsertKey inserts a separator key with its right-child pointer into the internal page.
func (ip *InternalPage) InsertKey(key []byte, rightChild uint32) error {
	rec := InternalRecord{Key: key, ChildID: rightChild}
	encoded := EncodeInternalRecord(rec)
	_, err := ip.InsertRecord(encoded, InternalKeyExtract)
	return err
}

// RemoveKey removes the separator at the given slot index.
func (ip *InternalPage) RemoveKey(slotIdx int) {
	ip.DeleteRecord(slotIdx)
}

// bytesLessOrEqual returns true if a <= b lexicographically.
func bytesLessOrEqual(a, b []byte) bool {
	la, lb := len(a), len(b)
	minLen := la
	if lb < minLen {
		minLen = lb
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return la <= lb
}
