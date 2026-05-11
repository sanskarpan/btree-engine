// Package disk implements the on-disk storage layer: page I/O, meta page, and free-list.
package disk

import (
	"encoding/binary"
	"errors"
)

// MetaPageID is the page ID of the database meta page (always page 0).
const (
	MetaPageID = uint32(0)
	metaMagic  = uint32(0xB17EE501)
)

// ErrInvalidMeta is returned when the meta page has an unexpected magic number.
var ErrInvalidMeta = errors.New("invalid meta page magic")

// MetaPage is stored at page 0 and tracks global engine state.
//
// Layout within the 4096-byte page (little-endian):
//
//	[0:4]   Magic        uint32
//	[4:8]   RootPageID   uint32
//	[8:12]  FreeListHead uint32
//	[12:16] NextPageID   uint32
//	[16:24] TxnIDCounter uint64
type MetaPage struct {
	Magic        uint32
	RootPageID   uint32
	FreeListHead uint32
	NextPageID   uint32
	TxnIDCounter uint64
}

// Encode serialises MetaPage into a 4096-byte page buffer.
func (m *MetaPage) Encode(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:], metaMagic)
	binary.LittleEndian.PutUint32(buf[4:], m.RootPageID)
	binary.LittleEndian.PutUint32(buf[8:], m.FreeListHead)
	binary.LittleEndian.PutUint32(buf[12:], m.NextPageID)
	binary.LittleEndian.PutUint64(buf[16:], m.TxnIDCounter)
}

// Decode parses a MetaPage from a 4096-byte page buffer.
func (m *MetaPage) Decode(buf []byte) error {
	magic := binary.LittleEndian.Uint32(buf[0:])
	if magic != metaMagic {
		return ErrInvalidMeta
	}
	m.Magic = magic
	m.RootPageID = binary.LittleEndian.Uint32(buf[4:])
	m.FreeListHead = binary.LittleEndian.Uint32(buf[8:])
	m.NextPageID = binary.LittleEndian.Uint32(buf[12:])
	m.TxnIDCounter = binary.LittleEndian.Uint64(buf[16:])
	return nil
}
