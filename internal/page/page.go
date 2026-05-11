// Package page implements the 4096-byte slotted page with a 32-byte header.
package page

import (
	"encoding/binary"
	"hash/crc32"
)

// Page layout constants.
const (
	PageSize    = 4096
	HeaderSize  = 32
	MagicNumber = uint32(0xB17EE501)
	SlotSize    = 4 // bytes: [Offset:2][Length:2]
)

// PageType identifies the role of a page.
type PageType uint8

// PageType values identifying the role of a page.
const (
	PageTypeInternal PageType = 1
	PageTypeLeaf     PageType = 2
	PageTypeMeta     PageType = 3
	PageTypeFree     PageType = 4
)

// Page flags (stored in header byte 19).
const (
	FlagIsRoot         uint8 = 0x01
	FlagIsDirty        uint8 = 0x02
	FlagHasOverflow    uint8 = 0x04
	FlagRightmostChild uint8 = 0x08 // internal: has a rightmost child pointer
)

// PageHeader mirrors the on-disk 32-byte header layout.
//
// Layout (little-endian):
//
//	[0:4]   Magic     uint32
//	[4:5]   Type      uint8
//	[5:7]   NumSlots  uint16
//	[7:9]   FreeStart uint16
//	[9:11]  FreeEnd   uint16
//	[11:19] PageLSN   uint64
//	[19:20] Flags     uint8
//	[20:24] PrevSib   uint32
//	[24:28] NextSib   uint32
//	[28:32] Checksum  uint32  (CRC32 of bytes [0:28])
type PageHeader struct {
	Magic     uint32
	Type      PageType
	NumSlots  uint16
	FreeStart uint16
	FreeEnd   uint16
	PageLSN   uint64
	Flags     uint8
	PrevSib   uint32
	NextSib   uint32
	Checksum  uint32
}

// Page is the in-memory representation of a 4096-byte on-disk page.
type Page struct {
	ID   uint32
	Data [PageSize]byte
}

// DecodeHeader parses the 32-byte header from the raw page data.
func (p *Page) DecodeHeader() PageHeader {
	b := p.Data[:]
	return PageHeader{
		Magic:     binary.LittleEndian.Uint32(b[0:]),
		Type:      PageType(b[4]),
		NumSlots:  binary.LittleEndian.Uint16(b[5:]),
		FreeStart: binary.LittleEndian.Uint16(b[7:]),
		FreeEnd:   binary.LittleEndian.Uint16(b[9:]),
		PageLSN:   binary.LittleEndian.Uint64(b[11:]),
		Flags:     b[19],
		PrevSib:   binary.LittleEndian.Uint32(b[20:]),
		NextSib:   binary.LittleEndian.Uint32(b[24:]),
		Checksum:  binary.LittleEndian.Uint32(b[28:]),
	}
}

// EncodeHeader writes a PageHeader into the page's raw data and computes a full-page
// CRC32 checksum covering all PageSize bytes (with the checksum field itself zeroed
// during computation to make the computation deterministic).
func (p *Page) EncodeHeader(h PageHeader) {
	b := p.Data[:]
	binary.LittleEndian.PutUint32(b[0:], MagicNumber)
	b[4] = byte(h.Type)
	binary.LittleEndian.PutUint16(b[5:], h.NumSlots)
	binary.LittleEndian.PutUint16(b[7:], h.FreeStart)
	binary.LittleEndian.PutUint16(b[9:], h.FreeEnd)
	binary.LittleEndian.PutUint64(b[11:], h.PageLSN)
	b[19] = h.Flags
	binary.LittleEndian.PutUint32(b[20:], h.PrevSib)
	binary.LittleEndian.PutUint32(b[24:], h.NextSib)
	// Zero checksum field before computing so the CRC is over a known-layout page.
	binary.LittleEndian.PutUint32(b[28:], 0)
	crc := crc32.ChecksumIEEE(b[0:PageSize])
	binary.LittleEndian.PutUint32(b[28:], crc)
}

// Init initialises a fresh page with the given type, resetting free-space pointers.
func (p *Page) Init(ptype PageType) {
	p.EncodeHeader(PageHeader{
		Type:      ptype,
		FreeStart: HeaderSize,
		FreeEnd:   PageSize,
	})
}

// VerifyChecksum returns true if the stored CRC32 matches a full-page checksum
// computed with the checksum field zeroed.
func (p *Page) VerifyChecksum() bool {
	stored := binary.LittleEndian.Uint32(p.Data[28:])
	// Compute on a local copy so we don't mutate the live page.
	var tmp [PageSize]byte
	copy(tmp[:], p.Data[:])
	binary.LittleEndian.PutUint32(tmp[28:], 0)
	computed := crc32.ChecksumIEEE(tmp[:])
	return stored == computed
}

// UpdateChecksum recalculates and writes the full-page CRC32 checksum.
func (p *Page) UpdateChecksum() {
	binary.LittleEndian.PutUint32(p.Data[28:], 0)
	crc := crc32.ChecksumIEEE(p.Data[0:PageSize])
	binary.LittleEndian.PutUint32(p.Data[28:], crc)
}

// FreeSpace returns bytes available between the slot array and the data area.
func (p *Page) FreeSpace() int {
	h := p.DecodeHeader()
	return int(h.FreeEnd) - int(h.FreeStart)
}

// SetPageLSN updates only the PageLSN field and recalculates the checksum.
func (p *Page) SetPageLSN(lsn uint64) {
	h := p.DecodeHeader()
	h.PageLSN = lsn
	p.EncodeHeader(h)
}

// Type returns the PageType stored in the header.
func (p *Page) Type() PageType {
	return PageType(p.Data[4])
}

// NumSlots returns the current slot count.
func (p *Page) NumSlots() int {
	return int(binary.LittleEndian.Uint16(p.Data[5:]))
}

// PrevSib returns the previous sibling page ID (0 = none).
func (p *Page) PrevSib() uint32 {
	return binary.LittleEndian.Uint32(p.Data[20:])
}

// NextSib returns the next sibling page ID (0 = none).
func (p *Page) NextSib() uint32 {
	return binary.LittleEndian.Uint32(p.Data[24:])
}

// SetPrevSib updates the PrevSib field and refreshes the checksum.
func (p *Page) SetPrevSib(id uint32) {
	binary.LittleEndian.PutUint32(p.Data[20:], id)
	p.UpdateChecksum()
}

// SetNextSib updates the NextSib field and refreshes the checksum.
func (p *Page) SetNextSib(id uint32) {
	binary.LittleEndian.PutUint32(p.Data[24:], id)
	p.UpdateChecksum()
}

// Flags returns the flags byte.
func (p *Page) Flags() uint8 {
	return p.Data[19]
}

// SetFlag sets a flag bit.
func (p *Page) SetFlag(flag uint8) {
	p.Data[19] |= flag
	p.UpdateChecksum()
}

// ClearFlag clears a flag bit.
func (p *Page) ClearFlag(flag uint8) {
	p.Data[19] &^= flag
	p.UpdateChecksum()
}
