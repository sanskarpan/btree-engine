package disk

import (
	"fmt"
	"os"

	"btree-engine/internal/page"
)

// DiskManager handles raw page I/O against an OS file.
type DiskManager struct {
	file     *os.File
	pageSize int
}

// Open opens (or creates) a database file and returns a DiskManager.
// If the file is new (size == 0) a fresh meta page is written at page 0.
func Open(path string) (*DiskManager, error) {
	if err := os.MkdirAll(dirOf(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	dm := &DiskManager{file: f, pageSize: page.PageSize}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		// Brand-new file: write meta page (page 0) and allocate page 1 as B+tree root placeholder
		meta := MetaPage{
			RootPageID:   1,
			FreeListHead: 0,
			NextPageID:   2, // next fresh page will be 2
			TxnIDCounter: 1,
		}
		if err := dm.WriteMeta(meta); err != nil {
			return nil, err
		}
		// Write an empty leaf page at page 1 (root)
		var rootBuf [page.PageSize]byte
		root := &page.Page{}
		root.Init(page.PageTypeLeaf)
		root.SetFlag(page.FlagIsRoot)
		copy(rootBuf[:], root.Data[:])
		if err := dm.WritePageRaw(1, rootBuf[:]); err != nil {
			return nil, err
		}
	}
	return dm, nil
}

// Close closes the underlying file.
func (dm *DiskManager) Close() error {
	return dm.file.Close()
}

// ReadPage reads a 4096-byte page at the given page ID into buf.
func (dm *DiskManager) ReadPage(pageID uint32, buf []byte) error {
	if len(buf) < page.PageSize {
		return fmt.Errorf("buffer too small: %d < %d", len(buf), page.PageSize)
	}
	off := int64(pageID) * int64(page.PageSize)
	_, err := dm.file.ReadAt(buf[:page.PageSize], off)
	return err
}

// WritePage writes a 4096-byte page at the given page ID.
func (dm *DiskManager) WritePage(pageID uint32, data []byte) error {
	return dm.WritePageRaw(pageID, data)
}

// WritePageRaw is the internal write (no lock needed since callers hold dm.mu or are single-threaded init).
func (dm *DiskManager) WritePageRaw(pageID uint32, data []byte) error {
	off := int64(pageID) * int64(page.PageSize)
	_, err := dm.file.WriteAt(data[:page.PageSize], off)
	return err
}

// ReadMeta reads and decodes the meta page (page 0).
func (dm *DiskManager) ReadMeta() (MetaPage, error) {
	var buf [page.PageSize]byte
	if err := dm.ReadPage(MetaPageID, buf[:]); err != nil {
		return MetaPage{}, err
	}
	var m MetaPage
	if err := m.Decode(buf[:]); err != nil {
		return MetaPage{}, err
	}
	return m, nil
}

// WriteMeta encodes and writes the meta page (page 0).
func (dm *DiskManager) WriteMeta(m MetaPage) error {
	var buf [page.PageSize]byte
	m.Encode(buf[:])
	return dm.WritePageRaw(MetaPageID, buf[:])
}

// Sync flushes all pending writes to disk.
func (dm *DiskManager) Sync() error {
	return dm.file.Sync()
}

// dirOf returns the directory portion of a file path.
func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[:i]
		}
	}
	return "."
}
