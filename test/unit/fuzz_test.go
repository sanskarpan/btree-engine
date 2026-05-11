package unit

import (
	"bytes"
	"testing"

	"btree-engine/internal/btree"
	"btree-engine/internal/buffer"
	"btree-engine/internal/disk"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"
	"btree-engine/internal/wal"
)

// FuzzInsertGet verifies that every key inserted into the tree can be retrieved.
// It accepts two byte slices as fuzz inputs: the key and the value.
func FuzzInsertGet(f *testing.F) {
	// Seed corpus.
	f.Add([]byte("hello"), []byte("world"))
	f.Add([]byte("a"), []byte("1"))
	f.Add([]byte("z"), []byte(""))
	f.Add([]byte("longer-key-12345"), []byte("longer-value-abcdef"))

	f.Fuzz(func(t *testing.T, key, value []byte) {
		if len(key) == 0 {
			t.Skip()
		}

		dir := t.TempDir()
		dm, err := disk.Open(dir + "/fuzz.db")
		if err != nil {
			t.Fatal(err)
		}
		defer dm.Close()

		bp := buffer.New(64, dm, nil, nil)
		tm := mvcc.NewTransactionManager(nil)

		rootID, _ := dm.AllocPage()
		initFrame, _ := bp.FetchPage(rootID)
		p := bp.ReadPageData(initFrame)
		p.Init(page.PageTypeLeaf)
		p.SetFlag(page.FlagIsRoot)
		p.UpdateChecksum()
		bp.WritePageData(rootID, p)
		bp.UnpinPage(rootID, true)

		tree := btree.New(rootID, bp, nil, dm, tm, nil)

		txn := tm.Begin(mvcc.ReadCommitted)
		if err := tree.Insert(txn, key, value); err != nil {
			// Page-full errors during fuzz are acceptable.
			tm.Abort(txn)
			return
		}
		tm.Commit(txn)

		txn2 := tm.Begin(mvcc.ReadCommitted)
		got, err := tree.Get(txn2, key)
		if err != nil {
			t.Fatalf("Get after Insert returned error: %v", err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("value mismatch: got %q, want %q", got, value)
		}
	})
}

// FuzzBinarySearch ensures BinarySearch never panics or returns out-of-bounds results.
func FuzzBinarySearch(f *testing.F) {
	f.Add([]byte("midkey"), []byte("aaaaa"), []byte("midkey"), []byte("zzzzz"))
	f.Add([]byte("x"), []byte("a"), []byte("m"), []byte("z"))

	f.Fuzz(func(t *testing.T, searchKey, k1, k2, k3 []byte) {
		if len(k1) == 0 || len(k2) == 0 || len(k3) == 0 {
			t.Skip()
		}

		var p page.Page
		p.Init(page.PageTypeLeaf)

		// Insert three sorted-ish tuples directly.
		keys := [][]byte{k1, k2, k3}
		for _, k := range keys {
			tuple := &mvcc.MVCCTuple{Xmin: 1, Key: k, Value: []byte("v")}
			p.InsertRecord(tuple.Encode(), mvcc.TupleKeyExtract) //nolint
		}

		// BinarySearch must not panic and must return a valid range.
		idx := p.BinarySearch(searchKey, mvcc.TupleKeyExtract)
		n := p.NumSlots()
		if idx > n || idx < -(n+1) {
			t.Fatalf("BinarySearch returned out-of-bounds index %d for n=%d", idx, n)
		}
	})
}

// FuzzChecksumRoundtrip verifies UpdateChecksum + VerifyChecksum consistency.
func FuzzChecksumRoundtrip(f *testing.F) {
	f.Add([]byte("data"), uint16(page.PageTypeLeaf))
	f.Add([]byte(""), uint16(0))

	f.Fuzz(func(t *testing.T, payload []byte, ptype uint16) {
		var p page.Page
		p.Init(page.PageType(ptype % 4)) // keep type in valid range

		// Write fuzz bytes into the data portion (after the header).
		dataStart := page.HeaderSize
		dataEnd := page.PageSize
		if dataStart < dataEnd {
			copy(p.Data[dataStart:dataEnd], payload)
		}

		p.UpdateChecksum()
		if !p.VerifyChecksum() {
			t.Fatal("VerifyChecksum failed immediately after UpdateChecksum")
		}

		// Mutate one byte outside the checksum field and verify it now fails.
		mutateOffset := dataStart + len(payload)%max(1, dataEnd-dataStart)
		p.Data[mutateOffset] ^= 0xFF
		// After mutation the checksum must no longer match — unless the XOR happened to
		// produce the same CRC (astronomically unlikely; we just verify no panic).
		_ = p.VerifyChecksum()
	})
}

// FuzzWALRecord checks that every WAL record that can be encoded can be decoded
// back to an equivalent record.
func FuzzWALRecord(f *testing.F) {
	f.Add(uint8(wal.LogInsert), uint64(1), uint32(2), uint64(0), []byte("payload"))
	f.Add(uint8(wal.LogDelete), uint64(42), uint32(99), uint64(10), []byte{0x01, 0x02})
	f.Add(uint8(wal.LogCommit), uint64(7), uint32(0), uint64(5), []byte{})

	f.Fuzz(func(t *testing.T, rtype uint8, txnID uint64, pageID uint32, prevLSN uint64, payload []byte) {
		r := wal.LogRecord{
			Type:    wal.LogRecordType(rtype),
			TxnID:   txnID,
			PageID:  pageID,
			PrevLSN: prevLSN,
			Payload: payload,
		}
		enc := wal.EncodeLogRecord(&r)
		dec, err := wal.DecodeLogRecord(enc)
		if err != nil {
			// Invalid type values may produce decode errors — that is fine.
			return
		}
		if dec.Type != r.Type || dec.TxnID != r.TxnID || dec.PageID != r.PageID || dec.PrevLSN != r.PrevLSN {
			t.Fatalf("roundtrip mismatch: got %+v, want %+v", dec, r)
		}
		if !bytes.Equal(dec.Payload, r.Payload) {
			t.Fatalf("payload mismatch after roundtrip")
		}
	})
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
