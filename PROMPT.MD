# CLAUDE CODE PROMPT — B+Tree Storage Engine with MVCC

## Project Mission

Build a from-scratch B+Tree storage engine in Go with:
- On-disk slotted pages (4096-byte, CRC32-protected)
- B+Tree with split, merge/borrow, sibling links for range scans
- MVCC: PostgreSQL-style xmin/xmax per tuple, version chains in leaf pages
- Snapshot isolation + READ COMMITTED isolation levels
- Buffer pool with Clock eviction + WAL-before-eviction rule
- WAL with LSN-tagged log records + ARIES 3-phase crash recovery
- VACUUM for dead tuple reclamation
- Live D3.js frontend showing all components in real time

**Read `SPEC.md` and `CHECKLIST.md` before writing any code.**
**Build phases in strict order. Phase N tests must pass before Phase N+1 begins.**
**Zero external DB dependencies.**

---

## Phase 0 — Bootstrap

```bash
go mod init btree-engine
go get github.com/gorilla/websocket github.com/stretchr/testify/assert
cd frontend && bun init
bun add elysia @elysiajs/eden d3
bun add -d typescript @types/d3
```

---

## Phase 1 — Page Layer

### Page Header (exact binary layout)

```go
// internal/page/page.go
package page

import (
    "encoding/binary"
    "hash/crc32"
)

const (
    PageSize    = 4096
    HeaderSize  = 32
    MagicNumber = uint32(0xB17EE501)
)

type PageType uint8
const (
    PageTypeInternal PageType = 1
    PageTypeLeaf     PageType = 2
    PageTypeMeta     PageType = 3
    PageTypeFree     PageType = 4
)

// Header layout (32 bytes, little-endian):
// [0:4]   Magic uint32
// [4:5]   Type  uint8
// [5:7]   NumSlots uint16
// [7:9]   FreeStart uint16  -- next slot goes here
// [9:11]  FreeEnd   uint16  -- next data goes here - len(data)
// [11:19] PageLSN   uint64
// [19:20] Flags     uint8
// [20:24] PrevSib   uint32
// [24:28] NextSib   uint32
// [28:32] Checksum  uint32  -- CRC32 of bytes [0:28]

type Page struct {
    ID   uint32
    Data [PageSize]byte
}

func (p *Page) DecodeHeader() (magic uint32, ptype PageType, numSlots, freeStart, freeEnd uint16,
    pageLSN uint64, flags uint8, prevSib, nextSib, checksum uint32) {
    b := p.Data[:]
    magic     = binary.LittleEndian.Uint32(b[0:])
    ptype     = PageType(b[4])
    numSlots  = binary.LittleEndian.Uint16(b[5:])
    freeStart = binary.LittleEndian.Uint16(b[7:])
    freeEnd   = binary.LittleEndian.Uint16(b[9:])
    pageLSN   = binary.LittleEndian.Uint64(b[11:])
    flags     = b[19]
    prevSib   = binary.LittleEndian.Uint32(b[20:])
    nextSib   = binary.LittleEndian.Uint32(b[24:])
    checksum  = binary.LittleEndian.Uint32(b[28:])
    return
}

func (p *Page) EncodeHeader(ptype PageType, numSlots, freeStart, freeEnd uint16,
    pageLSN uint64, flags uint8, prevSib, nextSib uint32) {
    b := p.Data[:]
    binary.LittleEndian.PutUint32(b[0:], MagicNumber)
    b[4] = byte(ptype)
    binary.LittleEndian.PutUint16(b[5:], numSlots)
    binary.LittleEndian.PutUint16(b[7:], freeStart)
    binary.LittleEndian.PutUint16(b[9:], freeEnd)
    binary.LittleEndian.PutUint64(b[11:], pageLSN)
    b[19] = flags
    binary.LittleEndian.PutUint32(b[20:], prevSib)
    binary.LittleEndian.PutUint32(b[24:], nextSib)
    // Checksum computed over [0:28]
    crc := crc32.ChecksumIEEE(b[0:28])
    binary.LittleEndian.PutUint32(b[28:], crc)
}

func (p *Page) VerifyChecksum() bool {
    stored := binary.LittleEndian.Uint32(p.Data[28:])
    computed := crc32.ChecksumIEEE(p.Data[0:28])
    return stored == computed
}
```

### Slotted Page (exact insert/get/binary search)

```go
// internal/page/slotted.go
// Slot = 4 bytes: [Offset:2][Length:2]
// Slot at index i lives at Data[HeaderSize + i*4 : HeaderSize + i*4 + 4]
// Data records live from FreeEnd downward

const SlotSize = 4 // bytes

func (p *Page) GetSlot(idx int) (offset, length uint16) {
    base := HeaderSize + idx*SlotSize
    offset = binary.LittleEndian.Uint16(p.Data[base:])
    length = binary.LittleEndian.Uint16(p.Data[base+2:])
    return
}

func (p *Page) SetSlot(idx int, offset, length uint16) {
    base := HeaderSize + idx*SlotSize
    binary.LittleEndian.PutUint16(p.Data[base:], offset)
    binary.LittleEndian.PutUint16(p.Data[base+2:], length)
}

// InsertRecord: data written at (freeEnd - len(data)); slot appended at freeStart
// Slot array is NOT kept sorted by this function; BinarySearch maintains logical order
func (p *Page) InsertRecord(data []byte, keyExtract func([]byte) []byte) (slotIdx int, err error) {
    _, _, numSlots, freeStart, freeEnd, pageLSN, flags, prevSib, nextSib, _ := p.DecodeHeader()
    needed := len(data) + SlotSize
    if int(freeEnd)-int(freeStart) < needed {
        return 0, ErrPageFull
    }
    // Write data at freeEnd - len(data)
    newFreeEnd := freeEnd - uint16(len(data))
    copy(p.Data[newFreeEnd:], data)
    // Find insertion position in slot array (maintain key-sorted order)
    newKey := keyExtract(data)
    insertPos := p.binarySearchInsertPos(numSlots, newKey, keyExtract)
    // Shift slots right to make room
    slotBase := HeaderSize + int(numSlots)*SlotSize
    copy(p.Data[HeaderSize+(insertPos+1)*SlotSize:slotBase+SlotSize],
         p.Data[HeaderSize+insertPos*SlotSize:slotBase])
    // Write new slot
    p.SetSlot(insertPos, newFreeEnd, uint16(len(data)))
    // Update header
    p.EncodeHeader(PageType(p.Data[4]), numSlots+1, freeStart+SlotSize, newFreeEnd, pageLSN, flags, prevSib, nextSib)
    return insertPos, nil
}

func (p *Page) GetRecord(slotIdx int) []byte {
    offset, length := p.GetSlot(slotIdx)
    if length == 0 { return nil } // tombstone
    return p.Data[offset : offset+length]
}

// BinarySearch: find slot for key (returns slot index or -(insertionPoint)-1)
func (p *Page) BinarySearch(key []byte, keyExtract func([]byte) []byte) int {
    _, _, numSlots, _, _, _, _, _, _, _ := p.DecodeHeader()
    lo, hi := 0, int(numSlots)-1
    for lo <= hi {
        mid := (lo + hi) / 2
        slotData := p.GetRecord(mid)
        if slotData == nil { lo = mid + 1; continue } // skip tombstone
        cmp := bytes.Compare(keyExtract(slotData), key)
        if cmp == 0 { return mid }
        if cmp < 0 { lo = mid + 1 } else { hi = mid - 1 }
    }
    return -(lo + 1) // not found; -(insertionPoint)-1
}
```

**Tests that must pass before Phase 3:**

```go
func TestPage_InsertAndGet(t *testing.T) {
    p := &Page{ID: 1}
    p.EncodeHeader(PageTypeLeaf, 0, HeaderSize, PageSize, 0, 0, 0, 0)

    keyExtract := func(d []byte) []byte { return d[:4] } // first 4 bytes = key

    for i := 0; i < 50; i++ {
        data := []byte(fmt.Sprintf("key%02d:value%02d", i, i))
        slotIdx, err := p.InsertRecord(data, keyExtract)
        require.NoError(t, err)
        assert.GreaterOrEqual(t, slotIdx, 0)
    }
    _, _, numSlots, _, _, _, _, _, _, _ := p.DecodeHeader()
    assert.Equal(t, uint16(50), numSlots)

    // Verify all records readable
    for i := 0; i < 50; i++ {
        expected := fmt.Sprintf("key%02d:value%02d", i, i)
        // Binary search
        idx := p.BinarySearch([]byte(expected[:6]), keyExtract)
        require.GreaterOrEqual(t, idx, 0)
        assert.Equal(t, []byte(expected), p.GetRecord(idx))
    }
}
```

---

## Phase 3 — MVCC Tuples + Visibility

### Tuple Encoding (exact wire format)

```go
// internal/mvcc/tuple.go

// Wire format: [Xmin:8][Xmax:8][KeyLen:2][ValLen:2][Flags:1][Key][Value]
const TupleHeaderSize = 8 + 8 + 2 + 2 + 1 // = 21 bytes

func (t *MVCCTuple) Encode() []byte {
    buf := make([]byte, TupleHeaderSize+len(t.Key)+len(t.Value))
    binary.LittleEndian.PutUint64(buf[0:], t.Xmin)
    binary.LittleEndian.PutUint64(buf[8:], t.Xmax)
    binary.LittleEndian.PutUint16(buf[16:], uint16(len(t.Key)))
    binary.LittleEndian.PutUint16(buf[18:], uint16(len(t.Value)))
    buf[20] = byte(t.Flags)
    copy(buf[21:], t.Key)
    copy(buf[21+len(t.Key):], t.Value)
    return buf
}

func DecodeTuple(data []byte) (*MVCCTuple, error) {
    if len(data) < TupleHeaderSize { return nil, ErrInvalidTuple }
    t := &MVCCTuple{}
    t.Xmin    = binary.LittleEndian.Uint64(data[0:])
    t.Xmax    = binary.LittleEndian.Uint64(data[8:])
    keyLen    := int(binary.LittleEndian.Uint16(data[16:]))
    valLen    := int(binary.LittleEndian.Uint16(data[18:]))
    t.Flags   = TupleFlags(data[20])
    if len(data) < TupleHeaderSize+keyLen+valLen { return nil, ErrInvalidTuple }
    t.Key   = make([]byte, keyLen)
    t.Value = make([]byte, valLen)
    copy(t.Key, data[21:])
    copy(t.Value, data[21+keyLen:])
    return t, nil
}

// Key extractor for slotted page binary search
func TupleKeyExtract(data []byte) []byte {
    if len(data) < TupleHeaderSize { return nil }
    keyLen := int(binary.LittleEndian.Uint16(data[16:]))
    return data[21 : 21+keyLen]
}
```

### Visibility Rules (the most important function in the engine)

```go
// internal/mvcc/snapshot.go

// IsVisible: 7-case visibility check (PostgreSQL-style)
// CRITICAL CORRECTNESS: this function must be exactly right.
// Any bug here causes dirty reads, lost updates, or phantom reads.
func (snap *Snapshot) IsVisible(t *MVCCTuple, tm *TransactionManager) bool {
    // =========== Check Xmin (creating transaction) ===========

    // Case 1: Creator is this transaction itself — always visible (own writes)
    if TxnID(t.Xmin) == snap.CreatorTxnID {
        goto checkXmax
    }

    // Case 2: Creator not committed — invisible
    if tm.GetStatus(TxnID(t.Xmin)) != TxnCommitted {
        return false
    }

    // Case 3: Creator committed after snapshot — invisible (future transaction)
    if TxnID(t.Xmin) >= snap.XmaxSnapshot {
        return false
    }

    // Case 4: Creator was in-progress when snapshot was taken — invisible
    for _, xid := range snap.XipList {
        if TxnID(t.Xmin) == xid {
            return false
        }
    }
    // Creator committed before snapshot and not in XipList → row was created before our snapshot

checkXmax:
    // =========== Check Xmax (deleting transaction) ===========

    // Case 5: Not deleted — visible
    if t.Xmax == 0 {
        return true
    }

    // Case 6: Deleted by this transaction — invisible (we deleted it)
    if TxnID(t.Xmax) == snap.CreatorTxnID {
        return false
    }

    // Case 7: Deleting transaction aborted — deletion rolled back, row is alive
    if tm.GetStatus(TxnID(t.Xmax)) == TxnAborted {
        return true
    }

    // Case 8: Deleting transaction committed after snapshot — deletion not yet visible to us
    if TxnID(t.Xmax) >= snap.XmaxSnapshot {
        return true
    }

    // Case 9: Deleting transaction was in-progress when snapshot taken — deletion not visible
    for _, xid := range snap.XipList {
        if TxnID(t.Xmax) == xid {
            return true // deleter still running when we took snapshot
        }
    }

    // Deleting transaction committed before our snapshot — row is deleted and invisible
    return false
}
```

**Critical tests — all must pass before Phase 4:**

```go
func TestVisibility_DirtyRead_Prevented(t *testing.T) {
    tm := NewTransactionManager()

    // T1 begins, inserts a tuple
    t1 := tm.Begin(SnapshotIsolation)
    tuple := &MVCCTuple{Xmin: uint64(t1.ID), Xmax: 0, Key: []byte("k1"), Value: []byte("v1")}

    // T2 begins (AFTER T1, so T1 is in T2's XipList)
    t2 := tm.Begin(SnapshotIsolation)

    // T2 MUST NOT see T1's uncommitted write
    assert.False(t, t2.Snapshot.IsVisible(tuple, tm),
        "dirty read: T2 must not see T1's uncommitted write")

    // T1 commits
    tm.Commit(t1)

    // T3 begins AFTER T1 committed
    t3 := tm.Begin(SnapshotIsolation)
    assert.True(t, t3.Snapshot.IsVisible(tuple, tm),
        "T3 must see T1's committed write")

    // T2 (snapshot taken before T1 committed) still must NOT see T1's write
    assert.False(t, t2.Snapshot.IsVisible(tuple, tm),
        "snapshot isolation: T2's snapshot predates T1's commit")
}

func TestVisibility_OwnWrites(t *testing.T) {
    tm := NewTransactionManager()
    t1 := tm.Begin(SnapshotIsolation)
    tuple := &MVCCTuple{Xmin: uint64(t1.ID), Xmax: 0, Key: []byte("k1"), Value: []byte("v1")}
    assert.True(t, t1.Snapshot.IsVisible(tuple, tm), "txn must see its own uncommitted writes")
}

func TestVisibility_DeletedByCommitted(t *testing.T) {
    tm := NewTransactionManager()
    t1 := tm.Begin(SnapshotIsolation)
    tm.Commit(t1)

    t2 := tm.Begin(SnapshotIsolation)
    // T2 deletes the tuple (sets Xmax)
    tuple := &MVCCTuple{Xmin: uint64(t1.ID), Xmax: uint64(t2.ID), Key: []byte("k1"), Value: []byte("v1")}
    tm.Commit(t2)

    // T3 begins after T2 committed: should NOT see the deleted tuple
    t3 := tm.Begin(SnapshotIsolation)
    assert.False(t, t3.Snapshot.IsVisible(tuple, tm), "deleted tuple must be invisible after deleter commits")
}
```

---

## Phase 4 — Buffer Pool

### Clock Eviction (exact algorithm)

```go
// internal/buffer/clock.go

func (bp *BufferPool) evict() (*Frame, error) {
    // Two-pass Clock: first pass tries RefBit=false; second pass forces eviction
    for pass := 0; pass < 2; pass++ {
        for i := 0; i < len(bp.frames); i++ {
            bp.clockHand = (bp.clockHand + 1) % len(bp.frames)
            frame := bp.frames[bp.clockHand]

            // Pinned frames cannot be evicted
            if atomic.LoadInt32(&frame.PinCount) > 0 { continue }

            if frame.RefBit && pass == 0 {
                frame.RefBit = false // give second chance
                continue
            }

            // Evict this frame
            if frame.Dirty {
                // WAL rule: flush log up to pageLSN before writing dirty page
                if err := bp.wal.FlushUpTo(frame.recLSN); err != nil { return nil, err }
                if err := bp.disk.WritePage(frame.PageID, frame.Data[:]); err != nil { return nil, err }
                delete(bp.dirtyPageTable, frame.PageID)
                bp.bus.Publish(Event{Type: EvtPageWrite, Extra: map[string]interface{}{
                    "page_id": frame.PageID,
                }})
            }
            delete(bp.pageTable, frame.PageID)
            bp.bus.Publish(Event{Type: EvtPageEvict, Extra: map[string]interface{}{
                "page_id": frame.PageID, "dirty": frame.Dirty,
            }})
            return frame, nil
        }
    }
    return nil, ErrBufferPoolFull // all frames pinned
}
```

---

## Phase 5 — WAL

### Log Record Encoding

```go
// internal/wal/record.go

// Wire format: [LSN:8][PrevLSN:8][TxnID:8][PageID:4][Type:1][PayloadLen:4][Payload][CRC32:4]
const LogRecordFixedSize = 8 + 8 + 8 + 4 + 1 + 4 + 4 // = 37 bytes

func EncodeLogRecord(r *LogRecord) []byte {
    buf := make([]byte, LogRecordFixedSize+len(r.Payload))
    binary.LittleEndian.PutUint64(buf[0:],  r.LSN)
    binary.LittleEndian.PutUint64(buf[8:],  r.PrevLSN)
    binary.LittleEndian.PutUint64(buf[16:], r.TxnID)
    binary.LittleEndian.PutUint32(buf[24:], r.PageID)
    buf[28] = byte(r.Type)
    binary.LittleEndian.PutUint32(buf[29:], uint32(len(r.Payload)))
    copy(buf[33:], r.Payload)
    crc := crc32.ChecksumIEEE(buf[:33+len(r.Payload)])
    binary.LittleEndian.PutUint32(buf[33+len(r.Payload):], crc)
    return buf
}

func DecodeLogRecord(data []byte) (*LogRecord, error) {
    if len(data) < LogRecordFixedSize { return nil, ErrInvalidLogRecord }
    r := &LogRecord{}
    r.LSN        = binary.LittleEndian.Uint64(data[0:])
    r.PrevLSN    = binary.LittleEndian.Uint64(data[8:])
    r.TxnID      = binary.LittleEndian.Uint64(data[16:])
    r.PageID     = binary.LittleEndian.Uint32(data[24:])
    r.Type       = LogRecordType(data[28])
    payloadLen   := int(binary.LittleEndian.Uint32(data[29:]))
    if len(data) < LogRecordFixedSize+payloadLen { return nil, ErrInvalidLogRecord }
    r.Payload = make([]byte, payloadLen)
    copy(r.Payload, data[33:])
    // Verify CRC
    storedCRC   := binary.LittleEndian.Uint32(data[33+payloadLen:])
    computedCRC := crc32.ChecksumIEEE(data[:33+payloadLen])
    if storedCRC != computedCRC { return nil, ErrCRCMismatch }
    return r, nil
}
```

---

## Phase 6 — B+Tree: Leaf Node MVCC Operations

### FindAllVersions and Visibility (most critical B+Tree function)

```go
// internal/btree/leaf.go

// FindAllVersions: return all tuple versions for key, newest Xmin first
// The leaf stores versions sorted by (Key ASC, Xmin DESC)
func (lp *LeafPage) FindAllVersions(key []byte) []*MVCCTuple {
    n := lp.NumSlots()
    // Binary search for first slot with this key
    startIdx := lp.BinarySearch(key, TupleKeyExtract)
    if startIdx < 0 { startIdx = -(startIdx + 1) }

    var versions []*MVCCTuple
    for i := startIdx; i < n; i++ {
        data := lp.GetRecord(i)
        if data == nil { continue } // tombstone
        t, err := DecodeTuple(data)
        if err != nil { continue }
        if !bytes.Equal(t.Key, key) { break } // moved past this key
        versions = append(versions, t)
    }
    return versions // already sorted: newest Xmin first (by sort order)
}

// GetVisible: return the visible value for key in this txn's snapshot
func (lp *LeafPage) GetVisible(txn *Transaction, key []byte) (*MVCCTuple, bool) {
    versions := lp.FindAllVersions(key)
    for _, t := range versions {
        if txn.Snapshot.IsVisible(t, txn.engine.tm) {
            return t, true
        }
    }
    return nil, false
}
```

### Insert with WAL Logging

```go
// internal/btree/tree.go

func (bt *BPlusTree) Insert(txn *Transaction, key, value []byte) error {
    // 1. Find the leaf page
    leaf, parentChain, err := bt.findLeafWithParents(key)
    if err != nil { return err }
    defer leaf.Unpin()

    // 2. Build the MVCC tuple
    tuple := &MVCCTuple{
        Xmin:  uint64(txn.ID),
        Xmax:  0,
        Key:   key,
        Value: value,
    }
    encoded := tuple.Encode()

    // 3. WAL: log the insert BEFORE modifying the page
    prevLSN := txn.LastLSN
    lsn := bt.wal.LogInsert(uint64(txn.ID), uint64(leaf.ID), prevLSN, encoded)
    txn.LastLSN = lsn

    // 4. Update page LSN
    leaf.SetPageLSN(lsn)
    bt.bufPool.UnpinPage(leaf.ID, true) // mark dirty

    // 5. Insert into page
    _, err = leaf.InsertRecord(encoded, TupleKeyExtract)
    if err == ErrPageFull {
        // Trigger split
        return bt.splitLeaf(txn, leaf, parentChain, key, tuple)
    }

    bt.bus.Publish(Event{Type: EvtTupleInsert, Extra: map[string]interface{}{
        "key": string(key), "txn_id": txn.ID, "lsn": lsn,
    }})
    return nil
}
```

---

## Phase 7 — Split Algorithm

### Leaf Split (exactly correct sibling link maintenance)

```go
// internal/btree/split.go

func (bt *BPlusTree) splitLeaf(txn *Transaction, leaf *Page, parents []*Page,
    newKey []byte, newTuple *MVCCTuple) error {

    // 1. Allocate new right page
    rightID := bt.disk.AllocPage()
    right := bt.bufPool.FetchPage(rightID)
    _, _, _, _, _, _, oldFlags, oldPrevSib, oldNextSib, _ := leaf.DecodeHeader()
    right.EncodeHeader(PageTypeLeaf, 0, HeaderSize, PageSize, 0, 0, 0, leaf.ID, oldNextSib)

    // 2. WAL log: log page allocation + split BEFORE modifying pages
    prevLSN := txn.LastLSN
    lsn := bt.wal.LogSplit(uint64(txn.ID), uint64(leaf.ID), rightID, prevLSN)
    txn.LastLSN = lsn
    leaf.SetPageLSN(lsn)
    right.SetPageLSN(lsn)

    // 3. Move top half of leaf's slots to right page
    _, _, numSlots, _, _, _, _, _, _, _ := leaf.DecodeHeader()
    midSlot := int(numSlots) / 2
    for i := midSlot; i < int(numSlots); i++ {
        data := leaf.GetRecord(i)
        right.InsertRecord(data, TupleKeyExtract)
        leaf.DeleteRecord(i) // mark tombstone
    }
    leaf.Compact() // clean up tombstones after move

    // 4. Update sibling links
    leaf.SetNextSib(rightID)
    right.SetPrevSib(leaf.ID)
    right.SetNextSib(oldNextSib)
    if oldNextSib != 0 {
        next := bt.bufPool.FetchPage(oldNextSib)
        next.SetPrevSib(rightID)
        bt.bufPool.UnpinPage(oldNextSib, true)
    }

    // 5. Insert the new tuple into the appropriate half
    _, _, _, rightFreeStart, _, _, _, _, _, _ := right.DecodeHeader()
    rightMinKey := TupleKeyExtract(right.GetRecord(0))
    if bytes.Compare(newKey, rightMinKey) >= 0 {
        right.InsertRecord(newTuple.Encode(), TupleKeyExtract)
    } else {
        leaf.InsertRecord(newTuple.Encode(), TupleKeyExtract)
    }

    // 6. Mark both dirty
    bt.bufPool.UnpinPage(leaf.ID, true)
    bt.bufPool.UnpinPage(rightID, true)

    // 7. Promote separator key to parent
    separatorKey := TupleKeyExtract(right.GetRecord(0))
    bt.bus.Publish(Event{Type: EvtTreeSplit, Extra: map[string]interface{}{
        "left_page": leaf.ID, "right_page": rightID,
        "separator": string(separatorKey),
    }})
    return bt.insertIntoParent(txn, parents, leaf.ID, separatorKey, rightID)
}
```

---

## Phase 9 — ARIES Recovery

### Three-Phase Recovery

```go
// internal/recovery/aries.go

func (r *RecoveryManager) Recover() error {
    r.bus.Publish(Event{Type: EvtRecoveryStart})

    // Find checkpoint
    checkpointLSN := r.wal.FindLastCheckpoint()

    // Phase 1: Analysis
    r.bus.Publish(Event{Type: EvtRecoveryAnalysis, Extra: map[string]interface{}{"from_lsn": checkpointLSN}})
    dpt, att := r.Analyze(checkpointLSN)

    // Phase 2: Redo
    r.bus.Publish(Event{Type: EvtRecoveryRedo, Extra: map[string]interface{}{"dirty_pages": len(dpt)}})
    if err := r.Redo(dpt); err != nil { return err }

    // Phase 3: Undo
    r.bus.Publish(Event{Type: EvtRecoveryUndo, Extra: map[string]interface{}{"active_txns": len(att)}})
    if err := r.Undo(att); err != nil { return err }

    r.bus.Publish(Event{Type: EvtRecoveryDone})
    return nil
}

func (r *RecoveryManager) Undo(att ActiveTxnTable) error {
    // Build max-heap of (lastLSN, txnID) for active transactions
    h := NewLSNMaxHeap()
    for txnID, entry := range att {
        if entry.Status == TxnActive && entry.LastLSN != 0 {
            h.Push(entry.LastLSN, txnID)
        }
    }

    for !h.Empty() {
        lsn, txnID := h.Pop()
        record, err := r.wal.FetchRecord(lsn)
        if err != nil { return err }

        if record.Type == LogCLR {
            // CLR: this was already undone; jump to undoNextLSN
            undoNext := decodeUndoNextLSN(record.Payload)
            if undoNext != 0 { h.Push(undoNext, txnID) }
            continue
        }

        // Undo this operation
        clrLSN, err := r.applyUndo(record)
        if err != nil { return err }

        // CLR.undoNextLSN = record.PrevLSN (next thing to undo for this txn)
        r.bus.Publish(Event{Type: EvtRecoveryUndo, Extra: map[string]interface{}{
            "txn_id": txnID, "undone_lsn": lsn, "clr_lsn": clrLSN,
        }})

        if record.PrevLSN != 0 { h.Push(record.PrevLSN, txnID) }
    }
    return nil
}
```

---

## Integration Tests (must all pass)

### TestMVCC_SnapshotIsolation

```go
func TestMVCC_SnapshotIsolation(t *testing.T) {
    engine := openTestEngine(t)
    defer engine.Close()

    // Setup: T1 inserts "balance"=100
    t1 := engine.Begin(SnapshotIsolation)
    engine.Put(t1, []byte("balance"), []byte("100"))
    engine.Commit(t1)

    // T2 starts snapshot isolation txn
    t2 := engine.Begin(SnapshotIsolation)

    // T3 updates "balance" to 200 and commits
    t3 := engine.Begin(SnapshotIsolation)
    engine.Update(t3, []byte("balance"), []byte("200"))
    engine.Commit(t3)

    // T2 must still see 100 (snapshot taken before T3's update)
    val, err := engine.Get(t2, []byte("balance"))
    require.NoError(t, err)
    assert.Equal(t, []byte("100"), val, "SI: T2 must see snapshot value, not T3's committed update")

    engine.Commit(t2)

    // T4 starts after T3: must see 200
    t4 := engine.Begin(SnapshotIsolation)
    val, err = engine.Get(t4, []byte("balance"))
    require.NoError(t, err)
    assert.Equal(t, []byte("200"), val)
    engine.Commit(t4)
}
```

### TestMVCC_WriteSkew (write skew is ALLOWED by SI — verify it happens)

```go
func TestMVCC_WriteSkew_Demonstrable(t *testing.T) {
    engine := openTestEngine(t)
    defer engine.Close()

    // Setup: two doctors on call
    setup := engine.Begin(SnapshotIsolation)
    engine.Put(setup, []byte("alice_on_call"), []byte("true"))
    engine.Put(setup, []byte("bob_on_call"),   []byte("true"))
    engine.Commit(setup)

    // T1 reads all on-call doctors: sees both
    t1 := engine.Begin(SnapshotIsolation)
    alice, _ := engine.Get(t1, []byte("alice_on_call"))
    bob,   _ := engine.Get(t1, []byte("bob_on_call"))
    assert.Equal(t, []byte("true"), alice)
    assert.Equal(t, []byte("true"), bob)

    // T2 also reads: sees both
    t2 := engine.Begin(SnapshotIsolation)
    alice2, _ := engine.Get(t2, []byte("alice_on_call"))
    bob2,   _ := engine.Get(t2, []byte("bob_on_call"))
    assert.Equal(t, []byte("true"), alice2)
    assert.Equal(t, []byte("true"), bob2)

    // T1 decides to clock out Alice (thinks Bob is still on call)
    engine.Update(t1, []byte("alice_on_call"), []byte("false"))
    engine.Commit(t1) // succeeds: no write-write conflict

    // T2 decides to clock out Bob (thinks Alice is still on call — based on OLD snapshot!)
    engine.Update(t2, []byte("bob_on_call"), []byte("false"))
    engine.Commit(t2) // ALSO succeeds: no write-write conflict with T1

    // Final state: BOTH clocked out — invariant violated!
    t3 := engine.Begin(SnapshotIsolation)
    aliceFinal, _ := engine.Get(t3, []byte("alice_on_call"))
    bobFinal,   _ := engine.Get(t3, []byte("bob_on_call"))
    assert.Equal(t, []byte("false"), aliceFinal) // both clocked out!
    assert.Equal(t, []byte("false"), bobFinal)   // WRITE SKEW confirmed
    engine.Commit(t3)

    t.Log("Write skew demonstrated: SI allowed both transactions to commit despite invariant violation")
}
```

### TestARIES_UncommittedRolledBack

```go
func TestARIES_UncommittedRolledBack(t *testing.T) {
    dir := t.TempDir()

    // Phase 1: open engine, write without committing, then "crash"
    {
        engine, _ := OpenEngine(Config{DataDir: dir, SyncWAL: true})
        txn := engine.Begin(SnapshotIsolation)
        for i := 0; i < 500; i++ {
            engine.Put(txn, []byte(fmt.Sprintf("k%04d", i)), []byte("v"))
        }
        // NO COMMIT — simulate crash
        engine.CrashClose() // closes file handles without graceful cleanup
    }

    // Phase 2: reopen → ARIES recovery → uncommitted writes must be absent
    {
        engine, _ := OpenEngine(Config{DataDir: dir, SyncWAL: true})
        defer engine.Close()
        txn := engine.Begin(SnapshotIsolation)
        for i := 0; i < 500; i++ {
            _, err := engine.Get(txn, []byte(fmt.Sprintf("k%04d", i)))
            assert.ErrorIs(t, err, ErrKeyNotFound,
                "uncommitted key k%04d must not exist after crash recovery", i)
        }
        engine.Commit(txn)
    }
}
```

---

## Frontend D3 Key Patterns

### B+Tree Layout

```typescript
// Panel 1: D3 tree layout with leaf-level sibling links

interface TreeNodeData {
  pageID: number;
  type: "internal" | "leaf";
  keys: string[];
  children?: number[]; // child page IDs (internal only)
  prevSib?: number;    // sibling links (leaf only)
  nextSib?: number;
  fillFactor: number;  // 0.0 - 1.0
}

function nodeColor(d: TreeNodeData): string {
  if (d.type === "internal") return "#94a3b8"; // slate gray
  const fill = d.fillFactor;
  if (fill < 0.5) return "#bfdbfe"; // light blue
  if (fill < 0.8) return "#3b82f6"; // blue
  if (fill < 0.9) return "#f97316"; // orange
  return "#ef4444";                  // red (split imminent)
}

// Animate split: node flashes red, fades, two nodes appear
function animateSplit(svg: d3.Selection<...>, leftID: number, rightID: number, sep: string) {
  d3.select(`#node-${leftID}`)
    .transition().duration(200).style("fill", "#ef4444")
    .transition().duration(300).style("fill", nodeColor(getNode(leftID)));
  // ... add right node with slide-in animation
}
```

### MVCC Version Timeline

```typescript
// Panel 3: horizontal bars for each version, colored by status

function renderVersionChain(versions: MVCCVersion[], currentSnapshotXid: number) {
  const xScale = d3.scaleLinear()
    .domain([0, Math.max(...versions.map(v => v.xmax || currentSnapshotXid + 100))])
    .range([0, panelWidth]);

  versions.forEach((v, i) => {
    const isVisible = checkVisibility(v, currentSnapshotXid);
    const color = v.xmax === 0 ? "#22c55e" :  // live
                  isVisible ? "#3b82f6" :        // visible to this snapshot
                  "#ef4444";                      // dead/hidden

    svg.append("rect")
      .attr("x", xScale(v.xmin))
      .attr("width", xScale(v.xmax || currentSnapshotXid) - xScale(v.xmin))
      .attr("y", i * 30)
      .attr("height", 25)
      .attr("fill", color)
      .attr("rx", 3)
      .on("mouseover", () => showTooltip(`Xmin=${v.xmin} Xmax=${v.xmax} Val=${v.value}`));
  });
}
```

---

## Critical Correctness Invariants

After all phases complete, verify:

1. **No dirty reads:** `TestMVCC_DirtyRead_Prevented` — uncommitted writes never visible
2. **Snapshot isolation repeatable reads:** `TestMVCC_SnapshotIsolation` — same key = same value within txn
3. **Write skew demonstrable:** `TestMVCC_WriteSkew_Demonstrable` — SI allows this (by design, not a bug)
4. **WAL-before-data:** `TestBuffer_DirtyNotLostBeforeWAL` — flushedLSN >= pageLSN before every WritePage
5. **Committed survives crash:** `TestARIES_CommitSurvivesCrash` — all committed writes present after recovery
6. **Uncommitted rolled back:** `TestARIES_UncommittedRolledBack` — zero uncommitted writes after recovery
7. **Tree balanced after splits:** `TestBTree_SplitOnInsert` — all leaves at same depth after N splits
8. **Sibling links consistent:** `TestBTree_SiblingLinks` — for all adjacent leaves L, R: `L.NextSib == R.ID AND R.PrevSib == L.ID`
9. **Vacuum safety:** `TestVacuum_PreservesActiveTuples` — vacuum never removes live/visible tuples
10. **Race free:** `go test ./... -race -count=5` zero data races

---

## Code Standards

- **No external DB libraries** — only `stdlib` + `testify`
- **WAL-before-page-write:** ALWAYS call `wal.FlushUpTo(frame.recLSN)` before writing dirty page to disk
- **Page LSN on every modification:** set `page.SetPageLSN(lsn)` after every WAL log record, before marking dirty
- **Visibility function must be exact:** `IsVisible()` has 9 cases; any shortcuts break ACID
- **Write-before-read in WAL:** `WALManager.AppendRecord()` then `page.SetPageLSN()` then `page modify`; never the other way
- **Sibling links on every split:** 4 links must be updated: `leaf.NextSib`, `right.PrevSib`, `right.NextSib`, `oldNext.PrevSib`
- **ARIES CLR undoNextLSN:** when writing a CLR, `undoNextLSN = undoneRecord.PrevLSN` (not the CLR's own prevLSN)
- **Vacuum never removes active tuples:** check `xmax != 0 AND committed(xmax) AND xmax <= oldestActiveTxn`
- **`go test ./... -race` zero races** — every shared field protected
- **TypeScript strict mode** — no `any`

---

## Startup

```bash
# Terminal 1
make run  # or: go run ./cmd/server

# Terminal 2
cd frontend && bun run server/bff.ts & bun run src/main.ts

# Terminal 3
make test-race
```

Open `http://localhost:3001`. Run **`ScenarioInsertAndSearch`** first:
- Watch B+tree Panel 1 grow as 1000 keys are inserted
- Watch splits animate when leaves fill up
- See tree height stay at 2-3

Run **`ScenarioWriteSkew`** for the most surprising demo (Panel 4):
- Both doctors read each other as on-call
- Both clock out
- SI allows this — write skew alert fires
- Explain: "This is NOT a bug in our engine — it's the fundamental limitation of Snapshot Isolation vs Serializability"

Run **`ScenarioCrashRecovery`** for Panel 6:
- 500 commits complete; WAL full of records
- "Crash" button fires
- "Recover" button starts ARIES: Analysis builds DPT+ATT (watch tables fill live)
- Redo phase: pages re-applied in LSN order
- Undo phase: uncommitted txn rolled back with CLR records
- All 500 committed keys verifiable after recovery
