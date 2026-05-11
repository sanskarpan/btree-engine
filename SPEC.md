# SPEC.md — B+Tree Storage Engine with MVCC

> **Backend:** Go 1.22+ (from-scratch; zero external DB dependencies)
> **Frontend:** Bun + TypeScript + Elysia BFF · Vanilla TypeScript + D3.js v7

---

## §1 Project Overview & Language Decision

**Why Go?** Go is the strongest choice for this project — the same reasons as the LSM engine, but even more critical here:
- In-place page mutation is the defining characteristic of B+trees vs LSM; Go's `[4096]byte` page slices and `encoding/binary` map directly to the on-disk format without a serialization layer
- Goroutines model concurrent transactions (each transaction = its own goroutine context)
- `sync.RWMutex` and `sync/atomic` are exactly what latch-based B+tree concurrency needs
- `os.File` with `os.O_SYNC` gives direct fsync control for WAL durability
- The contrast with LSM (previous project) is sharpest in Go: LSM = sequential appends + bloom filters; B+tree = in-place updates + buffer pool + MVCC

**Concepts Covered:**

| Layer | Concepts |
|-------|----------|
| Page layer | 4096-byte slotted pages, page header, slot array, data area, free space, overflow pages |
| B+Tree structure | Internal nodes (key + child pointers), leaf nodes (key + version chain), split, merge/borrow, sibling links |
| Buffer pool | Fixed frames, LRU-K eviction, pin count, dirty bit, page table, WAL-before-eviction rule |
| WAL / ARIES | LSN, log records (BEGIN/UPDATE/DELETE/COMMIT/ABORT/CLR/CHECKPOINT), pageLSN, ARIES 3-phase recovery |
| MVCC | xmin/xmax per tuple, transaction ID counter, transaction status table, snapshot creation |
| Snapshots | xmin_snapshot/xmax_snapshot/xip_list, READ COMMITTED vs SNAPSHOT ISOLATION |
| Visibility rules | 7-case visibility check; tuple version chain traversal; deleted-by-committed check |
| Isolation anomalies | Dirty reads (prevented), non-repeatable reads, phantom reads, write skew (SI allows, demo!) |
| VACUUM | Dead tuple identification, page compaction/defragmentation, oldest active transaction watermark |
| Free page list | Linked list of free pages in a metadata page; alloc/free page primitives |
| Amplification | Read amplification = tree height (O(log_B N)); Write amplification = page writes per leaf update |

**B+Tree vs LSM (this project vs previous):**

| Property | B+Tree (this project) | LSM (previous project) |
|----------|----------------------|----------------------|
| Write | In-place (mutable pages) | Append-only (immutable SSTs) |
| Read | O(log_B N) disk reads, single file | Multi-source merge, bloom filter check |
| Space | ~30% fill-factor overhead | Compaction-dependent amplification |
| MVCC | Per-tuple xmin/xmax in pages | Per-key version in MemTable/SSTables |
| Recovery | ARIES (redo+undo) | WAL replay → MemTable rebuild |
| Concurrency | Page-level latches, MVCC reads | MemTable mutex + immutable SSTables |

---

## §2 Architecture

```
Client (HTTP/WS)
      |
      v
Elysia BFF  (port 3001, Bun)
      |
      v
Engine HTTP Gateway  (port 8080, Go)
      |
      v
StorageEngine
  +-- TransactionManager
  |     +-- TxnIDCounter (atomic.Uint64)
  |     +-- TxnStatusTable (map[TxnID]Status + commit timestamp)
  |     +-- ActiveTxnList (for snapshot xip_list)
  +-- WALManager
  |     +-- LogBuffer (in-memory, flushed on commit)
  |     +-- LogFile (append-only, CRC-protected records)
  |     +-- flushedLSN (atomic)
  +-- BufferPool
  |     +-- Frames [N]*Frame (each = 4096 bytes + metadata)
  |     +-- PageTable map[PageID]FrameID
  |     +-- LRU list.List (eviction order)
  |     +-- DirtyPageTable map[PageID]LSN (recLSN for ARIES)
  +-- BPlusTree
  |     +-- Root PageID
  |     +-- FreeList (linked list in metadata page)
  +-- VacuumWorker (background goroutine)
  +-- CheckpointWorker (background goroutine)
  +-- EventBus (-> WebSocket)

SimulatedWorkload
  +-- ConcurrentTransactionRunner
  +-- IsolationAnomalyDemonstrator
  +-- WriteSkewDemonstrator
```

---

## §3 Page Layout

### 3.1 Page Header (32 bytes)

Every 4096-byte page begins with a fixed-size header:

```
+----------+--------+-----------+-----------+-----------+-----------+-----------+
| Magic    | Type   | NumSlots  | FreeStart | FreeEnd   | PageLSN   | Flags     |
| 4 bytes  | 1 byte | 2 bytes   | 2 bytes   | 2 bytes   | 8 bytes   | 1 byte    |
+----------+--------+-----------+-----------+-----------+-----------+-----------+
| PrevSib  | NextSib| Checksum  |           |           |           |           |
| 4 bytes  | 4 bytes| 4 bytes   |           |           |           |           |
+----------+--------+-----------+-----------+-----------+-----------+-----------+
Total header = 32 bytes

Magic = 0xB17EE501 (B+Tree engine, big-endian)
Type: PageTypeInternal=1, PageTypeLeaf=2, PageTypeMeta=3, PageTypeFree=4
FreeStart: offset from page start to beginning of free space (grows up with slot array)
FreeEnd: offset from page start to end of free space (grows down with data area)
PageLSN: LSN of most recent WAL record that modified this page (ARIES)
Flags: IS_ROOT=0x01, IS_DIRTY=0x02, HAS_OVERFLOW=0x04
PrevSib/NextSib: sibling page IDs for leaf-level linked list (0 = none)
Checksum: CRC32 of page content (computed on write, verified on read)
```

```go
// internal/page/page.go
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

type PageHeader struct {
    Magic     uint32
    Type      PageType
    NumSlots  uint16
    FreeStart uint16  // offset to start of free space
    FreeEnd   uint16  // offset to end of free space
    PageLSN   uint64  // WAL LSN for ARIES recovery
    Flags     uint8
    PrevSib   uint32  // previous sibling page ID (leaf level)
    NextSib   uint32  // next sibling page ID (leaf level)
    Checksum  uint32
}

type Page struct {
    ID   uint32
    Data [PageSize]byte // raw bytes; header + slot array + free space + data area
}

// Free space available for new slots + data
func (p *Page) FreeSpace() int {
    h := p.DecodeHeader()
    return int(h.FreeEnd) - int(h.FreeStart)
}
```

### 3.2 Slotted Page Layout

```
Page (4096 bytes):
+------------------+
| Header (32 B)    |    fixed, always at offset 0
+------------------+
| Slot[0] (4 B)    |    slot array grows DOWN (toward FreeEnd)
| Slot[1] (4 B)    |    each slot = { offset uint16, length uint16 }
| ...              |    sorted in key order (logical reordering)
| Slot[N-1] (4 B)  |
+------------------+
| Free Space       |    FreeStart to FreeEnd
+------------------+
| Data[N-1]        |    data area grows UP (toward FreeStart)
| ...              |    newest records at higher offsets
| Data[1]          |
| Data[0]          |
+------------------+  offset 4096

Invariant: FreeStart + (NumSlots * 4) <= FreeEnd
Insert: data written at FreeEnd - len; FreeEnd -= len; slot appended at FreeStart; FreeStart += 4
Delete: slot marked with length=0 (tombstone); compaction (VACUUM) reclaims space
Binary search: on slot key values (slot array is key-sorted)
```

```go
type Slot struct {
    Offset uint16 // byte offset within page of this record
    Length uint16 // 0 = deleted/tombstone
}

func (p *Page) InsertRecord(data []byte, keyStart, keyLen int) (slotIdx int, err error)
func (p *Page) GetRecord(slotIdx int) []byte
func (p *Page) DeleteRecord(slotIdx int) // marks slot length=0
func (p *Page) BinarySearch(key []byte) int // returns slot index or insertion point
func (p *Page) Compact() // defragments: re-packs data area, updates slot offsets
```

---

## §4 B+Tree Node Formats

### 4.1 Internal Node Record

Each record in an internal node's slot array is a **separator key + child pointer**:

```
Internal record format:
+----------+----------+----------+
| KeyLen   | Key      | ChildID  |
| 2 bytes  | variable | 4 bytes  |
+----------+----------+----------+

Internal node slot array (sorted by key):
Slot[0] -> { key: "banana", childID: page 7 }    // keys < "banana" -> page 7 subtree
Slot[1] -> { key: "mango",  childID: page 12 }   // "banana" <= keys < "mango" -> page 12
Slot[2] -> { key: "zebra",  childID: page 15 }   // "mango" <= keys < "zebra" -> page 15

Special: Header.Flags has RIGHTMOST_CHILD stored separately (last child page for keys >= last separator)
```

```go
type InternalRecord struct {
    Key     []byte
    ChildID uint32 // page ID of child subtree
}
```

### 4.2 Leaf Node Record (MVCC Tuple)

Each record in a leaf node is a **versioned tuple** with MVCC metadata:

```
Leaf record (MVCC tuple) format:
+-------+-------+--------+--------+--------+----------+----------+
| Xmin  | Xmax  | KeyLen | ValLen | Flags  | Key      | Value    |
| 8 B   | 8 B   | 2 B    | 2 B    | 1 B    | variable | variable |
+-------+-------+--------+--------+--------+----------+----------+

Xmin: transaction ID that created this tuple (INSERT or UPDATE)
Xmax: transaction ID that deleted/superseded this tuple (DELETE or UPDATE)
      0 = not deleted
Flags: TUPLE_DELETED=0x01, TUPLE_UPDATED=0x02, TUPLE_LOCKED=0x04

Multiple versions of the SAME key can exist as SEPARATE tuples in the leaf.
They are sorted primarily by key, secondarily by Xmin DESC (newest first).
This is the "PostgreSQL-style" MVCC: versions stored inline in the heap/index.

Example: key "alice" with balance updated three times:
  Slot[0] -> Xmin=100 Xmax=200 key="alice" val="$50"   (oldest version)
  Slot[1] -> Xmin=200 Xmax=300 key="alice" val="$75"   (intermediate)
  Slot[2] -> Xmin=300 Xmax=0   key="alice" val="$100"  (current live version)
```

```go
// internal/mvcc/tuple.go
type TupleFlags uint8
const (
    TupleDeleted TupleFlags = 1 << iota
    TupleUpdated
    TupleLocked
)

type MVCCTuple struct {
    Xmin   uint64 // creating transaction ID
    Xmax   uint64 // deleting transaction ID (0 = alive)
    Flags  TupleFlags
    Key    []byte
    Value  []byte
}

// Serialized size = 8+8+2+2+1 + len(Key) + len(Value) = 21 + key + val
func (t *MVCCTuple) Encode() []byte
func DecodeTuple(data []byte) (*MVCCTuple, error)
```

---

## §5 MVCC — Multi-Version Concurrency Control

### 5.1 Transaction IDs and Status

```go
// internal/mvcc/transaction.go
type TxnID uint64  // 0 = invalid; starts at 1
const InvalidTxnID TxnID = 0

type TxnStatus uint8
const (
    TxnActive    TxnStatus = iota
    TxnCommitted
    TxnAborted
)

type Transaction struct {
    ID          TxnID
    Status      TxnStatus
    IsoLevel    IsolationLevel
    Snapshot    *Snapshot          // taken at BEGIN (SI) or per-statement (RC)
    UndoLog     []UndoRecord       // for rollback; mirrors WAL undo info
    StartedAt   time.Time
    CommittedAt time.Time
}

type TransactionManager struct {
    mu          sync.RWMutex
    nextTxnID   atomic.Uint64      // monotonically increasing
    statusTable map[TxnID]TxnStatus // committed/aborted transactions
    activeTxns  map[TxnID]*Transaction
    eventBus    EventPublisher
}
```

### 5.2 Snapshot

A snapshot captures the database state at a point in time:

```go
type Snapshot struct {
    // PostgreSQL-style snapshot: a tuple is visible iff:
    //   xmin committed AND xmin < XmaxSnapshot
    //   AND (xmax == 0 OR xmax >= XmaxSnapshot OR xmax in XipList OR xmax aborted)
    XminSnapshot TxnID    // oldest active TxnID at snapshot time
    XmaxSnapshot TxnID    // next TxnID to assign (all >= this are future = invisible)
    XipList      []TxnID  // in-progress (started but not committed/aborted) TxnIDs

    // Snapshot creator's own TxnID: can always see own writes
    CreatorTxnID TxnID
}

func (tm *TransactionManager) TakeSnapshot(creator TxnID) *Snapshot {
    tm.mu.RLock()
    defer tm.mu.RUnlock()
    xmin := TxnID(math.MaxUint64)
    var xip []TxnID
    for id := range tm.activeTxns {
        if id < xmin { xmin = id }
        xip = append(xip, id)
    }
    return &Snapshot{
        XminSnapshot: xmin,
        XmaxSnapshot: TxnID(tm.nextTxnID.Load()),
        XipList:      xip,
        CreatorTxnID: creator,
    }
}
```

### 5.3 Visibility Rules

**The core of MVCC.** For a given snapshot S and tuple T:

```go
// IsVisible returns true if tuple T is visible to snapshot S.
// This is the most important correctness property in the entire engine.
func (snap *Snapshot) IsVisible(t *MVCCTuple, tm *TransactionManager) bool {
    // Rule 1: Tuple's own transaction can always see its writes
    if t.Xmin == snap.CreatorTxnID { goto checkXmax }

    // Rule 2: Creator must be committed
    if tm.GetStatus(t.Xmin) != TxnCommitted { return false }

    // Rule 3: Creator must have committed before snapshot was taken
    //   i.e., Xmin < XmaxSnapshot AND Xmin not in XipList
    if t.Xmin >= snap.XmaxSnapshot { return false } // future transaction
    for _, xid := range snap.XipList {
        if t.Xmin == xid { return false } // still in progress when snapshot taken
    }

checkXmax:
    // Rule 4: Not deleted, OR deleted by a future/aborted transaction
    if t.Xmax == 0 { return true } // not deleted

    // Xmax == own txn: this txn deleted it (visible as deleted to own txn)
    if t.Xmax == snap.CreatorTxnID { return false }

    // Xmax aborted: deletion rolled back, tuple is still alive
    if tm.GetStatus(t.Xmax) == TxnAborted { return true }

    // Xmax committed after snapshot: deletion not yet visible to this snapshot
    if t.Xmax >= snap.XmaxSnapshot { return true }
    for _, xid := range snap.XipList {
        if t.Xmax == xid { return true } // deleter still in progress
    }

    // Xmax committed before snapshot: tuple is deleted and invisible
    return false
}
```

### 5.4 Isolation Levels

```go
type IsolationLevel uint8
const (
    ReadCommitted   IsolationLevel = iota // new snapshot per statement
    SnapshotIsolation                     // one snapshot per transaction (REPEATABLE READ)
)

// At READ COMMITTED: snapshot refreshed before each query
// At SNAPSHOT ISOLATION: snapshot fixed at BEGIN

// Key anomaly: WRITE SKEW (allowed by Snapshot Isolation, demonstrated in Panel 4)
// Classic example: two doctors both check "is anyone else on call?"
// Both see the answer is "yes (the other one)" → both clock out
// Result: no doctor on call, invariant violated
// SI allows this because each reads a consistent snapshot with no write-write conflict
```

### 5.5 MVCC Operations

```go
// INSERT: create new tuple with Xmin=currentTxnID, Xmax=0
func (bt *BPlusTree) Insert(txn *Transaction, key, value []byte) error {
    tuple := &MVCCTuple{Xmin: uint64(txn.ID), Xmax: 0, Key: key, Value: value}
    // WAL: log INSERT before modifying page
    lsn := txn.engine.wal.LogInsert(txn.ID, leafPageID, tuple)
    page.SetPageLSN(lsn)
    // Insert tuple into leaf (may trigger split)
    return bt.insertTuple(txn, key, tuple)
}

// UPDATE = logical delete old version + insert new version
func (bt *BPlusTree) Update(txn *Transaction, key, newValue []byte) error {
    // 1. Find existing visible tuple
    old := bt.findVisible(txn, key)
    if old == nil { return ErrKeyNotFound }
    // 2. Log DELETE of old version
    lsn := txn.engine.wal.LogDelete(txn.ID, leafPageID, old)
    // 3. Mark old tuple Xmax = currentTxnID
    old.Xmax = uint64(txn.ID)
    // 4. Insert new version (Xmin = currentTxnID, Xmax = 0)
    return bt.Insert(txn, key, newValue)
}

// DELETE = set Xmax = currentTxnID on the visible tuple
func (bt *BPlusTree) Delete(txn *Transaction, key []byte) error {
    visible := bt.findVisible(txn, key)
    if visible == nil { return ErrKeyNotFound }
    lsn := txn.engine.wal.LogDelete(txn.ID, leafPageID, visible)
    visible.Xmax = uint64(txn.ID)
    page.SetPageLSN(lsn)
    return nil
}

// GET: find newest visible tuple for key
func (bt *BPlusTree) Get(txn *Transaction, key []byte) ([]byte, error) {
    leaf := bt.findLeaf(key)
    // Binary search for key; may find multiple versions
    // Walk versions newest-first; return first visible one
    for _, tuple := range leaf.FindAllVersions(key) { // newest first by Xmin DESC
        if txn.Snapshot.IsVisible(tuple, txn.engine.tm) {
            if tuple.Xmax != 0 && txn.Snapshot.IsVisible(&MVCCTuple{Xmin: tuple.Xmax}, txn.engine.tm) {
                return nil, ErrKeyDeleted
            }
            return tuple.Value, nil
        }
    }
    return nil, ErrKeyNotFound
}
```

---

## §6 B+Tree Operations

### 6.1 Search (Point Lookup)

```go
func (bt *BPlusTree) findLeaf(key []byte) (*Page, error) {
    // Start at root; traverse internal nodes top-down
    current := bt.bufPool.FetchPage(bt.rootPageID)
    defer current.Unpin()
    for current.Type() == PageTypeInternal {
        // Binary search slot array for largest key <= search key
        childID := current.FindChild(key)
        next := bt.bufPool.FetchPage(childID)
        current.Unpin() // release parent latch
        current = next
    }
    return current, nil // leaf page, still pinned
}

// Total disk reads = tree height (typically 3-4 for millions of records)
// Height = ceil(log_B(N)) where B = page fan-out (~100-200 keys/page)
```

### 6.2 Split (Leaf)

```go
func (bt *BPlusTree) splitLeaf(txn *Transaction, leaf *Page, newKey []byte, newTuple *MVCCTuple) error {
    // 1. Allocate new right page
    rightPage := bt.allocPage(PageTypeLeaf)

    // 2. Move top half of tuples to rightPage
    midSlot := leaf.NumSlots() / 2
    leaf.MoveSlots(midSlot, leaf.NumSlots(), rightPage)

    // 3. Update sibling links
    rightPage.SetPrevSib(leaf.ID)
    rightPage.SetNextSib(leaf.NextSib())
    if leaf.NextSib() != 0 {
        next := bt.bufPool.FetchPage(leaf.NextSib())
        next.SetPrevSib(rightPage.ID)
        next.Unpin()
    }
    leaf.SetNextSib(rightPage.ID)

    // 4. WAL: log split operation (page allocations + data movements)
    lsn := txn.engine.wal.LogSplit(txn.ID, leaf.ID, rightPage.ID, rightPage.MinKey())
    leaf.SetPageLSN(lsn); rightPage.SetPageLSN(lsn)

    // 5. Promote separator key (first key of rightPage) to parent
    return bt.insertIntoParent(txn, leaf, rightPage.MinKey(), rightPage.ID)
}

// If parent is full: recursively split internal node
// If root is full: create new root (tree height increases by 1)
```

### 6.3 Merge/Borrow (Leaf)

Triggered when a leaf falls below 50% fill factor after deletion:

```go
func (bt *BPlusTree) handleUnderflow(txn *Transaction, leaf *Page) error {
    parent := bt.fetchParent(leaf)
    sibling := bt.fetchSibling(leaf, parent)

    if sibling.CanLend() { // sibling has > 50% fill
        return bt.borrowFromSibling(txn, leaf, sibling, parent)
        // Move one tuple from sibling to leaf; update separator key in parent
    }
    // Merge: combine leaf and sibling into one page
    return bt.mergeLeaves(txn, leaf, sibling, parent)
    // After merge: free one page; remove separator from parent (may cascade)
}
```

### 6.4 Range Scan (uses leaf sibling links)

```go
func (bt *BPlusTree) Scan(txn *Transaction, start, end []byte) Iterator {
    startLeaf := bt.findLeaf(start)
    return &BTreeIterator{
        engine:  bt,
        txn:     txn,
        current: startLeaf,
        slotIdx: startLeaf.BinarySearch(start),
        end:     end,
    }
}

func (it *BTreeIterator) Next() (*MVCCTuple, error) {
    for {
        if it.slotIdx >= it.current.NumSlots() {
            // Advance to next sibling
            nextID := it.current.NextSib()
            it.current.Unpin()
            if nextID == 0 { return nil, io.EOF }
            it.current = it.engine.bufPool.FetchPage(nextID)
            it.slotIdx = 0
        }
        tuple := it.current.GetTuple(it.slotIdx)
        it.slotIdx++
        if bytes.Compare(tuple.Key, it.end) > 0 { return nil, io.EOF }
        // Skip non-visible versions
        if !it.txn.Snapshot.IsVisible(tuple, it.engine.tm) { continue }
        return tuple, nil
    }
}
```

---

## §7 Buffer Pool Manager

### 7.1 Frame Structure

```go
// internal/buffer/pool.go
const DefaultPoolSize = 256 // number of frames (256 × 4KB = 1MB default)

type Frame struct {
    PageID  uint32
    Data    [PageSize]byte
    PinCount int32          // atomic; 0 = evictable
    Dirty   bool
    RefBit  bool            // for Clock approximation
    recLSN  uint64          // ARIES: LSN that first dirtied this frame
}

type BufferPool struct {
    mu        sync.RWMutex
    frames    []*Frame
    pageTable map[uint32]*Frame // pageID -> frame
    clockHand int               // for Clock eviction
    // LRU list for exact LRU (alternative to Clock)
    lruList   *list.List
    lruMap    map[uint32]*list.Element
    // Dirty page table for ARIES
    dirtyPageTable map[uint32]uint64 // pageID -> recLSN
    // Statistics
    hits, misses uint64
}
```

### 7.2 Fetch (Pin) and Unpin

```go
func (bp *BufferPool) FetchPage(pageID uint32) *Frame {
    bp.mu.Lock()
    if frame, ok := bp.pageTable[pageID]; ok {
        // Cache hit: pin it
        atomic.AddInt32(&frame.PinCount, 1)
        bp.updateLRU(frame)
        atomic.AddUint64(&bp.hits, 1)
        bp.mu.Unlock()
        return frame
    }
    // Cache miss: need to evict and load
    victim := bp.evict()  // find unpinned frame (PinCount == 0)
    if victim.Dirty {
        // WAL-before-eviction: ensure log is flushed before writing dirty page
        bp.wal.FlushUpTo(victim.recLSN)
        bp.disk.WritePage(victim.PageID, victim.Data[:])
        delete(bp.dirtyPageTable, victim.PageID)
    }
    // Load new page
    bp.disk.ReadPage(pageID, victim.Data[:])
    victim.PageID = pageID
    victim.Dirty = false
    victim.PinCount = 1
    victim.recLSN = 0
    bp.pageTable[pageID] = victim
    atomic.AddUint64(&bp.misses, 1)
    bp.mu.Unlock()
    return victim
}

func (bp *BufferPool) UnpinPage(pageID uint32, dirty bool) {
    bp.mu.Lock()
    frame := bp.pageTable[pageID]
    atomic.AddInt32(&frame.PinCount, -1)
    if dirty && !frame.Dirty {
        frame.Dirty = true
        // Record recLSN for ARIES (first time this frame is dirtied)
        if _, exists := bp.dirtyPageTable[pageID]; !exists {
            bp.dirtyPageTable[pageID] = bp.wal.CurrentLSN()
        }
    }
    bp.mu.Unlock()
}
```

### 7.3 Clock Eviction

```go
func (bp *BufferPool) evict() *Frame {
    // Clock sweep: approximate LRU without timestamp overhead
    for {
        frame := bp.frames[bp.clockHand]
        bp.clockHand = (bp.clockHand + 1) % len(bp.frames)
        if atomic.LoadInt32(&frame.PinCount) > 0 { continue } // pinned
        if frame.RefBit {
            frame.RefBit = false // give second chance
            continue
        }
        // Evict this frame
        delete(bp.pageTable, frame.PageID)
        return frame
    }
}
```

### 7.4 WAL-Before-Eviction Rule (STEAL Policy)

The buffer pool uses the **STEAL** policy (dirty pages can be evicted before the creating transaction commits) combined with **NO-FORCE** (pages are not forced to disk at commit). This requires ARIES-style undo during recovery.

**Critical WAL rule:** Before a dirty page is written to disk, all WAL records that modified it (up to its `pageLSN`) must be flushed to disk first.

```go
// In evict():
if victim.Dirty {
    // ARIES WAL rule: pageLSN must be <= flushedLSN before page write
    bp.wal.FlushUpTo(victim.DecodeHeader().PageLSN) // blocks until WAL reaches pageLSN
    bp.disk.WritePage(victim.PageID, victim.Data[:])
}
```

---

## §8 Write-Ahead Log (WAL)

### 8.1 Log Record Format

```
Log record wire format:
+--------+----------+--------+---------+--------+----------+
| LSN    | PrevLSN  | TxnID  | PageID  | Type   | Payload  |
| 8 bytes| 8 bytes  | 8 bytes| 4 bytes | 1 byte | variable |
+--------+----------+--------+---------+--------+----------+

LSN: Log Sequence Number - monotonically increasing byte offset in log file
PrevLSN: LSN of previous log record by the same transaction (for undo chain)
TxnID: which transaction issued this log record
PageID: which data page is being modified (0 for non-page records)
Type: see below
Payload: type-specific content
```

```go
type LogRecordType uint8
const (
    LogBegin    LogRecordType = 1  // transaction started
    LogCommit   LogRecordType = 2  // transaction committed
    LogAbort    LogRecordType = 3  // transaction aborted
    LogInsert   LogRecordType = 4  // tuple inserted: payload = {slotIdx, tupleBytes}
    LogDelete   LogRecordType = 5  // tuple deleted: payload = {slotIdx, oldXmax}
    LogUpdate   LogRecordType = 6  // tuple updated: payload = {slotIdx, before, after}
    LogSplit    LogRecordType = 7  // page split: payload = {leftPageID, rightPageID, separator key}
    LogMerge    LogRecordType = 8  // pages merged
    LogCLR      LogRecordType = 9  // Compensation Log Record (during undo)
    LogCheckpoint LogRecordType = 10 // checkpoint: payload = {DPT, ATT}
    LogPageAlloc  LogRecordType = 11 // new page allocated
    LogPageFree   LogRecordType = 12 // page freed
)

type WALManager struct {
    mu          sync.Mutex
    file        *os.File
    buffer      []byte      // in-memory log buffer (flushed on commit or when full)
    currentLSN  uint64      // next LSN to assign
    flushedLSN  atomic.Uint64
    eventBus    EventPublisher
}
```

### 8.2 Durability Rules

```go
// Rule 1: WAL-before-data: pageLSN <= flushedLSN before writing dirty page to disk
// Rule 2: Write-ahead commit: all log records for a transaction must reach disk before COMMIT reply

func (w *WALManager) Commit(txnID uint64, prevLSN uint64) uint64 {
    lsn := w.AppendRecord(LogRecord{
        Type: LogCommit, TxnID: txnID, PrevLSN: prevLSN,
    })
    w.FlushUpTo(lsn) // force sync: COMMIT is only durable when this returns
    return lsn
}
```

### 8.3 Checkpointing (Fuzzy)

```go
type CheckpointRecord struct {
    // Dirty Page Table: pages in buffer pool with unflushed changes
    DPT map[uint32]uint64 // pageID -> recLSN

    // Active Transaction Table: in-flight transactions
    ATT map[uint64]struct{
        Status   TxnStatus
        LastLSN  uint64 // last log record by this txn
        UndoNextLSN uint64 // for ongoing undo
    }
}

// Fuzzy checkpoint: snapshot DPT+ATT at point in time, don't force dirty pages
// Recovery starts from checkpoint LSN, not beginning of log
```

---

## §9 ARIES Crash Recovery

Three-phase recovery after a crash:

### 9.1 Analysis Phase

```go
func (r *RecoveryManager) Analyze(checkpointLSN uint64) (dpt DirtyPageTable, att ActiveTxnTable) {
    // Scan forward from checkpoint to end of log
    r.wal.ScanFrom(checkpointLSN, func(record LogRecord) {
        switch record.Type {
        case LogBegin:
            att[record.TxnID] = ATTEntry{Status: TxnActive, LastLSN: record.LSN}
        case LogCommit:
            att[record.TxnID] = ATTEntry{Status: TxnCommitted, LastLSN: record.LSN}
        case LogAbort:
            att[record.TxnID] = ATTEntry{Status: TxnAborted, LastLSN: record.LSN}
        case LogInsert, LogDelete, LogUpdate, LogSplit:
            // Update ATT
            att[record.TxnID] = ATTEntry{LastLSN: record.LSN}
            // Update DPT: if page not yet in DPT, add with this recLSN
            if _, exists := dpt[record.PageID]; !exists {
                dpt[record.PageID] = record.LSN // recLSN = first log that dirtied page
            }
        }
    })
    return
}
```

### 9.2 Redo Phase

```go
func (r *RecoveryManager) Redo(dpt DirtyPageTable) {
    // Find earliest recLSN across all dirty pages
    startLSN := minRecLSN(dpt)
    r.wal.ScanFrom(startLSN, func(record LogRecord) {
        if record.Type != LogInsert && record.Type != LogDelete &&
           record.Type != LogUpdate && record.Type != LogSplit && record.Type != LogCLR {
            return // skip non-data records
        }
        // Skip if page not in DPT (already flushed before crash)
        recLSN, inDPT := dpt[record.PageID]
        if !inDPT || record.LSN < recLSN { return }
        // Skip if pageLSN >= record.LSN (already on disk)
        page := r.bufPool.FetchPage(record.PageID)
        if page.DecodeHeader().PageLSN >= record.LSN { page.Unpin(); return }
        // REDO the operation (reapply to page)
        r.applyRedo(page, record)
        page.Unpin()
    })
}
```

### 9.3 Undo Phase

```go
func (r *RecoveryManager) Undo(att ActiveTxnTable) {
    // Undo all ACTIVE (not committed/aborted) transactions
    // Uses a priority queue of LSNs to process in reverse order
    toUndo := NewMaxHeap()
    for txnID, entry := range att {
        if entry.Status == TxnActive {
            toUndo.Push(entry.LastLSN, txnID)
        }
    }
    for !toUndo.Empty() {
        lsn, txnID := toUndo.Pop()
        record := r.wal.FetchRecord(lsn)
        if record.Type == LogCLR {
            // CLR: skip to undoNextLSN (this operation was already undone)
            if record.UndoNextLSN != 0 { toUndo.Push(record.UndoNextLSN, txnID) }
            continue
        }
        // Undo this operation + write a CLR
        clrLSN := r.applyUndo(record)
        // CLR.UndoNextLSN = record.PrevLSN (next operation to undo for this txn)
        r.wal.WriteCLR(txnID, lsn, record.PrevLSN, clrLSN)
        if record.PrevLSN != 0 { toUndo.Push(record.PrevLSN, txnID) }
    }
}
```

---

## §10 VACUUM (Dead Tuple Reclamation)

Background goroutine that removes dead MVCC versions:

```go
// A tuple is "dead" if:
// 1. Xmax is committed AND
// 2. Xmax <= oldestActiveTxn (no active transaction can see the dead version)
//
// Dead tuples accumulate from UPDATE and DELETE operations.
// VACUUM walks all leaf pages and compacts them.

func (v *VacuumWorker) Run(ctx context.Context) {
    ticker := time.NewTicker(v.cfg.VacuumInterval)
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C:
            oldestActiveTxn := v.engine.tm.OldestActiveTxn()
            v.vacuum(oldestActiveTxn)
        }
    }
}

func (v *VacuumWorker) vacuumLeafPage(page *Page, oldestActive TxnID) {
    for i := 0; i < page.NumSlots(); i++ {
        tuple := page.GetTuple(i)
        if tuple.Xmax == 0 { continue } // alive
        if v.engine.tm.GetStatus(TxnID(tuple.Xmax)) != TxnCommitted { continue }
        if TxnID(tuple.Xmax) > oldestActive { continue } // some txn might still see it
        // This version is dead: mark slot as tombstone
        page.DeleteSlot(i)
        v.engine.eventBus.Publish(Event{Type: EvtVacuumTuple, Extra: map[string]interface{}{
            "key": string(tuple.Key), "xmin": tuple.Xmin, "xmax": tuple.Xmax,
        }})
    }
    // If page has many tombstones, compact it
    if page.FragmentationRatio() > 0.3 {
        page.Compact()
    }
}
```

---

## §11 Free Page Management

A dedicated **metadata page** (page 0) acts as a free-list header:

```go
type MetaPage struct {
    Magic          uint32  // 0xB17EE501
    RootPageID     uint32  // B+tree root
    FreeListHead   uint32  // head of free page linked list (0 = empty)
    NextPageID     uint32  // total pages allocated in file (for growing the file)
    TxnIDCounter   uint64  // persisted next TxnID (for recovery)
}

// Each free page stores its next-free pointer at offset 0:
type FreePage struct {
    NextFree uint32 // page ID of next free page (0 = end of list)
}

func (dm *DiskManager) AllocPage() uint32 {
    meta := dm.fetchMeta()
    if meta.FreeListHead != 0 {
        pageID := meta.FreeListHead
        freePage := dm.readPage(pageID)
        meta.FreeListHead = freePage.NextFree
        dm.writeMeta(meta)
        return pageID
    }
    // No free pages: grow the file
    pageID := meta.NextPageID
    meta.NextPageID++
    dm.writeMeta(meta)
    dm.growFile(pageID) // extend file by one page
    return pageID
}

func (dm *DiskManager) FreePage(pageID uint32) {
    meta := dm.fetchMeta()
    dm.writePage(pageID, FreePage{NextFree: meta.FreeListHead})
    meta.FreeListHead = pageID
    dm.writeMeta(meta)
}
```

---

## §12 Simulation & Demonstration Scenarios

```go
// 8 pre-built scenarios:
const (
    ScenarioInsertAndSearch    = "insert_search"      // 1000 inserts, verify B+tree structure
    ScenarioSplitVisual        = "split_visual"       // fill a leaf to trigger split, watch it
    ScenarioMVCCBasic          = "mvcc_basic"         // two txns read same key, one updates
    ScenarioReadCommitted       = "read_committed"     // demonstrate non-repeatable read at RC
    ScenarioSnapshotIso         = "snapshot_iso"       // REPEATABLE READ: consistent view
    ScenarioWriteSkew           = "write_skew"         // SI anomaly: on-call doctors example
    ScenarioCrashRecovery       = "crash_recovery"     // simulate crash mid-transaction; recover
    ScenarioConcurrentTxns      = "concurrent_txns"    // 5 concurrent transactions; show MVCC
    ScenarioVacuum              = "vacuum"             // fill with updates, run vacuum, show reclaim
)
```

---

## §13 Event System

```go
type EventType string
const (
    // Page operations
    EvtPageRead       EventType = "page_read"
    EvtPageWrite      EventType = "page_write"
    EvtPageEvict      EventType = "page_evict"
    EvtPageDirty      EventType = "page_dirty"
    EvtPageSplit      EventType = "page_split"
    EvtPageMerge      EventType = "page_merge"
    EvtPageAlloc      EventType = "page_alloc"
    EvtPageFree       EventType = "page_free"
    // B+Tree
    EvtTreeInsert     EventType = "tree_insert"
    EvtTreeSearch     EventType = "tree_search"
    EvtTreeDelete     EventType = "tree_delete"
    EvtTreeSplit      EventType = "tree_split"
    EvtTreeMerge      EventType = "tree_merge"
    // MVCC
    EvtTxnBegin       EventType = "txn_begin"
    EvtTxnCommit      EventType = "txn_commit"
    EvtTxnAbort       EventType = "txn_abort"
    EvtTupleInsert    EventType = "tuple_insert"
    EvtTupleDelete    EventType = "tuple_delete"
    EvtTupleVisible   EventType = "tuple_visible"
    EvtTupleHidden    EventType = "tuple_hidden"
    EvtSnapshotTaken  EventType = "snapshot_taken"
    EvtWriteSkew      EventType = "write_skew"
    // WAL
    EvtWALAppend      EventType = "wal_append"
    EvtWALFlush       EventType = "wal_flush"
    EvtWALCheckpoint  EventType = "wal_checkpoint"
    // Recovery
    EvtRecoveryStart  EventType = "recovery_start"
    EvtRecoveryAnalysis EventType = "recovery_analysis"
    EvtRecoveryRedo   EventType = "recovery_redo"
    EvtRecoveryUndo   EventType = "recovery_undo"
    EvtRecoveryDone   EventType = "recovery_done"
    // Buffer Pool
    EvtBufferHit      EventType = "buffer_hit"
    EvtBufferMiss     EventType = "buffer_miss"
    // Vacuum
    EvtVacuumStart    EventType = "vacuum_start"
    EvtVacuumTuple    EventType = "vacuum_tuple"
    EvtVacuumPage     EventType = "vacuum_page"
    EvtVacuumDone     EventType = "vacuum_done"
)
```

---

## §14 REST API

Base: `http://localhost:8080/api/v1`

### Engine / Transaction API
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/engine/open` | Open/init engine with config |
| `POST` | `/engine/close` | Graceful close |
| `GET`  | `/engine/stats` | All stats snapshot |
| `POST` | `/txn/begin` | Begin transaction → `{txnID, snapshot}` |
| `POST` | `/txn/:id/commit` | Commit |
| `POST` | `/txn/:id/abort` | Abort/rollback |
| `POST` | `/txn/:id/set-isolation` | Change isolation level |

### Data Operations (transactional)
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/txn/:id/put` | `{key, value}` |
| `GET`  | `/txn/:id/get` | `?key=...` |
| `DELETE` | `/txn/:id/delete` | `{key}` |
| `GET`  | `/txn/:id/scan` | `?start=...&end=...` |

### Tree Inspection
| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/tree/structure` | Full tree structure (pages, keys) |
| `GET`  | `/tree/page/:id` | Page contents (header + slots + tuples + MVCC) |
| `GET`  | `/tree/path` | `?key=...` → root-to-leaf traversal path |

### MVCC Inspection
| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/mvcc/versions` | `?key=...` → all versions of a key |
| `GET`  | `/mvcc/snapshot/:txnID` | Snapshot details for a transaction |
| `GET`  | `/mvcc/visibility` | `?key=...&txnID=...` → visible tuple + why |
| `GET`  | `/mvcc/dead-tuples` | Count of dead tuples per page |

### Buffer Pool
| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/buffer/stats` | Hit rate, frames, dirty pages |
| `GET`  | `/buffer/frames` | All frames with pageID, dirty, pinCount |

### WAL / Recovery
| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/wal/tail` | Last N log records |
| `POST` | `/wal/checkpoint` | Force checkpoint |
| `POST` | `/engine/crash` | Simulate crash (close abruptly) |
| `POST` | `/engine/recover` | Re-open and run ARIES recovery |

### Scenarios
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/scenarios/:name/run` | Run pre-built scenario |
| `GET`  | `/scenarios` | List available scenarios |

**WebSocket:** `ws://localhost:8080/ws`

---

## §15 Frontend — Seven Live Panels

### Panel 1: B+Tree Structure Visualizer

D3 tree layout showing the live B+tree structure:
- Each node = rectangle; internal nodes show separator keys; leaf nodes show key count
- Edges between nodes (parent → children) animated on traversal
- Node fill: gray=internal, blue=leaf; red border=recently split; yellow=accessed by current query
- Leaf nodes linked at bottom level (sibling links shown as horizontal arrows)
- Click node: expand to show slot array + actual key-value pairs with MVCC tuple headers
- On split: animate: one node grows, flashes red, splits into two with separator promoted to parent
- Tree height counter (typically 2-4); fan-out per internal node shown

### Panel 2: Page Inspector (Slotted Page Viewer)

Hex-dump-style visualization of a selected 4096-byte page:
- Top section: decoded page header (magic, type, numSlots, freeStart, freeEnd, pageLSN)
- Middle section: slot array as colored blocks (each 4 bytes); click slot → highlight corresponding data
- Free space: gray diagonal-hatch region
- Data area: colored blocks (each = one MVCC tuple); click → expand MVCC tuple details
- MVCC tuple decoded: `Xmin=100 Xmax=200 Key="alice" Val="$75"` with status badge (committed/active/aborted)
- Fill factor bar: % used; turns red when >80%

### Panel 3: MVCC Version Chain Viewer

For a selected key, show all versions over time:

```
Key: "alice"
  Slot 0: Xmin=100 Xmax=200  val="$50"   [DEAD - xmax committed]
  Slot 1: Xmin=200 Xmax=300  val="$75"   [DEAD - xmax committed]
  Slot 2: Xmin=300 Xmax=0    val="$100"  [LIVE - visible to snap >= 300]

Snapshot S (txn 250): sees val="$75" (xmin=200 < 250; xmax=300 > 250)
Snapshot S (txn 350): sees val="$100" (latest committed)
```

- Timeline visualization: X-axis = transaction IDs; each version = horizontal bar spanning [xmin, xmax]
- Live transaction shown with question mark at xmax=∞ (still active)
- "What does TxnID=X see?" slider → highlights the visible bar

### Panel 4: Isolation Level & Anomaly Demo

Split screen for two concurrent transactions:

**Left: READ COMMITTED**
- Transaction T1 reads "balance": sees 100 → another txn commits 200 → T1 reads again: sees 200
- Non-repeatable read highlighted in red: "Same query, different result!"

**Right: SNAPSHOT ISOLATION**
- T1 takes snapshot at start → both reads see 100 (consistent snapshot)
- BUT: write skew demo:
  ```
  T1: reads doctors_on_call = {Alice, Bob}  → decides to clock out Alice
  T2: reads doctors_on_call = {Alice, Bob}  → decides to clock out Bob
  T1: commits (Alice clocked out)
  T2: commits (Bob clocked out)
  Result: doctors_on_call = {} ← INVARIANT VIOLATED!
  SI allowed this because no write-write conflict, only write-skew!
  ```
- Red "WRITE SKEW DETECTED" banner appears; explain why SI doesn't prevent this

### Panel 5: Buffer Pool Visualizer

Grid of N frames (default 64, configurable):
- Each frame = colored rectangle; color: gray=empty, blue=clean, orange=dirty, red=pinned+dirty
- PageID label inside each frame
- LRU position indicator (1=most recently used)
- Clock hand pointer sweeping around a circular arrangement
- On cache hit: frame flashes green; on miss: frame eviction animation (old page writes back, new page reads in)
- Hit rate gauge (pie chart); target > 90%
- Dirty page count bar: WAL flush required before eviction

### Panel 6: WAL & Recovery Visualizer

Top: scrolling WAL log:
```
LSN=1001 TxnID=5 Type=BEGIN    prevLSN=0
LSN=1009 TxnID=5 Type=INSERT   prevLSN=1001  page=42  key="alice" val="$100"
LSN=1018 TxnID=5 Type=COMMIT   prevLSN=1009
LSN=1025 TxnID=6 Type=BEGIN    prevLSN=0
LSN=1031 TxnID=6 Type=UPDATE   prevLSN=1025  page=42  key="alice" old="$100" new="$150"
[CRASH HERE]
```
- Color: BEGIN=blue, INSERT=green, UPDATE=yellow, DELETE=red, COMMIT=teal, ABORT=orange, CLR=purple

Bottom: ARIES Recovery Simulator:
- "Crash" button → engine crashes mid-transaction
- "Recover" button → runs 3-phase ARIES:
  1. Analysis: scan from checkpoint → builds ATT (active txns) + DPT (dirty pages) in real time
  2. Redo: replay forward, page by page (animation: pages being rehydrated)
  3. Undo: roll back uncommitted txns (animation: CLR records being written)
- Dirty Page Table and Active Transaction Table shown as live tables during recovery

### Panel 7: Scenario Control + Metrics

Top: scenario dropdown + Run + Speed (0.1×–10×) + Step Mode

Metrics dashboard:
- **Tree height**: current depth (should be 2-4 for thousands of records)
- **Page count**: total pages allocated / freed
- **Buffer hit rate**: % of reads served from buffer pool without disk I/O
- **Dead tuple count**: live graph updated by vacuum
- **WAL write rate**: bytes/sec to WAL
- **Concurrent txns**: live count of BEGIN vs COMMIT/ABORT
- **Split count**: total splits since start (indicates write load)

Write skew detector: monitors for snapshot isolation anomalies in concurrent scenario and fires alert

---

## §16 Elysia BFF

```typescript
// frontend/server/bff.ts
import { Elysia } from "elysia";
const BACKEND = "http://localhost:8080";
const app = new Elysia()
  .get("/api/*",    ({ params }) => fetch(`${BACKEND}/api/${params["*"]}`).then(r => r.json()))
  .post("/api/*",   ({ params, body }) => fetch(`${BACKEND}/api/${params["*"]}`, {
      method: "POST", body: JSON.stringify(body),
      headers: { "Content-Type": "application/json" }}).then(r => r.json()))
  .delete("/api/*", ({ params }) => fetch(`${BACKEND}/api/${params["*"]}`, { method: "DELETE" }).then(r => r.json()))
  .ws("/ws", {
      open(ws) {
          const bk = new WebSocket("ws://localhost:8080/ws");
          bk.onmessage = m => ws.send(m.data);
          (ws as any)._bk = bk;
      },
      close(ws) { (ws as any)._bk?.close(); },
  })
  .listen(3001);
export type App = typeof app;
```

---

## §17 File Structure

```
btree-engine/
+-- cmd/server/main.go
+-- internal/
|   +-- page/
|   |   +-- page.go           # Page{}, PageHeader, encode/decode header
|   |   +-- slotted.go        # Slot array: insert, delete, binary search, compact
|   |   +-- overflow.go       # Overflow page handling for large values
|   +-- btree/
|   |   +-- internal.go       # Internal node: InternalRecord, FindChild, InsertKey
|   |   +-- leaf.go           # Leaf node: MVCCTuple, InsertTuple, FindVersions
|   |   +-- split.go          # Leaf/internal split algorithm
|   |   +-- merge.go          # Merge and borrow-from-sibling
|   |   +-- cursor.go         # Range scan iterator (sibling link traversal)
|   |   +-- tree.go           # BPlusTree: Get, Insert, Update, Delete, Scan
|   +-- mvcc/
|   |   +-- transaction.go    # Transaction, TxnID, TxnStatus, TransactionManager
|   |   +-- snapshot.go       # Snapshot, IsVisible(), TakeSnapshot()
|   |   +-- tuple.go          # MVCCTuple, encode/decode, visibility rules
|   |   +-- vacuum.go         # VacuumWorker: dead tuple detection + page compaction
|   +-- buffer/
|   |   +-- pool.go           # BufferPool: FetchPage, UnpinPage, eviction
|   |   +-- frame.go          # Frame: pageID, data, pinCount, dirty, refBit
|   |   +-- clock.go          # Clock eviction algorithm
|   +-- wal/
|   |   +-- wal.go            # WALManager: Append, FlushUpTo, Commit
|   |   +-- record.go         # LogRecord types, encode/decode, CRC32
|   |   +-- checkpoint.go     # Fuzzy checkpoint: DPT + ATT snapshot
|   +-- recovery/
|   |   +-- aries.go          # ARIES 3-phase recovery: Analyze, Redo, Undo
|   |   +-- clr.go            # Compensation Log Record handling
|   +-- disk/
|   |   +-- manager.go        # DiskManager: ReadPage, WritePage, AllocPage, FreePage
|   |   +-- freelist.go       # Free page linked list
|   |   +-- meta.go           # MetaPage: root, freelist head, txn counter
|   +-- engine/
|   |   +-- engine.go         # StorageEngine: Open, Close, recovery on Open
|   |   +-- config.go         # Config: pool size, WAL sync, vacuum interval
|   +-- simulation/
|   |   +-- scenarios.go      # 9 pre-built scenarios
|   |   +-- workload.go       # Insert/read/update workload generators
|   +-- events/
|       +-- bus.go            # EventBus: non-blocking fan-out
+-- gateway/
|   +-- server.go
|   +-- rest.go               # All REST handlers
|   +-- websocket.go
+-- frontend/
|   +-- server/bff.ts
|   +-- src/
|       +-- api/{client,types}.ts
|       +-- store/engine.ts
|       +-- components/
|       |   +-- tree/          # Panel 1: B+tree structure
|       |   +-- page/          # Panel 2: slotted page inspector
|       |   +-- versions/      # Panel 3: MVCC version chain
|       |   +-- isolation/     # Panel 4: anomaly demo
|       |   +-- buffer/        # Panel 5: buffer pool
|       |   +-- wal/           # Panel 6: WAL + recovery
|       |   +-- scenarios/     # Panel 7: control + metrics
|       +-- ws/client.ts
+-- test/
|   +-- unit/
|   |   +-- page_test.go
|   |   +-- slotted_test.go
|   |   +-- btree_test.go
|   |   +-- mvcc_visibility_test.go
|   |   +-- snapshot_test.go
|   |   +-- buffer_pool_test.go
|   |   +-- wal_test.go
|   |   +-- vacuum_test.go
|   +-- integration/
|       +-- crash_recovery_test.go
|       +-- concurrent_txns_test.go
|       +-- write_skew_test.go
|       +-- range_scan_test.go
+-- config.yaml
+-- go.mod
+-- Makefile
```

---

## §18 Configuration

```yaml
engine:
  data_file: "./data/btree.db"
  page_size: 4096
  buffer_pool_size: 256        # number of frames (256 × 4KB = 1MB)

wal:
  log_file: "./data/wal.log"
  buffer_size: 65536           # 64KB in-memory log buffer
  sync_on_commit: true         # fsync on every COMMIT

recovery:
  checkpoint_interval: "30s"   # fuzzy checkpoint frequency

mvcc:
  default_isolation: "snapshot" # "read_committed" | "snapshot"
  vacuum_interval: "60s"
  vacuum_dead_threshold: 0.20  # compact page if >20% dead tuples

tree:
  fill_factor: 0.75            # target fill: split if page > 75% full
  min_fill_factor: 0.40        # merge if page < 40% full

gateway:
  port: 8080
  ws_buffer: 512

frontend:
  bff_port: 3001
```

---

## §19 Key Correctness Properties

1. **Visibility Safety:** A transaction never sees uncommitted data from other transactions. `forall T1, T2: T2 active AND T1.snapshot.xip contains T2 => T1 cannot see any tuple with Xmin == T2`

2. **Consistent Read (Snapshot Isolation):** All reads within a snapshot isolation transaction see a consistent snapshot. Two reads of the same key return the same value (no non-repeatable reads).

3. **WAL Durability:** A committed transaction's writes survive any subsequent crash. `WAL.flushedLSN >= commitRecord.LSN` before COMMIT returns to client.

4. **WAL-Before-Data:** A dirty page is never written to disk before all WAL records with LSN <= pageLSN are flushed. `flushedLSN >= page.pageLSN` before `WritePage()` is called.

5. **ARIES Correctness:** After crash recovery, all committed transactions are fully applied (redo) and all uncommitted transactions are fully rolled back (undo). Database is in the state it would have been if no crash occurred.

6. **B+Tree Balance:** All leaf nodes are at the same height. After any split or merge, the tree remains valid.

7. **MVCC Non-Blocking Reads:** Reads never block writers; writers never block reads. A read-only transaction acquires no write locks, only buffer pool pin counts.

8. **Sibling Link Consistency:** For any two adjacent leaf pages L and R, `L.NextSib == R.ID AND R.PrevSib == L.ID`. Range scans via sibling traversal return complete, sorted results.

9. **Vacuum Safety:** Vacuum only removes tuples where `xmax committed AND xmax <= oldestActiveTxn`. A vacuum run never removes a tuple visible to any active transaction.

10. **Write Skew (by design):** Snapshot Isolation intentionally allows write skew. The `ScenarioWriteSkew` demonstrates this as expected behavior, not a bug.

---

## §20 Performance Targets

| Metric | Target |
|--------|--------|
| Point read (warm cache, tree height 3) | < 50µs |
| Point read (cold, 3 disk reads) | < 5ms |
| Point write (WAL sync, in-place update) | < 2ms |
| Range scan (1000 keys, sequential) | < 10ms |
| Buffer hit rate (random workload) | > 85% |
| WAL append throughput | > 100k records/s |
| Concurrent txns without conflict | > 1000 txns/s |
| ARIES recovery (100MB log) | < 10s |
| VACUUM throughput | > 10k dead tuples/s |
| Tree height (1M records, 4096B pages, ~100 keys/node) | 3-4 |
