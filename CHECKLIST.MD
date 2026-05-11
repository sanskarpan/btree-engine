# CHECKLIST.md — B+Tree Storage Engine with MVCC

> Priority: 🔴 blocking · 🟡 important · 🟢 enhancement · 🔵 stretch
> **Build phases in strict order. Each phase's tests must pass before next begins.**

---

## Phase 0 — Bootstrap (10 tasks)

- [x] 🔴 `go mod init btree-engine`; add `gorilla/websocket`, `stretchr/testify` only — zero external DB dependencies
- [x] 🔴 Directory structure: `cmd/server/`, `internal/{page,btree,mvcc,buffer,wal,recovery,disk,engine,simulation,events}/`, `gateway/`, `test/`
- [x] 🔴 `config.yaml` with all defaults from SPEC §18
- [x] 🔴 `cmd/server/main.go`: parse config, open engine, start gateway
- [x] 🔴 `Makefile`: `run`, `test`, `test-race`, `bench`, `lint`
- [x] 🔴 `gateway/server.go`: net/http router, REST routes, WS endpoint
- [x] 🔴 `internal/events/bus.go`: non-blocking `EventBus.Publish`, Subscribe, Unsubscribe
- [x] 🔴 `cd frontend && bun init`; add `elysia`, `@elysiajs/eden`, `d3`, TypeScript
- [x] 🔴 `frontend/server/bff.ts`: Elysia BFF (SPEC §16)
- [x] 🔴 `go test ./... -race` passes on empty stubs

---

## Phase 1 — Page Layer: Header + Slotted Page (18 tasks)

- [x] 🔴 `internal/page/page.go`: `PageSize=4096`, `HeaderSize=32`, `MagicNumber=0xB17EE501`
- [x] 🔴 `PageType` enum: `PageTypeInternal=1`, `PageTypeLeaf=2`, `PageTypeMeta=3`, `PageTypeFree=4`
- [x] 🔴 `PageHeader` struct: Magic, Type, NumSlots, FreeStart, FreeEnd, PageLSN, Flags, PrevSib, NextSib, Checksum
- [x] 🔴 `Page.EncodeHeader(h PageHeader)` and `Page.DecodeHeader() PageHeader` using `encoding/binary` (little-endian)
- [x] 🔴 `Page.VerifyChecksum() bool`: CRC32 over bytes 0..4091; stored at bytes 4092..4095
- [x] 🔴 `Page.UpdateChecksum()`: called after every write
- [x] 🔴 `internal/page/slotted.go`: `Slot{Offset, Length uint16}`
- [x] 🔴 `Page.InsertRecord(data []byte) (slotIdx int, err error)`: write data at FreeEnd−len; append slot at FreeStart; check FreeStart+4 <= FreeEnd−len
- [x] 🔴 `Page.GetRecord(slotIdx int) []byte`: decode slot → read data at slot.Offset with slot.Length bytes
- [x] 🔴 `Page.DeleteRecord(slotIdx int)`: set slot.Length = 0 (tombstone)
- [x] 🔴 `Page.BinarySearch(key []byte, extractKey func([]byte) []byte) int`: binary search over slot array by key
- [x] 🔴 `Page.Compact()`: rebuild data area from non-tombstone slots; reset FreeStart/FreeEnd; emit `EvtPageCompact`
- [x] 🔴 `Page.FreeSpace() int = int(FreeEnd) - int(FreeStart)`
- [x] 🔴 `Page.FragmentationRatio() float64`: tombstone bytes / total data bytes
- [x] 🔴 Unit test `TestPage_InsertAndGet`: insert 100 records; get each by slot index; verify content
- [x] 🔴 Unit test `TestPage_BinarySearch`: insert 50 keys in random order (sorted slot array); binary search for each
- [x] 🔴 Unit test `TestPage_Compact`: insert 20, delete 10, compact; verify 10 remain; FreeSpace increases
- [x] 🔴 Unit test `TestPage_ChecksumDetectsCorruption`: corrupt 1 byte; VerifyChecksum() returns false
- [x] 🔴 `go test ./internal/page/... -race` zero races

---

## Phase 2 — Disk Manager + Free Page List (10 tasks)

- [x] 🔴 `internal/disk/manager.go`: `DiskManager{file *os.File, pageSize int}`
- [x] 🔴 `DiskManager.ReadPage(pageID uint32, buf []byte) error`: `pread` at `pageID * 4096`
- [x] 🔴 `DiskManager.WritePage(pageID uint32, data []byte) error`: `pwrite` at `pageID * 4096`
- [x] 🔴 `internal/disk/meta.go`: `MetaPage{Magic, RootPageID, FreeListHead, NextPageID, TxnIDCounter}` at page 0
- [x] 🔴 `DiskManager.ReadMeta() MetaPage` and `DiskManager.WriteMeta(m MetaPage)`
- [x] 🔴 `internal/disk/freelist.go`: `DiskManager.AllocPage() uint32` (use free list head, or grow file)
- [x] 🔴 `DiskManager.FreePage(pageID uint32)`: push to free list head; write meta
- [x] 🔴 Unit test `TestDisk_ReadWritePage`: write page at ID 5; read back; verify bytes
- [x] 🔴 Unit test `TestDisk_FreeList`: alloc 10 pages; free 5; alloc 5 more → should reuse freed pages
- [x] 🔴 `go test ./internal/disk/... -race` zero races

---

## Phase 3 — MVCC Tuples + Visibility Rules (20 tasks)

- [x] 🔴 `internal/mvcc/tuple.go`: `MVCCTuple{Xmin, Xmax uint64, Flags TupleFlags, Key, Value []byte}`
- [x] 🔴 `TupleFlags` enum: `TupleDeleted=1`, `TupleUpdated=2`, `TupleLocked=4`
- [x] 🔴 `MVCCTuple.Encode() []byte`: `[Xmin:8][Xmax:8][KeyLen:2][ValLen:2][Flags:1][Key][Value]`
- [x] 🔴 `DecodeTuple(data []byte) (*MVCCTuple, error)`
- [x] 🔴 `internal/mvcc/transaction.go`: `TxnID uint64`, `TxnStatus` enum (Active/Committed/Aborted)
- [x] 🔴 `Transaction{ID, Status, IsoLevel, Snapshot, UndoLog []UndoRecord}`
- [x] 🔴 `TransactionManager{mu, nextTxnID atomic.Uint64, statusTable, activeTxns}`
- [x] 🔴 `TransactionManager.Begin(isoLevel) *Transaction`: assign next TxnID; add to activeTxns; emit `EvtTxnBegin`
- [x] 🔴 `TransactionManager.Commit(txn) error`: update statusTable; remove from activeTxns; emit `EvtTxnCommit`
- [x] 🔴 `TransactionManager.Abort(txn) error`: update statusTable; remove from activeTxns; emit `EvtTxnAbort`
- [x] 🔴 `internal/mvcc/snapshot.go`: `Snapshot{XminSnapshot, XmaxSnapshot TxnID, XipList []TxnID, CreatorTxnID TxnID}`
- [x] 🔴 `TransactionManager.TakeSnapshot(creator TxnID) *Snapshot`: lock; iterate activeTxns; build XipList
- [x] 🔴 `Snapshot.IsVisible(t *MVCCTuple, tm *TransactionManager) bool`: implement all 7 visibility rules from SPEC §5.3 EXACTLY
- [x] 🔴 `TransactionManager.GetStatus(txnID TxnID) TxnStatus`: check statusTable; unknown = aborted (safe default)
- [x] 🔴 `TransactionManager.OldestActiveTxn() TxnID`: min TxnID across all activeTxns
- [x] 🔴 Unit test `TestVisibility_OwnWrites`: txn sees its own uncommitted inserts
- [x] 🔴 Unit test `TestVisibility_CommittedBeforeSnapshot`: txn sees committed rows from before snapshot
- [x] 🔴 Unit test `TestVisibility_UncommittedHidden`: txn does NOT see uncommitted rows from other txns
- [x] 🔴 Unit test `TestVisibility_DeletedByCommitted`: txn does NOT see row deleted by committed txn
- [x] 🔴 Unit test `TestVisibility_DeletedByActive`: txn DOES see row with Xmax set by still-active txn
- [x] 🔴 Unit test `TestSnapshot_IsolationLevelDiff`: RC sees committed updates mid-txn; SI does not
- [x] 🔴 `go test ./internal/mvcc/... -race` zero races

---

## Phase 4 — Buffer Pool (14 tasks)

- [x] 🔴 `internal/buffer/frame.go`: `Frame{PageID uint32, Data [4096]byte, PinCount int32, Dirty bool, RefBit bool, recLSN uint64}`
- [x] 🔴 `internal/buffer/pool.go`: `BufferPool{mu, frames []*Frame, pageTable map[uint32]*Frame, clockHand int, dirtyPageTable map[uint32]uint64}`
- [x] 🔴 `BufferPool.FetchPage(pageID uint32) *Frame`: cache hit → pin + return; cache miss → evict + load
- [x] 🔴 `BufferPool.UnpinPage(pageID uint32, dirty bool)`: decrement pin; if dirty, set frame.Dirty + update dirtyPageTable with recLSN
- [x] 🔴 `internal/buffer/clock.go`: `BufferPool.evict() *Frame` — Clock algorithm: sweep circular list; skip pinned; clear RefBit on second pass; evict on zero RefBit
- [x] 🔴 Dirty eviction: call `wal.FlushUpTo(frame.recLSN)` BEFORE writing page to disk (ARIES WAL rule)
- [x] 🔴 `BufferPool.FlushDirtyPages()`: write all dirty pages (for checkpoint)
- [x] 🔴 `BufferPool.HitRate() float64`: hits/(hits+misses)
- [x] 🔴 `BufferPool.DirtyPageTable() map[uint32]uint64`: snapshot for ARIES checkpoint
- [x] 🔴 Emit `EvtBufferHit` / `EvtBufferMiss` / `EvtPageEvict` / `EvtPageDirty`
- [x] 🔴 Unit test `TestBuffer_FetchPin`: fetch page; verify pin count = 1; fetch again = 2; unpin twice = 0
- [x] 🔴 Unit test `TestBuffer_ClockEviction`: fill pool; verify eviction order follows Clock (not FIFO, not random)
- [x] 🔴 Unit test `TestBuffer_DirtyNotLostBeforeWAL`: mock WAL; verify FlushUpTo called before dirty page write
- [x] 🔴 `go test ./internal/buffer/... -race` zero races

---

## Phase 5 — WAL (Write-Ahead Log) (16 tasks)

- [x] 🔴 `internal/wal/record.go`: `LogRecordType` enum (12 types from SPEC §8.1)
- [x] 🔴 `LogRecord{LSN, PrevLSN, TxnID, PageID uint64/32, Type, Payload []byte}` + CRC32
- [x] 🔴 Binary encoding: `[LSN:8][PrevLSN:8][TxnID:8][PageID:4][Type:1][PayloadLen:4][Payload][CRC32:4]`
- [x] 🔴 `internal/wal/wal.go`: `WALManager{mu, file, buffer []byte, currentLSN uint64, flushedLSN atomic.Uint64}`
- [x] 🔴 `WALManager.AppendRecord(r LogRecord) uint64`: assign LSN; encode; append to in-memory buffer; return LSN
- [x] 🔴 `WALManager.FlushUpTo(targetLSN uint64)`: flush buffer to file; fsync if syncOnCommit; update flushedLSN
- [x] 🔴 `WALManager.Commit(txnID, prevLSN uint64) uint64`: append LogCommit; flush; return commitLSN
- [x] 🔴 `WALManager.Abort(txnID, prevLSN uint64) uint64`: append LogAbort; flush
- [x] 🔴 `WALManager.ScanFrom(startLSN uint64, fn func(LogRecord))`: iterate log file from startLSN
- [x] 🔴 `WALManager.FetchRecord(lsn uint64) LogRecord`: seek to lsn; decode; verify CRC32
- [x] 🔴 `WALManager.LogInsert(txnID, pageID uint64, prevLSN uint64, tuple *MVCCTuple) uint64`
- [x] 🔴 `WALManager.LogDelete(txnID, pageID uint64, prevLSN uint64, slotIdx int, oldXmax uint64) uint64`
- [x] 🔴 Emit `EvtWALAppend` on every append; `EvtWALFlush` on every flush
- [x] 🔴 Unit test `TestWAL_AppendAndScan`: append 100 records; scan from 0; verify all recovered in order
- [x] 🔴 Unit test `TestWAL_CRCDetectsCorruption`: corrupt 1 byte in payload; scan → record marked corrupt
- [x] 🔴 Unit test `TestWAL_CommitFlushes`: Commit() must call fsync before returning
- [x] 🔴 `go test ./internal/wal/... -race` zero races

---

## Phase 6 — B+Tree: Internal + Leaf Nodes (22 tasks)

**Internal node**
- [x] 🔴 `internal/btree/internal.go`: `InternalRecord{Key []byte, ChildID uint32}`
- [x] 🔴 `internal/btree/internal.go` `EncodeInternalRecord` / `DecodeInternalRecord`
- [x] 🔴 `InternalPage.FindChild(key []byte) uint32`: binary search separator keys; return child pageID
- [x] 🔴 `InternalPage.InsertKey(key []byte, rightChild uint32, afterSlot int)`: insert separator + child pointer
- [x] 🔴 `InternalPage.RemoveKey(slotIdx int)`: remove separator key (during merge/borrow)
- [x] 🔴 `InternalPage.RightmostChild() uint32`: stored in page header Flags area (child for keys > last separator)
- [x] 🔴 `InternalPage.SetRightmostChild(childID uint32)`

**Leaf node**
- [x] 🔴 `internal/btree/leaf.go`: leaf page encodes `MVCCTuple` in each slot
- [x] 🔴 `LeafPage.InsertTuple(tuple *MVCCTuple) error`: encode tuple; `Page.InsertRecord`
- [x] 🔴 `LeafPage.GetTuple(slotIdx int) *MVCCTuple`: `Page.GetRecord` → `DecodeTuple`
- [x] 🔴 `LeafPage.FindAllVersions(key []byte) []*MVCCTuple`: binary search + adjacent scan; newest Xmin first
- [x] 🔴 `LeafPage.FindInsertionSlot(key []byte) int`: binary search for where to insert new tuple (maintains sorted order)
- [x] 🔴 `LeafPage.SetXmax(slotIdx int, xmax uint64)`: update Xmax in-place on slot (for DELETE/UPDATE)
- [x] 🔴 Sibling links: `LeafPage.SetPrevSib`, `SetNextSib`, `PrevSib()`, `NextSib()`

**B+Tree operations**
- [x] 🔴 `internal/btree/tree.go`: `BPlusTree{rootPageID uint32, bufPool *BufferPool, wal *WALManager, dm *DiskManager}`
- [x] 🔴 `BPlusTree.findLeaf(key []byte) *Page`: top-down traversal through internal nodes to leaf; unpin internal nodes as we go
- [x] 🔴 `BPlusTree.Get(txn *Transaction, key []byte) ([]byte, error)`: SPEC §5.5 exact; all version visibility
- [x] 🔴 `BPlusTree.Insert(txn *Transaction, key, value []byte) error`: WAL log → leaf insert → split if needed
- [x] 🔴 `BPlusTree.Update(txn *Transaction, key, newValue []byte) error`: find visible → setXmax → insert new version
- [x] 🔴 `BPlusTree.Delete(txn *Transaction, key []byte) error`: find visible → setXmax
- [x] 🔴 Unit test `TestBTree_InsertAndGet`: insert 1000 keys; get each; verify values
- [x] 🔴 Unit test `TestBTree_LeafVersionsOrdered`: multiple versions of same key; FindAllVersions returns newest first
- [x] 🔴 `go test ./internal/btree/... -race` zero races

---

## Phase 7 — B+Tree: Split + Merge (16 tasks)

**Splits**
- [x] 🔴 `internal/btree/split.go`: `BPlusTree.splitLeaf(txn, leaf *Page, overflow key/tuple) error`
- [x] 🔴 Allocate right page; move top half slots to right; update sibling links (`PrevSib`/`NextSib`)
- [x] 🔴 WAL `LogSplit`: log both pages BEFORE modifying them; log new page allocation
- [x] 🔴 Promote separator key (first key of right page) to parent via `insertIntoParent`
- [x] 🔴 `BPlusTree.splitInternal(txn, internal *Page, midKey []byte, newChild uint32) error`
- [x] 🔴 If parent is root and root overflows: create new root page; tree height increases by 1
- [x] 🔴 Breadcrumb stack: track parent chain during top-down traversal (for propagating splits upward)
- [x] 🔴 Emit `EvtPageSplit`, `EvtTreeSplit` on every split

**Merge/Borrow**
- [x] 🔴 `internal/btree/merge.go`: `BPlusTree.handleUnderflow(txn, leaf *Page, parentChain []*Page)`
- [x] 🔴 Try borrow-from-right-sibling first; then borrow-from-left; then merge
- [x] 🔴 Borrow: move one tuple from sibling to leaf; update separator key in parent; WAL both
- [x] 🔴 Merge: copy all tuples from right to left; free right page; remove separator from parent
- [x] 🔴 Merge may cascade: parent underflows → recursive merge up to root; root may shrink (height decreases)
- [x] 🔴 Emit `EvtPageMerge`, `EvtTreeMerge` on every merge
- [x] 🔴 Unit test `TestBTree_SplitOnInsert`: insert keys until leaf splits; verify tree structure valid after split
- [x] 🔴 Unit test `TestBTree_MergeOnDelete`: fill a node; delete until underflow; verify merge; tree valid
- [x] 🔴 Integration test `TestBTree_1MInserts`: 1M sequential inserts; tree height <= 4; all readable back

---

## Phase 8 — Range Scan (8 tasks)

- [x] 🔴 `internal/btree/cursor.go`: `BTreeCursor{engine, txn, current *Page, slotIdx int, end []byte}`
- [x] 🔴 `BPlusTree.Scan(txn, start, end []byte) *BTreeCursor`
- [x] 🔴 `BTreeCursor.Next() (*MVCCTuple, error)`: advance slotIdx; when exhausted, follow NextSib; apply visibility
- [x] 🔴 Skip non-visible tuples and dead versions (Xmax committed and <= oldest active txn)
- [x] 🔴 `BTreeCursor.SeekTo(key []byte)`: binary search current leaf; or traverse to appropriate leaf
- [x] 🔴 Emit `EvtTreeSearch` with keys scanned and visibility decisions
- [x] 🔴 Unit test `TestScan_BasicRange`: insert "a".."z"; scan "f".."t"; verify exactly "f","g",...,"t" returned
- [x] 🔴 Unit test `TestScan_MVCCVisibility`: insert key; update key (new version); scan → only latest version seen by snapshot
- [x] 🔴 `go test ./internal/btree/... -race` zero races

---

## Phase 9 — ARIES Recovery (16 tasks)

- [x] 🔴 `internal/wal/checkpoint.go`: `WALManager.WriteCheckpoint()`: snapshot DPT + ATT; write `LogCheckpoint` record
- [x] 🔴 `internal/recovery/aries.go`: `RecoveryManager{wal, bufPool, dm, tm *TransactionManager}`
- [x] 🔴 `RecoveryManager.Analyze(checkpointLSN uint64) (DirtyPageTable, ActiveTxnTable)`
- [x] 🔴 Analysis: scan from checkpoint; populate DPT (page → recLSN) and ATT (txn → {status, lastLSN})
- [x] 🔴 `RecoveryManager.Redo(dpt DirtyPageTable)`: scan from min(recLSN in DPT); skip if pageLSN >= record.LSN
- [x] 🔴 Redo applies: `LogInsert` → insert tuple; `LogDelete` → set Xmax; `LogSplit` → reconstruct split
- [x] 🔴 Redo also applies CLR records (compensation log records)
- [x] 🔴 `RecoveryManager.Undo(att ActiveTxnTable)`: max-heap of LSNs; process in reverse; write CLRs
- [x] 🔴 Undo applies: `LogInsert` → delete the inserted tuple (set Xmax); `LogDelete` → restore Xmax=0
- [x] 🔴 Write CLR for each undone operation: `CLR.UndoNextLSN = record.PrevLSN`
- [x] 🔴 `StorageEngine.Open()` recovery sequence: read meta → find WAL → find checkpoint → ARIES 3 phases
- [x] 🔴 Emit `EvtRecoveryStart`, `EvtRecoveryAnalysis`, `EvtRecoveryRedo`, `EvtRecoveryUndo`, `EvtRecoveryDone`
- [x] 🔴 Integration test `TestARIES_CommitSurvivesCrash`: 500 inserts, commit; crash; recover; all 500 present
- [x] 🔴 Integration test `TestARIES_UncommittedRolledBack`: 500 inserts, NO commit; crash; recover; 0 present
- [x] 🔴 Integration test `TestARIES_PartialCrash`: 300 committed + 200 uncommitted; crash mid-write; recover; exactly 300 present
- [x] 🔴 `go test ./internal/recovery/... -race -timeout 60s` zero races

---

## Phase 10 — VACUUM (8 tasks)

- [x] 🔴 `internal/mvcc/vacuum.go`: `VacuumWorker{engine, cfg VacuumConfig}`
- [x] 🔴 `VacuumWorker.Run(ctx)`: ticker at `vacuumInterval`; call `vacuum(oldestActive)`
- [x] 🔴 Scan all leaf pages (walk tree left-to-right via sibling links)
- [x] 🔴 Per page: identify dead tuples (`xmax committed AND xmax <= oldestActive`); mark slots as tombstone
- [x] 🔴 If `page.FragmentationRatio() > vacuumDeadThreshold`: call `page.Compact()`; WAL log the compaction
- [x] 🔴 Emit `EvtVacuumStart`, `EvtVacuumTuple` (per dead tuple), `EvtVacuumPage` (per compacted page), `EvtVacuumDone`
- [x] 🔴 Unit test `TestVacuum_RemovesDeadTuples`: insert 1000; delete 500 (committed); run vacuum; page has 500 tuples
- [x] 🔴 Unit test `TestVacuum_PreservesActiveTuples`: delete but txn still active; vacuum must NOT remove

---

## Phase 11 — Full Engine API + Gateway (14 tasks)

- [x] 🔴 `internal/engine/engine.go`: `StorageEngine{disk, bufPool, wal, tree, tm, vacuum}`
- [x] 🔴 `StorageEngine.Open(cfg Config) (*StorageEngine, error)`: init all components; run ARIES recovery; start background workers
- [x] 🔴 `StorageEngine.Close() error`: flush WAL; checkpoint; close file
- [x] 🔴 `StorageEngine.Begin(isoLevel) *Transaction`: delegate to TransactionManager
- [x] 🔴 `StorageEngine.Commit(txn) error`: WAL commit; release resources
- [x] 🔴 `StorageEngine.Abort(txn) error`: undo changes via WAL; release resources
- [x] 🔴 `gateway/rest.go`: all REST handlers from SPEC §14
- [x] 🔴 `POST /txn/begin`, `POST /txn/:id/commit`, `POST /txn/:id/abort`
- [x] 🔴 `POST /txn/:id/put`, `GET /txn/:id/get`, `DELETE /txn/:id/delete`, `GET /txn/:id/scan`
- [x] 🔴 `GET /tree/structure`: DFS tree structure JSON (page IDs, types, key ranges)
- [x] 🔴 `GET /tree/page/:id`: decoded page contents with MVCC tuples
- [x] 🔴 `GET /mvcc/versions?key=...`: all versions of a key with xmin/xmax/status
- [x] 🔴 `GET /mvcc/visibility?key=...&txnID=...`: is this key visible to this txn + why?
- [x] 🔴 `POST /engine/crash` + `POST /engine/recover`: crash simulation + ARIES recovery

---

## Phase 12 — Simulation Scenarios (10 tasks)

- [x] 🟡 `internal/simulation/scenarios.go`: 9 scenarios from SPEC §12
- [x] 🟡 `ScenarioInsertAndSearch`: 1000 inserts; verify tree structure; search 100 keys
- [x] 🟡 `ScenarioSplitVisual`: fill leaf to trigger split; emit step-by-step events
- [x] 🟡 `ScenarioMVCCBasic`: txn T1 inserts; txn T2 reads before commit (hidden) and after commit (visible)
- [x] 🟡 `ScenarioReadCommitted`: T1 reads "balance"; T2 updates and commits; T1 reads again → different value (RC)
- [x] 🟡 `ScenarioSnapshotIso`: same as RC but T1 is SNAPSHOT ISOLATION → second read sees original value
- [x] 🟡 `ScenarioWriteSkew`: two doctors scenario (SPEC §5.4 exactly); demonstrate SI allows the anomaly
- [x] 🟡 `ScenarioCrashRecovery`: 300 commits + 200 mid-txn crash; recover; verify exactly 300
- [x] 🟡 `ScenarioConcurrentTxns`: 10 concurrent transactions; verify no dirty reads; MVCC stats shown
- [x] 🟡 `ScenarioVacuum`: 10k updates on 1k keys; run vacuum; show dead tuple count drop to near 0

---

## Phase 13 — Elysia BFF + TypeScript Types (6 tasks)

- [x] 🔴 `frontend/server/bff.ts`: complete REST proxy + WS fan-out
- [x] 🔴 Eden Treaty export: `export type App = typeof app`
- [x] 🔴 `frontend/src/api/client.ts`: typed API client for all endpoints
- [x] 🔴 `frontend/src/api/types.ts`: TypeScript interfaces for all 32 event types + PageHeader + MVCCTuple + Transaction + Snapshot
- [x] 🔴 `frontend/src/ws/client.ts`: WS connection + typed event dispatcher + exponential backoff
- [x] 🟡 `GET /health` route

---

## Phase 14 — Frontend Panel 1: B+Tree Visualizer (14 tasks)

- [x] 🔴 `frontend/src/store/engine.ts`: reactive state; TreeNode map; update on tree events
- [x] 🔴 `components/tree/tree.ts`: D3 tree layout (d3.hierarchy); nodes = pages; edges = parent→child
- [x] 🔴 Internal nodes: gray rectangle; show separator keys as labels
- [x] 🔴 Leaf nodes: blue rectangle; show key count; linked horizontally at bottom level
- [x] 🔴 Sibling links: horizontal arrows between leaf nodes at bottom level
- [x] 🔴 On `EvtTreeInsert`: highlight target leaf; if split, animate split (one node → two + separator rises to parent)
- [x] 🔴 On `EvtTreeSplit`: animate: node flashes red, splits, separator key floats up to parent
- [x] 🔴 Click node: show PageID, type, numSlots, fillFactor%, pageLSN; expand to show slot contents
- [x] 🟡 Root-to-leaf traversal highlight: on Get, animate path from root to leaf (nodes flash yellow in sequence)
- [x] 🟡 Tree height counter + fan-out per level stat
- [x] 🟡 Color by fill factor: empty=white, 25%=light blue, 75%=blue, >90%=orange (split imminent), >100%=red
- [x] 🟢 Zoom and pan (d3.zoom)
- [x] 🔴 Live: all state driven by WebSocket events

---

## Phase 15 — Frontend Panels 2–7 (28 tasks)

**Panel 2 — Page Inspector**
- [x] 🔴 Hex-dump of selected page (slot table with header fields, key/value/xmin/xmax)
- [x] 🔴 Decoded header fields as labeled boxes above hex view
- [x] 🔴 Click slot → highlight corresponding data area bytes; show decoded MVCCTuple
- [x] 🟡 Fill factor bar (green→yellow→red)

**Panel 3 — MVCC Version Chain**
- [x] 🔴 For selected key: horizontal timeline showing all versions as bars `[xmin → xmax]`
- [x] 🔴 Color: live=green, dead=red, uncommitted=yellow, aborted=gray
- [x] 🔴 "What does TxnID=X see?" slider → highlight the visible version
- [x] 🟡 Real-time update as transactions commit (bars snap from yellow to green/red)

**Panel 4 — Isolation Level Demo**
- [x] 🔴 Split screen: LEFT = READ COMMITTED, RIGHT = SNAPSHOT ISOLATION
- [x] 🔴 Animate two concurrent transactions on each side; show what each sees
- [x] 🔴 Non-repeatable read demo: highlight the two different values read under RC in red
- [x] 🔴 Write skew demo: doctors scenario step by step; final state shows invariant violation
- [x] 🟡 "WRITE SKEW DETECTED" alert banner with explanation

**Panel 5 — Buffer Pool**
- [x] 🔴 N-frame grid; each frame colored by status (empty/clean/dirty/pinned+dirty)
- [x] 🔴 Clock hand animation (rotating pointer)
- [x] 🔴 Hit rate gauge (pie chart); dirty page count bar
- [x] 🟡 Frame tooltip: pageID, pin count, recLSN, last accessed time

**Panel 6 — WAL + Recovery**
- [x] 🔴 Scrolling WAL log: each record with LSN, TxnID, Type, PageID, payload summary
- [x] 🔴 Color by type: BEGIN=blue, INSERT=green, DELETE=red, COMMIT=teal, ABORT=orange, CLR=purple
- [x] 🔴 "Simulate Crash" button; "Recover" button triggering ARIES 3 phases
- [x] 🔴 Recovery display: DPT and ATT tables built live during Analysis phase
- [x] 🟡 Redo animation: pages being re-applied one by one
- [x] 🟡 Undo animation: transactions rolling back; CLR records appearing in WAL

**Panel 7 — Scenarios + Metrics**
- [x] 🔴 Scenario dropdown + Run + Speed + Step Mode
- [x] 🔴 Metrics: tree height, page count, buffer hit rate, dead tuple count, WAL write rate, concurrent txns
- [x] 🟡 Split/merge count live graph

---

## Phase 16 — Integration Tests + Polish (14 tasks)

**Correctness**
- [x] 🔴 `TestMVCC_ReadCommitted_NonRepeatableRead`: RC shows updated value after other txn commits
- [x] 🔴 `TestMVCC_SnapshotIso_RepeatableRead`: SI shows same value regardless of other commits
- [x] 🔴 `TestMVCC_NoDirtyReads`: uncommitted writes never visible to other transactions
- [x] 🔴 `TestARIES_FullRecovery`: 1000 commits + in-flight txns; crash; recover; exact state matches pre-crash committed state
- [x] 🔴 `TestWriteSkew_Demonstrable`: write skew scenario completes without error under SI (it's allowed)
- [x] 🟡 `TestConcurrent_10Txns`: 10 goroutines each doing 100 ops; no panics; MVCC invariants hold
- [x] 🟡 `TestVacuum_FreeSpaceReclaimed`: run vacuum; verify page free space increases

**Race detection**
- [x] 🔴 `go test ./... -race -count=3` zero races

**Code quality**
- [x] 🟡 TypeScript strict mode; `bun run tsc --noEmit` zero errors
- [x] 🟡 `golangci-lint run` zero errors
- [x] 🟡 All exported Go types/functions have godoc comments

**Documentation**
- [x] 🟢 `README.md`: B+tree vs LSM comparison table; setup; panel descriptions
- [x] 🟢 Page format ASCII diagram
- [x] 🟢 MVCC visibility rules flowchart (ASCII)

---

## Summary Table

| Phase | Tasks |
|-------|-------|
| 0. Bootstrap | 10 |
| 1. Page Layer (slotted page) | 18 |
| 2. Disk Manager + Free List | 10 |
| 3. MVCC Tuples + Visibility | 20 |
| 4. Buffer Pool | 14 |
| 5. WAL | 16 |
| 6. B+Tree: Internal + Leaf | 22 |
| 7. B+Tree: Split + Merge | 16 |
| 8. Range Scan | 8 |
| 9. ARIES Recovery | 16 |
| 10. VACUUM | 8 |
| 11. Engine API + Gateway | 14 |
| 12. Simulation Scenarios | 10 |
| 13. Elysia BFF + TypeScript | 6 |
| 14. Frontend Panel 1 (Tree) | 14 |
| 15. Frontend Panels 2–7 | 28 |
| 16. Integration Tests + Polish | 14 |
| **TOTAL** | **234** |
