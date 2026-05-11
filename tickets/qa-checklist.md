# QA Testing Checklist

> Run this checklist before every release. Items marked `[auto]` are covered by CI.

---

## 1. Unit Tests `[auto]`

```bash
go test ./... -count=1 -timeout=300s
```

| Package | Tests | Expected |
|---------|-------|----------|
| `internal/page` | TestPage_InsertAndGet, BinarySearch, Compact, Checksum, Tombstones, FreeSpace | All pass |
| `internal/buffer` | Pool eviction, dirty tracking, checksum on load, invalidate, hit rate | All pass |
| `internal/mvcc` | All 9 visibility cases, snapshot isolation, read committed, vacuum | All pass |
| `internal/wal` | Record encoding, CRC, checkpoint, ScanFrom, group commit | All pass |
| `internal/recovery` | ARIES 3-phase, CLR chains, checkpoint restore | All pass |
| `internal/btree` | Get/Insert/Update/Delete, Split, Merge, WriteConflict, CascadeMerge | All pass |
| `gateway` | Auth (401/403), route validation, JSON responses | All pass |

---

## 2. Race Detector `[auto]`

```bash
go test ./... -race -count=3 -timeout=600s
```

**Expected:** Zero race conditions detected.

Critical areas to watch:
- `events.EventBus.Publish()` / `Subscribe()` concurrent access
- `buffer.BufferPool` pin count atomics
- `mvcc.TransactionManager` statusTable map
- `engine.StorageEngine` treeMu RWMutex

---

## 3. Integration Tests `[auto]`

```bash
go test ./test/integration/... -count=1 -v
```

| Test | Description | Expected |
|------|-------------|----------|
| `TestMVCC_Isolation_SnapshotIsolation` | Concurrent txns cannot see each other's uncommitted writes | Pass |
| `TestMVCC_Isolation_ReadCommitted` | RC sees newly committed writes mid-transaction | Pass |
| `TestConcurrentTxns_NoDirtyReads` | 10 concurrent txns, zero dirty reads | Pass |
| `TestCrashRecovery_CommittedData` | Crash after 500 commits; all data present after recovery | Pass |
| `TestCrashRecovery_InFlightAborted` | Crash with in-flight txns; in-flight changes absent after recovery | Pass |
| `TestRangeScan_Visibility` | Scan filters by MVCC visibility; dead tuples invisible | Pass |
| `TestRangeScan_SiblingLinks` | Range scan crosses multiple leaf pages via sibling links | Pass |

---

## 4. Scenario Tests

```bash
# Via API (requires running server)
for scenario in insert-and-search split-visual mvcc-basic read-committed \
                snapshot-iso write-skew crash-recovery concurrent-txns vacuum; do
  curl -s -X POST -H "Authorization: Bearer dev-key" \
       http://localhost:8080/api/v1/scenarios/$scenario/run | jq '.steps[] | select(.ok==false)'
done
```

**Expected:** All steps return `"ok": true` (except write-skew's intentional violation step).

---

## 5. Correctness Checks

### 5.1 MVCC Visibility

- [ ] Uncommitted insert invisible to concurrent transactions
- [ ] Committed insert visible to transactions starting after commit
- [ ] Committed insert invisible to snapshot-isolation transactions that started before commit
- [ ] Deleted row invisible to transactions starting after commit
- [ ] Deleted row visible to snapshot-isolation transactions that started before delete committed
- [ ] Aborted insert invisible to all transactions
- [ ] Aborted delete: row remains visible (rollback of deletion)
- [ ] Write-write conflict: second writer gets `ErrWriteConflict` (HTTP 409)

### 5.2 Crash Recovery

- [ ] Commit 1000 keys → CrashClose → Recover → verify all 1000 keys readable
- [ ] Begin transaction, insert 100 keys, CrashClose (no commit) → Recover → verify all 100 keys absent
- [ ] Mixed: 500 committed, 100 in-flight → crash → recover → 500 present, 100 absent
- [ ] Recovery is idempotent: run recovery twice → same result

### 5.3 Tree Structural Integrity

- [ ] After 10K inserts: all keys findable via Get
- [ ] After 10K inserts + 5K deletes: deleted keys return ErrKeyNotFound
- [ ] After 10K inserts + 5K deletes + vacuum: free space increased
- [ ] Range scan [A, Z] returns keys in sorted order
- [ ] Range scan after splits: crosses sibling links correctly
- [ ] Range scan after merges: no keys skipped or duplicated

### 5.4 Checksum Integrity

- [ ] Fresh page: VerifyChecksum returns true
- [ ] Modify data byte: VerifyChecksum returns false
- [ ] Modify header byte: VerifyChecksum returns false
- [ ] FetchPage on corrupted page: returns error, not silently corrupted

### 5.5 Concurrent Access

- [ ] 10 concurrent readers + 1 writer: no deadlock, no panic
- [ ] 10 concurrent writers on disjoint keys: all commit successfully
- [ ] 10 concurrent writers on same key: at most 1 commits; others get ErrWriteConflict
- [ ] Cursor iteration during concurrent inserts: no panic (may return ErrCursorInvalidated)

---

## 6. Performance Regression Tests `[auto]`

```bash
go test ./... -bench=. -benchmem -benchtime=5s 2>&1 | tee bench-results.txt
# Compare with baseline: benchstat baseline.txt bench-results.txt
```

**Regression thresholds:**
| Benchmark | Baseline | Fail if |
|-----------|----------|---------|
| `BenchmarkInsert_Sequential/1K` | ~5 µs/op | > 6.5 µs/op (+30%) |
| `BenchmarkGet_HotSet` | ~1 µs/op | > 1.5 µs/op (+50%) |
| `BenchmarkPage_BinarySearch/50slots` | < 500 ns/op | > 750 ns/op |
| `BenchmarkBuffer_FetchPage_Hit` | < 200 ns/op | > 300 ns/op |

---

## 7. API Contract Tests

For each REST endpoint, verify:
- [ ] Correct HTTP method (GET/POST/DELETE) enforced; wrong method → 405
- [ ] Missing required fields → 400 with descriptive error
- [ ] Invalid transaction ID → 404
- [ ] Transaction ID for committed/aborted txn → 404 or 400 (not 500)
- [ ] Correct Content-Type: `application/json` on all responses

**Endpoint inventory:**
```
POST /api/v1/txn/begin
POST /api/v1/txn/:id/commit
POST /api/v1/txn/:id/abort
POST /api/v1/txn/:id/put
GET  /api/v1/txn/:id/get
DELETE /api/v1/txn/:id/delete
GET  /api/v1/txn/:id/scan
GET  /api/v1/tree/structure
GET  /api/v1/tree/page/:id
GET  /api/v1/mvcc/versions
GET  /api/v1/mvcc/visibility
GET  /api/v1/buffer/stats
GET  /api/v1/buffer/frames
GET  /api/v1/wal/tail
POST /api/v1/wal/checkpoint
GET  /api/v1/scenarios
POST /api/v1/scenarios/:name/run
POST /api/v1/engine/crash
POST /api/v1/engine/recover
GET  /api/v1/engine/stats
GET  /health/live
GET  /health/ready
GET  /metrics
```

---

## 8. Security Validation

- [ ] All API routes require `Authorization: Bearer <key>` (except `/health/*`, `/metrics`)
- [ ] Missing key → 401 (not 403, not 500)
- [ ] Wrong key → 401
- [ ] Auth failure does not log the supplied key
- [ ] Oversized key (> 512 bytes) → 400 (not 500)
- [ ] Oversized value (> 65535 bytes) → 400 (not silent data truncation)
- [ ] Empty key → 400 at both gateway and engine layers
- [ ] Key with null bytes → handled cleanly (no panic)
- [ ] Very long transaction scan (cursor) → does not OOM server
- [ ] 1000 concurrent open transactions → server stable, oldest pruned or 429 returned

---

## 9. Edge Case Tests

### Page Layer
- [ ] Insert until `ErrPageFull`: correct error, page not corrupted
- [ ] Delete all records on page: FreeSpace returns PageSize - HeaderSize
- [ ] Compact empty page: no-op, header unchanged
- [ ] BinarySearch on page with all tombstones: returns negative (not found)

### WAL
- [ ] WAL buffer full during write: auto-flush, then append succeeds
- [ ] WAL file not writable: open returns error (not panic)
- [ ] ScanFrom(lsn) with lsn beyond file end: returns empty (not hang)

### Buffer Pool
- [ ] All frames pinned → FetchPage returns error (pool exhausted)
- [ ] UnpinPage on already-unpinned frame → no panic, no double-decrement

### Recovery
- [ ] Recovery on empty WAL file: succeeds, no-op
- [ ] Recovery on truncated WAL (mid-record): stops at last complete record
- [ ] Recovery on WAL with all COMMITs: all redo applied correctly

---

## 10. Fuzz Testing (Run periodically) `[auto - nightly CI]`

```bash
go test ./internal/page/... -fuzz=FuzzInsertGet -fuzztime=300s
go test ./internal/page/... -fuzz=FuzzBinarySearch -fuzztime=300s
go test ./internal/wal/...  -fuzz=FuzzWALRecord -fuzztime=300s
go test ./internal/page/... -fuzz=FuzzChecksumRoundtrip -fuzztime=300s
```

**Expected:** No panics, no unexpected failures (correctness assertions may trigger; investigate).

---

## 11. Load Testing

```bash
# Using wrk or hey for HTTP load test
hey -n 10000 -c 100 \
    -H "Authorization: Bearer dev-key" \
    -m POST -d '{"iso_level":"snapshot"}' \
    http://localhost:8080/api/v1/txn/begin

# Expected: < 1% error rate, p99 latency < 50ms
```

---

## 12. Frontend QA Checklist

- [ ] Tree visualizer renders correctly after 1000 insert scenario
- [ ] Tree nodes show correct fill percentage color (green → orange → red)
- [ ] Clicking a node opens Page Inspector panel with correct page contents
- [ ] MVCC Version Timeline shows all versions after 10 updates to same key
- [ ] WAL Log panel shows last 50 records in correct order
- [ ] Buffer Pool panel shows frame grid with correct dirty/pinned states
- [ ] Isolation Level Demo: RC panel shows different values for second read; SI panel shows same
- [ ] Crash + Recover buttons in WAL panel function correctly
- [ ] WebSocket auto-reconnects after server restart (within 5 seconds)
- [ ] All panels update in real-time after operations without page refresh
- [ ] No JavaScript errors in browser console during any scenario
- [ ] UI responsive at 1280×800 minimum resolution
