# Architecture Improvements

> Analysis date: 2026-05-09
> Purpose: Identify structural changes needed to evolve from research-grade to production-hardened system.

---

## 1. Authentication Layer (Missing)

### Current State
There is no authentication. Every caller that can reach port 8080 has full read/write/admin access. The gateway (`gateway/server.go`) registers all routes with no middleware chain.

### Proposed Architecture
```
HTTP Request
    │
    ▼
[TLS Terminator]           (Phase 5 — nginx or built-in)
    │
    ▼
[Auth Middleware]          (Phase 0 — API key or JWT)
    │
    ├─ No credentials → 401 Unauthorized
    ├─ Invalid key     → 401 Unauthorized
    ├─ Insufficient permission → 403 Forbidden
    │
    ▼
[Rate Limiter]             (Phase 0 — token bucket per IP)
    │
    ▼
[Request Timeout]          (Phase 0 — context deadline)
    │
    ▼
[Route Handler]
```

### Implementation Notes
- Auth middleware attaches a `Principal` struct to the request context: `{APIKey, Role, Permissions}`
- Route handlers extract the principal via `authFromContext(ctx)`, never reading headers directly
- This makes handlers testable without the HTTP layer

### Impact
- **Backend:** Add middleware chain to `gateway/server.go`; all handlers become context-aware
- **Testing:** Test helpers provide a fake principal; no need to spin up auth server in unit tests
- **Database:** API key store can be a flat YAML file initially; upgrade to engine-stored keys in Phase 5
- **Security:** Removes the #1 threat vector (unauthenticated access)

---

## 2. Observability Architecture (Missing)

### Current State
`engine.go` has `Stats()` returning 4 fields. Events are published on the internal bus but not exported to monitoring systems.

### Proposed Architecture
```
Internal Event Bus (existing)
    │
    ├─ EventBus.Subscribe() → MetricsCollector goroutine
    │                              │
    │                              └─ Updates prometheus.Counter / Gauge / Histogram
    │
    └─ EventBus.Subscribe() → StructuredLogger goroutine
                                   │
                                   └─ slog.Logger → stdout (JSON)

Prometheus Scraper
    │ (HTTP GET /metrics every 15s)
    ▼
[/metrics handler] ← prometheus.Registry.Gather()
```

### Key Design Decision
The metrics collector subscribes to the existing event bus rather than adding metric updates throughout the codebase. This keeps the hot path clean (no lock contention from metrics writes) and makes metrics optional.

### Histogram Buckets
```go
// WAL flush duration
walFlushDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
    Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
})

// Disk read/write duration
diskIODuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
    Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1},
}, []string{"op"}) // op = "read" | "write"
```

### Impact
- **No changes** to hot paths (page operations, WAL writes)
- **New file:** `internal/metrics/collector.go`
- **New dependency:** `github.com/prometheus/client_golang` (~2 MB)
- **Frontend:** Add Grafana panel for real-time metrics (optional)

---

## 3. WAL Lifecycle (Incomplete)

### Current State
`wal.go` appends forever. `WALManager.ScanFrom(0, ...)` in `WALTail()` reads from byte 0 every time — this becomes O(WAL file size), not O(tail size). After a crash-recovery cycle, old WAL records are kept but never needed again.

### Proposed Architecture: Segmented WAL

```
data/
├── btree.db
├── wal_000001.log       ← archived (before last checkpoint)
├── wal_000002.log       ← archived
└── wal_000003.log       ← current (active)
```

**Segment lifecycle:**
1. When segment exceeds `max_wal_segment_size` (256 MB default): close, open new segment
2. After successful checkpoint: identify segments entirely before `min(DPT.recLSN)`
3. Call archive hook with segment path (default: delete; can be: `aws s3 cp ...`)
4. `ScanFrom(lsn)` only scans segments that contain `lsn`

**Segment naming:** zero-padded sequence number, maps to starting LSN in segment catalog.

**Catalog file:** `data/wal_catalog.json` → `[{seq: N, startLSN: M, endLSN: K}]`

### Impact
- **WAL size:** Bounded to `max_wal_segment_size * 2` in steady state
- **Recovery time:** Unchanged (only needed segments are scanned)
- **Backup:** Archive hook enables incremental WAL backup to S3/GCS
- **`WALTail(n)`:** Only reads last segment — now O(tail size)

---

## 4. Isolation Level Architecture (Incomplete)

### Current State
Two isolation levels: `ReadCommitted` and `SnapshotIsolation`. Snapshot isolation allows write skew (demonstrated by the doctors scenario).

### Proposed: Serializable Snapshot Isolation (SSI)

SSI tracks two dependency types:
- **rw-antidependency:** T1 reads a version, T2 later writes a new version of that key while T1 is still active
- **Dangerous structure:** cycle of rw-antidependencies between two transactions = unsafe, one must abort

```
TransactionManager
    ├── statusTable         (existing)
    ├── activeTable         (existing)
    └── sireadLockManager   (new)
            ├── readSets    map[TxnID][]SIREADLock
            └── checkCycle(txnID) → bool
```

**SIREADLock:**
```go
type SIREADLock struct {
    TxnID   TxnID
    PageID  uint32
    KeyHash uint64  // fingerprint of the key read
}
```

**On read (SI level):** record `{txnID, pageID, keyHash}`
**On write:** for each committed SIREADLock on the modified key, record rw-edge T_reader → T_writer
**Cycle check:** DFS over rw-edges; if cycle detected → abort one transaction

**GC:** All SIREAD locks for a committed/aborted transaction are released immediately.

### Why Not Two-Phase Locking?
2PL would require reader-writer locks on every key, serializing reads and writes. SSI allows full read concurrency — reads never block writes. The abort rate under SSI is higher than 2PL but acceptable for typical OLTP workloads.

---

## 5. Cursor Safety Architecture (Gap)

### Current State
```go
// In engine.go
func (e *StorageEngine) Scan(...) (*btree.BTreeCursor, error) {
    e.treeMu.RLock()
    defer e.treeMu.RUnlock()        // ← lock released before cursor returned
    return e.tree.Scan(txn, ...)    // cursor holds pageID of current leaf
}
```

A concurrent `Insert` or `Delete` that triggers a split or merge may change the sibling links the cursor relies on. The cursor's `nextPageID` may now point to a freed or repurposed page.

### Fix: Structural Version Counter
```go
type StorageEngine struct {
    // ... existing fields ...
    structureVersion atomic.Uint64  // incremented on every split, merge, borrow
}

// In BTreeCursor
type BTreeCursor struct {
    // ... existing fields ...
    capturedVersion uint64  // version at Scan() time
    engine         *StorageEngine
}

func (c *BTreeCursor) Next() (key, value []byte, err error) {
    if c.engine.structureVersion.Load() != c.capturedVersion {
        return nil, nil, ErrCursorInvalidated
    }
    // ... existing Next() logic ...
}
```

This is an optimistic approach — cheap to check (atomic load), no locking during cursor traversal.

**Alternative:** Hold `treeMu.RLock()` for the entire cursor lifetime. This is safe but prevents any write from making progress while a long scan is active. Not appropriate for production.

---

## 6. Group Commit Architecture

### Current State
Every `engine.Commit()` → `walMgr.Commit()` → `FlushUpTo(lsn)` → `fsync()`.
At 100 commits/sec: 100 fsyncs/sec. fsync latency on SSD: ~1ms. **Maximum throughput: ~1000 commits/sec.**

### Proposed: Commit Queue + Batcher

```
goroutine A: Commit(txn) ──────────────────────────────────┐
goroutine B: Commit(txn) ────────────────────────────────┐  │
goroutine C: Commit(txn) ──────────────────────────────┐ │  │
                                                        ▼ ▼  ▼
                                               [commitQueue chan commitReq]
                                                        │
                                              [batcher goroutine]
                                                        │
                                              1. Drain queue (up to delay_ms)
                                              2. Append all commit records
                                              3. Single fsync
                                              4. Signal all waiters
                                                        │
                                            goroutines A, B, C unblock
```

```go
type commitReq struct {
    txnID  uint64
    prevLSN uint64
    done   chan error
}

type WALManager struct {
    // existing fields ...
    commitQueue chan commitReq
    groupCommitDelay time.Duration
}
```

**Throughput model:** If 100 goroutines commit simultaneously within a 1ms window: 1 fsync serves 100 commits → 100x throughput improvement (theoretical). Practical: 5-20x depending on workload distribution.

**Durability:** Unchanged. fsync still called before any goroutine's `Commit()` returns.

---

## 7. Buffer Pool Eviction Architecture

### Current State: Clock (Second-Chance)
- Single RefBit per frame
- Two passes: clear RefBit on pass 1, evict on pass 2
- Equivalent to LRU-1: distinguishes "used at least once" from "not used since last sweep"

### Proposed: LRU-K (K=2)

LRU-2 tracks the two most recent access times per frame. Eviction priority is based on the **second-most-recent** access time (backward K-distance). This better handles sequential scans:

- Sequential scan: pages accessed once, high K-distance → evicted quickly
- Hot pages: accessed repeatedly, low K-distance → retained

```go
type Frame struct {
    // existing fields ...
    lastAccessTimes [2]time.Time  // ring buffer of last K=2 access times
}

// Eviction candidate: frame with largest K-distance
// K-distance = time since second-most-recent access (or ∞ if accessed < K times)
```

**Configuration:** `buffer_eviction_policy: "clock" | "lru2"` — allow A/B testing.

**Implementation note:** Access time comparison requires `time.Now()` on every page access — add ~10ns overhead. On most workloads this is negligible. For ultra-high-frequency access patterns, use monotonic counter instead of wall clock.

---

## 8. Logging Architecture

### Current State
`fmt.Printf` and `fmt.Fprintf(os.Stderr, ...)` scattered through the codebase. No structured fields, no log levels, no JSON output.

### Proposed: `slog` Integration

Go 1.21's `log/slog` provides structured logging with zero external dependencies:

```go
// internal/logging/logger.go
package logging

import (
    "log/slog"
    "os"
)

var Engine *slog.Logger

func Init(level slog.Level, format string) {
    var handler slog.Handler
    opts := &slog.HandlerOptions{Level: level}
    if format == "json" {
        handler = slog.NewJSONHandler(os.Stdout, opts)
    } else {
        handler = slog.NewTextHandler(os.Stdout, opts)
    }
    Engine = slog.New(handler)
}
```

**Usage in hot paths:**
```go
// DEBUG — only in non-hot paths (setup, init)
slog.Debug("fetching page", "page_id", pageID, "pool_size", bp.FrameCount())

// INFO — notable lifecycle events
slog.Info("checkpoint complete", "lsn", lsn, "dirty_pages", dptSize, "duration_ms", dur.Milliseconds())

// WARN — degraded but operational
slog.Warn("buffer pool under pressure", "dirty_pct", dirtyPct, "free_frames", freeFrames)

// ERROR — operation failed
slog.Error("checksum mismatch on page load", "page_id", pageID, "err", err)
```

**Cost:** `slog` with `TextHandler` at INFO level: ~50ns per log call for suppressed DEBUG messages (check is a single level comparison). JSON output costs ~100-200ns when emitted.

---

## 9. Dependency Injection Architecture

### Current State
`OpenEngine()` constructs all subsystems and wires them together. This is correct but makes testing harder — tests that want to inject a mock disk manager must use the full `OpenEngine()` flow.

### Proposed: Interface-First Subsystems

Most subsystems already expose interfaces (`DiskRW`, `WALFlusher`). The remaining gap is the B+Tree itself, which accepts concrete types.

**Proposed additions:**
```go
// internal/btree/interfaces.go
type TreeReader interface {
    Get(txn *mvcc.Transaction, key []byte) ([]byte, error)
    Scan(txn *mvcc.Transaction, start, end []byte) (Cursor, error)
}

type TreeWriter interface {
    Insert(txn *mvcc.Transaction, key, value []byte) error
    Update(txn *mvcc.Transaction, key, newValue []byte) error
    Delete(txn *mvcc.Transaction, key []byte) error
}

type Tree interface {
    TreeReader
    TreeWriter
}
```

**Benefit:** The gateway REST handlers can accept a `Tree` interface. Tests inject a mock tree that returns controlled responses. This decouples HTTP handler logic from storage logic — enabling faster unit tests that don't require a full engine.

---

## 10. Frontend Architecture Gaps

### Gap 1: No Error Boundary
The frontend panels have no error handling for failed API calls. If `/api/v1/tree/structure` returns 500, the D3 panel silently shows stale data.

**Fix:** Add `ErrorBoundary` wrapper component; show error message + retry button.

### Gap 2: No State Persistence
Panel state (selected page, current transaction, scroll position) is lost on page refresh.

**Fix:** Serialize panel state to `localStorage` on every state change; restore on mount.

### Gap 3: Mobile Layout
The 7-panel grid layout breaks on screens < 1200px wide.

**Fix:** Responsive CSS grid; on mobile, stack panels vertically with accordion collapse.

### Gap 4: WebSocket Reconnect Storm
If the backend restarts, all frontend clients reconnect simultaneously. The existing exponential backoff in `ws/client.ts` mitigates this but doesn't add jitter.

**Fix:** Add random jitter to backoff: `delay = base * 2^attempt + random(0, base)`.

### Gap 5: Large Tree Performance
The D3 tree layout recalculates all node positions on every update. For trees with 500+ nodes, this causes visible jank.

**Fix:** Implement incremental updates — only reposition nodes that changed; use D3 transitions for smooth animation.

---

## Summary: Priority Order for Architecture Changes

| # | Change | Phase | Effort | Impact |
|---|--------|-------|--------|--------|
| 1 | Auth middleware | 0 | M | Critical security fix |
| 2 | Structured logging (`slog`) | 0 | S | Operational necessity |
| 3 | WAL segmentation | 0 | L | Prevents disk exhaustion |
| 4 | Prometheus metrics | 1 | M | Enables production monitoring |
| 5 | Background checkpoint | 1 | S | Bounds recovery time |
| 6 | Cursor version counter | 1 | S | Prevents silent data corruption |
| 7 | Group commit | 3 | M | 5-20x write throughput |
| 8 | LRU-K eviction | 3 | M | 20-50% buffer hit rate improvement |
| 9 | SSI | 2 | XL | Prevents write skew |
| 10 | Interface injection | 4 | M | Better testability |

> Effort: S = 1-3 days, M = 1 week, L = 2 weeks, XL = 3-4 weeks
