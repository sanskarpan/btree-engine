# P3-001 to P3-004 — Group Commit

**Priority:** P3 (Performance)
**Phase:** 3-A
**Effort:** M (1 week)
**Depends on:** P0-015 (WAL segmentation, clean WAL abstraction)
**Blocks:** P3-005 (WAL pipelining builds on this)

---

## Problem Statement

Every `engine.Commit()` calls `walMgr.Commit()` → `FlushUpTo(lsn)` → `file.Sync()` (fsync). This serialises all commits on a single fsync system call.

**Measured bottleneck:**
- SSD fsync latency: ~0.5-1ms
- Maximum commit throughput: 1000-2000 commits/sec (1/fsync_latency)
- The engine currently achieves approximately this theoretical maximum

**Real-world implication:** 100 goroutines each committing a transaction simultaneously spend 99ms waiting for each other's fsync to complete, despite the fact that one fsync could durably commit all 100 transactions simultaneously.

Group commit is the standard database technique for amortizing fsync cost.

---

## Goals

1. Multiple concurrent commits batched into a single fsync
2. All batched commits return only after fsync completes (durability unchanged)
3. Configurable batch window (`group_commit_delay_ms`, default 0 = disabled)
4. Configurable max batch size (`group_commit_max_size`, default 100)
5. 5-20x commit throughput improvement under concurrent workloads (benchmarked)

---

## Technical Design

### Commit Queue Architecture

```
Goroutine A ──commit(lsn=100)──┐
Goroutine B ──commit(lsn=101)──┤──→ [commitChan] ──→ [batcher goroutine]
Goroutine C ──commit(lsn=102)──┘                           │
                                                 1. Drain chan (up to maxWait)
                                                 2. Append all COMMIT records
                                                 3. Single fsync
                                                 4. Write notify on all done chans
                                                           │
                              Goroutines A, B, C ←─── receive on done chan
                              (all return at same time, all durable)
```

### New File: `internal/wal/group_commit.go`

```go
package wal

import (
    "sync"
    "time"
)

type commitReq struct {
    txnID   uint64
    prevLSN uint64
    lsnCh   chan uint64  // receives the assigned LSN after flush
    errCh   chan error   // receives nil or error after flush
}

type GroupCommitter struct {
    wal          *WALManager
    queue        chan commitReq
    maxBatchSize int
    maxWait      time.Duration
    wg           sync.WaitGroup
    shutdown     chan struct{}
}

func NewGroupCommitter(wal *WALManager, maxBatchSize int, maxWait time.Duration) *GroupCommitter {
    gc := &GroupCommitter{
        wal:          wal,
        queue:        make(chan commitReq, maxBatchSize*4),
        maxBatchSize: maxBatchSize,
        maxWait:      maxWait,
        shutdown:     make(chan struct{}),
    }
    gc.wg.Add(1)
    go gc.run()
    return gc
}

// Commit enqueues a commit request and blocks until the WAL is durable.
func (gc *GroupCommitter) Commit(txnID uint64, prevLSN uint64) (uint64, error) {
    req := commitReq{
        txnID:   txnID,
        prevLSN: prevLSN,
        lsnCh:   make(chan uint64, 1),
        errCh:   make(chan error, 1),
    }
    gc.queue <- req
    select {
    case lsn := <-req.lsnCh:
        return lsn, <-req.errCh
    case <-gc.shutdown:
        return 0, ErrWALClosed
    }
}

func (gc *GroupCommitter) run() {
    defer gc.wg.Done()
    for {
        // Wait for at least one commit request
        var batch []commitReq
        select {
        case req := <-gc.queue:
            batch = append(batch, req)
        case <-gc.shutdown:
            return
        }

        // Drain additional requests within the batch window
        deadline := time.After(gc.maxWait)
        draining := true
        for draining && len(batch) < gc.maxBatchSize {
            select {
            case req := <-gc.queue:
                batch = append(batch, req)
            case <-deadline:
                draining = false
            }
        }

        // Append all commit records and flush once
        var lastLSN uint64
        var flushErr error
        for _, req := range batch {
            lsn := gc.wal.appendCommitRecord(req.txnID, req.prevLSN)
            req.lsnCh <- lsn
            lastLSN = lsn
        }
        flushErr = gc.wal.flushUpTo(lastLSN)

        // Notify all waiters
        for _, req := range batch {
            req.errCh <- flushErr
        }
    }
}

func (gc *GroupCommitter) Close() {
    close(gc.shutdown)
    gc.wg.Wait()
}
```

### Integration with WALManager

```go
// internal/wal/wal.go

type WALManager struct {
    // existing fields ...
    groupCommitter *GroupCommitter  // nil if group commit disabled
}

func (w *WALManager) Commit(txnID uint64, prevLSN uint64) uint64 {
    if w.groupCommitter != nil {
        lsn, _ := w.groupCommitter.Commit(txnID, prevLSN)
        return lsn
    }
    // original path: immediate fsync
    lsn := w.appendCommitRecord(txnID, prevLSN)
    w.flushUpTo(lsn)
    return lsn
}
```

### Config Changes

```yaml
wal:
  group_commit_delay_ms: 1    # 0 = disabled (default); 1-50 = batch window
  group_commit_max_size: 100  # max transactions per batch
```

---

## Performance Analysis

**Model:** 100 concurrent goroutines each committing once.

| Mode | fsync calls | Latency (per goroutine) | Total throughput |
|------|-------------|------------------------|-----------------|
| No group commit | 100 | ~1ms (wait for individual fsync) | ~1000 commits/sec |
| Group commit (1ms window) | 1 | ~1ms (wait for one shared fsync) | ~100,000 commits/sec |

**Practical improvement:** 10-50x for bursty workloads; minimal improvement for serial workloads.

---

## Benchmark

```go
// test/unit/wal_test.go

func BenchmarkGroupCommit_10(b *testing.B) {
    benchGroupCommit(b, 10, 1*time.Millisecond)
}

func BenchmarkGroupCommit_100(b *testing.B) {
    benchGroupCommit(b, 100, 1*time.Millisecond)
}

func BenchmarkGroupCommit_1000(b *testing.B) {
    benchGroupCommit(b, 1000, 5*time.Millisecond)
}

func benchGroupCommit(b *testing.B, goroutines int, delay time.Duration) {
    // Spin up `goroutines` goroutines, each committing b.N/goroutines transactions
    // Report ns/commit, commits/sec
}
```

---

## Durability Guarantee (Unchanged)

Group commit does NOT weaken durability:
- A `Commit()` call returns only after `fsync()` completes
- The fsync covers this commit's WAL record
- A crash after `Commit()` returns → record is durable → recovery will not undo it

The only change: multiple commits share one fsync, so each waits up to `group_commit_delay_ms` longer. This is the **same trade-off** as `innodb_flush_log_at_trx_commit=2` in MySQL (but MySQL weakens durability; this implementation does not).

---

## Edge Cases

| Scenario | Expected Behavior |
|----------|------------------|
| Only one goroutine committing | Falls through to immediate fsync after `maxWait` timeout |
| Engine shutdown during batch | All pending commits return `ErrWALClosed` |
| fsync fails | All goroutines in batch receive the error |
| `group_commit_delay_ms: 0` | GroupCommitter not instantiated; original code path used |
| Batcher goroutine panics | Recovered; all pending requests receive error |

---

## Testing Plan

```go
func TestGroupCommit_AllCommitsDurable(t *testing.T) {
    // 10 goroutines commit concurrently
    // Crash engine, recover
    // Verify all 10 keys present
}

func TestGroupCommit_FsyncFailureReturnsError(t *testing.T) {
    // Inject fsync failure
    // Verify all goroutines in batch receive error
}

func TestGroupCommit_Disabled(t *testing.T) {
    // delay=0: verify original sync code path used
}
```

---

## Rollout Strategy

1. Implement `GroupCommitter` with tests (zero integration risk — not wired in yet)
2. Wire into `WALManager.Commit()` behind `groupCommitter != nil` guard
3. Keep `group_commit_delay_ms: 0` (disabled) in default config
4. Run `BenchmarkGroupCommit_100` to measure improvement
5. Enable with `group_commit_delay_ms: 1` in production after benchmarking

---

## Definition of Done

- [ ] `GroupCommitter` implemented in `internal/wal/group_commit.go`
- [ ] `WALManager.Commit()` uses group committer when configured
- [ ] Config options: `group_commit_delay_ms`, `group_commit_max_size`
- [ ] Default config: disabled (0ms delay)
- [ ] Durability test: crash after concurrent commits, verify all present after recovery
- [ ] Benchmarks show ≥5x improvement at 100 concurrent committers
- [ ] Engine shutdown cleanly drains pending commits
