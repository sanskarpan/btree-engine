# P1-001 — Prometheus Metrics Endpoint

**Priority:** P1 (High)
**Phase:** 1-A
**Effort:** M (1 week)
**Depends on:** P0-008 (structured logging, shares event bus pattern)
**Blocks:** Grafana dashboard, alerting rules

---

## Problem Statement

The current `GET /api/v1/engine/stats` endpoint returns 4 fields: `buffer_hit_rate`, `dirty_pages`, `total_frames`, `active_txns`. This is insufficient for production monitoring:
- No time-series data (Prometheus scrape history)
- No WAL metrics (write rate, flush latency)
- No tree structural metrics (height, splits, merges)
- No I/O latency histograms
- Not compatible with standard monitoring stack (Prometheus/Grafana)

---

## Goals

1. Standard `GET /metrics` endpoint in Prometheus exposition format
2. 30+ metrics covering all engine subsystems
3. Latency histograms with appropriate bucket boundaries
4. Zero hot-path overhead (event bus subscription model)
5. Compatible with Prometheus 2.x scrape format

---

## Technical Design

### New Dependency

```
github.com/prometheus/client_golang v1.21.0
```

### New File: `internal/metrics/collector.go`

```go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "btree-engine/internal/events"
)

type Collector struct {
    // Buffer pool
    bufferHits      prometheus.Counter
    bufferMisses    prometheus.Counter
    bufferHitRatio  prometheus.Gauge
    bufferDirty     prometheus.Gauge
    bufferPinned    prometheus.Gauge

    // WAL
    walBytesWritten prometheus.Counter
    walFlushes      prometheus.Counter
    walFlushDuration prometheus.Histogram
    walSizeBytes    prometheus.Gauge

    // Transactions
    txnsBegun       prometheus.Counter
    txnsCommitted   prometheus.Counter
    txnsAborted     prometheus.Counter
    txnsActive      prometheus.Gauge
    txnsWriteConflicts prometheus.Counter

    // Tree structure
    treeHeight      prometheus.Gauge
    treeSplits      prometheus.Counter
    treeMerges      prometheus.Counter
    treeLeafPages   prometheus.Gauge
    treeInternalPages prometheus.Gauge

    // Vacuum
    vacuumRuns      prometheus.Counter
    vacuumDeadTuplesReclaimed prometheus.Counter
    vacuumPagesCompacted prometheus.Counter

    // Disk I/O
    diskReads       prometheus.Counter
    diskWrites      prometheus.Counter
    diskReadDuration  prometheus.Histogram
    diskWriteDuration prometheus.Histogram

    // Recovery
    recoveryRedoRecords prometheus.Counter
    recoveryUndoRecords prometheus.Counter
    recoveryDuration    prometheus.Histogram
}

func NewCollector(reg prometheus.Registerer) *Collector {
    c := &Collector{
        bufferHits: prometheus.NewCounter(prometheus.CounterOpts{
            Name: "btree_buffer_hits_total",
            Help: "Number of buffer pool cache hits.",
        }),
        // ... register all metrics ...
    }
    reg.MustRegister(
        c.bufferHits, c.bufferMisses, /* ... all metrics ... */
    )
    return c
}

// Subscribe starts consuming events from the bus and updating metrics.
func (c *Collector) Subscribe(bus *events.EventBus) {
    bus.Subscribe(func(e events.Event) {
        switch e.Type {
        case events.EvtBufferHit:
            c.bufferHits.Inc()
        case events.EvtBufferMiss:
            c.bufferMisses.Inc()
        case events.EvtPageDirty:
            c.bufferDirty.Inc()
        case events.EvtTxnBegin:
            c.txnsBegun.Inc()
            c.txnsActive.Inc()
        case events.EvtTxnCommit:
            c.txnsCommitted.Inc()
            c.txnsActive.Dec()
        case events.EvtTxnAbort:
            c.txnsAborted.Inc()
            c.txnsActive.Dec()
        case events.EvtWriteConflict:
            c.txnsWriteConflicts.Inc()
        case events.EvtPageSplit:
            c.treeSplits.Inc()
        case events.EvtPageMerge:
            c.treeMerges.Inc()
        case events.EvtWALWrite:
            if bytes, ok := e.Extra["bytes"].(int); ok {
                c.walBytesWritten.Add(float64(bytes))
            }
        case events.EvtWALFlush:
            c.walFlushes.Inc()
            if dur, ok := e.Extra["duration_ns"].(int64); ok {
                c.walFlushDuration.Observe(float64(dur) / 1e9)
            }
        case events.EvtDiskRead:
            c.diskReads.Inc()
            if dur, ok := e.Extra["duration_ns"].(int64); ok {
                c.diskReadDuration.Observe(float64(dur) / 1e9)
            }
        case events.EvtDiskWrite:
            c.diskWrites.Inc()
            if dur, ok := e.Extra["duration_ns"].(int64); ok {
                c.diskWriteDuration.Observe(float64(dur) / 1e9)
            }
        case events.EvtVacuumComplete:
            c.vacuumRuns.Inc()
            if n, ok := e.Extra["dead_removed"].(int); ok {
                c.vacuumDeadTuplesReclaimed.Add(float64(n))
            }
        }
    })
}
```

### New Events Required

The following event types must be added to `internal/events/events.go`:

```go
const (
    // existing ...
    EvtTxnBegin         EventType = "txn_begin"       // already: EvtTxnBegin?
    EvtTxnCommit        EventType = "txn_commit"
    EvtTxnAbort         EventType = "txn_abort"
    EvtWriteConflict    EventType = "write_conflict"   // NEW
    EvtPageSplit        EventType = "page_split"       // already exists
    EvtPageMerge        EventType = "page_merge"       // NEW
    EvtWALWrite         EventType = "wal_write"        // NEW — with bytes field
    EvtWALFlush         EventType = "wal_flush"        // NEW — with duration_ns field
    EvtDiskRead         EventType = "disk_read"        // NEW — with duration_ns field
    EvtDiskWrite        EventType = "disk_write"       // NEW — with duration_ns field
    EvtVacuumComplete   EventType = "vacuum_complete"  // NEW — with dead_removed field
    EvtWALCheckpoint    EventType = "wal_checkpoint"   // already exists
)
```

### Registration in `cmd/server/main.go`

```go
reg := prometheus.NewRegistry()
reg.MustRegister(prometheus.NewGoCollector())  // Go runtime metrics
reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

collector := metrics.NewCollector(reg)
collector.Subscribe(eng.EventBus())

// Register /metrics handler
http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

---

## Histogram Bucket Definitions

```go
// WAL flush duration (microseconds to seconds)
walFlushBuckets = []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0}

// Disk I/O duration (100µs to 500ms)
diskIOBuckets = []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5}

// Recovery duration (milliseconds to minutes)
recoveryBuckets = []float64{0.01, 0.1, 1, 10, 60, 300}
```

---

## Complete Metrics Reference

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `btree_buffer_hits_total` | counter | — | Cache hits |
| `btree_buffer_misses_total` | counter | — | Cache misses |
| `btree_buffer_hit_ratio` | gauge | — | hits/(hits+misses) |
| `btree_buffer_dirty_pages` | gauge | — | Current dirty frame count |
| `btree_buffer_pinned_pages` | gauge | — | Current pinned frame count |
| `btree_wal_bytes_written_total` | counter | — | Bytes appended to WAL |
| `btree_wal_flushes_total` | counter | — | fsync calls |
| `btree_wal_flush_duration_seconds` | histogram | — | fsync latency |
| `btree_wal_size_bytes` | gauge | — | Current WAL file size |
| `btree_txns_begun_total` | counter | — | Transactions started |
| `btree_txns_committed_total` | counter | iso_level | Committed transactions |
| `btree_txns_aborted_total` | counter | reason | Aborted transactions (reason: user/conflict/timeout) |
| `btree_txns_active` | gauge | — | Open transactions |
| `btree_txns_write_conflicts_total` | counter | — | ErrWriteConflict occurrences |
| `btree_tree_height` | gauge | — | Current tree height |
| `btree_page_splits_total` | counter | type | Page splits (leaf/internal) |
| `btree_page_merges_total` | counter | — | Page merges |
| `btree_leaf_pages` | gauge | — | Leaf page count |
| `btree_internal_pages` | gauge | — | Internal page count |
| `btree_vacuum_runs_total` | counter | — | Vacuum cycles completed |
| `btree_vacuum_dead_tuples_reclaimed_total` | counter | — | Dead tuples removed |
| `btree_vacuum_pages_compacted_total` | counter | — | Pages compacted |
| `btree_disk_reads_total` | counter | — | Page reads from disk |
| `btree_disk_writes_total` | counter | — | Page writes to disk |
| `btree_disk_read_duration_seconds` | histogram | — | Read latency |
| `btree_disk_write_duration_seconds` | histogram | — | Write latency |
| `btree_recovery_redo_records_total` | counter | — | Records applied during redo |
| `btree_recovery_undo_records_total` | counter | — | Records undone |
| `btree_recovery_duration_seconds` | histogram | — | Total recovery duration |
| `btree_auth_failures_total` | counter | — | Authentication failures |

---

## Frontend Implications

- No frontend changes required for Prometheus endpoint
- Optional: Add Grafana provisioning (`docs/grafana_dashboard.json`) with panels for each metric group
- The existing D3 dashboard can poll `/api/v1/engine/stats` for backward compatibility

---

## Testing Plan

### Unit Tests

```go
func TestMetricsCollector_BufferHit(t *testing.T) {
    // Publish EvtBufferHit → verify btree_buffer_hits_total incremented
}

func TestMetricsCollector_WriteConflict(t *testing.T) {
    // Publish EvtWriteConflict → verify counter incremented
}

func TestMetricsEndpoint_Format(t *testing.T) {
    // GET /metrics → verify Prometheus text format response
    // Parse with expfmt → verify all expected metrics present
}
```

### Integration Tests

```go
func TestMetrics_AfterScenario(t *testing.T) {
    // Run insert-and-search scenario
    // GET /metrics
    // Verify btree_txns_committed_total >= 2
    // Verify btree_buffer_hits_total > 0
}
```

---

## Definition of Done

- [ ] `/metrics` endpoint returns valid Prometheus format
- [ ] All 30 metrics listed in table are registered and updated
- [ ] Histograms have appropriate bucket boundaries
- [ ] Go runtime + process metrics included
- [ ] New event types published at all instrumented call sites
- [ ] Unit tests for each metric type
- [ ] Integration test verifying metrics update after scenarios
- [ ] `docs/grafana_dashboard.json` with 6 panels (one per subsystem)
- [ ] `/metrics` exempt from auth (liveness/readiness pattern)
