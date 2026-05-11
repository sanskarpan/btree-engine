// Package metrics implements a Prometheus metrics collector that subscribes
// to the engine event bus and updates counters, gauges, and histograms.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"btree-engine/internal/events"
)

var (
	walFlushBuckets = []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0}
	diskIOBuckets   = []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5}
	recoveryBuckets = []float64{0.01, 0.1, 1, 10, 60, 300}
)

// Collector subscribes to the engine event bus and updates Prometheus metrics.
type Collector struct {
	mu          sync.Mutex
	unsubscribe func()

	// Buffer pool
	bufferHits   prometheus.Counter
	bufferMisses prometheus.Counter
	bufferDirty  prometheus.Gauge

	// WAL
	walFlushes       prometheus.Counter
	walFlushDuration prometheus.Histogram
	walCheckpoints   prometheus.Counter

	// Transactions
	txnsBegun          prometheus.Counter
	txnsCommitted      prometheus.Counter
	txnsAborted        prometheus.Counter
	txnsActive         prometheus.Gauge
	txnsWriteConflicts prometheus.Counter

	// Tree structure
	treeSplits prometheus.Counter
	treeMerges prometheus.Counter

	// Vacuum
	vacuumRuns           prometheus.Counter
	vacuumDeadReclaimed  prometheus.Counter
	vacuumPagesCompacted prometheus.Counter

	// Recovery
	recoveryRedoRecords prometheus.Counter
	recoveryUndoRecords prometheus.Counter
	recoveryDuration    prometheus.Histogram

	// Auth
	authFailures prometheus.Counter

	// Deadlock
	deadlocksDetected prometheus.Counter
}

// NewCollector creates a Collector and registers all metrics with reg.
func NewCollector(reg prometheus.Registerer) *Collector {
	c := &Collector{
		bufferHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_buffer_hits_total",
			Help: "Buffer pool cache hits.",
		}),
		bufferMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_buffer_misses_total",
			Help: "Buffer pool cache misses.",
		}),
		bufferDirty: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "btree_buffer_dirty_pages",
			Help: "Current number of dirty frames in the buffer pool.",
		}),
		walFlushes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_wal_flushes_total",
			Help: "Number of WAL fsync calls.",
		}),
		walFlushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "btree_wal_flush_duration_seconds",
			Help:    "WAL fsync latency in seconds.",
			Buckets: walFlushBuckets,
		}),
		walCheckpoints: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_wal_checkpoints_total",
			Help: "Number of WAL checkpoints written.",
		}),
		txnsBegun: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_txns_begun_total",
			Help: "Transactions started.",
		}),
		txnsCommitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_txns_committed_total",
			Help: "Transactions committed.",
		}),
		txnsAborted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_txns_aborted_total",
			Help: "Transactions aborted.",
		}),
		txnsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "btree_txns_active",
			Help: "Number of currently open transactions.",
		}),
		txnsWriteConflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_txns_write_conflicts_total",
			Help: "Write-write conflicts detected.",
		}),
		treeSplits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_page_splits_total",
			Help: "B+Tree page splits.",
		}),
		treeMerges: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_page_merges_total",
			Help: "B+Tree page merges.",
		}),
		vacuumRuns: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_vacuum_runs_total",
			Help: "Vacuum cycles completed.",
		}),
		vacuumDeadReclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_vacuum_dead_tuples_reclaimed_total",
			Help: "Dead MVCC tuples reclaimed by vacuum.",
		}),
		vacuumPagesCompacted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_vacuum_pages_compacted_total",
			Help: "Pages compacted by vacuum.",
		}),
		recoveryRedoRecords: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_recovery_redo_records_total",
			Help: "WAL records applied during ARIES redo phase.",
		}),
		recoveryUndoRecords: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_recovery_undo_records_total",
			Help: "WAL records undone during ARIES undo phase.",
		}),
		recoveryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "btree_recovery_duration_seconds",
			Help:    "Total ARIES recovery duration in seconds.",
			Buckets: recoveryBuckets,
		}),
		authFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_auth_failures_total",
			Help: "API authentication failures.",
		}),
		deadlocksDetected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "btree_deadlocks_detected_total",
			Help: "Number of deadlock cycles detected by the waits-for graph.",
		}),
	}

	reg.MustRegister(
		c.bufferHits,
		c.bufferMisses,
		c.bufferDirty,
		c.walFlushes,
		c.walFlushDuration,
		c.walCheckpoints,
		c.txnsBegun,
		c.txnsCommitted,
		c.txnsAborted,
		c.txnsActive,
		c.txnsWriteConflicts,
		c.treeSplits,
		c.treeMerges,
		c.vacuumRuns,
		c.vacuumDeadReclaimed,
		c.vacuumPagesCompacted,
		c.recoveryRedoRecords,
		c.recoveryUndoRecords,
		c.recoveryDuration,
		c.authFailures,
		c.deadlocksDetected,
	)
	return c
}

// Subscribe attaches the collector to the engine event bus.
// Uses SubscribeAll so it receives every event type.
func (c *Collector) Subscribe(bus *events.EventBus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unsubscribe != nil {
		c.unsubscribe()
		c.unsubscribe = nil
	}
	if bus == nil {
		return
	}
	c.unsubscribe = bus.SubscribeAll(func(e events.Event) {
		switch e.Type {
		case events.EvtBufferHit:
			c.bufferHits.Inc()
		case events.EvtBufferMiss:
			c.bufferMisses.Inc()
		case events.EvtPageDirty:
			c.bufferDirty.Inc()

		case events.EvtWALFlush:
			c.walFlushes.Inc()
			if dur, ok := e.Extra["duration_ns"].(int64); ok {
				c.walFlushDuration.Observe(float64(dur) / 1e9)
			}
		case events.EvtWALCheckpoint:
			c.walCheckpoints.Inc()

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

		case events.EvtPageSplit, events.EvtTreeSplit:
			c.treeSplits.Inc()
		case events.EvtPageMerge, events.EvtTreeMerge:
			c.treeMerges.Inc()

		case events.EvtVacuumDone:
			c.vacuumRuns.Inc()
			if n, ok := e.Extra["dead_removed"].(int); ok {
				c.vacuumDeadReclaimed.Add(float64(n))
			}
			if n, ok := e.Extra["pages_compacted"].(int); ok {
				c.vacuumPagesCompacted.Add(float64(n))
			}

		case events.EvtRecoveryRedo:
			if n, ok := e.Extra["count"].(int); ok {
				c.recoveryRedoRecords.Add(float64(n))
			} else {
				c.recoveryRedoRecords.Inc()
			}
		case events.EvtRecoveryUndo:
			if n, ok := e.Extra["count"].(int); ok {
				c.recoveryUndoRecords.Add(float64(n))
			} else {
				c.recoveryUndoRecords.Inc()
			}
		case events.EvtRecoveryDone:
			if dur, ok := e.Extra["duration_ns"].(int64); ok {
				c.recoveryDuration.Observe(float64(dur) / 1e9)
			}

		case events.EvtAuthFailure:
			c.authFailures.Inc()

		case events.EvtDeadlock:
			c.deadlocksDetected.Inc()
		}
	})
}
