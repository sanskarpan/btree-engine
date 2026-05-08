package mvcc

import (
	"context"
	"log/slog"
	"time"

	"btree-engine/internal/events"
	"btree-engine/internal/page"
)

// VacuumConfig holds vacuum worker settings.
type VacuumConfig struct {
	Interval      time.Duration
	DeadThreshold float64 // compact if fragmentation > this
}

// DefaultVacuumConfig returns sensible defaults.
func DefaultVacuumConfig() VacuumConfig {
	return VacuumConfig{
		Interval:      60 * time.Second,
		DeadThreshold: 0.20,
	}
}

// PageScanner is the minimal interface vacuum needs to walk leaf pages.
type PageScanner interface {
	// FirstLeafPageID returns the leftmost leaf page ID.
	FirstLeafPageID() uint32
	// ReadLeafPage returns the page for reading/writing. Caller must call UnpinLeaf.
	ReadLeafPage(pageID uint32) *page.Page
	// WriteLeafPage writes back a modified leaf page.
	WriteLeafPage(p *page.Page)
	// UnpinLeaf releases the pin on a leaf page.
	UnpinLeaf(pageID uint32, dirty bool)
}

// VacuumWorker runs in the background and reclaims dead MVCC tuples.
type VacuumWorker struct {
	engine         PageScanner
	tm             *TransactionManager
	cfg            VacuumConfig
	bus            *events.EventBus
	bloomRebuildFn func(pageID uint32, keys [][]byte) // optional post-compact hook (P3-019)
}

// NewVacuumWorker creates a VacuumWorker.
func NewVacuumWorker(engine PageScanner, tm *TransactionManager, cfg VacuumConfig, bus *events.EventBus) *VacuumWorker {
	return &VacuumWorker{engine: engine, tm: tm, cfg: cfg, bus: bus}
}

// SetBloomRebuildHook registers a callback invoked after each leaf page is compacted.
// The callback receives the page ID and the list of live keys remaining on the page so
// that the per-page Bloom filter can be rebuilt without false-negatives.
func (v *VacuumWorker) SetBloomRebuildHook(fn func(pageID uint32, keys [][]byte)) {
	v.bloomRebuildFn = fn
}

// Run starts the vacuum loop until ctx is cancelled.
func (v *VacuumWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(v.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v.RunOnce()
		}
	}
}

// RunOnce performs a single vacuum pass over all leaf pages.
func (v *VacuumWorker) RunOnce() {
	start := time.Now()
	oldestActive := v.tm.OldestActiveTxn()
	slog.Info("vacuum run starting", "oldest_active_txn", uint64(oldestActive))
	if v.bus != nil {
		v.bus.Publish(events.Event{Type: events.EvtVacuumStart})
	}

	var pagesScanned, deadRemoved, pagesCompacted int
	pageID := v.engine.FirstLeafPageID()
	for pageID != 0 {
		p := v.engine.ReadLeafPage(pageID)
		if p == nil {
			// FetchPage failed (e.g. buffer pool full) — stop this vacuum pass.
			break
		}
		nextID := p.NextSib() // capture before unpin; p is a value copy so this is safe
		deadBefore := countDead(p, v.tm, oldestActive)
		dirty := v.vacuumLeafPage(p, oldestActive)
		if dirty {
			removed := deadBefore
			deadRemoved += removed
			pagesCompacted++
			v.engine.WriteLeafPage(p)
			// Rebuild bloom filter with only the live keys remaining after compaction.
			if v.bloomRebuildFn != nil {
				var liveKeys [][]byte
				for i := 0; i < p.NumSlots(); i++ {
					data := p.GetRecord(i)
					if data == nil {
						continue
					}
					t, err := DecodeTuple(data)
					if err == nil {
						liveKeys = append(liveKeys, t.Key)
					}
				}
				v.bloomRebuildFn(pageID, liveKeys)
			}
		}
		v.engine.UnpinLeaf(pageID, dirty)
		pagesScanned++
		pageID = nextID
	}

	// Prune the statusTable: entries below the oldest active txn can never affect
	// any current or future snapshot, so they are safe to discard.
	v.tm.PruneStatusesBefore(oldestActive)

	if v.bus != nil {
		v.bus.Publish(events.Event{Type: events.EvtVacuumDone})
	}
	slog.Info("vacuum run complete",
		"pages_scanned", pagesScanned,
		"dead_tuples_removed", deadRemoved,
		"pages_compacted", pagesCompacted,
		"duration_ms", time.Since(start).Milliseconds())
}

// countDead counts dead tuples on a page without modifying it.
func countDead(p *page.Page, tm *TransactionManager, oldestActive TxnID) int {
	n := p.NumSlots()
	count := 0
	for i := 0; i < n; i++ {
		data := p.GetRecord(i)
		if data == nil {
			continue
		}
		t, err := DecodeTuple(data)
		if err != nil {
			continue
		}
		if tm.GetStatus(TxnID(t.Xmin)) == TxnAborted {
			count++
			continue
		}
		if t.Xmax != 0 &&
			tm.GetStatus(TxnID(t.Xmax)) == TxnCommitted &&
			(oldestActive == 0 || TxnID(t.Xmax) < oldestActive) {
			count++
		}
	}
	return count
}

// vacuumLeafPage removes dead tuples from a leaf page. Returns true if the page was modified.
// Dead tuples come in two forms:
//  1. Tuples inserted by an aborted transaction (Xmin = TxnAborted): always invisible and safe
//     to reclaim regardless of the oldest active snapshot.
//  2. Tuples deleted by a committed transaction (Xmax committed, Xmax < oldestActive): no
//     remaining active transaction can see them, so they are safe to reclaim.
func (v *VacuumWorker) vacuumLeafPage(p *page.Page, oldestActive TxnID) bool {
	n := p.NumSlots()
	modified := false
	for i := 0; i < n; i++ {
		data := p.GetRecord(i)
		if data == nil {
			continue // already tombstone
		}
		t, err := DecodeTuple(data)
		if err != nil {
			continue
		}

		dead := false
		// Form 1: inserting transaction aborted — tuple is always invisible.
		if v.tm.GetStatus(TxnID(t.Xmin)) == TxnAborted {
			dead = true
		}
		// Form 2: tuple was deleted by a committed transaction that is older than
		// the oldest active snapshot (no one can see the live version anymore).
		if !dead && t.Xmax != 0 &&
			v.tm.GetStatus(TxnID(t.Xmax)) == TxnCommitted &&
			(oldestActive == 0 || TxnID(t.Xmax) < oldestActive) {
			dead = true
		}

		if !dead {
			continue
		}
		p.DeleteRecord(i)
		modified = true
		if v.bus != nil {
			v.bus.Publish(events.Event{Type: events.EvtVacuumTuple,
				Extra: map[string]interface{}{
					"key": string(t.Key), "xmin": t.Xmin, "xmax": t.Xmax,
				}})
		}
	}
	if modified && p.FragmentationRatio() > v.cfg.DeadThreshold {
		p.Compact()
	}
	return modified
}
