// Package events provides a non-blocking fan-out event bus for internal engine events.
package events

import "sync"

// EventType identifies the category of an engine event.
type EventType string

const (
	// Page operations
	EvtPageRead    EventType = "page_read"
	EvtPageWrite   EventType = "page_write"
	EvtPageEvict   EventType = "page_evict"
	EvtPageDirty   EventType = "page_dirty"
	EvtPageSplit   EventType = "page_split"
	EvtPageMerge   EventType = "page_merge"
	EvtPageAlloc   EventType = "page_alloc"
	EvtPageFree    EventType = "page_free"
	EvtPageCompact EventType = "page_compact"
	// B+Tree
	EvtTreeInsert EventType = "tree_insert"
	EvtTreeSearch EventType = "tree_search"
	EvtTreeDelete EventType = "tree_delete"
	EvtTreeSplit  EventType = "tree_split"
	EvtTreeMerge  EventType = "tree_merge"
	// MVCC
	EvtTxnBegin      EventType = "txn_begin"
	EvtTxnCommit     EventType = "txn_commit"
	EvtTxnAbort      EventType = "txn_abort"
	EvtTupleInsert   EventType = "tuple_insert"
	EvtTupleDelete   EventType = "tuple_delete"
	EvtTupleVisible  EventType = "tuple_visible"
	EvtTupleHidden   EventType = "tuple_hidden"
	EvtSnapshotTaken EventType = "snapshot_taken"
	EvtWriteSkew     EventType = "write_skew"
	// WAL
	EvtWALAppend     EventType = "wal_append"
	EvtWALFlush      EventType = "wal_flush"
	EvtWALCheckpoint EventType = "wal_checkpoint"
	EvtWALRotated    EventType = "wal_rotated"
	EvtWALTruncated  EventType = "wal_truncated"
	// Recovery
	EvtRecoveryStart    EventType = "recovery_start"
	EvtRecoveryAnalysis EventType = "recovery_analysis"
	EvtRecoveryRedo     EventType = "recovery_redo"
	EvtRecoveryUndo     EventType = "recovery_undo"
	EvtRecoveryDone     EventType = "recovery_done"
	// Buffer Pool
	EvtBufferHit  EventType = "buffer_hit"
	EvtBufferMiss EventType = "buffer_miss"
	// Vacuum
	EvtVacuumStart EventType = "vacuum_start"
	EvtVacuumTuple EventType = "vacuum_tuple"
	EvtVacuumPage  EventType = "vacuum_page"
	EvtVacuumDone  EventType = "vacuum_done"
	// Auth
	EvtAuthFailure EventType = "auth_failure"
	// Conflicts
	EvtWriteConflict EventType = "write_conflict"
	// Checkpoints
	EvtCheckpointDone EventType = "checkpoint_done"
	// Deadlock detection
	EvtDeadlock EventType = "deadlock_detected"
)

// Event is a single engine event published to subscribers.
type Event struct {
	Type  EventType
	Extra map[string]interface{}
}

// Handler is a callback invoked when an event is published.
type Handler func(Event)

// EventBus is a non-blocking fan-out pub/sub bus.
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[EventType][]Handler
	global      []Handler // subscribers that receive all events
}

// NewEventBus creates a new EventBus.
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[EventType][]Handler),
	}
}

// Subscribe registers a handler for a specific event type.
// Returns an unsubscribe function.
func (b *EventBus) Subscribe(t EventType, h Handler) func() {
	b.mu.Lock()
	b.subscribers[t] = append(b.subscribers[t], h)
	idx := len(b.subscribers[t]) - 1
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		s := b.subscribers[t]
		if idx < len(s) {
			s[idx] = nil // nil out; Publish skips nils
		}
	}
}

// SubscribeAll registers a handler that receives every event.
func (b *EventBus) SubscribeAll(h Handler) func() {
	b.mu.Lock()
	b.global = append(b.global, h)
	idx := len(b.global) - 1
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if idx < len(b.global) {
			b.global[idx] = nil
		}
	}
}

// Publish sends an event to all matching subscribers. Calls are non-blocking:
// each handler is invoked in a new goroutine so slow handlers cannot stall the engine.
//
// Handlers are collected into a local slice under the read-lock so that
// concurrent unsubscribe (which nils out elements) cannot race with iteration.
func (b *EventBus) Publish(evt Event) {
	b.mu.RLock()
	// Snapshot live handlers under the lock to avoid a data race with unsubscribe,
	// which nils out elements in the same backing array without the lock held.
	var handlers []Handler
	for _, h := range b.subscribers[evt.Type] {
		if h != nil {
			handlers = append(handlers, h)
		}
	}
	for _, h := range b.global {
		if h != nil {
			handlers = append(handlers, h)
		}
	}
	b.mu.RUnlock()

	for _, h := range handlers {
		h := h
		go h(evt)
	}
}
