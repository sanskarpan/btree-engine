// Package mvcc — waits-for graph and deadlock detection for transaction management.
//
// When T_waiter is blocked waiting for a lock held by T_holder, an edge
// T_waiter → T_holder is added to the graph.  If this creates a cycle, a
// deadlock is detected and the youngest transaction in the cycle is chosen
// as the victim and aborted.
package mvcc

import (
	"errors"
	"sync"
)

// ErrDeadlockVictim is returned to the transaction chosen as the deadlock victim.
var ErrDeadlockVictim = errors.New("transaction aborted: deadlock detected")

// DeadlockDetector maintains a waits-for graph and detects cycles.
type DeadlockDetector struct {
	mu    sync.Mutex
	edges map[TxnID]TxnID // waiter → holder (one holder per waiter at a time)
}

// NewDeadlockDetector creates an empty detector.
func NewDeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{
		edges: make(map[TxnID]TxnID),
	}
}

// AddWait records that waiterID is waiting for holderID.
// Returns ErrDeadlockVictim if the waiter forms a cycle and is chosen as the
// victim.  Returns ErrDeadlockVictim with the victim ID embedded in the message
// when the cycle exists but the waiter is NOT the victim (i.e., the caller
// should abort the victim transaction on behalf of the waiter).
//
// The method adds the edge, runs DFS to find a cycle, and if found:
//   - determines the youngest (highest ID) transaction in the cycle as victim
//   - removes all edges for the victim
//   - returns ErrDeadlockVictim so the caller can abort the victim
func (d *DeadlockDetector) AddWait(waiterID, holderID TxnID) (victimID TxnID, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.edges[waiterID] = holderID

	cycle := d.findCycle(waiterID)
	if len(cycle) == 0 {
		return 0, nil // no deadlock
	}

	// Choose the youngest (highest TxnID) transaction in the cycle as the victim.
	victim := cycle[0]
	for _, id := range cycle[1:] {
		if id > victim {
			victim = id
		}
	}

	// Remove all outgoing edges from the victim (it will be aborted).
	delete(d.edges, victim)

	return victim, ErrDeadlockVictim
}

// RemoveWait removes any wait edge from waiterID (called after it acquires
// the lock or is aborted).
func (d *DeadlockDetector) RemoveWait(waiterID TxnID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.edges, waiterID)
}

// findCycle performs a DFS from start and returns the transactions forming a cycle,
// or nil if no cycle is reachable.  Must be called with d.mu held.
func (d *DeadlockDetector) findCycle(start TxnID) []TxnID {
	// visited: set of nodes already fully processed (no cycle through them).
	visited := make(map[TxnID]bool)
	// path: current DFS path stack.
	path := make([]TxnID, 0, 8)
	// inPath: fast membership check for path.
	inPath := make(map[TxnID]bool)

	var dfs func(node TxnID) []TxnID
	dfs = func(node TxnID) []TxnID {
		if visited[node] {
			return nil
		}
		if inPath[node] {
			// Found the start of the cycle — return all nodes from this node forward in path.
			cycleStart := -1
			for i, n := range path {
				if n == node {
					cycleStart = i
					break
				}
			}
			cycle := make([]TxnID, len(path)-cycleStart)
			copy(cycle, path[cycleStart:])
			return cycle
		}

		path = append(path, node)
		inPath[node] = true

		if next, ok := d.edges[node]; ok {
			if cycle := dfs(next); cycle != nil {
				return cycle
			}
		}

		path = path[:len(path)-1]
		inPath[node] = false
		visited[node] = true
		return nil
	}

	return dfs(start)
}

// CycleExists returns true if a cycle exists in the current waits-for graph.
// Primarily for testing.
func (d *DeadlockDetector) CycleExists() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for start := range d.edges {
		if d.findCycle(start) != nil {
			return true
		}
	}
	return false
}
