// Package buffer — pluggable eviction policy interface and implementations.
package buffer

import (
	"math"
	"sync/atomic"
)

// EvictionPolicy is the interface that buffer pool eviction strategies must implement.
type EvictionPolicy interface {
	// OnAccess is called whenever the frame at frameIdx is accessed (pinned).
	OnAccess(frameIdx int, frames []*Frame)
	// ChooseVictim returns the index of the best frame to evict, or -1 if all
	// frames are pinned and eviction is impossible.
	ChooseVictim(frames []*Frame) int
}

// ── Clock Policy ─────────────────────────────────────────────────────────────

// ClockPolicy implements the two-pass Clock (second-chance) eviction algorithm.
// This is the original eviction policy used by the buffer pool.
type ClockPolicy struct {
	hand int
}

// NewClockPolicy creates a Clock eviction policy.
func NewClockPolicy() *ClockPolicy { return &ClockPolicy{} }

func (p *ClockPolicy) OnAccess(frameIdx int, frames []*Frame) {
	frames[frameIdx].RefBit = true
}

// ChooseVictim scans frames using the Clock algorithm (two passes).
func (p *ClockPolicy) ChooseVictim(frames []*Frame) int {
	n := len(frames)
	for pass := 0; pass < 2; pass++ {
		for i := 0; i < n; i++ {
			p.hand = (p.hand + 1) % n
			f := frames[p.hand]
			if atomic.LoadInt32(&f.PinCount) > 0 {
				continue
			}
			if f.PageID == 0 { // empty frame — evict immediately
				return p.hand
			}
			if f.RefBit && pass == 0 {
				f.RefBit = false // second chance
				continue
			}
			return p.hand
		}
	}
	return -1 // all frames pinned
}

// ── LRU-K Policy ─────────────────────────────────────────────────────────────

// LRUKPolicy implements LRU-K (O'Neil et al., 1993) eviction.
// K=2 is recommended and is the default used in production DBMS systems.
//
// Each frame tracks the last K access timestamps.  Eviction priority:
//  1. Empty frames (PageID == 0) — evicted immediately.
//  2. "New" frames (fewer than K accesses) — evicted FIFO (oldest-loaded first)
//     to prevent one-time sequential scans from polluting the cache.
//  3. "Established" frames (≥ K accesses) — evict the one whose K-th most
//     recent access was longest ago (largest backward K-distance).
type LRUKPolicy struct {
	k    int
	tick atomic.Uint64 // logical clock; incremented on each access
}

// NewLRUKPolicy creates an LRU-K policy with the given K (typically 2).
func NewLRUKPolicy(k int) *LRUKPolicy {
	if k < 1 {
		k = 2
	}
	return &LRUKPolicy{k: k}
}

// OnAccess records an access for the frame at frameIdx.
func (p *LRUKPolicy) OnAccess(frameIdx int, frames []*Frame) {
	t := p.tick.Add(1)
	f := frames[frameIdx]
	// Shift the history ring: move [1] → [0] (discard the oldest), record new time at [1].
	if f.accessCount >= 1 {
		f.accessHist[0] = f.accessHist[1]
	}
	f.accessHist[1] = t
	f.accessCount++
}

// ChooseVictim selects the frame to evict according to LRU-K(2) priority rules.
func (p *LRUKPolicy) ChooseVictim(frames []*Frame) int {
	now := p.tick.Load()

	// Track the best "new" candidate (FIFO among new pages).
	fifoIdx := -1
	fifoFirst := uint64(math.MaxUint64)

	// Track the best "established" candidate (largest backward K-distance).
	estIdx := -1
	estDist := uint64(0)

	for i, f := range frames {
		if atomic.LoadInt32(&f.PinCount) > 0 {
			continue
		}
		// Empty frame — highest priority victim.
		if f.PageID == 0 {
			return i
		}
		if f.accessCount < uint64(p.k) {
			// New page: FIFO priority based on first access time.
			first := f.accessHist[1] // only one access recorded in [1]
			if f.accessCount == 0 {
				first = 0
			}
			if first < fifoFirst {
				fifoFirst = first
				fifoIdx = i
			}
		} else {
			// Established page: backward K-distance = now - K-th most recent access.
			kthTime := f.accessHist[0] // for K=2, index 0 is the 2nd most recent
			dist := now - kthTime
			if dist > estDist {
				estDist = dist
				estIdx = i
			}
		}
	}

	// Prefer new pages over established pages (prevents cache pollution).
	if fifoIdx >= 0 {
		return fifoIdx
	}
	return estIdx // may be -1 if all frames are pinned
}
