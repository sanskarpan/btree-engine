package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// segmentInfo describes a single WAL segment file.
type segmentInfo struct {
	Seq      int    `json:"seq"`
	StartLSN uint64 `json:"start_lsn"`
	EndLSN   uint64 `json:"end_lsn"` // 0 = current active segment
	Path     string `json:"path"`
}

// catalogState is the serialised form of the segment catalog.
type catalogState struct {
	Segments          []segmentInfo `json:"segments"`
	OldestRequiredLSN uint64        `json:"oldest_required_lsn"`
}

// segmentCatalog manages the set of WAL segment files.
type segmentCatalog struct {
	mu   sync.Mutex
	path string // path to .catalog.json
	catalogState
}

// catalogPath returns the catalog JSON file path for basePath.
func catalogPath(basePath string) string {
	return basePath + ".catalog.json"
}

// segmentPath returns the segment file path for basePath and sequence number.
func segmentPath(basePath string, seq int) string {
	return fmt.Sprintf("%s_%06d.log", basePath, seq)
}

// loadOrCreateCatalog loads an existing catalog, or creates a new one for basePath.
// Migration: if an old single-file WAL exists at basePath (no catalog yet), it is
// registered as segment 1 in the new catalog.
func loadOrCreateCatalog(basePath string) (*segmentCatalog, error) {
	cp := catalogPath(basePath)
	data, err := os.ReadFile(cp)
	if err == nil {
		c := &segmentCatalog{path: cp}
		if err2 := json.Unmarshal(data, &c.catalogState); err2 != nil {
			return nil, fmt.Errorf("corrupt WAL catalog %s: %w", cp, err2)
		}
		return c, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	// No catalog yet. Check for a legacy single-file WAL at basePath.
	c := &segmentCatalog{path: cp}
	if _, statErr := os.Stat(basePath); statErr == nil {
		// Legacy file found — migrate: register it as segment 1.
		c.Segments = []segmentInfo{{
			Seq:      1,
			StartLSN: 0,
			Path:     basePath,
		}}
	} else {
		// Fresh start — create first segment.
		seg1 := segmentPath(basePath, 1)
		c.Segments = []segmentInfo{{
			Seq:      1,
			StartLSN: 0,
			Path:     seg1,
		}}
	}
	if err2 := c.save(); err2 != nil {
		return nil, err2
	}
	return c, nil
}

// save atomically writes the catalog to disk (temp file + rename).
func (c *segmentCatalog) save() error {
	data, err := json.MarshalIndent(c.catalogState, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}

// currentSegment returns the last (active) segment info (no lock — caller must hold c.mu).
func (c *segmentCatalog) currentSegment() segmentInfo {
	return c.Segments[len(c.Segments)-1]
}

// nextSeq returns the sequence number for the next segment.
func (c *segmentCatalog) nextSeq() int {
	return c.Segments[len(c.Segments)-1].Seq + 1
}

// closeCurrentSegment marks the current segment as complete (sets EndLSN).
// Must be called with c.mu held. Does not save.
func (c *segmentCatalog) closeCurrentSegment(endLSN uint64) {
	c.Segments[len(c.Segments)-1].EndLSN = endLSN
}

// appendSegment adds a new active segment. Must be called with c.mu held. Does not save.
func (c *segmentCatalog) appendSegment(info segmentInfo) {
	c.Segments = append(c.Segments, info)
}

// segmentsContaining returns segments that may contain records at or after startLSN,
// in order from oldest to newest.
func (c *segmentCatalog) segmentsContaining(startLSN uint64) []segmentInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []segmentInfo
	for _, seg := range c.Segments {
		// Include segment if:
		//  - it is active (EndLSN == 0), OR
		//  - its end is after startLSN
		// AND its start is <= startLSN (we don't skip segments that start at startLSN).
		if seg.EndLSN == 0 || seg.EndLSN > startLSN {
			result = append(result, seg)
		}
	}
	return result
}

// segmentContaining returns the segment that contains lsn exactly.
func (c *segmentCatalog) segmentContaining(lsn uint64) (segmentInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.Segments) - 1; i >= 0; i-- {
		seg := c.Segments[i]
		if seg.StartLSN <= lsn && (seg.EndLSN == 0 || lsn < seg.EndLSN) {
			return seg, true
		}
	}
	return segmentInfo{}, false
}

// truncateUpTo removes segments whose EndLSN <= truncateLSN.
// If archiveCmd is non-empty, it is executed for each segment before deletion.
// Returns the number of segments removed. Saves the catalog atomically.
func (c *segmentCatalog) truncateUpTo(_ string, truncateLSN uint64, archiveCmd string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0
	remaining := c.Segments[:0]
	for _, seg := range c.Segments {
		if seg.EndLSN > 0 && seg.EndLSN <= truncateLSN {
			// Run archive hook if configured.
			if archiveCmd != "" {
				runArchiveHook(archiveCmd, seg.Path)
			}
			// Remove the file; ignore error (orphan cleaned up on next cycle).
			_ = os.Remove(seg.Path)
			removed++
		} else {
			remaining = append(remaining, seg)
		}
	}
	if removed == 0 {
		return 0
	}
	c.Segments = remaining
	c.OldestRequiredLSN = truncateLSN
	_ = c.save()
	return removed
}

// TotalSize returns the sum of all segment file sizes on disk.
func (c *segmentCatalog) TotalSize() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total int64
	for _, seg := range c.Segments {
		if info, err := os.Stat(seg.Path); err == nil {
			total += info.Size()
		}
	}
	return total
}
