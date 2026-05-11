package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"btree-engine/internal/events"
)

// ErrLogCorrupt is returned when a scan encounters an unrecoverable record.
var ErrLogCorrupt = errors.New("WAL log corrupt")

// WALManager manages the write-ahead log using segment files.
// LSNs are absolute byte offsets that span across all segments.
// Segment N covers bytes [startLSN, endLSN); the active segment has endLSN == 0.
type WALManager struct {
	mu           sync.Mutex
	basePath     string
	catalog      *segmentCatalog
	currentFile  *os.File
	segmentStart uint64  // absolute LSN where the current segment file starts
	buffer       []byte
	bufferSize   int
	currentLSN   uint64 // absolute byte offset of next record to write
	flushedLSN   atomic.Uint64
	syncOnCommit bool
	maxSegSize   int64  // rotate when current segment exceeds this (0 = no rotation)
	archiveCmd   string // shell command after rotation; "" = delete
	bus          *events.EventBus

	// Group commit fields (P3-001/P3-002).
	// gcMu protects gcLeader and gcEpoch.
	// gcCond is broadcast after each group flush.
	gcMu    sync.Mutex
	gcCond  *sync.Cond
	gcLeader bool   // true when a goroutine is sleeping before the batch flush
	gcEpoch  uint64 // incremented after each group flush
}

// Open opens (or creates) a segmented WAL at basePath.
// maxSegmentSize 0 disables rotation. archiveCommand "" means delete on truncation.
func Open(basePath string, bufferSize int, syncOnCommit bool, maxSegmentSize int64, archiveCommand string, bus *events.EventBus) (*WALManager, error) {
	cat, err := loadOrCreateCatalog(basePath)
	if err != nil {
		return nil, err
	}

	cur := cat.currentSegment()
	f, err := os.OpenFile(cur.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	currentLSN := cur.StartLSN + uint64(info.Size())

	w := &WALManager{
		basePath:     basePath,
		catalog:      cat,
		currentFile:  f,
		segmentStart: cur.StartLSN,
		buffer:       make([]byte, 0, bufferSize),
		bufferSize:   bufferSize,
		currentLSN:   currentLSN,
		syncOnCommit: syncOnCommit,
		maxSegSize:   maxSegmentSize,
		archiveCmd:   archiveCommand,
		bus:          bus,
	}
	w.flushedLSN.Store(currentLSN)
	w.gcCond = sync.NewCond(&w.gcMu)
	return w, nil
}

// Close flushes the buffer and closes the current segment file.
func (w *WALManager) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.flushLocked(); err != nil {
		return err
	}
	return w.currentFile.Close()
}

// CrashClose closes the WAL file without flushing the in-memory buffer.
// Simulates process death where buffered log records are lost.
func (w *WALManager) CrashClose() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buffer = w.buffer[:0]
	return w.currentFile.Close()
}

// AppendRecord assigns an LSN, encodes, and appends a record to the in-memory buffer.
// Returns the assigned LSN.
func (w *WALManager) AppendRecord(r LogRecord) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	encoded := EncodeLogRecord(&r)

	// Rotate to a new segment if the current segment would exceed maxSegSize.
	if w.maxSegSize > 0 {
		segSize := int64(w.currentLSN-w.segmentStart) + int64(len(w.buffer)) + int64(len(encoded))
		if segSize > w.maxSegSize {
			if err := w.rotateSegmentLocked(); err != nil {
				slog.Error("wal segment rotation failed", "err", err)
				// Continue writing to existing segment rather than losing data.
			}
		}
	}

	r.LSN = w.currentLSN
	encoded = EncodeLogRecord(&r)
	w.buffer = append(w.buffer, encoded...)
	w.currentLSN += uint64(len(encoded))

	if w.bus != nil {
		w.bus.Publish(events.Event{Type: events.EvtWALAppend,
			Extra: map[string]interface{}{
				"lsn":    r.LSN,
				"txn_id": r.TxnID,
				"type":   r.Type,
			}})
	}

	// Auto-flush if buffer is full.
	if len(w.buffer) >= w.bufferSize {
		_ = w.flushLocked()
	}

	return r.LSN
}

// rotateSegmentLocked closes the current segment and opens a new one.
// Must be called with w.mu held.
func (w *WALManager) rotateSegmentLocked() error {
	if err := w.flushLocked(); err != nil {
		return err
	}
	if w.syncOnCommit {
		if err := w.currentFile.Sync(); err != nil {
			return err
		}
	}
	oldPath := w.currentFile.Name()
	endLSN := w.currentLSN
	if err := w.currentFile.Close(); err != nil {
		return err
	}

	// Update catalog: mark old segment complete.
	w.catalog.mu.Lock()
	w.catalog.closeCurrentSegment(endLSN)
	seq := w.catalog.nextSeq()
	newPath := segmentPath(w.basePath, seq)
	newSeg := segmentInfo{Seq: seq, StartLSN: endLSN, Path: newPath}
	w.catalog.appendSegment(newSeg)
	if err := w.catalog.save(); err != nil {
		w.catalog.mu.Unlock()
		return err
	}
	w.catalog.mu.Unlock()

	// Open new segment file.
	f, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.currentFile = f
	w.segmentStart = endLSN

	slog.Info("wal segment rotated",
		"old_segment", oldPath,
		"new_segment", newPath,
		"end_lsn", endLSN)

	if w.bus != nil {
		w.bus.Publish(events.Event{Type: events.EvtWALRotated,
			Extra: map[string]interface{}{
				"old_path": oldPath,
				"new_path": newPath,
				"end_lsn":  endLSN,
			}})
	}
	return nil
}

// FlushUpTo ensures all records up to (and including) targetLSN are on disk.
func (w *WALManager) FlushUpTo(targetLSN uint64) error {
	if w.flushedLSN.Load() >= targetLSN {
		return nil // already flushed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

// flushLocked writes the in-memory buffer to disk. Must be called with w.mu held.
func (w *WALManager) flushLocked() error {
	if len(w.buffer) == 0 {
		return nil
	}
	if _, err := w.currentFile.Write(w.buffer); err != nil {
		return err
	}
	if w.syncOnCommit {
		if err := w.currentFile.Sync(); err != nil {
			return err
		}
	}
	w.buffer = w.buffer[:0]
	w.flushedLSN.Store(w.currentLSN)

	if w.bus != nil {
		w.bus.Publish(events.Event{Type: events.EvtWALFlush,
			Extra: map[string]interface{}{"flushed_lsn": w.currentLSN}})
	}
	return nil
}

// CurrentLSN returns the byte offset of the next record to be written.
func (w *WALManager) CurrentLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLSN
}

// FlushedLSN returns the byte offset up to which the log is durable.
func (w *WALManager) FlushedLSN() uint64 {
	return w.flushedLSN.Load()
}

// ActiveSegmentPath returns the path of the current (active) segment file.
func (w *WALManager) ActiveSegmentPath() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentFile.Name()
}

// WALSizeBytes returns the total size of all segment files on disk.
func (w *WALManager) WALSizeBytes() int64 {
	return w.catalog.TotalSize()
}

// SegmentCount returns the number of segment files currently tracked.
func (w *WALManager) SegmentCount() int {
	w.catalog.mu.Lock()
	defer w.catalog.mu.Unlock()
	return len(w.catalog.Segments)
}

// OldestLSN returns the oldest required LSN (start of the oldest segment).
func (w *WALManager) OldestLSN() uint64 {
	w.catalog.mu.Lock()
	defer w.catalog.mu.Unlock()
	if len(w.catalog.Segments) == 0 {
		return 0
	}
	return w.catalog.Segments[0].StartLSN
}

// Commit writes and flushes a LogCommit record. Blocks until fsync.
func (w *WALManager) Commit(txnID, prevLSN uint64) uint64 {
	lsn := w.AppendRecord(LogRecord{Type: LogCommit, TxnID: txnID, PrevLSN: prevLSN})
	_ = w.FlushUpTo(lsn)
	return lsn
}

// CommitGrouped writes a LogCommit record and batches the fsync with other
// concurrent commits within the given delay window.  When delay == 0 it
// behaves identically to Commit (immediate fsync per commit).
//
// Algorithm (leader-follower):
//  1. One goroutine becomes the "leader" and sleeps for `delay`.
//  2. All concurrent callers ("followers") wait on a condition variable.
//  3. After the sleep the leader flushes once covering all buffered records,
//     then broadcasts to wake the followers.
//  4. Any follower whose LSN is not yet flushed (rare edge case: arrived after
//     the flush) retries by becoming the next leader.
func (w *WALManager) CommitGrouped(txnID, prevLSN uint64, delay time.Duration) uint64 {
	lsn := w.AppendRecord(LogRecord{Type: LogCommit, TxnID: txnID, PrevLSN: prevLSN})

	if delay <= 0 {
		_ = w.FlushUpTo(lsn)
		return lsn
	}

	for w.flushedLSN.Load() < lsn {
		w.gcMu.Lock()
		if w.flushedLSN.Load() >= lsn {
			w.gcMu.Unlock()
			break
		}

		if !w.gcLeader {
			// Become the leader for this flush batch.
			w.gcLeader = true
			startEpoch := w.gcEpoch
			w.gcMu.Unlock()

			time.Sleep(delay)

			// Flush all records buffered up to now.
			w.mu.Lock()
			_ = w.flushLocked()
			w.mu.Unlock()

			// Notify followers.
			w.gcMu.Lock()
			w.gcLeader = false
			w.gcEpoch = startEpoch + 1
			w.gcCond.Broadcast()
			w.gcMu.Unlock()
		} else {
			// Follow: wait for the current leader to complete its flush.
			epoch := w.gcEpoch
			for w.gcEpoch == epoch && w.flushedLSN.Load() < lsn {
				w.gcCond.Wait()
			}
			w.gcMu.Unlock()
		}
	}
	return lsn
}

// Abort writes and flushes a LogAbort record.
func (w *WALManager) Abort(txnID, prevLSN uint64) uint64 {
	lsn := w.AppendRecord(LogRecord{Type: LogAbort, TxnID: txnID, PrevLSN: prevLSN})
	_ = w.FlushUpTo(lsn)
	return lsn
}

// LogBeginTxn writes a LogBegin record.
func (w *WALManager) LogBeginTxn(txnID uint64) uint64 {
	return w.AppendRecord(LogRecord{Type: LogBegin, TxnID: txnID})
}

// LogInsertRecord writes a LogInsert record with the encoded tuple as payload.
func (w *WALManager) LogInsertRecord(txnID, pageID, prevLSN uint64, tupleData []byte) uint64 {
	return w.AppendRecord(LogRecord{
		Type:    LogInsert,
		TxnID:   txnID,
		PageID:  uint32(pageID),
		PrevLSN: prevLSN,
		Payload: tupleData,
	})
}

// LogDeleteRecord writes a LogDelete record.
func (w *WALManager) LogDeleteRecord(txnID, pageID, prevLSN uint64, slotIdx int, oldXmax uint64) uint64 {
	payload := make([]byte, 4+8)
	binary.LittleEndian.PutUint32(payload[0:], uint32(slotIdx))
	binary.LittleEndian.PutUint64(payload[4:], oldXmax)
	return w.AppendRecord(LogRecord{
		Type:    LogDelete,
		TxnID:   txnID,
		PageID:  uint32(pageID),
		PrevLSN: prevLSN,
		Payload: payload,
	})
}

// LogSplitRecord writes a LogSplit record.
// Payload: [rightPageID:4][newTupleLen:4][newTuple]
func (w *WALManager) LogSplitRecord(txnID, leftPageID, rightPageID, prevLSN uint64, separatorKey []byte, newTuple []byte) uint64 {
	payload := make([]byte, 4+4+len(newTuple))
	binary.LittleEndian.PutUint32(payload[0:], uint32(rightPageID))
	binary.LittleEndian.PutUint32(payload[4:], uint32(len(newTuple)))
	copy(payload[8:], newTuple)
	return w.AppendRecord(LogRecord{
		Type:    LogSplit,
		TxnID:   txnID,
		PageID:  uint32(leftPageID),
		PrevLSN: prevLSN,
		Payload: payload,
	})
}

// LogMergeRecord writes a LogMerge record containing full after-images of the surviving
// and parent pages. This enables crash-recovery via after-image redo.
func (w *WALManager) LogMergeRecord(txnID uint64, survivingPageID, freedPageID, parentPageID uint32, prevLSN uint64, survivingAfterImage, parentAfterImage []byte) uint64 {
	payload := make([]byte, 4+4+len(survivingAfterImage)+len(parentAfterImage))
	binary.LittleEndian.PutUint32(payload[0:], freedPageID)
	binary.LittleEndian.PutUint32(payload[4:], parentPageID)
	copy(payload[8:], survivingAfterImage)
	copy(payload[8+len(survivingAfterImage):], parentAfterImage)
	return w.AppendRecord(LogRecord{
		Type:    LogMerge,
		TxnID:   txnID,
		PageID:  survivingPageID,
		PrevLSN: prevLSN,
		Payload: payload,
	})
}

// ScanFrom iterates all log records starting at startLSN across all relevant segments,
// calling fn for each valid record. Only segments that may contain startLSN or later
// records are opened.
func (w *WALManager) ScanFrom(startLSN uint64, fn func(LogRecord)) error {
	// Flush buffer first so the current segment file is up to date.
	w.mu.Lock()
	if err := w.flushLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.mu.Unlock()

	segments := w.catalog.segmentsContaining(startLSN)
	for _, seg := range segments {
		if err := scanSegment(seg, startLSN, fn); err != nil {
			return err
		}
	}
	return nil
}

// scanSegment scans a single segment file starting at startLSN.
func scanSegment(seg segmentInfo, startLSN uint64, fn func(LogRecord)) error {
	f, err := os.Open(seg.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // segment already deleted (truncated); skip
		}
		return err
	}
	defer func() { _ = f.Close() }()

	// Seek to the position within this segment corresponding to startLSN.
	var seekOffset int64
	if startLSN > seg.StartLSN {
		seekOffset = int64(startLSN - seg.StartLSN)
	}
	if _, err := f.Seek(seekOffset, io.SeekStart); err != nil {
		return err
	}

	header := make([]byte, LogRecordFixedSize)
	for {
		if _, err := io.ReadFull(f, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		payloadLen := int(binary.LittleEndian.Uint32(header[29:]))
		full := make([]byte, LogRecordFixedSize+payloadLen)
		copy(full, header)
		if _, err := io.ReadFull(f, full[LogRecordFixedSize:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		r, err := DecodeLogRecord(full)
		if err != nil {
			continue // skip corrupt record
		}
		// Adjust the LSN to be absolute (LSN stored in record is relative to segment start).
		// Actually, LSNs are already absolute in our encoding — the record's LSN field
		// stores the absolute offset at time of write. No adjustment needed.
		fn(*r)
	}
	return nil
}

// FetchRecord reads and decodes the single record at the given absolute LSN.
func (w *WALManager) FetchRecord(lsn uint64) (*LogRecord, error) {
	w.mu.Lock()
	if err := w.flushLocked(); err != nil {
		w.mu.Unlock()
		return nil, err
	}
	w.mu.Unlock()

	seg, ok := w.catalog.segmentContaining(lsn)
	if !ok {
		return nil, errors.New("LSN not found in any segment")
	}

	f, err := os.Open(seg.Path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	seekOffset := int64(lsn - seg.StartLSN)
	if _, err := f.Seek(seekOffset, io.SeekStart); err != nil {
		return nil, err
	}
	header := make([]byte, LogRecordFixedSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}
	payloadLen := int(binary.LittleEndian.Uint32(header[29:]))
	full := make([]byte, LogRecordFixedSize+payloadLen)
	copy(full, header)
	if _, err := io.ReadFull(f, full[LogRecordFixedSize:]); err != nil {
		return nil, err
	}
	return DecodeLogRecord(full)
}

// LogRootChangeRecord writes a LogRootChange record with full new-root structure.
// Payload: [newRootID:4][leftChildID:4][rightChildID:4][sepKeyLen:2][sepKey]
func (w *WALManager) LogRootChangeRecord(newRootID, leftChildID, rightChildID uint32, separatorKey []byte, prevLSN uint64) uint64 {
	payload := make([]byte, 4+4+4+2+len(separatorKey))
	binary.LittleEndian.PutUint32(payload[0:], newRootID)
	binary.LittleEndian.PutUint32(payload[4:], leftChildID)
	binary.LittleEndian.PutUint32(payload[8:], rightChildID)
	binary.LittleEndian.PutUint16(payload[12:], uint16(len(separatorKey)))
	copy(payload[14:], separatorKey)
	lsn := w.AppendRecord(LogRecord{Type: LogRootChange, TxnID: 0, PrevLSN: prevLSN, Payload: payload})
	_ = w.FlushUpTo(lsn) // persist immediately
	return lsn
}

// FindLastCheckpoint scans the log from the beginning and returns the LSN of the last
// LogCheckpoint record, or 0 if none found.
func (w *WALManager) FindLastCheckpoint() uint64 {
	var lastCkpt uint64
	_ = w.ScanFrom(0, func(r LogRecord) {
		if r.Type == LogCheckpoint {
			lastCkpt = r.LSN
		}
	})
	return lastCkpt
}

// TruncateUpTo removes WAL segments whose content is entirely before truncateLSN.
// Segments with EndLSN <= truncateLSN are deleted or archived.
func (w *WALManager) TruncateUpTo(truncateLSN uint64) {
	if truncateLSN == 0 {
		return
	}
	removed := w.catalog.truncateUpTo(w.basePath, truncateLSN, w.archiveCmd)
	if removed > 0 {
		slog.Info("wal segments truncated",
			"truncate_lsn", truncateLSN,
			"segments_removed", removed)
	}
}

// runArchiveHook executes the archive command, substituting %s with the segment path.
func runArchiveHook(cmd, path string) {
	expanded := strings.ReplaceAll(cmd, "%s", path)
	// Use os/exec via a helper to avoid import cycle issues.
	archiveExec(expanded)
}
