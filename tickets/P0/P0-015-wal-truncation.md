# P0-015 — WAL Truncation After Checkpoint

**Priority:** P0 (Critical)
**Phase:** 0-E
**Effort:** L (2 weeks)
**Depends on:** P1-009 (background checkpoint)
**Blocks:** P0-016 (WAL rotation), P5-008 (incremental backup)

---

## Problem Statement

The WAL file (`data/wal.log`) grows without bound. Every committed transaction appends at least 3 records (BEGIN, op, COMMIT). At 100 commits/second for 24 hours, the WAL contains ~26M records. At 50 bytes average record size, this is ~1.3 GB per day.

After a successful checkpoint:
- All dirty pages have been flushed to disk
- The checkpoint record contains the full DPT and ATT
- All WAL records before `min(DPT.recLSN)` are **provably unnecessary** for recovery
- They waste disk space and make `WALTail()` O(WAL size) instead of O(N)

Additionally, `engine.go:WALTail()` calls `walMgr.ScanFrom(0, ...)` — scanning from byte 0 on every invocation. This is O(WAL size) and will become a latency spike as the file grows.

---

## Goals

1. After each checkpoint, truncate WAL records before `min(DPT.recLSN)` OR before the checkpoint record LSN (whichever is smaller)
2. Truncation must be crash-safe: the data file must be consistent before truncating the WAL
3. `WALTail(n)` must not scan from byte 0; it must seek to the last N records efficiently
4. WAL size stabilises under steady-state operation

---

## Technical Design

### Approach: Segment-Based WAL (Recommended)

Rather than in-place truncation of a single file (which requires `ftruncate` from the head — not POSIX-portable and requires special handling), use numbered segment files.

**File layout after this change:**
```
data/
├── btree.db
├── wal.catalog.json      ← segment catalog
├── wal_000001.log        ← archived segment
├── wal_000002.log        ← archived segment
└── wal_000003.log        ← current active segment (append-only)
```

**Catalog format (`wal.catalog.json`):**
```json
{
  "segments": [
    {"seq": 1, "start_lsn": 0,       "end_lsn": 102400, "path": "wal_000001.log"},
    {"seq": 2, "start_lsn": 102400,  "end_lsn": 204800, "path": "wal_000002.log"},
    {"seq": 3, "start_lsn": 204800,  "end_lsn": null,   "path": "wal_000003.log"}
  ],
  "oldest_required_lsn": 156000
}
```

`oldest_required_lsn` is updated after each checkpoint to `min(DPT.recLSN, checkpointLSN)`.

### Truncation Algorithm

After `WriteCheckpoint()` returns:

```
truncateLSN = min(checkpointLSN, minDPTRecLSN)

for each segment in catalog where segment.endLSN <= truncateLSN:
    call archiveHook(segment.path)  // default: delete; configurable: copy to backup
    os.Remove(segment.path)
    remove from catalog

catalog.oldest_required_lsn = truncateLSN
write catalog atomically (temp file + rename)
```

**Crash safety:**
- The catalog rename is atomic on POSIX systems
- If crash occurs after page flush but before catalog write: next recovery re-reads old catalog, scans more WAL records than needed (harmless)
- If crash occurs after catalog write but before segment deletion: orphan segment exists; cleaned up on next truncation cycle (harmless)
- The page data on disk is already consistent before any WAL deletion

### Segment Rotation

A segment is "rotated" (closed and a new one opened) when it exceeds `max_segment_size`:

```go
func (w *WALManager) appendRecord(record []byte) {
    if w.currentSegmentSize + len(record) > w.maxSegmentSize {
        w.rotateSegment()
    }
    // ... append to current segment ...
}
```

`rotateSegment()`:
1. `fsync` + close current segment file
2. Assign new sequence number
3. Open new file
4. Update catalog (atomic rename)
5. Publish `EvtWALRotated` event

### `ScanFrom(lsn, fn)` Fix

Current implementation scans from byte 0. After segmentation:

```go
func (w *WALManager) ScanFrom(lsn uint64, fn func(LogRecord)) error {
    segs := w.catalog.segmentsContaining(lsn)  // O(segments) — usually 1-3
    for _, seg := range segs {
        f, _ := os.Open(seg.path)
        // seek to offset within segment for the given LSN
        seekOffset := lsn - seg.startLSN
        f.Seek(int64(seekOffset), io.SeekStart)
        // ... scan records ...
    }
}
```

`WALTail(n)` can be further optimised to scan from the end of the current segment backward, but the segment approach already limits scan to one or two files.

### Config Changes

```yaml
wal:
  log_file: "./data/wal"           # base path; segments become wal_000001.log etc.
  max_segment_size: 268435456      # 256 MB per segment
  archive_command: ""              # "" = delete; "cp %s /backup/wal/" = copy
  buffer_size: 65536
  sync_on_commit: true
```

---

## API Changes

`GET /api/v1/engine/stats` — add fields:
```json
{
  "wal_size_bytes": 12345678,
  "wal_segment_count": 3,
  "wal_oldest_lsn": 156000,
  "wal_current_lsn": 204900
}
```

---

## Database/Storage Changes

- New files: `data/wal_NNNNNN.log` (replaces `data/wal.log`)
- New file: `data/wal.catalog.json`
- Migration: on first startup with new code, rename existing `wal.log` → `wal_000001.log` and create catalog

---

## Infrastructure Implications

- Any backup procedure that copies `data/wal.log` must be updated to copy all `data/wal_*.log` files
- Archive hook enables integration with S3/GCS: `archive_command: "aws s3 cp %s s3://bucket/wal/"`

---

## Edge Cases & Failure Scenarios

| Scenario | Expected Behavior |
|----------|------------------|
| Crash during segment rotation | Old segment still valid; catalog unchanged; next startup uses old segment |
| Archive hook fails | Log ERROR, skip truncation for this segment, retry next checkpoint |
| All segments needed for recovery | No segments deleted; log WARN "cannot truncate: all segments needed" |
| Manual WAL file deletion | Startup detects missing segment, refuses to start with clear error |
| `WALTail()` spans segment boundary | `ScanFrom` handles multi-segment scan transparently |

---

## Testing Plan

### Unit Tests

```go
func TestWAL_SegmentRotation(t *testing.T)         // rotation at size limit
func TestWAL_CatalogAtomicUpdate(t *testing.T)     // catalog rename atomicity
func TestWAL_TruncationAfterCheckpoint(t *testing.T) // segments deleted after checkpoint
func TestWAL_ScanFromAcrossSegments(t *testing.T)  // scan spanning 2 segments
func TestWAL_ArchiveHookCalled(t *testing.T)       // hook invoked on rotation
```

### Integration Tests

```go
func TestWAL_SizeStabilises(t *testing.T) {
    // Write 10K records, checkpoint, write 10K more, checkpoint
    // Verify WAL file size does not grow beyond ~2x segment size
}

func TestRecovery_AfterTruncation(t *testing.T) {
    // Truncate WAL up to LSN X, then crash after X, recover
    // Verify all data committed before truncation is present
}
```

---

## Rollout Strategy

1. Implement catalog + segment naming (backward compatible: just rename existing file)
2. Implement `ScanFrom` using catalog
3. Implement rotation
4. Implement truncation (add to `WriteCheckpoint` flow)
5. Test with crash-recovery integration tests
6. Deploy with `archive_command: ""` (delete mode — safe default)
7. Monitor WAL size metric stabilises

---

## Monitoring Requirements

- `btree_wal_size_bytes` gauge
- `btree_wal_segment_count` gauge
- `btree_wal_truncations_total` counter
- WARN log when segments cannot be truncated (all needed for recovery)

---

## Definition of Done

- [ ] WAL uses segment files with catalog
- [ ] Rotation occurs at `max_segment_size`
- [ ] Truncation runs after every checkpoint
- [ ] Archive hook configurable and tested
- [ ] `ScanFrom` does not scan from byte 0
- [ ] WAL size gauge in `/metrics`
- [ ] Migration from single-file WAL handled at startup
- [ ] All recovery integration tests pass after truncation
- [ ] `WALTail(50)` latency < 1ms even when historical WAL would be 10 GB
