# P0-008 — Structured Logging with slog

**Priority:** P0 (Critical)
**Phase:** 0-C
**Effort:** S (2-3 days)
**Depends on:** none
**Blocks:** P1-001 (metrics collector subscribes to log events)

---

## Problem Statement

The engine has zero structured logging. Production diagnosis requires:
- Redeploying with added print statements
- Correlating events across subsystems is impossible without transaction IDs / LSNs
- Log aggregation tools (Datadog, Splunk, CloudWatch) cannot parse unstructured output
- No log level control — debug noise vs. missing critical errors

---

## Goals

1. All production code paths log via `slog` structured logger
2. Log levels: DEBUG, INFO, WARN, ERROR with configurable minimum (default INFO)
3. Every log entry carries relevant context: `page_id`, `txn_id`, `lsn`, `op`, `duration_ms`
4. JSON output for production; colored text for development
5. Zero performance regression on hot paths (log calls below min level are ~5ns)

---

## Technical Design

### New File: `internal/logging/logger.go`

```go
package logging

import (
    "log/slog"
    "os"
)

// L is the global engine logger. Call Init() before use.
var L *slog.Logger = slog.Default()

// Init configures the global logger. Call once at startup.
func Init(level string, format string) {
    var lvl slog.Level
    switch level {
    case "debug":
        lvl = slog.LevelDebug
    case "warn":
        lvl = slog.LevelWarn
    case "error":
        lvl = slog.LevelError
    default:
        lvl = slog.LevelInfo
    }

    opts := &slog.HandlerOptions{Level: lvl}
    var handler slog.Handler
    if format == "json" {
        handler = slog.NewJSONHandler(os.Stdout, opts)
    } else {
        handler = slog.NewTextHandler(os.Stdout, opts)
    }
    L = slog.New(handler)
    slog.SetDefault(L)
}
```

### Config Changes: `internal/engine/config.go`

```go
type Config struct {
    // ... existing fields ...
    LogLevel  string `yaml:"log_level"`   // "debug" | "info" | "warn" | "error"
    LogFormat string `yaml:"log_format"`  // "json" | "text"
}
```

### `config.yaml` additions

```yaml
log_level: "info"     # debug in development
log_format: "text"    # json in production
```

---

## Log Call Sites by Subsystem

### `internal/buffer/pool.go` — buffer pool

```go
// On cache miss eviction (DEBUG — high frequency)
slog.Debug("evicting page", "page_id", frame.PageID, "dirty", frame.Dirty)

// On checksum mismatch (ERROR — should never happen post-fix)
slog.Error("page checksum mismatch on load",
    "page_id", pageID, "stored", stored, "computed", computed)

// On dirty page write during eviction (DEBUG)
slog.Debug("flushing dirty page on evict",
    "page_id", frame.PageID, "rec_lsn", frame.recLSN)
```

### `internal/wal/wal.go` — WAL manager

```go
// On commit flush (DEBUG)
slog.Debug("wal flush", "lsn", lsn, "flushed_bytes", n, "duration_ms", dur.Milliseconds())

// On checkpoint (INFO)
slog.Info("checkpoint written",
    "lsn", lsn, "dpt_size", dptSize, "att_size", attSize, "duration_ms", dur.Milliseconds())

// On WAL rotation (INFO)
slog.Info("wal segment rotated",
    "old_segment", oldPath, "new_segment", newPath, "size_bytes", segSize)
```

### `internal/recovery/aries.go` — recovery

```go
// Recovery start (INFO)
slog.Info("aries recovery starting", "wal_start_lsn", startLSN)

// Analysis phase complete (INFO)
slog.Info("aries analysis complete",
    "dpt_entries", len(dpt), "att_entries", len(att), "duration_ms", dur.Milliseconds())

// Redo phase progress (DEBUG)
slog.Debug("redo applying record",
    "lsn", record.LSN, "type", record.Type, "page_id", record.PageID)

// Recovery complete (INFO)
slog.Info("aries recovery complete",
    "redo_records", redoCount, "undo_records", undoCount,
    "total_duration_ms", dur.Milliseconds())
```

### `internal/btree/tree.go` — B+Tree

```go
// Write conflict (INFO — visible in logs for debugging)
slog.Info("write-write conflict detected",
    "key", key, "txn_id", txn.ID, "conflicting_xmax", oldTuple.Xmax)

// Split (DEBUG)
slog.Debug("leaf split", "page_id", leafPageID, "new_page_id", rightID,
    "separator_key", separatorKey)

// Merge (DEBUG)
slog.Debug("leaf merge", "surviving_page", survivingID, "freed_page", freedID,
    "parent_page", parentID)
```

### `internal/mvcc/vacuum.go` — vacuum

```go
// Vacuum run start (INFO)
slog.Info("vacuum run starting", "oldest_active_txn", oldestActive)

// Per-page compaction (DEBUG)
slog.Debug("vacuum compact page",
    "page_id", pageID, "dead_tuples", deadCount, "live_tuples", liveCount)

// Vacuum run complete (INFO)
slog.Info("vacuum run complete",
    "pages_scanned", pagesScanned, "dead_tuples_removed", deadRemoved,
    "pages_compacted", pagesCompacted, "duration_ms", dur.Milliseconds())
```

### `internal/engine/engine.go` — engine lifecycle

```go
// Open (INFO)
slog.Info("engine opening", "data_file", cfg.DataFile, "wal_file", cfg.WALFile)

// Recovery complete (INFO) — already covered by aries.go
// Close (INFO)
slog.Info("engine closing gracefully",
    "dirty_pages", dpt, "active_txns", activeTxns)

// Crash simulation (WARN)
slog.Warn("engine crash-close invoked (test/admin only)")
```

### `gateway/rest.go` — HTTP layer

```go
// Auth failure (WARN)
slog.Warn("auth failure", "path", r.URL.Path, "method", r.Method, "remote", r.RemoteAddr)

// Write conflict returned to caller (INFO)
slog.Info("write conflict",
    "path", r.URL.Path, "txn_id", txnID, "key", key)

// Internal error (ERROR)
slog.Error("internal error", "path", r.URL.Path, "err", err)
```

---

## What NOT to Log at INFO

The following are DEBUG-only to avoid log flooding:
- Individual page reads/writes (every `FetchPage`, `WritePage`)
- Individual WAL record appends (except commit/checkpoint)
- Individual visibility checks
- Binary search iterations

**Rule of thumb:** If it happens > 1000x per second under normal load, it is DEBUG or not logged at all.

---

## Performance Notes

- `slog.Debug(...)` at INFO level: single integer comparison → ~5ns (< 1% overhead at 1M ops/sec)
- `slog.Info(...)` with 4 attributes to text handler: ~200ns
- `slog.Info(...)` with 4 attributes to JSON handler: ~300ns
- Do NOT call `slog.Debug(...)` with computed arguments unless guarded:
  ```go
  // BAD: string formatting always happens
  slog.Debug("data: " + hex.EncodeToString(data))

  // GOOD: formatting only when DEBUG enabled
  if slog.Default().Enabled(ctx, slog.LevelDebug) {
      slog.Debug("data", "hex", hex.EncodeToString(data))
  }
  ```

---

## Testing Plan

### Unit Tests

```go
func TestLogging_JSONOutput(t *testing.T) {
    // Set up JSON logger writing to buffer
    // Trigger a log event (e.g., checksum mismatch)
    // Parse JSON output, verify all expected fields present
}

func TestLogging_LevelFilter(t *testing.T) {
    // Set log level to WARN
    // Call slog.Debug(...) — verify nothing written to buffer
    // Call slog.Warn(...)  — verify written to buffer
}
```

---

## Rollout Strategy

1. Add `logging` package first (no callers yet) — zero risk
2. Add config fields and `logging.Init()` call at startup
3. Replace `fmt.Printf` in non-hot paths first (recovery, vacuum, lifecycle)
4. Replace in hot paths last (buffer pool, WAL)
5. Run full test suite + race detector after each step

---

## Definition of Done

- [ ] `internal/logging/logger.go` with `Init()` and global `L`
- [ ] `config.yaml` has `log_level` and `log_format` fields
- [ ] Zero `fmt.Printf` / `fmt.Fprintf(os.Stderr, ...)` in non-test production code
- [ ] All subsystems log via `slog` at appropriate levels
- [ ] Unit test verifies JSON output format
- [ ] Unit test verifies level filtering
- [ ] No performance regression in benchmarks (< 1% overhead at INFO level)
