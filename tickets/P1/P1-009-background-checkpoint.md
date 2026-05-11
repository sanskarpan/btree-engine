# P1-009 to P1-011 — Background Checkpoint Scheduling

**Priority:** P1 (High)
**Phase:** 1-B
**Effort:** S (1-2 days)
**Depends on:** P0-012 (graceful shutdown, needed to cleanly stop the background goroutine)
**Blocks:** P0-015 (WAL truncation runs after checkpoint)

---

## Problem Statement

Checkpoints are currently only triggered:
1. Manually via `POST /api/v1/wal/checkpoint`
2. On clean `engine.Close()`

If neither happens, the WAL grows without bound. More critically, if the engine crashes after running for hours without a checkpoint, ARIES recovery must replay the **entire WAL from the beginning** (or from the startup checkpoint). At 1000 commits/sec, a 1-hour WAL contains ~10M records, making recovery take 10+ seconds.

**Target:** Recovery time bounded by `checkpoint_interval × write_rate`. At 30s interval and 1000 commits/sec = 30K records to replay ≈ 30ms recovery time.

---

## Goals

1. Background goroutine fires `engine.Checkpoint()` every `checkpoint_interval`
2. Checkpoint skipped if no modifications since last checkpoint (idempotent)
3. Goroutine cleanly stopped on `engine.Close()`
4. Checkpoint timing metrics available

---

## Technical Design

### Changes to `internal/engine/engine.go`

```go
type StorageEngine struct {
    // existing fields ...
    checkpointTicker *time.Ticker
    checkpointDone   chan struct{}
    lastCheckpointLSN atomic.Uint64  // LSN at last checkpoint
    lastCheckpointAt  atomic.Int64   // Unix nanoseconds
}

func OpenEngine(cfg Config) (*StorageEngine, error) {
    // ... existing setup ...

    if cfg.CheckpointInterval > 0 {
        eng.startBackgroundCheckpoint(cfg.CheckpointInterval)
    }
    return eng, nil
}

func (e *StorageEngine) startBackgroundCheckpoint(interval time.Duration) {
    e.checkpointTicker = time.NewTicker(interval)
    e.checkpointDone = make(chan struct{})

    go func() {
        for {
            select {
            case <-e.checkpointTicker.C:
                e.maybeCheckpoint()
            case <-e.checkpointDone:
                return
            }
        }
    }()
}

func (e *StorageEngine) maybeCheckpoint() {
    // Skip if no dirty pages since last checkpoint
    dpt := e.bufPool.DirtyPageTable()
    if len(dpt) == 0 {
        return
    }

    start := time.Now()
    lsn, err := e.Checkpoint()
    if err != nil {
        slog.Error("background checkpoint failed", "err", err)
        return
    }
    e.lastCheckpointLSN.Store(lsn)
    e.lastCheckpointAt.Store(time.Now().UnixNano())

    slog.Info("background checkpoint complete",
        "lsn", lsn,
        "dirty_pages_at_ckpt", len(dpt),
        "duration_ms", time.Since(start).Milliseconds())

    // Publish event for metrics collector
    if e.bus != nil {
        e.bus.Publish(events.Event{
            Type: events.EvtWALCheckpoint,
            Extra: map[string]interface{}{
                "lsn":          lsn,
                "dpt_size":     len(dpt),
                "duration_ns":  time.Since(start).Nanoseconds(),
            },
        })
    }
}

func (e *StorageEngine) Close() error {
    if e.crashed {
        return nil
    }
    // Stop background checkpoint before final flush
    if e.checkpointTicker != nil {
        e.checkpointTicker.Stop()
        close(e.checkpointDone)
    }
    // ... existing flush and close ...
    // Write a final checkpoint on clean close
    _, _ = e.Checkpoint()  // best-effort; ignore error on close
    // ... close WAL and disk ...
}
```

### Config Change

```go
type Config struct {
    // existing ...
    CheckpointInterval time.Duration `yaml:"checkpoint_interval"`
}
```

```yaml
recovery:
  checkpoint_interval: "30s"  # 0 = disabled (manual only)
```

### Checkpoint Age in Health Endpoint

```go
// gateway/rest.go — /health/ready

func (s *Server) handleHealthReady(w http.ResponseWriter, r *http.Request) {
    // ...
    lastCkptNano := s.eng.LastCheckpointAt()
    var ckptAgo float64
    if lastCkptNano > 0 {
        ckptAgo = time.Since(time.Unix(0, lastCkptNano)).Seconds()
    }
    json.NewEncoder(w).Encode(map[string]any{
        "status":                       "ready",
        "last_checkpoint_lsn":          s.eng.LastCheckpointLSN(),
        "last_checkpoint_ago_seconds":  ckptAgo,
        "dirty_pages":                  len(s.eng.bufPool.DirtyPageTable()),
        "active_txns":                  s.eng.tm.ActiveCount(),
    })
}
```

---

## Testing Plan

```go
func TestBackgroundCheckpoint_TriggersAfterInterval(t *testing.T) {
    cfg := testConfig()
    cfg.CheckpointInterval = 100 * time.Millisecond  // fast for test
    eng := openTestEngine(t, cfg)

    // Write some data
    txn := eng.Begin(mvcc.SnapshotIsolation)
    eng.Put(txn, []byte("key"), []byte("val"))
    eng.Commit(txn)

    // Wait for checkpoint to trigger
    time.Sleep(300 * time.Millisecond)
    assert.Greater(t, eng.LastCheckpointLSN(), uint64(0))
}

func TestBackgroundCheckpoint_SkipsWhenClean(t *testing.T) {
    cfg := testConfig()
    cfg.CheckpointInterval = 50 * time.Millisecond
    eng := openTestEngine(t, cfg)

    // No writes — dirty page table is empty
    time.Sleep(200 * time.Millisecond)

    // Checkpoint may fire, but with 0 dirty pages, no WAL write occurs
    // LSN should remain 0 or initial value
    assert.Equal(t, uint64(0), eng.LastCheckpointLSN())
}

func TestBackgroundCheckpoint_StopsOnClose(t *testing.T) {
    cfg := testConfig()
    cfg.CheckpointInterval = 50 * time.Millisecond
    eng := openTestEngine(t, cfg)
    eng.Close()
    // Verify no goroutine leak: runtime.NumGoroutine() should decrease
}

func TestBackgroundCheckpoint_BoundsRecoveryTime(t *testing.T) {
    cfg := testConfig()
    cfg.CheckpointInterval = 100 * time.Millisecond
    eng := openTestEngine(t, cfg)

    // Write 10000 records over 500ms (checkpoints fire 5 times)
    for i := 0; i < 10000; i++ {
        txn := eng.Begin(mvcc.SnapshotIsolation)
        eng.Put(txn, []byte(fmt.Sprintf("k%d", i)), []byte("v"))
        eng.Commit(txn)
    }

    // Crash close
    eng.CrashClose()

    // Reopen — measure recovery time
    start := time.Now()
    eng2 := openTestEngine(t, cfg)
    recoveryDuration := time.Since(start)

    // Recovery should be much less than replaying all 10K records
    assert.Less(t, recoveryDuration, 200*time.Millisecond,
        "recovery should be fast with recent checkpoint")
    eng2.Close()
}
```

---

## Definition of Done

- [ ] Background checkpoint goroutine starts with engine (when `checkpoint_interval > 0`)
- [ ] Checkpoint skipped when dirty page table is empty
- [ ] Goroutine cleanly stopped in `Close()` (no goroutine leak)
- [ ] Final checkpoint written on clean `Close()`
- [ ] `last_checkpoint_ago_seconds` in `/health/ready` response
- [ ] Checkpoint metrics published to event bus
- [ ] `TestBackgroundCheckpoint_BoundsRecoveryTime` passes
- [ ] Integration test: 30s checkpoint interval → recovery after 35s of writes takes < 2s
