# P0-012 to P0-014 — Graceful Shutdown & Idle Transaction Timeout

**Priority:** P0 (Critical)
**Phase:** 0-D
**Effort:** S (2 days)
**Depends on:** none
**Blocks:** P4-001 (Docker health check requires ready/shutdown endpoints)

---

## Problem Statement

### Issue 1: No Signal Handler
`cmd/server/main.go` starts the HTTP server but does not register a SIGTERM/SIGINT handler. When a container orchestrator (Docker, Kubernetes) sends SIGTERM for a rolling restart, the Go runtime exits immediately with the default behavior: open file descriptors closed, in-flight page writes incomplete, WAL buffer unflushed.

The WAL exists precisely to handle this case (ARIES recovery on restart), but it adds unnecessary recovery time on every graceful restart. More critically, if a checkpoint was being written when SIGTERM arrived, the metadata page may be partially written.

### Issue 2: Transaction Leak
Clients that crash or disconnect without committing or aborting leave transactions open indefinitely. The `statusTable` map grows. Cursors hold page pins. Memory and pin slots are never reclaimed.

---

## Goals

1. SIGTERM/SIGINT → drain in-flight requests (max 30s) → `engine.Close()` → clean exit
2. `/health/ready` returns 503 during shutdown (load balancer stops routing)
3. Idle transactions auto-abort after configurable timeout (default 5 minutes)
4. Idle cursors auto-close after configurable timeout (default 2 minutes)

---

## Technical Design

### Signal Handler in `cmd/server/main.go`

```go
package main

import (
    "context"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

    "log/slog"
)

func main() {
    // ... existing setup ...

    srv := &http.Server{
        Addr:    fmt.Sprintf(":%d", cfg.Gateway.Port),
        Handler: mux,
    }

    // Channel to signal shutdown readiness to health endpoint
    shutdownCh := make(chan struct{})
    server.SetShutdownChan(shutdownCh)  // gateway/server.go: srv.isShuttingDown = true

    // Start server in background
    go func() {
        if err := srv.ListenAndServe(); err != http.ErrServerClosed {
            slog.Error("http server error", "err", err)
            os.Exit(1)
        }
    }()

    slog.Info("engine ready", "port", cfg.Gateway.Port)

    // Wait for termination signal
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
    <-quit

    slog.Info("shutdown signal received, draining requests")
    close(shutdownCh)  // health/ready starts returning 503

    // Drain in-flight requests (max 30 seconds)
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := srv.Shutdown(ctx); err != nil {
        slog.Warn("http server shutdown timed out", "err", err)
    }

    // Flush WAL, write checkpoint, close files
    slog.Info("closing engine")
    if err := eng.Close(); err != nil {
        slog.Error("engine close error", "err", err)
        os.Exit(1)
    }

    slog.Info("shutdown complete")
}
```

### Health Endpoint Update (`gateway/rest.go`)

```go
type Server struct {
    // existing fields ...
    isShuttingDown atomic.Bool
}

func (s *Server) handleHealthReady(w http.ResponseWriter, r *http.Request) {
    if s.isShuttingDown.Load() {
        w.WriteHeader(http.StatusServiceUnavailable)
        json.NewEncoder(w).Encode(map[string]string{"status": "shutting_down"})
        return
    }
    // ... existing ready check ...
    json.NewEncoder(w).Encode(map[string]any{
        "status":                    "ready",
        "wal_flushed_lsn":           walLSN,
        "dirty_pages":               dirtyPages,
        "active_txns":               activeTxns,
        "last_checkpoint_ago_secs":  checkpointAge,
    })
}
```

### Idle Transaction Timeout

Add to `gateway/server.go`:

```go
// TransactionTracker wraps active transactions with a last-used timestamp.
type TransactionTracker struct {
    mu      sync.Mutex
    txns    map[uint64]*TrackedTxn
    timeout time.Duration
}

type TrackedTxn struct {
    Txn      *mvcc.Transaction
    LastUsed time.Time
}

func (tt *TransactionTracker) Touch(id uint64) {
    tt.mu.Lock()
    defer tt.mu.Unlock()
    if t, ok := tt.txns[id]; ok {
        t.LastUsed = time.Now()
    }
}

func (tt *TransactionTracker) PruneIdle(eng *engine.StorageEngine) {
    tt.mu.Lock()
    defer tt.mu.Unlock()
    cutoff := time.Now().Add(-tt.timeout)
    for id, t := range tt.txns {
        if t.LastUsed.Before(cutoff) {
            slog.Warn("aborting idle transaction", "txn_id", id,
                "idle_seconds", time.Since(t.LastUsed).Seconds())
            eng.Abort(t.Txn) //nolint
            delete(tt.txns, id)
        }
    }
}
```

Background pruner (in `gateway/server.go` goroutine):

```go
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            s.tracker.PruneIdle(s.eng)
        case <-s.shutdownCh:
            return
        }
    }
}()
```

### Config

```yaml
gateway:
  port: 8080
  ws_buffer: 512
  api_keys: [...]
  idle_txn_timeout: "5m"     # auto-abort idle transactions
  request_drain_timeout: "30s"  # graceful shutdown drain window
```

---

## Testing Plan

```go
func TestGracefulShutdown_FlushesWAL(t *testing.T) {
    // Start engine, commit 100 keys
    // Send SIGTERM via syscall.Kill(os.Getpid(), syscall.SIGTERM)
    // Verify engine.Close() was called (check WAL checkpoint record written)
    // Reopen engine, verify all 100 keys present (no recovery needed)
}

func TestHealthReady_Returns503DuringShutdown(t *testing.T) {
    // Begin shutdown
    // GET /health/ready → expect 503
}

func TestIdleTxnTimeout(t *testing.T) {
    // Begin transaction, do nothing for timeout duration
    // Attempt to use transaction → expect 404 or 400 (transaction no longer active)
}
```

---

## Definition of Done

- [ ] SIGTERM handler registered in `main.go`
- [ ] In-flight requests drained before `engine.Close()` (30s max)
- [ ] `/health/ready` returns 503 from signal receipt to process exit
- [ ] `idle_txn_timeout` config option; default 5 minutes
- [ ] Background pruner aborts idle transactions every 30s
- [ ] `TestGracefulShutdown` integration test
- [ ] `docker stop` → no data loss verified in container test
