# B+Tree Storage Engine — TODO

> Generated: 2026-05-09 | Status legend: `[ ]` open · `[~]` in progress · `[x]` done

---

## P0 — Critical (must fix before any production deployment)

### Authentication & Authorization
- [x] `P0-001` API key middleware for all routes (see `tickets/P0/P0-001-api-key-auth.md`)
- [ ] `P0-002` Per-route permission model: read / write / admin
- [ ] `P0-003` Secret rotation without restart

### Input Validation
- [x] `P0-004` Max key size enforcement (512 bytes default) with HTTP 400 on violation
- [ ] `P0-005` Max value size enforcement (65535 bytes default)
- [ ] `P0-006` Engine-layer empty-key guard (currently only in gateway)
- [ ] `P0-007` Document binary key encoding contract

### Structured Logging
- [x] `P0-008` Replace all `fmt.Printf` in engine with `slog` structured logger
- [ ] `P0-009` Log levels: DEBUG / INFO / WARN / ERROR with configurable minimum level
- [ ] `P0-010` Structured fields on every log entry: `page_id`, `txn_id`, `lsn`, `op`, `duration_ms`
- [ ] `P0-011` JSON log output in production; text in development (configurable via config.yaml)

### Graceful Shutdown
- [x] `P0-012` SIGTERM/SIGINT handler: drain requests → `engine.Close()`
- [ ] `P0-013` `/api/v1/health/ready` returns 503 during startup and shutdown
- [ ] `P0-014` Idle transaction timeout (default 5 minutes)

### WAL Lifecycle
- [x] `P0-015` WAL truncation after checkpoint (remove records before `min(DPT.recLSN)`)
- [ ] `P0-016` WAL rotation at configurable size limit (default 256 MB)
- [ ] `P0-017` WAL archive hook (post-rotate shell command for backup integration)
- [ ] `P0-018` WAL size in `/api/v1/engine/stats`

### Rate Limiting
- [x] `P0-019` Per-IP token bucket rate limiter
- [ ] `P0-020` Max concurrent open transactions (default 100); HTTP 429 when exceeded
- [ ] `P0-021` Request timeout middleware (configurable, HTTP 504 on exceed)

### Frontend / BFF Security
- [x] `P0-022` Prevent path traversal in BFF static asset serving

---

## P1 — High (important for production reliability)

### Observability
- [x] `P1-001` Prometheus `/metrics` endpoint
- [ ] `P1-002` Buffer pool metrics (5 counters/gauges)
- [ ] `P1-003` WAL metrics (4 counters + 1 histogram)
- [ ] `P1-004` Transaction metrics (5 counters/gauges including write conflicts)
- [ ] `P1-005` Tree structural metrics (height, splits, merges, page counts)
- [ ] `P1-006` Vacuum metrics (runs, dead tuples reclaimed, pages compacted)
- [ ] `P1-007` Disk I/O metrics (reads, writes, latency histograms)
- [ ] `P1-008` Recovery metrics (redo/undo counts, duration)

### Background Checkpoint
- [x] `P1-009` Background checkpoint goroutine (configurable interval)
- [ ] `P1-010` Skip checkpoint if no dirty pages since last
- [ ] `P1-011` Checkpoint metrics (last LSN, time since last, dirty pages at checkpoint)

### Health Checks
- [x] `P1-012` `/health/live` liveness endpoint
- [ ] `P1-013` `/health/ready` readiness endpoint (503 during startup/shutdown)
- [ ] `P1-014` Rich readiness response body (WAL LSN, dirty pages, active txns)

### Frontend Reliability
- [x] `P1-015` Propagate backend WebSocket disconnects so browser clients recover cleanly
- [x] `P1-016` Tree node click drives the Page Inspector instead of an alert-only flow
- [x] `P1-017` README/runtime docs stay aligned with actual Go/tooling requirements and RC semantics
- [x] `P1-018` Browser-level frontend smoke coverage in CI

### Cursor Safety
- [x] `P1-019` Document cursor safety contract
- [ ] `P1-020` Engine structural version counter + cursor invalidation on `Next()`
- [ ] `P1-021` Cursor idle timeout

### Vacuum Improvements
- [x] `P1-018` Per-page lock release/re-acquire during vacuum (avoid long blocking)
- [ ] `P1-022` Vacuum progress tracking via metrics
- [ ] `P1-023` Priority-based vacuum (worst pages first)
- [ ] `P1-024` `POST /api/v1/vacuum/run` manual trigger

---

## P2 — Medium (correctness improvements)

### Serializable Isolation
- [x] `P2-001` `SIREADLock` struct definition
- [ ] `P2-002` `SIREADLockManager`
- [ ] `P2-003` Acquire SIREAD lock on `Get()` under Serializable level
- [ ] `P2-004` Check rw-antidependency on write operations
- [ ] `P2-005` `IsolationLevel = Serializable` added to transaction manager
- [ ] `P2-006` SIREAD lock GC on commit/abort
- [ ] `P2-007` Serializable scenario in simulation package
- [ ] `P2-008` `TestSerializable_WriteSkewPrevented` integration test

### Transaction Savepoints
- [x] `P2-009` `Savepoint(txn, name)` API
- [ ] `P2-010` `RollbackToSavepoint(txn, name)` API
- [ ] `P2-011` WAL CLR entries for partial undo
- [ ] `P2-012` Savepoint REST endpoints

### Deadlock Detection
- [x] `P2-013` Waits-for graph implementation
- [ ] `P2-014` Cycle detection on new wait edge
- [ ] `P2-015` Youngest-transaction-aborts strategy
- [ ] `P2-016` `btree_deadlocks_detected_total` metric

### Consistent Snapshot Export
- [ ] `P2-017` `ConsistentSnapshot()` engine API
- [ ] `P2-018` `POST /api/v1/snapshot` endpoint
- [ ] `P2-019` Online backup procedure documentation

---

## P3 — Performance

- [x] `P3-001` Group commit queue implementation
- [ ] `P3-002` Group commit batcher goroutine
- [ ] `P3-003` `group_commit_delay_ms` configuration
- [ ] `P3-004` `BenchmarkGroupCommit` benchmark
- [ ] `P3-005` Async WAL append (non-commit records)
- [x] `P3-008` LRU-K (K=2) eviction policy
- [x] `P3-009` Pluggable eviction policy interface
- [ ] `P3-010` Clock vs LRU-2 benchmark on Zipf distribution
- [ ] `P3-012` Snappy value compression
- [ ] `P3-013` `TupleCompressed` flag in tuple encoding
- [ ] `P3-014` Transparent decompression on read
- [x] `P3-017` Per-leaf Bloom filter (1024-bit)
- [x] `P3-018` Bloom filter update on insert, rebuild on compact
- [x] `P3-019` Bloom filter check in `Get()` path
- [ ] `P3-021` Parallel vacuum with worker pool
- [ ] `P3-022` Configurable `vacuum_workers`

---

## P4 — DevOps & Developer Experience

- [x] `P4-001` Multi-stage Dockerfile
- [ ] `P4-002` `docker-compose.yml` (engine + frontend + Prometheus + Grafana)
- [ ] `P4-003` `docker-compose.dev.yml` with volume mounts
- [ ] `P4-004` Container health check in Dockerfile
- [ ] `P4-005` Image size < 30 MB
- [x] `P4-006` GitHub Actions: test + lint + build workflow
- [ ] `P4-007` Benchmark regression CI
- [ ] `P4-008` Fuzz CI (nightly)
- [ ] `P4-009` Coverage upload to Codecov
- [ ] `P4-010` Release workflow (cross-platform binaries)
- [x] `P4-011` `.golangci.yml` configuration
- [ ] `P4-012` Fix existing lint warnings
- [x] `P4-014` `FuzzInsertGet` fuzz test
- [x] `P4-015` `FuzzBinarySearch` fuzz test
- [x] `P4-016` `FuzzChecksumRoundtrip` fuzz test
- [x] `P4-017` `FuzzWALRecord` fuzz test
- [x] `P4-018` `btree-admin` CLI binary
- [x] `P4-019` `btree-admin inspect page <id>`
- [x] `P4-020` `btree-admin wal tail`
- [x] `P4-021` `btree-admin checkpoint`
- [x] `P4-022` `btree-admin vacuum`
- [x] `P4-023` `btree-admin stats`
- [x] `P4-024` Sequential/random insert benchmarks
- [x] `P4-025` Hot/cold set get benchmarks
- [x] `P4-026` Range scan benchmarks
- [x] `P4-027` Concurrent writers benchmarks
- [ ] `P4-028` Crash recovery benchmark
- [ ] `P4-029` `docs/performance.md` results table

---

## P5 — Enterprise (v1.0)

- [ ] `P5-001` WAL streaming sender
- [ ] `P5-002` WAL streaming receiver
- [ ] `P5-003` Standby read-only mode
- [ ] `P5-006` `btree-admin backup` command
- [ ] `P5-007` `btree-admin restore` with PITR
- [ ] `P5-008` Incremental backup
- [ ] `P5-009` TLS termination at gateway
- [ ] `P5-012` RBAC role definitions
- [ ] `P5-013` Key-range permissions
- [ ] `P5-015` Audit log (per-request structured record)
- [ ] `P5-016` Audit log rotation
- [ ] `P5-018` Overflow page chain implementation
- [ ] `P5-021` Secondary index API
- [ ] `P5-022` Transactional index maintenance
- [ ] `P5-023` `IndexScan` range query
- [ ] `P5-024` Index WAL logging

---

## Completed ✓

- [x] Full-page CRC32 checksum (Task #7)
- [x] Cascade internal node merge (Task #6)
- [x] WAL logging for merge/borrow operations (Task #5)
- [x] VACUUM reclaims aborted inserts (Task #4)
- [x] statusTable pruning — memory leak fix (Task #3)
- [x] RWMutex for concurrent read throughput (Task #2)
- [x] Write-write conflict detection — SI lost-update prevention (Task #1)
- [x] BinarySearch leftmost match for MVCC version chains (Session 2)
- [x] MVCC visibility endpoint (Session 2)
- [x] Empty key validation (Session 2)
- [x] JSON null → empty array normalization (Session 2)
- [x] Cursor inclusive bounds (Session 1)
- [x] Vacuum nil-crash fix (Session 1)
- [x] Buffer pool freelist corruption (Session 1)
- [x] Events data race fix (Session 1)
