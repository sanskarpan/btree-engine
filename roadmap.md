# B+Tree Storage Engine — Production Roadmap

> **Audit date:** 2026-05-09
> **Current production-readiness score:** 85 / 100
> **Engine version:** post-Session-3 (7 gap fixes applied)

---

## Executive Summary

The engine has a textbook-correct core: ARIES 3-phase recovery, PostgreSQL-style MVCC, clock-eviction buffer pool, full-page CRC32 checksums, and a rich D3.js visualization frontend. The gap between "research-grade excellent" and "production-hardened" is a focused set of cross-cutting concerns: **observability, authentication, WAL management, serializable isolation, and DevOps infrastructure**.

This roadmap is organised into six phases. Each phase delivers a shippable increment with measurable acceptance criteria.

---

## Phase Overview

| Phase | Theme | Target Version | Sprints |
|-------|-------|---------------|---------|
| 0 | Foundation Hardening | v0.1 | 3 |
| 1 | Observability & Reliability | v0.2 | 2 |
| 2 | Correctness & Isolation | v0.3 | 3 |
| 3 | Performance | v0.4 | 3 |
| 4 | Developer Experience & DevOps | v0.5 | 2 |
| 5 | Enterprise & Scale | v1.0 | ongoing |

---

## Phase 0 — Foundation Hardening (v0.1)

**Goal:** Close gaps that make the engine unsafe to expose to any untrusted caller.

### Epic 0-A: Authentication & Authorization

**Why:** The REST API has no authentication. Any process that can reach port 8080 can read all data, crash the engine, or run recovery. This is a complete security boundary failure.

**Business impact:** Blocks any production or shared-environment deployment.

**Tickets:**
- `P0-001` — API key middleware for all routes
- `P0-002` — Per-route permission model (read / write / admin)
- `P0-003` — Secret rotation without restart

**Sprint 1 deliverables:** Auth middleware active, all existing tests updated to pass API key header, integration test for 401/403 responses.

---

### Epic 0-B: Input Validation & Bounds

**Why:** Keys and values are currently unbounded byte slices. A 4 MB value silently overflows a 4096-byte page causing either `ErrPageFull` (silent loss) or page corruption. Key-length > page header is rejected but with a confusing error.

**Business impact:** Any caller mistake can silently corrupt data.

**Tickets:**
- `P0-004` — Max key size (configurable, default 512 bytes); reject with HTTP 400
- `P0-005` — Max value size (configurable, default 65535 bytes, with overflow page placeholder)
- `P0-006` — Empty key rejection (partially done in gateway; enforce at engine layer too)
- `P0-007` — Null-byte / non-UTF8 key handling (allow raw bytes; document encoding contract)

**Sprint 1 deliverables:** All boundary violations return structured error responses. Fuzz corpus covers boundary conditions.

---

### Epic 0-C: Structured Logging

**Why:** The engine currently emits no structured logs. Diagnosing issues in a running system requires adding print statements and redeploying.

**Business impact:** Impossible to operate in production without log aggregation.

**Tickets:**
- `P0-008` — Add `slog` (Go 1.21 standard library) throughout engine; replace all `fmt.Printf`
- `P0-009` — Log levels: DEBUG (page ops), INFO (txn events), WARN (eviction pressure), ERROR (checksum failures, recovery errors)
- `P0-010` — Structured fields: `page_id`, `txn_id`, `lsn`, `op`, `duration_ms` on every log entry
- `P0-011` — Log output format: JSON in production, text in development (configurable)

**Sprint 1 deliverables:** Zero unstructured `fmt.Print*` calls in production paths. Logs parseable by `jq`.

---

### Epic 0-D: Graceful Shutdown & Signal Handling

**Why:** `Close()` is correct but the HTTP server does not register signal handlers. A `SIGTERM` from a container orchestrator will kill the process without flushing the WAL, potentially causing data loss despite the WAL implementation being correct.

**Business impact:** Container deployments (Docker, Kubernetes) will corrupt data on rolling restarts.

**Tickets:**
- `P0-012` — SIGTERM/SIGINT handler: drain in-flight requests (30s timeout), then `engine.Close()`
- `P0-013` — `/api/v1/health/ready` returns 503 during shutdown so load balancer stops routing
- `P0-014` — Idle transaction timeout: abort transactions open > N seconds (configurable, default 5 minutes)

**Sprint 2 deliverables:** `docker stop` triggers clean flush; no data loss in container restart test.

---

### Epic 0-E: WAL Lifecycle Management

**Why:** The WAL file grows unbounded. After a checkpoint, log records before the checkpoint LSN are never needed for recovery again. Without truncation, the WAL will fill disk in any long-running deployment.

**Business impact:** Disk exhaustion is a time-bomb in any production deployment.

**Tickets:**
- `P0-015` — WAL truncation: after successful checkpoint, truncate log records before `min(DPT.recLSN)`
- `P0-016` — WAL rotation: when WAL file exceeds configurable size limit (default 256 MB), rotate to new segment file
- `P0-017` — WAL archive hook: after rotation, call a configurable shell command (for backup integration)
- `P0-018` — WAL size reporting in `/api/v1/engine/stats`

**Sprint 2 deliverables:** WAL file stabilises in size after checkpoints. Rotation tested with concurrent writes.

---

### Epic 0-F: Rate Limiting & Abuse Protection

**Why:** No rate limiting on the API. A single misbehaving client can OOM the engine by opening thousands of transactions or flooding the WAL.

**Tickets:**
- `P0-019` — Per-IP rate limiter: max N requests/sec (token bucket, configurable)
- `P0-020` — Max concurrent open transactions (configurable, default 100); return 429 when exceeded
- `P0-021` — Request timeout middleware: return 504 if request takes > configurable duration

**Sprint 3 deliverables:** Load test shows 429 responses under flood; legitimate clients unaffected.

---

## Phase 1 — Observability & Reliability (v0.2)

**Goal:** Make the running engine fully inspectable without restarting it.

### Epic 1-A: Prometheus Metrics

**Why:** The `/api/v1/engine/stats` endpoint returns 4 fields. A production operator needs 30+ metrics to understand system health.

**Tickets:**
- `P1-001` — Prometheus `/metrics` endpoint (add `prometheus/client_golang` dependency)
- `P1-002` — Buffer pool metrics: `btree_buffer_hits_total`, `btree_buffer_misses_total`, `btree_buffer_hit_ratio`, `btree_buffer_dirty_pages`, `btree_buffer_pinned_pages`
- `P1-003` — WAL metrics: `btree_wal_bytes_written_total`, `btree_wal_flushes_total`, `btree_wal_flush_duration_seconds` (histogram), `btree_wal_size_bytes`
- `P1-004` — Transaction metrics: `btree_txns_begun_total`, `btree_txns_committed_total`, `btree_txns_aborted_total`, `btree_txns_active`, `btree_txns_write_conflicts_total`
- `P1-005` — Tree metrics: `btree_tree_height`, `btree_page_splits_total`, `btree_page_merges_total`, `btree_leaf_pages`, `btree_internal_pages`
- `P1-006` — Vacuum metrics: `btree_vacuum_runs_total`, `btree_vacuum_dead_tuples_reclaimed_total`, `btree_vacuum_pages_compacted_total`
- `P1-007` — Disk metrics: `btree_disk_reads_total`, `btree_disk_writes_total`, `btree_disk_read_duration_seconds`, `btree_disk_write_duration_seconds`
- `P1-008` — Recovery metrics: `btree_recovery_redo_records_total`, `btree_recovery_undo_records_total`, `btree_recovery_duration_seconds`

**Sprint 4 deliverables:** Grafana dashboard JSON file in `docs/grafana_dashboard.json`. All metrics visible after running any scenario.

---

### Epic 1-B: Background Checkpoint Scheduling

**Why:** Checkpoints are currently only triggered manually via API. Without periodic checkpoints, recovery time after a crash grows proportionally with the WAL file size.

**Tickets:**
- `P1-009` — Background goroutine in `engine.go` fires checkpoint every `checkpoint_interval`
- `P1-010` — Checkpoint skipped if no dirty pages since last checkpoint (idempotent)
- `P1-011` — Checkpoint metrics: last checkpoint LSN, time since last checkpoint, dirty pages at checkpoint

**Sprint 4 deliverables:** Recovery time bounded by `checkpoint_interval * write_rate`.

---

### Epic 1-C: Health Checks

**Why:** The `/health` endpoint exists but its implementation and semantics are not documented.

**Tickets:**
- `P1-012` — `/health/live` (liveness): returns 200 if process is running
- `P1-013` — `/health/ready` (readiness): returns 200 only after ARIES recovery completes; 503 during startup and shutdown
- `P1-014` — `/health/ready` response includes: `wal_flushed_lsn`, `dirty_pages`, `active_txns`, `last_checkpoint_ago_seconds`

**Sprint 4 deliverables:** Kubernetes liveness/readiness probe config in `docs/k8s/`.

---

### Epic 1-D: Cursor Safety

**Why:** `Scan()` releases `treeMu.RLock()` before returning the cursor. The cursor holds a pin on the current leaf page, but if a concurrent writer splits or merges that page after the lock is released, the cursor's `nextPageID` link may be stale, causing incorrect scan results or use-after-free on the frame.

**Tickets:**
- `P1-015` — Document current cursor safety contract (caller must not modify tree during scan)
- `P1-016` — Add cursor version counter: engine increments on every structural change; cursor checks on `Next()` and returns `ErrCursorInvalidated` if changed
- `P1-017` — Cursor auto-close after configurable idle timeout

**Sprint 5 deliverables:** `TestCursor_InvalidatedOnConcurrentSplit` integration test passes.

---

### Epic 1-E: Vacuum Improvements

**Why:** VACUUM is a single goroutine that stops and holds the tree read lock for the entire scan. On a large database, this blocks all writers for seconds.

**Tickets:**
- `P1-018` — Page-by-page vacuum: release and re-acquire lock between pages
- `P1-019` — Vacuum progress tracking: current page, pages processed, dead tuples reclaimed (exposed via metrics)
- `P1-020` — Vacuum priority: skip pages below dead_threshold, process worst pages first
- `P1-021` — Manual vacuum trigger: `POST /api/v1/vacuum/run` (for testing and admin)

**Sprint 5 deliverables:** Vacuum completes on 100K key database without blocking writes > 10ms.

---

## Phase 2 — Correctness & Isolation (v0.3)

**Goal:** Close the remaining isolation gaps and make transaction semantics complete.

### Epic 2-A: Serializable Snapshot Isolation (SSI)

**Why:** The write-skew anomaly is a known limitation of Snapshot Isolation. The doctors scenario in the demo explicitly shows this. For applications with multi-key invariants (balances, inventory, at-least-one-on-call constraints), this is a correctness bug.

**Implementation approach:** PostgreSQL SSI (Ports & Grabs 2012) using SIREAD locks and rw-dependency tracking.

**Tickets:**
- `P2-001` — Define `SIREADLock` struct: `{TxnID, PageID, SlotRange}`
- `P2-002` — `SIREADLockManager`: track read-lock sets per transaction
- `P2-003` — On `Get()`: acquire SIREAD lock on (page, slot) under SI
- `P2-004` — On `Insert()`/`Update()`/`Delete()`: check for rw-antidependency; if cycle forms → abort
- `P2-005` — Add `IsolationLevel = Serializable` to transaction manager
- `P2-006` — SIREAD lock GC: release when transaction commits/aborts
- `P2-007` — Add serializable scenario to simulation package
- `P2-008` — `TestSerializable_WriteSkewPrevented` test (doctors scenario must fail under SSI)

**Sprint 6-7 deliverables:** Serializable isolation level available. Write-skew impossible under `serializable` isolation.

---

### Epic 2-B: Transaction Savepoints

**Why:** Long transactions that fail partway through currently must abort the entire transaction. Applications need partial rollback.

**Tickets:**
- `P2-009` — `Savepoint(txn, name string)` API: snapshot current txn state (undo log position)
- `P2-010` — `RollbackToSavepoint(txn, name string)`: apply undo log entries back to savepoint
- `P2-011` — WAL CLR entries for savepoint rollback (partial undo)
- `P2-012` — REST endpoint: `POST /api/v1/txn/:id/savepoint` and `POST /api/v1/txn/:id/rollback/:savepoint`

**Sprint 7 deliverables:** Savepoint rollback tested with 3-level nesting.

---

### Epic 2-C: Deadlock Detection

**Why:** While MVCC prevents read-write deadlocks, write-write conflicts use abort-on-conflict. Applications that retry aborted transactions can livelock. A simple waits-for graph prevents this.

**Tickets:**
- `P2-013` — Waits-for graph: `WFG map[TxnID]TxnID` (who is blocked waiting for whom)
- `P2-014` — Cycle detection on every new wait edge (DFS, O(txn count))
- `P2-015` — On cycle: abort youngest transaction in cycle (lowest priority)
- `P2-016` — `btree_deadlocks_detected_total` metric

**Sprint 7 deliverables:** Deadlock scenario test verifies one transaction aborts, other proceeds.

---

### Epic 2-D: Consistent Snapshot Export

**Why:** No way to take a consistent online backup. `FlushDirtyPages()` is not safe to call while writers are active (data may be partially written from concurrent transactions).

**Tickets:**
- `P2-017` — `ConsistentSnapshot()`: acquires tree write lock, flushes all dirty pages, writes checkpoint, returns snapshot LSN
- `P2-018` — `POST /api/v1/snapshot` endpoint
- `P2-019` — Online backup: cp data/*.db data/*.log while snapshot LSN is held → consistent point-in-time copy

**Sprint 8 deliverables:** Online backup procedure documented and tested with concurrent workload.

---

## Phase 3 — Performance (v0.4)

**Goal:** 10x write throughput improvement for batch workloads; sub-millisecond read latency at 90th percentile.

### Epic 3-A: Group Commit

**Why:** Every `Commit()` calls `fsync()`. At 100 commits/sec, that is 100 fsyncs/sec — a typical spinning disk can only do ~100-200 fsyncs/sec. Group commit batches multiple commits into one fsync.

**Tickets:**
- `P3-001` — Commit queue: goroutines waiting for fsync park in a channel
- `P3-002` — Commit batcher: goroutine drains queue, appends all commit records, single fsync, wakes all waiters
- `P3-003` — Configurable: `group_commit_delay_ms` (0 = disabled, 1-50 = batch window)
- `P3-004` — Benchmark: `BenchmarkGroupCommit` at 1K, 10K concurrent committers

**Sprint 9 deliverables:** 5x commit throughput improvement demonstrated by benchmark.

---

### Epic 3-B: WAL Pipelining

**Why:** Log writes and disk I/O are serialized. Non-commit log records (INSERT, UPDATE, DELETE) do not need to wait for fsync — only COMMIT does. Decoupling log append from fsync unblocks concurrent writers.

**Tickets:**
- `P3-005` — Async log append: write to in-memory buffer without waiting for flush
- `P3-006` — Flush only on COMMIT (already done) or when buffer is full (already done) — verify no regression
- `P3-007` — Parallel write to WAL and page modifications (currently sequential)

**Sprint 9 deliverables:** P99 INSERT latency < 1ms at 1000 concurrent writers.

---

### Epic 3-C: LRU-K Buffer Eviction

**Why:** Clock (second-chance) is a good approximation of LRU-1. For workloads with skewed hot/cold page distributions (OLTP with a small hot set), LRU-K retains hot pages better, reducing disk I/O by up to 50%.

**Tickets:**
- `P3-008` — LRU-K eviction policy (K=2) using per-frame access time ring buffer
- `P3-009` — Make eviction policy pluggable (`EvictionPolicy` interface)
- `P3-010` — Benchmark: compare Clock vs LRU-2 on Zipf-distributed workload
- `P3-011` — Configuration: `buffer_eviction_policy: "clock" | "lru2"`

**Sprint 10 deliverables:** LRU-2 reduces buffer misses by ≥ 20% on Zipf workload.

---

### Epic 3-D: Value Compression

**Why:** MVCC creates multiple versions of the same key, and values are often highly compressible (JSON, text). A 10:1 compression ratio means 10x more versions fit per page, reducing both disk and memory usage.

**Tickets:**
- `P3-012` — Add `snappy` dependency for value compression
- `P3-013` — Tuple flag `TupleCompressed = 0x08`; set on insert when compressed smaller
- `P3-014` — Transparent decompression on read
- `P3-015` — Compression stats: ratio, compressed bytes saved, compression misses (value grew)
- `P3-016` — Per-key compression override via Insert API

**Sprint 10 deliverables:** Value storage 30-50% smaller on JSON workloads. No correctness regressions.

---

### Epic 3-E: Bloom Filters for Negative Lookups

**Why:** `Get()` on a non-existent key traverses the tree to a leaf, reads the page from disk, and finds nothing. For workloads with many "key not found" queries (caches, existence checks), this is pure wasted I/O.

**Tickets:**
- `P3-017` — Per-leaf bloom filter: 1024-bit Bloom filter stored in leaf page header extension
- `P3-018` — Update on insert; rebuild on compact
- `P3-019` — Check on `Get()`: if key not in bloom filter, return `ErrKeyNotFound` without disk read
- `P3-020` — False positive rate ≤ 1% at page fill factor 0.75

**Sprint 11 deliverables:** `BenchmarkBloomFilter_NegativeLookup` shows 10x speedup for 100% miss workloads.

---

### Epic 3-F: Parallel Vacuum

**Why:** Current vacuum is single-goroutine and processes one page at a time with per-page lock acquisition. With 100K pages, vacuum takes O(minutes).

**Tickets:**
- `P3-021` — Segment vacuum: partition pages into N segments, vacuum each segment in parallel
- `P3-022` — Worker pool: configurable `vacuum_workers` (default 4)
- `P3-023` — Per-segment statistics: dead tuple count per page tracked between vacuum runs
- `P3-024` — Vacuum velocity metric: pages/sec, dead tuples/sec

**Sprint 11 deliverables:** Vacuum throughput scales linearly with `vacuum_workers`.

---

## Phase 4 — Developer Experience & DevOps (v0.5)

**Goal:** Make the engine trivially deployable and testable by anyone.

### Epic 4-A: Docker & Container Support

**Tickets:**
- `P4-001` — Multi-stage `Dockerfile`: builder (Go) + minimal runtime (distroless)
- `P4-002` — `docker-compose.yml`: engine + frontend BFF + optional Prometheus + Grafana
- `P4-003` — `docker-compose.dev.yml`: with volume mounts for live code reload
- `P4-004` — Container health check via `/health/ready`
- `P4-005` — Image size target: < 30 MB final image

**Sprint 12 deliverables:** `docker compose up` brings entire stack to running state in < 30 seconds.

---

### Epic 4-B: CI/CD Pipeline

**Tickets:**
- `P4-006` — GitHub Actions workflow: test (`go test ./... -race`), lint (`golangci-lint`), build
- `P4-007` — Benchmark regression CI: `go test -bench=. -benchmem` baseline stored as artifact; fail if > 20% regression
- `P4-008` — Fuzz CI: `go test -fuzz=FuzzInsert -fuzztime=60s` in nightly workflow
- `P4-009` — Coverage report: `go test -cover` → upload to Codecov
- `P4-010` — Release workflow: tag → build binaries for darwin/linux/windows → GitHub Release

**Sprint 12 deliverables:** PRs auto-tested; benchmarks tracked across commits.

---

### Epic 4-C: golangci-lint Configuration

**Tickets:**
- `P4-011` — `.golangci.yml`: enable `errcheck`, `govet`, `staticcheck`, `gosec`, `revive`, `gocognit`
- `P4-012` — Fix all existing lint warnings (expected: ~20-30 minor issues)
- `P4-013` — Lint runs in < 30 seconds via GitHub Actions cache

---

### Epic 4-D: Fuzz Testing

**Tickets:**
- `P4-014` — `FuzzInsertGet`: random key/value bytes → insert → get → assert equal
- `P4-015` — `FuzzBinarySearch`: random record set → binary search → assert all results correct
- `P4-016` — `FuzzChecksumRoundtrip`: random page data → encode → corrupt random byte → verify detects
- `P4-017` — `FuzzWALRecord`: random payloads → decode → re-encode → assert idempotent

---

### Epic 4-E: Admin CLI

**Tickets:**
- `P4-018` — `cmd/btree-admin/main.go`: CLI tool (`cobra` or `flag`)
- `P4-019` — `btree-admin inspect page <id>` → dumps page header + slot table
- `P4-020` — `btree-admin wal tail --n=50` → recent WAL records
- `P4-021` — `btree-admin checkpoint` → trigger checkpoint
- `P4-022` — `btree-admin vacuum` → trigger manual vacuum
- `P4-023` — `btree-admin stats` → engine stats as formatted table

---

### Epic 4-F: Benchmark Suite

**Tickets:**
- `P4-024` — `BenchmarkInsert_Sequential` / `_Random` (1K, 10K, 100K, 1M keys)
- `P4-025` — `BenchmarkGet_HotSet` / `_ColdSet` (cache hit vs disk read)
- `P4-026` — `BenchmarkScan_1K` / `_10K` range scan throughput
- `P4-027` — `BenchmarkConcurrentWriters_10` / `_100` / `_1000`
- `P4-028` — `BenchmarkCrashRecovery` (time to recover from 10K, 100K WAL records)
- `P4-029` — Benchmark results table in `docs/performance.md`

---

## Phase 5 — Enterprise & Scale (v1.0)

### Epic 5-A: WAL Streaming Replication

**Why:** Single-node failure is the #1 availability risk. WAL-based replication (like PostgreSQL streaming replication) provides a hot standby with RPO near zero.

**Tickets:**
- `P5-001` — WAL sender: stream new WAL records to subscriber connections
- `P5-002` — WAL receiver: apply incoming records to standby buffer pool
- `P5-003` — Standby read-only mode: serve `Get()` and `Scan()` from standby
- `P5-004` — Replication lag metric: `btree_replication_lag_bytes`
- `P5-005` — Failover procedure documentation

---

### Epic 5-B: Backup & Point-in-Time Recovery

**Tickets:**
- `P5-006` — `btree-admin backup --output=<dir>`: consistent snapshot + WAL segments
- `P5-007` — `btree-admin restore --from=<dir> --to-lsn=<lsn>`: PITR replay
- `P5-008` — Incremental backup: only WAL segments since last backup

---

### Epic 5-C: TLS & Network Security

**Tickets:**
- `P5-009` — TLS termination at gateway: configurable cert/key paths
- `P5-010` — mTLS option for client authentication
- `P5-011` — CORS configuration for frontend

---

### Epic 5-D: Role-Based Access Control

**Tickets:**
- `P5-012` — Role definitions: `reader`, `writer`, `admin`
- `P5-013` — Key-range permissions: `role X can read/write keys matching prefix Y`
- `P5-014` — RBAC config in `config.yaml`

---

### Epic 5-E: Audit Logging

**Tickets:**
- `P5-015` — Audit log: structured record for every API call (user, operation, key, timestamp, result)
- `P5-016` — Audit log rotation and retention policy
- `P5-017` — GDPR-related: key deletion audit trail

---

### Epic 5-F: Overflow Pages for Large Values

**Why:** Values > ~1800 bytes (half page minus headers) are currently rejected or cause `ErrPageFull` on first insert.

**Tickets:**
- `P5-018` — Overflow page chain: `FlagHasOverflow` is already defined; implement the chain
- `P5-019` — Tuple encoding for overflow: store `[overflow_page_id:4]` in value field
- `P5-020` — Buffer pool integration: fetch overflow pages transparently on read

---

### Epic 5-G: Secondary Indexes

**Tickets:**
- `P5-021` — Index API: `CreateIndex(name, keyExtractor func(value []byte) []byte)`
- `P5-022` — Index maintained transactionally on every insert/update/delete
- `P5-023` — `IndexScan(indexName, start, end)` range query through index
- `P5-024` — Index WAL logging

---

## Sprint Plan

| Sprint | Epics | Duration | Key Deliverables |
|--------|-------|----------|-----------------|
| 1 | 0-A, 0-B, 0-C | 2 weeks | Auth, input validation, structured logs |
| 2 | 0-D, 0-E | 1 week | Graceful shutdown, WAL truncation |
| 3 | 0-F | 1 week | Rate limiting, txn timeout |
| 4 | 1-A, 1-B, 1-C | 2 weeks | Prometheus metrics, background checkpoint, health |
| 5 | 1-D, 1-E | 1 week | Cursor safety, vacuum improvements |
| 6 | 2-A (part 1) | 2 weeks | SIREAD locks, rw-antidependency tracking |
| 7 | 2-A (part 2), 2-B, 2-C | 2 weeks | SSI complete, savepoints, deadlock detection |
| 8 | 2-D | 1 week | Consistent snapshot export |
| 9 | 3-A, 3-B | 2 weeks | Group commit, WAL pipelining |
| 10 | 3-C, 3-D | 2 weeks | LRU-K, value compression |
| 11 | 3-E, 3-F | 1 week | Bloom filters, parallel vacuum |
| 12 | 4-A, 4-B, 4-C, 4-D, 4-E, 4-F | 2 weeks | Docker, CI, lint, fuzz, admin CLI, benchmarks |
| 13+ | 5-A through 5-G | ongoing | Replication, PITR, TLS, RBAC, secondary indexes |

---

## Success Metrics

| Metric | Current | v0.1 Target | v0.2 Target | v1.0 Target |
|--------|---------|-------------|-------------|-------------|
| API auth coverage | 0% | 100% | 100% | 100% |
| WAL disk growth | unbounded | stable | stable | < 256 MB |
| Prometheus metrics | 0 | 0 | 30+ | 30+ |
| Recovery time (100K records) | ~1s | ~1s | < 500ms | < 100ms |
| Write throughput (commits/sec) | ~500 | ~500 | ~1000 | ~5000 |
| P99 read latency | ~2ms | ~2ms | ~1ms | <500µs |
| Test coverage | ~70% | ~75% | ~85% | ~90% |
| Docker image size | N/A | N/A | N/A | < 30 MB |
