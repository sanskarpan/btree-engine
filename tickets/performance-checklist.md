# Performance Benchmarking Checklist

> Run before any release that touches the hot path (page layer, WAL, buffer pool, B+Tree operations).

---

## 1. Benchmark Suite

### Run

```bash
go test ./... -bench=. -benchmem -benchtime=10s -count=3 2>&1 | tee bench-$(date +%Y%m%d).txt
benchstat baseline.txt bench-$(date +%Y%m%d).txt
```

### Benchmark Inventory

| Benchmark | Location | What it measures |
|-----------|----------|-----------------|
| `BenchmarkPage_BinarySearch/10slots` | `test/unit/page_test.go` | Binary search small page |
| `BenchmarkPage_BinarySearch/50slots` | `test/unit/page_test.go` | Binary search full page |
| `BenchmarkPage_InsertRecord` | `test/unit/page_test.go` | Single record insert with slot shift |
| `BenchmarkBuffer_FetchPage_Hit` | `test/unit/buffer_pool_test.go` | Buffer hit (no disk) |
| `BenchmarkBuffer_FetchPage_Miss` | `test/unit/buffer_pool_test.go` | Buffer miss (disk read) |
| `BenchmarkWAL_AppendRecord` | `test/unit/wal_test.go` | WAL append (no fsync) |
| `BenchmarkWAL_Commit` | `test/unit/wal_test.go` | WAL commit (with fsync) |
| `BenchmarkBTree_Insert_Sequential` | `test/unit/btree_test.go` | Sequential key insert |
| `BenchmarkBTree_Insert_Random` | `test/unit/btree_test.go` | Random key insert |
| `BenchmarkBTree_Get_HotSet` | `test/unit/btree_test.go` | Get on cached pages |
| `BenchmarkBTree_Get_ColdSet` | `test/unit/btree_test.go` | Get triggering disk reads |
| `BenchmarkBTree_Scan_1K` | `test/unit/btree_test.go` | Range scan 1K results |
| `BenchmarkBTree_Scan_10K` | `test/unit/btree_test.go` | Range scan 10K results |
| `BenchmarkEngine_ConcurrentWriters_10` | `test/unit/btree_test.go` | 10 concurrent committers |
| `BenchmarkEngine_ConcurrentWriters_100` | `test/unit/btree_test.go` | 100 concurrent committers |
| `BenchmarkCrashRecovery_10K` | `test/integration/crash_recovery_test.go` | Recovery from 10K WAL records |
| `BenchmarkGroupCommit_100` | `test/unit/wal_test.go` | Group commit at 100 concurrent (P3) |

---

## 2. Performance Targets (Baseline System: Apple M3, NVMe SSD)

| Benchmark | Target | Acceptable | Fail |
|-----------|--------|------------|------|
| Page binary search (50 slots) | < 300 ns | < 500 ns | > 750 ns |
| Buffer hit | < 150 ns | < 250 ns | > 400 ns |
| WAL append (no fsync) | < 500 ns | < 1 µs | > 2 µs |
| WAL commit (fsync) | < 2 ms | < 5 ms | > 10 ms |
| BTree insert (sequential, cached) | < 5 µs | < 10 µs | > 20 µs |
| BTree get (hot, cached) | < 1 µs | < 2 µs | > 5 µs |
| Scan throughput (1K keys) | > 500K keys/sec | > 200K keys/sec | < 100K keys/sec |
| Concurrent writers (100) | > 2000 commits/sec | > 1000 commits/sec | < 500 commits/sec |
| Recovery (10K records) | < 100 ms | < 500 ms | > 1s |

---

## 3. Memory Allocation Profile

```bash
go test ./internal/btree/... -bench=BenchmarkBTree_Insert_Sequential \
    -benchmem -memprofile mem.out
go tool pprof -alloc_objects mem.out
```

**Targets:**
- Insert: < 3 heap allocations per insert (tuple + page copy + slot)
- Get: 0-1 heap allocations (just the returned byte slice)
- BinarySearch: 0 heap allocations (operates on existing page data)

**Known allocation sources to minimize:**
- `mvcc.DecodeTuple()`: allocates Key and Value byte slices — consider `DecodeInPlace` variant
- `page.GetRecord()`: returns slice into page data (zero alloc ✓)
- `buffer.ReadPageData()`: copies page into new `page.Page` struct — necessary for safety

---

## 4. CPU Profile

```bash
go test ./test/integration/... -bench=BenchmarkConcurrentWriters_100 \
    -cpuprofile cpu.out -benchtime=30s
go tool pprof -http=:8081 cpu.out
```

**Expected hot functions:**
1. `crc32.ChecksumIEEE` — page checksums (~10-15% CPU)
2. `file.Sync` — WAL fsync (wall clock dominant, not CPU)
3. `page.BinarySearch` → `bytes.Compare` (~5-10% CPU)
4. `sync.Mutex.Lock/Unlock` — tree/buffer pool locks (~5% CPU)

**Flags to investigate:**
- `runtime.mallocgc` > 15% → too many allocations in hot path
- `syscall.pread/pwrite` > 30% → buffer pool too small; increase pool size

---

## 5. Lock Contention Analysis

```bash
go test ./test/integration/... -bench=BenchmarkConcurrentWriters_100 \
    -mutexprofile mutex.out -benchtime=30s
go tool pprof mutex.out
```

**Expected lock hierarchy (from hottest to coldest):**
1. `btree.BPlusTree.treeMu` — write lock (acceptable under write-heavy workload)
2. `wal.WALManager.mu` — WAL append lock (should be brief)
3. `buffer.BufferPool.mu` — frame lock (brief per fetch)
4. `mvcc.TransactionManager.mu` — txn begin/commit lock (brief)

**Red flags:**
- Any lock held > 1ms per acquisition → investigate split into finer-grained locks
- `treeMu.RLock` contentious → readers starved by writers → consider read queue fairness

---

## 6. Disk I/O Profile

```bash
# macOS
sudo dtrace -n 'syscall::pread:return { @[execname] = sum(arg0); }'

# Linux
sudo strace -e trace=pread64,pwrite64,fsync -p <pid> -c
```

**Expected for sequential insert workload (1K keys, 256-frame pool):**
- `pread` calls: ~10 (tree traversal, mostly cache hits after warm-up)
- `pwrite` calls: ~15 (dirty page flushes during eviction)
- `fsync` calls: 1 per commit (or 1 per batch with group commit)

**Red flags:**
- `pread` calls proportional to inserts → buffer pool too small or poor eviction policy
- `pwrite` calls far exceeding `fsync` calls → forced dirty page eviction (pool too small)

---

## 7. WAL Write Amplification

For every user-visible `Put()` operation, the following WAL records are written:
- `LogBegin` (if not already begun): ~45 bytes
- `LogInsert`: ~45 bytes + key + value
- `LogCommit`: ~45 bytes

**Expected write amplification for 100-byte key-value:**
- WAL bytes per Put: ~235 bytes (2.35x amplification over raw data size)
- This is acceptable for ARIES; PostgreSQL's amplification is similar

**Red flag:** WAL bytes per operation growing faster than `O(key + value)` → investigate spurious log records.

---

## 8. Buffer Pool Hit Rate

**Target:** > 80% hit rate for any dataset smaller than buffer pool × page size.

```bash
# After warm-up run (engine loaded):
curl http://localhost:8080/api/v1/engine/stats | jq .buffer_hit_rate
```

**Expectations:**
- 256 frames × 4096 bytes = 1 MB pool
- Dataset < 1 MB: hit rate ~95%+
- Dataset = 10× pool size: hit rate depends on access pattern
  - Sequential: ~5% (poor — clock evicts pages that will never be reused)
  - Zipf (α=1.2): ~60-70% (most accesses to hot 20% of pages)
  - Uniform random: ~10% (equal to pool/dataset ratio)

---

## 9. Long-Running Stability Test

```bash
# Run for 1 hour: 4 writer goroutines + 4 reader goroutines
go test ./test/integration/... -run=TestLongRunning -v -timeout=3600s
```

**Monitor during run:**
- Memory: should stabilise (no leak)
  - statusTable pruned by vacuum (P0-003 ✓)
  - Buffer pool fixed size (✓)
  - Active transaction count < configured max
- WAL file size: should stabilise (after P0-015 — WAL truncation)
- Goroutine count: should be constant (no goroutine leak from closed cursors)
- CPU: should be constant (no background work accumulating)

---

## 10. Before/After Comparison for Phase 3 Features

| Feature | Benchmark | Expected Improvement |
|---------|-----------|---------------------|
| Group commit (P3-001) | `BenchmarkConcurrentWriters_100` | 5-20x commit throughput |
| LRU-K eviction (P3-008) | Buffer hit rate on Zipf workload | +20% hit rate |
| Bloom filters (P3-017) | `BenchmarkBTree_Get` on 100% miss workload | 10x speedup (skip disk read) |
| Parallel vacuum (P3-021) | Vacuum duration on 100K dead tuples | Linear scaling with workers |
| Value compression (P3-012) | Storage bytes per tuple | 30-50% size reduction on JSON |

---

## 11. Benchmark Regression Gate (CI)

```yaml
# .github/workflows/benchmark.yml
- name: Run benchmarks
  run: go test ./... -bench=. -benchmem -benchtime=5s -count=1 | tee bench.txt

- name: Compare with baseline
  run: |
    benchstat main-bench.txt bench.txt
    # Fail if any benchmark regresses > 20%
```

**Store baseline:** On every merge to `main`, update `bench-baseline.txt` artifact in CI.

---

## 12. Memory Leak Check

```bash
# Run pprof heap profiler, compare before/after 10K operations
curl http://localhost:6060/debug/pprof/heap > heap-before.prof
# ... run 10K inserts ...
curl http://localhost:6060/debug/pprof/heap > heap-after.prof
go tool pprof -diff_base heap-before.prof heap-after.prof
```

**Add pprof endpoint to engine:** `import _ "net/http/pprof"` and register on debug port 6060.

**Expected:** After 10K inserts + 10K deletes + vacuum, heap in-use should return to near-baseline.
