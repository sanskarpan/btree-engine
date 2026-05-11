# P2-001 to P2-008 — Serializable Snapshot Isolation (SSI)

**Priority:** P2 (Medium — correctness improvement)
**Phase:** 2-A
**Effort:** XL (3-4 weeks)
**Depends on:** none (self-contained extension)
**Blocks:** P2-013 (deadlock detection complements this)

---

## Problem Statement

Snapshot Isolation allows the **write-skew anomaly**. The write-skew scenario is actively demonstrated in `internal/simulation/scenarios.go`:

```
T1 reads: alice=true, bob=true  → decides to clock out alice
T2 reads: alice=true, bob=true  → decides to clock out bob
T1 commits: alice=false
T2 commits: bob=false
Result: both doctors off-call — invariant violated
```

For applications with multi-key integrity constraints (financial balances, inventory allocation, at-least-one-on-call rules), this is a correctness bug, not an acceptable limitation.

PostgreSQL solved this with **Serializable Snapshot Isolation (SSI)** (Ports & Grabs, 2012). SSI achieves full serializability at roughly the same throughput as SI by tracking read-write antidependencies and aborting transactions that form dangerous cycles.

---

## Goals

1. New isolation level: `Serializable` (alias: `SSI`)
2. Write-skew impossible under Serializable level
3. Read throughput unchanged (reads never block)
4. Abort rate proportional to actual conflicts (not all concurrent transactions)
5. SIREAD locks released immediately on commit/abort (no long-lived reader locks)

---

## Background: SSI Algorithm

### Dependency Types

| Type | When | Meaning |
|------|------|---------|
| **wr-dependency** | T1 writes X, T2 reads X after T1 commits | T2 reads T1's effect |
| **ww-dependency** | T1 writes X, T2 overwrites X | T2 supersedes T1 |
| **rw-antidependency** | T1 reads X at version V, T2 writes X (new version V') | T2 creates data T1 would read differently |

**Dangerous structure (write skew):**
```
T1 ─rw→ T2 ─rw→ T1   (cycle of rw-antidependencies)
```

T1 reads something that T2 later modifies, AND T2 reads something that T1 later modifies. If both commit, the result is not serializable.

### SSI Rule
When a rw-antidependency cycle is detected: **abort one transaction in the cycle** (typically the one that committed last or has done less work — youngest).

---

## Technical Design

### P2-001 — SIREADLock Struct

```go
// internal/mvcc/siread.go

// SIREADLock records that a transaction read from a specific page and key range.
type SIREADLock struct {
    TxnID    TxnID
    PageID   uint32
    KeyHash  uint64 // FNV-64 hash of the key (not the full key for memory efficiency)
}
```

### P2-002 — SIREADLockManager

```go
// internal/mvcc/siread.go

type SIREADLockManager struct {
    mu       sync.RWMutex
    readSets map[TxnID][]SIREADLock    // locks held by each active transaction
    rwEdges  map[TxnID]map[TxnID]bool  // rw-antidependencies: src → dst
}

func NewSIREADLockManager() *SIREADLockManager

// AcquireRead records that txnID read key from pageID.
func (m *SIREADLockManager) AcquireRead(txnID TxnID, pageID uint32, key []byte)

// CheckAndRecordWrite checks for rw-antidependencies when txnID writes to key on pageID.
// Returns the TxnID to abort if a dangerous cycle is formed, or 0 if safe.
func (m *SIREADLockManager) CheckAndRecordWrite(txnID TxnID, pageID uint32, key []byte) (TxnID, bool)

// Release removes all SIREAD locks for txnID.
func (m *SIREADLockManager) Release(txnID TxnID)

// hasCycle detects if adding edge src→dst creates a cycle in rwEdges.
func (m *SIREADLockManager) hasCycle(src, dst TxnID) bool
```

### Cycle Detection (DFS)

```go
func (m *SIREADLockManager) hasCycle(src, dst TxnID) bool {
    // Does a path from dst → src already exist?
    // If yes, adding src→dst creates a cycle.
    visited := map[TxnID]bool{}
    var dfs func(TxnID) bool
    dfs = func(cur TxnID) bool {
        if cur == src { return true } // reached src from dst → cycle
        if visited[cur] { return false }
        visited[cur] = true
        for next := range m.rwEdges[cur] {
            if dfs(next) { return true }
        }
        return false
    }
    return dfs(dst)
}
```

**Complexity:** O(V + E) where V = active transactions, E = rw-edges. With 100 concurrent transactions and typical workloads, this is microseconds.

### P2-003 — Acquire SIREAD on Get()

In `internal/btree/tree.go`, `Get()`:

```go
func (bt *BPlusTree) Get(txn *mvcc.Transaction, key []byte) ([]byte, error) {
    if txn.IsoLevel == mvcc.Serializable {
        bt.tm.SIREADMgr.AcquireRead(txn.ID, leafPageID, key)
    }
    // ... existing Get logic ...
}
```

**Note:** SIREAD lock acquired AFTER the read, with the actual page ID determined during traversal.

### P2-004 — Check rw-Antidependency on Write

In `internal/btree/tree.go`, `Insert()`, `Update()`, `Delete()`:

```go
func (bt *BPlusTree) Insert(txn *mvcc.Transaction, key, value []byte) error {
    if txn.IsoLevel == mvcc.Serializable {
        if victim, cycle := bt.tm.SIREADMgr.CheckAndRecordWrite(txn.ID, leafPageID, key); cycle {
            // Abort the victim (or self if victim == txn.ID)
            if victim == txn.ID {
                return ErrSerializationFailure
            }
            // Signal victim to abort (via transaction manager)
            bt.tm.AbortSerializability(victim)
            return ErrSerializationFailure
        }
    }
    // ... existing Insert logic ...
}
```

### P2-005 — New Isolation Level

In `internal/mvcc/transaction.go`:

```go
type IsolationLevel int

const (
    ReadCommitted    IsolationLevel = iota
    SnapshotIsolation
    Serializable     // NEW
)
```

In `internal/mvcc/transaction.go`:

```go
type TransactionManager struct {
    // existing fields ...
    SIREADMgr *SIREADLockManager // NEW — nil if unused
}

func NewTransactionManager(bus *events.EventBus) *TransactionManager {
    return &TransactionManager{
        // existing init ...
        SIREADMgr: NewSIREADLockManager(),
    }
}
```

### P2-006 — SIREAD Lock Release

In `Commit()` and `Abort()`:

```go
func (tm *TransactionManager) Commit(txn *Transaction) error {
    // existing ...
    tm.SIREADMgr.Release(txn.ID)
    return nil
}
```

### P2-007 — Serializable Scenario

In `internal/simulation/scenarios.go`:

```go
func scenarioSerializable(eng *engine.StorageEngine) *ScenarioResult {
    res := &ScenarioResult{Name: "serializable", Stats: map[string]any{}}

    // Setup: both doctors on-call
    setup := eng.Begin(mvcc.SnapshotIsolation)
    eng.Put(setup, []byte("ser-alice"), []byte("true"))
    eng.Put(setup, []byte("ser-bob"), []byte("true"))
    eng.Commit(setup)

    // T1 and T2 under SERIALIZABLE — write skew must be prevented
    t1 := eng.Begin(mvcc.Serializable)
    t2 := eng.Begin(mvcc.Serializable)

    // Both read both doctors
    eng.Get(t1, []byte("ser-alice"))
    eng.Get(t1, []byte("ser-bob"))
    eng.Get(t2, []byte("ser-alice"))
    eng.Get(t2, []byte("ser-bob"))

    eng.Put(t1, []byte("ser-alice"), []byte("false"))
    eng.Put(t2, []byte("ser-bob"), []byte("false"))

    err1 := eng.Commit(t1)
    err2 := eng.Commit(t2)

    // One must succeed, one must abort
    oneAborted := (err1 != nil) != (err2 != nil)
    step(res, "one-aborted", "one transaction aborted to prevent write skew", oneAborted, "")

    res.Stats["t1_ok"] = err1 == nil
    res.Stats["t2_ok"] = err2 == nil
    return res
}
```

### P2-008 — Test

```go
// test/unit/btree_test.go

func TestSerializable_WriteSkewPrevented(t *testing.T) {
    // Same doctors scenario
    // After both commits attempted, exactly one must have returned ErrSerializationFailure
}

func TestSerializable_IndependentTransactionsCommit(t *testing.T) {
    // T1 writes key "a", T2 writes key "b" — no shared reads
    // Both must succeed (no false positives)
}

func TestSerializable_ReadOnlyAlwaysCommits(t *testing.T) {
    // Read-only transaction under Serializable must always commit
}
```

---

## New Error Type

```go
var ErrSerializationFailure = errors.New("serialization failure: transaction aborted to prevent anomaly")
```

**REST response:**
```json
HTTP 409
{"error": "serialization_failure: transaction aborted to prevent anomaly; retry the transaction"}
```

---

## Performance Implications

**Memory:** SIREAD lock set per transaction. At 100 concurrent transactions, each reading 100 keys: 100 × 100 × ~24 bytes = ~240 KB (trivial).

**CPU:** Cycle detection on every write: O(active_txns + rw_edges). At 100 concurrent transactions: O(100) per write. At 10K writes/sec: ~100K cycle checks/sec = ~1ms total CPU per second (< 0.01% overhead).

**Abort rate:** Under normal OLTP (each transaction touches disjoint key ranges), the abort rate is near zero. Under write-skew-prone workloads, the abort rate reflects the actual anomaly rate.

---

## Testing Plan

### Unit Tests

```go
func TestSIREAD_AcquireAndRelease(t *testing.T)
func TestSIREAD_CycleDetection_TwoTxns(t *testing.T)
func TestSIREAD_NoCycleOnDisjointKeys(t *testing.T)
func TestSIREAD_ReleaseOnCommit(t *testing.T)
func TestSerializable_WriteSkewPrevented(t *testing.T)
func TestSerializable_IndependentTxnsSucceed(t *testing.T)
func TestSerializable_ReadOnlyAlwaysSucceeds(t *testing.T)
```

### Integration Tests

```go
func TestSerializable_DoctorsScenario(t *testing.T)
func TestSerializable_BankTransfer_Consistent(t *testing.T)
// T1: check(a)>100 → withdraw(a,-50); T2: check(b)>100 → withdraw(b,-50)
// Initial: a=200, b=200. Under SI: both succeed, total balance 300 (ok here since disjoint)
// But: T1: check(a+b)>100 → withdraw(a,-50); T2: check(a+b)>100 → withdraw(b,-50)
// Under Serializable: one aborts
```

---

## Rollout Strategy

1. Implement `SIREADLockManager` with tests (isolated, no engine changes)
2. Add `Serializable` isolation level to `TransactionManager`
3. Wire SIREAD acquisition into `Get()` — no write-path changes yet
4. Wire antidependency check into write paths
5. Add serializable scenario to simulation
6. Gate behind config: `mvcc.default_isolation: "serializable"` opt-in

---

## Definition of Done

- [ ] `SIREADLockManager` implemented with cycle detection
- [ ] `Serializable` isolation level available in transaction manager
- [ ] SIREAD lock acquired on all `Get()` calls under Serializable
- [ ] rw-antidependency checked on all write operations
- [ ] `ErrSerializationFailure` returned correctly; REST returns 409
- [ ] Doctors scenario correctly aborts one transaction under Serializable
- [ ] Read-only transactions never abort under Serializable
- [ ] Independent transactions never produce false-positive aborts
- [ ] Memory usage of SIREAD lock set < 1 MB for 100 concurrent transactions
- [ ] Serializable scenario added to simulation package
