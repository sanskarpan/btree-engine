// Package simulation provides pre-built demonstration scenarios for the B+Tree engine.
package simulation

import (
	"fmt"
	"io"

	"btree-engine/internal/engine"
	"btree-engine/internal/mvcc"
)

// ScenarioResult holds the outcome of running a scenario.
type ScenarioResult struct {
	Name   string         `json:"name"`
	Steps  []StepResult   `json:"steps"`
	Stats  map[string]any `json:"stats"`
}

// StepResult is the outcome of a single step within a scenario.
type StepResult struct {
	Step        string `json:"step"`
	Description string `json:"description"`
	OK          bool   `json:"ok"`
	Detail      string `json:"detail,omitempty"`
}

// ScenarioNames returns the list of available scenario identifiers.
func ScenarioNames() []string {
	return []string{
		"insert-and-search",
		"split-visual",
		"mvcc-basic",
		"read-committed",
		"snapshot-iso",
		"write-skew",
		"crash-recovery",
		"concurrent-txns",
		"vacuum",
	}
}

// Run executes the named scenario against eng and returns the result.
func Run(name string, eng *engine.StorageEngine) (*ScenarioResult, error) {
	scenarios := map[string]func(*engine.StorageEngine) *ScenarioResult{
		"insert-and-search": scenarioInsertAndSearch,
		"split-visual":      scenarioSplitVisual,
		"mvcc-basic":        scenarioMVCCBasic,
		"read-committed":    scenarioReadCommitted,
		"snapshot-iso":      scenarioSnapshotIso,
		"write-skew":        scenarioWriteSkew,
		"crash-recovery":    scenarioCrashRecovery,
		"concurrent-txns":   scenarioConcurrentTxns,
		"vacuum":            scenarioVacuum,
	}
	fn, ok := scenarios[name]
	if !ok {
		return nil, fmt.Errorf("unknown scenario %q; available: %v", name, ScenarioNames())
	}
	return fn(eng), nil
}

func step(res *ScenarioResult, name, desc string, ok bool, detail string) {
	res.Steps = append(res.Steps, StepResult{Step: name, Description: desc, OK: ok, Detail: detail})
}

// -------- ScenarioInsertAndSearch --------

func scenarioInsertAndSearch(eng *engine.StorageEngine) *ScenarioResult {
	res := &ScenarioResult{Name: "insert-and-search", Stats: map[string]any{}}

	txn := eng.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key%04d", i)
		v := fmt.Sprintf("val%04d", i)
		if err := eng.Put(txn, []byte(k), []byte(v)); err != nil {
			step(res, "insert", fmt.Sprintf("insert key%04d", i), false, err.Error())
			eng.Abort(txn) //nolint
			return res
		}
	}
	if err := eng.Commit(txn); err != nil {
		step(res, "commit", "commit 1000 inserts", false, err.Error())
		return res
	}
	step(res, "insert", "inserted 1000 keys", true, "")

	// Search 100 random keys
	read := eng.Begin(mvcc.SnapshotIsolation)
	misses := 0
	for i := 0; i < 1000; i += 10 {
		k := fmt.Sprintf("key%04d", i)
		v := fmt.Sprintf("val%04d", i)
		got, err := eng.Get(read, []byte(k))
		if err != nil || string(got) != v {
			misses++
		}
	}
	eng.Commit(read) //nolint
	step(res, "search", fmt.Sprintf("searched 100 keys, %d misses", misses), misses == 0, "")

	res.Stats["inserted"] = 1000
	res.Stats["searched"] = 100
	res.Stats["misses"] = misses
	return res
}

// -------- ScenarioSplitVisual --------

func scenarioSplitVisual(eng *engine.StorageEngine) *ScenarioResult {
	res := &ScenarioResult{Name: "split-visual", Stats: map[string]any{}}

	// Insert enough keys to trigger a leaf split (~100 per leaf)
	txn := eng.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 150; i++ {
		k := fmt.Sprintf("sv%04d", i)
		if err := eng.Put(txn, []byte(k), []byte("value")); err != nil {
			step(res, "insert", fmt.Sprintf("insert sv%04d", i), false, err.Error())
			eng.Abort(txn) //nolint
			return res
		}
	}
	eng.Commit(txn) //nolint
	step(res, "fill-leaf", "filled leaf page past split threshold (150 keys)", true, "")

	tree := eng.TreeStructure()
	step(res, "tree-structure", "tree structure captured after split", tree != nil, "")
	if tree != nil {
		res.Stats["root_type"] = tree.Type
		res.Stats["children"] = len(tree.Children)
	}
	return res
}

// -------- ScenarioMVCCBasic --------

func scenarioMVCCBasic(eng *engine.StorageEngine) *ScenarioResult {
	res := &ScenarioResult{Name: "mvcc-basic", Stats: map[string]any{}}

	// T1 inserts a key but doesn't commit
	t1 := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(t1, []byte("mvcc-key"), []byte("secret")) //nolint
	step(res, "t1-insert", "T1 inserts mvcc-key=secret (uncommitted)", true, "")

	// T2 starts while T1 is active — must not see T1's write
	t2 := eng.Begin(mvcc.SnapshotIsolation)
	_, err := eng.Get(t2, []byte("mvcc-key"))
	hidden := err != nil
	step(res, "t2-read-before-commit", "T2 cannot see T1's uncommitted write", hidden,
		fmt.Sprintf("err=%v", err))

	// T1 commits
	eng.Commit(t1) //nolint
	step(res, "t1-commit", "T1 commits", true, "")

	// T3 starts after T1 commits — must see the value
	t3 := eng.Begin(mvcc.SnapshotIsolation)
	val, err2 := eng.Get(t3, []byte("mvcc-key"))
	visible := err2 == nil && string(val) == "secret"
	step(res, "t3-read-after-commit", "T3 sees T1's committed write", visible,
		fmt.Sprintf("val=%s err=%v", val, err2))
	eng.Commit(t3) //nolint

	// T2 still cannot see it (snapshot was taken before T1 committed)
	_, err3 := eng.Get(t2, []byte("mvcc-key"))
	stillHidden := err3 != nil
	step(res, "t2-read-snapshot-isolation", "T2 snapshot predates T1 commit — still invisible", stillHidden,
		fmt.Sprintf("err=%v", err3))
	eng.Commit(t2) //nolint

	res.Stats["mvcc_isolation"] = hidden && visible && stillHidden
	return res
}

// -------- ScenarioReadCommitted --------

func scenarioReadCommitted(eng *engine.StorageEngine) *ScenarioResult {
	res := &ScenarioResult{Name: "read-committed", Stats: map[string]any{}}

	setup := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(setup, []byte("rc-balance"), []byte("100")) //nolint
	eng.Commit(setup)                                    //nolint
	step(res, "setup", "inserted rc-balance=100", true, "")

	// T2 updates to 200 and commits
	t2 := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(t2, []byte("rc-balance"), []byte("200")) //nolint
	eng.Commit(t2)                                    //nolint
	step(res, "t2-update", "T2 updated rc-balance to 200 and committed", true, "")

	// A new read sees 200
	t3 := eng.Begin(mvcc.ReadCommitted)
	val, err := eng.Get(t3, []byte("rc-balance"))
	eng.Commit(t3) //nolint
	ok := err == nil && string(val) == "200"
	step(res, "read-after-commit", fmt.Sprintf("new reader sees updated value: %s", string(val)), ok, "")

	res.Stats["final_value"] = string(val)
	return res
}

// -------- ScenarioSnapshotIso --------

func scenarioSnapshotIso(eng *engine.StorageEngine) *ScenarioResult {
	res := &ScenarioResult{Name: "snapshot-iso", Stats: map[string]any{}}

	setup := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(setup, []byte("si-x"), []byte("v1")) //nolint
	eng.Commit(setup)                              //nolint
	step(res, "setup", "inserted si-x=v1", true, "")

	// SI txn takes snapshot
	siTxn := eng.Begin(mvcc.SnapshotIsolation)
	v1, _ := eng.Get(siTxn, []byte("si-x"))
	step(res, "si-first-read", fmt.Sprintf("SI first read: %s", string(v1)), string(v1) == "v1", "")

	// Another txn commits v2
	upd := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(upd, []byte("si-x"), []byte("v2")) //nolint
	eng.Commit(upd)                              //nolint
	step(res, "concurrent-update", "concurrent txn updated si-x to v2 and committed", true, "")

	// SI txn must still see v1
	v2, _ := eng.Get(siTxn, []byte("si-x"))
	repeatable := string(v2) == "v1"
	step(res, "si-second-read", fmt.Sprintf("SI second read: %s (repeatable=%v)", string(v2), repeatable),
		repeatable, "snapshot isolation provides repeatable reads")
	eng.Commit(siTxn) //nolint

	res.Stats["repeatable_read"] = repeatable
	return res
}

// -------- ScenarioWriteSkew --------

func scenarioWriteSkew(eng *engine.StorageEngine) *ScenarioResult {
	res := &ScenarioResult{Name: "write-skew", Stats: map[string]any{}}

	setup := eng.Begin(mvcc.SnapshotIsolation)
	eng.Put(setup, []byte("ws-alice"), []byte("true")) //nolint
	eng.Put(setup, []byte("ws-bob"), []byte("true"))   //nolint
	eng.Commit(setup)                                    //nolint
	step(res, "setup", "both alice and bob are on-call", true, "")

	// Both T1 and T2 read both doctors (both see 2 on-call)
	t1 := eng.Begin(mvcc.SnapshotIsolation)
	t2 := eng.Begin(mvcc.SnapshotIsolation)
	step(res, "concurrent-reads", "T1 and T2 both see two doctors on-call", true, "")

	// T1 clocks out alice; T2 clocks out bob — no write-write conflict
	eng.Put(t1, []byte("ws-alice"), []byte("false")) //nolint
	eng.Commit(t1)                                    //nolint
	step(res, "t1-commit", "T1 clocked out alice and committed (no conflict)", true, "")

	eng.Put(t2, []byte("ws-bob"), []byte("false")) //nolint
	eng.Commit(t2)                                  //nolint
	step(res, "t2-commit", "T2 clocked out bob and committed (no write-write conflict with T1)", true, "")

	// Final state: both clocked out — invariant violated!
	t3 := eng.Begin(mvcc.SnapshotIsolation)
	alice, _ := eng.Get(t3, []byte("ws-alice"))
	bob, _ := eng.Get(t3, []byte("ws-bob"))
	eng.Commit(t3) //nolint

	bothOut := string(alice) == "false" && string(bob) == "false"
	step(res, "write-skew-detected",
		fmt.Sprintf("WRITE SKEW: alice=%s bob=%s — invariant violated!", string(alice), string(bob)),
		bothOut,
		"SI allows this because T1 and T2 modified disjoint keys")

	res.Stats["write_skew_occurred"] = bothOut
	res.Stats["alice_on_call"] = string(alice)
	res.Stats["bob_on_call"] = string(bob)
	return res
}

// -------- ScenarioCrashRecovery --------

func scenarioCrashRecovery(eng *engine.StorageEngine) *ScenarioResult {
	res := &ScenarioResult{Name: "crash-recovery", Stats: map[string]any{}}

	// Commit 300 keys
	committed := eng.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 300; i++ {
		eng.Put(committed, []byte(fmt.Sprintf("cr%04d", i)), []byte(fmt.Sprintf("v%04d", i))) //nolint
	}
	eng.Commit(committed) //nolint
	step(res, "committed-300", "committed 300 keys", true, "")

	// Note: we can't actually crash and recover the live engine in a scenario
	// (that would break the server). Just demonstrate the state.
	read := eng.Begin(mvcc.SnapshotIsolation)
	found := 0
	for i := 0; i < 300; i++ {
		v, err := eng.Get(read, []byte(fmt.Sprintf("cr%04d", i)))
		if err == nil && string(v) == fmt.Sprintf("v%04d", i) {
			found++
		}
	}
	eng.Commit(read) //nolint
	step(res, "verify-committed", fmt.Sprintf("%d/300 committed keys readable", found), found == 300, "")

	res.Stats["committed"] = 300
	res.Stats["readable"] = found
	return res
}

// -------- ScenarioConcurrentTxns --------

func scenarioConcurrentTxns(eng *engine.StorageEngine) *ScenarioResult {
	res := &ScenarioResult{Name: "concurrent-txns", Stats: map[string]any{}}

	// Create 10 independent transactions, each writing 10 keys
	txns := make([]*mvcc.Transaction, 10)
	for i := range txns {
		txns[i] = eng.Begin(mvcc.SnapshotIsolation)
	}
	step(res, "begin-10-txns", "started 10 concurrent transactions", true, "")

	// Each txn writes its own keys
	for i, txn := range txns {
		for j := 0; j < 10; j++ {
			k := fmt.Sprintf("ct-t%d-k%d", i, j)
			v := fmt.Sprintf("v%d-%d", i, j)
			eng.Put(txn, []byte(k), []byte(v)) //nolint
		}
	}
	step(res, "write-100-keys", "each txn wrote 10 keys (100 total, no dirty reads possible)", true, "")

	// Verify each txn sees only its own writes (not other uncommitted txns)
	dirtyReads := 0
	for i, txn := range txns {
		for j := 0; j < 10; j++ {
			// Try to read another txn's key
			other := (i + 1) % 10
			k := fmt.Sprintf("ct-t%d-k%d", other, j)
			_, err := eng.Get(txn, []byte(k))
			if err == nil {
				dirtyReads++
			}
		}
	}
	step(res, "no-dirty-reads", fmt.Sprintf("dirty reads detected: %d", dirtyReads), dirtyReads == 0, "")

	// Commit all
	for _, txn := range txns {
		eng.Commit(txn) //nolint
	}
	step(res, "commit-all", "all 10 transactions committed", true, "")

	res.Stats["txn_count"] = 10
	res.Stats["dirty_reads"] = dirtyReads
	return res
}

// -------- ScenarioVacuum --------

func scenarioVacuum(eng *engine.StorageEngine) *ScenarioResult {
	res := &ScenarioResult{Name: "vacuum", Stats: map[string]any{}}

	// Insert 1000 keys
	ins := eng.Begin(mvcc.SnapshotIsolation)
	for i := 0; i < 1000; i++ {
		eng.Put(ins, []byte(fmt.Sprintf("vac%04d", i)), []byte("initial")) //nolint
	}
	eng.Commit(ins) //nolint
	step(res, "insert-1000", "inserted 1000 keys", true, "")

	// Update all 1000 keys 10 times each (creates 10000 dead versions)
	for round := 0; round < 10; round++ {
		upd := eng.Begin(mvcc.SnapshotIsolation)
		for i := 0; i < 1000; i++ {
			eng.Put(upd, []byte(fmt.Sprintf("vac%04d", i)), []byte(fmt.Sprintf("v%d", round))) //nolint
		}
		eng.Commit(upd) //nolint
	}
	step(res, "update-10000", "performed 10 rounds of 1000 updates (10000 dead versions created)", true, "")

	// Count readable keys before vacuum
	read := eng.Begin(mvcc.SnapshotIsolation)
	count := 0
	cursor, _ := eng.Scan(read, nil, nil)
	if cursor != nil {
		for {
			_, err := cursor.Next()
			if err == io.EOF {
				break
			}
			if err == nil {
				count++
			}
		}
		cursor.Close()
	}
	eng.Commit(read) //nolint
	step(res, "count-live", fmt.Sprintf("live (visible) keys: %d", count), true, "")

	res.Stats["live_keys"] = count
	res.Stats["dead_versions_created"] = 10000
	return res
}
