package unit

import (
	"fmt"
	"math/rand"
	"testing"

	"btree-engine/internal/btree"
	"btree-engine/internal/buffer"
	"btree-engine/internal/disk"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"
)

// newBenchTree creates a fresh B+Tree backed by an in-memory-like on-disk store in a temp dir.
func newBenchTree(b *testing.B) (*btree.BPlusTree, *mvcc.TransactionManager, func()) {
	b.Helper()
	dir := b.TempDir()
	dm, err := disk.Open(dir + "/bench.db")
	if err != nil {
		b.Fatal(err)
	}
	bp := buffer.New(512, dm, nil, nil)
	tm := mvcc.NewTransactionManager(nil)

	rootID, err := dm.AllocPage()
	if err != nil {
		b.Fatal(err)
	}
	initFrame, err := bp.FetchPage(rootID)
	if err != nil {
		b.Fatal(err)
	}
	p := bp.ReadPageData(initFrame)
	p.Init(page.PageTypeLeaf)
	p.SetFlag(page.FlagIsRoot)
	p.UpdateChecksum()
	bp.WritePageData(rootID, p)
	bp.UnpinPage(rootID, true)

	tree := btree.New(rootID, bp, nil, dm, tm, nil)
	return tree, tm, func() { dm.Close() }
}

// BenchmarkSequentialInsert measures throughput for inserting keys in ascending order.
func BenchmarkSequentialInsert(b *testing.B) {
	tree, tm, cleanup := newBenchTree(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%010d", i)
		txn := tm.Begin(mvcc.ReadCommitted)
		if err := tree.Insert(txn, []byte(key), []byte("value")); err != nil {
			tm.Abort(txn)
			b.Fatal(err)
		}
		tm.Commit(txn)
	}
}

// BenchmarkRandomInsert measures throughput for inserting keys in random order.
func BenchmarkRandomInsert(b *testing.B) {
	tree, tm, cleanup := newBenchTree(b)
	defer cleanup()

	// Pre-generate keys to avoid rand overhead inside the timed loop.
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%010d", rand.Int63()) //nolint:gosec
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := tm.Begin(mvcc.ReadCommitted)
		if err := tree.Insert(txn, []byte(keys[i]), []byte("value")); err != nil {
			tm.Abort(txn)
			// Ignore duplicate-key errors during random insert.
			continue
		}
		tm.Commit(txn)
	}
}

// BenchmarkHotGet measures throughput for reading a single hot key repeatedly.
func BenchmarkHotGet(b *testing.B) {
	tree, tm, cleanup := newBenchTree(b)
	defer cleanup()

	// Seed with one key.
	txn := tm.Begin(mvcc.ReadCommitted)
	if err := tree.Insert(txn, []byte("hotkey"), []byte("value")); err != nil {
		b.Fatal(err)
	}
	tm.Commit(txn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := tm.Begin(mvcc.ReadCommitted)
		_, err := tree.Get(txn, []byte("hotkey"))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkColdGet measures throughput for reading distinct keys (cold path, no bloom skip).
func BenchmarkColdGet(b *testing.B) {
	const seed = 1000
	tree, tm, cleanup := newBenchTree(b)
	defer cleanup()

	// Seed with many keys.
	for i := 0; i < seed; i++ {
		key := fmt.Sprintf("key-%05d", i)
		txn := tm.Begin(mvcc.ReadCommitted)
		if err := tree.Insert(txn, []byte(key), []byte("v")); err != nil {
			tm.Abort(txn)
		} else {
			tm.Commit(txn)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%05d", i%seed)
		txn := tm.Begin(mvcc.ReadCommitted)
		tree.Get(txn, []byte(key)) //nolint
	}
}

// BenchmarkRangeScan measures throughput for scanning a 100-key range.
func BenchmarkRangeScan(b *testing.B) {
	const totalKeys = 2000
	tree, tm, cleanup := newBenchTree(b)
	defer cleanup()

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key-%05d", i)
		txn := tm.Begin(mvcc.ReadCommitted)
		if err := tree.Insert(txn, []byte(key), []byte("v")); err != nil {
			tm.Abort(txn)
		} else {
			tm.Commit(txn)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := fmt.Sprintf("key-%05d", (i*100)%totalKeys)
		end := fmt.Sprintf("key-%05d", ((i*100)+100)%totalKeys)
		if start > end {
			start, end = end, start
		}
		txn := tm.Begin(mvcc.ReadCommitted)
		cur, err := tree.Scan(txn, []byte(start), []byte(end))
		if err == nil {
			for {
				_, serr := cur.Next()
				if serr != nil {
					break
				}
			}
			cur.Close()
		}
	}
}

// BenchmarkConcurrentWriters measures throughput with multiple goroutines inserting in parallel.
func BenchmarkConcurrentWriters(b *testing.B) {
	tree, tm, cleanup := newBenchTree(b)
	defer cleanup()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d-%d", rand.Int63(), i) //nolint:gosec
			txn := tm.Begin(mvcc.ReadCommitted)
			if err := tree.Insert(txn, []byte(key), []byte("v")); err != nil {
				tm.Abort(txn)
			} else {
				tm.Commit(txn)
			}
			i++
		}
	})
}
