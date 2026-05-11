package unit

import (
	"fmt"
	"testing"

	"btree-engine/internal/btree"
	"btree-engine/internal/buffer"
	"btree-engine/internal/disk"
	"btree-engine/internal/mvcc"
	"btree-engine/internal/page"
)

// ── BloomFilter unit tests ────────────────────────────────────────────────────

func TestBloomFilter_AddAndMayContain(t *testing.T) {
	var f btree.BloomFilter
	key := []byte("hello")
	if f.MayContain(key) {
		t.Fatal("empty filter should return false")
	}
	f.Add(key)
	if !f.MayContain(key) {
		t.Fatal("key should be present after Add")
	}
}

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	var f btree.BloomFilter
	f.Add([]byte("present"))
	if !f.MayContain([]byte("present")) {
		t.Fatal("false negative: key was added but MayContain returned false")
	}
}

func TestBloomFilter_Reset(t *testing.T) {
	var f btree.BloomFilter
	f.Add([]byte("x"))
	f.Reset()
	if f.MayContain([]byte("x")) {
		t.Fatal("key should not be present after Reset")
	}
}

func TestBloomFilter_EncodeDecodeRoundtrip(t *testing.T) {
	var f btree.BloomFilter
	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	for _, k := range keys {
		f.Add(k)
	}
	enc := f.Encode()
	f2 := btree.DecodeBloomFilter(enc)
	for _, k := range keys {
		if !f2.MayContain(k) {
			t.Fatalf("key %q missing after decode roundtrip", k)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	// With a 1024-bit filter and 3 hash functions, the FPR at 100 entries is ≈ 1.6 %.
	// We verify it stays below 15 % (generous bound to avoid flakiness).
	var f btree.BloomFilter
	const n = 100
	for i := 0; i < n; i++ {
		f.Add([]byte(fmt.Sprintf("key-%05d", i)))
	}
	fps := 0
	const probes = 10000
	for i := 0; i < probes; i++ {
		if f.MayContain([]byte(fmt.Sprintf("absent-%07d", i))) {
			fps++
		}
	}
	fpr := float64(fps) / float64(probes)
	if fpr > 0.15 {
		t.Fatalf("false positive rate %.2f%% exceeds 15%% threshold", fpr*100)
	}
}

// ── BloomIndex unit tests ─────────────────────────────────────────────────────

func TestBloomIndex_AddMayContain(t *testing.T) {
	bi := btree.NewBloomIndex()
	bi.Add(1, []byte("k1"))
	if !bi.MayContain(1, []byte("k1")) {
		t.Fatal("k1 must be present in page 1")
	}
}

func TestBloomIndex_MissingPageDefaultsTrue(t *testing.T) {
	bi := btree.NewBloomIndex()
	// No filter for page 2 — safe default is true (never claim absent without data).
	if !bi.MayContain(2, []byte("any")) {
		t.Fatal("missing page filter should default to true")
	}
}

func TestBloomIndex_Rebuild(t *testing.T) {
	bi := btree.NewBloomIndex()
	bi.Add(1, []byte("dead"))
	bi.Rebuild(1, [][]byte{[]byte("live")})
	if !bi.MayContain(1, []byte("live")) {
		t.Fatal("live key must be present after Rebuild")
	}
}

func TestBloomIndex_Remove(t *testing.T) {
	bi := btree.NewBloomIndex()
	bi.Add(5, []byte("key"))
	bi.Remove(5)
	// After Remove, missing filter should default to true (safe).
	if !bi.MayContain(5, []byte("key")) {
		t.Fatal("after Remove, missing filter should default to true")
	}
}

func TestBloomIndex_Stats(t *testing.T) {
	bi := btree.NewBloomIndex()
	bi.Add(1, []byte("a"))
	bi.Add(2, []byte("b"))
	stats := bi.Stats()
	if stats["pages_with_filter"].(int) != 2 {
		t.Fatalf("expected 2 pages with filter, got %v", stats["pages_with_filter"])
	}
}

// ── Integration: bloom wired into BPlusTree ───────────────────────────────────

func TestBPlusTree_BloomMissingKeyFastPath(t *testing.T) {
	// Verify Get returns ErrKeyNotFound for a key never inserted (bloom early-exit).
	dir := t.TempDir()
	dm, _ := disk.Open(dir + "/bloom.db")
	defer dm.Close()

	bp := buffer.New(64, dm, nil, nil)
	tm := mvcc.NewTransactionManager(nil)

	rootID, _ := dm.AllocPage()
	initFrame, _ := bp.FetchPage(rootID)
	initPg := bp.ReadPageData(initFrame)
	initPg.Init(page.PageTypeLeaf)
	initPg.SetFlag(page.FlagIsRoot)
	initPg.UpdateChecksum()
	bp.WritePageData(rootID, initPg)
	bp.UnpinPage(rootID, true)

	tree := btree.New(rootID, bp, nil, dm, tm, nil)

	txn := tm.Begin(mvcc.ReadCommitted)
	_, err := tree.Get(txn, []byte("missing-key"))
	if err != btree.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestBPlusTree_BloomAfterInsert(t *testing.T) {
	dir := t.TempDir()
	dm, _ := disk.Open(dir + "/bloom2.db")
	defer dm.Close()

	bp := buffer.New(64, dm, nil, nil)
	tm := mvcc.NewTransactionManager(nil)

	rootID, _ := dm.AllocPage()
	initFrame, _ := bp.FetchPage(rootID)
	initPg := bp.ReadPageData(initFrame)
	initPg.Init(page.PageTypeLeaf)
	initPg.SetFlag(page.FlagIsRoot)
	initPg.UpdateChecksum()
	bp.WritePageData(rootID, initPg)
	bp.UnpinPage(rootID, true)

	tree := btree.New(rootID, bp, nil, dm, tm, nil)

	txn := tm.Begin(mvcc.ReadCommitted)
	if err := tree.Insert(txn, []byte("bloom-key"), []byte("val")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	tm.Commit(txn)

	txn2 := tm.Begin(mvcc.ReadCommitted)
	val, err := tree.Get(txn2, []byte("bloom-key"))
	if err != nil {
		t.Fatalf("Get after Insert: %v", err)
	}
	if string(val) != "val" {
		t.Fatalf("expected 'val', got %q", val)
	}
}
