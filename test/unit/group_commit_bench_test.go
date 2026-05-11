package unit

import (
	"fmt"
	"testing"
	"time"

	"btree-engine/internal/engine"
	"btree-engine/internal/mvcc"
)

// BenchmarkGroupCommit measures throughput with and without group commit.
// Run with: go test -bench=BenchmarkGroupCommit -benchtime=5s ./test/unit/
func BenchmarkGroupCommit(b *testing.B) {
	delays := []struct {
		name  string
		delay time.Duration
	}{
		{"NoGroupCommit_0ms", 0},
		{"GroupCommit_1ms", 1 * time.Millisecond},
		{"GroupCommit_5ms", 5 * time.Millisecond},
	}

	for _, d := range delays {
		d := d
		b.Run(d.name, func(b *testing.B) {
			dir := b.TempDir()
			cfg := engine.DefaultConfig(dir)
			cfg.CheckpointInterval = 0
			cfg.GroupCommitDelay = d.delay
			eng, err := engine.OpenEngine(cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer eng.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := eng.Begin(mvcc.SnapshotIsolation)
				key := []byte(fmt.Sprintf("key%d", i))
				if err := eng.Put(txn, key, []byte("value")); err != nil {
					b.Fatal(err)
				}
				if err := eng.Commit(txn); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGroupCommit_Concurrent measures concurrent commit throughput.
func BenchmarkGroupCommit_Concurrent(b *testing.B) {
	delays := []struct {
		name  string
		delay time.Duration
	}{
		{"NoGroupCommit", 0},
		{"GroupCommit_2ms", 2 * time.Millisecond},
	}
	workers := 8

	for _, d := range delays {
		d := d
		b.Run(d.name, func(b *testing.B) {
			dir := b.TempDir()
			cfg := engine.DefaultConfig(dir)
			cfg.CheckpointInterval = 0
			cfg.GroupCommitDelay = d.delay
			eng, err := engine.OpenEngine(cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer eng.Close()

			b.ResetTimer()
			b.SetParallelism(workers)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					txn := eng.Begin(mvcc.SnapshotIsolation)
					key := []byte(fmt.Sprintf("k%d", i))
					_ = eng.Put(txn, key, []byte("v"))
					_ = eng.Commit(txn)
					i++
				}
			})
		})
	}
}
