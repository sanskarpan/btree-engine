// Package btree — per-leaf-page Bloom filters (P3-017/P3-018/P3-019).
//
// A BloomFilter is a 1024-bit (128-byte) probabilistic set membership structure.
// It uses 3 independent hash functions derived from FNV-1a with different seeds.
// Expected false-positive rates (FPR = (1-e^(-kn/m))^k, k=3, m=1024):
//   100 entries ≈ 1.6 %,  50 entries ≈ 0.3 %,  500 entries ≈ 45 %.
//
// Filters are in-memory only: they do not need WAL protection or crash recovery
// because they can be rebuilt from the page data on restart.
package btree

import (
	"encoding/binary"
	"sync"
)

const bloomBits = 1024              // 1024-bit filter
const bloomBytes = bloomBits / 8   // 128 bytes
const bloomHashCount = 3           // number of independent hash functions

// BloomFilter is a 1024-bit Bloom filter for a single leaf page.
type BloomFilter struct {
	bits [bloomBytes]byte
}

// fnv1a64 is a FNV-1a 64-bit hash seeded with a given offset basis.
func fnv1a64(data []byte, seed uint64) uint64 {
	const prime = 1099511628211
	h := seed
	for _, b := range data {
		h ^= uint64(b)
		h *= prime
	}
	return h
}

// bloomHashK returns the k-th bit position (0 < bloomBits) for key.
func bloomHashK(key []byte, k int) uint64 {
	// Use FNV-1a with different offset-basis seeds for each hash function.
	seeds := [bloomHashCount]uint64{
		14695981039346656037, // FNV-1a standard offset basis
		2166136261,           // FNV-1a 32-bit basis promoted to 64-bit
		0xc4ceb9fe1a85ec53,   // arbitrary seed
	}
	// Double-hashing: h_i(key) = h1(key) + i * h2(key)
	h1 := fnv1a64(key, seeds[0])
	h2 := fnv1a64(key, seeds[1])
	h := h1 + uint64(k)*h2
	// XOR-fold the extra seed for the third function
	if k == 2 {
		h ^= fnv1a64(key, seeds[2])
	}
	return h % bloomBits
}

// Add sets the bloom filter bits for key.
func (f *BloomFilter) Add(key []byte) {
	for k := 0; k < bloomHashCount; k++ {
		bit := bloomHashK(key, k)
		f.bits[bit/8] |= 1 << (bit % 8)
	}
}

// MayContain returns false if key is definitely not in the set,
// or true if it might be (with a small false-positive probability).
func (f *BloomFilter) MayContain(key []byte) bool {
	for k := 0; k < bloomHashCount; k++ {
		bit := bloomHashK(key, k)
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false // definitely absent
		}
	}
	return true // probably present
}

// Reset clears all bits.
func (f *BloomFilter) Reset() {
	f.bits = [bloomBytes]byte{}
}

// Encode serialises the filter to 128 bytes.
func (f *BloomFilter) Encode() []byte {
	b := make([]byte, bloomBytes)
	copy(b, f.bits[:])
	return b
}

// DecodeBloomFilter deserialises a BloomFilter from 128 bytes.
func DecodeBloomFilter(b []byte) *BloomFilter {
	f := &BloomFilter{}
	n := bloomBytes
	if len(b) < n {
		n = len(b)
	}
	copy(f.bits[:n], b)
	return f
}

// ── BloomIndex ───────────────────────────────────────────────────────────────

// BloomIndex manages per-leaf-page Bloom filters for a BPlusTree.
// All methods are safe for concurrent use.
type BloomIndex struct {
	mu      sync.RWMutex
	filters map[uint32]*BloomFilter // pageID → filter
}

func newBloomIndex() *BloomIndex {
	return &BloomIndex{filters: make(map[uint32]*BloomFilter)}
}

// NewBloomIndex creates and returns a new empty BloomIndex.
// Exposed for use by tests that import the btree package directly.
func NewBloomIndex() *BloomIndex { return newBloomIndex() }

// Add records that key was inserted into leaf page pageID.
func (bi *BloomIndex) Add(pageID uint32, key []byte) {
	bi.mu.Lock()
	defer bi.mu.Unlock()
	f := bi.filters[pageID]
	if f == nil {
		f = &BloomFilter{}
		bi.filters[pageID] = f
	}
	f.Add(key)
}

// MayContain returns false only when key is definitely absent from pageID.
func (bi *BloomIndex) MayContain(pageID uint32, key []byte) bool {
	bi.mu.RLock()
	f := bi.filters[pageID]
	bi.mu.RUnlock()
	if f == nil {
		return true // no filter yet → assume present (safe default)
	}
	return f.MayContain(key)
}

// Rebuild replaces the filter for pageID by scanning all keys in keyList.
// Called by vacuum/compaction after dead tuples are removed.
func (bi *BloomIndex) Rebuild(pageID uint32, keys [][]byte) {
	f := &BloomFilter{}
	for _, k := range keys {
		f.Add(k)
	}
	bi.mu.Lock()
	bi.filters[pageID] = f
	bi.mu.Unlock()
}

// Remove discards the filter for pageID (e.g. when the page is freed).
func (bi *BloomIndex) Remove(pageID uint32) {
	bi.mu.Lock()
	delete(bi.filters, pageID)
	bi.mu.Unlock()
}

// Stats returns the number of pages that have a bloom filter entry.
func (bi *BloomIndex) Stats() map[string]interface{} {
	bi.mu.RLock()
	n := len(bi.filters)
	bi.mu.RUnlock()
	totalBits := bloomBits
	_ = binary.LittleEndian // suppress import
	return map[string]interface{}{
		"pages_with_filter": n,
		"bits_per_filter":   totalBits,
		"hash_functions":    bloomHashCount,
	}
}
