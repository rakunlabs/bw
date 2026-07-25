package bw

import (
	"fmt"
	"math"
	"testing"
)

// wideVec carries a vector wide enough that the cache budget, not an entry
// count, is what decides how many of them fit.
type wideVec struct {
	ID  string    `bw:"id,pk"`
	Emb []float32 `bw:"emb,vector(dim=128,metric=cosine)"`
}

// wideVector builds a distinct direction per seed. The components must not be
// periodic in the seed, or two records end up with the same vector and a
// nearest-neighbour assertion becomes a coin flip.
func wideVector(seed int) []float32 {
	v := make([]float32, 128)
	for i := range v {
		v[i] = float32(math.Sin(float64(seed)*0.7301 + float64(i)*0.1117))
	}

	return v
}

// TestVectorCacheHonoursByteBudget is the regression test for a cache bounded
// by entry count: 200,000 entries is 75 MB of 96-dimension vectors and 1.2 GB
// of 1536-dimension ones, so the bound has to be expressed in bytes for it to
// mean anything to a process with a memory limit.
func TestVectorCacheHonoursByteBudget(t *testing.T) {
	// Room for a handful of entries only, so eviction is exercised heavily.
	const budget = 8 << 10

	db, err := Open("", WithInMemory(true), WithLogger(nil), WithVectorCacheBytes(budget))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b, err := RegisterBucket[wideVec](db, "wv")
	if err != nil {
		t.Fatal(err)
	}

	for i := range 400 {
		if err := b.Insert(t.Context(), &wideVec{ID: fmt.Sprintf("v%03d", i), Emb: wideVector(i)}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Searching walks the graph, which is what fills the cache.
	for i := range 20 {
		if _, err := b.SearchVector(t.Context(), wideVector(i*7), SearchVectorOptions{K: 10}); err != nil {
			t.Fatalf("search %d: %v", i, err)
		}
	}

	cache := db.vec.get("wv").cache

	cache.mu.RLock()
	used, entries := cache.bytes, len(cache.m)
	cache.mu.RUnlock()

	if used > budget {
		t.Errorf("cache holds %d bytes, over its %d byte budget", used, budget)
	}
	if entries == 0 {
		t.Error("cache is empty; the budget starved it entirely")
	}

	// The accounting must match what is actually in the map, otherwise the
	// budget drifts away from reality over a long-running process.
	var recomputed int64
	cache.mu.RLock()
	for k, e := range cache.m {
		recomputed += entryBytes(k, e)
	}
	cache.mu.RUnlock()

	if recomputed != used {
		t.Errorf("tracked size %d does not match the entries' actual size %d", used, recomputed)
	}
}

// TestVectorCacheAccountingSurvivesChurn checks the byte counter against the
// operations that mutate it from different directions: overwrites (which must
// replace, not double-count) and invalidations (which must give the bytes
// back).
func TestVectorCacheAccountingSurvivesChurn(t *testing.T) {
	c := newVecCache(1 << 20)

	entry := func(dim int) vecEntry { return vecEntry{vec: make([]float32, dim)} }

	c.put([]byte("a"), entry(16))
	c.put([]byte("b"), entry(16))
	afterTwo := c.bytes

	// Overwriting a key must not count it twice.
	c.put([]byte("a"), entry(16))
	if c.bytes != afterTwo {
		t.Errorf("overwrite changed the tracked size: %d, want %d", c.bytes, afterTwo)
	}

	// Overwriting with a different width must be reflected.
	c.put([]byte("a"), entry(32))
	if c.bytes != afterTwo+16*4 {
		t.Errorf("widening an entry tracked %d, want %d", c.bytes, afterTwo+16*4)
	}

	c.invalidate([]byte("a"))
	c.invalidate([]byte("b"))
	if c.bytes != 0 {
		t.Errorf("tracked size after removing everything = %d, want 0", c.bytes)
	}

	// Invalidating an absent key must not drive the counter negative.
	c.invalidate([]byte("missing"))
	if c.bytes != 0 {
		t.Errorf("invalidating an absent key changed the size to %d", c.bytes)
	}

	c.put([]byte("c"), entry(16))
	c.clear()
	if c.bytes != 0 || len(c.m) != 0 {
		t.Errorf("clear left %d bytes in %d entries", c.bytes, len(c.m))
	}
}

// TestVectorCacheRejectsOversizedEntry guards the eviction loop: an entry
// larger than the whole budget can never fit, and trying to make room for it
// would walk the map to no effect.
func TestVectorCacheRejectsOversizedEntry(t *testing.T) {
	c := newVecCache(256)

	c.put([]byte("small"), vecEntry{vec: make([]float32, 4)})
	before := c.bytes

	c.put([]byte("huge"), vecEntry{vec: make([]float32, 4096)})

	if _, ok := c.m["huge"]; ok {
		t.Error("an entry larger than the budget was cached")
	}
	if c.bytes != before {
		t.Errorf("tracked size changed to %d after a rejected put, want %d", c.bytes, before)
	}
	if _, ok := c.m["small"]; !ok {
		t.Error("the rejected put evicted an entry that fit")
	}
}

// TestDefaultVectorCacheBytesApplied makes sure a database opened without the
// option still gets a bound, rather than an unbounded cache.
func TestDefaultVectorCacheBytesApplied(t *testing.T) {
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := RegisterBucket[wideVec](db, "wv"); err != nil {
		t.Fatal(err)
	}

	if got := db.vec.get("wv").cache.budget; got != DefaultVectorCacheBytes {
		t.Errorf("cache budget = %d, want the default %d", got, DefaultVectorCacheBytes)
	}
}

// TestVectorCacheEvictionKeepsSearchCorrect is the point of the whole cache:
// evicting entries may cost latency but must never change an answer.
func TestVectorCacheEvictionKeepsSearchCorrect(t *testing.T) {
	const budget = 4 << 10 // a few entries at most

	db, err := Open("", WithInMemory(true), WithLogger(nil), WithVectorCacheBytes(budget))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b, err := RegisterBucket[wideVec](db, "wv")
	if err != nil {
		t.Fatal(err)
	}

	for i := range 200 {
		if err := b.Insert(t.Context(), &wideVec{ID: fmt.Sprintf("v%03d", i), Emb: wideVector(i)}); err != nil {
			t.Fatal(err)
		}
	}

	// An exact query must find its own record first even though the cache
	// cannot hold the graph.
	target := 137
	hits, err := b.SearchVector(t.Context(), wideVector(target), SearchVectorOptions{K: 1, EfSearch: 200})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("got %d hits, want 1", len(hits))
	}
	if want := fmt.Sprintf("v%03d", target); hits[0].Record.ID != want {
		t.Errorf("nearest neighbour = %s, want %s", hits[0].Record.ID, want)
	}
}
