package bw

import (
	"context"
	"testing"
)

type cacheVec struct {
	ID  string    `bw:"id,pk"`
	Emb []float32 `bw:"emb,vector(dim=3,metric=cosine)"`
}

// TestVectorCacheInvalidatedOnUpdate ensures the read-through vector cache
// never serves stale geometry after a record is overwritten: a query that
// matched the old vector must follow the new one.
func TestVectorCacheInvalidatedOnUpdate(t *testing.T) {
	ctx := context.Background()
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b, err := RegisterBucket[cacheVec](db, "cv")
	if err != nil {
		t.Fatal(err)
	}

	// Two anchors far apart, plus a moving point near anchor A.
	_ = b.Insert(ctx, &cacheVec{ID: "A", Emb: []float32{1, 0, 0}})
	_ = b.Insert(ctx, &cacheVec{ID: "B", Emb: []float32{0, 1, 0}})
	_ = b.Insert(ctx, &cacheVec{ID: "M", Emb: []float32{0.9, 0.1, 0}})

	// Warm the cache: query near A pulls M's vector into the cache.
	if _, err := b.SearchVector(ctx, []float32{1, 0, 0}, SearchVectorOptions{K: 3}); err != nil {
		t.Fatal(err)
	}

	// Move M next to B.
	if err := b.Insert(ctx, &cacheVec{ID: "M", Emb: []float32{0, 0.95, 0}}); err != nil {
		t.Fatal(err)
	}

	// A query near B must now rank M above A; if the cache were stale it
	// would still think M sits near A.
	res, err := b.SearchVector(ctx, []float32{0, 1, 0}, SearchVectorOptions{K: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) < 2 {
		t.Fatalf("want 2 hits, got %d", len(res))
	}
	rank := map[string]int{}
	for i, r := range res {
		rank[r.Record.ID] = i
	}
	mPos, mOK := rank["M"]
	aPos, aOK := rank["A"]
	if !mOK {
		t.Fatalf("M missing from results: %+v", res)
	}
	if aOK && mPos > aPos {
		t.Errorf("after update M should outrank A near B; got M@%d A@%d", mPos, aPos)
	}
}

// TestVectorCacheInvalidatedOnDelete ensures a deleted record stops
// appearing even after it was cached by a prior query.
func TestVectorCacheInvalidatedOnDelete(t *testing.T) {
	ctx := context.Background()
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b, err := RegisterBucket[cacheVec](db, "cv")
	if err != nil {
		t.Fatal(err)
	}

	_ = b.Insert(ctx, &cacheVec{ID: "A", Emb: []float32{1, 0, 0}})
	_ = b.Insert(ctx, &cacheVec{ID: "B", Emb: []float32{0, 1, 0}})

	// Warm cache.
	if _, err := b.SearchVector(ctx, []float32{1, 0, 0}, SearchVectorOptions{K: 2}); err != nil {
		t.Fatal(err)
	}

	if err := b.Delete(ctx, "A"); err != nil {
		t.Fatal(err)
	}

	res, err := b.SearchVector(ctx, []float32{1, 0, 0}, SearchVectorOptions{K: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.Record.ID == "A" {
			t.Fatalf("deleted record A still returned: %+v", res)
		}
	}
}
