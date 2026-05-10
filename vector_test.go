package bw_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/query"
)

type VecDoc struct {
	ID    string    `bw:"id,pk"`
	Title string    `bw:"title"`
	Tag   string    `bw:"tag,index"`
	Embed []float32 `bw:"embed,vector(dim=4,metric=cosine)"`
}

// L2-normalise so cosine and dot product produce identical scores.
func unit(v []float32) []float32 {
	var n float64
	for _, x := range v {
		n += float64(x) * float64(x)
	}
	n = math.Sqrt(n)
	if n == 0 {
		return v
	}
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = float32(float64(x) / n)
	}
	return out
}

// TestVector_Basic exercises the happy path: insert a few unit
// vectors, search with a query that's identical to one of them, expect
// that vector first.
func TestVector_Basic(t *testing.T) {
	ctx := context.Background()
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucket, err := bw.RegisterBucket[VecDoc](db, "vec_basic")
	if err != nil {
		t.Fatal(err)
	}

	docs := []*VecDoc{
		{ID: "a", Title: "alpha", Embed: unit([]float32{1, 0, 0, 0})},
		{ID: "b", Title: "beta", Embed: unit([]float32{0, 1, 0, 0})},
		{ID: "c", Title: "gamma", Embed: unit([]float32{0, 0, 1, 0})},
		{ID: "d", Title: "delta", Embed: unit([]float32{0.7, 0.7, 0, 0})},
	}
	for _, d := range docs {
		if err := bucket.Insert(ctx, d); err != nil {
			t.Fatalf("insert %s: %v", d.ID, err)
		}
	}

	// Query a → expect a first, then d (overlap), then b/c.
	hits, err := bucket.SearchVector(ctx, unit([]float32{1, 0, 0, 0}))
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 4 {
		t.Fatalf("want 4 hits, got %d", len(hits))
	}
	if hits[0].Record.ID != "a" {
		t.Fatalf("want a first, got %q (scores: %v)", hits[0].Record.ID, scoreList(hits))
	}
	if hits[1].Record.ID != "d" {
		t.Fatalf("want d second, got %q (scores: %v)", hits[1].Record.ID, scoreList(hits))
	}
	if hits[0].Score < hits[1].Score {
		t.Fatalf("scores not descending: %v", scoreList(hits))
	}
}

// TestVector_TopK respects the K parameter.
func TestVector_TopK(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[VecDoc](db, "vec_topk")

	for i := 0; i < 20; i++ {
		v := []float32{float32(i), 0, 0, 0}
		_ = bucket.Insert(ctx, &VecDoc{ID: string(rune('a' + i)), Embed: unit(v)})
	}

	hits, err := bucket.SearchVector(ctx, unit([]float32{1, 0, 0, 0}), bw.SearchVectorOptions{K: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 5 {
		t.Fatalf("K=5 want 5 hits, got %d", len(hits))
	}
}

// TestVector_DimMismatch validates that a vector of the wrong size is
// rejected with the sentinel error.
func TestVector_DimMismatch(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[VecDoc](db, "vec_dim")

	if err := bucket.Insert(ctx, &VecDoc{ID: "a", Embed: unit([]float32{1, 0, 0, 0})}); err != nil {
		t.Fatal(err)
	}

	// Wrong-length vector → ErrDimMismatch.
	err := bucket.Insert(ctx, &VecDoc{ID: "b", Embed: []float32{1, 0, 0}})
	if !errors.Is(err, bw.ErrDimMismatch) {
		t.Fatalf("want ErrDimMismatch, got %v", err)
	}

	// Wrong-length search query → also ErrDimMismatch.
	_, err = bucket.SearchVector(ctx, []float32{1, 0, 0})
	if !errors.Is(err, bw.ErrDimMismatch) {
		t.Fatalf("search: want ErrDimMismatch, got %v", err)
	}
}

// TestVector_NoVectorBucket returns ErrNoVector cleanly.
func TestVector_NoVectorBucket(t *testing.T) {
	type Plain struct {
		ID string `bw:"id,pk"`
	}
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, err := bw.RegisterBucket[Plain](db, "plain_vec")
	if err != nil {
		t.Fatal(err)
	}
	_, err = bucket.SearchVector(context.Background(), []float32{1, 2, 3})
	if !errors.Is(err, bw.ErrNoVector) {
		t.Fatalf("want ErrNoVector, got %v", err)
	}
}

// TestVector_Update re-inserts the same pk with a new vector and
// verifies search reflects the new direction.
func TestVector_Update(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[VecDoc](db, "vec_upd")

	_ = bucket.Insert(ctx, &VecDoc{ID: "a", Embed: unit([]float32{1, 0, 0, 0})})
	_ = bucket.Insert(ctx, &VecDoc{ID: "b", Embed: unit([]float32{0, 1, 0, 0})})

	// Now move "a" toward b.
	_ = bucket.Insert(ctx, &VecDoc{ID: "a", Embed: unit([]float32{0, 1, 0, 0})})

	hits, err := bucket.SearchVector(ctx, unit([]float32{0, 1, 0, 0}), bw.SearchVectorOptions{K: 2})
	if err != nil {
		t.Fatal(err)
	}
	// Both "a" and "b" should be tied at score 1.0; tiebreaker is
	// pk lexicographic ascending → "a" first.
	if len(hits) != 2 {
		t.Fatalf("want 2 hits, got %d", len(hits))
	}
	if hits[0].Score < 0.99 || hits[1].Score < 0.99 {
		t.Fatalf("expected near-1.0 scores, got %v", scoreList(hits))
	}
}

// TestVector_Delete removes a record and confirms it stops appearing.
func TestVector_Delete(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[VecDoc](db, "vec_del")

	_ = bucket.Insert(ctx, &VecDoc{ID: "a", Embed: unit([]float32{1, 0, 0, 0})})
	_ = bucket.Insert(ctx, &VecDoc{ID: "b", Embed: unit([]float32{0, 1, 0, 0})})

	if err := bucket.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	hits, err := bucket.SearchVector(ctx, unit([]float32{1, 0, 0, 0}))
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hits {
		if h.Record.ID == "a" {
			t.Fatalf("deleted record still appears in results")
		}
	}
}

// TestVector_RollbackLeavesNoTrace verifies the atomicity guarantee:
// a transaction that inserts a vector but ultimately fails leaves no
// vector keys behind.
func TestVector_RollbackLeavesNoTrace(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[VecDoc](db, "vec_rb")

	sentinel := errors.New("rollback")
	err := db.Update(func(tx *bw.Tx) error {
		if err := bucket.InsertTx(tx, &VecDoc{ID: "phantom", Embed: unit([]float32{1, 0, 0, 0})}); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}

	hits, err := bucket.SearchVector(ctx, unit([]float32{1, 0, 0, 0}))
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hits {
		if h.Record.ID == "phantom" {
			t.Fatalf("rollback leaked: %+v", h)
		}
	}
}

// TestVector_BackupRestorePortable proves the headline benefit: a
// backup taken on one path round-trips to a different directory and
// SearchVector keeps working — no rebuild step required.
func TestVector_BackupRestorePortable(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	src, err := bw.Open(srcDir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	srcBucket, err := bw.RegisterBucket[VecDoc](src, "vec_bk")
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range [][]float32{
		{1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 1, 0},
	} {
		id := string(rune('a' + i))
		if err := srcBucket.Insert(ctx, &VecDoc{ID: id, Embed: unit(v)}); err != nil {
			t.Fatal(err)
		}
	}

	var buf bytes.Buffer
	if _, err := src.Backup(&buf, 0, false); err != nil {
		t.Fatal(err)
	}
	src.Close()

	dst, err := bw.Open(dstDir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if err := dst.Restore(&buf); err != nil {
		t.Fatal(err)
	}

	dstBucket, err := bw.RegisterBucket[VecDoc](dst, "vec_bk")
	if err != nil {
		t.Fatal(err)
	}
	hits, err := dstBucket.SearchVector(ctx, unit([]float32{0, 1, 0, 0}), bw.SearchVectorOptions{K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Record.ID != "b" {
		t.Fatalf("post-restore search: want b, got %+v", hits)
	}
}

// TestVector_Filter combines a query.Query filter with the vector pass.
func TestVector_Filter(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[VecDoc](db, "vec_filter")

	_ = bucket.Insert(ctx, &VecDoc{ID: "a", Tag: "blog", Embed: unit([]float32{1, 0, 0, 0})})
	_ = bucket.Insert(ctx, &VecDoc{ID: "b", Tag: "doc", Embed: unit([]float32{1, 0, 0, 0})}) // same vector, different tag
	_ = bucket.Insert(ctx, &VecDoc{ID: "c", Tag: "blog", Embed: unit([]float32{0, 1, 0, 0})})

	q, err := query.Parse("tag=blog")
	if err != nil {
		t.Fatal(err)
	}
	hits, err := bucket.SearchVector(ctx, unit([]float32{1, 0, 0, 0}), bw.SearchVectorOptions{
		Filter: q,
		K:      10,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Only "a" and "c" match the tag filter; "b" is excluded.
	if len(hits) != 2 {
		t.Fatalf("want 2 hits, got %d (%v)", len(hits), hits)
	}
	for _, h := range hits {
		if h.Record.Tag != "blog" {
			t.Fatalf("filter leak: tag=%q", h.Record.Tag)
		}
	}
	// "a" beats "c" because it's the exact direction.
	if hits[0].Record.ID != "a" {
		t.Fatalf("want a first, got %v", scoreList(hits))
	}
}

// TestVector_Embedder validates auto-embedding on Insert.
func TestVector_Embedder(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()

	// Embedder maps Title length → fake 4-D vector.
	embed := func(_ context.Context, r *VecDoc) ([]float32, error) {
		x := float32(len(r.Title))
		return unit([]float32{x, 0, 0, 0}), nil
	}
	bucket, err := bw.RegisterBucket[VecDoc](db, "vec_embed", bw.WithEmbedder[VecDoc](embed))
	if err != nil {
		t.Fatal(err)
	}

	// Insert without setting Embed — embedder fills it in.
	if err := bucket.Insert(ctx, &VecDoc{ID: "a", Title: "abcd"}); err != nil {
		t.Fatal(err)
	}
	hits, err := bucket.SearchVector(ctx, unit([]float32{1, 0, 0, 0}))
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Record.ID != "a" {
		t.Fatalf("embedder didn't populate vector: %+v", hits)
	}

	// Caller-supplied vector wins over the embedder.
	custom := unit([]float32{0, 1, 0, 0})
	if err := bucket.Insert(ctx, &VecDoc{ID: "b", Title: "anything", Embed: custom}); err != nil {
		t.Fatal(err)
	}
	hits, err = bucket.SearchVector(ctx, custom, bw.SearchVectorOptions{K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if hits[0].Record.ID != "b" {
		t.Fatalf("custom vector didn't take precedence: %+v", hits)
	}
}

// TestVector_Metrics covers all three distance functions.
func TestVector_Metrics(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[VecDoc](db, "vec_metrics")

	_ = bucket.Insert(ctx, &VecDoc{ID: "near", Embed: unit([]float32{1, 0, 0, 0})})
	_ = bucket.Insert(ctx, &VecDoc{ID: "far", Embed: unit([]float32{-1, 0, 0, 0})})

	q := unit([]float32{1, 0, 0, 0})

	for _, m := range []bw.VectorMetric{bw.Cosine, bw.DotProduct, bw.Euclidean} {
		hits, err := bucket.SearchVector(ctx, q, bw.SearchVectorOptions{Metric: m, K: 1})
		if err != nil {
			t.Fatalf("metric %s: %v", m, err)
		}
		if len(hits) != 1 || hits[0].Record.ID != "near" {
			t.Fatalf("metric %s: want near first, got %+v", m, hits)
		}
	}
}

func scoreList(hits []bw.VectorHit[VecDoc]) []string {
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.Record.ID + ":"
	}
	return out
}
