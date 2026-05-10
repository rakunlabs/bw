package bw_test

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/query"
)

// Test-local aliases so the bigger HNSW tests stay readable.
var parseQ = query.Parse

type bytesBuffer = bytes.Buffer

type HNSWDoc struct {
	ID    string    `bw:"id,pk"`
	Tag   string    `bw:"tag,index"`
	Embed []float32 `bw:"embed,vector(dim=32,metric=cosine)"`
}

// randUnit returns a deterministic L2-normalised random vector of
// length d, derived from rng.
func randUnit(rng *rand.Rand, d int) []float32 {
	v := make([]float32, d)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	var n float64
	for _, x := range v {
		n += float64(x) * float64(x)
	}
	n = math.Sqrt(n)
	if n == 0 {
		return v
	}
	for i := range v {
		v[i] = float32(float64(v[i]) / n)
	}
	return v
}

// bruteTopK is the reference implementation: cosine-rank every doc by
// hand and return the top-k pks. Used to score recall@k of the HNSW
// pass against ground truth.
func bruteTopK(docs []*HNSWDoc, q []float32, k int) []string {
	type p struct {
		id    string
		score float64
	}
	out := make([]p, 0, len(docs))
	for _, d := range docs {
		var dotV float64
		for i := range q {
			dotV += float64(q[i]) * float64(d.Embed[i])
		}
		out = append(out, p{id: d.ID, score: dotV})
	}
	// Insertion sort (k small).
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].score > out[j-1].score; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	if k > len(out) {
		k = len(out)
	}
	ids := make([]string, k)
	for i := 0; i < k; i++ {
		ids[i] = out[i].id
	}
	return ids
}

// TestHNSW_RecallAt10 inserts 500 vectors and checks that HNSW search
// reaches ≥80% recall@10 against brute-force ground truth for a
// sample of queries. The graph is well above the brute-fallback
// threshold so this exercises the actual HNSW path.
func TestHNSW_RecallAt10(t *testing.T) {
	ctx := context.Background()
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bucket, err := bw.RegisterBucket[HNSWDoc](db, "hnsw_recall")
	if err != nil {
		t.Fatal(err)
	}

	const (
		N    = 500
		dim  = 32
		k    = 10
		seed = 42
	)
	rng := rand.New(rand.NewSource(seed))
	docs := make([]*HNSWDoc, 0, N)
	for i := 0; i < N; i++ {
		d := &HNSWDoc{
			ID:    iToID(i),
			Embed: randUnit(rng, dim),
		}
		docs = append(docs, d)
		if err := bucket.Insert(ctx, d); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	const queries = 20
	totalRecall := 0.0
	for qi := 0; qi < queries; qi++ {
		q := randUnit(rng, dim)
		want := bruteTopK(docs, q, k)
		hits, err := bucket.SearchVector(ctx, q, bw.SearchVectorOptions{K: k, EfSearch: 100})
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		got := make(map[string]bool, len(hits))
		for _, h := range hits {
			got[h.Record.ID] = true
		}
		match := 0
		for _, w := range want {
			if got[w] {
				match++
			}
		}
		totalRecall += float64(match) / float64(k)
	}
	avg := totalRecall / queries
	if avg < 0.80 {
		t.Fatalf("avg recall@%d = %.3f, want >= 0.80", k, avg)
	}
	t.Logf("avg recall@%d = %.3f over %d queries (N=%d)", k, avg, queries, N)
}

// TestHNSW_BruteFallbackForTinyN confirms a tiny dataset still gets
// exact results — the search routes through bruteSearch instead of
// the (degenerate) graph.
func TestHNSW_BruteFallbackForTinyN(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[HNSWDoc](db, "hnsw_tiny")

	rng := rand.New(rand.NewSource(7))
	docs := make([]*HNSWDoc, 5)
	for i := range docs {
		docs[i] = &HNSWDoc{ID: iToID(i), Embed: randUnit(rng, 32)}
		_ = bucket.Insert(ctx, docs[i])
	}
	q := randUnit(rng, 32)
	want := bruteTopK(docs, q, 3)

	hits, err := bucket.SearchVector(ctx, q, bw.SearchVectorOptions{K: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i, h := range hits {
		if h.Record.ID != want[i] {
			t.Fatalf("brute fallback: hit[%d]=%q want %q", i, h.Record.ID, want[i])
		}
	}
}

// TestHNSW_Update re-inserts the same pk with a new vector and
// confirms the new direction wins under HNSW.
func TestHNSW_Update(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[HNSWDoc](db, "hnsw_upd")

	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 200; i++ {
		_ = bucket.Insert(ctx, &HNSWDoc{ID: iToID(i), Embed: randUnit(rng, 32)})
	}

	// Move "0" to a known direction.
	target := make([]float32, 32)
	target[0] = 1.0
	_ = bucket.Insert(ctx, &HNSWDoc{ID: iToID(0), Embed: target})

	hits, err := bucket.SearchVector(ctx, target, bw.SearchVectorOptions{K: 1, EfSearch: 200})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Record.ID != iToID(0) {
		t.Fatalf("update didn't win: got %+v", hits)
	}
	if hits[0].Score < 0.99 {
		t.Fatalf("score too low: %.3f", hits[0].Score)
	}
}

// TestHNSW_DeleteSkipsTombstoned removes a pk and verifies search no
// longer surfaces it, even though its raw key may still be reachable
// via stale edges in neighbours' lists.
func TestHNSW_DeleteSkipsTombstoned(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[HNSWDoc](db, "hnsw_del")

	rng := rand.New(rand.NewSource(123))
	for i := 0; i < 300; i++ {
		_ = bucket.Insert(ctx, &HNSWDoc{ID: iToID(i), Embed: randUnit(rng, 32)})
	}
	doomed := iToID(42)
	if err := bucket.Delete(ctx, doomed); err != nil {
		t.Fatal(err)
	}

	// 50 random queries should never surface "doomed".
	for qi := 0; qi < 50; qi++ {
		q := randUnit(rng, 32)
		hits, err := bucket.SearchVector(ctx, q, bw.SearchVectorOptions{K: 20})
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range hits {
			if h.Record.ID == doomed {
				t.Fatalf("query %d: doomed pk surfaced", qi)
			}
		}
	}
}

// TestHNSW_FilterIntegration confirms a query.Query filter still
// correctly narrows the result set when the search uses HNSW.
func TestHNSW_FilterIntegration(t *testing.T) {
	ctx := context.Background()
	db, _ := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	defer db.Close()
	bucket, _ := bw.RegisterBucket[HNSWDoc](db, "hnsw_filt")

	rng := rand.New(rand.NewSource(9))
	for i := 0; i < 300; i++ {
		tag := "blog"
		if i%2 == 0 {
			tag = "doc"
		}
		_ = bucket.Insert(ctx, &HNSWDoc{ID: iToID(i), Tag: tag, Embed: randUnit(rng, 32)})
	}

	q := randUnit(rng, 32)
	qq, err := parseQ("tag=blog")
	if err != nil {
		t.Fatal(err)
	}
	hits, err := bucket.SearchVector(ctx, q, bw.SearchVectorOptions{K: 10, Filter: qq})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 {
		t.Fatal("filtered search returned 0")
	}
	for _, h := range hits {
		if h.Record.Tag != "blog" {
			t.Fatalf("filter leak: tag=%q", h.Record.Tag)
		}
	}
}

// TestHNSW_BackupRestorePortable verifies the graph survives a
// Backup/Restore round-trip across directories. This is the same
// portability guarantee Stage A had, but now tested with a graph
// large enough to exercise the HNSW path on the restored side.
func TestHNSW_BackupRestorePortable(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	src, _ := bw.Open(srcDir, bw.WithLogger(nil))
	srcBucket, _ := bw.RegisterBucket[HNSWDoc](src, "hnsw_bk")

	rng := rand.New(rand.NewSource(33))
	docs := make([]*HNSWDoc, 200)
	for i := range docs {
		docs[i] = &HNSWDoc{ID: iToID(i), Embed: randUnit(rng, 32)}
		_ = srcBucket.Insert(ctx, docs[i])
	}

	var buf bytesBuffer
	if _, err := src.Backup(&buf, 0, false); err != nil {
		t.Fatal(err)
	}
	src.Close()

	dst, _ := bw.Open(dstDir, bw.WithLogger(nil))
	defer dst.Close()
	if err := dst.Restore(&buf); err != nil {
		t.Fatal(err)
	}
	dstBucket, _ := bw.RegisterBucket[HNSWDoc](dst, "hnsw_bk")

	q := randUnit(rng, 32)
	want := bruteTopK(docs, q, 5)
	hits, err := dstBucket.SearchVector(ctx, q, bw.SearchVectorOptions{K: 5, EfSearch: 200})
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool)
	for _, h := range hits {
		got[h.Record.ID] = true
	}
	match := 0
	for _, w := range want {
		if got[w] {
			match++
		}
	}
	if match < 4 { // allow 1 miss for HNSW approx
		t.Fatalf("post-restore recall too low: %d/5", match)
	}
}

// iToID makes a stable string ID from an integer (Tx-byte 4-digit hex
// is fine for our purposes).
func iToID(i int) string {
	const hex = "0123456789abcdef"
	out := []byte{0, 0, 0, 0}
	v := i
	for j := 3; j >= 0; j-- {
		out[j] = hex[v&0xF]
		v >>= 4
	}
	return string(out)
}
