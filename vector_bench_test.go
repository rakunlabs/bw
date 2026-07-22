package bw

import (
	"context"
	"math/rand"
	"sort"
	"testing"
)

// benchVec is the fixed-dim record used by the vector micro-benchmarks
// and the recall measurement below.
type benchVec struct {
	ID  string    `bw:"id,pk"`
	Emb []float32 `bw:"emb,vector(dim=96,metric=cosine)"`
}

func randVecBench(rng *rand.Rand, d int) []float32 {
	v := make([]float32, d)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

func itoaBench(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	p := len(buf)
	for i > 0 {
		p--
		buf[p] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[p:])
}

// buildBenchIndex inserts n random vectors and returns the bucket plus
// the raw data (for exact ground-truth recall).
func buildBenchIndex(tb testing.TB, n, dim int) (*Bucket[benchVec], [][]float32, []string) {
	tb.Helper()
	rng := rand.New(rand.NewSource(42))
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	b, err := RegisterBucket[benchVec](db, "bench")
	if err != nil {
		tb.Fatal(err)
	}
	ctx := context.Background()
	data := make([][]float32, n)
	ids := make([]string, n)
	for i := range n {
		v := randVecBench(rng, dim)
		data[i] = v
		ids[i] = itoaBench(i)
		if err := b.Insert(ctx, &benchVec{ID: ids[i], Emb: v}); err != nil {
			tb.Fatalf("insert %d: %v", i, err)
		}
	}
	return b, data, ids
}

// measureRecall returns recall@k for the given efSearch over nQ queries.
func measureRecall(tb testing.TB, b *Bucket[benchVec], data [][]float32, ids []string, dim, k, ef, nQ int) float64 {
	tb.Helper()
	rng := rand.New(rand.NewSource(7))
	n := len(data)
	truth := func(q []float32) map[string]bool {
		type sc struct {
			id string
			s  float64
		}
		all := make([]sc, n)
		for i := range n {
			all[i] = sc{ids[i], cosine(q, data[i])}
		}
		sort.Slice(all, func(a, c int) bool { return all[a].s > all[c].s })
		out := make(map[string]bool, k)
		for i := 0; i < k; i++ {
			out[all[i].id] = true
		}
		return out
	}
	ctx := context.Background()
	hit := 0
	for range nQ {
		q := randVecBench(rng, dim)
		want := truth(q)
		res, err := b.SearchVector(ctx, q, SearchVectorOptions{K: k, EfSearch: ef})
		if err != nil {
			tb.Fatal(err)
		}
		for _, r := range res {
			if want[r.Record.ID] {
				hit++
			}
		}
	}
	return float64(hit) / float64(nQ*k)
}

// BenchmarkVectorSearch measures end-to-end query latency at the default
// efSearch on a 5k index.
func BenchmarkVectorSearch(b *testing.B) {
	const dim = 96
	bucket, _, _ := buildBenchIndex(b, 5000, dim)
	rng := rand.New(rand.NewSource(99))
	queries := make([][]float32, 256)
	for i := range queries {
		queries[i] = randVecBench(rng, dim)
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, err := bucket.SearchVector(ctx, queries[i%len(queries)], SearchVectorOptions{K: 10}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCosine isolates the distance-function cost.
func BenchmarkCosine(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	x := randVecBench(rng, 96)
	y := randVecBench(rng, 96)
	b.ResetTimer()
	var s float64
	for b.Loop() {
		s = cosine(x, y)
	}
	_ = s
}

// TestVectorRecallReport prints recall at several efSearch values so we
// can compare before/after algorithm changes.
func TestVectorRecallReport(t *testing.T) {
	if testing.Short() {
		t.Skip("recall report skipped in -short")
	}
	const (
		dim = 96
		k   = 10
		nQ  = 100
	)
	b, data, ids := buildBenchIndex(t, 5000, dim)
	for _, ef := range []int{100, 200, 400} {
		r := measureRecall(t, b, data, ids, dim, k, ef, nQ)
		t.Logf("recall@%d efSearch=%-4d = %.3f", k, ef, r)
	}
}
