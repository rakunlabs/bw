package bw

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/rakunlabs/query"
)

// The benchmarks in vector_bench_test.go measure an in-memory, unfiltered,
// 96-dimension index. Production workloads look nothing like that: they run
// on disk, at embedding widths of 768-3072, and virtually every query carries
// a filter that selects one tenant out of a shared bucket. This file measures
// that shape instead, because the filtered path and the unfiltered path have
// completely different cost drivers.
//
// Sizes are deliberately modest by default so `go test -bench .` stays usable.
// Override them for a heavier run:
//
//	BW_BENCH_N=100000 BW_BENCH_DIMS=768,3072 go test -run XXX -bench VectorFiltered -benchtime 20x
const (
	defaultBenchN   = 20000
	defaultBenchDim = 768
)

// benchChunk mirrors the shape of a real RAG chunk record: a primary key, an
// indexed tenant discriminator, some payload text, and the embedding. The
// vector tag deliberately omits dim= so one type serves every width.
type benchChunk struct {
	ID   string    `bw:"id,pk"`
	Repo string    `bw:"repo,index"`
	Text string    `bw:"text"`
	Emb  []float32 `bw:"emb,vector(metric=cosine)"`
}

// benchSelectivity names the tenant labels and the fraction of the corpus each
// one holds. The spread is interleaved rather than contiguous: a tenant whose
// records are adjacent in key order would be unrealistically friendly to both
// the block cache and the HNSW graph layout.
//
// Assignment (first match wins) gives approximately:
//
//	tiny   i%2000 == 0   0.05%
//	small  i%100  == 1   1%
//	medium i%10   == 2   10%
//	big    i%2    == 1   49%
//	rest   remainder     40%
var benchSelectivity = []string{"tiny", "small", "medium", "big"}

func benchRepoFor(i int) string {
	switch {
	case i%2000 == 0:
		return "tiny"
	case i%100 == 1:
		return "small"
	case i%10 == 2:
		return "medium"
	case i%2 == 1:
		return "big"
	default:
		return "rest"
	}
}

func benchN() int {
	if v := os.Getenv("BW_BENCH_N"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}

	return defaultBenchN
}

func benchDims() []int {
	v := os.Getenv("BW_BENCH_DIMS")
	if v == "" {
		return []int{defaultBenchDim}
	}

	var out []int
	for _, part := range splitComma(v) {
		if n, err := strconv.Atoi(part); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return []int{defaultBenchDim}
	}

	return out
}

func splitComma(s string) []string {
	var (
		out  []string
		cur  []rune
		emit = func() {}
	)
	emit = func() {
		if len(cur) > 0 {
			out = append(out, string(cur))
			cur = cur[:0]
		}
	}
	for _, r := range s {
		if r == ',' || r == ' ' {
			emit()

			continue
		}
		cur = append(cur, r)
	}
	emit()

	return out
}

// benchFixture is a built, on-disk index plus the ground truth needed to score
// recall. Building one is expensive (HNSW insert is the dominant cost at high
// dimension), so fixtures are memoised per (n, dim) for the life of the test
// binary.
type benchFixture struct {
	bucket  *Bucket[benchChunk]
	vecs    [][]float32
	ids     []string
	repos   []string
	byRepo  map[string][]int
	dim     int
	queries [][]float32
}

var (
	fixtureMu sync.Mutex
	fixtures  = map[string]*benchFixture{}
)

func benchFixtureFor(tb testing.TB, n, dim int) *benchFixture {
	tb.Helper()

	key := fmt.Sprintf("%d/%d", n, dim)

	fixtureMu.Lock()
	defer fixtureMu.Unlock()

	if f, ok := fixtures[key]; ok {
		return f
	}

	f := buildBenchFixture(tb, n, dim)
	fixtures[key] = f

	return f
}

// buildBenchFixture writes n random vectors to an on-disk database. The
// database is intentionally not registered with tb.Cleanup: it is shared
// across sub-benchmarks and must outlive any single one of them. The
// directory is created under the OS temp dir and reclaimed by the OS.
func buildBenchFixture(tb testing.TB, n, dim int) *benchFixture {
	tb.Helper()

	dir, err := os.MkdirTemp("", "bw-bench-*")
	if err != nil {
		tb.Fatal(err)
	}

	db, err := Open(dir, WithLogger(nil))
	if err != nil {
		tb.Fatal(err)
	}

	bucket, err := RegisterBucket[benchChunk](db, "chunks")
	if err != nil {
		tb.Fatal(err)
	}

	var (
		ctx    = context.Background()
		rng    = rand.New(rand.NewSource(42))
		vecs   = make([][]float32, n)
		ids    = make([]string, n)
		repos  = make([]string, n)
		byRepo = map[string][]int{}
		// A realistic chunk carries ~2 KB of text alongside the embedding.
		text = string(make([]byte, 2048))
	)

	for i := range n {
		v := randVecBench(rng, dim)
		id := itoaBench(i)
		repo := benchRepoFor(i)

		vecs[i] = v
		ids[i] = id
		repos[i] = repo
		byRepo[repo] = append(byRepo[repo], i)

		rec := &benchChunk{ID: id, Repo: repo, Text: text, Emb: v}
		if err := bucket.Insert(ctx, rec); err != nil {
			tb.Fatalf("insert %d: %v", i, err)
		}
	}

	qrng := rand.New(rand.NewSource(99))
	queries := make([][]float32, 128)
	for i := range queries {
		queries[i] = randVecBench(qrng, dim)
	}

	return &benchFixture{
		bucket:  bucket,
		vecs:    vecs,
		ids:     ids,
		repos:   repos,
		byRepo:  byRepo,
		dim:     dim,
		queries: queries,
	}
}

// exactTopK returns the ids of the k nearest vectors to q, restricted to repo
// when it is non-empty. This is the ground truth recall is measured against.
func (f *benchFixture) exactTopK(q []float32, repo string, k int) []string {
	idx := make([]int, 0, len(f.vecs))
	if repo == "" {
		for i := range f.vecs {
			idx = append(idx, i)
		}
	} else {
		idx = append(idx, f.byRepo[repo]...)
	}

	type scored struct {
		id string
		s  float64
	}

	all := make([]scored, 0, len(idx))
	for _, i := range idx {
		all = append(all, scored{id: f.ids[i], s: cosine(q, f.vecs[i])})
	}
	sort.Slice(all, func(a, b int) bool {
		if all[a].s != all[b].s {
			return all[a].s > all[b].s
		}

		return all[a].id < all[b].id
	})

	if k > len(all) {
		k = len(all)
	}
	out := make([]string, 0, k)
	for i := range k {
		out = append(out, all[i].id)
	}

	return out
}

func benchFilter(tb testing.TB, repo string) any {
	tb.Helper()

	if repo == "" {
		return nil
	}

	q, err := query.Parse("repo=" + repo)
	if err != nil {
		tb.Fatal(err)
	}

	return q
}

// BenchmarkVectorFilteredSearch is the benchmark that actually resembles a RAG
// query: an on-disk multi-tenant bucket, a per-tenant filter, and an embedding
// width a real model produces. The "none" case is the unfiltered baseline.
func BenchmarkVectorFilteredSearch(b *testing.B) {
	n := benchN()

	for _, dim := range benchDims() {
		fx := benchFixtureFor(b, n, dim)

		for _, repo := range append([]string{""}, benchSelectivity...) {
			label := repo
			if label == "" {
				label = "none"
			}

			b.Run(fmt.Sprintf("dim=%d/n=%d/filter=%s", dim, n, label), func(b *testing.B) {
				filter := benchFilter(b, repo)
				ctx := context.Background()

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; b.Loop(); i++ {
					_, err := fx.bucket.SearchVector(ctx, fx.queries[i%len(fx.queries)], SearchVectorOptions{
						K:      10,
						Filter: filter,
					})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkVectorFilteredSearchParallel exposes contention that the serial
// benchmark cannot: DB.View takes a mutex-backed gate and the vector cache
// serialises every miss behind a single write lock.
func BenchmarkVectorFilteredSearchParallel(b *testing.B) {
	n := benchN()

	for _, dim := range benchDims() {
		fx := benchFixtureFor(b, n, dim)

		for _, repo := range []string{"", "medium"} {
			label := repo
			if label == "" {
				label = "none"
			}

			b.Run(fmt.Sprintf("dim=%d/n=%d/filter=%s", dim, n, label), func(b *testing.B) {
				filter := benchFilter(b, repo)

				b.ReportAllocs()
				b.ResetTimer()

				b.RunParallel(func(pb *testing.PB) {
					ctx := context.Background()
					i := 0
					for pb.Next() {
						_, err := fx.bucket.SearchVector(ctx, fx.queries[i%len(fx.queries)], SearchVectorOptions{
							K:      10,
							Filter: filter,
						})
						if err != nil {
							b.Error(err)

							return
						}
						i++
					}
				})
			})
		}
	}
}

// BenchmarkDistanceKernels compares the three metrics at realistic widths.
// cosine is the default metric and the one every RAG deployment pays for.
func BenchmarkDistanceKernels(b *testing.B) {
	for _, dim := range []int{768, 1536, 3072} {
		rng := rand.New(rand.NewSource(1))
		x := randVecBench(rng, dim)
		y := randVecBench(rng, dim)

		b.Run(fmt.Sprintf("cosine/dim=%d", dim), func(b *testing.B) {
			var s float64
			for b.Loop() {
				s = cosine(x, y)
			}
			runtimeSink = s
		})

		b.Run(fmt.Sprintf("dot/dim=%d", dim), func(b *testing.B) {
			var s float64
			for b.Loop() {
				s = dot(x, y)
			}
			runtimeSink = s
		})

		b.Run(fmt.Sprintf("l2/dim=%d", dim), func(b *testing.B) {
			var s float64
			for b.Loop() {
				s = l2(x, y)
			}
			runtimeSink = s
		})
	}
}

// runtimeSink keeps benchmark results observable so the compiler cannot
// eliminate the call being measured.
var runtimeSink float64

// TestVectorFilteredSearchQuality is the guard that the filtered-search
// optimisations must not regress. For every selectivity it checks two things:
//
//   - correctness: no result may fall outside the filtered tenant, and the
//     result count must be min(k, tenant size);
//   - quality: recall against exact cosine over the tenant's own vectors must
//     meet a floor.
//
// The floors are deliberately close to 1.0. A filtered search that selects a
// small slice of the corpus should be answered exactly; only the broad filters
// fall back to the approximate graph.
func TestVectorFilteredSearchQuality(t *testing.T) {
	if testing.Short() {
		t.Skip("filtered-search quality report skipped in -short")
	}

	const (
		n   = 5000
		dim = 128
		k   = 10
		nQ  = 50
	)

	fx := benchFixtureFor(t, n, dim)
	ctx := context.Background()

	floors := map[string]float64{
		"tiny":   1.0,
		"small":  1.0,
		"medium": 0.90,
		"big":    0.85,
	}

	for _, repo := range benchSelectivity {
		want := len(fx.byRepo[repo])
		if want == 0 {
			t.Fatalf("selectivity %q selected no records; fixture assignment is wrong", repo)
		}

		filter := benchFilter(t, repo)
		expectCount := min(k, want)

		hits := 0
		for qi := range nQ {
			q := fx.queries[qi%len(fx.queries)]

			res, err := fx.bucket.SearchVector(ctx, q, SearchVectorOptions{K: k, Filter: filter})
			if err != nil {
				t.Fatalf("%s: %v", repo, err)
			}

			if len(res) != expectCount {
				t.Errorf("%s: got %d hits, want %d (filtered set has %d)",
					repo, len(res), expectCount, want)
			}

			truth := make(map[string]bool, k)
			for _, id := range fx.exactTopK(q, repo, k) {
				truth[id] = true
			}

			for _, r := range res {
				if r.Record.Repo != repo {
					t.Fatalf("%s: filter leak, got record from repo %q", repo, r.Record.Repo)
				}
				if truth[r.Record.ID] {
					hits++
				}
			}
		}

		recall := float64(hits) / float64(nQ*expectCount)
		t.Logf("filter=%-6s selected=%-5d recall@%d = %.3f", repo, want, k, recall)

		if floor := floors[repo]; recall < floor {
			t.Errorf("%s: recall@%d = %.3f, want >= %.2f", repo, k, recall, floor)
		}
	}
}
