package bw

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/rakunlabs/query"
)

// filterRec is a multi-tenant vector record: the shape where a filtered
// vector search has to separate one tenant's vectors from everyone else's.
type filterRec struct {
	ID    string    `bw:"id,pk"`
	Repo  string    `bw:"repo,index"`
	Tier  int       `bw:"tier,index"`
	Notes string    `bw:"notes"`
	Emb   []float32 `bw:"emb,vector(metric=cosine)"`
}

func openFilterBucket(t *testing.T) *Bucket[filterRec] {
	t.Helper()

	db, err := Open(t.TempDir(), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	b, err := RegisterBucket[filterRec](db, "recs")
	if err != nil {
		t.Fatal(err)
	}

	return b
}

// seedFilterRecs writes n records spread over the given repos, returning the
// vectors by id so a test can compute exact answers.
func seedFilterRecs(t *testing.T, b *Bucket[filterRec], n, dim int, repos []string) map[string][]float32 {
	t.Helper()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(11))
	vecs := make(map[string][]float32, n)

	for i := range n {
		id := fmt.Sprintf("r%05d", i)
		v := randVecBench(rng, dim)
		vecs[id] = v

		rec := &filterRec{
			ID:    id,
			Repo:  repos[i%len(repos)],
			Tier:  i % 3,
			Notes: "note",
			Emb:   v,
		}
		if err := b.Insert(ctx, rec); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}

	return vecs
}

func mustParse(t *testing.T, s string) *query.Query {
	t.Helper()

	q, err := query.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}

	return q
}

// TestSearchVectorFilterMatchesFind is the correctness contract for the
// keys-only filter resolution: whatever set Find would have produced, the
// vector search must restrict itself to exactly that set.
func TestSearchVectorFilterMatchesFind(t *testing.T) {
	const (
		n   = 400
		dim = 16
	)

	b := openFilterBucket(t)
	seedFilterRecs(t, b, n, dim, []string{"alpha", "beta", "gamma"})

	ctx := context.Background()
	rng := rand.New(rand.NewSource(3))

	cases := []struct {
		name string
		q    *query.Query
	}{
		// Indexed, no residual: these take the keys-only path.
		{"eq", mustParse(t, "repo=alpha")},
		{"in", mustParse(t, "repo[in]=alpha|beta")},
		{"other index", mustParse(t, "tier=1")},
		{"empty result", mustParse(t, "repo=nosuchrepo")},
		// Indexed with a residual, and unindexed: these fall back to
		// materialising records.
		{"eq with residual", mustParse(t, "repo=alpha&tier=2")},
		{"unindexed field", mustParse(t, "notes=note")},
		// Like is not an indexable operator, so it degrades to a full scan.
		// Built directly rather than parsed because '%' is not URL-safe.
		{"like prefix", &query.Query{
			Where: []query.Expression{
				query.NewExpressionCmp(query.OperatorLike, "repo", "al%"),
			},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.q

			want, err := b.Find(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			wantIDs := map[string]bool{}
			for _, rec := range want {
				wantIDs[rec.ID] = true
			}

			// K larger than the whole corpus, so the search cannot drop a
			// record for any reason other than the filter.
			hits, err := b.SearchVector(ctx, randVecBench(rng, dim), SearchVectorOptions{
				K:      n + 1,
				Filter: q,
			})
			if err != nil {
				t.Fatal(err)
			}

			if len(hits) != len(wantIDs) {
				t.Fatalf("got %d hits, Find returned %d records", len(hits), len(wantIDs))
			}
			for _, h := range hits {
				if !wantIDs[h.Record.ID] {
					t.Fatalf("filter leak: %q is not in the Find result set", h.Record.ID)
				}
			}
		})
	}
}

// TestSearchVectorFilterIsExact pins the ordering too, not just membership:
// a filter selective enough to be answered by scanning must return the exact
// top-k of the filtered subset, in exact order.
func TestSearchVectorFilterIsExact(t *testing.T) {
	const (
		n   = 600
		dim = 24
		k   = 5
	)

	repos := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}

	b := openFilterBucket(t)
	vecs := seedFilterRecs(t, b, n, dim, repos)

	ctx := context.Background()
	rng := rand.New(rand.NewSource(5))

	// "alpha" holds n/len(repos) = 100 of 600 records: under a sixth of the
	// corpus, so the selectivity rule routes it to the exact scan.
	q := mustParse(t, "repo=alpha")

	subset, err := b.Find(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	for range 20 {
		qv := randVecBench(rng, dim)

		type scored struct {
			id string
			s  float64
		}
		exact := make([]scored, 0, len(subset))
		for _, rec := range subset {
			exact = append(exact, scored{id: rec.ID, s: cosine(qv, vecs[rec.ID])})
		}
		sort.Slice(exact, func(i, j int) bool {
			if exact[i].s != exact[j].s {
				return exact[i].s > exact[j].s
			}

			return exact[i].id < exact[j].id
		})

		hits, err := b.SearchVector(ctx, qv, SearchVectorOptions{K: k, Filter: q})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != k {
			t.Fatalf("got %d hits, want %d", len(hits), k)
		}

		for i, h := range hits {
			if h.Record.ID != exact[i].id {
				t.Fatalf("rank %d: got %q (%.6f), want %q (%.6f)",
					i, h.Record.ID, h.Score, exact[i].id, exact[i].s)
			}
			if math.Abs(h.Score-exact[i].s) > 1e-6 {
				t.Fatalf("rank %d score: got %.9f, want %.9f", i, h.Score, exact[i].s)
			}
		}
	}
}

// TestSearchVectorFilterSkipsDeleted makes sure the exact-scan path still
// honours tombstones: a deleted record must not resurface just because its
// primary key is no longer read through the record-materialising path.
func TestSearchVectorFilterSkipsDeleted(t *testing.T) {
	const (
		n   = 200
		dim = 8
	)

	b := openFilterBucket(t)
	seedFilterRecs(t, b, n, dim, []string{"alpha", "beta"})

	ctx := context.Background()

	before, err := b.SearchVector(ctx, randVecBench(rand.New(rand.NewSource(1)), dim), SearchVectorOptions{
		K:      n,
		Filter: mustParse(t, "repo=alpha"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(before) == 0 {
		t.Fatal("no hits to delete")
	}

	victim := before[0].Record.ID
	if err := b.Delete(ctx, victim); err != nil {
		t.Fatal(err)
	}

	after, err := b.SearchVector(ctx, randVecBench(rand.New(rand.NewSource(1)), dim), SearchVectorOptions{
		K:      n,
		Filter: mustParse(t, "repo=alpha"),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(after) != len(before)-1 {
		t.Fatalf("got %d hits after delete, want %d", len(after), len(before)-1)
	}
	for _, h := range after {
		if h.Record.ID == victim {
			t.Fatalf("deleted record %q still returned", victim)
		}
	}
}

// TestSearchVectorFilterRecordsWithoutVectors covers a pk that the filter
// selects but that carries no embedding. The exact-scan path reads vectors by
// key, so a missing one must be skipped rather than aborting the query.
func TestSearchVectorFilterRecordsWithoutVectors(t *testing.T) {
	const dim = 8

	b := openFilterBucket(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(17))

	for i := range 10 {
		rec := &filterRec{ID: fmt.Sprintf("v%02d", i), Repo: "alpha", Emb: randVecBench(rng, dim)}
		if err := b.Insert(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	// Same tenant, no embedding at all.
	for i := range 5 {
		rec := &filterRec{ID: fmt.Sprintf("n%02d", i), Repo: "alpha"}
		if err := b.Insert(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}

	hits, err := b.SearchVector(ctx, randVecBench(rng, dim), SearchVectorOptions{
		K:      50,
		Filter: mustParse(t, "repo=alpha"),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(hits) != 10 {
		t.Fatalf("got %d hits, want the 10 records that have a vector", len(hits))
	}
	for _, h := range hits {
		if len(h.Record.Emb) == 0 {
			t.Fatalf("record %q has no vector but was ranked", h.Record.ID)
		}
	}
}

// TestSearchVectorFilterHonoursCancellation checks that a cancelled context
// stops the search instead of being noticed only after it finishes.
func TestSearchVectorFilterHonoursCancellation(t *testing.T) {
	const (
		n   = 300
		dim = 8
	)

	b := openFilterBucket(t)
	seedFilterRecs(t, b, n, dim, []string{"alpha"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := b.SearchVector(ctx, randVecBench(rand.New(rand.NewSource(2)), dim), SearchVectorOptions{
		K:      10,
		Filter: mustParse(t, "repo=alpha"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

// TestBruteBeatsGraph documents the routing rule directly, so a change to the
// threshold is a deliberate edit rather than a silent shift in behaviour.
func TestBruteBeatsGraph(t *testing.T) {
	set := func(n int) map[string]struct{} {
		m := make(map[string]struct{}, n)
		for i := range n {
			m[fmt.Sprintf("%d", i)] = struct{}{}
		}

		return m
	}

	for _, tc := range []struct {
		name    string
		allowed map[string]struct{}
		count   uint64
		want    bool
	}{
		{"no filter never routes to brute", nil, 1_000_000, false},
		{"tiny filter on a huge corpus", set(10), 1_000_000, true},
		{"filter at the selectivity limit", set(1000), 8000, true},
		{"filter just past the selectivity limit", set(1001), 8000, false},
		{"small absolute filter beats a small corpus ratio", set(64), 100, true},
		{"broad filter stays on the graph", set(5000), 8000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := bruteBeatsGraph(tc.allowed, tc.count); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
