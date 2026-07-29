package bw

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// scorerTolerance bounds how far queryScorer may drift from the plain kernel.
//
// The two differ only in the order they sum float32 products. Over d terms
// that accumulates on the order of d * 2^-24 relative error, so 3072
// dimensions sits near 2e-4 in the worst case; 1e-5 is comfortably above what
// random unit-ish vectors actually produce while still catching a genuine
// arithmetic mistake.
const scorerTolerance = 1e-5

// TestQueryScorerMatchesScore checks the scorer against the plain kernel for
// every metric and a spread of widths, including the non-multiple-of-4 lengths
// that exercise the unrolled loop's tail.
func TestQueryScorerMatchesScore(t *testing.T) {
	rng := rand.New(rand.NewSource(31))

	worst := 0.0
	for _, metric := range []VectorMetric{Cosine, DotProduct, Euclidean} {
		for _, dim := range []int{1, 2, 3, 4, 5, 7, 9, 96, 768, 1536, 3072} {
			q := randVecBench(rng, dim)
			sc := newQueryScorer(metric, q)

			for range 32 {
				v := randVecBench(rng, dim)

				want := score(metric, q, v)
				got, err := sc.score(v)
				if err != nil {
					t.Fatalf("metric=%v dim=%d: %v", metric, dim, err)
				}

				diff := math.Abs(got - want)
				if scale := math.Abs(want); scale > 1 {
					diff /= scale
				}
				if diff > worst {
					worst = diff
				}

				if diff > scorerTolerance {
					t.Fatalf("metric=%v dim=%d: got %.17g, want %.17g (rel diff %.3g)",
						metric, dim, got, want, diff)
				}
			}
		}
	}

	t.Logf("worst relative deviation from the plain kernel: %.3g", worst)
}

// TestQueryScorerDotAndEuclideanAreExact records that only cosine changed:
// the other two metrics delegate to the same kernels as before, so they must
// still agree bit for bit.
func TestQueryScorerDotAndEuclideanAreExact(t *testing.T) {
	rng := rand.New(rand.NewSource(37))

	for _, metric := range []VectorMetric{DotProduct, Euclidean} {
		for _, dim := range []int{7, 768, 3072} {
			q := randVecBench(rng, dim)
			sc := newQueryScorer(metric, q)

			for range 16 {
				v := randVecBench(rng, dim)

				want := score(metric, q, v)
				got, err := sc.score(v)
				if err != nil {
					t.Fatal(err)
				}
				if math.Float64bits(got) != math.Float64bits(want) {
					t.Fatalf("metric=%v dim=%d: got %.17g, want %.17g", metric, dim, got, want)
				}
			}
		}
	}
}

// TestQueryScorerRankingMatchesReference is the test that actually matters.
//
// Absolute score deviation is not interesting on its own; what would be a
// regression is the ranking moving. This ranks a corpus with both kernels and
// requires the ordered top-k to be identical, at the width production uses.
func TestQueryScorerRankingMatchesReference(t *testing.T) {
	const (
		n   = 2000
		dim = 3072
		k   = 20
		nQ  = 25
	)

	rng := rand.New(rand.NewSource(53))

	corpus := make([][]float32, n)
	for i := range corpus {
		corpus[i] = randVecBench(rng, dim)
	}

	rank := func(q []float32, scoreOne func(v []float32) float64) []int {
		idx := make([]int, n)
		scores := make([]float64, n)
		for i := range corpus {
			idx[i] = i
			scores[i] = scoreOne(corpus[i])
		}
		sort.SliceStable(idx, func(a, b int) bool {
			return scores[idx[a]] > scores[idx[b]]
		})

		return idx[:k]
	}

	for qi := range nQ {
		q := randVecBench(rng, dim)
		sc := newQueryScorer(Cosine, q)

		want := rank(q, func(v []float32) float64 { return cosine(q, v) })
		got := rank(q, func(v []float32) float64 {
			s, err := sc.score(v)
			if err != nil {
				t.Fatal(err)
			}

			return s
		})

		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("query %d: rank %d is corpus[%d] with the scorer but corpus[%d] with the plain kernel",
					qi, i, got[i], want[i])
			}
		}
	}
}

// TestQueryScorerZeroVector covers the degenerate case the plain kernel
// special-cases: a zero-length vector has no direction, so cosine is defined
// as 0 rather than NaN.
func TestQueryScorerZeroVector(t *testing.T) {
	zero := make([]float32, 8)
	nonZero := []float32{1, 0, 0, 0, 0, 0, 0, 0}

	for _, tc := range []struct {
		name string
		q, v []float32
	}{
		{"zero query", zero, nonZero},
		{"zero candidate", nonZero, zero},
		{"both zero", zero, zero},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := score(Cosine, tc.q, tc.v)

			got, err := newQueryScorer(Cosine, tc.q).score(tc.v)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("got %v, want %v", got, want)
			}
			if got != 0 {
				t.Fatalf("got %v, want 0", got)
			}
		})
	}
}

// TestQueryScorerDimMismatch keeps the error behaviour of scoreChecked.
func TestQueryScorerDimMismatch(t *testing.T) {
	sc := newQueryScorer(Cosine, make([]float32, 4))

	if _, err := sc.score(make([]float32, 5)); err == nil {
		t.Fatal("want a dimension-mismatch error, got nil")
	}
}

// BenchmarkQueryScorer shows what hoisting the query norm buys at the widths
// production embedding models produce. Compare against BenchmarkDistanceKernels.
func BenchmarkQueryScorer(b *testing.B) {
	for _, dim := range []int{768, 1536, 3072} {
		rng := rand.New(rand.NewSource(1))
		q := randVecBench(rng, dim)
		v := randVecBench(rng, dim)
		sc := newQueryScorer(Cosine, q)

		b.Run("cosine/dim="+itoaBench(dim), func(b *testing.B) {
			var s float64
			for b.Loop() {
				s, _ = sc.score(v)
			}
			runtimeSink = s
		})
	}
}
