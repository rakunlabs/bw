package bw_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/query"
)

// Person and PersonPlain live in testtypes_test.go so msgp can generate
// Marshal/Unmarshal methods on them.

// benchSize controls how many records are inserted into the benchmark
// dataset. 10k keeps things fast for `go test -bench` while still being
// large enough that filter cost is measurable. Override with
//
//	BW_BENCH_N=100000 go test -bench=. -benchtime=3s
const defaultBenchSize = 10_000

func benchSize() int {
	if v, ok := lookupEnv("BW_BENCH_N"); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}

	return defaultBenchSize
}

func lookupEnv(key string) (string, bool) {
	return os.LookupEnv(key)
}

// seedDB inserts n synthetic Person records with a deterministic RNG so
// every benchmark sees identical data.
func seedDB(b *testing.B, n int) (*bw.DB, *bw.Bucket[Person]) {
	b.Helper()
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	people, err := bw.RegisterBucket[Person](db, "people")
	if err != nil {
		b.Fatalf("register: %v", err)
	}
	seedPeople(b, people, n)

	return db, people
}

// seedDBPlain is the no-index counterpart used to measure full-scan
// performance on identical data.
func seedDBPlain(b *testing.B, n int) (*bw.DB, *bw.Bucket[PersonPlain]) {
	b.Helper()
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	people, err := bw.RegisterBucket[PersonPlain](db, "people_plain")
	if err != nil {
		b.Fatalf("register: %v", err)
	}
	seedPeoplePlain(b, people, n)

	return db, people
}

func seedPeople(b *testing.B, people *bw.Bucket[Person], n int) {
	rng := rand.New(rand.NewSource(42))
	countries := []string{"US", "DE", "FR", "TR", "BR", "JP", "IN", "GB", "AR", "CA"}
	cities := []string{"Berlin", "Paris", "Tokyo", "Buenos Aires", "London", "Toronto", "Mumbai", "São Paulo", "Istanbul", "New York"}
	first := []string{"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Kevin", "Laura", "Mike", "Nora", "Oscar", "Peggy", "Quinn", "Rita", "Steve", "Trudy"}

	ctx := context.Background()
	for i := 0; i < n; i++ {
		p := Person{
			ID:      fmt.Sprintf("p%07d", i),
			Name:    first[rng.Intn(len(first))],
			Email:   fmt.Sprintf("user%d@example.com", i),
			Age:     18 + rng.Intn(70),
			Country: countries[rng.Intn(len(countries))],
			Score:   rng.Float64() * 100,
			Tags:    []string{countries[rng.Intn(len(countries))], "tier" + strconv.Itoa(rng.Intn(5))},
		}
		p.Address.City = cities[rng.Intn(len(cities))]
		p.Address.Zip = fmt.Sprintf("%05d", rng.Intn(100000))
		if err := people.Insert(ctx, &p); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func seedPeoplePlain(b *testing.B, people *bw.Bucket[PersonPlain], n int) {
	rng := rand.New(rand.NewSource(42))
	countries := []string{"US", "DE", "FR", "TR", "BR", "JP", "IN", "GB", "AR", "CA"}
	cities := []string{"Berlin", "Paris", "Tokyo", "Buenos Aires", "London", "Toronto", "Mumbai", "São Paulo", "Istanbul", "New York"}
	first := []string{"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Kevin", "Laura", "Mike", "Nora", "Oscar", "Peggy", "Quinn", "Rita", "Steve", "Trudy"}

	ctx := context.Background()
	for i := 0; i < n; i++ {
		p := PersonPlain{
			ID:      fmt.Sprintf("p%07d", i),
			Name:    first[rng.Intn(len(first))],
			Email:   fmt.Sprintf("user%d@example.com", i),
			Age:     18 + rng.Intn(70),
			Country: countries[rng.Intn(len(countries))],
			Score:   rng.Float64() * 100,
			Tags:    []string{countries[rng.Intn(len(countries))], "tier" + strconv.Itoa(rng.Intn(5))},
		}
		p.Address.City = cities[rng.Intn(len(cities))]
		p.Address.Zip = fmt.Sprintf("%05d", rng.Intn(100000))
		if err := people.Insert(ctx, &p); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

// BenchmarkInsert measures single-record write throughput on a bucket
// with NO indexes — pure data write. Compare against BenchmarkInsert_Indexed
// to see the cost of index/unique maintenance.
func BenchmarkInsert(b *testing.B) {
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	people, err := bw.RegisterBucket[PersonPlain](db, "people_plain")
	if err != nil {
		b.Fatalf("register: %v", err)
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := PersonPlain{
			ID:      fmt.Sprintf("p%010d", i),
			Name:    "Alice",
			Email:   fmt.Sprintf("u%d@x", i),
			Age:     30,
			Country: "US",
		}
		if err := people.Insert(ctx, &p); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

// BenchmarkGet is the cheapest possible read path: direct pk lookup.
func BenchmarkGet(b *testing.B) {
	n := benchSize()
	_, people := seedDB(b, n)
	ctx := context.Background()

	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("p%07d", i%n)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := people.Get(ctx, keys[i]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

// runQueryBench is a small helper that re-parses the query once and then
// times Find calls against the indexed Person bucket.
func runQueryBench(b *testing.B, raw string, opts ...query.OptionQuery) {
	b.Helper()
	n := benchSize()
	_, people := seedDB(b, n)
	ctx := context.Background()

	q, err := query.Parse(raw, opts...)
	if err != nil {
		b.Fatalf("parse %q: %v", raw, err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := people.Find(ctx, q); err != nil {
			b.Fatalf("find: %v", err)
		}
	}
}

// runQueryBenchPlain runs the same kind of benchmark but on the un-indexed
// PersonPlain bucket so we can directly compare full-scan cost against
// the index-driven path.
func runQueryBenchPlain(b *testing.B, raw string) {
	b.Helper()
	n := benchSize()
	_, people := seedDBPlain(b, n)
	ctx := context.Background()

	q, err := query.Parse(raw)
	if err != nil {
		b.Fatalf("parse %q: %v", raw, err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := people.Find(ctx, q); err != nil {
			b.Fatalf("find: %v", err)
		}
	}
}

// --- Query benchmarks. Each is a representative shape. The _Indexed
// variants exercise the Phase 2 planner; _FullScan variants run on the
// PersonPlain bucket so cost can be compared directly. ---

// BenchmarkFind_EqIndexed: equality on an indexed field. Phase 2 turns
// this into an index seek + per-pk Get, eliminating the full bucket scan.
func BenchmarkFind_EqIndexed(b *testing.B) {
	runQueryBench(b, "country=US")
}

// BenchmarkFind_EqFullScan: same query shape on a non-indexed bucket.
// This is the baseline we compare against the indexed result above.
func BenchmarkFind_EqFullScan(b *testing.B) {
	runQueryBenchPlain(b, "country=US")
}

// BenchmarkFind_EqUnindexed: equality on a non-indexed field of an
// otherwise-indexed bucket. Falls back to full scan even when other
// fields are indexed.
func BenchmarkFind_EqUnindexed(b *testing.B) {
	runQueryBench(b, "score=42.5")
}

// BenchmarkFind_RangeIndexed: index range scan on `age`.
func BenchmarkFind_RangeIndexed(b *testing.B) {
	runQueryBench(b, "age[gte]=30&age[lt]=50")
}

// BenchmarkFind_RangeFullScan: same range, no index.
func BenchmarkFind_RangeFullScan(b *testing.B) {
	runQueryBenchPlain(b, "age[gte]=30&age[lt]=50")
}

// BenchmarkFind_OrAcrossFields: OR with two different fields. Phase 2 can
// only use one index hit, so this remains close to full-scan cost.
func BenchmarkFind_OrAcrossFields(b *testing.B) {
	runQueryBench(b, "country=US|age[gte]=60")
}

// BenchmarkFind_InListIndexed: IN list across 5 countries; planner emits
// 5 point seeks.
func BenchmarkFind_InListIndexed(b *testing.B) {
	runQueryBench(b, "country=US,DE,FR,JP,BR")
}

// BenchmarkFind_LikePrefix: LIKE with a literal prefix; not yet served
// by an index, full scan with cheap predicate.
func BenchmarkFind_LikePrefix(b *testing.B) {
	runQueryBench(b, "name[like]=Alice%25")
}

// BenchmarkFind_ILikeContains: case-insensitive LIKE with leading wildcard,
// fundamentally unindexable.
func BenchmarkFind_ILikeContains(b *testing.B) {
	runQueryBench(b, "name[ilike]=%25ali%25")
}

// BenchmarkFind_NestedDotPath: predicate on nested struct field, no index.
func BenchmarkFind_NestedDotPath(b *testing.B) {
	runQueryBench(b, "address.city=Berlin")
}

// BenchmarkFind_EqIndexedPlusResidual: seek by indexed country, then
// filter the small candidate set with an unindexable ILike on name. This
// is the sweet spot the planner targets.
func BenchmarkFind_EqIndexedPlusResidual(b *testing.B) {
	runQueryBench(b, "country=US&name[ilike]=ali%25")
}

// BenchmarkFind_ComplexAndOr: parenthesised mix of AND and OR plus a range.
func BenchmarkFind_ComplexAndOr(b *testing.B) {
	runQueryBench(b, "(country=US|country=DE)&age[gte]=30&age[lt]=60")
}

// BenchmarkFind_SortLimit: sorted by score desc, top 50. The seek hits
// `country=US`, then sort buffers the (smaller) candidate set.
func BenchmarkFind_SortLimit(b *testing.B) {
	runQueryBench(b, "country=US&_sort=-score&_limit=50")
}

// BenchmarkFind_FullScan: matches every record, then sorts by age desc.
// The most expensive supported shape; no Where means no index.
func BenchmarkFind_FullScan(b *testing.B) {
	runQueryBench(b, "_sort=-age")
}

// BenchmarkWalk_Indexed is the streaming counterpart of an indexed Find.
func BenchmarkWalk_Indexed(b *testing.B) {
	n := benchSize()
	_, people := seedDB(b, n)
	ctx := context.Background()
	q, err := query.Parse("country=US")
	if err != nil {
		b.Fatalf("parse: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var seen int
		err := people.Walk(ctx, q, func(*Person) error {
			seen++

			return nil
		})
		if err != nil {
			b.Fatalf("walk: %v", err)
		}
	}
}

// BenchmarkInsert_Indexed compares against BenchmarkInsert above to show
// the cost of maintaining indexes on writes.
func BenchmarkInsert_Indexed(b *testing.B) {
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	people, err := bw.RegisterBucket[Person](db, "people")
	if err != nil {
		b.Fatalf("register: %v", err)
	}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := Person{
			ID:      fmt.Sprintf("p%010d", i),
			Name:    "Alice",
			Email:   fmt.Sprintf("u%d@x", i),
			Age:     30,
			Country: "US",
		}
		if err := people.Insert(ctx, &p); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}
