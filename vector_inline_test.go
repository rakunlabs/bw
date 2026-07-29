package bw

import (
	"context"
	"math/rand"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

// inlineRec keeps the embedding in the record's encoded value (the default).
type inlineRec struct {
	ID   string    `bw:"id,pk"`
	Repo string    `bw:"repo,index"`
	Emb  []float32 `bw:"emb,vector(metric=cosine)"`
}

// noInlineRec stores the embedding only in the vector index.
type noInlineRec struct {
	ID   string    `bw:"id,pk"`
	Repo string    `bw:"repo,index"`
	Emb  []float32 `bw:"emb,vector(metric=cosine,inline=false)"`
}

// TestVectorInlineDefault documents the default: a vector round-trips through
// the record, because dropping it silently would surprise every existing user.
func TestVectorInlineDefault(t *testing.T) {
	db, err := Open(t.TempDir(), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	b, err := RegisterBucket[inlineRec](db, "inline")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	vec := randVecBench(rand.New(rand.NewSource(1)), 16)

	if err := b.Insert(ctx, &inlineRec{ID: "a", Repo: "alpha", Emb: vec}); err != nil {
		t.Fatal(err)
	}

	got, err := b.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Emb) != len(vec) {
		t.Fatalf("got %d components, want %d", len(got.Emb), len(vec))
	}
	for i := range vec {
		if got.Emb[i] != vec[i] {
			t.Fatalf("component %d: got %v, want %v", i, got.Emb[i], vec[i])
		}
	}
}

// TestVectorNoInlineOmitsFromRecord covers the opt-in: the field comes back
// empty, but everything that depends on the vector still works, because the
// vector index holds the real copy.
func TestVectorNoInlineOmitsFromRecord(t *testing.T) {
	db, err := Open(t.TempDir(), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	b, err := RegisterBucket[noInlineRec](db, "noinline")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	rng := rand.New(rand.NewSource(2))

	const n = 40
	vecs := make(map[string][]float32, n)
	for i := range n {
		id := itoaBench(i)
		v := randVecBench(rng, 16)
		vecs[id] = v

		if err := b.Insert(ctx, &noInlineRec{ID: id, Repo: "alpha", Emb: v}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("record no longer carries the vector", func(t *testing.T) {
		got, err := b.Get(ctx, "0")
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Emb) != 0 {
			t.Fatalf("got %d components, want the field left empty", len(got.Emb))
		}
		if got.Repo != "alpha" {
			t.Fatalf("other fields must survive: repo = %q", got.Repo)
		}
	})

	t.Run("search still ranks by the stored vector", func(t *testing.T) {
		// Query with a record's own embedding: it must rank itself first.
		want := "7"
		hits, err := b.SearchVector(ctx, vecs[want], SearchVectorOptions{K: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != 1 {
			t.Fatalf("got %d hits, want 1", len(hits))
		}
		if hits[0].Record.ID != want {
			t.Fatalf("got %q, want %q", hits[0].Record.ID, want)
		}
	})

	t.Run("filtered search still works", func(t *testing.T) {
		hits, err := b.SearchVector(ctx, vecs["3"], SearchVectorOptions{
			K:      5,
			Filter: mustParse(t, "repo=alpha"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != 5 {
			t.Fatalf("got %d hits, want 5", len(hits))
		}
		if hits[0].Record.ID != "3" {
			t.Fatalf("got %q first, want %q", hits[0].Record.ID, "3")
		}
	})
}

// TestVectorNoInlineDoesNotMutateCaller makes sure encoding a record without
// its embedding does not clear the embedding on the value the caller passed
// in. Insert takes a pointer; silently emptying a caller's field would be a
// nasty surprise for anyone reusing the struct.
func TestVectorNoInlineDoesNotMutateCaller(t *testing.T) {
	db, err := Open(t.TempDir(), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	b, err := RegisterBucket[noInlineRec](db, "noinline")
	if err != nil {
		t.Fatal(err)
	}

	vec := randVecBench(rand.New(rand.NewSource(3)), 16)
	rec := &noInlineRec{ID: "a", Repo: "alpha", Emb: vec}

	if err := b.Insert(context.Background(), rec); err != nil {
		t.Fatal(err)
	}

	if len(rec.Emb) != len(vec) {
		t.Fatalf("caller's record was mutated: got %d components, want %d", len(rec.Emb), len(vec))
	}
}

// TestVectorNoInlineShrinksStoredRecords is the point of the option: at the
// widths embedding models produce, the inline copy is the bulk of a record.
func TestVectorNoInlineShrinksStoredRecords(t *testing.T) {
	const (
		dim = 768
		n   = 50
	)

	measure := func(t *testing.T, noInline bool) int {
		t.Helper()

		db, err := Open(t.TempDir(), WithLogger(nil))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })

		rng := rand.New(rand.NewSource(4))
		ctx := context.Background()

		total := 0
		if noInline {
			b, err := RegisterBucket[noInlineRec](db, "b")
			if err != nil {
				t.Fatal(err)
			}
			for i := range n {
				if err := b.Insert(ctx, &noInlineRec{ID: itoaBench(i), Repo: "alpha", Emb: randVecBench(rng, dim)}); err != nil {
					t.Fatal(err)
				}
			}
			total = storedRecordBytes(t, db, "b")
		} else {
			b, err := RegisterBucket[inlineRec](db, "b")
			if err != nil {
				t.Fatal(err)
			}
			for i := range n {
				if err := b.Insert(ctx, &inlineRec{ID: itoaBench(i), Repo: "alpha", Emb: randVecBench(rng, dim)}); err != nil {
					t.Fatal(err)
				}
			}
			total = storedRecordBytes(t, db, "b")
		}

		return total
	}

	withVector := measure(t, false)
	withoutVector := measure(t, true)

	t.Logf("record bytes for %d records at dim=%d: inline=%d, no-inline=%d (%.1fx smaller)",
		n, dim, withVector, withoutVector, float64(withVector)/float64(withoutVector))

	// The embedding is 768 float32s at five msgpack bytes each against a
	// handful of bytes of id and repo, so the reduction is an order of
	// magnitude. Assert something well inside that so the test is about the
	// behaviour, not the exact encoding.
	if withoutVector*4 > withVector {
		t.Fatalf("no-inline records are %d bytes vs %d inline; expected a much larger reduction",
			withoutVector, withVector)
	}
}

// storedRecordBytes sums the encoded size of every record in a bucket's data
// keyspace.
func storedRecordBytes(t *testing.T, db *DB, bucket string) int {
	t.Helper()

	total := 0
	prefix := dataPrefix(bucket)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix

	if err := db.View(func(tx *Tx) error {
		it := tx.btx.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			total += int(it.Item().ValueSize())
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	return total
}
