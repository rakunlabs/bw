package bw

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/query"
)

// migChunkV1 and migChunkV2 model the common shape of a schema bump: a field
// is added, and the embedding rides along unchanged.
type migChunkV1 struct {
	ID   string    `bw:"id,pk"`
	Repo string    `bw:"repo,index"`
	Body string    `bw:"body"`
	Emb  []float32 `bw:"emb,vector(metric=cosine)"`
}

type migChunkV2 struct {
	ID   string    `bw:"id,pk"`
	Repo string    `bw:"repo,index"`
	Kind string    `bw:"kind,index"`
	Body string    `bw:"body"`
	Emb  []float32 `bw:"emb,vector(metric=cosine)"`
}

// snapshotVectorKeyspace returns every key and value under the bucket's vector
// namespace: the raw vectors, the HNSW neighbour lists, the per-node levels,
// the entry point and the manifest.
//
// Comparing two snapshots is how a test can tell whether the graph was
// genuinely left alone, rather than rebuilt into something that merely
// answers queries the same way.
func snapshotVectorKeyspace(t *testing.T, db *DB, bucket string) map[string]string {
	t.Helper()

	out := map[string]string{}
	prefix := vecBucketPrefix(bucket)

	if err := db.bdb.View(func(btx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix

		it := btx.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			out[string(item.KeyCopy(nil))] = string(val)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	return out
}

func seedMigChunks(t *testing.T, dir string, n, dim int) {
	t.Helper()

	db, err := Open(dir, WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}

	b, err := RegisterBucket[migChunkV1](db, "chunks", WithVersion[migChunkV1](1))
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	ctx := context.Background()
	rng := rand.New(rand.NewSource(1))

	for i := range n {
		rec := &migChunkV1{
			ID:   fmt.Sprintf("c%04d", i),
			Repo: "o/a",
			Body: "text",
			Emb:  randVecBench(rng, dim),
		}
		if err := b.Insert(ctx, rec); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestTypedMigrationLeavesVectorGraphAlone is the guard on what made a schema
// bump expensive.
//
// A typed migration that carries the embedding through unchanged used to wipe
// the vector keyspace and rebuild the entire HNSW graph — spending a full
// index build to arrive back at the index it started with. On a corpus of any
// size that turns "add a field" into an outage-length operation.
//
// The assertion is on the keyspace rather than on elapsed time, because the
// question is whether the work happened at all, not how fast it was.
func TestTypedMigrationLeavesVectorGraphAlone(t *testing.T) {
	const (
		n   = 300
		dim = 64
	)

	dir := t.TempDir()
	seedMigChunks(t, dir, n, dim)

	// Snapshot the graph as the v1 bucket left it.
	probe, err := Open(dir, WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterBucket[migChunkV1](probe, "chunks", WithVersion[migChunkV1](1)); err != nil {
		t.Fatal(err)
	}
	before := snapshotVectorKeyspace(t, probe, "chunks")
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	if len(before) == 0 {
		t.Fatal("no vector keys were written by the seed")
	}

	db, err := Open(dir, WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b, err := RegisterBucket[migChunkV2](db, "chunks",
		WithVersion[migChunkV2](2),
		WithTypedMigration(1, 2, func(_ context.Context, old *migChunkV1) (*migChunkV2, error) {
			return &migChunkV2{
				ID:   old.ID,
				Repo: old.Repo,
				Kind: "repo",
				Body: old.Body,
				Emb:  old.Emb,
			}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	after := snapshotVectorKeyspace(t, db, "chunks")

	if len(after) != len(before) {
		t.Fatalf("vector keyspace has %d keys after the migration, had %d before", len(after), len(before))
	}
	for k, want := range before {
		got, ok := after[k]
		if !ok {
			t.Fatalf("vector key %x disappeared during the migration", k)
		}
		if got != want {
			t.Fatalf("vector key %x was rewritten by a migration that did not change any embedding", k)
		}
	}

	// The migration still has to have done its actual job.
	ctx := context.Background()

	rec, err := b.Get(ctx, "c0007")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Kind != "repo" {
		t.Fatalf("Kind = %q, want the migration to have filled it", rec.Kind)
	}

	q, err := query.Parse("kind=repo")
	if err != nil {
		t.Fatal(err)
	}
	found, err := b.Find(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != n {
		t.Fatalf("the new index returned %d records, want %d", len(found), n)
	}

	// And search must still work through the untouched graph.
	hits, err := b.SearchVector(ctx, randVecBench(rand.New(rand.NewSource(9)), dim), SearchVectorOptions{K: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 5 {
		t.Fatalf("got %d hits, want 5", len(hits))
	}
}

// TestVectorReembedRebuildsGraph is the other side of the same switch: a
// migration whose whole purpose is to replace the embeddings must rebuild the
// graph, because the old edges describe geometry that no longer exists.
func TestVectorReembedRebuildsGraph(t *testing.T) {
	const (
		n   = 200
		dim = 64
	)

	dir := t.TempDir()
	seedMigChunks(t, dir, n, dim)

	probe, err := Open(dir, WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterBucket[migChunkV1](probe, "chunks", WithVersion[migChunkV1](1)); err != nil {
		t.Fatal(err)
	}
	before := snapshotVectorKeyspace(t, probe, "chunks")
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(dir, WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Re-embed every record with a different, deterministic vector.
	rng := rand.New(rand.NewSource(77))

	b, err := RegisterBucket[migChunkV1](db, "chunks",
		WithVersion[migChunkV1](2),
		WithVectorReembed(1, 2, func(_ context.Context, _ *migChunkV1) ([]float32, error) {
			return randVecBench(rng, dim), nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	after := snapshotVectorKeyspace(t, db, "chunks")

	changed := 0
	for k, want := range before {
		if after[k] != want {
			changed++
		}
	}
	if changed == 0 {
		t.Fatal("re-embedding left the vector keyspace untouched")
	}

	// The rebuilt index must still be searchable and complete.
	ctx := context.Background()

	hits, err := b.SearchVector(ctx, randVecBench(rand.New(rand.NewSource(5)), dim), SearchVectorOptions{K: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 10 {
		t.Fatalf("got %d hits, want 10", len(hits))
	}
}

// TestWriteVecSkipsUnchangedVector pins the mechanism the migration relies on,
// so it keeps working for ordinary updates too: rewriting a record without
// touching its embedding must not disturb the graph.
func TestWriteVecSkipsUnchangedVector(t *testing.T) {
	const dim = 32

	db, err := Open(t.TempDir(), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b, err := RegisterBucket[migChunkV2](db, "chunks")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	rng := rand.New(rand.NewSource(3))

	vecs := map[string][]float32{}
	for i := range 100 {
		id := fmt.Sprintf("c%03d", i)
		vecs[id] = randVecBench(rng, dim)

		if err := b.Insert(ctx, &migChunkV2{ID: id, Repo: "o/a", Kind: "repo", Body: "one", Emb: vecs[id]}); err != nil {
			t.Fatal(err)
		}
	}

	before := snapshotVectorKeyspace(t, db, "chunks")

	// Update a non-vector field on every record.
	for id, v := range vecs {
		if err := b.Insert(ctx, &migChunkV2{ID: id, Repo: "o/a", Kind: "repo", Body: "two", Emb: v}); err != nil {
			t.Fatal(err)
		}
	}

	after := snapshotVectorKeyspace(t, db, "chunks")

	if len(after) != len(before) {
		t.Fatalf("vector keyspace changed size: %d -> %d", len(before), len(after))
	}
	for k, want := range before {
		if after[k] != want {
			t.Fatalf("vector key %x was rewritten by an update that did not change the embedding", k)
		}
	}

	// The non-vector change must still have landed.
	got, err := b.Get(ctx, "c000")
	if err != nil {
		t.Fatal(err)
	}
	if got.Body != "two" {
		t.Fatalf("Body = %q, want the update to have been written", got.Body)
	}

	// Changing the embedding must still update the graph.
	newVec := randVecBench(rng, dim)
	if err := b.Insert(ctx, &migChunkV2{ID: "c000", Repo: "o/a", Kind: "repo", Body: "two", Emb: newVec}); err != nil {
		t.Fatal(err)
	}

	moved := snapshotVectorKeyspace(t, db, "chunks")
	if moved[string(vecRawKey("chunks", "emb", []byte("c000")))] == before[string(vecRawKey("chunks", "emb", []byte("c000")))] {
		t.Fatal("changing the embedding did not rewrite its stored vector")
	}
}
