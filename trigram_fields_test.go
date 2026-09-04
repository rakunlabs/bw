package bw

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// dualChunk carries two trigram-tagged fields, which is what makes
// indexedness a per-field question rather than a per-document one.
type dualChunk struct {
	ID   string `bw:"id,pk"`
	Code string `bw:"code,trigram"`
	Docs string `bw:"docs,trigram"`
}

func newDualBucket(t testing.TB) (*Bucket[dualChunk], context.Context) {
	t.Helper()

	db, err := Open("", WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	bucket, err := RegisterBucket[dualChunk](db, "dual")
	if err != nil {
		t.Fatal(err)
	}

	return bucket, context.Background()
}

// dualRegexIDs runs a regex search against one named field.
func dualRegexIDs(t testing.TB, b *Bucket[dualChunk], ctx context.Context, field, pattern string) []string {
	t.Helper()

	hits, total, err := b.RegexSearch(ctx, pattern, RegexOptions{Field: field, Limit: 100})
	if err != nil {
		t.Fatalf("regex search %q on %q: %v", pattern, field, err)
	}
	ids := make([]string, 0, len(hits))
	for _, h := range hits {
		ids = append(ids, h.ID)
	}
	if int(total) != len(ids) {
		t.Fatalf("total = %d but got %d ids for %q on %q", total, len(ids), pattern, field)
	}

	return ids
}

// triDocIDFor reads the interned document id of pk, or reports absence.
func triDocIDFor(t testing.TB, db *DB, bucket, pk string) (uint32, bool) {
	t.Helper()

	var (
		id    uint32
		found bool
	)
	if err := db.bdb.View(func(btx *badger.Txn) error {
		item, err := btx.Get(triDocIDKey(bucket, []byte(pk)))
		switch {
		case err == nil:
			found = true

			return item.Value(func(val []byte) error {
				id = binary.BigEndian.Uint32(val)

				return nil
			})
		case errors.Is(err, badger.ErrKeyNotFound):
			return nil
		default:
			return err
		}
	}); err != nil {
		t.Fatalf("docid for %q: %v", pk, err)
	}

	return id, found
}

// fieldMarked reports whether the per-(field, document) indexed marker
// exists — the flag the write path diffs against.
func fieldMarked(t testing.TB, db *DB, bucket, field, pk string) bool {
	t.Helper()

	id, ok := triDocIDFor(t, db, bucket, pk)
	if !ok {
		return false
	}

	marked := false
	if err := db.bdb.View(func(btx *badger.Txn) error {
		switch _, err := btx.Get(triFieldDocKey(bucket, field, id)); {
		case err == nil:
			marked = true

			return nil
		case errors.Is(err, badger.ErrKeyNotFound):
			return nil
		default:
			return err
		}
	}); err != nil {
		t.Fatalf("marker for %q/%q: %v", field, pk, err)
	}

	return marked
}

// TestTrigramSecondFieldBackfill is the sharp form of per-field
// indexedness: a bucket that already indexes one field gains the tag on
// a second one, and the identity migration that rewrites every record
// must index the second field's existing content.
//
// A per-document "already indexed" flag gets this wrong and cannot be
// made right: the document has an interned id (the first field earned
// it), so the rewrite diffs the second field's unchanged value against
// itself, produces no postings, and leaves the field permanently
// unsearchable with no error anywhere.
func TestTrigramSecondFieldBackfill(t *testing.T) {
	db, err := Open("", WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// v1 indexes only Code; Docs is stored but untagged.
	type dualV1 struct {
		ID   string `bw:"id,pk"`
		Code string `bw:"code,trigram"`
		Docs string `bw:"docs"`
	}

	ctx := context.Background()
	old, err := RegisterBucket[dualV1](db, "dual", WithVersion[dualV1](1))
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Insert(ctx, &dualV1{
		ID:   "a#0",
		Code: "func Handler() error { return ErrTxnTooBig }",
		Docs: "Handler flushes the zephyrquux before returning.",
	}); err != nil {
		t.Fatal(err)
	}

	// v2 tags Docs as well; the identity migration rewrites the record.
	bucket, err := RegisterBucket[dualChunk](db, "dual",
		WithTypedMigration[dualChunk, dualChunk](1, 2, func(_ context.Context, rec *dualChunk) (*dualChunk, error) { return rec, nil }),
	)
	if err != nil {
		t.Fatalf("re-register with the second trigram tag: %v", err)
	}

	if ids := dualRegexIDs(t, bucket, ctx, "docs", `zephyrquux`); len(ids) != 1 || ids[0] != "a#0" {
		t.Fatalf("docs search after backfill: ids = %v, want [a#0]", ids)
	}
	// The first field must not have been disturbed by the backfill.
	if ids := dualRegexIDs(t, bucket, ctx, "code", `ErrTxnTooBig`); len(ids) != 1 || ids[0] != "a#0" {
		t.Fatalf("code search after backfill: ids = %v, want [a#0]", ids)
	}
	if !fieldMarked(t, db, "dual", "docs", "a#0") {
		t.Error("docs marker missing after backfill")
	}
}

// TestTrigramSecondFieldAddedLater covers the plain-write shape of the
// same contract: a document first written with only one field populated
// must index the other field when a later write fills it in.
func TestTrigramSecondFieldAddedLater(t *testing.T) {
	bucket, ctx := newDualBucket(t)

	rec := &dualChunk{ID: "a#0", Code: "func Handler() error { return ErrTxnTooBig }"}
	if err := bucket.Insert(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if ids := dualRegexIDs(t, bucket, ctx, "docs", `zephyrquux`); len(ids) != 0 {
		t.Fatalf("docs search before the field is populated: ids = %v, want none", ids)
	}
	if fieldMarked(t, bucket.db, "dual", "docs", "a#0") {
		t.Error("docs marker set while the field is empty")
	}

	// Same Code, Docs now populated.
	rec.Docs = "Handler flushes the zephyrquux before returning."
	if err := bucket.Insert(ctx, rec); err != nil {
		t.Fatal(err)
	}

	if ids := dualRegexIDs(t, bucket, ctx, "docs", `zephyrquux`); len(ids) != 1 || ids[0] != "a#0" {
		t.Fatalf("docs search after the field is populated: ids = %v, want [a#0]", ids)
	}
	if ids := dualRegexIDs(t, bucket, ctx, "code", `ErrTxnTooBig`); len(ids) != 1 || ids[0] != "a#0" {
		t.Fatalf("code search after the second field was added: ids = %v, want [a#0]", ids)
	}
}

// TestTrigramEmptiedFieldClearsMarker pins the other half of the marker
// contract: a field that stops emitting trigrams must stop claiming to
// be indexed, so a later write that re-fills it is diffed against
// nothing rather than against a term set that is no longer on disk.
func TestTrigramEmptiedFieldClearsMarker(t *testing.T) {
	bucket, ctx := newDualBucket(t)

	rec := &dualChunk{
		ID:   "a#0",
		Code: "func Handler() error { return ErrTxnTooBig }",
		Docs: "Handler flushes the zephyrquux before returning.",
	}
	if err := bucket.Insert(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if ids := dualRegexIDs(t, bucket, ctx, "docs", `zephyrquux`); len(ids) != 1 {
		t.Fatalf("docs search after insert: ids = %v, want [a#0]", ids)
	}
	if !fieldMarked(t, bucket.db, "dual", "docs", "a#0") {
		t.Fatal("docs marker missing after insert")
	}

	// Empty the field: its postings and its marker both go away.
	rec.Docs = ""
	if err := bucket.Insert(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if ids := dualRegexIDs(t, bucket, ctx, "docs", `zephyrquux`); len(ids) != 0 {
		t.Fatalf("docs search after emptying: ids = %v, want none", ids)
	}
	if fieldMarked(t, bucket.db, "dual", "docs", "a#0") {
		t.Error("docs marker still set after the field was emptied")
	}
	if n := dualPostingCount(t, bucket.db, "dual", "docs", "zep"); n != 0 {
		t.Errorf("posting list for %q has %d entries after emptying, want 0", "zep", n)
	}
	// The untouched field keeps its own marker and postings.
	if !fieldMarked(t, bucket.db, "dual", "code", "a#0") {
		t.Error("code marker cleared by an unrelated field's update")
	}
	if ids := dualRegexIDs(t, bucket, ctx, "code", `ErrTxnTooBig`); len(ids) != 1 {
		t.Fatalf("code search after emptying docs: ids = %v, want [a#0]", ids)
	}

	// Re-fill it: the diff must start from an empty previous set again.
	rec.Docs = "Handler flushes the zephyrquux before returning."
	if err := bucket.Insert(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if ids := dualRegexIDs(t, bucket, ctx, "docs", `zephyrquux`); len(ids) != 1 || ids[0] != "a#0" {
		t.Fatalf("docs search after re-populating: ids = %v, want [a#0]", ids)
	}
	if !fieldMarked(t, bucket.db, "dual", "docs", "a#0") {
		t.Error("docs marker missing after re-populating")
	}
}

// TestTrigramDeleteClearsFieldMarkers checks the delete path takes the
// markers with the postings. A marker outliving its postings would make
// a re-index of the same pk diff against a term set that is gone.
func TestTrigramDeleteClearsFieldMarkers(t *testing.T) {
	bucket, ctx := newDualBucket(t)

	rec := &dualChunk{
		ID:   "a#0",
		Code: "func Handler() error { return ErrTxnTooBig }",
		Docs: "Handler flushes the zephyrquux before returning.",
	}
	if err := bucket.Insert(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Delete(ctx, "a#0"); err != nil {
		t.Fatal(err)
	}

	for _, field := range []string{"code", "docs"} {
		if fieldMarked(t, bucket.db, "dual", field, "a#0") {
			t.Errorf("%s marker survived the document's deletion", field)
		}
	}
	if n := dualPostingCount(t, bucket.db, "dual", "docs", "zep"); n != 0 {
		t.Errorf("posting list for %q has %d entries after delete, want 0", "zep", n)
	}

	// Re-inserting the same pk reuses its id and must index both fields.
	if err := bucket.Insert(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if ids := dualRegexIDs(t, bucket, ctx, "docs", `zephyrquux`); len(ids) != 1 {
		t.Fatalf("docs search after re-insert: ids = %v, want [a#0]", ids)
	}
	if ids := dualRegexIDs(t, bucket, ctx, "code", `ErrTxnTooBig`); len(ids) != 1 {
		t.Fatalf("code search after re-insert: ids = %v, want [a#0]", ids)
	}
}

// dualPostingCount counts the postings of one trigram, untyped so it can
// be used from a bucket of any record type.
func dualPostingCount(t testing.TB, db *DB, bucket, field, tri string) int {
	t.Helper()

	if len(tri) != 3 {
		t.Fatalf("trigram %q must be three bytes", tri)
	}
	packed := uint32(tri[0])<<16 | uint32(tri[1])<<8 | uint32(tri[2])
	prefix := triPostingTermPrefix(bucket, field, packed)

	n := 0
	if err := db.bdb.View(func(btx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := btx.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			n++
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	return n
}
