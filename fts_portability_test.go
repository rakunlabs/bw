package bw_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rakunlabs/bw"
)

type SearchableArticle struct {
	ID    string `bw:"id,pk"`
	Title string `bw:"title,fts"`
	Body  string `bw:"body,fts"`
}

// TestFTS_BackupRestorePortableAcrossDirs verifies the headline benefit
// of the all-Badger FTS layout: a backup taken on one path can be
// restored on a different path and Search continues to work without any
// rebuild step or external state.
func TestFTS_BackupRestorePortableAcrossDirs(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	src, err := bw.Open(srcDir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	srcBucket, err := bw.RegisterBucket[SearchableArticle](src, "articles")
	if err != nil {
		t.Fatal(err)
	}
	docs := []*SearchableArticle{
		{ID: "1", Title: "Go programming", Body: "Go is a statically typed language."},
		{ID: "2", Title: "Rust systems", Body: "Rust is memory-safe by default."},
		{ID: "3", Title: "Distributed databases", Body: "Replication and consistency."},
	}
	for _, d := range docs {
		if err := srcBucket.Insert(ctx, d); err != nil {
			t.Fatalf("insert %s: %v", d.ID, err)
		}
	}

	var buf bytes.Buffer
	if _, err := src.Backup(&buf, 0, false); err != nil {
		t.Fatal(err)
	}
	src.Close()

	// Fresh DB at a completely different path.
	dst, err := bw.Open(dstDir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if err := dst.Restore(&buf); err != nil {
		t.Fatal(err)
	}

	dstBucket, err := bw.RegisterBucket[SearchableArticle](dst, "articles")
	if err != nil {
		t.Fatal(err)
	}

	results, total, err := dstBucket.Search(ctx, "Rust", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(results) != 1 {
		t.Fatalf("Search Rust on restored: total=%d, len=%d", total, len(results))
	}
	if results[0].Record.ID != "2" {
		t.Fatalf("unexpected hit: %+v", results[0])
	}

	// Verify nothing was written under _fts_* — proof that the
	// FTS state lives entirely inside Badger.
	entries, _ := os.ReadDir(dstDir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == "" && e.IsDir() {
			if hasPrefix(e.Name(), "_fts_") {
				t.Fatalf("unexpected _fts_ directory in dst: %q", e.Name())
			}
		}
	}
}

// TestFTS_IncrementalBackupCarriesFTS confirms that an incremental
// Backup (since=N) carries FTS postings written after N alongside the
// data, so a follower restored from that incremental sees the new
// records as searchable.
func TestFTS_IncrementalBackupCarriesFTS(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	src, err := bw.Open(srcDir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	srcBucket, err := bw.RegisterBucket[SearchableArticle](src, "events")
	if err != nil {
		t.Fatal(err)
	}
	if err := srcBucket.Insert(ctx, &SearchableArticle{ID: "1", Title: "alpha", Body: "first"}); err != nil {
		t.Fatal(err)
	}

	// Bootstrap dst with a full backup.
	var full bytes.Buffer
	if _, err := src.Backup(&full, 0, false); err != nil {
		t.Fatal(err)
	}
	baselineV := src.Version()

	dst, err := bw.Open(dstDir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	dstBucket, err := bw.RegisterBucket[SearchableArticle](dst, "events")
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.Restore(&full); err != nil {
		t.Fatal(err)
	}

	// Source advances.
	if err := srcBucket.Insert(ctx, &SearchableArticle{ID: "2", Title: "beta", Body: "needle"}); err != nil {
		t.Fatal(err)
	}
	if err := srcBucket.Insert(ctx, &SearchableArticle{ID: "3", Title: "gamma", Body: "haystack"}); err != nil {
		t.Fatal(err)
	}

	// Incremental backup since baseline.
	var incr bytes.Buffer
	if _, err := src.Backup(&incr, baselineV, false); err != nil {
		t.Fatal(err)
	}
	if err := dst.Restore(&incr); err != nil {
		t.Fatal(err)
	}

	for _, q := range []struct {
		term, want string
	}{
		{"needle", "2"},
		{"haystack", "3"},
		{"alpha", "1"},
	} {
		results, _, err := dstBucket.Search(ctx, q.term, 10, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 || results[0].Record.ID != q.want {
			t.Fatalf("search %q: want id=%s, got %+v", q.term, q.want, results)
		}
	}

	src.Close()
}

// TestFTS_RollbackLeavesNoStaleTerms verifies the atomic property: a
// transaction that writes data + FTS terms but ultimately fails leaves
// no posting list residue.
func TestFTS_RollbackLeavesNoStaleTerms(t *testing.T) {
	ctx := context.Background()
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucket, err := bw.RegisterBucket[SearchableArticle](db, "rb")
	if err != nil {
		t.Fatal(err)
	}

	// Force a rollback: inside Update, insert via the typed bucket
	// (which writes data + FTS) and then return an error.
	wantErr := errSentinel
	err = db.Update(func(tx *bw.Tx) error {
		if err := bucket.InsertTx(tx, &SearchableArticle{ID: "x", Title: "phantom", Body: "ghost"}); err != nil {
			return err
		}
		return wantErr
	})
	if err != wantErr {
		t.Fatalf("expected sentinel, got %v", err)
	}

	// Search must NOT find phantom — txn was rolled back, no
	// postings should be visible.
	results, total, err := bucket.Search(ctx, "phantom", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(results) != 0 {
		t.Fatalf("after rollback: want 0 hits, got total=%d len=%d", total, len(results))
	}
}

var errSentinel = errSentinelType("rollback")

type errSentinelType string

func (e errSentinelType) Error() string { return string(e) }

// TestFTS_UpdateClearsStaleTerms verifies that updating a record drops
// terms from the previous version so they no longer match.
func TestFTS_UpdateClearsStaleTerms(t *testing.T) {
	ctx := context.Background()
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucket, err := bw.RegisterBucket[SearchableArticle](db, "upd")
	if err != nil {
		t.Fatal(err)
	}

	if err := bucket.Insert(ctx, &SearchableArticle{ID: "1", Title: "kitchen", Body: "spoon fork"}); err != nil {
		t.Fatal(err)
	}

	// Replace with non-overlapping terms.
	if err := bucket.Insert(ctx, &SearchableArticle{ID: "1", Title: "garage", Body: "wrench"}); err != nil {
		t.Fatal(err)
	}

	// Old terms must not match.
	for _, dead := range []string{"kitchen", "spoon", "fork"} {
		results, _, err := bucket.Search(ctx, dead, 10, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 0 {
			t.Fatalf("stale term %q still matches: %+v", dead, results)
		}
	}
	// New terms must match.
	for _, alive := range []string{"garage", "wrench"} {
		results, _, err := bucket.Search(ctx, alive, 10, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatalf("new term %q: want 1 hit, got %d", alive, len(results))
		}
	}
}

// TestFTS_BM25Ranking sanity-checks that BM25 prefers more-relevant
// documents (one with the query term in title + body twice over a
// merely-mentioning doc).
func TestFTS_BM25Ranking(t *testing.T) {
	ctx := context.Background()
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucket, err := bw.RegisterBucket[SearchableArticle](db, "rank")
	if err != nil {
		t.Fatal(err)
	}
	_ = bucket.Insert(ctx, &SearchableArticle{
		ID: "high", Title: "elephant elephant", Body: "elephant elephant elephant",
	})
	_ = bucket.Insert(ctx, &SearchableArticle{
		ID: "low", Title: "zebra", Body: "and an elephant nearby",
	})

	results, _, err := bucket.Search(ctx, "elephant", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("want 2 hits, got %d", len(results))
	}
	if results[0].Record.ID != "high" {
		t.Fatalf("BM25: want 'high' first, got %q (scores: %.3f, %.3f)",
			results[0].Record.ID, results[0].Score, results[1].Score)
	}
	if results[0].Score <= results[1].Score {
		t.Fatalf("BM25: want decreasing scores, got %.3f <= %.3f",
			results[0].Score, results[1].Score)
	}
}

// hasPrefix is a tiny helper to avoid dragging in strings for one call.
func hasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}
