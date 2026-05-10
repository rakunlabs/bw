package bw_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/bw/codec"
)

// --- shared types ---
//
// We define the v1 and v2 shapes inline so the tests double as
// living documentation of how user migrations are expected to be
// structured (define the old struct, decode-transform-encode in the
// migration callback, swap the bucket type to the new struct).

// userV1 is the original schema: just a name field.
type userV1 struct {
	ID   string `bw:"id,pk"`
	Name string `bw:"name"`
}

// userV2 splits Name into First / Last.
type userV2 struct {
	ID    string `bw:"id,pk"`
	First string `bw:"first,index"`
	Last  string `bw:"last"`
}

// userV3 adds a derived email built from first.last@example.com.
type userV3 struct {
	ID    string `bw:"id,pk"`
	First string `bw:"first,index"`
	Last  string `bw:"last"`
	Email string `bw:"email,index"`
}

// TestUserMigration_TypedSingleStep validates the simplest path: one
// typed migration step from v1 to v2.
func TestUserMigration_TypedSingleStep(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Open with v1 schema, insert a few records.
	db1, err := bw.Open(dir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	b1, err := bw.RegisterBucket[userV1](db1, "users", bw.WithVersion[userV1](1))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"Ali Veli", "Ayse Yilmaz", "Mehmet Demir"} {
		id := strings.ReplaceAll(strings.ToLower(strings.SplitN(n, " ", 2)[0]), " ", "")
		if err := b1.Insert(ctx, &userV1{ID: id, Name: n}); err != nil {
			t.Fatal(err)
		}
	}
	db1.Close()

	// Reopen as v2 with a typed migration registered.
	db2, err := bw.Open(dir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	b2, err := bw.RegisterBucket[userV2](db2, "users",
		bw.WithVersion[userV2](2),
		bw.WithTypedMigration[userV1, userV2](1, 2, func(_ context.Context, old *userV1) (*userV2, error) {
			parts := strings.SplitN(old.Name, " ", 2)
			first, last := parts[0], ""
			if len(parts) == 2 {
				last = parts[1]
			}
			return &userV2{ID: old.ID, First: first, Last: last}, nil
		}),
	)
	if err != nil {
		t.Fatalf("register v2: %v", err)
	}

	// Verify every record is now in v2 shape.
	want := map[string]struct{ first, last string }{
		"ali":    {"Ali", "Veli"},
		"ayse":   {"Ayse", "Yilmaz"},
		"mehmet": {"Mehmet", "Demir"},
	}
	for id, w := range want {
		got, err := b2.Get(ctx, id)
		if err != nil {
			t.Fatalf("get %q: %v", id, err)
		}
		if got.First != w.first || got.Last != w.last {
			t.Fatalf("id=%q: got (%q, %q), want (%q, %q)",
				id, got.First, got.Last, w.first, w.last)
		}
	}

	// Re-opening with the same version should be a no-op (no
	// migration re-run, no errors).
	if _, err := bw.RegisterBucket[userV2](db2, "users",
		bw.WithVersion[userV2](2),
		bw.WithTypedMigration[userV1, userV2](1, 2, func(_ context.Context, _ *userV1) (*userV2, error) {
			t.Fatalf("migration should not re-run on stable version")
			return nil, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
}

// TestUserMigration_TypedMultiStep chains v1 -> v2 -> v3 in a single
// RegisterBucket call.
func TestUserMigration_TypedMultiStep(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Seed v1.
	db1, _ := bw.Open(dir, bw.WithLogger(nil))
	b1, _ := bw.RegisterBucket[userV1](db1, "users_chain", bw.WithVersion[userV1](1))
	_ = b1.Insert(ctx, &userV1{ID: "1", Name: "Ali Veli"})
	_ = b1.Insert(ctx, &userV1{ID: "2", Name: "Mehmet Demir"})
	db1.Close()

	// Open as v3 with both migration steps registered. Order of
	// registration shouldn't matter — the runner sorts by fromV.
	db2, _ := bw.Open(dir, bw.WithLogger(nil))
	defer db2.Close()
	b3, err := bw.RegisterBucket[userV3](db2, "users_chain",
		bw.WithVersion[userV3](3),
		bw.WithTypedMigration[userV2, userV3](2, 3, func(_ context.Context, old *userV2) (*userV3, error) {
			email := fmt.Sprintf("%s.%s@example.com",
				strings.ToLower(old.First),
				strings.ToLower(old.Last))
			return &userV3{ID: old.ID, First: old.First, Last: old.Last, Email: email}, nil
		}),
		bw.WithTypedMigration[userV1, userV3](1, 2, func(_ context.Context, old *userV1) (*userV3, error) {
			// Transitional shape: fields populated, Email left
			// blank — the v2->v3 step fills it in.
			parts := strings.SplitN(old.Name, " ", 2)
			first, last := parts[0], ""
			if len(parts) == 2 {
				last = parts[1]
			}
			return &userV3{ID: old.ID, First: first, Last: last}, nil
		}),
	)
	if err != nil {
		t.Fatalf("register v3: %v", err)
	}

	got, err := b3.Get(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	if got.First != "Ali" || got.Last != "Veli" || got.Email != "ali.veli@example.com" {
		t.Fatalf("chained migration: %+v", got)
	}
}

// TestUserMigration_RawBytes uses the byte-level API. Demonstrates the
// codec.UnmarshalMap path for callers who don't want to maintain the
// old struct definition.
func TestUserMigration_RawBytes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db1, _ := bw.Open(dir, bw.WithLogger(nil))
	b1, _ := bw.RegisterBucket[userV1](db1, "users_raw", bw.WithVersion[userV1](1))
	_ = b1.Insert(ctx, &userV1{ID: "1", Name: "ALI VELI"})
	db1.Close()

	db2, _ := bw.Open(dir, bw.WithLogger(nil))
	defer db2.Close()
	c := codec.MsgPack()
	b2, err := bw.RegisterBucket[userV1](db2, "users_raw",
		bw.WithVersion[userV1](2),
		bw.WithRawMigration[userV1](1, 2, func(_ context.Context, raw []byte) ([]byte, error) {
			m, err := c.UnmarshalMap(raw)
			if err != nil {
				return nil, err
			}
			if name, ok := m["name"].(string); ok {
				m["name"] = strings.ToLower(name)
			}
			return c.Marshal(m)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := b2.Get(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "ali veli" {
		t.Fatalf("raw migration: name=%q", got.Name)
	}
}

// TestUserMigration_MissingStep ensures a chain with a gap fails
// loudly rather than silently skipping data.
func TestUserMigration_MissingStep(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db1, _ := bw.Open(dir, bw.WithLogger(nil))
	b1, _ := bw.RegisterBucket[userV1](db1, "users_gap", bw.WithVersion[userV1](1))
	_ = b1.Insert(ctx, &userV1{ID: "1", Name: "Ali"})
	db1.Close()

	db2, _ := bw.Open(dir, bw.WithLogger(nil))
	defer db2.Close()
	// Register v3 but only register the 2->3 step. Runner should
	// refuse with a clear error mentioning the missing 1->2.
	_, err := bw.RegisterBucket[userV3](db2, "users_gap",
		bw.WithVersion[userV3](3),
		bw.WithTypedMigration[userV2, userV3](2, 3, func(_ context.Context, old *userV2) (*userV3, error) {
			return &userV3{ID: old.ID, First: old.First, Last: old.Last}, nil
		}),
	)
	if err == nil {
		t.Fatal("expected error for missing migration step")
	}
	if !strings.Contains(err.Error(), "no migration registered") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestUserMigration_FailureRollsBackBatch verifies that an error from
// the user fn aborts the migration without losing data: the records
// in the failed batch revert to their pre-migration state and the
// schema version stays unchanged.
func TestUserMigration_FailureRollsBackBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db1, _ := bw.Open(dir, bw.WithLogger(nil))
	b1, _ := bw.RegisterBucket[userV1](db1, "users_fail", bw.WithVersion[userV1](1))
	for i := 0; i < 5; i++ {
		_ = b1.Insert(ctx, &userV1{ID: fmt.Sprintf("%d", i), Name: fmt.Sprintf("user-%d", i)})
	}
	db1.Close()

	sentinel := errors.New("boom")
	db2, _ := bw.Open(dir, bw.WithLogger(nil))
	defer db2.Close()
	_, err := bw.RegisterBucket[userV2](db2, "users_fail",
		bw.WithVersion[userV2](2),
		bw.WithTypedMigration[userV1, userV2](1, 2, func(_ context.Context, _ *userV1) (*userV2, error) {
			return nil, sentinel
		}),
	)
	if err == nil || !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}

	// Records should still decode under the v1 shape — the failed
	// migration didn't touch them.
	b1again, err := bw.RegisterBucket[userV1](db2, "users_fail", bw.WithVersion[userV1](1))
	if err != nil {
		t.Fatalf("re-register as v1: %v", err)
	}
	got, err := b1again.Get(ctx, "0")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "user-0" {
		t.Fatalf("rollback corrupted data: %+v", got)
	}
}

// TestUserMigration_Resumable simulates a crash mid-migration by
// writing a progress key by hand and verifying that the next run
// picks up where it left off without re-processing earlier records.
func TestUserMigration_Resumable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Seed enough records to span two batches.
	const N = 130 // > batch size of 64
	db1, _ := bw.Open(dir, bw.WithLogger(nil))
	b1, _ := bw.RegisterBucket[userV1](db1, "users_resume", bw.WithVersion[userV1](1))
	for i := 0; i < N; i++ {
		_ = b1.Insert(ctx, &userV1{ID: fmt.Sprintf("u%04d", i), Name: fmt.Sprintf("name-%d", i)})
	}
	db1.Close()

	// Run the migration with a callback that counts invocations and
	// fails after a set number — simulating a crash during the
	// migration.
	failAfter := 70
	count := 0
	dbCrash, _ := bw.Open(dir, bw.WithLogger(nil))
	_, err := bw.RegisterBucket[userV2](dbCrash, "users_resume",
		bw.WithVersion[userV2](2),
		bw.WithTypedMigration[userV1, userV2](1, 2, func(_ context.Context, old *userV1) (*userV2, error) {
			count++
			if count > failAfter {
				return nil, errors.New("simulated crash")
			}
			return &userV2{ID: old.ID, First: old.Name}, nil
		}),
	)
	if err == nil {
		dbCrash.Close()
		t.Fatal("expected crash error")
	}
	dbCrash.Close()

	// Resume on a fresh open. The runner should pick up where it
	// left off; we count remaining invocations and check it's
	// strictly less than N (i.e. the cursor really skipped what was
	// already done).
	dbOK, _ := bw.Open(dir, bw.WithLogger(nil))
	defer dbOK.Close()
	resumeCount := 0
	b2, err := bw.RegisterBucket[userV2](dbOK, "users_resume",
		bw.WithVersion[userV2](2),
		bw.WithTypedMigration[userV1, userV2](1, 2, func(_ context.Context, old *userV1) (*userV2, error) {
			resumeCount++
			return &userV2{ID: old.ID, First: old.Name}, nil
		}),
	)
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if resumeCount >= N {
		t.Fatalf("resume processed %d/%d, expected to skip earlier batch", resumeCount, N)
	}
	if resumeCount == 0 {
		t.Fatalf("resume processed nothing — cursor advanced too far?")
	}

	// Sanity: every record should now be in v2 shape.
	all, err := b2.Find(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != N {
		t.Fatalf("post-resume count = %d, want %d", len(all), N)
	}
	for _, r := range all {
		if r.First == "" {
			t.Fatalf("unmigrated record: %+v", r)
		}
	}
}

// TestUserMigration_VectorReembed exercises the WithVectorReembed
// shorthand: only the vector field changes between schema versions.
func TestUserMigration_VectorReembed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	type DocV1 struct {
		ID    string    `bw:"id,pk"`
		Title string    `bw:"title"`
		Embed []float32 `bw:"embed,vector(dim=4,metric=cosine)"`
	}

	// Seed v1 with arbitrary vectors.
	db1, _ := bw.Open(dir, bw.WithLogger(nil))
	b1, _ := bw.RegisterBucket[DocV1](db1, "docs_re", bw.WithVersion[DocV1](1))
	for i, t := range []string{"alpha", "beta", "gamma"} {
		_ = b1.Insert(ctx, &DocV1{
			ID:    fmt.Sprintf("%d", i),
			Title: t,
			Embed: []float32{1, 0, 0, 0}, // all identical, useless
		})
	}
	db1.Close()

	// Reopen at v2 with a reembed step that derives the vector
	// from the title length — distinguishing each record.
	db2, _ := bw.Open(dir, bw.WithLogger(nil))
	defer db2.Close()
	b2, err := bw.RegisterBucket[DocV1](db2, "docs_re",
		bw.WithVersion[DocV1](2),
		bw.WithVectorReembed[DocV1](1, 2, func(_ context.Context, d *DocV1) ([]float32, error) {
			n := float32(len(d.Title))
			return []float32{n, 0, 0, 0}, nil
		}),
	)
	if err != nil {
		t.Fatalf("reembed: %v", err)
	}

	// Verify each record now has the expected derived vector.
	for _, c := range []struct {
		id   string
		want float32
	}{{"0", 5}, {"1", 4}, {"2", 5}} {
		d, err := b2.Get(ctx, c.id)
		if err != nil {
			t.Fatal(err)
		}
		if d.Embed[0] != c.want {
			t.Fatalf("id=%q embed[0]=%v want %v", c.id, d.Embed[0], c.want)
		}
	}
}

// TestUserMigration_Progress exercises the optional progress hook.
func TestUserMigration_Progress(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	const N = 10
	db1, _ := bw.Open(dir, bw.WithLogger(nil))
	b1, _ := bw.RegisterBucket[userV1](db1, "users_prog", bw.WithVersion[userV1](1))
	for i := 0; i < N; i++ {
		_ = b1.Insert(ctx, &userV1{ID: fmt.Sprintf("u%d", i), Name: "x"})
	}
	db1.Close()

	var calls int
	var sawTotal uint64
	var lastProcessed uint64
	db2, _ := bw.Open(dir, bw.WithLogger(nil))
	defer db2.Close()
	if _, err := bw.RegisterBucket[userV2](db2, "users_prog",
		bw.WithVersion[userV2](2),
		bw.WithTypedMigration[userV1, userV2](1, 2, func(_ context.Context, old *userV1) (*userV2, error) {
			return &userV2{ID: old.ID, First: old.Name}, nil
		}),
		bw.WithMigrationProgress[userV2](func(bucket string, fromV, toV uint64, processed, total uint64) {
			calls++
			sawTotal = total
			lastProcessed = processed
			if bucket != "users_prog" || fromV != 1 || toV != 2 {
				t.Errorf("progress hook bad args: bucket=%q fromV=%d toV=%d", bucket, fromV, toV)
			}
		}),
	); err != nil {
		t.Fatal(err)
	}
	if calls == 0 {
		t.Fatal("progress hook never called")
	}
	if sawTotal != N {
		t.Fatalf("total = %d, want %d", sawTotal, N)
	}
	if lastProcessed != N {
		t.Fatalf("last processed = %d, want %d", lastProcessed, N)
	}
}
