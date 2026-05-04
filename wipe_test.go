package bw_test

import (
	"context"
	"testing"
)

// TestWipeRemovesAllData is the basic happy path: every record in
// every bucket is gone after Wipe, but the Bucket handle remains
// usable for fresh writes.
func TestWipeRemovesAllData(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	for _, name := range []string{"Alice", "Bob", "Carol"} {
		if err := users.Insert(ctx, &User{ID: name, Name: name, Email: name + "@x"}); err != nil {
			t.Fatalf("Insert %s: %v", name, err)
		}
	}

	pre, err := users.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count pre-wipe: %v", err)
	}
	if pre != 3 {
		t.Fatalf("pre-wipe count = %d, want 3", pre)
	}

	if err := db.Wipe(); err != nil {
		t.Fatalf("Wipe: %v", err)
	}

	post, err := users.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count post-wipe: %v", err)
	}
	if post != 0 {
		t.Fatalf("post-wipe count = %d, want 0", post)
	}

	// The cached Bucket handle must keep working after Wipe — its
	// schema/codec/encoder fields are derived from the Go type and
	// don't depend on on-disk meta.
	if err := users.Insert(ctx, &User{ID: "post", Name: "Post", Email: "p@x"}); err != nil {
		t.Fatalf("Insert post-wipe: %v", err)
	}
	got, err := users.Get(ctx, "post")
	if err != nil {
		t.Fatalf("Get post-wipe insert: %v", err)
	}
	if got.Name != "Post" {
		t.Fatalf("Get post-wipe: want Post, got %q", got.Name)
	}
}

// TestWipeClearsUniqueReservations is the tricky case: bw maintains
// "<bucket>\x00uniq\x00<field>" reservation keys that block duplicate
// inserts. A Wipe must clear them too, otherwise post-Wipe inserts
// of records that had unique-marked fields in the pre-Wipe data
// would falsely fail with ErrConflict.
func TestWipeClearsUniqueReservations(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "shared@x"}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	if err := db.Wipe(); err != nil {
		t.Fatalf("Wipe: %v", err)
	}

	// Re-using the same email on a different ID would conflict if the
	// unique index from before the wipe still existed.
	if err := users.Insert(ctx, &User{ID: "u2", Name: "Alice2", Email: "shared@x"}); err != nil {
		t.Fatalf("Insert post-wipe with previously-unique email: %v", err)
	}
}

// TestWipeOnEmptyDB is a safety check: Wipe on a fresh DB must be a
// clean no-op rather than an error.
func TestWipeOnEmptyDB(t *testing.T) {
	db := openTestDB(t)
	_ = newUsers(t, db) // register the bucket but insert nothing

	if err := db.Wipe(); err != nil {
		t.Fatalf("Wipe on empty DB: %v", err)
	}
}
