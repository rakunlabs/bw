package bw_test

import (
	"context"
	"testing"

	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/query"
)

// UserV1 is the original schema.
type UserV1 struct {
	ID   string `bw:"id,pk"`
	Name string `bw:"name,index"`
}

// UserV2 adds an Email field with a unique constraint and an Age index.
type UserV2 struct {
	ID    string `bw:"id,pk"`
	Name  string `bw:"name,index"`
	Email string `bw:"email,unique"`
	Age   int    `bw:"age,index"`
}

// UserV3 removes the Name index and keeps only Email unique + Age index.
type UserV3 struct {
	ID    string `bw:"id,pk"`
	Name  string `bw:"name"`
	Email string `bw:"email,unique"`
	Age   int    `bw:"age,index"`
}

func TestMigrateBucket_AddFields(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Register with V1 and insert some data.
	v1, err := bw.RegisterBucket[UserV1](db, "migrate_users", bw.WithVersion[UserV1](1))
	if err != nil {
		t.Fatal(err)
	}

	_ = v1.Insert(ctx, &UserV1{ID: "1", Name: "Alice"})
	_ = v1.Insert(ctx, &UserV1{ID: "2", Name: "Bob"})

	// Without WithVersion, fingerprint mismatch still errors.
	_, err = bw.RegisterBucket[UserV2](db, "migrate_users")
	if err == nil {
		t.Fatal("expected fingerprint mismatch error without WithVersion")
	}

	// With WithVersion(2) > stored(1), auto-migrates.
	v2, err := bw.RegisterBucket[UserV2](db, "migrate_users", bw.WithVersion[UserV2](2))
	if err != nil {
		t.Fatalf("RegisterBucket with WithVersion(2): %v", err)
	}

	// Old data is readable with new struct (new fields default to zero).
	u, err := v2.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if u.Name != "Alice" || u.Email != "" || u.Age != 0 {
		t.Fatalf("unexpected record: %+v", u)
	}

	// Insert a new record with all fields populated.
	_ = v2.Insert(ctx, &UserV2{ID: "3", Name: "Charlie", Email: "c@x.com", Age: 35})

	// The Name index still works (unchanged field, not rebuilt).
	q, _ := query.Parse("name=Bob")
	got, err := v2.Find(ctx, q)
	if err != nil {
		t.Fatalf("Find by name: %v", err)
	}
	if len(got) != 1 || got[0].ID != "2" {
		t.Fatalf("unexpected name query result: %+v", got)
	}

	// The new Age index works (newly built).
	q, _ = query.Parse("age=35")
	got, err = v2.Find(ctx, q)
	if err != nil {
		t.Fatalf("Find by age: %v", err)
	}
	if len(got) != 1 || got[0].ID != "3" {
		t.Fatalf("unexpected age query result: %+v", got)
	}

	// Unique constraint works on new field.
	err = v2.Insert(ctx, &UserV2{ID: "4", Name: "Dup", Email: "c@x.com", Age: 20})
	if err == nil {
		t.Fatal("expected unique conflict on email")
	}

	// Subsequent RegisterBucket with same version succeeds (no migration).
	_, err = bw.RegisterBucket[UserV2](db, "migrate_users", bw.WithVersion[UserV2](2))
	if err != nil {
		t.Fatalf("RegisterBucket after migration should succeed: %v", err)
	}
}

func TestMigrateBucket_RemoveIndex(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Start with V2 at version 1.
	v2, err := bw.RegisterBucket[UserV2](db, "migrate_remove", bw.WithVersion[UserV2](1))
	if err != nil {
		t.Fatal(err)
	}
	_ = v2.Insert(ctx, &UserV2{ID: "1", Name: "Alice", Email: "a@x", Age: 30})

	// Migrate to V3 (Name loses its index) by bumping version.
	v3, err := bw.RegisterBucket[UserV3](db, "migrate_remove", bw.WithVersion[UserV3](2))
	if err != nil {
		t.Fatalf("RegisterBucket V3 with version 2: %v", err)
	}

	// Data still accessible.
	u, err := v3.Get(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	if u.Name != "Alice" {
		t.Fatalf("unexpected: %+v", u)
	}

	// Age index still works.
	q, _ := query.Parse("age=30")
	got, err := v3.Find(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "1" {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestMigrateBucket_ExplicitMigrate(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Register without version.
	v1, err := bw.RegisterBucket[UserV1](db, "migrate_explicit")
	if err != nil {
		t.Fatal(err)
	}
	_ = v1.Insert(ctx, &UserV1{ID: "1", Name: "Alice"})

	// Explicit MigrateBucket still works (for users who don't use WithVersion).
	v2, err := bw.MigrateBucket[UserV2](db, "migrate_explicit")
	if err != nil {
		t.Fatalf("MigrateBucket: %v", err)
	}

	u, err := v2.Get(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	if u.Name != "Alice" {
		t.Fatalf("unexpected: %+v", u)
	}
}

func TestMigrateBucket_VersionNotBumped_Fails(t *testing.T) {
	db := openTestDB(t)

	// Register V1 at version 1.
	_, err := bw.RegisterBucket[UserV1](db, "migrate_nobump", bw.WithVersion[UserV1](1))
	if err != nil {
		t.Fatal(err)
	}

	// Try V2 with same version — should fail.
	_, err = bw.RegisterBucket[UserV2](db, "migrate_nobump", bw.WithVersion[UserV2](1))
	if err == nil {
		t.Fatal("expected error when version not bumped")
	}
}

// TestRegisterBucket_NoVersionBumpOnRestart asserts that re-opening a
// database and re-registering an unchanged bucket does NOT advance the
// Badger MaxVersion. Earlier versions of ensureSchemaOrMigrate
// unconditionally Set the fingerprint/version/manifest keys on every
// call, which silently bumped MaxVersion on each process start even
// when the schema was identical.
func TestRegisterBucket_NoVersionBumpOnRestart(t *testing.T) {
	dir := t.TempDir()

	// First open: creates the bucket and writes metadata.
	db1, err := bw.Open(dir, bw.WithLogger(nil))
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if _, err := bw.RegisterBucket[UserV1](db1, "noop", bw.WithVersion[UserV1](1)); err != nil {
		t.Fatalf("register 1: %v", err)
	}
	v1 := db1.Version()
	if err := db1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Re-open and re-register the same schema several times. None of
	// these should advance MaxVersion.
	for i := 0; i < 3; i++ {
		db2, err := bw.Open(dir, bw.WithLogger(nil))
		if err != nil {
			t.Fatalf("open %d: %v", i+2, err)
		}
		if _, err := bw.RegisterBucket[UserV1](db2, "noop", bw.WithVersion[UserV1](1)); err != nil {
			t.Fatalf("register %d: %v", i+2, err)
		}
		v := db2.Version()
		if v != v1 {
			t.Fatalf("re-open %d: version advanced from %d to %d without any change", i+2, v1, v)
		}
		if err := db2.Close(); err != nil {
			t.Fatalf("close %d: %v", i+2, err)
		}
	}
}

// TestMigrateBucket_NoOpWhenUnchanged asserts that explicit MigrateBucket
// calls also short-circuit when nothing has actually changed.
func TestMigrateBucket_NoOpWhenUnchanged(t *testing.T) {
	dir := t.TempDir()

	db1, err := bw.Open(dir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bw.RegisterBucket[UserV1](db1, "noop_mig"); err != nil {
		t.Fatal(err)
	}
	v1 := db1.Version()
	_ = db1.Close()

	db2, err := bw.Open(dir, bw.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, err := bw.MigrateBucket[UserV1](db2, "noop_mig"); err != nil {
		t.Fatalf("MigrateBucket: %v", err)
	}
	if v := db2.Version(); v != v1 {
		t.Fatalf("MigrateBucket bumped version from %d to %d without any change", v1, v)
	}
}
