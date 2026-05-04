package bw_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/rakunlabs/bw"
)

func TestBackupAndRestore(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	// Insert some data.
	seed := []*User{
		{ID: "u1", Name: "Ayse", Email: "ayse@x", Age: 30},
		{ID: "u2", Name: "Mehmet", Email: "mehmet@x", Age: 25},
		{ID: "u3", Name: "Fatma", Email: "fatma@x", Age: 40},
	}
	for _, u := range seed {
		if err := users.Insert(ctx, u); err != nil {
			t.Fatalf("insert %s: %v", u.ID, err)
		}
	}

	// Take a backup.
	var buf bytes.Buffer
	_, err := db.Backup(&buf, 0, false)
	if err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Delete all records.
	for _, u := range seed {
		if err := users.Delete(ctx, u.ID); err != nil {
			t.Fatalf("delete %s: %v", u.ID, err)
		}
	}

	// Verify they are gone.
	for _, u := range seed {
		if _, err := users.Get(ctx, u.ID); err == nil {
			t.Fatalf("expected %s to be deleted", u.ID)
		}
	}

	// Restore into a fresh DB (simulates recovering from a backup).
	db2, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	if err := db2.Restore(&buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	users2, err := bw.RegisterBucket[User](db2, "users")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Verify all records are back.
	for _, want := range seed {
		got, err := users2.Get(ctx, want.ID)
		if err != nil {
			t.Fatalf("get %s after restore: %v", want.ID, err)
		}
		if got.Name != want.Name || got.Email != want.Email || got.Age != want.Age {
			t.Fatalf("mismatch for %s: got %+v, want %+v", want.ID, got, want)
		}
	}
}

func TestBackupWithDeletedData(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	// Insert and then delete a record so delete markers exist.
	if err := users.Insert(ctx, &User{ID: "u1", Name: "Kemal", Email: "kemal@x", Age: 30}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := users.Delete(ctx, "u1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Insert another record that stays alive.
	if err := users.Insert(ctx, &User{ID: "u2", Name: "Zeynep", Email: "zeynep@x", Age: 25}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Backup with deleted data included.
	var buf bytes.Buffer
	_, err := db.Backup(&buf, 0, true)
	if err != nil {
		t.Fatalf("backup with deleted data: %v", err)
	}

	if buf.Len() == 0 {
		t.Fatal("backup produced no data")
	}

	// Open a fresh DB, restore, and verify the live record is present.
	db2, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	if err := db2.Restore(&buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	users2, err := bw.RegisterBucket[User](db2, "users")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	got, err := users2.Get(ctx, "u2")
	if err != nil {
		t.Fatalf("get u2 after restore: %v", err)
	}
	if got.Name != "Zeynep" {
		t.Fatalf("got name %q, want Zeynep", got.Name)
	}
}

func TestBackupUntil(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	// Insert records.
	if err := users.Insert(ctx, &User{ID: "u1", Name: "Elif", Email: "elif@x", Age: 30}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := users.Insert(ctx, &User{ID: "u2", Name: "Hakan", Email: "hakan@x", Age: 25}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Save the current version — this is our restore point.
	savedVersion := db.Version()

	// Now delete a record; this creates a newer version.
	if err := users.Delete(ctx, "u1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify u1 is gone.
	if _, err := users.Get(ctx, "u1"); err == nil {
		t.Fatal("expected u1 to be deleted")
	}

	// BackupUntil the saved version — should recover the state before the delete.
	var buf bytes.Buffer
	_, err := db.BackupUntil(&buf, savedVersion)
	if err != nil {
		t.Fatalf("backup until: %v", err)
	}

	// Restore into a fresh DB.
	db2, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	if err := db2.Restore(&buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	users2, err := bw.RegisterBucket[User](db2, "users")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// u1 should be back — it was alive at savedVersion.
	got, err := users2.Get(ctx, "u1")
	if err != nil {
		t.Fatalf("get u1 after restore: %v", err)
	}
	if got.Name != "Elif" {
		t.Fatalf("got name %q, want Elif", got.Name)
	}

	// u2 should also exist.
	got, err = users2.Get(ctx, "u2")
	if err != nil {
		t.Fatalf("get u2 after restore: %v", err)
	}
	if got.Name != "Hakan" {
		t.Fatalf("got name %q, want Hakan", got.Name)
	}
}

func TestVersion(t *testing.T) {
	db := openTestDB(t)
	users := newUsers(t, db)
	ctx := context.Background()

	v0 := db.Version()

	if err := users.Insert(ctx, &User{ID: "u1", Name: "Emre", Email: "emre@x", Age: 30}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	v1 := db.Version()
	if v1 <= v0 {
		t.Fatalf("version did not advance: v0=%d v1=%d", v0, v1)
	}
}

func TestIncrementalBackup(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	// Insert first record and take a full backup.
	if err := users.Insert(ctx, &User{ID: "u1", Name: "Deniz", Email: "deniz@x", Age: 30}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var fullBuf bytes.Buffer
	since, err := db.Backup(&fullBuf, 0, false)
	if err != nil {
		t.Fatalf("full backup: %v", err)
	}

	// Insert a second record and take an incremental backup.
	if err := users.Insert(ctx, &User{ID: "u2", Name: "Burak", Email: "burak@x", Age: 25}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var incrBuf bytes.Buffer
	_, err = db.Backup(&incrBuf, since, false)
	if err != nil {
		t.Fatalf("incremental backup: %v", err)
	}

	// The incremental backup should be smaller than a new full backup
	// (it only contains u2 + meta, not u1).
	var fullBuf2 bytes.Buffer
	_, err = db.Backup(&fullBuf2, 0, false)
	if err != nil {
		t.Fatalf("second full backup: %v", err)
	}

	if incrBuf.Len() >= fullBuf2.Len() {
		t.Fatalf("incremental (%d bytes) should be smaller than full (%d bytes)",
			incrBuf.Len(), fullBuf2.Len())
	}

	// Restore both into a fresh DB — full first, then incremental.
	db2, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	if err := db2.Restore(&fullBuf); err != nil {
		t.Fatalf("restore full: %v", err)
	}
	if err := db2.Restore(&incrBuf); err != nil {
		t.Fatalf("restore incremental: %v", err)
	}

	users2, err := bw.RegisterBucket[User](db2, "users")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Both records should be present.
	for _, id := range []string{"u1", "u2"} {
		if _, err := users2.Get(ctx, id); err != nil {
			t.Fatalf("get %s after incremental restore: %v", id, err)
		}
	}
}
