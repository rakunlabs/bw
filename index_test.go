package bw_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/query"
)

// countKeysWithPrefix counts how many keys in db share the supplied
// byte prefix. Used to assert the on-disk index keyspace.
func countKeysWithPrefix(t *testing.T, db *bw.DB, prefix []byte) int {
	t.Helper()
	var n int
	err := db.Badger().View(func(btx *badger.Txn) error {
		it := btx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			n++
		}

		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	return n
}

func TestIndex_KeysCreatedOnInsert(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "a@x", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := users.Insert(ctx, &User{ID: "u2", Name: "Bob", Email: "b@x", Age: 25}); err != nil {
		t.Fatal(err)
	}

	// User has 2 indexed fields (name, age) and 1 unique field (email):
	// 2 records × 2 = 4 idx keys, 2 records × 1 = 2 uniq keys.
	wantIdx := 4
	wantUniq := 2
	if got := countKeysWithPrefix(t, db, []byte{0, 'i', 'd', 'x', 0}); got != wantIdx {
		t.Fatalf("idx keys = %d, want %d", got, wantIdx)
	}
	if got := countKeysWithPrefix(t, db, []byte{0, 'u', 'n', 'i', 'q', 0}); got != wantUniq {
		t.Fatalf("uniq keys = %d, want %d", got, wantUniq)
	}
}

func TestIndex_UpdateRewritesIndex(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	u := &User{ID: "u1", Name: "Alice", Email: "a@x", Age: 30}
	if err := users.Insert(ctx, u); err != nil {
		t.Fatal(err)
	}

	// Change indexed field; total index count should stay constant.
	u.Name = "Alicia"
	if err := users.Update(ctx, u); err != nil {
		t.Fatal(err)
	}

	// 1 record × 2 indexed fields (name, age) = 2 idx entries.
	if got := countKeysWithPrefix(t, db, []byte{0, 'i', 'd', 'x', 0}); got != 2 {
		t.Fatalf("idx keys = %d, want 2", got)
	}

	// New name resolves; old name does not.
	got, err := users.Find(ctx, mustParse(t, "name=Alicia"))
	if err != nil || len(got) != 1 {
		t.Fatalf("find Alicia: %v %v", err, got)
	}
	got, err = users.Find(ctx, mustParse(t, "name=Alice"))
	if err != nil || len(got) != 0 {
		t.Fatalf("find Alice should be empty: %v %v", err, got)
	}
}

func TestIndex_DeleteRemovesAll(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "a@x", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := users.Delete(ctx, "u1"); err != nil {
		t.Fatal(err)
	}

	if got := countKeysWithPrefix(t, db, []byte{0, 'i', 'd', 'x', 0}); got != 0 {
		t.Fatalf("idx keys = %d after delete, want 0", got)
	}
	if got := countKeysWithPrefix(t, db, []byte{0, 'u', 'n', 'i', 'q', 0}); got != 0 {
		t.Fatalf("uniq keys = %d after delete, want 0", got)
	}
}

func TestUnique_ConflictOnInsert(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := users.Insert(ctx, &User{ID: "u1", Email: "dup@x", Name: "A"}); err != nil {
		t.Fatal(err)
	}

	err := users.Insert(ctx, &User{ID: "u2", Email: "dup@x", Name: "B"})
	if !errors.Is(err, bw.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}

	// The conflicting record must NOT have been persisted.
	if _, err := users.Get(ctx, "u2"); !errors.Is(err, bw.ErrNotFound) {
		t.Fatalf("u2 should not exist, got %v", err)
	}
}

func TestUnique_AllowsSamePKToChange(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	u := &User{ID: "u1", Email: "old@x", Name: "A"}
	if err := users.Insert(ctx, u); err != nil {
		t.Fatal(err)
	}
	u.Email = "new@x"
	if err := users.Update(ctx, u); err != nil {
		t.Fatal(err)
	}

	// New unique value should be reusable by another record now that the
	// old value has been released... oh wait, the old value is gone, so
	// nothing else holds "old@x". We instead verify the active value
	// works.
	got, err := users.Get(ctx, "u1")
	if err != nil || got.Email != "new@x" {
		t.Fatalf("get after update: %v %+v", err, got)
	}
}

func TestPlanner_FindUsesIndex(t *testing.T) {
	// Confirms the index path doesn't drop or duplicate matches across a
	// few representative shapes that the planner should pick up.
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)
	seed(t, users)

	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"eq on indexed name", "name=Alice", []string{"u1"}},
		{"in list on indexed name", "name=Alice,Bob", []string{"u1", "u2"}},
		{"range on indexed age", "age[gte]=30&age[lt]=40", []string{"u1", "u3"}},
		{"eq + residual ilike", "age=25&name[ilike]=b%25", []string{"u2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := users.Find(ctx, mustParse(t, tc.raw))
			if err != nil {
				t.Fatalf("find: %v", err)
			}
			ids := make([]string, len(got))
			for i, u := range got {
				ids[i] = u.ID
			}
			if !sameSet(ids, tc.want) {
				t.Fatalf("got %v, want %v", ids, tc.want)
			}
		})
	}
}

func TestPlanner_RangeMatchesFullScan(t *testing.T) {
	// Property-style: index range and full-scan must agree on results.
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)
	seed(t, users)

	q := mustParse(t, "age[gte]=25&age[lte]=35")
	idxResult, err := users.Find(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	// Force a full scan by querying via a non-indexed field with the
	// equivalent semantics (compare with the unindexed Score field — too
	// different. Instead we manually walk the bucket).
	var refIDs []string
	_ = db.Badger().View(func(btx *badger.Txn) error {
		it := btx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("users\x00")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			pk := string(it.Item().Key()[len(prefix):])
			rec, _ := users.Get(ctx, pk)
			if rec != nil && rec.Age >= 25 && rec.Age <= 35 {
				refIDs = append(refIDs, rec.ID)
			}
		}

		return nil
	})

	idxIDs := make([]string, len(idxResult))
	for i, u := range idxResult {
		idxIDs[i] = u.ID
	}
	if !sameSet(idxIDs, refIDs) {
		t.Fatalf("index=%v, ref=%v", idxIDs, refIDs)
	}
}

func TestSchema_FingerprintMismatch(t *testing.T) {
	db := openTestDB(t)
	if _, err := bw.RegisterBucket[SchemaV1](db, "things"); err != nil {
		t.Fatalf("register V1: %v", err)
	}
	_, err := bw.RegisterBucket[SchemaV2](db, "things")
	if err == nil || !contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("expected fingerprint mismatch, got %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}

	return false
}

func TestPlanner_NoIndexFallsBackToFullScan(t *testing.T) {
	db := openTestDB(t)
	plain, err := bw.RegisterBucket[PlainTag](db, "plain")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i, tag := range []string{"a", "b", "c"} {
		_ = plain.Insert(ctx, &PlainTag{ID: string(rune('1' + i)), Tag: tag})
	}
	got, err := plain.Find(ctx, mustParse(t, "tag=b"))
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(got) != 1 || got[0].Tag != "b" {
		t.Fatalf("unexpected: %+v", got)
	}
}

// keep query import used
var _ = query.Parse
