package bw_test

import (
	"context"
	"errors"
	"testing"

	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/query"
)

// User and other test record types live in testtypes_test.go so the msgp
// code generator can produce Marshal/Unmarshal methods for them.

func openTestDB(t *testing.T) *bw.DB {
	t.Helper()
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func newUsers(t *testing.T, db *bw.DB) *bw.Bucket[User] {
	t.Helper()
	users, err := bw.RegisterBucket[User](db, "users")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	return users
}

func TestOpenInMemoryAndClose(t *testing.T) {
	db := openTestDB(t)
	if db.Codec() == nil {
		t.Fatalf("codec must not be nil")
	}
	if db.Codec().Name() != "msgpack" {
		t.Fatalf("default codec = %s, want msgpack", db.Codec().Name())
	}
}

func TestBucketCRUD(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	// Insert
	u := &User{ID: "u1", Name: "Alice", Email: "a@x", Age: 30, Bio: "ignored"}
	if err := users.Insert(ctx, u); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Get
	got, err := users.Get(ctx, "u1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "Alice" || got.Age != 30 {
		t.Fatalf("got = %+v", got)
	}
	if got.Bio != "" {
		t.Fatalf("bio should be omitted, got %q", got.Bio)
	}

	// Update (overwrite)
	u.Age = 31
	if err := users.Update(ctx, u); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err = users.Get(ctx, "u1")
	if err != nil {
		t.Fatalf("get post-update: %v", err)
	}
	if got.Age != 31 {
		t.Fatalf("age not updated: %d", got.Age)
	}

	// Delete
	if err := users.Delete(ctx, "u1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := users.Get(ctx, "u1"); !errors.Is(err, bw.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestInsertNew(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := users.InsertNew(ctx, &User{ID: "u1", Name: "Alice"}); err != nil {
		t.Fatal(err)
	}
	err := users.InsertNew(ctx, &User{ID: "u1", Name: "Alice2"})
	if !errors.Is(err, bw.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func seed(t *testing.T, users *bw.Bucket[User]) {
	t.Helper()
	ctx := context.Background()
	people := []User{
		{ID: "u1", Name: "Alice", Email: "a@x", Age: 30},
		{ID: "u2", Name: "Bob", Email: "b@x", Age: 25},
		{ID: "u3", Name: "Carol", Email: "c@x", Age: 35},
		{ID: "u4", Name: "Dave", Email: "d@x", Age: 25},
		{ID: "u5", Name: "alice", Email: "e@x", Age: 40},
	}
	for i := range people {
		if err := users.Insert(ctx, &people[i]); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

func mustParse(t *testing.T, raw string, opts ...query.OptionQuery) *query.Query {
	t.Helper()
	q, err := query.Parse(raw, opts...)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}

	return q
}

func TestFindOperators(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)
	seed(t, users)

	cases := []struct {
		name     string
		query    string
		opts     []query.OptionQuery
		wantPKs  []string
		anyOrder bool
	}{
		{
			name:     "eq numeric",
			query:    "age=25",
			opts:     []query.OptionQuery{query.WithKeyType("age", query.ValueTypeNumber)},
			wantPKs:  []string{"u2", "u4"},
			anyOrder: true,
		},
		{
			name:     "ne numeric",
			query:    "age[ne]=25",
			opts:     []query.OptionQuery{query.WithKeyType("age", query.ValueTypeNumber)},
			wantPKs:  []string{"u1", "u3", "u5"},
			anyOrder: true,
		},
		{
			name:     "gt numeric",
			query:    "age[gt]=30",
			wantPKs:  []string{"u3", "u5"},
			anyOrder: true,
		},
		{
			name:     "gte numeric",
			query:    "age[gte]=30",
			wantPKs:  []string{"u1", "u3", "u5"},
			anyOrder: true,
		},
		{
			name:     "lt numeric",
			query:    "age[lt]=30",
			wantPKs:  []string{"u2", "u4"},
			anyOrder: true,
		},
		{
			name:     "lte numeric",
			query:    "age[lte]=30",
			wantPKs:  []string{"u1", "u2", "u4"},
			anyOrder: true,
		},
		{
			name:     "in implicit comma",
			query:    "name=Alice,Bob",
			wantPKs:  []string{"u1", "u2"},
			anyOrder: true,
		},
		{
			name:     "nin",
			query:    "name[nin]=Alice,Bob",
			wantPKs:  []string{"u3", "u4", "u5"},
			anyOrder: true,
		},
		{
			name:     "like",
			query:    "name[like]=A%25",
			wantPKs:  []string{"u1"},
			anyOrder: true,
		},
		{
			name:     "ilike case insensitive",
			query:    "name[ilike]=a%25",
			wantPKs:  []string{"u1", "u5"},
			anyOrder: true,
		},
		{
			name:     "nlike",
			query:    "name[nlike]=A%25",
			wantPKs:  []string{"u2", "u3", "u4", "u5"},
			anyOrder: true,
		},
		{
			name:     "nilike",
			query:    "name[nilike]=a%25",
			wantPKs:  []string{"u2", "u3", "u4"},
			anyOrder: true,
		},
		{
			name:     "or across fields",
			query:    "name=Alice|age=25",
			opts:     []query.OptionQuery{query.WithKeyType("age", query.ValueTypeNumber)},
			wantPKs:  []string{"u1", "u2", "u4"},
			anyOrder: true,
		},
		{
			name:     "and combined",
			query:    "name=Bob&age=25",
			opts:     []query.OptionQuery{query.WithKeyType("age", query.ValueTypeNumber)},
			wantPKs:  []string{"u2"},
			anyOrder: true,
		},
		{
			name:    "is null",
			query:   "missing[is]=",
			wantPKs: []string{"u1", "u2", "u3", "u4", "u5"}, anyOrder: true,
		},
		{
			name:    "is not null",
			query:   "name[not]=",
			wantPKs: []string{"u1", "u2", "u3", "u4", "u5"}, anyOrder: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := mustParse(t, tc.query, tc.opts...)
			got, err := users.Find(ctx, q)
			if err != nil {
				t.Fatalf("find: %v", err)
			}
			gotIDs := make([]string, len(got))
			for i, u := range got {
				gotIDs[i] = u.ID
			}
			if !sameSet(gotIDs, tc.wantPKs) {
				t.Fatalf("got %v, want %v", gotIDs, tc.wantPKs)
			}
		})
	}
}

func TestFindSortLimitOffset(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)
	seed(t, users)

	q := mustParse(t, "_sort=-age&_limit=2&_offset=1")
	got, err := users.Find(ctx, q)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2", len(got))
	}
	// Sorted desc by age: u5(40), u3(35), u1(30), u2(25), u4(25)
	// With offset=1 we expect: u3, u1
	if got[0].ID != "u3" || got[1].ID != "u1" {
		t.Fatalf("unexpected order: %v %v", got[0].ID, got[1].ID)
	}
}

func TestSortMultiKey(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)
	seed(t, users)

	// Sort by age ASC, then name ASC. Among u2(Bob,25) and u4(Dave,25) we
	// expect u2 first.
	q := mustParse(t, "_sort=age,name")
	got, err := users.Find(ctx, q)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	want := []string{"u2", "u4", "u1", "u3", "u5"}
	for i, u := range got {
		if u.ID != want[i] {
			t.Fatalf("pos %d: %s, want %s (full=%v)", i, u.ID, want[i], idsOf(got))
		}
	}
}

func TestWalk(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)
	seed(t, users)

	var seen []string
	q := mustParse(t, "age[gte]=30")
	if err := users.Walk(ctx, q, func(u *User) error {
		seen = append(seen, u.ID)

		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if !sameSet(seen, []string{"u1", "u3", "u5"}) {
		t.Fatalf("seen=%v", seen)
	}
}

func TestCount(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)
	seed(t, users)

	n, err := users.Count(ctx, mustParse(t, "age[gte]=30"))
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Fatalf("count=%d, want 3", n)
	}

	// Count ignores limit/offset.
	n, err = users.Count(ctx, mustParse(t, "_limit=1"))
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 5 {
		t.Fatalf("count=%d, want 5", n)
	}
}

func TestNoPKBucket(t *testing.T) {
	db := openTestDB(t)
	_, err := bw.RegisterBucket[Thing](db, "things")
	if err == nil {
		t.Fatalf("expected error for missing pk")
	}

	// With WithKeyFn it works.
	things, err := bw.RegisterBucket[Thing](db, "things", bw.WithKeyFn(func(t *Thing) ([]byte, error) {
		return []byte(t.Name), nil
	}))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := things.Insert(context.Background(), &Thing{Name: "x"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := things.Get(context.Background(), "x")
	if err != nil || got.Name != "x" {
		t.Fatalf("get: %v %+v", err, got)
	}
}

func TestNestedDotPath(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	people, err := bw.RegisterBucket[NestedPerson](db, "people")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	for _, p := range []NestedPerson{
		{ID: "1", Address: PersonAddress{City: "Paris"}},
		{ID: "2", Address: PersonAddress{City: "Berlin"}},
	} {
		p := p
		if err := people.Insert(ctx, &p); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	q := mustParse(t, "address.city=Paris")
	got, err := people.Find(ctx, q)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(got) != 1 || got[0].ID != "1" {
		t.Fatalf("got %v", idsOfPersons(got))
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
		if m[s] < 0 {
			return false
		}
	}

	return true
}

func idsOf(us []*User) []string {
	out := make([]string, len(us))
	for i, u := range us {
		out[i] = u.ID
	}

	return out
}

// idsOfPersons returns a debug-friendly slice of pointers; only used in
// failure messages.
func idsOfPersons[T any](xs []*T) []*T { return xs }

func TestInsertMany(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	batch := []*User{
		{ID: "u1", Name: "Alice", Email: "a@x", Age: 30},
		{ID: "u2", Name: "Bob", Email: "b@x", Age: 25},
		{ID: "u3", Name: "Carol", Email: "c@x", Age: 40},
	}
	if err := users.InsertMany(ctx, batch); err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	for _, u := range batch {
		got, err := users.Get(ctx, u.ID)
		if err != nil {
			t.Fatalf("Get(%s): %v", u.ID, err)
		}
		if got.Name != u.Name {
			t.Fatalf("Get(%s).Name = %s, want %s", u.ID, got.Name, u.Name)
		}
	}
}

func TestInsertMany_UniqueViolationRollsBack(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	// Seed one record.
	if err := users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "a@x", Age: 30}); err != nil {
		t.Fatal(err)
	}

	// Batch where the second record conflicts on the unique email.
	batch := []*User{
		{ID: "u2", Name: "Bob", Email: "b@x", Age: 25},
		{ID: "u3", Name: "Carol", Email: "a@x", Age: 40}, // duplicate email
	}
	err := users.InsertMany(ctx, batch)
	if !errors.Is(err, bw.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	// u2 should not exist because the whole batch rolled back.
	if _, err := users.Get(ctx, "u2"); !errors.Is(err, bw.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for u2 after rollback, got %v", err)
	}
}

func TestInsertTx(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	err := db.Update(func(tx *bw.Tx) error {
		for _, u := range []*User{
			{ID: "u1", Name: "Alice", Email: "a@x", Age: 30},
			{ID: "u2", Name: "Bob", Email: "b@x", Age: 25},
		} {
			if err := users.InsertTx(tx, u); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	for _, id := range []string{"u1", "u2"} {
		if _, err := users.Get(ctx, id); err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
	}
}

func TestDeleteTx(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "a@x", Age: 30}); err != nil {
		t.Fatal(err)
	}

	err := db.Update(func(tx *bw.Tx) error {
		return users.DeleteTx(tx, "u1")
	})
	if err != nil {
		t.Fatalf("DeleteTx: %v", err)
	}

	if _, err := users.Get(ctx, "u1"); !errors.Is(err, bw.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after DeleteTx, got %v", err)
	}
}

func TestFindAndUpdate(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	for _, u := range []*User{
		{ID: "u1", Name: "Alice", Email: "a@x", Age: 20},
		{ID: "u2", Name: "Bob", Email: "b@x", Age: 30},
		{ID: "u3", Name: "Carol", Email: "c@x", Age: 40},
	} {
		if err := users.Insert(ctx, u); err != nil {
			t.Fatal(err)
		}
	}

	// Bump age by 1 for users with age >= 30; skip others.
	q, _ := query.Parse("age[gte]=30")
	err := users.FindAndUpdate(ctx, q, func(u *User) (*User, error) {
		u.Age++
		return u, nil
	})
	if err != nil {
		t.Fatalf("FindAndUpdate: %v", err)
	}

	// Alice untouched.
	alice, _ := users.Get(ctx, "u1")
	if alice.Age != 20 {
		t.Fatalf("Alice age = %d, want 20", alice.Age)
	}
	// Bob bumped.
	bob, _ := users.Get(ctx, "u2")
	if bob.Age != 31 {
		t.Fatalf("Bob age = %d, want 31", bob.Age)
	}
	// Carol bumped.
	carol, _ := users.Get(ctx, "u3")
	if carol.Age != 41 {
		t.Fatalf("Carol age = %d, want 41", carol.Age)
	}
}

func TestFindAndUpdate_NilSkips(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "a@x", Age: 20}); err != nil {
		t.Fatal(err)
	}

	// Return nil to skip the update.
	err := users.FindAndUpdate(ctx, nil, func(u *User) (*User, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("FindAndUpdate: %v", err)
	}

	got, _ := users.Get(ctx, "u1")
	if got.Age != 20 {
		t.Fatalf("Age = %d, want 20 (should be unchanged)", got.Age)
	}
}

func TestCompositeUniqueConstraint(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	locs, err := bw.RegisterBucket[Location](db, "locations")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Insert two locations with different composite unique values.
	if err := locs.Insert(ctx, &Location{ID: "l1", Country: "US", City: "NYC", Code: "US", Prefix: "NY"}); err != nil {
		t.Fatalf("insert l1: %v", err)
	}
	if err := locs.Insert(ctx, &Location{ID: "l2", Country: "US", City: "LA", Code: "US", Prefix: "LA"}); err != nil {
		t.Fatalf("insert l2: %v", err)
	}

	// Insert a third with the same composite unique (Code=US, Prefix=NY) as l1
	// but different pk → should conflict.
	err = locs.Insert(ctx, &Location{ID: "l3", Country: "DE", City: "Berlin", Code: "US", Prefix: "NY"})
	if !errors.Is(err, bw.ErrConflict) {
		t.Fatalf("expected ErrConflict for duplicate composite unique, got %v", err)
	}

	// Same composite unique on the same pk (update) should succeed.
	if err := locs.Insert(ctx, &Location{ID: "l1", Country: "US", City: "NYC", Code: "US", Prefix: "NY"}); err != nil {
		t.Fatalf("re-insert l1 (same pk): %v", err)
	}
}

func TestCompositeUniqueAllowsDifferentCombinations(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	locs, err := bw.RegisterBucket[Location](db, "locations")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Same Code but different Prefix → different composite → should work.
	if err := locs.Insert(ctx, &Location{ID: "l1", Country: "US", City: "NYC", Code: "US", Prefix: "NY"}); err != nil {
		t.Fatal(err)
	}
	if err := locs.Insert(ctx, &Location{ID: "l2", Country: "US", City: "LA", Code: "US", Prefix: "CA"}); err != nil {
		t.Fatal(err)
	}
}

func TestCompositeIndexMaintenance(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	locs, err := bw.RegisterBucket[Location](db, "locations")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Insert, update, and delete should not panic or error.
	if err := locs.Insert(ctx, &Location{ID: "l1", Country: "US", City: "NYC", Code: "A", Prefix: "1"}); err != nil {
		t.Fatal(err)
	}
	// Update: change the composite index fields.
	if err := locs.Insert(ctx, &Location{ID: "l1", Country: "DE", City: "Berlin", Code: "A", Prefix: "1"}); err != nil {
		t.Fatal(err)
	}
	// Delete.
	if err := locs.Delete(ctx, "l1"); err != nil {
		t.Fatal(err)
	}
}

func TestCompositeIndexQuery(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	locs, err := bw.RegisterBucket[Location](db, "locations")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	data := []*Location{
		{ID: "l1", Country: "US", City: "NYC", Code: "A", Prefix: "1"},
		{ID: "l2", Country: "US", City: "LA", Code: "B", Prefix: "2"},
		{ID: "l3", Country: "DE", City: "Berlin", Code: "C", Prefix: "3"},
		{ID: "l4", Country: "DE", City: "Munich", Code: "D", Prefix: "4"},
	}
	for _, loc := range data {
		if err := locs.Insert(ctx, loc); err != nil {
			t.Fatal(err)
		}
	}

	// Query both composite index fields → should use the composite index.
	q, _ := query.Parse("country=US&city=NYC")
	results, err := locs.Find(ctx, q)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(results) != 1 || results[0].ID != "l1" {
		ids := make([]string, len(results))
		for i, r := range results {
			ids[i] = r.ID
		}
		t.Fatalf("expected [l1], got %v", ids)
	}

	// Query with only one field of the composite → should still work (full scan fallback).
	q2, _ := query.Parse("country=DE")
	results2, err := locs.Find(ctx, q2)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(results2) != 2 {
		t.Fatalf("expected 2 results for country=DE, got %d", len(results2))
	}
}
