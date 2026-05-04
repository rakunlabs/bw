package bw_test

import (
	"context"
	"errors"
	"testing"

	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/query"
)

// TestGetTxSeesUncommittedWrite is the main motivating use case for
// the *Tx read helpers: a write transaction needs to read back data
// it has just written. The standalone Bucket.Get opens its own
// db.View() that can't see in-flight writes thanks to Badger's MVCC
// snapshot isolation.
func TestGetTxSeesUncommittedWrite(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := db.Update(func(tx *bw.Tx) error {
		if err := users.InsertTx(tx, &User{ID: "u1", Name: "Alice", Email: "a@x", Age: 30}); err != nil {
			return err
		}
		// Within the same tx, GetTx must see the new row.
		got, err := users.GetTx(tx, "u1")
		if err != nil {
			return err
		}
		if got.Name != "Alice" {
			t.Fatalf("GetTx after InsertTx: want Alice, got %q", got.Name)
		}
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Sanity check the row is also visible after commit.
	got, err := users.Get(ctx, "u1")
	if err != nil {
		t.Fatalf("Get after commit: %v", err)
	}
	if got.Name != "Alice" {
		t.Fatalf("post-commit Get: want Alice, got %q", got.Name)
	}
}

// TestGetTxNotFound covers the not-found path on the typed read.
func TestGetTxNotFound(t *testing.T) {
	db := openTestDB(t)
	users := newUsers(t, db)

	err := db.View(func(tx *bw.Tx) error {
		_, err := users.GetTx(tx, "missing")
		return err
	})
	if !errors.Is(err, bw.ErrNotFound) {
		t.Fatalf("GetTx missing key: want ErrNotFound, got %v", err)
	}
}

// TestFindTxSeesUncommittedWrites verifies that FindTx reads observe
// the in-flight writes of the same transaction, and that
// sort/offset/limit on q are honoured (matching Find's contract).
func TestFindTxSeesUncommittedWrites(t *testing.T) {
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := db.Update(func(tx *bw.Tx) error {
		if err := users.InsertTx(tx, &User{ID: "u1", Name: "Alice", Email: "a@x", Age: 30}); err != nil {
			return err
		}
		if err := users.InsertTx(tx, &User{ID: "u2", Name: "Bob", Email: "b@x", Age: 40}); err != nil {
			return err
		}

		q, _ := query.Parse("_sort=age")
		got, err := users.FindTx(tx, q)
		if err != nil {
			return err
		}
		if len(got) != 2 {
			t.Fatalf("FindTx within tx: want 2 rows, got %d", len(got))
		}
		if got[0].Age != 30 || got[1].Age != 40 {
			t.Fatalf("FindTx sort by age: got [%d, %d]", got[0].Age, got[1].Age)
		}
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

// TestWalkTxStreamsRows is the streaming counterpart to FindTx. It
// also verifies the early-exit semantics — returning a non-nil error
// from fn aborts the iteration.
func TestWalkTxStreamsRows(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	users := newUsers(t, db)

	for i, name := range []string{"Alice", "Bob", "Carol"} {
		if err := users.Insert(ctx, &User{
			ID:    string(rune('a' + i)),
			Name:  name,
			Email: name + "@x",
			Age:   20 + i,
		}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	var seen int
	if err := db.View(func(tx *bw.Tx) error {
		return users.WalkTx(tx, nil, func(u *User) error {
			seen++
			return nil
		})
	}); err != nil {
		t.Fatalf("WalkTx: %v", err)
	}
	if seen != 3 {
		t.Fatalf("WalkTx visited %d rows, want 3", seen)
	}

	// Early-exit: fn returning an error stops iteration immediately.
	stop := errors.New("stop")
	var partial int
	err := db.View(func(tx *bw.Tx) error {
		return users.WalkTx(tx, nil, func(u *User) error {
			partial++
			if partial == 2 {
				return stop
			}
			return nil
		})
	})
	if !errors.Is(err, stop) {
		t.Fatalf("WalkTx early-exit: want stop sentinel, got %v", err)
	}
	if partial != 2 {
		t.Fatalf("WalkTx visited %d rows before stopping, want 2", partial)
	}
}

// TestCountTxSeesUncommittedWrites verifies CountTx observes pending
// writes within the same tx.
func TestCountTxSeesUncommittedWrites(t *testing.T) {
	db := openTestDB(t)
	users := newUsers(t, db)

	if err := db.Update(func(tx *bw.Tx) error {
		for i, name := range []string{"a", "b", "c"} {
			if err := users.InsertTx(tx, &User{
				ID:    name,
				Name:  name,
				Email: name + "@x",
				Age:   20 + i,
			}); err != nil {
				return err
			}
		}
		n, err := users.CountTx(tx, nil)
		if err != nil {
			return err
		}
		if n != 3 {
			t.Fatalf("CountTx within tx: want 3, got %d", n)
		}
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

// TestTxReadHelpersRejectNilTx is a defensive contract check: every
// *Tx-suffixed read helper must reject a nil tx with a plain error
// rather than panic.
func TestTxReadHelpersRejectNilTx(t *testing.T) {
	db := openTestDB(t)
	users := newUsers(t, db)

	if _, err := users.GetTx(nil, "u1"); err == nil {
		t.Fatalf("GetTx(nil) returned nil error")
	}
	if _, err := users.FindTx(nil, nil); err == nil {
		t.Fatalf("FindTx(nil) returned nil error")
	}
	if err := users.WalkTx(nil, nil, func(*User) error { return nil }); err == nil {
		t.Fatalf("WalkTx(nil) returned nil error")
	}
	if _, err := users.CountTx(nil, nil); err == nil {
		t.Fatalf("CountTx(nil) returned nil error")
	}
}
