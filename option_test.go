package bw_test

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw"
)

// TestWithBadgerTune_AppliesAfterDefaults verifies that the user's tune
// function runs on top of the bw-default in-memory options, and that
// its field assignments survive into badger.Open.
func TestWithBadgerTune_AppliesAfterDefaults(t *testing.T) {
	var seen badger.Options
	called := false

	db, err := bw.Open("",
		bw.WithInMemory(true),
		bw.WithLogger(nil),
		bw.WithBadgerTune(func(bo *badger.Options) {
			called = true
			seen = *bo
			bo.NumVersionsToKeep = 7
			bo.NumGoroutines = 3
		}),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if !called {
		t.Fatal("tune function was not invoked")
	}
	// bw's lighter defaults must already be visible inside the tune
	// callback — they are applied before the callback runs.
	if seen.BlockCacheSize != bw.DefaultCacheSize {
		t.Errorf("BlockCacheSize inside tune = %d, want bw default %d",
			seen.BlockCacheSize, bw.DefaultCacheSize)
	}
	if seen.ValueLogFileSize != bw.DefaultLogSize {
		t.Errorf("ValueLogFileSize inside tune = %d, want bw default %d",
			seen.ValueLogFileSize, bw.DefaultLogSize)
	}
	if !seen.InMemory {
		t.Error("InMemory inside tune = false, want true")
	}

	// Verify the mutation made it through to the underlying badger
	// instance.
	bdb := db.Badger()
	if got := bdb.Opts().NumVersionsToKeep; got != 7 {
		t.Errorf("NumVersionsToKeep on badger = %d, want 7", got)
	}
}

// TestWithBadgerTune_NilIsNoOp verifies that passing a nil tune
// function is accepted and does not crash.
func TestWithBadgerTune_NilIsNoOp(t *testing.T) {
	db, err := bw.Open("",
		bw.WithInMemory(true),
		bw.WithLogger(nil),
		bw.WithBadgerTune(nil),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = db.Close()
}

// TestWithBadgerTune_OverridesWithBadgerOptions verifies that the tune
// callback can mutate options that came from a full WithBadgerOptions
// override, not just bw's defaults.
func TestWithBadgerTune_OverridesWithBadgerOptions(t *testing.T) {
	base := badger.DefaultOptions("").
		WithInMemory(true).
		WithLogger(nil).
		WithNumVersionsToKeep(1)

	db, err := bw.Open("",
		bw.WithBadgerOptions(base),
		bw.WithLogger(nil),
		bw.WithBadgerTune(func(bo *badger.Options) {
			bo.NumVersionsToKeep = 9
		}),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if got := db.Badger().Opts().NumVersionsToKeep; got != 9 {
		t.Errorf("NumVersionsToKeep = %d, want 9 (tune should override "+
			"WithBadgerOptions)", got)
	}
}
