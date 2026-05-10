// Package bw is a thin BadgerDB wrapper that provides a typed bucket API
// and exposes filtering through github.com/rakunlabs/query.
//
// Usage:
//
//	db, err := bw.Open("/var/lib/foo")
//	defer db.Close()
//
//	users, _ := bw.RegisterBucket[User](db, "users")
//	_ = users.Insert(ctx, &User{ID: "1", Name: "Alice"})
//
//	q, _ := query.Parse("name=Alice&_limit=10")
//	got, _ := users.Find(ctx, q)
package bw

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw/codec"
)

// DB is a bw database handle. It wraps a *badger.DB plus the codec and any
// registered bucket schemas.
type DB struct {
	bdb   *badger.DB
	codec codec.Codec
	path  string          // filesystem path; empty for in-memory databases
	fts   *ftsRegistry    // full-text search indexes
	vec   *vectorRegistry // vector search indexes
}

// Open opens (or creates) a bw database at the given filesystem path.
//
// If options include WithBadgerOptions, the embedded badger options take
// precedence and path is ignored. WithInMemory(true) overrides path and
// the badger options' Dir/ValueDir.
func Open(path string, opts ...Option) (*DB, error) {
	o := &options{
		codec:  codec.MsgPack(),
		logger: newLogger(),
	}
	for _, opt := range opts {
		opt(o)
	}

	var bo badger.Options

	switch {
	case o.inMem != nil && *o.inMem:
		bo = applyDefaults(badger.DefaultOptions("").WithInMemory(true))
	case o.badger != nil:
		// Caller supplied an explicit badger.Options; respect it verbatim.
		bo = *o.badger
	default:
		bo = applyDefaults(badger.DefaultOptions(path))
	}

	bo = bo.WithLogger(o.logger)

	bdb, err := badger.Open(bo)
	if err != nil {
		return nil, err
	}

	dbPath := ""
	if o.inMem == nil || !*o.inMem {
		dbPath = path
	}

	return &DB{
		bdb:   bdb,
		codec: o.codec,
		path:  dbPath,
		fts:   newFTSRegistry(),
		vec:   newVectorRegistry(),
	}, nil
}

// Close closes the underlying Badger database. The full-text-search
// and vector indexes live entirely as Badger keys, so there is nothing
// else to release.
func (db *DB) Close() error {
	if db == nil || db.bdb == nil {
		return nil
	}
	if db.fts != nil {
		db.fts.closeAll()
	}
	if db.vec != nil {
		db.vec.closeAll()
	}
	return db.bdb.Close()
}

// Codec returns the codec the DB was opened with.
func (db *DB) Codec() codec.Codec { return db.codec }

// Badger returns the underlying *badger.DB. Use sparingly: writes that
// bypass bw's bucket abstraction will not maintain indexes.
func (db *DB) Badger() *badger.DB { return db.bdb }

// Wipe drops every key from the database — data, indexes, unique
// reservations, and bucket schema metadata — and resets every
// registered FTS index. It is destructive and irreversible. Use it
// when you want to follow up with Restore for a clean swap.
//
// In-process bucket handles returned from RegisterBucket remain valid
// after Wipe: their schema, codec and FTS pointers are mutated in
// place. The next write into a bucket re-establishes its meta keys
// transparently.
//
// Caveats inherited from Badger:
//   - DropAll blocks all writes for the duration of the wipe.
//   - DropAll is NOT safe to run concurrently with reads. Quiesce all
//     RPCs that touch the DB before calling Wipe.
func (db *DB) Wipe() error {
	if db == nil || db.bdb == nil {
		return nil
	}
	if err := db.bdb.DropAll(); err != nil {
		return err
	}
	// FTS and vector state live as Badger keys, so DropAll has
	// already removed them. The registry resets here are kept for
	// symmetry and as a hook for future implementations that may
	// hold off-Badger state.
	if db.fts != nil {
		if err := db.fts.resetAll(db); err != nil {
			return err
		}
	}
	if db.vec != nil {
		if err := db.vec.resetAll(db); err != nil {
			return err
		}
	}
	return nil
}
