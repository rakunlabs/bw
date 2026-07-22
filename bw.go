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
	"sync"

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

	// Index and HNSW maintenance update shared keys. Holding this lock
	// for the full write transaction lifetime keeps those read-modify-
	// write operations safe even when Badger conflict detection is off.
	writeMu sync.Mutex
	// schemaMu serializes registration/migration with destructive or
	// schema-bearing backup maintenance.
	schemaMu sync.Mutex
	// Destructive maintenance takes the write side while ordinary
	// transactions hold a read lock for their complete lifetime.
	accessMu  maintenanceGate
	runtimeMu sync.RWMutex
	epochs    map[string]uint64
}

// maintenanceGate blocks new transactions only while maintenance is active.
// It deliberately allows readers while maintenance is waiting, so callbacks
// can safely perform nested reads.
type maintenanceGate struct {
	mu      sync.Mutex
	cond    *sync.Cond
	readers int
	writer  bool
	waiting int
}

func (g *maintenanceGate) init() { g.cond = sync.NewCond(&g.mu) }

func (g *maintenanceGate) RLock() {
	g.mu.Lock()
	// Readers already inside a callback may open a nested read while a
	// writer waits. Once the active reader group drains to zero, waiting
	// writers get priority over a new group.
	for g.writer || (g.waiting > 0 && g.readers == 0) {
		g.cond.Wait()
	}
	g.readers++
	g.mu.Unlock()
}

func (g *maintenanceGate) RUnlock() {
	g.mu.Lock()
	g.readers--
	if g.readers == 0 {
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

func (g *maintenanceGate) Lock() {
	g.mu.Lock()
	g.waiting++
	for g.writer || g.readers > 0 {
		g.cond.Wait()
	}
	g.waiting--
	g.writer = true
	g.mu.Unlock()
}

func (g *maintenanceGate) Unlock() {
	g.mu.Lock()
	g.writer = false
	g.cond.Broadcast()
	g.mu.Unlock()
}

// Open opens (or creates) a bw database at the given filesystem path.
//
// If options include WithBadgerOptions, the embedded badger options take
// precedence and path is ignored. WithInMemory(true) overrides path and
// the badger options' Dir/ValueDir. WithBadgerTune runs last and lets
// the caller mutate individual fields on top of whichever base was
// chosen.
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

	// Apply user-supplied per-field tuning on top of the base options
	// (path, in-memory, or full override). Runs before WithLogger so
	// the logger choice from WithLogger always wins.
	if o.badgerFn != nil {
		o.badgerFn(&bo)
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

	db := &DB{
		bdb:    bdb,
		codec:  o.codec,
		path:   dbPath,
		fts:    newFTSRegistry(),
		vec:    newVectorRegistry(),
		epochs: make(map[string]uint64),
	}
	db.accessMu.init()
	return db, nil
}

func (db *DB) bucketEpoch(bucket string) uint64 {
	db.runtimeMu.RLock()
	defer db.runtimeMu.RUnlock()
	return db.epochs[bucket]
}

func (db *DB) trackBucket(bucket string) uint64 {
	db.runtimeMu.Lock()
	defer db.runtimeMu.Unlock()
	epoch := db.epochs[bucket]
	db.epochs[bucket] = epoch
	return epoch
}

func (db *DB) bumpBucketEpoch(bucket string) uint64 {
	db.runtimeMu.Lock()
	defer db.runtimeMu.Unlock()
	db.epochs[bucket]++
	return db.epochs[bucket]
}

func (db *DB) invalidateBucket(bucket string) uint64 {
	db.accessMu.Lock()
	defer db.accessMu.Unlock()
	return db.bumpBucketEpoch(bucket)
}

func (db *DB) invalidateBucketHandles() {
	db.runtimeMu.Lock()
	defer db.runtimeMu.Unlock()
	for bucket := range db.epochs {
		db.epochs[bucket]++
	}
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
	db.schemaMu.Lock()
	defer db.schemaMu.Unlock()
	db.accessMu.Lock()
	defer db.accessMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	return db.bdb.DropAll()
}
