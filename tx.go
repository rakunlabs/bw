package bw

import (
	"github.com/dgraph-io/badger/v4"
)

// Tx is a transaction handle wrapping a *badger.Txn.
//
// Use db.View(fn) for a read-only transaction or db.Update(fn) for a
// read-write transaction. db.Begin/BeginRead are also available for
// users who need finer control over the lifecycle.
type Tx struct {
	db   *DB
	btx  *badger.Txn
	rw   bool
	done bool

	release func()
}

// View runs fn inside a read-only transaction. The transaction is
// automatically discarded after fn returns.
func (db *DB) View(fn func(tx *Tx) error) error {
	db.accessMu.RLock()
	defer db.accessMu.RUnlock()
	return db.bdb.View(func(btx *badger.Txn) error {
		tx := &Tx{db: db, btx: btx, rw: false}
		return fn(tx)
	})
}

// Update runs fn inside a read-write transaction. If fn returns nil,
// the transaction is committed; otherwise it is discarded. Callbacks must
// use the Tx variants of bucket operations; starting another standalone
// write from the callback would attempt to nest the single-writer gate.
func (db *DB) Update(fn func(tx *Tx) error) error {
	db.accessMu.RLock()
	defer db.accessMu.RUnlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	return db.bdb.Update(func(btx *badger.Txn) error {
		tx := &Tx{db: db, btx: btx, rw: true}
		return fn(tx)
	})
}

// Begin starts a read-write transaction. The caller is responsible for
// calling Commit or Discard.
func (db *DB) Begin() *Tx {
	db.accessMu.RLock()
	db.writeMu.Lock()
	return &Tx{db: db, btx: db.bdb.NewTransaction(true), rw: true, release: func() {
		db.writeMu.Unlock()
		db.accessMu.RUnlock()
	}}
}

// BeginRead starts a read-only transaction.
func (db *DB) BeginRead() *Tx {
	db.accessMu.RLock()
	return &Tx{db: db, btx: db.bdb.NewTransaction(false), rw: false, release: db.accessMu.RUnlock}
}

// Commit commits the transaction. Calling Commit on a read-only
// transaction is a no-op apart from discarding it.
func (tx *Tx) Commit() error {
	if tx.done {
		return nil
	}
	tx.done = true
	if tx.release != nil {
		defer tx.release()
		tx.release = nil
	}

	if !tx.rw {
		tx.btx.Discard()
		return nil
	}
	return tx.btx.Commit()
}

// Discard releases the transaction without committing.
func (tx *Tx) Discard() {
	if tx.done {
		return
	}
	tx.done = true
	tx.btx.Discard()
	if tx.release != nil {
		tx.release()
		tx.release = nil
	}
}

// Badger returns the underlying *badger.Txn for advanced use.
func (tx *Tx) Badger() *badger.Txn { return tx.btx }

// DB returns the parent DB.
func (tx *Tx) DB() *DB { return tx.db }

// ReadOnly reports whether the transaction is read-only.
func (tx *Tx) ReadOnly() bool { return !tx.rw }
