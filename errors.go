package bw

import "errors"

// Sentinel errors returned by bw.
var (
	// ErrNotFound is returned when a key does not exist in a bucket.
	ErrNotFound = errors.New("bw: not found")
	// ErrNoPK is returned when a typed bucket operation requires a primary
	// key but the registered schema has none and no key extractor was
	// provided.
	ErrNoPK = errors.New("bw: no primary-key field")
	// ErrConflict is returned when a write would violate a unique
	// constraint (Phase 2; reserved).
	ErrConflict = errors.New("bw: unique constraint violated")
	// ErrClosed is returned when an operation is attempted on a closed DB.
	ErrClosed = errors.New("bw: database is closed")
	// ErrReadOnlyTx is returned when a mutating call is made on a
	// read-only transaction.
	ErrReadOnlyTx = errors.New("bw: read-only transaction")
	// ErrNoFTS is returned when Search is called on a bucket with no
	// FTS-tagged fields.
	ErrNoFTS = errors.New("bw: no full-text search fields configured")
)
