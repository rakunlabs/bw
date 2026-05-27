package bw

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw/codec"
)

// Sensible defaults applied to the underlying badger.Options when the user
// does not override them via WithBadgerOptions. Badger's stock defaults
// are tuned for very large datasets (256 MiB block cache, 1 GiB value
// log file size, 64 MiB memtables) and feel wasteful for the small/medium
// embedded use-cases bw targets, so we shrink them here.
//
// These are package-level vars on purpose: callers may tune them once at
// process start without going through WithBadgerOptions.
var (
	// DefaultCacheSize is the BlockCacheSize applied by default. Badger's
	// own default is 256 MiB.
	DefaultCacheSize int64 = 100 << 20 // 100 MiB
	// DefaultLogSize is the ValueLogFileSize applied by default. Badger's
	// own default is 1 GiB.
	DefaultLogSize int64 = 100 << 20 // 100 MiB
)

// Option configures a *DB at Open time.
type Option func(*options)

type options struct {
	codec    codec.Codec
	badger   *badger.Options
	badgerFn func(*badger.Options)
	logger   badger.Logger
	inMem    *bool
}

// WithCodec sets the codec used to serialize record values.
// The default is msgpack (codec.MsgPack).
func WithCodec(c codec.Codec) Option {
	return func(o *options) { o.codec = c }
}

// WithBadgerOptions overrides the underlying badger.Options entirely.
//
// When provided, bw does NOT apply DefaultCacheSize / DefaultLogSize: the
// caller is assumed to know what they want. The path argument to Open is
// also ignored in favour of bo.Dir / bo.ValueDir.
func WithBadgerOptions(bo badger.Options) Option {
	return func(o *options) { o.badger = &bo }
}

// WithBadgerTune lets the caller tweak individual badger.Options fields
// after bw has assembled its own defaults. Unlike [WithBadgerOptions],
// this preserves the path argument to Open and keeps bw's lighter
// DefaultCacheSize / DefaultLogSize in place; the caller only supplies
// a delta.
//
// The function is invoked AFTER the base options are built (whether
// from path, in-memory mode, or a wholesale [WithBadgerOptions]
// override) and BEFORE the logger from [WithLogger] is reapplied, so
// any change to bo.Logger here will be overwritten — use [WithLogger]
// for logger changes.
//
// Example — direct field assignment (recommended):
//
//	db, _ := bw.Open("/var/lib/foo", bw.WithBadgerTune(func(bo *badger.Options) {
//	    bo.NumVersionsToKeep = 3
//	    bo.NumGoroutines     = 8
//	    bo.NumCompactors     = 4
//	}))
//
// Badger's With* methods return a new Options value (value receivers),
// so if you prefer the chained form you must reassign through the
// pointer:
//
//	bw.WithBadgerTune(func(bo *badger.Options) {
//	    *bo = bo.WithNumVersionsToKeep(3).WithNumGoroutines(8)
//	})
//
// Passing nil is allowed and is a no-op.
func WithBadgerTune(fn func(*badger.Options)) Option {
	return func(o *options) { o.badgerFn = fn }
}

// WithLogger sets the badger.Logger. Pass nil to silence Badger entirely.
func WithLogger(l badger.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithInMemory configures Badger to run entirely in memory. Convenient for
// tests; the path argument to Open is ignored.
func WithInMemory(b bool) Option {
	return func(o *options) {
		o.inMem = &b
	}
}

// applyDefaults applies bw's lighter-than-Badger defaults. Called for the
// path-based and in-memory paths, but not when the user supplied a fully
// custom badger.Options via WithBadgerOptions.
func applyDefaults(bo badger.Options) badger.Options {
	return bo.
		WithBlockCacheSize(DefaultCacheSize).
		WithValueLogFileSize(DefaultLogSize)
}
