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
	codec  codec.Codec
	badger *badger.Options
	logger badger.Logger
	inMem  *bool
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
