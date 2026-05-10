package bw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw/schema"
)

// User migrations let callers transform every record in a bucket as
// part of a schema version bump. They run after RegisterBucket has
// reconciled the static schema (indexes, manifest, fingerprint) but
// before the bucket is handed back to the caller.
//
// Each migration is bound to a (fromVersion, toVersion) pair. When
// RegisterBucket sees that the stored bucket version is below the
// requested WithVersion, it walks every registered migration whose
// fromVersion equals the stored version, applies it, advances the
// stored version, and repeats — chaining 1→2→3 in order.
//
// Three flavours of migration are supplied for ergonomics:
//
//   - WithRawMigration: receives the encoded record bytes and
//     returns new bytes. The caller decides how to decode (typed
//     struct, map[string]any, or anything else). Codec-agnostic.
//
//   - WithTypedMigration[Old,New]: decodes the old shape into the
//     supplied generic type and re-encodes the result. Convenient
//     when both shapes are well-known Go structs.
//
//   - WithVectorReembed[T]: shorthand for "the only thing that
//     changed is the vector field; recompute it for every record".
//
// All three paths feed into the same runner: per-batch Badger
// transactions (so very large buckets don't blow ErrTxnTooBig), a
// resume cursor written between batches (so a crash leaves a
// well-defined partial state to pick up from), and full secondary-
// index / FTS / vector maintenance via the standard Insert path.

// RawMigrationFn transforms the encoded bytes of one record. It must
// be deterministic and idempotent (the runner may legitimately replay
// the same record on resume).
type RawMigrationFn func(ctx context.Context, raw []byte) ([]byte, error)

// userMigration is one registered migration step.
type userMigration struct {
	fromV uint64
	toV   uint64
	apply RawMigrationFn
}

// WithRawMigration registers a byte-level migration step from
// fromVersion to toVersion. fn is invoked once per record in the
// bucket; the returned bytes replace the record in place.
//
// The migration runs inside RegisterBucket when stored bucket version
// equals fromVersion. Multiple migrations can be chained by calling
// WithRawMigration repeatedly with consecutive (fromV, toV) pairs.
func WithRawMigration[T any](fromV, toV uint64, fn RawMigrationFn) BucketOption[T] {
	return func(b *Bucket[T]) {
		b.userMigrations = append(b.userMigrations, userMigration{
			fromV: fromV,
			toV:   toV,
			apply: fn,
		})
	}
}

// WithTypedMigration registers a typed migration step. Old is the
// previous record shape; the supplied function reads it and returns
// the new record (which must be encodable by the bucket's codec).
//
// Old does not need to live in any specific package — define it
// alongside the migration with whatever bw tags it had at the time of
// the previous schema version. The runner unmarshals raw bytes into
// *Old, calls fn, and re-marshals the *T it returns.
//
// Use this when both shapes are full Go structs you maintain. For
// quick map-style edits without defining the old struct, use
// WithRawMigration with codec.UnmarshalMap.
func WithTypedMigration[Old, New any](fromV, toV uint64, fn func(ctx context.Context, old *Old) (*New, error)) BucketOption[New] {
	return func(b *Bucket[New]) {
		raw := func(ctx context.Context, in []byte) ([]byte, error) {
			oldRec := new(Old)
			if err := b.codec.Unmarshal(in, oldRec); err != nil {
				return nil, fmt.Errorf("typed migration v%d->v%d: decode old: %w", fromV, toV, err)
			}
			newRec, err := fn(ctx, oldRec)
			if err != nil {
				return nil, err
			}
			if newRec == nil {
				return nil, fmt.Errorf("typed migration v%d->v%d: returned nil record", fromV, toV)
			}
			out, err := b.codec.Marshal(newRec)
			if err != nil {
				return nil, fmt.Errorf("typed migration v%d->v%d: encode new: %w", fromV, toV, err)
			}
			return out, nil
		}
		b.userMigrations = append(b.userMigrations, userMigration{
			fromV: fromV,
			toV:   toV,
			apply: raw,
		})
	}
}

// WithVectorReembed registers a migration step that only recomputes
// the bucket's vector field. Useful when switching embedding models
// (the data shape is unchanged, only the embedding's dim or its
// numeric values shift).
//
// The bucket must declare exactly one `vector` field; the embedder is
// invoked per record and its output replaces the existing Embed
// slice. Other fields pass through unchanged.
func WithVectorReembed[T any](fromV, toV uint64, embedder func(ctx context.Context, record *T) ([]float32, error)) BucketOption[T] {
	return func(b *Bucket[T]) {
		raw := func(ctx context.Context, in []byte) ([]byte, error) {
			rec := new(T)
			if err := b.codec.Unmarshal(in, rec); err != nil {
				return nil, fmt.Errorf("vector reembed v%d->v%d: decode: %w", fromV, toV, err)
			}
			vec, err := embedder(ctx, rec)
			if err != nil {
				return nil, fmt.Errorf("vector reembed v%d->v%d: embedder: %w", fromV, toV, err)
			}
			if err := setVectorField(rec, vec, b.schema); err != nil {
				return nil, fmt.Errorf("vector reembed v%d->v%d: set field: %w", fromV, toV, err)
			}
			out, err := b.codec.Marshal(rec)
			if err != nil {
				return nil, fmt.Errorf("vector reembed v%d->v%d: encode: %w", fromV, toV, err)
			}
			return out, nil
		}
		b.userMigrations = append(b.userMigrations, userMigration{
			fromV: fromV,
			toV:   toV,
			apply: raw,
		})
	}
}

// MigrationProgress is the callback invoked between batches by the
// migration runner. Processed counts records migrated so far in the
// current step; total is the bucket-wide record count captured before
// the step began (so it stays stable across the call).
type MigrationProgress func(bucket string, fromV, toV uint64, processed, total uint64)

// WithMigrationProgress installs a progress hook. Called once per
// batch and once at completion of each step.
func WithMigrationProgress[T any](fn MigrationProgress) BucketOption[T] {
	return func(b *Bucket[T]) {
		b.migrationProgress = fn
	}
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

// migrationBatchSize is the number of records processed per Badger
// transaction. Tuned to stay well below MaxBatchCount for the typical
// secondary-index / FTS write fan-out (a record with a few indexes
// and a vector field can produce ~10-20 Badger writes).
const migrationBatchSize = 64

// runUserMigrations walks every registered migration whose fromV is
// at or above storedV and applies them in order, advancing the
// manifest version after each successful step. Returns the new
// version (>= storedV).
//
// On successful completion of the full chain, the bucket's schema
// fingerprint and manifest are also rewritten to match the current
// runtime schema — that's what tells ensureSchemaOrMigrate "the data
// is already in the new shape, no incremental rebuild needed".
func (b *Bucket[T]) runUserMigrations(ctx context.Context, storedV uint64) (uint64, error) {
	if len(b.userMigrations) == 0 || b.version == 0 {
		return storedV, nil
	}
	if storedV >= b.version {
		return storedV, nil
	}

	// Sort by fromV so chained migrations apply in order regardless
	// of registration sequence.
	steps := append([]userMigration(nil), b.userMigrations...)
	sort.Slice(steps, func(i, j int) bool { return steps[i].fromV < steps[j].fromV })

	current := storedV
	for current < b.version {
		// Find the migration that starts at current.
		var (
			step userMigration
			ok   bool
		)
		for _, s := range steps {
			if s.fromV == current {
				step = s
				ok = true
				break
			}
		}
		if !ok {
			return current, fmt.Errorf("bw: no migration registered for bucket %q version %d -> %d (chain stops at %d)",
				b.name, current, b.version, current)
		}
		if step.toV <= step.fromV {
			return current, fmt.Errorf("bw: migration v%d->v%d is not forward", step.fromV, step.toV)
		}

		if err := b.runMigrationStep(ctx, step); err != nil {
			return current, err
		}

		// Persist the new version atomically; resume key has
		// already been cleared by runMigrationStep on success.
		if err := b.bumpStoredVersion(step.toV); err != nil {
			return current, fmt.Errorf("bw: bump version after migration v%d->v%d: %w", step.fromV, step.toV, err)
		}
		current = step.toV
	}

	// Chain done — the data is now in the schema's current shape.
	// Update the persisted fingerprint + manifest so subsequent
	// opens take the fast (no-op) ensureSchemaOrMigrate path.
	if err := b.finalizeUserMigration(); err != nil {
		return current, fmt.Errorf("bw: finalize migration: %w", err)
	}
	return current, nil
}

// runMigrationStep performs one fromV->toV migration over every
// record in the bucket. Resumable: a progress key remembers the last
// successfully processed pk, and a crashed run picks up from the
// next pk on restart.
func (b *Bucket[T]) runMigrationStep(ctx context.Context, step userMigration) error {
	prefix := dataPrefix(b.name)

	progKey := migProgressKey(b.name, step.fromV, step.toV)
	resumePK, err := b.readMigrationResume(progKey)
	if err != nil {
		return err
	}

	total, err := b.countData()
	if err != nil {
		return err
	}

	slog.Info("bw: user migration starting",
		"bucket", b.name, "fromV", step.fromV, "toV", step.toV,
		"total", total, "resume_from", fmt.Sprintf("%x", resumePK))

	var processed uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batch, lastPK, err := b.collectMigrationBatch(prefix, resumePK)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}

		// Apply each record in the batch inside a single txn so
		// secondary indexes / FTS / vector stay consistent with
		// the data write. ctx is threaded through to the user
		// fn so it can honour cancellation (network calls).
		if err := b.applyMigrationBatch(ctx, step, batch); err != nil {
			return err
		}
		// Advance the resume cursor in its own tiny txn so the
		// data write is durable before we record progress.
		if err := b.writeMigrationResume(progKey, lastPK); err != nil {
			return err
		}
		processed += uint64(len(batch))
		resumePK = append([]byte(nil), lastPK...)

		if b.migrationProgress != nil {
			b.migrationProgress(b.name, step.fromV, step.toV, processed, total)
		}

		if len(batch) < migrationBatchSize {
			break
		}
	}

	// Final progress callback so the caller sees a clean
	// "processed == total" signal.
	if b.migrationProgress != nil {
		b.migrationProgress(b.name, step.fromV, step.toV, processed, total)
	}

	// Drop the resume key — step done.
	return b.db.bdb.Update(func(btx *badger.Txn) error {
		return btx.Delete(progKey)
	})
}

// countData returns the number of records currently stored in the
// bucket. Used by the migration runner to surface a "total" to the
// progress hook.
func (b *Bucket[T]) countData() (uint64, error) {
	prefix := dataPrefix(b.name)
	var n uint64
	err := b.db.bdb.View(func(btx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := btx.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			n++
		}
		return nil
	})
	return n, err
}

// migrationRecord pairs the pk with the raw value bytes for one
// scheduled migration. Loaded as a slice so the read txn stays open
// for as short as possible.
type migrationRecord struct {
	pk  []byte
	raw []byte
}

// collectMigrationBatch reads up to migrationBatchSize records whose
// pk is strictly greater than resumePK (or the first batch when
// resumePK is empty). Returns the loaded records plus the pk of the
// last entry, used as the next iteration's seek anchor.
func (b *Bucket[T]) collectMigrationBatch(prefix, resumePK []byte) ([]migrationRecord, []byte, error) {
	var (
		out    []migrationRecord
		lastPK []byte
	)
	err := b.db.bdb.View(func(btx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := btx.NewIterator(opts)
		defer it.Close()

		var seek []byte
		if len(resumePK) > 0 {
			seek = append(append([]byte{}, prefix...), resumePK...)
		} else {
			seek = prefix
		}
		it.Seek(seek)
		// Skip the seek anchor itself when resuming so we don't
		// re-process the last successful record.
		if len(resumePK) > 0 && it.ValidForPrefix(prefix) && vecBytesEqual(it.Item().Key(), seek) {
			it.Next()
		}

		for ; it.ValidForPrefix(prefix) && len(out) < migrationBatchSize; it.Next() {
			item := it.Item()
			k := item.Key()
			pk := append([]byte(nil), k[len(prefix):]...)
			var raw []byte
			if err := item.Value(func(val []byte) error {
				raw = append([]byte(nil), val...)
				return nil
			}); err != nil {
				return err
			}
			out = append(out, migrationRecord{pk: pk, raw: raw})
			lastPK = pk
		}
		return nil
	})
	return out, lastPK, err
}

// applyMigrationBatch transforms each record via step.apply and
// writes the result back. We route through Insert so secondary
// indexes, FTS postings and the vector graph stay in sync — the user
// migration is at heart "rewrite this record", and rewrites should
// look just like ordinary writes.
func (b *Bucket[T]) applyMigrationBatch(ctx context.Context, step userMigration, batch []migrationRecord) error {
	return b.db.Update(func(tx *Tx) error {
		for _, r := range batch {
			newRaw, err := step.apply(ctx, r.raw)
			if err != nil {
				return fmt.Errorf("bw: migration v%d->v%d apply pk=%x: %w",
					step.fromV, step.toV, r.pk, err)
			}
			rec := new(T)
			if err := b.codec.Unmarshal(newRaw, rec); err != nil {
				return fmt.Errorf("bw: migration v%d->v%d decode result pk=%x: %w",
					step.fromV, step.toV, r.pk, err)
			}
			// upsertTx (via InsertTx) reads the old record to
			// diff index entries, then writes the new one. FTS
			// + vector hooks fire automatically.
			if err := b.InsertTx(tx, rec); err != nil {
				return fmt.Errorf("bw: migration v%d->v%d insert pk=%x: %w",
					step.fromV, step.toV, r.pk, err)
			}
		}
		return nil
	})
}

// readMigrationResume returns the resume cursor for the given step,
// or nil when no migration is in progress.
func (b *Bucket[T]) readMigrationResume(progKey []byte) ([]byte, error) {
	var pk []byte
	err := b.db.bdb.View(func(btx *badger.Txn) error {
		item, err := btx.Get(progKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			pk = append([]byte(nil), val...)
			return nil
		})
	})
	return pk, err
}

func (b *Bucket[T]) writeMigrationResume(progKey, pk []byte) error {
	return b.db.bdb.Update(func(btx *badger.Txn) error {
		return btx.Set(progKey, append([]byte(nil), pk...))
	})
}

// bumpStoredVersion writes the new version into the bucket's manifest
// version key. Mirrors the schema-migration version write but kept
// separate so user migrations don't accidentally overwrite the
// fingerprint or other meta.
func (b *Bucket[T]) bumpStoredVersion(newV uint64) error {
	return b.db.bdb.Update(func(btx *badger.Txn) error {
		var buf [8]byte
		bigEndianPutUint64(buf[:], newV)
		return btx.Set(versionKey(b.name), buf[:])
	})
}

// finalizeUserMigration writes the new schema fingerprint and manifest
// in a single Badger transaction. Called once a chain of user
// migrations has run to completion: the data is now in the new shape,
// so the persisted schema metadata should reflect that. Without this
// the next ensureSchemaOrMigrate would refuse the bucket on the
// "fingerprint mismatch + version not bumped" check.
func (b *Bucket[T]) finalizeUserMigration() error {
	want := b.schema.Fingerprint()
	wantManifest, err := json.Marshal(buildManifest(b.schema))
	if err != nil {
		return err
	}
	return b.db.bdb.Update(func(btx *badger.Txn) error {
		if err := btx.Set(metaKey(b.name), []byte(want)); err != nil {
			return err
		}
		return btx.Set(manifestKey(b.name), wantManifest)
	})
}

// readStoredVersion returns the bucket's currently persisted version,
// or zero when the bucket is brand new (no version key yet).
func (b *Bucket[T]) readStoredVersion() (uint64, error) {
	var v uint64
	err := b.db.bdb.View(func(btx *badger.Txn) error {
		item, err := btx.Get(versionKey(b.name))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) == 8 {
				v = bigEndianUint64(val)
			}
			return nil
		})
	})
	return v, err
}

func bigEndianUint64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[7]) |
		uint64(b[6])<<8 |
		uint64(b[5])<<16 |
		uint64(b[4])<<24 |
		uint64(b[3])<<32 |
		uint64(b[2])<<40 |
		uint64(b[1])<<48 |
		uint64(b[0])<<56
}

// bigEndianPutUint64 mirrors binary.BigEndian.PutUint64 without the
// import (kept local so this file stays focused).
func bigEndianPutUint64(b []byte, v uint64) {
	_ = b[7]
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

// setVectorField locates the bucket's single vector field via the
// schema and writes vec into it. Used by WithVectorReembed.
func setVectorField(record any, vec []float32, s *schema.Schema) error {
	fields := s.VectorFields()
	if len(fields) == 0 {
		return fmt.Errorf("bucket has no vector field")
	}
	if len(fields) > 1 {
		return fmt.Errorf("bucket has %d vector fields; reembed is ambiguous", len(fields))
	}
	rv := reflect.ValueOf(record)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return fmt.Errorf("nil record")
		}
		rv = rv.Elem()
	}
	fv := rv.FieldByIndex(fields[0].Index)
	if fv.Kind() != reflect.Slice || fv.Type().Elem().Kind() != reflect.Float32 {
		return fmt.Errorf("vector field %q is not []float32", fields[0].Name)
	}
	fv.Set(reflect.ValueOf(vec))
	return nil
}
