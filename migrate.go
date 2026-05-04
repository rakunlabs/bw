package bw

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw/engine"
	"github.com/rakunlabs/bw/schema"
)

// MigrateBucket re-registers a bucket whose struct definition has changed
// (e.g. new fields were added, indexes added/removed). Unlike a full
// rebuild, it performs an incremental migration:
//
//   - Fields whose index/unique flags are unchanged are left alone.
//   - Only newly indexed/unique fields have their entries built.
//   - Only removed indexes/unique constraints have their entries deleted.
//
// This is safe for additive changes (new fields default to their zero value
// in old records) and for removing/renaming indexed fields. It is NOT safe
// if you change the primary key field or rename the `bw` tag of the pk
// field — that requires a manual data migration.
//
// MigrateBucket returns the ready-to-use Bucket, just like RegisterBucket.
func MigrateBucket[T any](db *DB, name string, opts ...BucketOption[T]) (*Bucket[T], error) {
	if db == nil || db.bdb == nil {
		return nil, ErrClosed
	}
	if err := validateBucketName(name); err != nil {
		return nil, err
	}

	var zero T
	s, err := schema.Of(reflect.TypeOf(zero))
	if err != nil {
		return nil, err
	}

	b := &Bucket[T]{
		db:     db,
		codec:  db.codec,
		name:   name,
		schema: s,
	}
	for _, o := range opts {
		o(b)
	}

	if b.keyFn == nil {
		if s.PK == nil {
			return nil, fmt.Errorf("bw: type %s has no `pk` tag and no WithKeyFn provided", s.Type.Name())
		}
		b.keyFn = makePKExtractor[T](s)
	}

	b.fieldEncoders = buildFieldEncoders(s)
	b.hasMaintenance = len(b.fieldEncoders) > 0

	for _, fe := range b.fieldEncoders {
		if !fe.Indexed {
			continue
		}
		fe := fe
		b.indexedQueryFields = append(b.indexedQueryFields, engine.IndexedField{
			Name:   fe.Name,
			Encode: fe.EncodeFromQuery,
		})
	}
	for _, cg := range s.CompositeIndexes {
		cf := engine.CompositeIndexedField{
			Name:   cg.Name,
			Fields: make([]string, len(cg.Fields)),
		}
		for i, f := range cg.Fields {
			cf.Fields[i] = f.Name
			enc := makeEncoderFor(f)
			if enc == nil {
				break
			}
			cf.Encoders = append(cf.Encoders, enc.EncodeFromQuery)
		}
		if len(cf.Encoders) == len(cg.Fields) {
			b.compositeQueryFields = append(b.compositeQueryFields, cf)
		}
	}

	// Perform incremental migration.
	if err := db.migrateBucketIncremental(name, s, b.version, b); err != nil {
		return nil, err
	}

	return b, nil
}

// fieldManifestEntry describes one indexed/unique field for diffing.
type fieldManifestEntry struct {
	Name    string `json:"n"`
	Indexed bool   `json:"i,omitempty"`
	Unique  bool   `json:"u,omitempty"`
}

// manifestKey stores the field manifest alongside the fingerprint.
func manifestKey(bucket string) []byte {
	prefix := []byte{sep, 'm', 'a', 'n', 'i', sep}
	out := make([]byte, 0, len(prefix)+len(bucket))
	out = append(out, prefix...)
	out = append(out, bucket...)
	return out
}

// buildManifest creates the manifest entries from a schema.
func buildManifest(s *schema.Schema) []fieldManifestEntry {
	var entries []fieldManifestEntry
	for _, fe := range buildFieldEncoders(s) {
		entries = append(entries, fieldManifestEntry{
			Name:    fe.Name,
			Indexed: fe.Indexed,
			Unique:  fe.Unique,
		})
	}
	return entries
}

// migrateBucketIncremental performs an incremental migration:
// - Diffs old manifest vs new to find added/removed index/unique fields.
// - Only scans data for newly added fields.
// - Only deletes keys for removed fields.
func (db *DB) migrateBucketIncremental(name string, s *schema.Schema, version uint64, b interface {
	rebuildFieldIndexes(btx *badger.Txn, fields []*fieldEncoder) error
}) error {
	newManifest := buildManifest(s)

	return db.bdb.Update(func(btx *badger.Txn) error {
		// Load old manifest (may not exist for legacy buckets).
		var oldManifest []fieldManifestEntry
		if item, err := btx.Get(manifestKey(name)); err == nil {
			_ = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &oldManifest)
			})
		}

		// Build lookup of old fields.
		type flags struct{ indexed, unique bool }
		oldFields := make(map[string]flags, len(oldManifest))
		for _, e := range oldManifest {
			oldFields[e.Name] = flags{e.Indexed, e.Unique}
		}

		// Build lookup of new fields.
		newFields := make(map[string]flags, len(newManifest))
		for _, e := range newManifest {
			newFields[e.Name] = flags{e.Indexed, e.Unique}
		}

		// Find fields that lost their index → delete their index keys.
		for _, old := range oldManifest {
			newF, exists := newFields[old.Name]
			if old.Indexed && (!exists || !newF.indexed) {
				if err := deleteFieldIndexKeys(btx, name, old.Name); err != nil {
					return err
				}
			}
			if old.Unique && (!exists || !newF.unique) {
				if err := deleteFieldUniqueKeys(btx, name, old.Name); err != nil {
					return err
				}
			}
		}

		// Find fields that are newly indexed/unique → need entries built.
		// Collect the corresponding fieldEncoders from the bucket.
		var addedEncoders []*fieldEncoder
		allEncoders := buildFieldEncoders(s)
		for _, fe := range allEncoders {
			oldF, existed := oldFields[fe.Name]
			needBuildIndex := fe.Indexed && (!existed || !oldF.indexed)
			needBuildUnique := fe.Unique && (!existed || !oldF.unique)
			if needBuildIndex || needBuildUnique {
				// Clone with only the "new" flags so we only build what's needed.
				clone := *fe
				clone.Indexed = needBuildIndex
				clone.Unique = needBuildUnique
				addedEncoders = append(addedEncoders, &clone)
			}
		}

		// Scan data and build entries only for added fields.
		if len(addedEncoders) > 0 {
			if err := b.rebuildFieldIndexes(btx, addedEncoders); err != nil {
				return err
			}
		}

		// Update fingerprint, version, and manifest.
		if err := btx.Set(metaKey(name), []byte(s.Fingerprint())); err != nil {
			return err
		}
		if version > 0 {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], version)
			if err := btx.Set(versionKey(name), buf[:]); err != nil {
				return err
			}
		}
		manifestBytes, _ := json.Marshal(newManifest)
		return btx.Set(manifestKey(name), manifestBytes)
	})
}

// rebuildFieldIndexes scans all data records and builds index/unique entries
// only for the specified field encoders.
func (b *Bucket[T]) rebuildFieldIndexes(btx *badger.Txn, fields []*fieldEncoder) error {
	if len(fields) == 0 {
		return nil
	}

	prefix := dataPrefix(b.name)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix

	it := btx.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()
		pk := key[len(prefix):]

		err := item.Value(func(val []byte) error {
			rec := new(T)
			if err := b.codec.Unmarshal(val, rec); err != nil {
				return err
			}
			return writeIndexesMigrate(btx, b.name, fields, pk, rec)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// writeIndexesMigrate writes index/unique keys without conflict checks,
// and skips zero-value unique entries (so old records missing new fields
// don't collide on the empty value).
func writeIndexesMigrate(btx *badger.Txn, bucket string, encoders []*fieldEncoder, pk []byte, record any) error {
	entries, err := indexEntries(encoders, record)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.Indexed && len(e.Encoded) > 0 {
			if err := btx.Set(indexKey(bucket, e.Name, e.Encoded, pk), nil); err != nil {
				return err
			}
		}
		if e.Unique && len(e.Encoded) > 0 {
			if err := btx.Set(uniqueKey(bucket, e.Name, e.Encoded), append([]byte(nil), pk...)); err != nil {
				return err
			}
		}
	}

	return nil
}

// deleteFieldIndexKeys removes all index keys for a specific field.
func deleteFieldIndexKeys(btx *badger.Txn, bucket, field string) error {
	return deleteByPrefix(btx, indexFieldPrefix(bucket, field))
}

// deleteFieldUniqueKeys removes all unique keys for a specific field.
func deleteFieldUniqueKeys(btx *badger.Txn, bucket, field string) error {
	prefix := make([]byte, 0, len(uniqPrefix)+len(bucket)+1+len(field)+1)
	prefix = append(prefix, uniqPrefix...)
	prefix = append(prefix, bucket...)
	prefix = append(prefix, sep)
	prefix = append(prefix, field...)
	prefix = append(prefix, sep)
	return deleteByPrefix(btx, prefix)
}

// deleteByPrefix deletes all keys with the given prefix.
func deleteByPrefix(btx *badger.Txn, prefix []byte) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix

	it := btx.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().KeyCopy(nil)
		if err := btx.Delete(key); err != nil {
			return err
		}
	}

	return nil
}

// versionKey returns the key used to store the bucket's schema version.
func versionKey(bucket string) []byte {
	prefix := []byte{sep, 'v', 'e', 'r', sep}
	out := make([]byte, 0, len(prefix)+len(bucket))
	out = append(out, prefix...)
	out = append(out, bucket...)
	return out
}

// migrator is the interface that both Bucket[T] and MigrateBucket's internal
// bucket satisfy — it allows the migration logic to call rebuildFieldIndexes.
type migrator interface {
	rebuildFieldIndexes(btx *badger.Txn, fields []*fieldEncoder) error
}

// ensureSchemaOrMigrate is called by RegisterBucket. It handles three cases:
//
//  1. First registration (no stored fingerprint) → store fingerprint + version + manifest.
//  2. Fingerprint matches → check version is not lower, done.
//  3. Fingerprint mismatch + WithVersion provided + version > stored → auto-migrate incrementally.
//  4. Fingerprint mismatch + no version or version <= stored → return error.
func (db *DB) ensureSchemaOrMigrate(bucket string, s *schema.Schema, version uint64, m migrator) error {
	if s == nil {
		return nil
	}
	want := s.Fingerprint()
	mkey := metaKey(bucket)
	vkey := versionKey(bucket)

	var needsMigration bool

	// Check fingerprint.
	err := db.bdb.View(func(btx *badger.Txn) error {
		item, err := btx.Get(mkey)
		switch {
		case err == nil:
			got, vErr := item.ValueCopy(nil)
			if vErr != nil {
				return vErr
			}
			if string(got) == want {
				return nil // fingerprint matches, no migration needed
			}
			// Fingerprint differs — check if we can auto-migrate.
			if version == 0 {
				return fmt.Errorf("bw: schema fingerprint mismatch for bucket %q (have %s, want %s); migrate or pick a new bucket name",
					bucket, string(got), want)
			}
			// Check stored version.
			var storedVersion uint64
			if vItem, vErr := btx.Get(vkey); vErr == nil {
				_ = vItem.Value(func(val []byte) error {
					if len(val) == 8 {
						storedVersion = binary.BigEndian.Uint64(val)
					}
					return nil
				})
			}
			if version <= storedVersion {
				return fmt.Errorf("bw: schema fingerprint mismatch for bucket %q but version %d <= stored %d; bump WithVersion to trigger migration",
					bucket, version, storedVersion)
			}
			needsMigration = true
			return nil
		case errors.Is(err, badger.ErrKeyNotFound):
			return nil // first registration
		default:
			return err
		}
	})
	if err != nil {
		return err
	}

	if needsMigration {
		// Perform incremental migration.
		return db.migrateBucketIncremental(bucket, s, version, m)
	}

	// First registration or fingerprint matched — write metadata.
	return db.bdb.Update(func(btx *badger.Txn) error {
		// Set fingerprint (idempotent if already matches).
		if err := btx.Set(mkey, []byte(want)); err != nil {
			return err
		}
		// Set version if provided.
		if version > 0 {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], version)
			if err := btx.Set(vkey, buf[:]); err != nil {
				return err
			}
		}
		// Set manifest.
		manifest := buildManifest(s)
		data, _ := json.Marshal(manifest)
		return btx.Set(manifestKey(bucket), data)
	})
}
