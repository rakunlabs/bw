package bw

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw/codec"
	"github.com/rakunlabs/bw/engine"
	"github.com/rakunlabs/bw/internal/keyenc"
	"github.com/rakunlabs/bw/schema"
	"github.com/rakunlabs/query"
)

// Bucket is a typed view of a Badger key-prefix that stores records of
// type T. Use RegisterBucket to construct one.
type Bucket[T any] struct {
	db     *DB
	codec  codec.Codec
	name   string
	schema *schema.Schema

	// keyFn extracts the primary-key bytes from a record. If the record
	// has a `pk` tag, this is generated from the schema; otherwise the
	// caller must supply it via WithKeyFn.
	keyFn func(*T) ([]byte, error)

	// hasMaintenance is true when the schema declares any index or
	// unique fields, so write paths know they need to perform the
	// read-old-record dance. Buckets without indexes skip that cost.
	hasMaintenance bool

	// fieldEncoders is the precomputed list of index/unique encoders for
	// this bucket's schema. Built once at registration; both the write
	// path (writeIndexes/deleteIndexes) and the read path (plan) share
	// it. nil when no indexable fields exist.
	fieldEncoders []*fieldEncoder

	// indexedQueryFields is the slice handed to the query planner —
	// derived from fieldEncoders, filtered to fields tagged with `index`.
	indexedQueryFields []engine.IndexedField

	// compositeQueryFields is the slice of composite indexed fields
	// handed to the query planner.
	compositeQueryFields []engine.CompositeIndexedField

	// version is the caller-supplied schema version. When set (>0) and
	// higher than the stored version, RegisterBucket auto-migrates
	// instead of returning a fingerprint mismatch error.
	version uint64

	// ftsIdx is the full-text search index for this bucket, or nil when
	// no FTS-tagged fields exist.
	ftsIdx *ftsIndex

	// vecIdx is the vector index for this bucket, or nil when no
	// vector-tagged field exists.
	vecIdx *vectorIndex

	// embedder, when non-nil, is invoked for every Insert whose
	// vector field is empty so the caller can ship plain records and
	// let bw call out to an embedding model.
	embedder func(ctx context.Context, record any) ([]float32, error)

	// userMigrations is the list of (fromV, toV, applyFn) steps the
	// caller registered via WithRawMigration / WithTypedMigration /
	// WithVectorReembed. Applied during RegisterBucket once the
	// schema-level migration has settled.
	userMigrations []userMigration

	// migrationProgress is an optional hook fired between batches
	// during user migration runs. See WithMigrationProgress.
	migrationProgress MigrationProgress
}

// BucketOption configures a Bucket at registration time.
type BucketOption[T any] func(*Bucket[T])

// WithKeyFn supplies a custom primary-key extractor. Use this when the
// record type has no `pk`-tagged field, or when the natural key is
// composed from multiple fields.
func WithKeyFn[T any](fn func(*T) ([]byte, error)) BucketOption[T] {
	return func(b *Bucket[T]) { b.keyFn = fn }
}

// WithVersion sets the schema version for this bucket. When the stored
// version is lower than the provided one, RegisterBucket automatically
// performs an incremental migration (only rebuilding indexes for changed
// fields) instead of failing with a fingerprint mismatch.
//
// Bump this number each time you change the struct's index/unique surface.
func WithVersion[T any](v uint64) BucketOption[T] {
	return func(b *Bucket[T]) { b.version = v }
}

// WithVectorParams overrides the HNSW knobs for the bucket's vector
// field. M is the graph degree (neighbours kept per node, default 16);
// efConstruction is the candidate-set size used during inserts
// (default 200). Pass zero to leave the corresponding default in
// place.
//
// Once a vector has been inserted, the values are persisted to the
// manifest and become immutable — changing them later orphans
// existing neighbour lists. Pick once, ideally before the first
// insert.
func WithVectorParams[T any](M, efConstruction int) BucketOption[T] {
	return func(b *Bucket[T]) {
		// Stash on the bucket; openVectorIndex has already been
		// called by RegisterBucket but b.vecIdx is the one
		// instance, so we patch its tunables directly. If
		// vector tag is absent the option is silently ignored.
		if b.vecIdx == nil {
			return
		}
		if M > 0 {
			b.vecIdx.wantM = M
		}
		if efConstruction > 0 {
			b.vecIdx.wantEfConstruction = efConstruction
		}
	}
}

// WithEmbedder installs an embedding function that bw calls during
// Insert when the record's `vector`-tagged field is empty. Returning a
// non-nil error from the embedder fails the Insert; the data write is
// rolled back along with any other index maintenance, so partial state
// is impossible.
//
// Records that already carry a populated vector skip the embedder, so
// callers can mix "auto-embed by text" and "I already computed the
// vector" calls in the same bucket.
func WithEmbedder[T any](fn func(ctx context.Context, record *T) ([]float32, error)) BucketOption[T] {
	return func(b *Bucket[T]) {
		b.embedder = func(ctx context.Context, rec any) ([]float32, error) {
			typed, ok := rec.(*T)
			if !ok {
				return nil, fmt.Errorf("bw: embedder: record type mismatch")
			}
			return fn(ctx, typed)
		}
	}
}

// RegisterBucket parses T's schema and returns a typed Bucket bound to db.
//
// The bucket name is used as the key prefix and must not contain NUL
// bytes. The schema is cached per type, so repeated calls are cheap.
//
// T may be any struct type. The default codec (msgpack via shamaton)
// is reflect-based, so no codegen step is required: declare your bucket
// type and call RegisterBucket.
func RegisterBucket[T any](db *DB, name string, opts ...BucketOption[T]) (*Bucket[T], error) {
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

	// Build the planner's IndexedField slice once; the encoders are
	// captured into closures so the hot path never touches reflect.
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

	// Build composite indexed fields for the planner.
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

	// Open the FTS / vector handles up front so user migrations
	// (which call Insert internally) reach the full maintenance
	// path. They're cheap config records — no I/O happens until a
	// write fires.
	ftsFields := s.FTSFields()
	if len(ftsFields) > 0 {
		if existing := db.fts.get(name); existing != nil {
			b.ftsIdx = existing
		} else {
			fi, err := openFTSIndex(db, name, ftsFields)
			if err != nil {
				return nil, err
			}
			b.ftsIdx = fi
			db.fts.set(name, fi)
		}
	}
	vecFields := s.VectorFields()
	if len(vecFields) > 0 {
		if existing := db.vec.get(name); existing != nil {
			b.vecIdx = existing
		} else {
			vi, err := openVectorIndex(db, name, vecFields)
			if err != nil {
				return nil, err
			}
			b.vecIdx = vi
			db.vec.set(name, vi)
		}
	}

	// User migrations run BEFORE the schema reconcile step so a
	// failure leaves the stored fingerprint and version untouched
	// (callers can re-open with the previous T and read their data
	// as if no migration had been attempted). Each migration step
	// advances the bucket's version key as it succeeds, so a
	// resumed run picks up where the last one stopped.
	storedV, err := b.readStoredVersion()
	if err != nil {
		return nil, err
	}
	if _, err := b.runUserMigrations(context.Background(), storedV); err != nil {
		return nil, err
	}

	if err := db.ensureSchemaOrMigrate(name, s, b.version, b); err != nil {
		return nil, err
	}

	return b, nil
}

// makePKExtractor builds a key-fn that fetches the pk-tagged field by index.
func makePKExtractor[T any](s *schema.Schema) func(*T) ([]byte, error) {
	idx := s.PK.Index

	return func(v *T) ([]byte, error) {
		if v == nil {
			return nil, fmt.Errorf("bw: nil record")
		}
		rv := reflect.ValueOf(v).Elem()
		fv := rv.FieldByIndex(idx)

		return pkBytes(fv)
	}
}

// pkBytes converts a primary-key field value to a byte slice. Strings and
// []byte pass through; numeric kinds use the sortable encoding from the
// keyenc package so range scans on integer pk fields stay in natural order.
func pkBytes(v reflect.Value) ([]byte, error) {
	switch v.Kind() {
	case reflect.String:
		return []byte(v.String()), nil
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			out := make([]byte, v.Len())
			copy(out, v.Bytes())

			return out, nil
		}
	}
	if !v.IsValid() {
		return nil, fmt.Errorf("bw: invalid pk value")
	}

	if b, err := keyenc.Encode(v.Interface()); err == nil {
		return b, nil
	}

	return []byte(fmt.Sprint(v.Interface())), nil
}

// Name returns the bucket's key prefix name.
func (b *Bucket[T]) Name() string { return b.name }

// Insert encodes record and writes it under its primary key, maintaining
// every index/unique entry along the way. If the key already exists the
// record is overwritten and stale index entries are removed.
//
// If the schema declares a `unique` field whose value is already owned by
// another pk, ErrConflict is returned and the transaction is rolled back.
func (b *Bucket[T]) Insert(ctx context.Context, record *T) error {
	_ = ctx

	return b.db.Update(func(tx *Tx) error { return b.upsertTx(tx, record, false) })
}

// InsertNew is like Insert but returns ErrConflict if the primary key
// already exists. Use it when you want strict create-only semantics.
func (b *Bucket[T]) InsertNew(ctx context.Context, record *T) error {
	_ = ctx

	return b.db.Update(func(tx *Tx) error { return b.upsertTx(tx, record, true) })
}

// InsertTx is like Insert but operates on a caller-controlled transaction.
// Use it to batch multiple writes into a single atomic commit.
func (b *Bucket[T]) InsertTx(tx *Tx, record *T) error {
	return b.upsertTx(tx, record, false)
}

// InsertNewTx is like InsertNew but operates on a caller-controlled
// transaction. Returns ErrConflict if the primary key already exists.
func (b *Bucket[T]) InsertNewTx(tx *Tx, record *T) error {
	return b.upsertTx(tx, record, true)
}

// InsertMany inserts all records in a single read-write transaction.
// If any record fails (e.g. a unique constraint violation), the entire
// batch is rolled back and the error is returned.
func (b *Bucket[T]) InsertMany(ctx context.Context, records []*T) error {
	_ = ctx

	return b.db.Update(func(tx *Tx) error {
		for _, rec := range records {
			if err := b.upsertTx(tx, rec, false); err != nil {
				return err
			}
		}

		return nil
	})
}

// Update is an alias for Insert. Both perform a read-old-then-write-new so
// the indexes stay consistent.
func (b *Bucket[T]) Update(ctx context.Context, record *T) error {
	return b.Insert(ctx, record)
}

// UpdateTx is like Update but operates on a caller-controlled transaction.
func (b *Bucket[T]) UpdateTx(tx *Tx, record *T) error {
	return b.upsertTx(tx, record, false)
}

// FindAndUpdate finds all records matching q, calls fn on each one, and
// writes back the results — all inside a single read-write transaction.
//
// fn receives each matched record and returns either:
//   - a (possibly modified) *T to write back, or
//   - nil to skip that record (no write).
//
// If fn returns an error, the entire transaction is rolled back.
// Sort, Offset, and Limit from q are honoured before fn is called.
func (b *Bucket[T]) FindAndUpdate(ctx context.Context, q *query.Query, fn func(*T) (*T, error)) error {
	_ = ctx

	scanQ, sortSpec, offset, limit := splitSortPaging(q)

	return b.db.Update(func(tx *Tx) error {
		matches, err := b.scan(tx.btx, scanQ)
		if err != nil {
			return err
		}

		records := make([]*T, 0, len(matches))
		for _, m := range matches {
			rec := new(T)
			if err := b.codec.Unmarshal(m.Value, rec); err != nil {
				return err
			}
			records = append(records, rec)
		}

		if len(sortSpec) > 0 {
			b.sortTyped(records, sortSpec)
		}
		records = applyOffsetLimitTyped(records, offset, limit)

		for _, rec := range records {
			updated, err := fn(rec)
			if err != nil {
				return err
			}
			if updated == nil {
				continue
			}
			if err := b.upsertTx(tx, updated, false); err != nil {
				return err
			}
		}

		return nil
	})
}

// DeleteTx removes the record with the given key within a caller-controlled
// transaction. Idempotent: deleting a missing key returns nil.
func (b *Bucket[T]) DeleteTx(tx *Tx, key any) error {
	pk, err := keyAsBytes(key)
	if err != nil {
		return err
	}

	dKey := dataKey(b.name, pk)
	if b.hasMaintenance {
		item, gerr := tx.btx.Get(dKey)
		switch {
		case gerr == nil:
			old := new(T)
			if err := item.Value(func(val []byte) error {
				return b.codec.Unmarshal(val, old)
			}); err != nil {
				return err
			}
			if err := deleteIndexes(tx.btx, b.name, b.fieldEncoders, pk, old); err != nil {
				return err
			}
		case errors.Is(gerr, badger.ErrKeyNotFound):
			return nil
		default:
			return gerr
		}
	}

	if err := tx.btx.Delete(dKey); err != nil {
		return err
	}

	if b.ftsIdx != nil {
		if err := b.ftsIdx.deleteDoc(tx.btx, pk); err != nil {
			return fmt.Errorf("bw: fts delete: %w", err)
		}
	}
	if b.vecIdx != nil {
		if err := b.vecIdx.deleteVec(tx.btx, pk); err != nil {
			return fmt.Errorf("bw: vector delete: %w", err)
		}
	}

	return nil
}

// upsertTx implements both Insert (insertNewOnly=false, overwrite ok) and
// InsertNew (insertNewOnly=true, ErrConflict on existing pk).
func (b *Bucket[T]) upsertTx(tx *Tx, record *T, insertNewOnly bool) error {
	if !tx.rw {
		return ErrReadOnlyTx
	}
	pk, err := b.keyFn(record)
	if err != nil {
		return err
	}

	dKey := dataKey(b.name, pk)

	// Maybe load the old record. We need it only when:
	//   - the bucket has indexable fields (so we can clean up stale keys), OR
	//   - we're in InsertNew mode (so we can refuse on existing pk).
	var oldRecord *T
	needRead := insertNewOnly || b.hasMaintenance
	if needRead {
		item, gerr := tx.btx.Get(dKey)
		switch {
		case gerr == nil:
			if insertNewOnly {
				return ErrConflict
			}
			if b.hasMaintenance {
				oldRecord = new(T)
				if err := item.Value(func(val []byte) error {
					return b.codec.Unmarshal(val, oldRecord)
				}); err != nil {
					return err
				}
			}
		case errors.Is(gerr, badger.ErrKeyNotFound):
			// no prior record; nothing to clean up
		default:
			return gerr
		}
	}

	val, err := b.codec.Marshal(record)
	if err != nil {
		return err
	}

	if b.hasMaintenance {
		var oldAny any
		if oldRecord != nil {
			oldAny = oldRecord
		}
		if err := writeIndexes(tx.btx, b.name, b.fieldEncoders, pk, oldAny, record); err != nil {
			return err
		}
	}

	if err := tx.btx.Set(dKey, val); err != nil {
		return err
	}

	// FTS postings are written inside the same Badger txn as the
	// data: a rollback discards them, a commit makes them visible
	// atomically with the record itself.
	if b.ftsIdx != nil {
		if err := b.ftsIdx.writeDoc(tx.btx, pk, record); err != nil {
			return fmt.Errorf("bw: fts write: %w", err)
		}
	}

	// Vector field, same atomicity story.
	if b.vecIdx != nil {
		v := b.vecIdx.extractVector(record)
		if v == nil && b.embedder != nil {
			ev, eErr := b.embedder(context.Background(), record)
			if eErr != nil {
				return fmt.Errorf("bw: embedder: %w", eErr)
			}
			v = ev
		}
		if v != nil {
			if err := b.vecIdx.writeVec(tx.btx, pk, v); err != nil {
				return fmt.Errorf("bw: vector write: %w", err)
			}
		}
	}

	return nil
}

// Get retrieves the record stored under key. Returns ErrNotFound if there
// is no such key.
//
// key may be string, []byte, an integer, or any value matching the type of
// the pk field; non-string types are encoded the same way as during
// Insert.
func (b *Bucket[T]) Get(ctx context.Context, key any) (*T, error) {
	_ = ctx

	var out *T
	err := b.db.View(func(tx *Tx) error {
		rec, err := b.getTx(tx, key)
		if err != nil {
			return err
		}
		out = rec
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetTx is like Get but operates on a caller-controlled transaction.
// Use it when a downstream Insert/Update/Delete needs to observe writes
// that have not yet been committed: the bucket's standalone Get opens
// its own read-only view, and Badger's MVCC isolation hides in-flight
// writes from that view.
func (b *Bucket[T]) GetTx(tx *Tx, key any) (*T, error) {
	if tx == nil {
		return nil, fmt.Errorf("bw: nil transaction")
	}
	return b.getTx(tx, key)
}

// getTx is the shared implementation for Get and GetTx. The caller
// must supply the transaction; both rw and read-only txs are accepted.
func (b *Bucket[T]) getTx(tx *Tx, key any) (*T, error) {
	pk, err := keyAsBytes(key)
	if err != nil {
		return nil, err
	}

	item, err := tx.btx.Get(dataKey(b.name, pk))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	out := new(T)
	if err := item.Value(func(val []byte) error {
		return b.codec.Unmarshal(val, out)
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// Delete removes the record with the given key, plus every index/unique
// entry attached to it. The call is idempotent: deleting a missing key
// returns nil.
func (b *Bucket[T]) Delete(ctx context.Context, key any) error {
	_ = ctx
	pk, err := keyAsBytes(key)
	if err != nil {
		return err
	}

	return b.db.Update(func(tx *Tx) error {
		dKey := dataKey(b.name, pk)
		if b.hasMaintenance {
			item, gerr := tx.btx.Get(dKey)
			switch {
			case gerr == nil:
				old := new(T)
				if err := item.Value(func(val []byte) error {
					return b.codec.Unmarshal(val, old)
				}); err != nil {
					return err
				}
				if err := deleteIndexes(tx.btx, b.name, b.fieldEncoders, pk, old); err != nil {
					return err
				}
			case errors.Is(gerr, badger.ErrKeyNotFound):
				return nil
			default:
				return gerr
			}
		}

		if err := tx.btx.Delete(dKey); err != nil {
			return err
		}

		if b.ftsIdx != nil {
			if err := b.ftsIdx.deleteDoc(tx.btx, pk); err != nil {
				return fmt.Errorf("bw: fts delete: %w", err)
			}
		}
		if b.vecIdx != nil {
			if err := b.vecIdx.deleteVec(tx.btx, pk); err != nil {
				return fmt.Errorf("bw: vector delete: %w", err)
			}
		}

		return nil
	})
}

// Find scans the bucket and returns every record matching q. Sort,
// Offset and Limit from q are honoured. q may be nil (returns every
// record).
//
// When q's top-level Where contains an equality, IN-list, or range
// comparison on an `index`-tagged field, Find executes via an index
// seek and falls back to a full scan only for the residual filter.
//
// Sort is performed on the typed *T slice rather than at the engine
// layer, so the codec only decodes each value once.
func (b *Bucket[T]) Find(ctx context.Context, q *query.Query) ([]*T, error) {
	_ = ctx

	var results []*T
	err := b.db.View(func(tx *Tx) error {
		out, err := b.findTx(tx, q)
		if err != nil {
			return err
		}
		results = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// FindTx is like Find but operates on a caller-controlled transaction.
// Sort/Offset/Limit/Select from q are honoured. Use this when the
// caller is mid-transaction and needs to observe pending writes.
func (b *Bucket[T]) FindTx(tx *Tx, q *query.Query) ([]*T, error) {
	if tx == nil {
		return nil, fmt.Errorf("bw: nil transaction")
	}
	return b.findTx(tx, q)
}

// findTx is the shared implementation for Find and FindTx.
func (b *Bucket[T]) findTx(tx *Tx, q *query.Query) ([]*T, error) {
	// Strip Sort/Offset/Limit from the engine's view of q: we apply
	// them on the typed slice ourselves so each record is decoded only
	// once.
	scanQ, sortSpec, offset, limit := splitSortPaging(q)

	matches, err := b.scan(tx.btx, scanQ)
	if err != nil {
		return nil, err
	}

	results := make([]*T, 0, len(matches))
	for _, m := range matches {
		rec := new(T)
		if err := b.codec.Unmarshal(m.Value, rec); err != nil {
			return nil, err
		}
		results = append(results, rec)
	}

	if len(sortSpec) > 0 {
		b.sortTyped(results, sortSpec)
	}
	results = applyOffsetLimitTyped(results, offset, limit)
	return results, nil
}

// Walk streams matching records to fn. Sort in q is ignored; Offset and
// Limit are honoured.
func (b *Bucket[T]) Walk(ctx context.Context, q *query.Query, fn func(*T) error) error {
	_ = ctx
	return b.db.View(func(tx *Tx) error {
		return b.walkTx(tx, q, fn)
	})
}

// WalkTx is like Walk but operates on a caller-controlled transaction.
func (b *Bucket[T]) WalkTx(tx *Tx, q *query.Query, fn func(*T) error) error {
	if tx == nil {
		return fmt.Errorf("bw: nil transaction")
	}
	return b.walkTx(tx, q, fn)
}

// walkTx is the shared implementation for Walk and WalkTx.
func (b *Bucket[T]) walkTx(tx *Tx, q *query.Query, fn func(*T) error) error {
	plan := b.plan(q)
	walkFn := func(m engine.Match) error {
		rec := new(T)
		if err := b.codec.Unmarshal(m.Value, rec); err != nil {
			return err
		}
		return fn(rec)
	}
	switch plan.Kind {
	case engine.PlanIndexEq, engine.PlanIndexRange:
		return engine.IndexWalk(tx.btx, b.indexScanOpts(plan, q), walkFn)
	default:
		return engine.Walk(tx.btx, engine.ScanOptions{
			Prefix: dataPrefix(b.name),
			Codec:  b.codec,
			Query:  withWhere(q, plan.ResidualWhere),
		}, walkFn)
	}
}

// scan picks a plan and returns the matching set.
func (b *Bucket[T]) scan(btx *badger.Txn, q *query.Query) ([]engine.Match, error) {
	plan := b.plan(q)
	switch plan.Kind {
	case engine.PlanIndexEq, engine.PlanIndexRange:
		return engine.IndexScan(btx, b.indexScanOpts(plan, q))
	default:
		return engine.FullScan(btx, engine.ScanOptions{
			Prefix: dataPrefix(b.name),
			Codec:  b.codec,
			Query:  withWhere(q, plan.ResidualWhere),
		})
	}
}

// plan builds an execution plan from the bucket's precomputed indexable
// fields. The slice is built once at registration time so this method is
// allocation-free apart from the plan's own residual slice.
func (b *Bucket[T]) plan(q *query.Query) engine.Plan {
	return engine.PlanQuery(q, b.indexedQueryFields, b.compositeQueryFields...)
}

func (b *Bucket[T]) indexScanOpts(plan engine.Plan, q *query.Query) engine.IndexScanOptions {
	return engine.IndexScanOptions{
		DataPrefix:       dataPrefix(b.name),
		IndexFieldPrefix: indexFieldPrefix(b.name, plan.IndexField),
		Codec:            b.codec,
		Plan:             plan,
		Query:            q,
	}
}

// withWhere returns a shallow clone of q with its Where replaced. q may be
// nil. The clone keeps Sort/Offset/Limit/Select.
func withWhere(q *query.Query, where []query.Expression) *query.Query {
	if q == nil {
		if len(where) == 0 {
			return nil
		}
		return &query.Query{Where: where}
	}
	clone := *q
	clone.Where = where

	return &clone
}

// Count scans the bucket and returns the number of records matching q.
// Offset and Limit from q are ignored; Sort is ignored.
func (b *Bucket[T]) Count(ctx context.Context, q *query.Query) (uint64, error) {
	_ = ctx
	var n uint64
	err := b.db.View(func(tx *Tx) error {
		count, err := b.countTx(tx, q)
		n = count
		return err
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// CountTx is like Count but operates on a caller-controlled transaction.
func (b *Bucket[T]) CountTx(tx *Tx, q *query.Query) (uint64, error) {
	if tx == nil {
		return 0, fmt.Errorf("bw: nil transaction")
	}
	return b.countTx(tx, q)
}

// countTx is the shared implementation for Count and CountTx.
func (b *Bucket[T]) countTx(tx *Tx, q *query.Query) (uint64, error) {
	// Drop offset/limit/sort while counting; Count's contract ignores
	// those even when present in q.
	var qq *query.Query
	if q != nil {
		clone := *q
		clone.Offset = nil
		clone.Limit = nil
		clone.Sort = nil
		qq = &clone
	}

	var n uint64
	err := engine.Walk(tx.btx, engine.ScanOptions{
		Prefix: dataPrefix(b.name),
		Codec:  b.codec,
		Query:  qq,
	}, func(engine.Match) error {
		n++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// keyAsBytes accepts the common ways callers pass a key and produces the
// byte form used in the data prefix.
func keyAsBytes(k any) ([]byte, error) {
	switch v := k.(type) {
	case nil:
		return nil, fmt.Errorf("bw: nil key")
	case string:
		return []byte(v), nil
	case []byte:
		out := make([]byte, len(v))
		copy(out, v)

		return out, nil
	}

	return pkBytes(reflect.ValueOf(k))
}

// SearchResult holds a matched record along with its relevance score.
type SearchResult[T any] struct {
	Record *T
	Score  float64
}

// Search performs a full-text search over the bucket's FTS-tagged fields
// using Bleve's query string syntax. It returns matched records hydrated
// from BadgerDB, ordered by relevance score (highest first).
//
// Returns ErrNoFTS if the bucket has no FTS-tagged fields.
// limit and offset control pagination (0 limit means default 10).
func (b *Bucket[T]) Search(ctx context.Context, query string, limit, offset int) ([]SearchResult[T], uint64, error) {
	_ = ctx

	if b.ftsIdx == nil {
		return nil, 0, ErrNoFTS
	}

	if limit <= 0 {
		limit = 10
	}

	hits, total, err := b.ftsIdx.search(b.db, query, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("bw: search: %w", err)
	}

	results := make([]SearchResult[T], 0, len(hits))
	err = b.db.View(func(tx *Tx) error {
		for _, h := range hits {
			pk := []byte(h.ID)
			item, getErr := tx.btx.Get(dataKey(b.name, pk))
			if getErr != nil {
				if errors.Is(getErr, badger.ErrKeyNotFound) {
					continue
				}
				return getErr
			}
			rec := new(T)
			if err := item.Value(func(val []byte) error {
				return b.codec.Unmarshal(val, rec)
			}); err != nil {
				return err
			}
			results = append(results, SearchResult[T]{Record: rec, Score: h.Score})
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return results, total, nil
}

// SearchVector returns the top-K records whose vector field is closest
// to q under the configured distance metric. The bucket must declare a
// `vector`-tagged field at registration time; otherwise [ErrNoVector]
// is returned. Stage A implementation is brute-force over all
// non-deleted vectors; recall is exact.
//
// opts is variadic for ergonomics: SearchVector(ctx, q) takes the
// schema defaults (K=10, schema metric, no filter). Pass at most one
// SearchVectorOptions to override.
//
// When opts.Filter is non-nil it must be a *query.Query; the bucket
// resolves it via the ordinary Find path (so any indexable predicate
// the planner understands is fast) and the vector pass only sees pks
// that survived the filter.
func (b *Bucket[T]) SearchVector(ctx context.Context, q []float32, opts ...SearchVectorOptions) ([]VectorHit[T], error) {
	_ = ctx

	if b.vecIdx == nil {
		return nil, ErrNoVector
	}
	if len(q) == 0 {
		return nil, ErrVectorEmpty
	}

	var so SearchVectorOptions
	if len(opts) > 0 {
		so = opts[0]
	}
	if so.K <= 0 {
		so.K = 10
	}
	metric := so.Metric
	if metric == MetricDefault {
		metric = b.vecIdx.defaultM
	}

	// Resolve the filter into a pk allow-set when present. We use
	// the typed Find path so indexed predicates are cheap.
	var allowed map[string]struct{}
	if so.Filter != nil {
		qq, ok := so.Filter.(*query.Query)
		if !ok {
			return nil, fmt.Errorf("bw: SearchVector Filter must be *query.Query, got %T", so.Filter)
		}
		matches, err := b.Find(ctx, qq)
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return nil, nil
		}
		allowed = make(map[string]struct{}, len(matches))
		for _, m := range matches {
			pk, kerr := b.keyFn(m)
			if kerr != nil {
				return nil, kerr
			}
			allowed[string(pk)] = struct{}{}
		}
	}

	out := make([]VectorHit[T], 0, so.K)
	err := b.vecIdx.search(b.db, q, so.K, so.EfSearch, metric, allowed, func(pk []byte, score float64) error {
		// Hydrate the record. Use a fresh read txn per call rather
		// than a shared one so search results stay close to the
		// committed view at the moment SearchVector returned.
		return b.db.View(func(tx *Tx) error {
			item, gErr := tx.btx.Get(dataKey(b.name, pk))
			if gErr != nil {
				if errors.Is(gErr, badger.ErrKeyNotFound) {
					return nil
				}
				return gErr
			}
			rec := new(T)
			if vErr := item.Value(func(val []byte) error {
				return b.codec.Unmarshal(val, rec)
			}); vErr != nil {
				return vErr
			}
			out = append(out, VectorHit[T]{Record: rec, Score: score})
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("bw: vector search: %w", err)
	}
	return out, nil
}
