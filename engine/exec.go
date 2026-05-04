package engine

import (
	"bytes"
	"errors"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw/codec"
	"github.com/rakunlabs/query"
)

// Match is a single record matched by a scan: it carries the raw key/value
// bytes and the lazily-decoded map used for filter evaluation.
type Match struct {
	Key   []byte
	Value []byte
	// Decoded is the map produced by codec.UnmarshalMap; reuse it to avoid
	// re-decoding when projecting fields or sorting.
	Decoded map[string]any
}

// ScanOptions controls a full-scan over a bucket prefix.
type ScanOptions struct {
	// Prefix is the byte prefix every scanned key must share (e.g.
	// "<bucket>\x00").
	Prefix []byte
	// Codec decodes record values for filter evaluation.
	Codec codec.Codec
	// Query supplies Where/Sort/Offset/Limit. May be nil.
	Query *query.Query
}

// IndexScanOptions controls an index-driven scan. The executor walks the
// index keyspace described by Plan to recover candidate primary keys,
// fetches each record from the data prefix, applies the residual filter,
// and honours sort/offset/limit.
type IndexScanOptions struct {
	// DataPrefix is the bucket data prefix ("<bucket>\x00").
	DataPrefix []byte
	// IndexFieldPrefix is the prefix shared by every index entry on the
	// chosen field ("\x00idx\x00<bucket>\x00<field>\x00").
	IndexFieldPrefix []byte
	// Codec decodes record values when the residual filter or the typed
	// caller needs the record materialised.
	Codec codec.Codec
	// Plan is the chosen execution plan (must be PlanIndexEq or
	// PlanIndexRange).
	Plan Plan
	// Query supplies Sort/Offset/Limit; Where is supplied via
	// Plan.ResidualWhere instead.
	Query *query.Query
}

// FullScan iterates every key under opts.Prefix, applies the Where filter
// from opts.Query, and returns the matching records.
//
// Sort, Offset and Limit on opts.Query are NOT honoured here. Callers
// (notably Bucket.Find) strip those out and apply them on the typed
// result slice so each record gets decoded only once. The leftover
// helpers (sortMatches, applyOffsetLimit) remain for the index-driven
// path that still needs them when Sort isn't index-handled.
func FullScan(txn *badger.Txn, opts ScanOptions) ([]Match, error) {
	return collectAll(txn, opts)
}

// Walk iterates every key under opts.Prefix, applies the Where filter, and
// invokes fn for each match. Sort is ignored (callers wanting sorted
// output must use FullScan); Offset/Limit are honoured.
//
// Records that fail the residual filter are evaluated against a borrowed
// slice (Badger's mmap'd bytes), so they never pay the value-copy cost.
// The data bytes are copied into a fresh slice only when the record will
// actually be delivered to fn.
func Walk(txn *badger.Txn, opts ScanOptions, fn func(Match) error) error {
	skip, limit := offsetLimit(opts.Query)
	delivered := uint64(0)

	var residual []query.Expression
	if opts.Query != nil {
		residual = opts.Query.Where
	}
	ctx := newScanCtx(opts.Codec, residual)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	var stop bool
	var stopErr error

	for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)

		err := item.Value(func(borrowed []byte) error {
			if len(ctx.residual) > 0 {
				decoded, err := ctx.decodeForFilter(borrowed)
				if err != nil {
					return err
				}
				ok, eerr := Eval(decoded, ctx.residual)
				if eerr != nil {
					return eerr
				}
				if !ok {
					return nil
				}
			}

			if skip > 0 {
				skip--

				return nil
			}

			// Promote the borrowed slice to a stable copy now that the
			// record is actually being delivered.
			val := append([]byte(nil), borrowed...)
			if cbErr := fn(Match{Key: key, Value: val}); cbErr != nil {
				stopErr = cbErr
				stop = true

				return nil
			}
			delivered++
			if limit > 0 && delivered >= limit {
				stop = true
			}

			return nil
		})
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}

	return stopErr
}

// IndexScan executes an index-driven plan and returns all matching
// records. Sort, Offset and Limit from opts.Query are honoured.
func IndexScan(txn *badger.Txn, opts IndexScanOptions) ([]Match, error) {
	needSort := !opts.Plan.SortHandled && opts.Query != nil && len(opts.Query.Sort) > 0
	collected, err := collectIndexWithSortFields(txn, opts, needSort)
	if err != nil {
		return nil, err
	}

	// Already ordered by index when SortHandled.
	if needSort {
		sortMatches(collected, opts.Query.Sort)
	}
	collected = applyOffsetLimit(collected, opts.Query)

	return collected, nil
}

// IndexWalk streams matching records from an index plan to fn. Sort is
// ignored; Offset/Limit are honoured.
func IndexWalk(txn *badger.Txn, opts IndexScanOptions, fn func(Match) error) error {
	skip, limit := offsetLimit(opts.Query)
	delivered := uint64(0)

	return iterIndex(txn, opts, func(m Match) (bool, error) {
		if skip > 0 {
			skip--

			return true, nil
		}
		if err := fn(m); err != nil {
			return false, err
		}
		delivered++
		if limit > 0 && delivered >= limit {
			return false, nil
		}

		return true, nil
	})
}

// collectIndexWithSortFields runs the index plan and accumulates every
// match. When needSort is true, each Match.Decoded is populated with the
// fields the sort comparator needs so sortMatches can read them.
func collectIndexWithSortFields(txn *badger.Txn, opts IndexScanOptions, needSort bool) ([]Match, error) {
	ctx := newScanCtx(opts.Codec, opts.Plan.ResidualWhere)
	if needSort && opts.Query != nil {
		sortFields := make([]string, 0, len(opts.Query.Sort))
		for _, s := range opts.Query.Sort {
			sortFields = append(sortFields, topField(s.Field))
		}
		ctx.sortFields = sortFields
		// Make sure the lazy decode (when used for residual) also returns
		// the sort fields, so we don't need a second decode round.
		if ctx.lazy != nil {
			if len(ctx.lazyFields) == 0 {
				ctx.lazyFields = sortFields
			} else {
				ctx.lazyFields = mergeUnique(ctx.lazyFields, sortFields)
			}
		}
	}

	var out []Match
	err := iterIndexWith(txn, opts, ctx, func(m Match) (bool, error) {
		out = append(out, m)

		return true, nil
	})

	return out, err
}

// iterIndex walks the index keys described by the plan, fetches each
// referenced data record, runs the residual filter, and invokes cb for
// every matching record. cb returns (continue, error) — stop iterating
// when continue is false.
//
// Decoding strategy: only records that the residual filter actually needs
// get unmarshalled, and only the fields the residual references are
// pulled out of the codec when it implements codec.LazyCodec.
func iterIndex(txn *badger.Txn, opts IndexScanOptions, cb func(Match) (bool, error)) error {
	return iterIndexWith(txn, opts, newScanCtx(opts.Codec, opts.Plan.ResidualWhere), cb)
}

func iterIndexWith(txn *badger.Txn, opts IndexScanOptions, ctx *scanCtx, cb func(Match) (bool, error)) error {
	if opts.Plan.Kind == PlanIndexEq {
		for _, val := range opts.Plan.IndexValues {
			pfx := append(append([]byte{}, opts.IndexFieldPrefix...), encodeLP(val)...)
			cont, err := iterIndexPrefix(txn, opts, pfx, ctx, cb)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}

		return nil
	}

	if opts.Plan.Kind == PlanIndexRange {
		return iterIndexRange(txn, opts, ctx, cb)
	}

	return errors.New("engine: iterIndex called with non-index plan")
}

// scanCtx carries the precomputed residual-eval state so we don't recompute
// it per record.
type scanCtx struct {
	residual []query.Expression
	// lazyFields, when non-empty, lists the top-level keys the residual
	// references — passed to codec.LazyCodec.UnmarshalFields when the
	// codec supports it.
	lazyFields []string
	// sortFields are the top-level keys the sort comparator needs. They
	// trigger a partial decode even when there's no residual to evaluate.
	sortFields []string
	lazy       codec.LazyCodec
	codec      codec.Codec
}

func newScanCtx(c codec.Codec, residual []query.Expression) *scanCtx {
	ctx := &scanCtx{
		residual: residual,
		codec:    c,
	}
	if len(residual) > 0 {
		ctx.lazyFields = CollectFields(residual)
		if lc, ok := c.(codec.LazyCodec); ok {
			ctx.lazy = lc
		}
	}

	return ctx
}

// decodeForFilter returns a map containing the fields the residual filter
// needs, or the entire record's top-level if the codec doesn't expose a
// lazy path.
func (s *scanCtx) decodeForFilter(val []byte) (map[string]any, error) {
	if s.lazy != nil {
		return s.lazy.UnmarshalFields(val, s.lazyFields)
	}

	return s.codec.UnmarshalMap(val)
}

func iterIndexPrefix(txn *badger.Txn, opts IndexScanOptions, prefix []byte, ctx *scanCtx, cb func(Match) (bool, error)) (bool, error) {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	// Reusable scratch buffer for the data key. Capacity grows once and
	// is then reset between iterations.
	dKey := make([]byte, 0, len(opts.DataPrefix)+32)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()
		pk := pkSliceFromIndexKey(key, opts.IndexFieldPrefix)
		if pk == nil {
			continue
		}

		dKey = append(dKey[:0], opts.DataPrefix...)
		dKey = append(dKey, pk...)
		cont, err := loadAndCallback(txn, ctx, dKey, cb)
		if err != nil {
			return false, err
		}
		if !cont {
			return false, nil
		}
	}

	return true, nil
}

func iterIndexRange(txn *badger.Txn, opts IndexScanOptions, ctx *scanCtx, cb func(Match) (bool, error)) error {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	// Compose start key: field prefix + (lo if bounded, else nothing).
	startKey := append([]byte{}, opts.IndexFieldPrefix...)
	if !opts.Plan.LoOpen && len(opts.Plan.Lo) > 0 {
		startKey = append(startKey, encodeLP(opts.Plan.Lo)...)
	}

	hiBound := opts.Plan.Hi
	hiInclusive := opts.Plan.HiInclusive
	hiOpen := opts.Plan.HiOpen

	dKey := make([]byte, 0, len(opts.DataPrefix)+32)

	for it.Seek(startKey); it.ValidForPrefix(opts.IndexFieldPrefix); it.Next() {
		key := it.Item().Key()
		// Decode the embedded value to compare against bounds. Strip the
		// field-prefix and parse the uvarint-prefixed value.
		rest := key[len(opts.IndexFieldPrefix):]
		valLen, n := uvarintRead(rest)
		if n <= 0 || uint64(len(rest)-n) < valLen {
			continue
		}
		entryVal := rest[n : n+int(valLen)]

		if !opts.Plan.LoOpen && len(opts.Plan.Lo) > 0 {
			c := bytes.Compare(entryVal, opts.Plan.Lo)
			if c < 0 || (c == 0 && !opts.Plan.LoInclusive) {
				continue
			}
		}
		if !hiOpen && len(hiBound) > 0 {
			c := bytes.Compare(entryVal, hiBound)
			if c > 0 || (c == 0 && !hiInclusive) {
				return nil
			}
		}

		pk := rest[n+int(valLen):]
		dKey = append(dKey[:0], opts.DataPrefix...)
		dKey = append(dKey, pk...)
		cont, err := loadAndCallback(txn, ctx, dKey, cb)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}

	return nil
}

// loadAndCallback fetches the record at dKey, runs the residual filter,
// and invokes cb. dKey is the caller-owned scratch buffer; the callback
// receives a fresh copy (the bucket's typed callback will further copy
// into a *T anyway).
func loadAndCallback(txn *badger.Txn, ctx *scanCtx, dKey []byte, cb func(Match) (bool, error)) (bool, error) {
	item, err := txn.Get(dKey)
	if err != nil {
		// pk in index but not in data is corruption; skip rather than
		// abort the whole query.
		if errors.Is(err, badger.ErrKeyNotFound) {
			return true, nil
		}

		return false, err
	}
	// Copy the data key out of the caller's scratch buffer so the Match
	// remains valid after the next iteration overwrites the scratch.
	keyCopy := make([]byte, len(dKey))
	copy(keyCopy, dKey)

	var (
		passed     bool
		stableVal  []byte
		decodedMap map[string]any
	)
	err = item.Value(func(borrowed []byte) error {
		switch {
		case len(ctx.residual) > 0:
			decoded, derr := ctx.decodeForFilter(borrowed)
			if derr != nil {
				return derr
			}
			ok, eerr := Eval(decoded, ctx.residual)
			if eerr != nil {
				return eerr
			}
			if !ok {
				return nil
			}
			if len(ctx.sortFields) > 0 {
				decodedMap = decoded
			}
		case len(ctx.sortFields) > 0:
			decoded, derr := ctx.decodeSortFields(borrowed)
			if derr != nil {
				return derr
			}
			decodedMap = decoded
		}
		passed = true
		// Promote: only paid for surviving records.
		stableVal = append([]byte(nil), borrowed...)

		return nil
	})
	if err != nil {
		return false, err
	}
	if !passed {
		return true, nil
	}

	return cb(Match{Key: keyCopy, Value: stableVal, Decoded: decodedMap})
}

// decodeSortFields fetches just the sort-key fields when there's no
// residual filter to amortise against.
func (s *scanCtx) decodeSortFields(val []byte) (map[string]any, error) {
	if s.lazy != nil {
		return s.lazy.UnmarshalFields(val, s.sortFields)
	}

	return s.codec.UnmarshalMap(val)
}

// encodeLP writes a uvarint-prefixed copy of v.
func encodeLP(v []byte) []byte {
	var buf [10]byte
	n := putUvarint(buf[:], uint64(len(v)))
	out := make([]byte, 0, n+len(v))
	out = append(out, buf[:n]...)
	out = append(out, v...)

	return out
}

// putUvarint mirrors encoding/binary.PutUvarint without an extra import in
// this file (the package is already used elsewhere in the module).
func putUvarint(buf []byte, x uint64) int {
	i := 0
	for x >= 0x80 {
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	buf[i] = byte(x)

	return i + 1
}

// uvarintRead decodes a uvarint and returns (value, bytesRead). Mirrors
// encoding/binary.Uvarint's contract.
func uvarintRead(buf []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range buf {
		if i == 10 {
			return 0, -1
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, -1
			}

			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}

	return 0, 0
}

// pkSliceFromIndexKey returns a sub-slice of key pointing at the trailing
// pk bytes. The returned slice aliases the iterator's internal buffer, so
// callers must finish using it (typically copying into a data-key
// scratch) before advancing the iterator.
func pkSliceFromIndexKey(key, fieldPrefix []byte) []byte {
	if len(key) < len(fieldPrefix) {
		return nil
	}
	rest := key[len(fieldPrefix):]
	valLen, n := uvarintRead(rest)
	if n <= 0 {
		return nil
	}
	rest = rest[n:]
	if uint64(len(rest)) < valLen {
		return nil
	}

	return rest[valLen:]
}

func collectAll(txn *badger.Txn, opts ScanOptions) ([]Match, error) {
	var out []Match

	var residual []query.Expression
	if opts.Query != nil {
		residual = opts.Query.Where
	}
	ctx := newScanCtx(opts.Codec, residual)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)

		err := item.Value(func(borrowed []byte) error {
			if len(ctx.residual) > 0 {
				decoded, err := ctx.decodeForFilter(borrowed)
				if err != nil {
					return err
				}
				ok, eerr := Eval(decoded, ctx.residual)
				if eerr != nil {
					return eerr
				}
				if !ok {
					return nil
				}
			}

			// Stable copy: only paid for surviving records.
			val := append([]byte(nil), borrowed...)
			out = append(out, Match{Key: key, Value: val})

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// mergeUnique returns the union of a and b preserving order (a first,
// then b's new elements).
func mergeUnique(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, s := range a {
		seen[s] = struct{}{}
	}
	out := append([]string(nil), a...)
	for _, s := range b {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}

	return out
}

func sortMatches(matches []Match, sortSpec []query.ExpressionSort) {
	if len(sortSpec) == 0 {
		return
	}
	sort.SliceStable(matches, func(i, j int) bool {
		for _, s := range sortSpec {
			ai, _ := Lookup(matches[i].Decoded, s.Field)
			bj, _ := Lookup(matches[j].Decoded, s.Field)
			c, ok := compareValues(ai, bj)
			if !ok {
				// Fallback: compare string forms.
				as, bs := toString(ai), toString(bj)
				switch {
				case as < bs:
					c = -1
				case as > bs:
					c = 1
				default:
					c = 0
				}
			}
			if c == 0 {
				continue
			}
			if s.Desc {
				return c > 0
			}

			return c < 0
		}

		return false
	})
}

func offsetLimit(q *query.Query) (skip, limit uint64) {
	if q == nil {
		return 0, 0
	}

	return q.GetOffset(), q.GetLimit()
}

func applyOffsetLimit(in []Match, q *query.Query) []Match {
	skip, limit := offsetLimit(q)
	if skip == 0 && limit == 0 {
		return in
	}

	if skip >= uint64(len(in)) {
		return nil
	}
	in = in[skip:]
	if limit > 0 && uint64(len(in)) > limit {
		in = in[:limit]
	}

	return in
}
