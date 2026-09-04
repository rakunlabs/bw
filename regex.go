package bw

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// Regular-expression search over a `trigram`-tagged field.
//
// The trigram index only ever narrows the candidate set; the compiled
// regexp run over each candidate's stored value is what decides a match
// and produces its offsets. A pattern the planner cannot constrain
// (`.*`, a bare character class) still returns exactly the right answer,
// it just pays a bucket scan to do it.
//
// Results arrive in primary-key order rather than in relevance order:
// there is no score to rank by, and a key-ordered walk of a chunked
// index groups a file's matches together, which is what a caller
// rendering them wants.

// regexDefaultMaxMatches bounds the offsets reported per record. A
// minified line can match thousands of times; a caller rendering hits
// needs the first few and the fact that there were more.
const regexDefaultMaxMatches = 16

// RegexMatch is one occurrence of the pattern inside the searched field.
type RegexMatch struct {
	// Start and End are byte offsets into the field value.
	Start int
	// End is exclusive.
	End int
	// Line is the 1-based line number containing Start.
	Line int
}

// RegexResult is one matched record together with where it matched.
type RegexResult[T any] struct {
	// ID is the record's primary key, string-coerced.
	ID string
	// Record is the hydrated record.
	Record *T
	// Matches lists up to RegexOptions.MaxMatches occurrences, in order.
	Matches []RegexMatch
	// Truncated reports that the record had more matches than were
	// reported.
	Truncated bool
}

// RegexOptions configures Bucket.RegexSearch / Bucket.RegexWalk.
type RegexOptions struct {
	// Field names the trigram-indexed field to search. It may be empty
	// only when the bucket declares exactly one such field.
	Field string
	// CaseSensitive matches case exactly. The default is
	// case-insensitive, which costs nothing extra: the index is folded
	// either way and only the compiled regexp differs.
	CaseSensitive bool
	// Limit caps the number of records handed to the callback (0 means
	// no limit for RegexWalk, 10 for RegexSearch). The returned total is
	// unaffected.
	Limit int
	// Offset skips this many matching records before delivering any.
	Offset int
	// KeyFilter, when non-nil, rejects candidates by primary key before
	// the record is read. Use it to scope a multi-tenant or chunked
	// index to one partition: a rejected candidate costs a function call
	// instead of a point read plus a decode plus a regexp run.
	KeyFilter func(id string) bool
	// MaxMatches caps the offsets reported per record (0 uses 16).
	// Reaching the cap sets Truncated on the result; it never changes
	// whether the record matched.
	MaxMatches int
	// MaxCandidates caps how many document ids the prefilter will
	// materialise before giving up on it and scanning the bucket
	// instead (0 uses 250 000). Past that point the scan is both cheaper
	// and more predictable.
	MaxCandidates int
}

// RegexSearch returns the records whose trigram-indexed field matches
// pattern, in primary-key order, together with the total number of
// matching records.
//
// The total is exact and independent of Limit and Offset: every
// candidate is verified, so counting is what the search already did.
func (b *Bucket[T]) RegexSearch(ctx context.Context, pattern string, opts RegexOptions) ([]RegexResult[T], uint64, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}
	opts.Limit = limit

	out := make([]RegexResult[T], 0, min(limit, searchPrealloc))
	total, err := b.RegexWalk(ctx, pattern, opts, func(hit RegexResult[T]) (bool, error) {
		out = append(out, hit)

		return true, nil
	})
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

// RegexWalk streams matching records to fn in primary-key order and
// returns the total number of records that matched.
//
// fn reports whether to continue. Stopping early stops the verification
// pass with it, so a caller that wants the first page pays for the first
// page — but then the returned total counts only what was verified. Ask
// for the exact total by walking to the end (Limit bounds delivery, not
// verification).
func (b *Bucket[T]) RegexWalk(ctx context.Context, pattern string, opts RegexOptions, fn func(RegexResult[T]) (bool, error)) (uint64, error) {
	if err := b.checkCurrent(); err != nil {
		return 0, err
	}
	if b.triIdx == nil {
		return 0, ErrNoTrigram
	}
	if ctx == nil {
		ctx = context.Background()
	}

	field, err := b.triIdx.resolveField(opts.Field)
	if err != nil {
		return 0, err
	}

	// The same effective pattern feeds the planner and the matcher, so
	// the constraint the index applies can never disagree with the
	// expression that decides the match.
	effective := pattern
	if !opts.CaseSensitive {
		effective = "(?i)" + pattern
	}
	re, err := regexp.Compile(effective)
	if err != nil {
		return 0, fmt.Errorf("bw: regex search: %w", err)
	}
	plan, err := regexTrigramQuery(effective)
	if err != nil {
		return 0, fmt.Errorf("bw: regex search: plan: %w", err)
	}

	maxMatches := opts.MaxMatches
	if maxMatches <= 0 {
		maxMatches = regexDefaultMaxMatches
	}

	var total uint64
	err = b.db.View(func(tx *Tx) error {
		if err := b.checkCurrent(); err != nil {
			return err
		}

		docs, all, err := b.triIdx.resolve(ctx, tx.btx, field.Name, plan, opts.MaxCandidates)
		if err != nil {
			return fmt.Errorf("bw: regex search: %w", err)
		}

		state := &regexWalk[T]{
			bucket:     b,
			ctx:        ctx,
			re:         re,
			fieldName:  field.Name,
			maxMatches: maxMatches,
			keyFilter:  opts.KeyFilter,
			limit:      opts.Limit,
			skip:       opts.Offset,
			fn:         fn,
		}
		defer func() { total = state.total }()

		if all {
			return state.scan(tx)
		}

		return state.candidates(tx, docs)
	})
	if err != nil {
		return total, err
	}

	return total, nil
}

// regexWalk carries the per-query verification state. It exists so the
// candidate path and the scan path share one record-verification body.
type regexWalk[T any] struct {
	bucket     *Bucket[T]
	ctx        context.Context
	re         *regexp.Regexp
	fieldName  string
	maxMatches int
	keyFilter  func(string) bool

	limit int
	skip  int

	fn        func(RegexResult[T]) (bool, error)
	total     uint64
	delivered int
	stopped   bool
}

// scan verifies every record in the bucket, in key order. This is the
// plan for a pattern with no usable literal, and the fallback when the
// candidate set grows past the point where reading it is cheaper.
func (w *regexWalk[T]) scan(tx *Tx) error {
	prefix := dataPrefix(w.bucket.name)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := tx.btx.NewIterator(opts)
	defer it.Close()

	n := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if n%ctxCheckInterval == 0 {
			if err := ctxErr(w.ctx); err != nil {
				return err
			}
		}
		n++

		item := it.Item()
		pk := string(item.Key()[len(prefix):])
		if w.keyFilter != nil && !w.keyFilter(pk) {
			continue
		}
		var raw []byte
		if err := item.Value(func(val []byte) error {
			raw = append(raw[:0], val...)

			return nil
		}); err != nil {
			return err
		}
		done, err := w.verify(pk, raw)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}

	return nil
}

// candidates verifies the prefiltered document ids. Ids are resolved to
// primary keys first and then sorted, so delivery order matches the scan
// path's: a caller cannot tell which plan ran except by how long it took.
func (w *regexWalk[T]) candidates(tx *Tx, docs []uint32) error {
	pks := make([]string, 0, len(docs))
	for i, docID := range docs {
		if i%ctxCheckInterval == 0 {
			if err := ctxErr(w.ctx); err != nil {
				return err
			}
		}
		pk, ok, err := w.bucket.triIdx.pkOf(tx.btx, docID)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		id := string(pk)
		if w.keyFilter != nil && !w.keyFilter(id) {
			continue
		}
		pks = append(pks, id)
	}
	slices.Sort(pks)

	for i, pk := range pks {
		if i%ctxCheckInterval == 0 {
			if err := ctxErr(w.ctx); err != nil {
				return err
			}
		}
		item, err := tx.btx.Get(dataKey(w.bucket.name, []byte(pk)))
		if err != nil {
			// A posting outlives its record only between a delete and
			// the compaction that removes it; skipping is correct.
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}

			return err
		}
		var raw []byte
		if err := item.Value(func(val []byte) error {
			raw = append(raw[:0], val...)

			return nil
		}); err != nil {
			return err
		}
		done, err := w.verify(pk, raw)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}

	return nil
}

// verify decodes one candidate, runs the regexp over its field and, when
// it matches, counts it and possibly delivers it. done reports that the
// walk should stop.
func (w *regexWalk[T]) verify(pk string, raw []byte) (bool, error) {
	rec := new(T)
	if err := w.bucket.codec.Unmarshal(raw, rec); err != nil {
		return false, err
	}
	fv, ok := w.bucket.schema.FieldValue(rec, w.fieldName)
	if !ok || fv.Kind() != reflect.String {
		return false, nil
	}
	text := fv.String()

	matches, truncated := regexMatches(w.re, text, w.maxMatches)
	if len(matches) == 0 {
		return false, nil
	}
	w.total++

	if w.skip > 0 {
		w.skip--

		return false, nil
	}
	if w.stopped {
		return false, nil
	}
	if w.limit > 0 && w.delivered >= w.limit {
		w.stopped = true

		return false, nil
	}

	cont, err := w.fn(RegexResult[T]{ID: pk, Record: rec, Matches: matches, Truncated: truncated})
	if err != nil {
		return false, err
	}
	w.delivered++
	if !cont {
		return true, nil
	}

	return false, nil
}

// regexMatches returns up to max match offsets with their line numbers.
//
// Line numbers are counted incrementally across the matches rather than
// recomputed per match, so a record with many hits costs one pass over
// its value.
func regexMatches(re *regexp.Regexp, text string, max int) ([]RegexMatch, bool) {
	locs := re.FindAllStringIndex(text, max+1)
	if len(locs) == 0 {
		return nil, false
	}
	truncated := len(locs) > max
	if truncated {
		locs = locs[:max]
	}

	out := make([]RegexMatch, 0, len(locs))
	line := 1
	cursor := 0
	for _, loc := range locs {
		line += strings.Count(text[cursor:loc[0]], "\n")
		cursor = loc[0]
		out = append(out, RegexMatch{Start: loc[0], End: loc[1], Line: line})
	}

	return out, truncated
}
