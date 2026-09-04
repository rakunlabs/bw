package bw

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw/schema"
)

// Trigram search is the prefilter half of regular-expression search: the
// index narrows the corpus to the documents that *could* match, and the
// caller's regexp decides which of them actually do. It lives under the
// \x00tri\x00<bucket>\x00 namespace (see keys.go), so it inherits the
// same properties as the FTS index — a posting is written in the same
// Badger transaction as the record, and Backup/Restore carries it with
// no separate index directory.
//
// Every 3-byte window of a field's value is a term. ASCII letters are
// folded to lower case on both sides, so the index answers
// case-insensitive and case-sensitive queries alike, and the regexp run
// over the stored value is what enforces case. Byte folding is narrower
// than the Unicode simple folding `(?i)` applies, so a case-insensitive
// literal whose fold orbit leaves ASCII is refused a constraint rather
// than given a wrong one — see foldASCII and triFoldEscapesASCII.
//
// A field shorter than three bytes emits no trigrams. Such a document is
// invisible to a trigram-constrained query, which is sound because a
// query that constrains at all requires a literal run of three bytes
// that a shorter value cannot contain.

const (
	// triProbeCap bounds how far a posting list is walked when the
	// planner is only comparing sizes to pick an intersection base. A
	// list that reaches the cap is "big enough not to be the base"; the
	// exact figure never matters.
	triProbeCap = 512

	// triMaxVerify bounds how many of an AND's trigrams are checked per
	// candidate after the base list is chosen. Dropping the rest only
	// admits false candidates, which the regexp rejects, and it keeps a
	// long literal from costing a point read per trigram per candidate.
	triMaxVerify = 8

	// triDefaultMaxCandidates is the candidate ceiling above which the
	// prefilter stops paying for itself: materialising more document ids
	// than this costs more than scanning the bucket, and at that
	// selectivity the scan is also the more predictable plan. Mirrors the
	// brute-force fallback the vector index uses for the same reason.
	triDefaultMaxCandidates = 250_000
)

// ErrNoTrigram is returned when a regex search is issued against a
// bucket that declares no `trigram` field.
var ErrNoTrigram = errors.New("bw: bucket has no trigram-indexed field")

// triIndex bundles the per-bucket trigram configuration. Like ftsIndex it
// holds no live handle: every operation is a plain Badger read or write.
type triIndex struct {
	bucket string
	fields []*schema.Field // trigram-tagged fields
}

// openTrigramIndex returns the trigram handle for a bucket. It performs
// no I/O, so it cannot fail.
func openTrigramIndex(_ *DB, bucket string, fields []*schema.Field) (*triIndex, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	return &triIndex{bucket: bucket, fields: fields}, nil
}

// resolveField picks the field a search runs against. An empty name is
// allowed only when the bucket declares exactly one trigram field, so a
// caller can never silently search the wrong column.
func (ti *triIndex) resolveField(name string) (*schema.Field, error) {
	if name == "" {
		if len(ti.fields) != 1 {
			names := make([]string, 0, len(ti.fields))
			for _, f := range ti.fields {
				names = append(names, f.Name)
			}

			return nil, fmt.Errorf("bw: bucket %q has %d trigram fields %v; name the one to search", ti.bucket, len(ti.fields), names)
		}

		return ti.fields[0], nil
	}
	for _, f := range ti.fields {
		if f.Name == name {
			return f, nil
		}
	}

	return nil, fmt.Errorf("bw: field %q is not trigram-indexed on bucket %q", name, ti.bucket)
}

// ---------------------------------------------------------------------------
// Term extraction
// ---------------------------------------------------------------------------

// foldASCII lower-cases A-Z and leaves every other byte alone. Folding
// per byte rather than per rune is what keeps a trigram exactly three
// bytes wide, which is what makes the posting key fixed-width.
//
// This is the write side of an asymmetry, and it is deliberately the
// cheap side: it is applied to every byte of every indexed field. It is
// narrower than the Unicode simple folding a case-insensitive regexp
// uses, which also folds 'k' with U+212A KELVIN SIGN and 's' with
// U+017F LATIN SMALL LETTER LONG S. Widening the write side would mean
// folding runes, and a folded rune is not three bytes. The query side
// closes the gap instead by refusing to constrain on a folded literal
// whose fold orbit escapes ASCII — see triFoldEscapesASCII.
func foldASCII(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}

	return c
}

// trigramSet returns the case-folded trigrams of s, sorted and deduped.
// A string shorter than three bytes has none.
func trigramSet(s string) []uint32 {
	if len(s) < 3 {
		return nil
	}
	out := make([]uint32, 0, len(s)-2)
	a, b := foldASCII(s[0]), foldASCII(s[1])
	for i := 2; i < len(s); i++ {
		c := foldASCII(s[i])
		out = append(out, uint32(a)<<16|uint32(b)<<8|uint32(c))
		a, b = b, c
	}
	slices.Sort(out)

	return slices.Compact(out)
}

// fieldText reads a trigram field's value off a record. Non-string
// fields and nil records yield "", matching how the FTS index skips
// anything it cannot tokenise.
func fieldText(record any, f *schema.Field) string {
	if record == nil {
		return ""
	}
	rv := reflect.ValueOf(record)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return ""
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return ""
	}
	fv := rv.FieldByIndex(f.Index)
	if fv.Kind() != reflect.String {
		return ""
	}

	return fv.String()
}

// ---------------------------------------------------------------------------
// Update path
// ---------------------------------------------------------------------------

// writeDoc reconciles the postings of pk from oldRecord to newRecord.
//
// The previous term set is recomputed from the previous record rather
// than read from a back-index: unlike FTS, a trigram posting carries no
// payload, so the old value is all that is needed and a per-document
// term memo would double the key count of the largest index in the
// database. Callers must therefore pass the record that was stored under
// pk (nil when there was none) — bucket.upsertTx reads it for exactly
// this reason.
//
// What *is* stored per document is one marker key per tagged field,
// saying that field's postings exist (see triFieldDocKey): the diff is
// only correct against a term set that was actually written. That is
// one key per (document, tagged field) against the thousand-odd
// postings the same document emits, so the index's size story is
// unchanged — a term memo, at one key per distinct term, would not be.
func (ti *triIndex) writeDoc(btx *badger.Txn, pk []byte, oldRecord, newRecord any) error {
	return ti.applyDoc(btx, pk, oldRecord, newRecord)
}

// deleteDoc removes every posting oldRecord contributed, and with them
// the per-field markers that said those postings exist. The pk<->docid
// mapping is deliberately kept: re-indexing the same pk then reuses its
// id, which is the normal shape for a refresh that deletes and rewrites
// a repository's documents.
func (ti *triIndex) deleteDoc(btx *badger.Txn, pk []byte, oldRecord any) error {
	return ti.applyDoc(btx, pk, oldRecord, nil)
}

// fieldIndexed reports whether f's postings for docID were written. The
// question has to be asked per field, not per document: two tagged
// fields on one record are indexed independently, and a document whose
// second field only gains content on a later write has an interned id
// long before that field has a single posting.
func (ti *triIndex) fieldIndexed(btx *badger.Txn, field string, docID uint32) (bool, error) {
	_, err := btx.Get(triFieldDocKey(ti.bucket, field, docID))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return false, nil
	default:
		return false, err
	}
}

func (ti *triIndex) applyDoc(btx *badger.Txn, pk []byte, oldRecord, newRecord any) error {
	// A pk with no interned id has never been trigram-indexed at all, so
	// none of its fields can be. Reading the id up front also keeps the
	// marker lookups below to one point read per tagged field.
	docID, haveID, err := ti.docID(btx, pk, false)
	if err != nil {
		return err
	}

	type delta struct {
		field          string
		added, removed []uint32
		mark, unmark   bool
	}

	deltas := make([]delta, 0, len(ti.fields))
	needID := false
	for _, f := range ti.fields {
		// An unindexed field has no previous term set to diff against.
		// Taking the previous value as indexed instead would make a
		// *backfill* a no-op — adding the tag to an existing bucket
		// rewrites every record with an unchanged value, and diffing
		// that value against itself produces no postings at all — and
		// would drop the postings of a field that gains content after
		// the document was first indexed through another field.
		indexed := false
		if haveID {
			indexed, err = ti.fieldIndexed(btx, f.Name, docID)
			if err != nil {
				return err
			}
		}

		var oldSet []uint32
		if indexed {
			oldSet = trigramSet(fieldText(oldRecord, f))
		}
		newSet := trigramSet(fieldText(newRecord, f))
		added, removed := triDiff(oldSet, newSet)

		// The marker tracks exactly "this field has postings": set it
		// when the field starts emitting, clear it when it stops, so an
		// emptied field cannot keep claiming an old term set.
		mark := len(newSet) > 0 && !indexed
		unmark := len(newSet) == 0 && indexed
		if len(added) == 0 && len(removed) == 0 && !mark && !unmark {
			continue
		}
		if len(added) > 0 || mark {
			needID = true
		}
		deltas = append(deltas, delta{field: f.Name, added: added, removed: removed, mark: mark, unmark: unmark})
	}
	if len(deltas) == 0 {
		return nil
	}

	if !haveID {
		if !needID {
			return nil
		}
		docID, _, err = ti.docID(btx, pk, true)
		if err != nil {
			return err
		}
	}

	for _, d := range deltas {
		for _, tri := range d.removed {
			if err := btx.Delete(triPostingKey(ti.bucket, d.field, tri, docID)); err != nil {
				return err
			}
		}
		for _, tri := range d.added {
			if err := btx.Set(triPostingKey(ti.bucket, d.field, tri, docID), nil); err != nil {
				return err
			}
		}
		switch {
		case d.mark:
			if err := btx.Set(triFieldDocKey(ti.bucket, d.field, docID), nil); err != nil {
				return err
			}
		case d.unmark:
			if err := btx.Delete(triFieldDocKey(ti.bucket, d.field, docID)); err != nil {
				return err
			}
		}
	}

	return nil
}

// triDiff returns the elements of next missing from prev and vice versa.
// Both inputs must be sorted and deduped.
func triDiff(prev, next []uint32) (added, removed []uint32) {
	i, j := 0, 0
	for i < len(prev) && j < len(next) {
		switch {
		case prev[i] == next[j]:
			i++
			j++
		case prev[i] < next[j]:
			removed = append(removed, prev[i])
			i++
		default:
			added = append(added, next[j])
			j++
		}
	}
	removed = append(removed, prev[i:]...)
	added = append(added, next[j:]...)

	return added, removed
}

// docID returns pk's interned document id, allocating one when create is
// set. The second result reports whether an id exists.
//
// Allocation is a read-modify-write of one counter key inside the
// caller's transaction. That is safe because bw admits a single writer at
// a time (DB.Update / DB.Begin hold writeMu for the transaction's whole
// lifetime), and a transaction reads its own pending writes, so a batch
// inserting many documents hands out consecutive ids.
func (ti *triIndex) docID(btx *badger.Txn, pk []byte, create bool) (uint32, bool, error) {
	key := triDocIDKey(ti.bucket, pk)
	item, err := btx.Get(key)
	switch {
	case err == nil:
		var id uint32
		if verr := item.Value(func(val []byte) error {
			if len(val) != 4 {
				return fmt.Errorf("bw: trigram: corrupt docid for pk %q", pk)
			}
			id = binary.BigEndian.Uint32(val)

			return nil
		}); verr != nil {
			return 0, false, verr
		}

		return id, true, nil
	case errors.Is(err, badger.ErrKeyNotFound):
		if !create {
			return 0, false, nil
		}
	default:
		return 0, false, err
	}

	next, err := ti.nextDocID(btx)
	if err != nil {
		return 0, false, err
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], next)
	if err := btx.Set(key, buf[:]); err != nil {
		return 0, false, err
	}
	if err := btx.Set(triPKKey(ti.bucket, next), append([]byte(nil), pk...)); err != nil {
		return 0, false, err
	}

	return next, true, nil
}

// nextDocID consumes one id from the bucket's counter. Ids start at 1 so
// a zero read from a truncated value can never be mistaken for a valid
// document.
func (ti *triIndex) nextDocID(btx *badger.Txn) (uint32, error) {
	key := triSeqKey(ti.bucket)
	cur := uint32(1)
	item, err := btx.Get(key)
	switch {
	case err == nil:
		if verr := item.Value(func(val []byte) error {
			if len(val) != 4 {
				return fmt.Errorf("bw: trigram: corrupt docid sequence for bucket %q", ti.bucket)
			}
			cur = binary.BigEndian.Uint32(val)

			return nil
		}); verr != nil {
			return 0, verr
		}
	case errors.Is(err, badger.ErrKeyNotFound):
	default:
		return 0, err
	}
	if cur == math.MaxUint32 {
		return 0, fmt.Errorf("bw: trigram: document id space exhausted for bucket %q", ti.bucket)
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], cur+1)
	if err := btx.Set(key, buf[:]); err != nil {
		return 0, err
	}

	return cur, nil
}

// pkOf resolves an interned document id back to its primary key.
func (ti *triIndex) pkOf(btx *badger.Txn, docID uint32) ([]byte, bool, error) {
	item, err := btx.Get(triPKKey(ti.bucket, docID))
	switch {
	case err == nil:
		pk, cerr := item.ValueCopy(nil)
		if cerr != nil {
			return nil, false, cerr
		}

		return pk, true, nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return nil, false, nil
	default:
		return nil, false, err
	}
}

// ---------------------------------------------------------------------------
// Candidate resolution
// ---------------------------------------------------------------------------

// resolve turns a planned query into a sorted list of candidate document
// ids. all=true means "no usable constraint": the caller must consider
// every document in the bucket.
func (ti *triIndex) resolve(ctx context.Context, btx *badger.Txn, field string, q *triQuery, maxDocs int) (docs []uint32, all bool, err error) {
	if err := ctxErr(ctx); err != nil {
		return nil, false, err
	}
	if maxDocs <= 0 {
		maxDocs = triDefaultMaxCandidates
	}

	switch q.op {
	case triAll:
		return nil, true, nil
	case triNone:
		return nil, false, nil
	case triOr:
		return ti.resolveOr(ctx, btx, field, q, maxDocs)
	case triAnd:
		return ti.resolveAnd(ctx, btx, field, q, maxDocs)
	}

	return nil, true, nil
}

func (ti *triIndex) resolveOr(ctx context.Context, btx *badger.Txn, field string, q *triQuery, maxDocs int) ([]uint32, bool, error) {
	var union []uint32
	add := func(set []uint32) bool {
		union = triUnion(union, set)

		return len(union) <= maxDocs
	}

	for _, tri := range q.tri {
		set, overflow, err := ti.postings(ctx, btx, field, tri, maxDocs)
		if err != nil {
			return nil, false, err
		}
		if overflow || !add(set) {
			return nil, true, nil
		}
	}
	for _, sub := range q.sub {
		set, all, err := ti.resolve(ctx, btx, field, sub, maxDocs)
		if err != nil {
			return nil, false, err
		}
		if all || !add(set) {
			return nil, true, nil
		}
	}

	return union, false, nil
}

func (ti *triIndex) resolveAnd(ctx context.Context, btx *badger.Txn, field string, q *triQuery, maxDocs int) ([]uint32, bool, error) {
	// Sub-queries are resolved first: one of them may already be empty,
	// which settles the whole conjunction without touching a posting
	// list, and a materialised sub-set is a candidate base that costs
	// nothing more to use.
	var (
		subSets [][]uint32
		base    []uint32
		hasBase bool
	)
	for _, sub := range q.sub {
		set, all, err := ti.resolve(ctx, btx, field, sub, maxDocs)
		if err != nil {
			return nil, false, err
		}
		if all {
			continue
		}
		if len(set) == 0 {
			return nil, false, nil
		}
		subSets = append(subSets, set)
		if !hasBase || len(set) < len(base) {
			base, hasBase = set, true
		}
	}

	// Pick the rarest trigram as the intersection base when it beats
	// whatever a sub-query offered. Sizes come from a capped probe: the
	// exact length of a long posting list is irrelevant to the choice and
	// walking it to find out would cost more than the plan saves.
	baseTri := -1
	baseSize := math.MaxInt
	if hasBase {
		baseSize = len(base)
	}
	sizes := make([]int, len(q.tri))
	for i, tri := range q.tri {
		n, err := ti.probe(ctx, btx, field, tri, triProbeCap)
		if err != nil {
			return nil, false, err
		}
		if n == 0 {
			return nil, false, nil
		}
		sizes[i] = n
		if n < baseSize {
			baseSize, baseTri = n, i
		}
	}

	if baseTri >= 0 {
		set, overflow, err := ti.postings(ctx, btx, field, q.tri[baseTri], maxDocs)
		if err != nil {
			return nil, false, err
		}
		if overflow {
			return nil, true, nil
		}
		base, hasBase = set, true
	}
	if !hasBase {
		return nil, true, nil
	}

	// Verify the remaining trigrams by point read, rarest first so the
	// cheapest rejections happen earliest.
	rest := make([]int, 0, len(q.tri))
	for i := range q.tri {
		if i != baseTri {
			rest = append(rest, i)
		}
	}
	slices.SortFunc(rest, func(a, b int) int { return sizes[a] - sizes[b] })
	if len(rest) > triMaxVerify {
		rest = rest[:triMaxVerify]
	}

	out := base[:0:0]
	for i, docID := range base {
		if i%ctxCheckInterval == 0 {
			if err := ctxErr(ctx); err != nil {
				return nil, false, err
			}
		}
		keep := true
		for _, idx := range rest {
			_, err := btx.Get(triPostingKey(ti.bucket, field, q.tri[idx], docID))
			switch {
			case err == nil:
			case errors.Is(err, badger.ErrKeyNotFound):
				keep = false
			default:
				return nil, false, err
			}
			if !keep {
				break
			}
		}
		if keep {
			for _, set := range subSets {
				if _, found := slices.BinarySearch(set, docID); !found {
					keep = false

					break
				}
			}
		}
		if keep {
			out = append(out, docID)
		}
	}

	return out, false, nil
}

// postings materialises the document ids carrying one trigram. overflow
// reports that the list is longer than max, in which case docs is nil:
// the caller falls back to a scan rather than holding a list it has
// already decided is too big to be useful.
func (ti *triIndex) postings(ctx context.Context, btx *badger.Txn, field string, tri uint32, max int) (docs []uint32, overflow bool, err error) {
	prefix := triPostingTermPrefix(ti.bucket, field, tri)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix
	it := btx.NewIterator(opts)
	defer it.Close()

	n := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if n%ctxCheckInterval == 0 {
			if cerr := ctxErr(ctx); cerr != nil {
				return nil, false, cerr
			}
		}
		id, ok := docIDFromPostingKey(it.Item().Key(), prefix)
		if !ok {
			continue
		}
		if len(docs) >= max {
			return nil, true, nil
		}
		docs = append(docs, id)
		n++
	}

	return docs, false, nil
}

// probe counts a posting list up to limit. The count is only ever
// compared against other probes, so saturating at limit is enough.
func (ti *triIndex) probe(ctx context.Context, btx *badger.Txn, field string, tri uint32, limit int) (int, error) {
	prefix := triPostingTermPrefix(ti.bucket, field, tri)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix
	it := btx.NewIterator(opts)
	defer it.Close()

	n := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		n++
		if n >= limit {
			break
		}
	}
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}

	return n, nil
}

// triUnion merges two sorted, deduped id lists.
func triUnion(a, b []uint32) []uint32 {
	if len(a) == 0 {
		return append(a[:0:0], b...)
	}
	if len(b) == 0 {
		return a
	}
	out := make([]uint32, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)

	return append(out, b[j:]...)
}
