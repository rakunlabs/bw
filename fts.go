package bw

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/bw/schema"
)

// Full-text search is implemented as a pure-Badger inverted index. All
// state lives under the \x00fts\x00<bucket>\x00 namespace (see keys.go),
// so:
//
//   - A record write and its FTS update commit atomically inside the
//     same Badger transaction. A rollback never leaves stale terms.
//   - Backup / Restore are automatically portable: replicating the
//     Badger key-space replicates the FTS index too, with no separate
//     directory tree, no replay worker, no external file-system state.
//   - Cluster sync (incremental Backup of Badger keys) carries FTS
//     updates as a side-effect of carrying data updates.
//
// Scoring uses BM25 with the standard parameters k1=1.2, b=0.75. Search
// AND-combines the query terms; OR/NOT/phrase queries are not
// implemented in this iteration but the storage layout is general
// enough to support them later.

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

// Tokenizer turns a raw field value into the lower-cased, normalised
// terms that participate in the inverted index. Implementations must be
// deterministic — search uses the same Tokenizer to break the query
// string, so any inconsistency makes documents unreachable.
type Tokenizer interface {
	Tokenize(text string) []string
}

// DefaultTokenizer splits on Unicode non-letter/non-digit boundaries,
// lowercases each run, and skips runs shorter than MinLen. It works
// reasonably for Latin and most BMP scripts; for serious Turkish
// stemming, plug in a domain-specific tokenizer via WithFTSTokenizer.
type DefaultTokenizer struct {
	// MinLen is the inclusive minimum rune count for a token to be
	// retained. Zero or negative means 1 (single-character tokens
	// allowed).
	MinLen int
}

// Tokenize splits text into normalised terms.
func (d DefaultTokenizer) Tokenize(text string) []string {
	min := d.MinLen
	if min <= 0 {
		min = 1
	}
	var (
		out []string
		cur []rune
	)
	flush := func() {
		if len(cur) >= min {
			out = append(out, strings.ToLower(string(cur)))
		}
		cur = cur[:0]
	}
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			cur = append(cur, r)
			continue
		}
		flush()
	}
	flush()
	return out
}

var defaultTokenizer Tokenizer = DefaultTokenizer{MinLen: 1}

// ---------------------------------------------------------------------------
// FTS handle
// ---------------------------------------------------------------------------

// ftsIndex bundles the per-bucket FTS configuration. Unlike the previous
// Bleve-based implementation, this struct holds no live disk handle —
// every operation is a plain Badger read or write. The mutex is purely
// to coordinate the very rare reset path with concurrent searches.
type ftsIndex struct {
	bucket    string
	fields    []*schema.Field // FTS-tagged fields
	tokenizer Tokenizer

	mu sync.RWMutex
}

// ftsRegistry tracks per-bucket FTS handles. Kept on DB so closing the
// DB has somewhere to coordinate, even though there's nothing to close.
type ftsRegistry struct {
	mu      sync.RWMutex
	indexes map[string]*ftsIndex
}

func newFTSRegistry() *ftsRegistry {
	return &ftsRegistry{indexes: make(map[string]*ftsIndex)}
}

func (r *ftsRegistry) get(bucket string) *ftsIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.indexes[bucket]
}

func (r *ftsRegistry) set(bucket string, idx *ftsIndex) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.indexes[bucket] = idx
}

func (r *ftsRegistry) snapshot() map[string]*ftsIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]*ftsIndex, len(r.indexes))
	for k, v := range r.indexes {
		out[k] = v
	}
	return out
}

// closeAll is a no-op kept for API symmetry with the prior Bleve-backed
// registry. There is no per-index resource to release.
func (r *ftsRegistry) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.indexes = make(map[string]*ftsIndex)
}

// resetAll wipes every FTS key for every registered bucket. Called by
// DB.Wipe; exported via the registry rather than DB so the call site
// matches the legacy interface.
func (r *ftsRegistry) resetAll(db *DB) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for bucket := range r.indexes {
		if err := wipeFTSBucket(db, bucket); err != nil {
			return fmt.Errorf("bw: fts wipe %q: %w", bucket, err)
		}
	}
	return nil
}

// openFTSIndex returns the FTS handle for a bucket. It performs no I/O
// (every state read happens lazily during writes/searches), so it
// cannot fail except via callers that pass a nil schema.
func openFTSIndex(_ *DB, bucket string, fields []*schema.Field) (*ftsIndex, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	return &ftsIndex{
		bucket:    bucket,
		fields:    fields,
		tokenizer: defaultTokenizer,
	}, nil
}

// ---------------------------------------------------------------------------
// Update path: writeDoc / deleteDoc — called from upsertTx / DeleteTx.
// ---------------------------------------------------------------------------

// writeDoc indexes (or re-indexes) a record. It first removes every
// posting/terms entry the previous version of this pk emitted (using
// the fts-terms back-index for O(unique-terms) work, no full scans),
// then writes the new postings and stats updates. All operations share
// the supplied Badger txn so the entire FTS update commits atomically
// with the data write.
func (fi *ftsIndex) writeDoc(btx *badger.Txn, pk []byte, record any) error {
	terms := fi.extractTerms(record)
	return fi.applyDoc(btx, pk, terms)
}

// deleteDoc removes every posting and stats contribution for pk.
func (fi *ftsIndex) deleteDoc(btx *badger.Txn, pk []byte) error {
	return fi.applyDoc(btx, pk, nil)
}

// applyDoc is the shared write path: clear the previous postings (if
// any) and emit the new ones. When newTerms is empty, the doc is
// effectively deleted.
func (fi *ftsIndex) applyDoc(btx *badger.Txn, pk []byte, newTerms map[fieldTermKey]uint64) error {
	// 1) Read the previous (field, term, tf) tuples from the
	//    fts-terms back-index, plus the previous total length.
	oldTerms, oldDocLen, err := fi.readDocState(btx, pk)
	if err != nil {
		return err
	}

	// 2) Drop every posting and back-index entry the old version
	//    emitted. Skipping entries whose (field, term) appears in
	//    newTerms with the SAME tf is a small optimisation we don't
	//    bother with — overwriting is cheap.
	for ft := range oldTerms {
		if err := btx.Delete(ftsPostingKey(fi.bucket, ft.field, []byte(ft.term), pk)); err != nil {
			return err
		}
		if err := btx.Delete(ftsTermsKey(fi.bucket, pk, ft.field, []byte(ft.term))); err != nil {
			return err
		}
	}

	// 3) Compute new doc length (sum of tf over new terms).
	var newDocLen uint64
	for _, tf := range newTerms {
		newDocLen += tf
	}

	// 4) Write new postings + back-index entries.
	for ft, tf := range newTerms {
		var buf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(buf[:], tf)
		if err := btx.Set(ftsPostingKey(fi.bucket, ft.field, []byte(ft.term), pk), append([]byte(nil), buf[:n]...)); err != nil {
			return err
		}
		if err := btx.Set(ftsTermsKey(fi.bucket, pk, ft.field, []byte(ft.term)), append([]byte(nil), buf[:n]...)); err != nil {
			return err
		}
	}

	// 5) Maintain the per-doc length key.
	dlKey := ftsDoclenKey(fi.bucket, pk)
	if newDocLen == 0 {
		if err := btx.Delete(dlKey); err != nil {
			return err
		}
	} else {
		var buf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(buf[:], newDocLen)
		if err := btx.Set(dlKey, append([]byte(nil), buf[:n]...)); err != nil {
			return err
		}
	}

	// 6) Maintain corpus stats (docCount, sumLen). docCount
	//    increments on a fresh insert and decrements on a delete; an
	//    overwrite leaves it unchanged.
	hadOld := len(oldTerms) > 0 || oldDocLen > 0
	hasNew := newDocLen > 0
	deltaCount := 0
	if !hadOld && hasNew {
		deltaCount = +1
	} else if hadOld && !hasNew {
		deltaCount = -1
	}
	if deltaCount != 0 || newDocLen != oldDocLen {
		if err := fi.updateStats(btx, deltaCount, int64(newDocLen)-int64(oldDocLen)); err != nil {
			return err
		}
	}

	return nil
}

// fieldTermKey identifies one (field, term) pair within a doc.
type fieldTermKey struct {
	field string
	term  string // term bytes interned as string for map use
}

// extractTerms tokenises every FTS field of the record into a
// (field,term) -> term-frequency map.
func (fi *ftsIndex) extractTerms(record any) map[fieldTermKey]uint64 {
	rv := reflect.ValueOf(record)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	out := make(map[fieldTermKey]uint64)
	for _, f := range fi.fields {
		fv := rv.FieldByIndex(f.Index)
		var text string
		switch fv.Kind() {
		case reflect.String:
			text = fv.String()
		default:
			continue
		}
		if text == "" {
			continue
		}
		for _, tok := range fi.tokenizer.Tokenize(text) {
			out[fieldTermKey{field: f.Name, term: tok}]++
		}
	}
	return out
}

// readDocState loads every (field, term, tf) the doc previously
// emitted, plus its prior total length. Both maps are empty when the
// pk has not been indexed before.
func (fi *ftsIndex) readDocState(btx *badger.Txn, pk []byte) (map[fieldTermKey]uint64, uint64, error) {
	terms := make(map[fieldTermKey]uint64)

	prefix := ftsTermsPKPrefix(fi.bucket, pk)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := btx.NewIterator(opts)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		k := it.Item().KeyCopy(nil)
		field, term, ok := parseTermsKey(k, prefix)
		if !ok {
			continue
		}
		var tf uint64
		err := it.Item().Value(func(val []byte) error {
			t, n := binary.Uvarint(val)
			if n <= 0 {
				return fmt.Errorf("bw: fts: malformed terms value")
			}
			tf = t
			return nil
		})
		if err != nil {
			it.Close()
			return nil, 0, err
		}
		terms[fieldTermKey{field: field, term: string(term)}] = tf
	}
	it.Close()

	var docLen uint64
	dlKey := ftsDoclenKey(fi.bucket, pk)
	item, err := btx.Get(dlKey)
	switch {
	case err == nil:
		err = item.Value(func(val []byte) error {
			t, n := binary.Uvarint(val)
			if n <= 0 {
				return fmt.Errorf("bw: fts: malformed doclen value")
			}
			docLen = t
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
	case errors.Is(err, badger.ErrKeyNotFound):
		// no prior length
	default:
		return nil, 0, err
	}

	return terms, docLen, nil
}

// updateStats applies a (count, totalLen) delta to the corpus stats key.
func (fi *ftsIndex) updateStats(btx *badger.Txn, deltaCount int, deltaLen int64) error {
	docCount, sumLen, err := fi.readStats(btx)
	if err != nil {
		return err
	}
	if deltaCount < 0 {
		if uint64(-deltaCount) > docCount {
			docCount = 0
		} else {
			docCount -= uint64(-deltaCount)
		}
	} else {
		docCount += uint64(deltaCount)
	}
	if deltaLen < 0 {
		if uint64(-deltaLen) > sumLen {
			sumLen = 0
		} else {
			sumLen -= uint64(-deltaLen)
		}
	} else {
		sumLen += uint64(deltaLen)
	}

	if docCount == 0 && sumLen == 0 {
		// Bucket emptied: drop the stats key for cleanliness.
		return btx.Delete(ftsStatsKey(fi.bucket))
	}

	var buf [2 * binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], docCount)
	n += binary.PutUvarint(buf[n:], sumLen)
	return btx.Set(ftsStatsKey(fi.bucket), append([]byte(nil), buf[:n]...))
}

// readStats loads (docCount, sumLen) from the stats key. Returns zero
// values when the key is absent.
func (fi *ftsIndex) readStats(btx *badger.Txn) (uint64, uint64, error) {
	item, err := btx.Get(ftsStatsKey(fi.bucket))
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return 0, 0, nil
	case err != nil:
		return 0, 0, err
	}
	var docCount, sumLen uint64
	err = item.Value(func(val []byte) error {
		c, n := binary.Uvarint(val)
		if n <= 0 {
			return fmt.Errorf("bw: fts: malformed stats value")
		}
		docCount = c
		s, m := binary.Uvarint(val[n:])
		if m <= 0 {
			return fmt.Errorf("bw: fts: malformed stats value")
		}
		sumLen = s
		return nil
	})
	return docCount, sumLen, err
}

// ---------------------------------------------------------------------------
// Search path
// ---------------------------------------------------------------------------

// search executes the query and returns the top-N hits ordered by
// descending BM25 score. The query is tokenised with the same
// Tokenizer used at index time, then ANDed across every FTS field of
// the bucket.
func (fi *ftsIndex) search(db *DB, queryStr string, limit, offset int) ([]SearchHit, uint64, error) {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	terms := fi.tokenizer.Tokenize(queryStr)
	if len(terms) == 0 {
		return nil, 0, nil
	}
	// Deduplicate to avoid redundant scans for repeated terms in the
	// same query (e.g. "go go go").
	seen := make(map[string]bool, len(terms))
	uniq := terms[:0]
	for _, t := range terms {
		if !seen[t] {
			seen[t] = true
			uniq = append(uniq, t)
		}
	}
	terms = uniq

	// Per-doc score accumulator and per-doc match counter (we keep
	// only docs that hit every query term).
	type docState struct {
		score   float64
		matches int
	}
	docs := make(map[string]*docState)

	var docCount, sumLen uint64
	err := db.bdb.View(func(btx *badger.Txn) error {
		var sErr error
		docCount, sumLen, sErr = fi.readStats(btx)
		if sErr != nil {
			return sErr
		}
		if docCount == 0 {
			return nil
		}
		avgLen := float64(sumLen) / float64(docCount)
		const k1 = 1.2
		const b = 0.75

		for _, term := range terms {
			// Sum the (field-level) document frequency across
			// every FTS field for this term.
			var df uint64
			fieldHits := make([]postingHit, 0, 8)
			for _, f := range fi.fields {
				hits, fErr := fi.scanPostings(btx, f.Name, []byte(term))
				if fErr != nil {
					return fErr
				}
				fieldHits = append(fieldHits, hits...)
			}
			df = uint64(uniqueDocs(fieldHits))
			if df == 0 {
				// One required term has no postings -> AND
				// produces zero results.
				docs = nil
				return nil
			}
			// Robertson-style BM25 IDF with the +1 smoothing
			// recommended by Lucene; never goes negative.
			idf := math.Log(1 + (float64(docCount)-float64(df)+0.5)/(float64(df)+0.5))

			for _, h := range fieldHits {
				docLen, err := fi.readDocLen(btx, h.pk)
				if err != nil {
					return err
				}
				tfNorm := (float64(h.tf) * (k1 + 1)) /
					(float64(h.tf) + k1*(1-b+b*float64(docLen)/maxFloat(avgLen, 1)))
				key := string(h.pk)
				st, ok := docs[key]
				if !ok {
					st = &docState{}
					docs[key] = st
				}
				st.score += idf * tfNorm
				// matches counter is incremented at most once
				// per query-term: we tally per (term, doc).
				if h.firstSeen {
					st.matches++
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if len(docs) == 0 {
		return nil, 0, nil
	}

	// Filter to docs that matched every query term.
	type ranked struct {
		pk    string
		score float64
	}
	hits := make([]ranked, 0, len(docs))
	for pk, st := range docs {
		if st.matches < len(terms) {
			continue
		}
		hits = append(hits, ranked{pk: pk, score: st.score})
	}
	if len(hits) == 0 {
		return nil, 0, nil
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return hits[i].pk < hits[j].pk
	})

	total := uint64(len(hits))
	if offset > 0 {
		if offset >= len(hits) {
			return nil, total, nil
		}
		hits = hits[offset:]
	}
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}

	out := make([]SearchHit, 0, len(hits))
	for _, h := range hits {
		out = append(out, SearchHit{ID: h.pk, Score: h.score})
	}
	return out, total, nil
}

// postingHit is the per-(doc, field) result of a posting-list scan.
type postingHit struct {
	pk        []byte
	tf        uint64
	firstSeen bool // true on the first occurrence of pk across all fields for the current term
}

// scanPostings returns every (pk, tf) pair from the posting list for
// (bucket, field, term).
func (fi *ftsIndex) scanPostings(btx *badger.Txn, field string, term []byte) ([]postingHit, error) {
	prefix := ftsPostingTermPrefix(fi.bucket, field, term)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := btx.NewIterator(opts)
	defer it.Close()

	var (
		out  []postingHit
		seen = make(map[string]bool)
	)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		pk := pkFromPostingKey(key, prefix)
		if pk == nil {
			continue
		}
		var tf uint64
		err := item.Value(func(val []byte) error {
			t, n := binary.Uvarint(val)
			if n <= 0 {
				return fmt.Errorf("bw: fts: malformed posting tf")
			}
			tf = t
			return nil
		})
		if err != nil {
			return nil, err
		}
		first := !seen[string(pk)]
		seen[string(pk)] = true
		out = append(out, postingHit{pk: pk, tf: tf, firstSeen: first})
	}
	return out, nil
}

// uniqueDocs counts distinct pks in a posting-hit slice.
func uniqueDocs(hits []postingHit) int {
	if len(hits) == 0 {
		return 0
	}
	seen := make(map[string]bool, len(hits))
	for _, h := range hits {
		seen[string(h.pk)] = true
	}
	return len(seen)
}

// readDocLen returns the per-doc length for pk, or 0 if missing.
func (fi *ftsIndex) readDocLen(btx *badger.Txn, pk []byte) (uint64, error) {
	item, err := btx.Get(ftsDoclenKey(fi.bucket, pk))
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return 0, nil
	case err != nil:
		return 0, err
	}
	var v uint64
	err = item.Value(func(val []byte) error {
		t, n := binary.Uvarint(val)
		if n <= 0 {
			return fmt.Errorf("bw: fts: malformed doclen")
		}
		v = t
		return nil
	})
	return v, err
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// SearchHit is one result returned by ftsIndex.search. ID is the raw
// (string-coerced) primary key bytes; Score is the BM25 sum across
// query terms.
type SearchHit struct {
	ID    string
	Score float64
}

// ---------------------------------------------------------------------------
// Maintenance (reindex / wipe)
// ---------------------------------------------------------------------------

// wipeFTSBucket deletes every FTS key for a bucket. Used by Wipe and
// the explicit reindex path.
func wipeFTSBucket(db *DB, bucket string) error {
	prefix := ftsBucketPrefix(bucket)
	const batchSize = 1024

	for {
		var keys [][]byte
		err := db.bdb.View(func(btx *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := btx.NewIterator(opts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keys) < batchSize; it.Next() {
				keys = append(keys, it.Item().KeyCopy(nil))
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		err = db.bdb.Update(func(btx *badger.Txn) error {
			for _, k := range keys {
				if err := btx.Delete(k); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(keys) < batchSize {
			return nil
		}
	}
}

// WaitForFTSSync is retained for API stability with the prior
// outbox-based implementation. Writes are now synchronous (committed
// inside the same Badger txn as the data), so this method always
// returns immediately.
func (db *DB) WaitForFTSSync(_ context.Context) error {
	if db == nil || db.bdb == nil {
		return ErrClosed
	}
	return nil
}
