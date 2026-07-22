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

// Token is one emitted term together with its ordinal position in the
// source text. Positions are consecutive integers starting at 0 in the
// order words appear; they drive phrase (adjacency) matching. A single
// source word may emit several tokens sharing the same position (see the
// "emit both" behaviour of DefaultTokenizer for dotted identifiers).
type Token struct {
	Term string
	Pos  uint32
}

// PositionalTokenizer is the richer contract used by the index and the
// phrase-search path: it returns per-token positions. Any Tokenizer that
// does not implement it is adapted by assigning each returned term a
// sequential position, so custom tokenizers keep working (phrase search
// simply behaves as if every word were its own position).
type PositionalTokenizer interface {
	TokenizePositions(text string) []Token
}

// DefaultTokenizer splits on Unicode non-letter/non-digit boundaries,
// lowercases each run, and skips runs shorter than MinLen. It works
// reasonably for Latin and most BMP scripts; for serious Turkish
// stemming, plug in a domain-specific tokenizer via WithFTSTokenizer.
//
// "Emit both": a word that contains internal punctuation joining
// alphanumeric runs — e.g. a version "v0.39.0", an import path
// "github.com/x", or "foo_bar" — is emitted BOTH as the whole joined
// token ("v0.39.0") AND as its individual sub-runs ("v0", "39", "0").
// This keeps a search for the exact string findable as a single term
// while a search for a fragment ("39") still matches. The whole token
// and its first sub-run share a position; subsequent sub-runs advance
// the position so phrase queries over fragments still work.
type DefaultTokenizer struct {
	// MinLen is the inclusive minimum rune count for a token to be
	// retained. Zero or negative means 1 (single-character tokens
	// allowed).
	MinLen int
}

// Tokenize splits text into normalised terms (positions discarded).
func (d DefaultTokenizer) Tokenize(text string) []string {
	toks := d.TokenizePositions(text)
	out := make([]string, 0, len(toks))
	for _, t := range toks {
		out = append(out, t.Term)
	}
	return out
}

// isTokenByte reports whether r may appear inside a token: letters,
// digits, or the connector punctuation that joins compound identifiers
// and versions. Splitting still happens on whitespace and most symbols.
func isTokenRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// isJoiner reports whether r is internal punctuation that keeps a
// compound token whole (in addition to being a sub-run boundary).
func isJoiner(r rune) bool {
	switch r {
	case '.', '-', '_', '/', '+', '@':
		return true
	}
	return false
}

// TokenizePositions splits text into normalised terms with positions,
// applying the "emit both" rule for compound tokens.
func (d DefaultTokenizer) TokenizePositions(text string) []Token {
	minLen := d.MinLen
	if minLen <= 0 {
		minLen = 1
	}

	var (
		out []Token
		pos uint32
	)

	// A "word" is a maximal run of token-runes and interior joiners.
	// Within a word we track sub-runs (split on joiners). We emit each
	// sub-run as its own token, and — when the word had more than one
	// sub-run — also emit the whole joined word as a single token.
	var (
		word    []rune   // whole word incl. joiners
		sub     []rune   // current sub-run
		subs    []string // completed sub-runs of the current word
		subPos  []uint32 // starting position assigned to each sub-run
		nextPos = &pos
	)

	flushSub := func() {
		if len(sub) > 0 {
			subs = append(subs, strings.ToLower(string(sub)))
			subPos = append(subPos, *nextPos)
			*nextPos++
			sub = sub[:0]
		}
	}

	flushWord := func() {
		flushSub()
		if len(subs) == 0 {
			word = word[:0]
			return
		}
		// Emit sub-runs.
		for i, s := range subs {
			if len([]rune(s)) >= minLen {
				out = append(out, Token{Term: s, Pos: subPos[i]})
			}
		}
		// Emit the whole compound token (only when it differs from the
		// single sub-run) sharing the first sub-run's position. Trailing
		// joiners (e.g. the dot in "foo.bar.") are trimmed so the token
		// is exactly the joined identifier.
		if len(subs) > 1 {
			trimmed := word
			for len(trimmed) > 0 && isJoiner(trimmed[len(trimmed)-1]) {
				trimmed = trimmed[:len(trimmed)-1]
			}
			whole := strings.ToLower(string(trimmed))
			if len([]rune(whole)) >= minLen {
				out = append(out, Token{Term: whole, Pos: subPos[0]})
			}
		}
		subs = subs[:0]
		subPos = subPos[:0]
		word = word[:0]
	}

	for _, r := range text {
		switch {
		case isTokenRune(r):
			sub = append(sub, r)
			word = append(word, r)
		case isJoiner(r) && len(word) > 0:
			// Interior joiner: end the sub-run but keep the word open.
			flushSub()
			word = append(word, r)
		default:
			flushWord()
		}
	}
	flushWord()

	return out
}

var defaultTokenizer Tokenizer = DefaultTokenizer{MinLen: 1}

// tokenizePositions runs the configured tokenizer, upgrading to
// PositionalTokenizer when available and otherwise synthesising
// sequential positions so phrase search degrades gracefully.
func tokenizePositions(tk Tokenizer, text string) []Token {
	if pt, ok := tk.(PositionalTokenizer); ok {
		return pt.TokenizePositions(text)
	}
	terms := tk.Tokenize(text)
	out := make([]Token, len(terms))
	for i, t := range terms {
		out[i] = Token{Term: t, Pos: uint32(i)}
	}
	return out
}

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
func (fi *ftsIndex) applyDoc(btx *badger.Txn, pk []byte, newTerms map[fieldTermKey]*termInfo) error {
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
	for _, ti := range newTerms {
		newDocLen += ti.tf
	}

	// 4) Write new postings + back-index entries. The posting value
	//    carries tf + positions (for phrase search); the back-index
	//    value only needs tf (used for cleanup and doc-state reads).
	for ft, ti := range newTerms {
		if err := btx.Set(ftsPostingKey(fi.bucket, ft.field, []byte(ft.term), pk), encodePosting(ti)); err != nil {
			return err
		}
		var buf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(buf[:], ti.tf)
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

// termInfo is the per-(field,term) posting payload: the term frequency
// plus the sorted list of positions at which the term occurred in the
// field. Positions enable phrase (adjacency) matching.
type termInfo struct {
	tf        uint64
	positions []uint32
}

// extractTerms tokenises every FTS field of the record into a
// (field,term) -> termInfo map (frequency + positions).
func (fi *ftsIndex) extractTerms(record any) map[fieldTermKey]*termInfo {
	rv := reflect.ValueOf(record)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	out := make(map[fieldTermKey]*termInfo)
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
		for _, tok := range tokenizePositions(fi.tokenizer, text) {
			k := fieldTermKey{field: f.Name, term: tok.Term}
			ti := out[k]
			if ti == nil {
				ti = &termInfo{}
				out[k] = ti
			}
			ti.tf++
			ti.positions = append(ti.positions, tok.Pos)
		}
	}
	// Positions arrive in emission order which is already ascending per
	// field for sub-runs, but the "emit both" whole-token shares the
	// first sub-run position, so sort to guarantee ascending order for
	// delta encoding and phrase checks.
	for _, ti := range out {
		if len(ti.positions) > 1 {
			sort.Slice(ti.positions, func(i, j int) bool { return ti.positions[i] < ti.positions[j] })
		}
	}
	return out
}

// encodePosting serialises a termInfo into the posting value:
//
//	varint(tf) varint(nPos) varint(pos0) varint(pos1-pos0) ...
//
// Positions are delta-encoded (ascending) for compactness. A legacy
// value carrying only varint(tf) decodes as tf with zero positions, so
// old indexes remain readable (phrase search just skips those docs).
func encodePosting(ti *termInfo) []byte {
	buf := make([]byte, 0, 2+len(ti.positions))
	var tmp [binary.MaxVarintLen64]byte

	n := binary.PutUvarint(tmp[:], ti.tf)
	buf = append(buf, tmp[:n]...)

	n = binary.PutUvarint(tmp[:], uint64(len(ti.positions)))
	buf = append(buf, tmp[:n]...)

	var prev uint32
	for i, p := range ti.positions {
		d := p
		if i > 0 {
			d = p - prev
		}
		n = binary.PutUvarint(tmp[:], uint64(d))
		buf = append(buf, tmp[:n]...)
		prev = p
	}
	return buf
}

// decodePosting parses a posting value written by encodePosting, or a
// legacy tf-only value. wantPositions=false skips position decoding for
// the common ranking-only path.
func decodePosting(val []byte, wantPositions bool) (uint64, []uint32, error) {
	tf, n := binary.Uvarint(val)
	if n <= 0 {
		return 0, nil, fmt.Errorf("bw: fts: malformed posting tf")
	}
	rest := val[n:]
	if len(rest) == 0 {
		// Legacy tf-only posting: no positions stored.
		return tf, nil, nil
	}
	nPos, m := binary.Uvarint(rest)
	if m <= 0 {
		return tf, nil, nil // tolerate trailing garbage as "no positions"
	}
	if !wantPositions || nPos == 0 {
		return tf, nil, nil
	}
	rest = rest[m:]
	positions := make([]uint32, 0, nPos)
	var prev uint64
	for range nPos {
		d, k := binary.Uvarint(rest)
		if k <= 0 {
			return tf, nil, fmt.Errorf("bw: fts: malformed posting positions")
		}
		rest = rest[k:]
		cur := prev + d
		positions = append(positions, uint32(cur))
		prev = cur
	}
	return tf, positions, nil
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

// search parses the query, evaluates its clauses (AND / OR / NOT /
// phrase / prefix) and returns the top-N hits ordered by descending
// BM25 score. See fts_query.go for the query grammar.
func (fi *ftsIndex) search(btx *badger.Txn, queryStr string, limit, offset int) ([]SearchHit, uint64, error) {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	pq := parseQuery(fi.tokenizer, queryStr)
	if pq.isEmpty() {
		return nil, 0, nil
	}

	docCount, sumLen, err := fi.readStats(btx)
	if err != nil {
		return nil, 0, err
	}
	if docCount == 0 {
		return nil, 0, nil
	}
	avgLen := float64(sumLen) / float64(docCount)

	ev := &evaluator{fi: fi, btx: btx, docCount: docCount, avgLen: avgLen}

	// Per-doc score plus a satisfied flag per required clause / OR group.
	type docState struct {
		score  float64
		reqMet []bool
		orMet  []bool
	}
	docs := make(map[string]*docState)

	ensure := func(pk string) *docState {
		st, ok := docs[pk]
		if !ok {
			st = &docState{
				reqMet: make([]bool, len(pq.required)),
				orMet:  make([]bool, len(pq.orGroups)),
			}
			docs[pk] = st
		}
		return st
	}

	// Required clauses (AND).
	for i, c := range pq.required {
		matches, cerr := ev.evalClause(c)
		if cerr != nil {
			return nil, 0, cerr
		}
		if len(matches) == 0 {
			// A required clause with no matches means zero results.
			return nil, 0, nil
		}
		for pk, score := range matches {
			st := ensure(pk)
			st.reqMet[i] = true
			st.score += score
		}
	}

	// OR groups: a doc satisfies the group if it matches any alternative.
	for gi, group := range pq.orGroups {
		for _, c := range group {
			matches, cerr := ev.evalClause(c)
			if cerr != nil {
				return nil, 0, cerr
			}
			for pk, score := range matches {
				st := ensure(pk)
				st.orMet[gi] = true
				st.score += score
			}
		}
	}

	// Excluded clauses (NOT): collect the set of docs to drop.
	excluded := make(map[string]bool)
	for _, c := range pq.excluded {
		matches, cerr := ev.evalClause(c)
		if cerr != nil {
			return nil, 0, cerr
		}
		for pk := range matches {
			excluded[pk] = true
		}
	}

	// Keep docs that satisfied every required clause and every OR group,
	// minus the excluded set.
	type ranked struct {
		pk    string
		score float64
	}
	hits := make([]ranked, 0, len(docs))
	for pk, st := range docs {
		if excluded[pk] {
			continue
		}
		if !allTrue(st.reqMet) || !allTrue(st.orMet) {
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

func allTrue(bs []bool) bool {
	for _, b := range bs {
		if !b {
			return false
		}
	}
	return true
}

// evaluator carries the shared state for scoring one query's clauses.
type evaluator struct {
	fi       *ftsIndex
	btx      *badger.Txn
	docCount uint64
	avgLen   float64
}

const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// idf computes the Robertson/Lucene BM25 IDF for a document frequency.
func (ev *evaluator) idf(df uint64) float64 {
	return math.Log(1 + (float64(ev.docCount)-float64(df)+0.5)/(float64(df)+0.5))
}

// bm25 scores one (tf, docLen) contribution under the shared idf.
func (ev *evaluator) bm25(idf float64, tf, docLen uint64) float64 {
	tfNorm := (float64(tf) * (bm25K1 + 1)) /
		(float64(tf) + bm25K1*(1-bm25B+bm25B*float64(docLen)/maxFloat(ev.avgLen, 1)))
	return idf * tfNorm
}

// evalClause returns pk -> BM25 score for every document matching the
// clause across all FTS fields.
func (ev *evaluator) evalClause(c queryClause) (map[string]float64, error) {
	switch c.kind {
	case clausePrefix:
		return ev.evalPrefix(c.term)
	case clausePhrase:
		return ev.evalPhrase(c.phrase)
	default:
		return ev.evalTerm(c.term)
	}
}

// evalTerm scores an exact single-term clause.
func (ev *evaluator) evalTerm(term string) (map[string]float64, error) {
	fieldHits := make([]postingHit, 0, 8)
	for _, f := range ev.fi.fields {
		hits, err := ev.fi.scanPostings(ev.btx, f.Name, []byte(term), false)
		if err != nil {
			return nil, err
		}
		fieldHits = append(fieldHits, hits...)
	}
	df := uint64(uniqueDocs(fieldHits))
	if df == 0 {
		return nil, nil
	}
	idf := ev.idf(df)

	out := make(map[string]float64, len(fieldHits))
	for _, h := range fieldHits {
		docLen, err := ev.fi.readDocLen(ev.btx, h.pk)
		if err != nil {
			return nil, err
		}
		out[string(h.pk)] += ev.bm25(idf, h.tf, docLen)
	}
	return out, nil
}

// evalPrefix scores a prefix (wildcard) clause. Every term beginning
// with the prefix contributes; a doc's score is the max over matching
// terms (avoids over-weighting docs that happen to contain many
// variants). df is the number of distinct docs matched by the prefix.
func (ev *evaluator) evalPrefix(prefix string) (map[string]float64, error) {
	fieldHits := make([]postingHit, 0, 16)
	for _, f := range ev.fi.fields {
		hits, err := ev.fi.scanPostingsPrefix(ev.btx, f.Name, []byte(prefix))
		if err != nil {
			return nil, err
		}
		fieldHits = append(fieldHits, hits...)
	}
	df := uint64(uniqueDocs(fieldHits))
	if df == 0 {
		return nil, nil
	}
	idf := ev.idf(df)

	out := make(map[string]float64, len(fieldHits))
	for _, h := range fieldHits {
		docLen, err := ev.fi.readDocLen(ev.btx, h.pk)
		if err != nil {
			return nil, err
		}
		s := ev.bm25(idf, h.tf, docLen)
		if s > out[string(h.pk)] {
			out[string(h.pk)] = s
		}
	}
	return out, nil
}

// evalPhrase scores a phrase clause: a doc matches only when the phrase
// tokens occur at consecutive positions within a single field. Docs
// indexed before positions were stored cannot satisfy a phrase (their
// postings carry no positions) and are simply excluded.
func (ev *evaluator) evalPhrase(phrase []string) (map[string]float64, error) {
	if len(phrase) == 1 {
		return ev.evalTerm(phrase[0])
	}

	// For each field, gather per-doc positions of every phrase token,
	// then keep docs where some position p has token0@p, token1@p+1, ...
	out := make(map[string]float64)

	for _, f := range ev.fi.fields {
		// tokenPositions[i] : pk -> sorted positions of phrase[i].
		tokenPositions := make([]map[string][]uint32, len(phrase))
		ok := true
		for i, tok := range phrase {
			hits, err := ev.fi.scanPostings(ev.btx, f.Name, []byte(tok), true)
			if err != nil {
				return nil, err
			}
			if len(hits) == 0 {
				ok = false
				break
			}
			m := make(map[string][]uint32, len(hits))
			for _, h := range hits {
				m[string(h.pk)] = h.positions
			}
			tokenPositions[i] = m
		}
		if !ok {
			continue
		}

		// Candidate docs are those present for the first token.
		for pk, firstPositions := range tokenPositions[0] {
			if phraseMatches(tokenPositions, pk, firstPositions) {
				// Score with a phrase bonus: treat the whole phrase as
				// one occurrence weighted by its length so multi-word
				// phrases rank above incidental single-term hits.
				docLen, err := ev.fi.readDocLen(ev.btx, []byte(pk))
				if err != nil {
					return nil, err
				}
				// df for a phrase is unknown up front; approximate with
				// the rarer token's df via len(map). Use token0's here.
				idf := ev.idf(uint64(len(tokenPositions[0])))
				score := ev.bm25(idf, uint64(len(phrase)), docLen)
				if score > out[pk] {
					out[pk] = score
				}
			}
		}
	}

	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// phraseMatches reports whether pk has the phrase tokens at consecutive
// positions in the field described by tokenPositions.
func phraseMatches(tokenPositions []map[string][]uint32, pk string, firstPositions []uint32) bool {
	for _, start := range firstPositions {
		matched := true
		for i := 1; i < len(tokenPositions); i++ {
			positions, ok := tokenPositions[i][pk]
			if !ok || !containsPosition(positions, start+uint32(i)) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

// containsPosition reports whether the sorted slice contains target.
func containsPosition(sorted []uint32, target uint32) bool {
	lo, hi := 0, len(sorted)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		switch {
		case sorted[mid] == target:
			return true
		case sorted[mid] < target:
			lo = mid + 1
		default:
			hi = mid - 1
		}
	}
	return false
}

// postingHit is the per-(doc, field) result of a posting-list scan.
type postingHit struct {
	pk        []byte
	tf        uint64
	positions []uint32 // populated only when wantPositions was requested
	firstSeen bool     // true on the first occurrence of pk across all fields for the current term
}

// scanPostings returns every (pk, tf) pair from the posting list for
// (bucket, field, term). Positions are decoded only when wantPositions
// is true (phrase queries), keeping the common ranking path cheap.
func (fi *ftsIndex) scanPostings(btx *badger.Txn, field string, term []byte, wantPositions bool) ([]postingHit, error) {
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
		var (
			tf  uint64
			pos []uint32
		)
		err := item.Value(func(val []byte) error {
			t, p, derr := decodePosting(val, wantPositions)
			if derr != nil {
				return derr
			}
			tf = t
			if wantPositions {
				pos = append([]uint32(nil), p...)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		first := !seen[string(pk)]
		seen[string(pk)] = true
		out = append(out, postingHit{pk: pk, tf: tf, positions: pos, firstSeen: first})
	}
	return out, nil
}

// scanPostingsPrefix returns posting hits for every term in (bucket,
// field) that begins with termPrefix. Multiple matching terms may
// contribute the same pk; callers dedupe via uniqueDocs. This backs
// wildcard queries like "test*".
func (fi *ftsIndex) scanPostingsPrefix(btx *badger.Txn, field string, termPrefix []byte) ([]postingHit, error) {
	// The posting key is fieldPrefix + lp(term) + pk. A bare term
	// prefix is not length-prefix-aligned, so we scan the whole field
	// keyspace and filter by decoding each key's term. Field posting
	// lists are modest; this keeps prefix logic simple and correct.
	fieldPrefix := ftsPostingFieldPrefix(fi.bucket, field)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = fieldPrefix
	opts.PrefetchValues = false
	it := btx.NewIterator(opts)
	defer it.Close()

	var (
		out  []postingHit
		seen = make(map[string]bool)
	)
	for it.Seek(fieldPrefix); it.ValidForPrefix(fieldPrefix); it.Next() {
		key := it.Item().KeyCopy(nil)
		term, pk := termAndPKFromPostingKey(key, fieldPrefix)
		if term == nil || pk == nil {
			continue
		}
		if !bytesHasPrefix(term, termPrefix) {
			continue
		}
		first := !seen[string(pk)]
		seen[string(pk)] = true
		// tf=1 as a floor; prefix matches contribute presence, not
		// precise term frequency. Ranking still works via IDF/among
		// docs. (Exact tf would require a value read per key.)
		out = append(out, postingHit{pk: pk, tf: 1, firstSeen: first})
	}
	return out, nil
}

func bytesHasPrefix(b, prefix []byte) bool {
	return len(b) >= len(prefix) && string(b[:len(prefix)]) == string(prefix)
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
