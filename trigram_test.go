package bw

import (
	"context"
	"encoding/binary"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// Chunk mirrors the shape a code index uses: a partition key encoded into
// the primary key, a secondary index for other queries, and one large
// text field carrying both a BM25 and a trigram index.
type Chunk struct {
	ID      string `bw:"id,pk"`
	Repo    string `bw:"repo,index"`
	Snippet string `bw:"snippet,fts,trigram"`
}

func newChunkBucket(t testing.TB, chunks []*Chunk) (*Bucket[Chunk], context.Context) {
	t.Helper()

	db, err := Open("", WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	bucket, err := RegisterBucket[Chunk](db, "code")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for start := 0; start < len(chunks); start += 200 {
		end := min(start+200, len(chunks))
		if err := bucket.InsertMany(ctx, chunks[start:end]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	return bucket, ctx
}

func regexIDs(t testing.TB, b *Bucket[Chunk], ctx context.Context, pattern string, opts RegexOptions) ([]string, uint64) {
	t.Helper()

	if opts.Limit == 0 {
		opts.Limit = 1000
	}
	hits, total, err := b.RegexSearch(ctx, pattern, opts)
	if err != nil {
		t.Fatalf("regex search %q: %v", pattern, err)
	}
	ids := make([]string, 0, len(hits))
	for _, h := range hits {
		ids = append(ids, h.ID)
	}

	return ids, total
}

// TestTrigramPlan pins the planner's output for the shapes a code search
// actually takes. A regression here is a silent loss of selectivity (or,
// worse, a constraint that is not implied by the pattern).
func TestTrigramPlan(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		// A literal requires every one of its trigrams.
		{`abcd`, "(abc AND bcd)"},
		// Case folding happens on both sides, so an upper-case literal
		// asks for the folded trigrams the index actually wrote.
		{`(?i)Foo`, "(foo)"},
		{`Foo`, "(foo)"},
		// Adjacent literals merge across the parser's atom boundaries,
		// which is what makes a call-site pattern selective.
		{`errors\.Is\(`, "(.is AND err AND is( AND ors AND ror AND rro AND rs. AND s.i)"},
		{`ab`, "ALL"},
		{`a.c`, "ALL"},
		// Alternation is a disjunction of its branches.
		{`foo|bar`, "(bar OR foo)"},
		// One unconstrained branch makes the whole alternation useless.
		{`foo|a.`, "ALL"},
		// Zero-or-more and optional occurrences guarantee nothing.
		{`(abcd)*`, "ALL"},
		{`(abcd)?`, "ALL"},
		// One occurrence is guaranteed by + and by {1,}.
		{`(abcd)+`, "(abc AND bcd)"},
		{`(abcd){2,}`, "(abc AND bcd)"},
		// A character class is never enumerated.
		{`[a-z]+`, "ALL"},
		// An impossible pattern needs no candidates at all.
		{`[^\x00-\x{10FFFF}]`, "NONE"},
	}

	for _, tc := range tests {
		t.Run(tc.pattern, func(t *testing.T) {
			q, err := regexTrigramQuery(tc.pattern)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if got := q.String(); got != tc.want {
				t.Errorf("plan(%q) = %s, want %s", tc.pattern, got, tc.want)
			}
		})
	}
}

// TestTrigramPlanPrunesCandidates checks the prefilter is doing the work
// rather than the verification pass: a rare literal must resolve to a
// candidate set far smaller than the corpus.
func TestTrigramPlanPrunesCandidates(t *testing.T) {
	var chunks []*Chunk
	for i := range 500 {
		chunks = append(chunks, &Chunk{
			ID:      fmt.Sprintf("acme/app/f%03d.go#0", i),
			Repo:    "acme/app",
			Snippet: fmt.Sprintf("package app\n\nfunc handler%d() error {\n\treturn nil\n}\n", i),
		})
	}
	chunks = append(chunks, &Chunk{
		ID:      "acme/app/rare.go#0",
		Repo:    "acme/app",
		Snippet: "func flush() error {\n\treturn ErrTxnTooBig\n}\n",
	})
	bucket, ctx := newChunkBucket(t, chunks)

	plan, err := regexTrigramQuery("ErrTxnTooBig")
	if err != nil {
		t.Fatal(err)
	}
	var (
		docs []uint32
		all  bool
	)
	if err := bucket.db.View(func(tx *Tx) error {
		docs, all, err = bucket.triIdx.resolve(ctx, tx.btx, "snippet", plan, 0)

		return err
	}); err != nil {
		t.Fatal(err)
	}
	if all {
		t.Fatal("planner fell back to a full scan for a rare literal")
	}
	if len(docs) != 1 {
		t.Fatalf("candidates = %d, want 1 (the prefilter is not pruning)", len(docs))
	}

	ids, total := regexIDs(t, bucket, ctx, "ErrTxnTooBig", RegexOptions{})
	if total != 1 || len(ids) != 1 || ids[0] != "acme/app/rare.go#0" {
		t.Fatalf("ids = %v, total = %d", ids, total)
	}
}

// TestTrigramMatchOffsets checks the reported offsets and line numbers,
// which are what a caller renders.
func TestTrigramMatchOffsets(t *testing.T) {
	bucket, ctx := newChunkBucket(t, []*Chunk{{
		ID:      "acme/app/a.go#0",
		Repo:    "acme/app",
		Snippet: "package app\n\nfunc Alpha() {}\nfunc Beta() {}\nfunc Alpha2() {}\n",
	}})

	hits, total, err := bucket.RegexSearch(ctx, `func Alpha`, RegexOptions{CaseSensitive: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(hits) != 1 {
		t.Fatalf("total = %d, hits = %d", total, len(hits))
	}
	got := hits[0].Matches
	if len(got) != 2 {
		t.Fatalf("matches = %v, want 2", got)
	}
	if got[0].Line != 3 || got[1].Line != 5 {
		t.Errorf("lines = %d,%d, want 3,5", got[0].Line, got[1].Line)
	}
	text := hits[0].Record.Snippet
	if s := text[got[0].Start:got[0].End]; s != "func Alpha" {
		t.Errorf("first match slice = %q", s)
	}
	if hits[0].Truncated {
		t.Error("Truncated set for a record with two matches and a cap of 16")
	}
}

// TestTrigramMatchTruncation checks the per-record match cap reports
// itself instead of silently dropping occurrences.
func TestTrigramMatchTruncation(t *testing.T) {
	bucket, ctx := newChunkBucket(t, []*Chunk{{
		ID:      "acme/app/a.go#0",
		Repo:    "acme/app",
		Snippet: strings.Repeat("alpha\n", 20),
	}})

	hits, _, err := bucket.RegexSearch(ctx, `alpha`, RegexOptions{MaxMatches: 3, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("hits = %d", len(hits))
	}
	if len(hits[0].Matches) != 3 || !hits[0].Truncated {
		t.Fatalf("matches = %d truncated = %v, want 3 true", len(hits[0].Matches), hits[0].Truncated)
	}
	if hits[0].Matches[2].Line != 3 {
		t.Errorf("third match line = %d, want 3", hits[0].Matches[2].Line)
	}
}

// TestTrigramCaseSensitivity checks the index answers both modes: it is
// folded, so case can only be enforced by the regexp.
func TestTrigramCaseSensitivity(t *testing.T) {
	bucket, ctx := newChunkBucket(t, []*Chunk{
		{ID: "a#0", Repo: "r", Snippet: "value := Manager{}"},
		{ID: "b#0", Repo: "r", Snippet: "value := manager{}"},
	})

	ids, total := regexIDs(t, bucket, ctx, `Manager`, RegexOptions{CaseSensitive: true})
	if total != 1 || len(ids) != 1 || ids[0] != "a#0" {
		t.Fatalf("case-sensitive ids = %v total = %d", ids, total)
	}

	ids, total = regexIDs(t, bucket, ctx, `Manager`, RegexOptions{})
	if total != 2 || len(ids) != 2 {
		t.Fatalf("case-insensitive ids = %v total = %d", ids, total)
	}
}

// TestTrigramScanFallback checks a pattern the planner cannot constrain
// still returns the right answer, by scanning.
func TestTrigramScanFallback(t *testing.T) {
	bucket, ctx := newChunkBucket(t, []*Chunk{
		{ID: "a#0", Repo: "r", Snippet: "alpha"},
		{ID: "b#0", Repo: "r", Snippet: "beta"},
		{ID: "c#0", Repo: "r", Snippet: "zzz"},
	})

	plan, err := regexTrigramQuery(`[ab]`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.op != triAll {
		t.Fatalf("plan for a bare class = %s, want ALL", plan)
	}

	ids, total := regexIDs(t, bucket, ctx, `[ab]`, RegexOptions{})
	if total != 2 || strings.Join(ids, ",") != "a#0,b#0" {
		t.Fatalf("ids = %v total = %d", ids, total)
	}
}

// TestTrigramUpdateRemovesStalePostings checks an overwrite retires the
// terms the previous value contributed. Without the old-record diff a
// rewritten chunk would stay findable by its previous contents.
func TestTrigramUpdateRemovesStalePostings(t *testing.T) {
	bucket, ctx := newChunkBucket(t, []*Chunk{
		{ID: "a#0", Repo: "r", Snippet: "ErrTxnTooBig is returned here"},
	})

	if err := bucket.Insert(ctx, &Chunk{ID: "a#0", Repo: "r", Snippet: "ErrConflict is returned here"}); err != nil {
		t.Fatal(err)
	}

	if _, total := regexIDs(t, bucket, ctx, `ErrTxnTooBig`, RegexOptions{}); total != 0 {
		t.Fatalf("stale text still matches (total = %d)", total)
	}
	if ids, total := regexIDs(t, bucket, ctx, `ErrConflict`, RegexOptions{}); total != 1 || ids[0] != "a#0" {
		t.Fatalf("new text ids = %v total = %d", ids, total)
	}

	// "txn" belonged only to the retired value, so its posting list must
	// be empty rather than merely unreachable.
	if n := postingCount(t, bucket, "snippet", "txn"); n != 0 {
		t.Errorf("posting list for %q has %d entries after overwrite", "txn", n)
	}
	if n := postingCount(t, bucket, "snippet", "con"); n != 1 {
		t.Errorf("posting list for %q has %d entries, want 1", "con", n)
	}
}

// TestTrigramDeleteRemovesPostings checks a delete leaves no postings and
// that the pk's id mapping survives so a re-insert reuses it.
func TestTrigramDeleteRemovesPostings(t *testing.T) {
	bucket, ctx := newChunkBucket(t, []*Chunk{
		{ID: "a#0", Repo: "r", Snippet: "ErrTxnTooBig"},
		{ID: "b#0", Repo: "r", Snippet: "ErrConflict"},
	})

	first := docIDOf(t, bucket, "a#0")

	if err := bucket.Delete(ctx, "a#0"); err != nil {
		t.Fatal(err)
	}
	if _, total := regexIDs(t, bucket, ctx, `ErrTxnTooBig`, RegexOptions{}); total != 0 {
		t.Fatal("deleted record still matches")
	}
	if n := postingCount(t, bucket, "snippet", "txn"); n != 0 {
		t.Errorf("posting list for %q has %d entries after delete", "txn", n)
	}
	if _, total := regexIDs(t, bucket, ctx, `ErrConflict`, RegexOptions{}); total != 1 {
		t.Error("delete disturbed the other record")
	}

	if err := bucket.Insert(ctx, &Chunk{ID: "a#0", Repo: "r", Snippet: "ErrTxnTooBig"}); err != nil {
		t.Fatal(err)
	}
	if again := docIDOf(t, bucket, "a#0"); again != first {
		t.Errorf("docid after re-insert = %d, want the original %d", again, first)
	}
	if _, total := regexIDs(t, bucket, ctx, `ErrTxnTooBig`, RegexOptions{}); total != 1 {
		t.Error("re-inserted record is not findable")
	}
}

// TestTrigramKeyFilterAndPaging checks partition scoping and that the
// total stays exact across pages.
func TestTrigramKeyFilterAndPaging(t *testing.T) {
	var chunks []*Chunk
	for i := range 6 {
		chunks = append(chunks,
			&Chunk{ID: fmt.Sprintf("acme/app/f%d.go#0", i), Repo: "acme/app", Snippet: "call handler(ctx)"},
			&Chunk{ID: fmt.Sprintf("other/lib/f%d.go#0", i), Repo: "other/lib", Snippet: "call handler(ctx)"},
		)
	}
	bucket, ctx := newChunkBucket(t, chunks)

	scoped := RegexOptions{KeyFilter: func(id string) bool { return strings.HasPrefix(id, "acme/app/") }}
	ids, total := regexIDs(t, bucket, ctx, `handler\(`, scoped)
	if total != 6 || len(ids) != 6 {
		t.Fatalf("scoped ids = %v total = %d", ids, total)
	}
	for _, id := range ids {
		if !strings.HasPrefix(id, "acme/app/") {
			t.Fatalf("key filter leaked %q", id)
		}
	}

	page := scoped
	page.Limit = 2
	page.Offset = 2
	ids, total = regexIDs(t, bucket, ctx, `handler\(`, page)
	if total != 6 {
		t.Errorf("total with paging = %d, want 6", total)
	}
	if strings.Join(ids, ",") != "acme/app/f2.go#0,acme/app/f3.go#0" {
		t.Errorf("page = %v", ids)
	}
}

// TestTrigramWalkStopsEarly checks a caller that stops gets no further
// records read on its behalf.
func TestTrigramWalkStopsEarly(t *testing.T) {
	var chunks []*Chunk
	for i := range 50 {
		chunks = append(chunks, &Chunk{ID: fmt.Sprintf("r/f%02d.go#0", i), Repo: "r", Snippet: "needle here"})
	}
	bucket, ctx := newChunkBucket(t, chunks)

	seen := 0
	total, err := bucket.RegexWalk(ctx, `needle`, RegexOptions{}, func(RegexResult[Chunk]) (bool, error) {
		seen++

		return seen < 3, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != 3 {
		t.Fatalf("delivered = %d, want 3", seen)
	}
	if total != 3 {
		t.Fatalf("total = %d, want 3 (an aborted walk counts what it verified)", total)
	}
}

// TestTrigramMultilinePattern checks a pattern spanning a newline works,
// which requires newlines to participate in the index.
func TestTrigramMultilinePattern(t *testing.T) {
	bucket, ctx := newChunkBucket(t, []*Chunk{
		{ID: "a#0", Repo: "r", Snippet: "func f() {\n\treturn nil\n}\n"},
		{ID: "b#0", Repo: "r", Snippet: "func g() {\n\treturn err\n}\n"},
	})

	ids, total := regexIDs(t, bucket, ctx, "\\{\n\treturn nil", RegexOptions{})
	if total != 1 || ids[0] != "a#0" {
		t.Fatalf("ids = %v total = %d", ids, total)
	}
}

// TestRegexWithoutTrigramField checks the failure mode is named rather
// than an empty result.
func TestRegexWithoutTrigramField(t *testing.T) {
	bucket, ctx := newDocBucket(t, []*Doc{{ID: "1", Repo: "r", Title: "t", Body: "b"}})

	if _, _, err := bucket.RegexSearch(ctx, "b", RegexOptions{}); err == nil {
		t.Fatal("expected ErrNoTrigram")
	} else if !strings.Contains(err.Error(), "trigram") {
		t.Fatalf("err = %v", err)
	}
}

// TestTrigramBackfillsExistingRecords is the upgrade contract: adding the
// tag to a bucket that already holds records must make them searchable.
//
// The write path diffs the new term set against the previous record's, which
// is the right thing for an edit and the wrong thing here — a backfill
// rewrites a record whose value did not change, so the diff is empty. The
// index resolves it by treating a pk it has never interned as having no
// previous terms at all.
func TestTrigramBackfillsExistingRecords(t *testing.T) {
	db, err := Open("", WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	type chunkV1 struct {
		ID      string `bw:"id,pk"`
		Repo    string `bw:"repo,index"`
		Snippet string `bw:"snippet,fts"`
	}

	ctx := context.Background()
	old, err := RegisterBucket[chunkV1](db, "code", WithVersion[chunkV1](1))
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Insert(ctx, &chunkV1{ID: "a#0", Repo: "r", Snippet: "return ErrTxnTooBig"}); err != nil {
		t.Fatal(err)
	}

	bucket, err := RegisterBucket[Chunk](db, "code",
		WithTypedMigration[Chunk, Chunk](1, 2, func(_ context.Context, rec *Chunk) (*Chunk, error) { return rec, nil }),
	)
	if err != nil {
		t.Fatalf("re-register with the trigram tag: %v", err)
	}

	ids, total := regexIDs(t, bucket, ctx, `ErrTxnTooBig`, RegexOptions{})
	if total != 1 || len(ids) != 1 || ids[0] != "a#0" {
		t.Fatalf("ids = %v total = %d, want the pre-existing record", ids, total)
	}
	if n := postingCount(t, bucket, "snippet", "txn"); n != 1 {
		t.Errorf("posting list for %q has %d entries after backfill, want 1", "txn", n)
	}
}

// TestTrigramMatchesBruteForce is the correctness net: for a corpus and a
// set of patterns covering every planner branch, the indexed answer must
// equal the answer a linear scan with the same regexp gives. The
// prefilter is only allowed to be faster, never different.
func TestTrigramMatchesBruteForce(t *testing.T) {
	words := []string{"handler", "Manager", "flush", "ErrTxnTooBig", "retry", "commit", "badger", "txn", "Snippet", "alpha"}
	var chunks []*Chunk
	for i := range 300 {
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("package p%d\n\n", i%7))
		for j := range 4 {
			w := words[(i*3+j*5)%len(words)]
			sb.WriteString(fmt.Sprintf("func %s%d(ctx context.Context) error {\n\treturn %s\n}\n", w, j, words[(i+j)%len(words)]))
		}
		chunks = append(chunks, &Chunk{
			ID:      fmt.Sprintf("repo%d/pkg/f%03d.go#%d", i%3, i, i%2),
			Repo:    fmt.Sprintf("repo%d", i%3),
			Snippet: sb.String(),
		})
	}
	bucket, ctx := newChunkBucket(t, chunks)

	patterns := []string{
		`ErrTxnTooBig`,
		`(?i)errtxntoobig`,
		`Manager|badger`,
		`func (handler|flush)\d`,
		`context\.Context`,
		`return\s+txn`,
		`Snippet\d?\(`,
		`^func`,
		`p[0-6]$`,
		`[A-Z]{3,}`,
		`fl.sh`,
		`nomatchanywhere`,
		`func \w+\(ctx context\.Context\) error \{`,
	}

	for _, sensitive := range []bool{false, true} {
		for _, pat := range patterns {
			name := pat
			if sensitive {
				name += " (case-sensitive)"
			}
			t.Run(name, func(t *testing.T) {
				effective := pat
				if !sensitive {
					effective = "(?i)" + pat
				}
				re, err := regexp.Compile("(?m)" + effective)
				if err != nil {
					t.Skipf("pattern does not compile: %v", err)
				}
				// The searched pattern must be the same expression the
				// reference uses, multiline flag included, so anchors
				// mean the same thing on both sides. Results arrive in
				// primary-key order, so the reference is sorted too.
				want := make([]string, 0)
				for _, c := range chunks {
					if re.MatchString(c.Snippet) {
						want = append(want, c.ID)
					}
				}
				slices.Sort(want)

				got, total := regexIDs(t, bucket, ctx, "(?m)"+pat, RegexOptions{CaseSensitive: sensitive, Limit: len(chunks) + 1})
				if int(total) != len(want) {
					t.Fatalf("total = %d, want %d", total, len(want))
				}
				if strings.Join(got, ",") != strings.Join(want, ",") {
					t.Fatalf("ids differ\n got: %v\nwant: %v", got, want)
				}
			})
		}
	}
}

// postingCount counts the posting entries for one trigram of a field.
func postingCount(t testing.TB, b *Bucket[Chunk], field, tri string) int {
	t.Helper()

	if len(tri) != 3 {
		t.Fatalf("trigram %q must be three bytes", tri)
	}
	packed := uint32(tri[0])<<16 | uint32(tri[1])<<8 | uint32(tri[2])
	prefix := triPostingTermPrefix(b.name, field, packed)

	n := 0
	if err := b.db.bdb.View(func(btx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := btx.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			n++
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	return n
}

// docIDOf reads the interned document id of a primary key.
func docIDOf(t testing.TB, b *Bucket[Chunk], pk string) uint32 {
	t.Helper()

	var id uint32
	if err := b.db.bdb.View(func(btx *badger.Txn) error {
		item, err := btx.Get(triDocIDKey(b.name, []byte(pk)))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			id = binary.BigEndian.Uint32(val)

			return nil
		})
	}); err != nil {
		t.Fatalf("docid for %q: %v", pk, err)
	}

	return id
}
