// Vector + filter + embedder demo.
//
//  1. Load every *.md file under ./notes (Türkiye / Türk mutfağı içerikli).
//  2. WithEmbedder hooks a fake hash-based embedder so callers can
//     Insert raw records and let bw fill in the vector field.
//  3. Run a SearchVector on a query topic, print the ranked hits.
//  4. Add a query.Query filter to restrict results to a category and
//     show that the filter narrows the hits without bypassing scoring.
//  5. Rewrite one of the notes, re-insert it, and re-run the search:
//     the embedder produces a new vector, the HNSW graph re-wires
//     around it, and the score moves accordingly.
//  6. Backup the database, restore it into a brand-new directory, and
//     repeat the search there — vectors and HNSW graph travel as
//     Badger keys, no rebuild step needed.
//
// Run from the repo root:
//
//	go run ./example/vector
//
// The embedder is intentionally fake: a deterministic 64-dim hash-of-
// tokens "bag of words" projection. It is good enough to demonstrate
// that semantically-similar Turkish food notes cluster together. In a
// real application you'd swap it for an OpenAI / Cohere / local model
// call inside the same WithEmbedder hook.
package main

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/query"
)

// Note is one markdown file in the bucket. Title and Body stay as plain
// data; Category is indexed so query.Query can find it cheaply; Embed
// is the vector slot the embedder fills in for us.
type Note struct {
	ID       string    `bw:"id,pk"`
	Title    string    `bw:"title"`
	Category string    `bw:"category,index"`
	Body     string    `bw:"body"`
	Embed    []float32 `bw:"embed,vector(dim=64,metric=cosine)"`
}

const embedDim = 64

func main() {
	ctx := context.Background()

	dataDir, err := os.MkdirTemp("", "bw-vec-demo-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dataDir)

	notesDir, err := filepath.Abs("example/vector/notes")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stat(notesDir); err != nil {
		log.Fatalf("notes dir %q not found (run from the repo root): %v", notesDir, err)
	}

	db, err := bw.Open(dataDir, bw.WithLogger(nil))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	notes, err := bw.RegisterBucket[Note](db, "notes",
		bw.WithEmbedder[Note](func(_ context.Context, n *Note) ([]float32, error) {
			// Combine title + category + body so the embedding
			// reflects everything the reader sees. The category
			// boost is tiny (single token) but keeps records of
			// the same category slightly closer in vector space.
			return embed(n.Title + " " + n.Category + " " + n.Body), nil
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// ----- Scene 1: bulk insert. Embed left zero -> embedder runs.
	files, err := filepath.Glob(filepath.Join(notesDir, "*.md"))
	if err != nil {
		log.Fatal(err)
	}
	if len(files) == 0 {
		log.Fatalf("no markdown files in %s", notesDir)
	}

	for _, path := range files {
		body, err := os.ReadFile(path)
		if err != nil {
			log.Fatal(err)
		}
		base := filepath.Base(path)
		n := &Note{
			ID:       base,
			Title:    titleFromFilename(base),
			Category: categoryFromFilename(base),
			Body:     string(body),
			// Embed is intentionally empty — the embedder fills it in.
		}
		if err := notes.Insert(ctx, n); err != nil {
			log.Fatalf("insert %q: %v", path, err)
		}
		fmt.Printf("indexed %-32s (category=%-8s, %d bytes)\n", base, n.Category, len(body))
	}

	// ----- Scene 2: pure vector search.
	section("Scene 2: pure vector search")
	const q1 = "lezzetli Türk yemeği tarifi pastırma fıstık"
	q1Vec := embed(q1)
	fmt.Printf("query: %q\n", q1)
	printVecHits(ctx, notes, q1Vec, nil)

	// ----- Scene 3: vector + query.Query filter.
	section(`Scene 3: vector + filter category="sehir"`)
	const q2 = "Akdeniz kıyısında tarihi sokaklar ve antik kalıntılar"
	q2Vec := embed(q2)
	cityFilter, err := query.Parse("category=sehir")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("query: %q\n", q2)
	printVecHits(ctx, notes, q2Vec, cityFilter)

	// ----- Scene 4: rewrite a note and watch the ranking move.
	section("Scene 4: rewrite a note, embedder + HNSW re-wire")
	target := filepath.Join(notesDir, "sehir-hatay.md")
	originalBody, err := os.ReadFile(target)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := os.WriteFile(target, originalBody, 0o644); err != nil {
			log.Printf("restore %s: %v", target, err)
		}
	}()

	// Make hatay.md heavily emphasise food terms — it should jump
	// above the actual food notes for a food-leaning query.
	const hatayFoodHeavy = `# Hatay Yemek Rotası

Hatay mutfağı pastırma, baklava, fıstık, biberli ekmek, künefe,
oruk, humus, muhammara, kâğıt kebabı, tepsi kebabı ile dolup taşar.
Antakya Uzun Çarşı baklava, Harbiye künefe, Samandağ humus,
Antakya biberli ekmek, fıstıklı tatlılar, kekikli zeytinyağı.

Pastırmalı kuru fasulye, fıstıklı baklava, biberli ekmek — hepsi
Hatay lokantalarında bulunur. Türk mutfağının zenginliği bu şehirde
bir araya gelir.
`
	if err := os.WriteFile(target, []byte(hatayFoodHeavy), 0o644); err != nil {
		log.Fatal(err)
	}
	updatedBody, _ := os.ReadFile(target)
	if err := notes.Insert(ctx, &Note{
		ID:       "sehir-hatay.md",
		Title:    titleFromFilename("sehir-hatay.md"),
		Category: "sehir",
		Body:     string(updatedBody),
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rewrote sehir-hatay.md (now food-heavy, %d bytes)\n", len(updatedBody))
	fmt.Printf("re-running query: %q\n", q1)
	printVecHits(ctx, notes, q1Vec, nil)

	// ----- Scene 5: backup → fresh dir → restore → search still works.
	section("Scene 5: Backup → fresh dir → Restore → SearchVector")
	var buf bytes.Buffer
	if _, err := db.Backup(&buf, 0, false); err != nil {
		log.Fatalf("backup: %v", err)
	}
	freshDir, err := os.MkdirTemp("", "bw-vec-restore-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(freshDir)

	db2, err := bw.Open(freshDir, bw.WithLogger(nil))
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()
	if err := db2.Restore(&buf); err != nil {
		log.Fatalf("restore: %v", err)
	}
	notes2, err := bw.RegisterBucket[Note](db2, "notes",
		bw.WithEmbedder[Note](func(_ context.Context, n *Note) ([]float32, error) {
			return embed(n.Title + " " + n.Category + " " + n.Body), nil
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("restored into %s\n", freshDir)
	fmt.Printf("query: %q (no rebuild step, no _fts_/_vec_ dirs)\n", q1)
	printVecHits(ctx, notes2, q1Vec, nil)
}

// ---------------------------------------------------------------------------
// Embedder: deterministic hash-based bag of words
// ---------------------------------------------------------------------------

// embed produces a 64-dim L2-normalised vector from the text. It's a
// fake but useful embedder: words that co-occur produce overlapping
// hash buckets, so similar texts have similar vectors.
//
// Real applications swap this for an OpenAI / Cohere / local model
// call. The bw side stays the same — WithEmbedder takes any function
// returning []float32.
func embed(text string) []float32 {
	v := make([]float32, embedDim)
	for _, tok := range tokenize(text) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		v[int(h.Sum32())%embedDim] += 1.0
	}
	// L2-normalise so cosine works cleanly across docs of different lengths.
	var n float64
	for _, x := range v {
		n += float64(x) * float64(x)
	}
	if n == 0 {
		// Vector tag requires non-empty; pad with a tiny constant
		// so zero-token inputs still produce a well-formed unit
		// vector. Real embedders would rarely hit this.
		v[0] = 1.0
		return v
	}
	n = math.Sqrt(n)
	for i := range v {
		v[i] = float32(float64(v[i]) / n)
	}
	return v
}

// tokenize lower-cases and splits on Unicode non-letter / non-digit
// boundaries. Matches the FTS DefaultTokenizer's behaviour so the
// vectors and any future FTS index see the same terms.
func tokenize(text string) []string {
	var (
		out []string
		cur []rune
	)
	flush := func() {
		if len(cur) > 0 {
			out = append(out, strings.ToLower(string(cur)))
			cur = cur[:0]
		}
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

// ---------------------------------------------------------------------------
// Helpers: derive Title and Category from a filename like
// "yemek-pastirmali-kuru-fasulye.md"
// ---------------------------------------------------------------------------

func titleFromFilename(base string) string {
	stem := strings.TrimSuffix(base, ".md")
	parts := strings.Split(stem, "-")
	if len(parts) <= 1 {
		return stem
	}
	// Drop the leading category token, replace dashes with spaces,
	// title-case each word.
	titleParts := parts[1:]
	for i, p := range titleParts {
		if p == "" {
			continue
		}
		r := []rune(p)
		r[0] = unicode.ToUpper(r[0])
		titleParts[i] = string(r)
	}
	return strings.Join(titleParts, " ")
}

func categoryFromFilename(base string) string {
	stem := strings.TrimSuffix(base, ".md")
	idx := strings.IndexByte(stem, '-')
	if idx < 0 {
		return "diger"
	}
	return stem[:idx]
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

func section(title string) {
	fmt.Printf("\n--- %s ---\n", title)
}

func printVecHits(ctx context.Context, b *bw.Bucket[Note], q []float32, filter *query.Query) {
	opts := bw.SearchVectorOptions{K: 5}
	if filter != nil {
		opts.Filter = filter
	}
	hits, err := b.SearchVector(ctx, q, opts)
	if err != nil {
		log.Fatalf("search: %v", err)
	}
	if len(hits) == 0 {
		fmt.Println("(no hits)")
		return
	}
	for _, h := range hits {
		fmt.Printf("  [%.3f] %-34s category=%-8s — %s\n",
			h.Score, h.Record.ID, h.Record.Category, firstNonEmptyLine(h.Record.Body))
	}
}

func firstNonEmptyLine(body string) string {
	for _, line := range strings.Split(body, "\n") {
		t := strings.TrimSpace(strings.TrimLeft(line, "#"))
		if t != "" {
			return t
		}
	}
	return ""
}
