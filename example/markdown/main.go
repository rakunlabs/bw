// Markdown FTS demo.
//
//  1. Read every *.md file under ./notes into a typed bucket.
//  2. Run a full-text search and print the hits.
//  3. Rewrite one of the files in place, re-insert it, and run the
//     same search again — the new content shows up immediately because
//     the FTS update committed atomically with the data write.
//
// Run it with:
//
//	go run ./example/markdown
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/rakunlabs/bw"
)

// Note is one markdown file. Title comes from the filename, Body from
// the file contents. Both are FTS-indexed.
type Note struct {
	ID    string `bw:"id,pk"`
	Title string `bw:"title,fts"`
	Body  string `bw:"body,fts"`
}

func main() {
	ctx := context.Background()

	dataDir, err := os.MkdirTemp("", "bw-md-demo-*")
	if err != nil {
		log.Fatal(err)
	}
	// Keep it tidy: the demo blows the directory away on exit so
	// re-runs always start from a clean slate.
	defer os.RemoveAll(dataDir)

	notesDir, err := filepath.Abs("example/markdown/notes")
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

	notes, err := bw.RegisterBucket[Note](db, "notes")
	if err != nil {
		log.Fatal(err)
	}

	// 1. Load every *.md from disk and insert it.
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
			ID:    base,
			Title: strings.TrimSuffix(base, ".md"),
			Body:  string(body),
		}
		if err := notes.Insert(ctx, n); err != nil {
			log.Fatalf("insert %q: %v", path, err)
		}
		fmt.Printf("indexed %s (%d bytes)\n", base, len(body))
	}

	// 2. Search before any change.
	const query = "vector"
	fmt.Printf("\n--- search %q (before edit) ---\n", query)
	printSearch(ctx, notes, query)

	// 3. Rewrite intro.md so it actually mentions vectors.
	target := filepath.Join(notesDir, "intro.md")
	originalBody, err := os.ReadFile(target)
	if err != nil {
		log.Fatal(err)
	}
	// Restore the original content when the demo exits so the repo
	// stays in a known state for the next run.
	defer func() {
		if err := os.WriteFile(target, originalBody, 0o644); err != nil {
			log.Printf("restore %s: %v", target, err)
		}
	}()

	const newBody = `# Introduction to bw

bw now ships an HNSW vector index in addition to its full-text search.
Stage A was a brute-force fallback; Stage B added the graph layer.

## Vectors live in Badger

Vectors, doc lengths, posting lists and HNSW neighbour edges are all
plain Badger keys. That means a Backup taken on one machine carries
the vector index along with the data — restoring on a fresh
directory keeps SearchVector working without any rebuild step.

## Tags

Tag a field with vector(dim=N,metric=cosine) and bw takes care of the
graph for you.
`
	if err := os.WriteFile(target, []byte(newBody), 0o644); err != nil {
		log.Fatal(err)
	}

	// Re-insert the changed file. The FTS update overwrites the
	// previous postings inside the same Badger transaction.
	updatedBody, err := os.ReadFile(target)
	if err != nil {
		log.Fatal(err)
	}
	if err := notes.Insert(ctx, &Note{
		ID:    "intro.md",
		Title: "intro",
		Body:  string(updatedBody),
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nre-indexed intro.md (%d bytes)\n", len(updatedBody))

	// 4. Run the same search again.
	fmt.Printf("\n--- search %q (after edit) ---\n", query)
	printSearch(ctx, notes, query)
}

func printSearch(ctx context.Context, b *bw.Bucket[Note], q string) {
	results, total, err := b.Search(ctx, q, 10, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("total: %d\n", total)
	if total == 0 {
		fmt.Println("(no hits)")
		return
	}
	for _, r := range results {
		snippet := previewLine(r.Record.Body, q)
		fmt.Printf("  [%.3f] %s — %s\n", r.Score, r.Record.ID, snippet)
	}
}

// previewLine returns the first line of body that contains q (case
// insensitive), or the first non-empty line as a fallback.
func previewLine(body, q string) string {
	lower := strings.ToLower(q)
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(strings.ToLower(line), lower) {
			return strings.TrimSpace(line)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			return t
		}
	}
	return ""
}
