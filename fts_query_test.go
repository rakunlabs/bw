package bw

import (
	"context"
	"testing"
)

type Dep struct {
	ID   string `bw:"id,pk"`
	Path string `bw:"path,fts"`
	Body string `bw:"body,fts"`
}

// ids collects result primary keys for order-independent assertions.
func ids(results []SearchResult[Dep]) map[string]bool {
	out := make(map[string]bool, len(results))
	for _, r := range results {
		out[r.Record.ID] = true
	}
	return out
}

func newDepBucket(t *testing.T, docs ...*Dep) *Bucket[Dep] {
	t.Helper()
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	b, err := RegisterBucket[Dep](db, "deps")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range docs {
		if err := b.Insert(context.Background(), d); err != nil {
			t.Fatalf("insert %s: %v", d.ID, err)
		}
	}
	return b
}

// TestTokenizer_EmitBoth verifies a version string is findable both as
// the whole compound token and as its numeric fragments.
func TestTokenizer_EmitBoth(t *testing.T) {
	tk := DefaultTokenizer{}
	toks := tk.Tokenize("github.com/testcontainers/testcontainers-go v0.39.0")

	want := map[string]bool{
		// whole compounds
		"github.com/testcontainers/testcontainers-go": false,
		"v0.39.0": false,
		// fragments
		"github": false, "com": false, "testcontainers": false, "go": false,
		"v0": false, "39": false, "0": false,
	}
	for _, tok := range toks {
		if _, ok := want[tok]; ok {
			want[tok] = true
		}
	}
	for term, seen := range want {
		if !seen {
			t.Errorf("expected token %q to be emitted; got %v", term, toks)
		}
	}
}

// TestSearch_VersionExactAndFragment is the headline case: the exact
// version query returns the doc, and a fragment also works.
func TestSearch_VersionExactAndFragment(t *testing.T) {
	b := newDepBucket(t,
		&Dep{ID: "tc", Path: "go.mod", Body: "github.com/testcontainers/testcontainers-go v0.39.0"},
		&Dep{ID: "pg", Path: "go.mod", Body: "github.com/jackc/pgx/v5 v5.6.0"},
	)
	ctx := context.Background()

	for _, q := range []string{"v0.39.0", "testcontainers v0.39.0", "\"v0.39.0\"", "testcontainers"} {
		res, _, err := b.Search(ctx, q, 10, 0)
		if err != nil {
			t.Fatalf("search %q: %v", q, err)
		}
		if !ids(res)["tc"] {
			t.Errorf("query %q: expected to find 'tc', got %v", q, ids(res))
		}
		if ids(res)["pg"] {
			t.Errorf("query %q: did not expect 'pg', got %v", q, ids(res))
		}
	}
}

func TestSearch_OR(t *testing.T) {
	b := newDepBucket(t,
		&Dep{ID: "1", Body: "the go programming language"},
		&Dep{ID: "2", Body: "the rust programming language"},
		&Dep{ID: "3", Body: "the python programming language"},
	)
	res, _, err := b.Search(context.Background(), "go OR rust", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := ids(res)
	if !got["1"] || !got["2"] {
		t.Errorf("go OR rust: want {1,2}, got %v", got)
	}
	if got["3"] {
		t.Errorf("go OR rust: did not expect python doc 3, got %v", got)
	}
}

func TestSearch_NOT(t *testing.T) {
	b := newDepBucket(t,
		&Dep{ID: "1", Body: "database with postgres backend"},
		&Dep{ID: "2", Body: "database with mysql backend"},
	)
	for _, q := range []string{"database -postgres", "database NOT postgres"} {
		res, _, err := b.Search(context.Background(), q, 10, 0)
		if err != nil {
			t.Fatalf("search %q: %v", q, err)
		}
		got := ids(res)
		if !got["2"] || got["1"] {
			t.Errorf("query %q: want {2}, got %v", q, got)
		}
	}
}

func TestSearch_Phrase(t *testing.T) {
	b := newDepBucket(t,
		&Dep{ID: "adj", Body: "install testcontainers go now"},
		&Dep{ID: "split", Body: "go here to install testcontainers later"},
	)
	// Phrase requires adjacency: only the doc with "testcontainers go"
	// consecutive should match.
	res, _, err := b.Search(context.Background(), "\"testcontainers go\"", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := ids(res)
	if !got["adj"] {
		t.Errorf("phrase: want 'adj', got %v", got)
	}
	if got["split"] {
		t.Errorf("phrase: 'split' has the words but not adjacent; got %v", got)
	}
}

func TestSearch_Prefix(t *testing.T) {
	b := newDepBucket(t,
		&Dep{ID: "1", Body: "testcontainers integration"},
		&Dep{ID: "2", Body: "testing framework"},
		&Dep{ID: "3", Body: "production code"},
	)
	res, _, err := b.Search(context.Background(), "test*", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := ids(res)
	if !got["1"] || !got["2"] {
		t.Errorf("prefix test*: want {1,2}, got %v", got)
	}
	if got["3"] {
		t.Errorf("prefix test*: did not expect 3, got %v", got)
	}
}

// TestSearch_DefaultANDUnchanged confirms bare multi-term queries still
// AND, preserving prior behaviour.
func TestSearch_DefaultANDUnchanged(t *testing.T) {
	b := newDepBucket(t,
		&Dep{ID: "both", Body: "alpha and beta together"},
		&Dep{ID: "one", Body: "only alpha here"},
	)
	res, _, err := b.Search(context.Background(), "alpha beta", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := ids(res)
	if !got["both"] || got["one"] {
		t.Errorf("alpha beta (AND): want {both}, got %v", got)
	}
}

// TestSearch_PhraseNegation excludes docs containing a phrase.
func TestSearch_PhraseNegation(t *testing.T) {
	b := newDepBucket(t,
		&Dep{ID: "keep", Body: "database with mysql backend"},
		&Dep{ID: "drop", Body: "database with postgres backend"},
	)
	res, _, err := b.Search(context.Background(), "database -\"postgres backend\"", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := ids(res)
	if !got["keep"] || got["drop"] {
		t.Errorf("phrase negation: want {keep}, got %v", got)
	}
}
