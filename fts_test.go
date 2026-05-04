package bw

import (
	"context"
	"testing"
)

type Article struct {
	ID    string `bw:"id,pk"`
	Title string `bw:"title,fts"`
	Body  string `bw:"body,fts"`
}

func TestSearch_Basic(t *testing.T) {
	db, err := Open("", WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucket, err := RegisterBucket[Article](db, "articles")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	articles := []*Article{
		{ID: "1", Title: "Introduction to Go", Body: "Go is a statically typed language designed at Google."},
		{ID: "2", Title: "Full-Text Search", Body: "Bleve provides full-text search capabilities for Go applications."},
		{ID: "3", Title: "BadgerDB Tutorial", Body: "Badger is an embeddable key-value store written in Go."},
		{ID: "4", Title: "Python Basics", Body: "Python is a dynamically typed language popular for data science."},
	}

	for _, a := range articles {
		if err := bucket.Insert(ctx, a); err != nil {
			t.Fatalf("insert %s: %v", a.ID, err)
		}
	}

	// Search for "Go"
	results, total, err := bucket.Search(ctx, "Go", 10, 0)
	if err != nil {
		t.Fatal(err)
	}

	if total == 0 {
		t.Fatal("expected at least one result for 'Go'")
	}

	// Articles 1, 2, 3 all mention Go
	if len(results) < 2 {
		t.Errorf("expected at least 2 results, got %d", len(results))
	}

	// Verify scores are positive and records are hydrated
	for _, r := range results {
		if r.Score <= 0 {
			t.Errorf("expected positive score, got %f", r.Score)
		}
		if r.Record == nil {
			t.Error("expected non-nil record")
		}
	}
}

func TestSearch_AfterDelete(t *testing.T) {
	db, err := Open("", WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucket, err := RegisterBucket[Article](db, "articles")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = bucket.Insert(ctx, &Article{ID: "1", Title: "Unique Snowflake", Body: "This document is very unique."})

	// Should find it
	results, _, err := bucket.Search(ctx, "Snowflake", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Delete and search again
	if err := bucket.Delete(ctx, "1"); err != nil {
		t.Fatal(err)
	}

	results, _, err = bucket.Search(ctx, "Snowflake", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results))
	}
}

func TestSearch_NoFTSFields(t *testing.T) {
	type Plain struct {
		ID   string `bw:"id,pk"`
		Name string `bw:"name"`
	}

	db, err := Open("", WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucket, err := RegisterBucket[Plain](db, "plain")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = bucket.Search(context.Background(), "test", 10, 0)
	if err != ErrNoFTS {
		t.Errorf("expected ErrNoFTS, got %v", err)
	}
}
