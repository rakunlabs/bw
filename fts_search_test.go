package bw

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rakunlabs/query"
)

// Doc mirrors the shape a documentation index uses: a partition key that is
// indexed but not searchable, plus FTS-tagged text.
type Doc struct {
	ID    string `bw:"id,pk"`
	Repo  string `bw:"repo,index"`
	Title string `bw:"title,fts"`
	Body  string `bw:"body,fts"`
}

func newDocBucket(t testing.TB, docs []*Doc) (*Bucket[Doc], context.Context) {
	t.Helper()

	db, err := Open("", WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	bucket, err := RegisterBucket[Doc](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for start := 0; start < len(docs); start += 200 {
		end := min(start+200, len(docs))
		if err := bucket.InsertMany(ctx, docs[start:end]); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	return bucket, ctx
}

func repoFilter(repo string) *query.Query {
	q := query.New()
	q.Where = append(q.Where, query.NewExpressionCmp(query.OperatorEq, "repo", repo).Expression())

	return q
}

// TestSearchWithFilterRestrictsResults checks the basic contract: only records
// matching the filter come back, ranked by the same corpus-wide scores.
func TestSearchWithFilterRestrictsResults(t *testing.T) {
	docs := []*Doc{
		{ID: "a1", Repo: "web:jira", Title: "Gateway timeout", Body: "payment gateway timeout on capture"},
		{ID: "a2", Repo: "web:jira", Title: "Capture retry", Body: "capture retried after gateway timeout"},
		{ID: "b1", Repo: "acme/payments", Title: "Gateway client", Body: "gateway timeout handling in the client"},
	}
	bucket, ctx := newDocBucket(t, docs)

	all, total, err := bucket.Search(ctx, "gateway timeout", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || total != 3 {
		t.Fatalf("unfiltered search = %d results, total %d", len(all), total)
	}

	filtered, total, err := bucket.SearchWith(ctx, "gateway timeout", SearchOptions{
		Limit:  10,
		Filter: repoFilter("web:jira"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 2 {
		t.Fatalf("filtered search = %#v", filtered)
	}
	for _, hit := range filtered {
		if hit.Record.Repo != "web:jira" {
			t.Fatalf("filter leaked a record from %q", hit.Record.Repo)
		}
	}
	// Total is documented as the pre-filter hit count.
	if total != 3 {
		t.Fatalf("total = %d, want the unfiltered hit count", total)
	}

	// Scores must be the corpus-wide ones, not recomputed over the subset.
	byID := map[string]float64{}
	for _, hit := range all {
		byID[hit.Record.ID] = hit.Score
	}
	for _, hit := range filtered {
		if got, want := hit.Score, byID[hit.Record.ID]; got != want {
			t.Fatalf("%s scored %v when filtered, %v unfiltered", hit.Record.ID, got, want)
		}
	}
}

// TestSearchWithFilterPagingCountsFilteredResults pins that Offset skips
// records the caller actually sees, rather than raw hits.
func TestSearchWithFilterPagingCountsFilteredResults(t *testing.T) {
	var docs []*Doc
	for i := range 10 {
		repo := "other"
		if i%2 == 0 {
			repo = "wanted"
		}
		docs = append(docs, &Doc{
			ID:    fmt.Sprintf("d%02d", i),
			Repo:  repo,
			Title: fmt.Sprintf("alpha %d", i),
			Body:  strings.Repeat("alpha ", 10-i), // descending term frequency
		})
	}
	bucket, ctx := newDocBucket(t, docs)

	page1, _, err := bucket.SearchWith(ctx, "alpha", SearchOptions{Limit: 2, Filter: repoFilter("wanted")})
	if err != nil {
		t.Fatal(err)
	}
	page2, _, err := bucket.SearchWith(ctx, "alpha", SearchOptions{Limit: 2, Offset: 2, Filter: repoFilter("wanted")})
	if err != nil {
		t.Fatal(err)
	}

	if len(page1) != 2 || len(page2) != 2 {
		t.Fatalf("pages = %d and %d results", len(page1), len(page2))
	}

	seen := map[string]bool{}
	for _, hit := range append(append([]SearchResult[Doc]{}, page1...), page2...) {
		if hit.Record.Repo != "wanted" {
			t.Fatalf("filter leaked %q", hit.Record.Repo)
		}
		if seen[hit.Record.ID] {
			t.Fatalf("%s appeared on both pages", hit.Record.ID)
		}
		seen[hit.Record.ID] = true
	}

	// The two pages must be the head of the single-call ranking.
	full, _, err := bucket.SearchWith(ctx, "alpha", SearchOptions{Limit: 4, Filter: repoFilter("wanted")})
	if err != nil {
		t.Fatal(err)
	}
	for i, hit := range append(append([]SearchResult[Doc]{}, page1...), page2...) {
		if hit.Record.ID != full[i].Record.ID {
			t.Fatalf("paged order %v diverges from ranked order %v at %d", hit.Record.ID, full[i].Record.ID, i)
		}
	}
}

// TestSearchPagingIsConsistentWithSingleCall guards the unfiltered paging path
// after ranking was split from slicing.
func TestSearchPagingIsConsistentWithSingleCall(t *testing.T) {
	var docs []*Doc
	for i := range 25 {
		docs = append(docs, &Doc{
			ID:    fmt.Sprintf("d%02d", i),
			Repo:  "r",
			Title: fmt.Sprintf("beta %d", i),
			Body:  strings.Repeat("beta ", 25-i),
		})
	}
	bucket, ctx := newDocBucket(t, docs)

	full, total, err := bucket.Search(ctx, "beta", 25, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 25 {
		t.Fatalf("total = %d", total)
	}

	for offset := 0; offset < 25; offset += 5 {
		page, pageTotal, err := bucket.Search(ctx, "beta", 5, offset)
		if err != nil {
			t.Fatal(err)
		}
		if pageTotal != total {
			t.Fatalf("total drifted between pages: %d vs %d", pageTotal, total)
		}
		for i, hit := range page {
			if hit.Record.ID != full[offset+i].Record.ID {
				t.Fatalf("offset %d item %d = %s, want %s", offset, i, hit.Record.ID, full[offset+i].Record.ID)
			}
		}
	}

	if page, _, err := bucket.Search(ctx, "beta", 5, 100); err != nil || len(page) != 0 {
		t.Fatalf("offset past the end = %d results, err %v", len(page), err)
	}
}

// TestSearchHonoursContextCancellation is the regression test for a search that
// kept running after its caller had gone away. A cancelled request must not
// keep burning CPU over the posting lists.
func TestSearchHonoursContextCancellation(t *testing.T) {
	var docs []*Doc
	for i := range 5000 {
		docs = append(docs, &Doc{
			ID:    fmt.Sprintf("d%05d", i),
			Repo:  "r",
			Title: fmt.Sprintf("gamma %d", i),
			Body:  "gamma delta epsilon zeta",
		})
	}
	bucket, ctx := newDocBucket(t, docs)

	// Sanity: the query does match, so a cancellation failure cannot be
	// mistaken for an empty result.
	if _, total, err := bucket.Search(ctx, "gamma", 1, 0); err != nil || total == 0 {
		t.Fatalf("baseline search: total %d, err %v", total, err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	done := make(chan error, 1)
	go func() {
		_, _, err := bucket.Search(cancelled, "gamma delta epsilon zeta", 10, 0)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled search returned successfully instead of aborting")
		}
		if !strings.Contains(err.Error(), context.Canceled.Error()) {
			t.Fatalf("cancelled search error = %v, want context cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled search did not return")
	}
}

// TestSearchWalkStopsEarly pins the property the streaming API exists for: a
// caller that stops after a few results must not pay to materialise the rest
// of the hit set, however large it is.
func TestSearchWalkStopsEarly(t *testing.T) {
	var docs []*Doc
	for i := range 2000 {
		docs = append(docs, &Doc{
			ID:    fmt.Sprintf("d%05d", i),
			Repo:  "r",
			Title: fmt.Sprintf("iota %d", i),
			Body:  strings.Repeat("iota kappa ", 20),
		})
	}
	bucket, ctx := newDocBucket(t, docs)

	hydrated := 0
	total, err := bucket.SearchWalk(ctx, "iota kappa", SearchOptions{}, func(SearchResult[Doc]) (bool, error) {
		hydrated++

		return hydrated < 5, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2000 {
		t.Fatalf("total = %d, want every matching document counted", total)
	}
	if hydrated != 5 {
		t.Fatalf("walk hydrated %d records after being told to stop at 5", hydrated)
	}
}

// TestSearchWalkPropagatesCallbackError checks a caller can abort with a reason.
func TestSearchWalkPropagatesCallbackError(t *testing.T) {
	bucket, ctx := newDocBucket(t, []*Doc{{ID: "1", Repo: "r", Title: "lambda", Body: "lambda mu"}})

	sentinel := fmt.Errorf("stop now")
	if _, err := bucket.SearchWalk(ctx, "lambda", SearchOptions{}, func(SearchResult[Doc]) (bool, error) {
		return true, sentinel
	}); !strings.Contains(fmt.Sprint(err), "stop now") {
		t.Fatalf("callback error = %v", err)
	}
}

// TestSearchNilContext keeps the API usable from callers that pass no context.
func TestSearchNilContext(t *testing.T) {
	bucket, _ := newDocBucket(t, []*Doc{{ID: "1", Repo: "r", Title: "eta", Body: "eta theta"}})

	//nolint:staticcheck // exercising the nil-context tolerance on purpose.
	results, _, err := bucket.Search(nil, "eta", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %#v", results)
	}
}

// TestCountAndExistsUseTheIndex pins that the streaming readers go through the
// query planner. Count used to full-scan the bucket and decode every record
// even when the predicate was an equality on an indexed field, which made an
// existence probe cost the whole bucket.
func TestCountAndExistsUseTheIndex(t *testing.T) {
	var docs []*Doc
	for i := range 500 {
		repo := "bulk"
		if i == 499 {
			repo = "needle"
		}
		docs = append(docs, &Doc{
			ID:    fmt.Sprintf("d%03d", i),
			Repo:  repo,
			Title: fmt.Sprintf("doc %d", i),
			Body:  "omicron pi rho",
		})
	}
	bucket, ctx := newDocBucket(t, docs)

	n, err := bucket.Count(ctx, repoFilter("bulk"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 499 {
		t.Fatalf("count = %d, want 499", n)
	}

	if n, err := bucket.Count(ctx, nil); err != nil || n != 500 {
		t.Fatalf("unfiltered count = %d, err %v", n, err)
	}

	for _, tc := range []struct {
		repo string
		want bool
	}{{repo: "needle", want: true}, {repo: "bulk", want: true}, {repo: "absent", want: false}} {
		got, err := bucket.Exists(ctx, repoFilter(tc.repo))
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("Exists(%q) = %v, want %v", tc.repo, got, tc.want)
		}
	}

	if got, err := bucket.Exists(ctx, nil); err != nil || !got {
		t.Fatalf("Exists(nil) = %v, err %v; a non-empty bucket must report true", got, err)
	}
}

// BenchmarkExistsVsCount shows why the probe matters: Exists must stop at the
// first match instead of walking the whole partition.
func BenchmarkExistsVsCount(b *testing.B) {
	docs := make([]*Doc, 0, 20000)
	for i := range 20000 {
		docs = append(docs, &Doc{
			ID:    fmt.Sprintf("d%05d", i),
			Repo:  "bulk",
			Title: fmt.Sprintf("doc %d", i),
			Body:  "sigma tau upsilon",
		})
	}
	bucket, ctx := newDocBucket(b, docs)

	b.Run("Exists", func(b *testing.B) {
		for b.Loop() {
			if _, err := bucket.Exists(ctx, repoFilter("bulk")); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Count", func(b *testing.B) {
		for b.Loop() {
			if _, err := bucket.Count(ctx, repoFilter("bulk")); err != nil {
				b.Fatal(err)
			}
		}
	})
}
