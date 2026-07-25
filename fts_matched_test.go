package bw

import (
	"fmt"
	"strings"
	"testing"
)

// matchedDocs indexes n documents per repository, all matching the same query,
// so a filtered search has to walk far past its page to know the true total.
func matchedDocs(t *testing.T, perRepo int) (*Bucket[Doc], []string) {
	t.Helper()

	repos := []string{"alpha", "beta"}

	var docs []*Doc
	for _, repo := range repos {
		for i := range perRepo {
			docs = append(docs, &Doc{
				ID:    fmt.Sprintf("%s/d%05d", repo, i),
				Repo:  repo,
				Title: fmt.Sprintf("iota %d", i),
				Body:  strings.Repeat("iota kappa lambda ", 40),
			})
		}
	}

	bucket, _ := newDocBucket(t, docs)

	return bucket, repos
}

// TestSearchMatchedCountsPastThePage is the point of SearchOptions.Matched: a
// paginated filtered search must be able to report an exact total without
// being handed — and therefore fully decoding — every match.
func TestSearchMatchedCountsPastThePage(t *testing.T) {
	const perRepo = 500

	bucket, _ := matchedDocs(t, perRepo)

	var (
		matched   uint64
		delivered int
	)

	total, err := bucket.SearchWalk(t.Context(), "iota kappa", SearchOptions{
		Filter:  repoFilter("alpha"),
		Limit:   20,
		Matched: &matched,
	}, func(SearchResult[Doc]) (bool, error) {
		delivered++

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if delivered != 20 {
		t.Errorf("hydrated %d records, want only the page of 20", delivered)
	}
	if matched != perRepo {
		t.Errorf("Matched = %d, want every hit passing the filter (%d)", matched, perRepo)
	}
	// The returned total keeps its documented meaning: hits before the filter.
	if total != perRepo*2 {
		t.Errorf("returned total = %d, want the pre-filter count %d", total, perRepo*2)
	}
}

// TestSearchMatchedRespectsOffset checks the count is independent of paging:
// page five must report the same total as page one.
func TestSearchMatchedRespectsOffset(t *testing.T) {
	const perRepo = 300

	bucket, _ := matchedDocs(t, perRepo)

	counts := map[int]uint64{}
	for _, page := range []int{0, 1, 4} {
		var matched uint64
		delivered := 0

		if _, err := bucket.SearchWalk(t.Context(), "iota kappa", SearchOptions{
			Filter:  repoFilter("beta"),
			Limit:   20,
			Offset:  page * 20,
			Matched: &matched,
		}, func(SearchResult[Doc]) (bool, error) {
			delivered++

			return true, nil
		}); err != nil {
			t.Fatal(err)
		}

		if delivered != 20 {
			t.Errorf("page %d hydrated %d records, want 20", page, delivered)
		}
		counts[page] = matched
	}

	for page, got := range counts {
		if got != perRepo {
			t.Errorf("page %d reported %d matches, want %d", page, got, perRepo)
		}
	}
}

// TestSearchMatchedIsExactWhenEverythingFits guards the ordinary case, where
// the page covers every match and the walk never enters counting mode.
func TestSearchMatchedIsExactWhenEverythingFits(t *testing.T) {
	bucket, _ := matchedDocs(t, 7)

	var matched uint64
	if _, err := bucket.SearchWalk(t.Context(), "iota kappa", SearchOptions{
		Filter:  repoFilter("alpha"),
		Limit:   100,
		Matched: &matched,
	}, func(SearchResult[Doc]) (bool, error) {
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}

	if matched != 7 {
		t.Errorf("Matched = %d, want 7", matched)
	}
}

// TestSearchWalkWithoutMatchedStillStopsEarly is the regression guard for the
// property counting could have cost: a caller that does not ask for a total
// must still stop dead when it says so, however large the hit set is.
func TestSearchWalkWithoutMatchedStillStopsEarly(t *testing.T) {
	bucket, _ := matchedDocs(t, 1000)

	hydrated := 0
	if _, err := bucket.SearchWalk(t.Context(), "iota kappa", SearchOptions{
		Filter: repoFilter("alpha"),
	}, func(SearchResult[Doc]) (bool, error) {
		hydrated++

		return hydrated < 5, nil
	}); err != nil {
		t.Fatal(err)
	}

	if hydrated != 5 {
		t.Errorf("walk hydrated %d records after being told to stop at 5", hydrated)
	}
}

// TestSearchMatchedFinishesCountingAfterAnEarlyStop covers the interaction of
// the two: a caller that stops early but wants the total gets both.
func TestSearchMatchedFinishesCountingAfterAnEarlyStop(t *testing.T) {
	const perRepo = 200

	bucket, _ := matchedDocs(t, perRepo)

	var matched uint64
	hydrated := 0

	if _, err := bucket.SearchWalk(t.Context(), "iota kappa", SearchOptions{
		Filter:  repoFilter("alpha"),
		Matched: &matched,
	}, func(SearchResult[Doc]) (bool, error) {
		hydrated++

		return hydrated < 5, nil
	}); err != nil {
		t.Fatal(err)
	}

	if hydrated != 5 {
		t.Errorf("walk hydrated %d records after being told to stop at 5", hydrated)
	}
	if matched != perRepo {
		t.Errorf("Matched = %d, want %d", matched, perRepo)
	}
}

// TestSearchMatchedWithoutAFilter reports the same figure as the returned
// total, since with nothing to filter the two are the same thing.
func TestSearchMatchedWithoutAFilter(t *testing.T) {
	const perRepo = 40

	bucket, _ := matchedDocs(t, perRepo)

	var matched uint64
	total, err := bucket.SearchWalk(t.Context(), "iota kappa", SearchOptions{
		Limit:   10,
		Matched: &matched,
	}, func(SearchResult[Doc]) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if matched != total {
		t.Errorf("Matched = %d but total = %d; without a filter they must agree", matched, total)
	}
}

// prefixOf returns the key filter a key-partitioned index uses: matchedDocs
// writes ids as "<repo>/dNNNNN", so the repository is decidable from the key
// alone. The trailing separator keeps "alpha" from matching "alphabet".
func prefixOf(repo string) func(string) bool {
	return func(id string) bool { return strings.HasPrefix(id, repo+"/") }
}

// TestKeyFilterSelectsTheSameRecordsAsAFieldFilter is the correctness anchor:
// the cheap filter and the expensive one must not disagree.
func TestKeyFilterSelectsTheSameRecordsAsAFieldFilter(t *testing.T) {
	const perRepo = 120

	bucket, _ := matchedDocs(t, perRepo)

	collect := func(opts SearchOptions) ([]string, uint64) {
		var (
			ids     []string
			matched uint64
		)
		opts.Matched = &matched
		if _, err := bucket.SearchWalk(t.Context(), "iota kappa", opts,
			func(hit SearchResult[Doc]) (bool, error) {
				ids = append(ids, hit.Record.ID)

				return true, nil
			}); err != nil {
			t.Fatal(err)
		}

		return ids, matched
	}

	byField, fieldCount := collect(SearchOptions{Filter: repoFilter("alpha"), Limit: 30})
	byKey, keyCount := collect(SearchOptions{KeyFilter: prefixOf("alpha"), Limit: 30})

	if fieldCount != keyCount {
		t.Errorf("counts differ: field filter %d, key filter %d", fieldCount, keyCount)
	}
	if keyCount != perRepo {
		t.Errorf("key filter counted %d, want %d", keyCount, perRepo)
	}
	if len(byField) != len(byKey) {
		t.Fatalf("delivered %d vs %d records", len(byField), len(byKey))
	}
	for i := range byField {
		if byField[i] != byKey[i] {
			t.Fatalf("rank %d differs: field filter %s, key filter %s", i, byField[i], byKey[i])
		}
	}
}

// TestKeyFilterCountsWithoutReadingRecords is the point of the option. A hit
// the key rejects, and a hit that is only being counted, must never be fetched
// or decoded — otherwise an exact total over a large index costs a point read
// per hit and the filter is no cheaper than the field one.
func TestKeyFilterCountsWithoutReadingRecords(t *testing.T) {
	const perRepo = 400

	bucket, _ := matchedDocs(t, perRepo)

	decoded := 0
	var matched uint64

	if _, err := bucket.SearchWalk(t.Context(), "iota kappa", SearchOptions{
		KeyFilter: prefixOf("alpha"),
		Limit:     20,
		Matched:   &matched,
	}, func(SearchResult[Doc]) (bool, error) {
		decoded++

		return true, nil
	}); err != nil {
		t.Fatal(err)
	}

	if matched != perRepo {
		t.Errorf("Matched = %d, want %d", matched, perRepo)
	}
	if decoded != 20 {
		t.Errorf("hydrated %d records, want only the page of 20", decoded)
	}
}

// TestKeyFilterCombinesWithAFieldFilter checks the two compose: the key
// narrows cheaply, the field decides the rest.
func TestKeyFilterCombinesWithAFieldFilter(t *testing.T) {
	bucket, _ := matchedDocs(t, 50)

	var matched uint64
	if _, err := bucket.SearchWalk(t.Context(), "iota kappa", SearchOptions{
		KeyFilter: prefixOf("alpha"),
		Filter:    repoFilter("beta"), // contradicts the key filter
		Limit:     10,
		Matched:   &matched,
	}, func(SearchResult[Doc]) (bool, error) {
		t.Error("a record passed two contradictory filters")

		return true, nil
	}); err != nil {
		t.Fatal(err)
	}

	if matched != 0 {
		t.Errorf("Matched = %d, want 0 when the filters contradict", matched)
	}
}

// TestKeyFilterPagesConsistently makes sure Offset counts key-filtered hits,
// so paging lines up with what the caller is shown.
func TestKeyFilterPagesConsistently(t *testing.T) {
	const perRepo = 90

	bucket, _ := matchedDocs(t, perRepo)

	page := func(offset int) []string {
		var ids []string
		if _, err := bucket.SearchWalk(t.Context(), "iota kappa", SearchOptions{
			KeyFilter: prefixOf("beta"),
			Offset:    offset,
			Limit:     10,
		}, func(hit SearchResult[Doc]) (bool, error) {
			ids = append(ids, hit.Record.ID)

			return true, nil
		}); err != nil {
			t.Fatal(err)
		}

		return ids
	}

	first, second := page(0), page(10)
	if len(first) != 10 || len(second) != 10 {
		t.Fatalf("pages of %d and %d, want 10 each", len(first), len(second))
	}

	seen := map[string]struct{}{}
	for _, id := range first {
		if !strings.HasPrefix(id, "beta/") {
			t.Errorf("%s leaked past the key filter", id)
		}
		seen[id] = struct{}{}
	}
	for _, id := range second {
		if _, dup := seen[id]; dup {
			t.Errorf("%s appears on both pages", id)
		}
	}
}
