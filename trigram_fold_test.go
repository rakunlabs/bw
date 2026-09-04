package bw

import (
	"regexp"
	"strings"
	"testing"
)

// The trigram index folds per byte over ASCII, while a case-insensitive
// regexp folds per rune with Unicode simple folding. The two disagree on
// exactly the letters whose fold orbit leaves ASCII — 'k' (U+212A KELVIN
// SIGN) and 's' (U+017F LATIN SMALL LETTER LONG S) today. These tests
// pin the query side's conservative resolution of that asymmetry: such a
// literal must contribute no trigram constraint, so the search degrades
// to a scan instead of dropping a document the regexp matches.

// TestTrigramFoldOrbitLeavesASCII pins the planner's degradation
// directly, so the loss of selectivity is visible and cannot be
// "optimised" back into a false negative.
func TestTrigramFoldOrbitLeavesASCII(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		// No fold requested: the bytes are compared verbatim, so 'k'
		// and 's' are ordinary letters and stay selective.
		{`kelvin`, "(elv AND kel AND lvi AND vin)"},
		{`sign`, "(ign AND sig)"},
		// Folded: 'k' folds with U+212A and 's' with U+017F, neither of
		// which the byte-wise index can spell, so no constraint holds.
		{`(?i)kelvin`, "ALL"},
		{`(?i)sign`, "ALL"},
		// A single tainted letter is enough to disarm the whole literal
		// run, because the run's trigrams span it.
		{`(?i)packages`, "ALL"},
		// Letters whose orbit stays inside ASCII are unaffected.
		{`(?i)abc`, "(abc)"},
		{`(?i)Foo`, "(foo)"},
		{`(?i)errtxntoobig`, "(big AND err AND nto AND obi AND oob AND rrt AND rtx AND too AND txn AND xnt)"},
		// A tainted atom only costs its own run: the surrounding
		// literals still constrain, since concatenation keeps them
		// contiguous in the matched text.
		{`(?i)float[0-9]`, "(flo AND loa AND oat)"},
		{`(?i)super[0-9]charge`, "(arg AND cha AND har AND rge)"},
		// One unconstrained alternative makes the alternation
		// unconstrained, which is the pre-existing rule.
		{`(?i)kelvin|border`, "ALL"},
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

// TestTrigramFoldOrbitMatchesUnicodeVariant is the end-to-end statement
// of the same defect: a document spelled with U+212A or U+017F matches
// the case-insensitive regexp, so the search must return it even though
// the index holds no posting for the folded ASCII trigrams.
func TestTrigramFoldOrbitMatchesUnicodeVariant(t *testing.T) {
	const kelvin = "\u212A" // KELVIN SIGN, folds with 'K'/'k'
	const longS = "\u017F"  // LATIN SMALL LETTER LONG S, folds with 'S'/'s'

	chunks := []*Chunk{
		{ID: "a#0", Repo: "r", Snippet: "func " + kelvin + "elvinScale(t float64) float64 {\n\treturn t + 273.15\n}"},
		{ID: "b#0", Repo: "r", Snippet: "func kelvinScale(t float64) float64 {\n\treturn t + 273.15\n}"},
		{ID: "c#0", Repo: "r", Snippet: "var " + longS + "ignature = []byte{0x7f}"},
		{ID: "d#0", Repo: "r", Snippet: "var signature = []byte{0x7f}"},
		{ID: "e#0", Repo: "r", Snippet: "func celsiusScale(t float64) float64 { return t }"},
	}
	bucket, ctx := newChunkBucket(t, chunks)

	cases := []struct {
		pattern string
		want    string
	}{
		{`kelvinscale`, "a#0,b#0"},
		{`signature`, "c#0,d#0"},
	}

	for _, tc := range cases {
		t.Run(tc.pattern, func(t *testing.T) {
			// The reference is Go's own case-insensitive regexp, which
			// uses simple folding: it is the definition of the right
			// answer the prefilter may not narrow away.
			re := regexp.MustCompile("(?i)" + tc.pattern)
			var want []string
			for _, c := range chunks {
				if re.MatchString(c.Snippet) {
					want = append(want, c.ID)
				}
			}
			if strings.Join(want, ",") != tc.want {
				t.Fatalf("reference regexp matched %v, want %s", want, tc.want)
			}

			ids, total := regexIDs(t, bucket, ctx, tc.pattern, RegexOptions{})
			if strings.Join(ids, ",") != tc.want || int(total) != len(want) {
				t.Fatalf("case-insensitive %q ids = %v total = %d, want %s", tc.pattern, ids, total, tc.want)
			}
		})
	}
}
