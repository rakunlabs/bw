package bw

import (
	"strings"
)

// Query syntax
// ------------
//
// bw full-text search accepts a small, predictable query language on top
// of the tokenizer. It is intentionally flat (no nested parentheses) so
// behaviour stays easy to reason about:
//
//	go rust            both terms required (AND, the default)
//	go OR rust         either term satisfies the OR group
//	db -postgres       require db, exclude docs containing postgres
//	db NOT postgres    same as above (NOT is an alias for the - prefix)
//	"v0.39.0"          phrase: the tokens must appear adjacently
//	"testcontainers go" phrase across words
//	test*              prefix/wildcard: any term starting with "test"
//	-"foo bar"         exclude docs containing the phrase "foo bar"
//
// Evaluation semantics for a document:
//   - it must satisfy every required clause (AND);
//   - for each OR group it must satisfy at least one alternative;
//   - it must satisfy none of the excluded clauses;
//   - its score is the sum of BM25 contributions of the positive
//     clauses it matched.
//
// A bare query with no operators behaves exactly like the previous
// implementation (all terms ANDed), so existing callers are unaffected.

// clauseKind classifies a single parsed clause.
type clauseKind int

const (
	clauseTerm   clauseKind = iota // single term, exact match
	clausePrefix                   // term* prefix match
	clausePhrase                   // "a b c" adjacency match
)

// queryClause is one atomic matcher within a parsed query.
type queryClause struct {
	kind clauseKind

	// term holds the single term (clauseTerm) or the prefix without its
	// trailing '*' (clausePrefix).
	term string

	// phrase holds the ordered tokens for clausePhrase.
	phrase []string
}

// parsedQuery is the fully parsed query: required clauses (AND), groups
// of alternatives (each an OR group), and excluded clauses (NOT).
type parsedQuery struct {
	required []queryClause
	orGroups [][]queryClause
	excluded []queryClause
}

// isEmpty reports whether the query has no positive clauses to match.
func (q *parsedQuery) isEmpty() bool {
	return len(q.required) == 0 && len(q.orGroups) == 0
}

// rawToken is one lexical unit produced by the lexer, before it is
// classified into a clause: the text plus flags for negation and quoting.
type rawToken struct {
	text     string
	negated  bool
	quoted   bool
	isOR     bool // the bare keyword OR
	isPrefix bool // had a trailing '*'
}

// lexQuery splits the query string into raw tokens, honouring double
// quotes (which may contain spaces), a leading '-' for negation, a
// trailing '*' for prefix, and the bare word OR / NOT.
func lexQuery(s string) []rawToken {
	var (
		out     []rawToken
		cur     strings.Builder
		inQuote bool
		negated bool
		quoted  bool
	)

	flush := func() {
		text := cur.String()
		cur.Reset()
		if text == "" && !quoted {
			negated = false
			return
		}
		// Recognise bare operator keywords only when unquoted.
		if !quoted {
			switch strings.ToUpper(text) {
			case "OR":
				out = append(out, rawToken{isOR: true})
				negated = false
				return
			case "NOT":
				// NOT negates the following clause.
				negated = true
				return
			}
		}
		isPrefix := false
		if !quoted && strings.HasSuffix(text, "*") && len(text) > 1 {
			isPrefix = true
			text = strings.TrimSuffix(text, "*")
		}
		out = append(out, rawToken{text: text, negated: negated, quoted: quoted, isPrefix: isPrefix})
		negated = false
		quoted = false
	}

	for _, r := range s {
		switch {
		case r == '"':
			if inQuote {
				inQuote = false
				quoted = true
				flush()
			} else {
				// A '-' immediately before the opening quote negates it;
				// that was captured while scanning the previous run.
				inQuote = true
			}
		case inQuote:
			cur.WriteRune(r)
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			flush()
		case r == '-' && cur.Len() == 0 && !negated:
			negated = true
		default:
			cur.WriteRune(r)
		}
	}
	if inQuote {
		// Unterminated quote: treat the remainder as a quoted phrase.
		quoted = true
	}
	flush()

	return out
}

// parseQuery lexes and classifies the query string into a parsedQuery,
// using tk to tokenize term and phrase text so index and query use the
// same normalisation. Returns an empty (isEmpty) query when nothing
// meaningful was provided.
func parseQuery(tk Tokenizer, s string) parsedQuery {
	raws := lexQuery(s)

	var (
		pq       parsedQuery
		pendOR   bool // previous raw token was OR
		lastPos  int  // index into required of the last positive clause, for OR grouping
		haveLast bool
	)

	appendPositive := func(c queryClause, negated bool) {
		if negated {
			pq.excluded = append(pq.excluded, c)
			haveLast = false
			return
		}
		if pendOR {
			pendOR = false
			// Attach to an OR group with the previous positive clause.
			if haveLast {
				prev := pq.required[lastPos]
				// Remove prev from required; start a new OR group.
				pq.required = append(pq.required[:lastPos], pq.required[lastPos+1:]...)
				pq.orGroups = append(pq.orGroups, []queryClause{prev, c})
				haveLast = false
				return
			}
			if len(pq.orGroups) > 0 {
				// Extend the most recent OR group.
				g := &pq.orGroups[len(pq.orGroups)-1]
				*g = append(*g, c)
				return
			}
			// OR with nothing on the left: treat as a plain required.
		}
		pq.required = append(pq.required, c)
		lastPos = len(pq.required) - 1
		haveLast = true
	}

	for _, rt := range raws {
		if rt.isOR {
			pendOR = true
			continue
		}

		switch {
		case rt.quoted:
			// Build the phrase from position-ordered tokens. A quoted
			// string that is really a single "word" (e.g. "v0.39.0",
			// which tokenizes to the whole compound plus its fragments
			// all sharing overlapping positions) must NOT become a
			// phrase — collapse it to its compound term instead. We
			// detect this via the number of distinct token positions.
			pts := tokenizePositions(tk, rt.text)
			if len(pts) == 0 {
				continue
			}
			phrase, distinct := phraseTokens(pts)
			if distinct <= 1 {
				appendPositive(queryClause{kind: clauseTerm, term: longestToken(tokenStrings(pts))}, rt.negated)
			} else {
				appendPositive(queryClause{kind: clausePhrase, phrase: phrase}, rt.negated)
			}
		case rt.isPrefix:
			// Normalise the prefix through the tokenizer's lowercasing,
			// but keep it as a single unit (take the first token).
			norm := strings.ToLower(rt.text)
			if toks := tk.Tokenize(rt.text); len(toks) == 1 {
				norm = toks[0]
			}
			appendPositive(queryClause{kind: clausePrefix, term: norm}, rt.negated)
		default:
			// A bare word may itself tokenize into several terms (e.g.
			// "v0.39.0" -> v0.39.0 plus sub-tokens, or "transaction_shared"
			// -> transaction, shared, transaction_shared).
			pts := tokenizePositions(tk, rt.text)
			switch len(pts) {
			case 0:
				continue
			case 1:
				appendPositive(queryClause{kind: clauseTerm, term: pts[0].Term}, rt.negated)
			default:
				// The word is a compound. Match it two ways, ORed:
				//   1. the whole "emit both" compound token as an exact
				//      term, so "v0.39.0" matches precisely and legacy
				//      postings without positions still work; and
				//   2. the ordered sub-fragments as a phrase, so a word
				//      that only ever appears embedded in a longer path
				//      (e.g. "transaction_shared" inside
				//      ".../transaction_shared.git", which is only indexed
				//      as the leaf fragments plus the full path) is still
				//      found via its adjacent fragments.
				exact := queryClause{kind: clauseTerm, term: longestToken(tokenStrings(pts))}
				phrase, distinct := phraseTokens(pts)
				if distinct <= 1 {
					appendPositive(exact, rt.negated)
					break
				}
				alt := queryClause{kind: clausePhrase, phrase: phrase}
				if rt.negated || pendOR {
					// Negation and OR-chaining operate on a single clause;
					// fall back to the exact compound to keep grouping
					// semantics simple and predictable.
					appendPositive(exact, rt.negated)
					break
				}
				pq.orGroups = append(pq.orGroups, []queryClause{exact, alt})
				haveLast = false
			}
		}
	}

	return pq
}

// longestToken returns the longest string in toks (ties: first seen).
func longestToken(toks []string) string {
	best := toks[0]
	for _, t := range toks[1:] {
		if len(t) > len(best) {
			best = t
		}
	}
	return best
}

// tokenStrings extracts just the term strings from a token slice.
func tokenStrings(toks []Token) []string {
	out := make([]string, len(toks))
	for i, t := range toks {
		out[i] = t.Term
	}
	return out
}

// phraseTokens reduces position-tagged tokens to one representative term
// per distinct position (the shortest fragment at that position, which is
// what the index stores for adjacency), returning the ordered phrase and
// the count of distinct positions. Using the shortest fragment keeps
// phrase adjacency aligned with the sub-token positions emitted at index
// time (compound tokens share the first fragment's position and would
// break consecutive matching).
func phraseTokens(toks []Token) (phrase []string, distinct int) {
	// Group by position, keeping the shortest term per position (the
	// fragment the index stores at that exact position).
	byPos := map[uint32]string{}
	var order []uint32
	for _, t := range toks {
		cur, ok := byPos[t.Pos]
		if !ok {
			byPos[t.Pos] = t.Term
			order = append(order, t.Pos)
			continue
		}
		if len(t.Term) < len(cur) {
			byPos[t.Pos] = t.Term
		}
	}
	sortUint32(order)
	phrase = make([]string, 0, len(order))
	for _, p := range order {
		phrase = append(phrase, byPos[p])
	}
	return phrase, len(order)
}

// sortUint32 sorts a small slice of positions ascending (insertion sort;
// phrase lengths are tiny).
func sortUint32(a []uint32) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j-1] > a[j]; j-- {
			a[j-1], a[j] = a[j], a[j-1]
		}
	}
}
