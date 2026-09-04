package bw

import (
	"regexp/syntax"
	"slices"
	"strings"
	"unicode"
)

// Planning a regular expression into a trigram query is the classic
// code-search trick: a pattern that must match some literal run of three
// or more bytes can only match documents that contain that run's
// trigrams, so the index answers "which documents are worth reading"
// before the regexp engine reads any of them.
//
// The planner is deliberately conservative. Every construct it does not
// understand widens to "no constraint", which costs a scan but can never
// lose a match — the regexp itself is the only thing that decides a
// match. What it does understand is the shape real searches take:
// literals, literal runs inside a concatenation, alternations of
// literals, captures, and repetitions that are guaranteed to occur at
// least once.
//
// It does not derive constraints across a non-literal atom (so `ab.cd`
// yields nothing, since neither run reaches three bytes), and it does not
// enumerate character classes. Both are recoverable losses of
// selectivity, never of correctness.

type triOp uint8

const (
	// triAll matches every document: the pattern gave nothing to filter on.
	triAll triOp = iota
	// triNone matches no document (an impossible pattern).
	triNone
	// triAnd requires every trigram in tri and every sub-query in sub.
	triAnd
	// triOr requires any trigram in tri or any sub-query in sub.
	triOr
)

// triQuery is a boolean expression over trigrams. Leaves live in tri
// rather than in dedicated nodes so the common shape — one literal, a
// handful of required trigrams — is a single allocation.
type triQuery struct {
	op  triOp
	tri []uint32
	sub []*triQuery
}

// triMaxLiteralTrigrams bounds how many trigrams one literal contributes.
//
// Overlapping trigrams of a literal are highly correlated, so past a
// handful they add cost without adding selectivity: each one is a point
// read per candidate at intersection time. Long literals are therefore
// sampled at non-overlapping stride, which keeps the trigrams
// independent, and then capped.
const triMaxLiteralTrigrams = 32

// triLiteralStrideAbove is the literal length past which trigrams are
// taken at stride 3 instead of at every offset.
const triLiteralStrideAbove = 16

func triQueryAll() *triQuery  { return &triQuery{op: triAll} }
func triQueryNone() *triQuery { return &triQuery{op: triNone} }

// regexTrigramQuery plans pattern into a trigram query. The pattern is
// parsed with Perl syntax, matching regexp.Compile, so a caller can hand
// the same string to both.
func regexTrigramQuery(pattern string) (*triQuery, error) {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil, err
	}

	return triRequired(re.Simplify()), nil
}

// triRequired returns a query every string matching re must satisfy.
func triRequired(re *syntax.Regexp) *triQuery {
	switch re.Op {
	case syntax.OpNoMatch:
		return triQueryNone()

	case syntax.OpCharClass:
		// An empty class matches nothing, so neither does the pattern
		// containing it. Non-empty classes are never enumerated.
		if len(re.Rune) == 0 {
			return triQueryNone()
		}

		return triQueryAll()

	case syntax.OpLiteral:
		s, ok := triLiteralString(re)
		if !ok {
			return triQueryAll()
		}

		return triQueryLiteral(s)

	case syntax.OpCapture:
		return triRequired(re.Sub[0])

	case syntax.OpPlus:
		// One occurrence is guaranteed.
		return triRequired(re.Sub[0])

	case syntax.OpRepeat:
		if re.Min >= 1 {
			return triRequired(re.Sub[0])
		}

		return triQueryAll()

	case syntax.OpAlternate:
		parts := make([]*triQuery, 0, len(re.Sub))
		for _, sub := range re.Sub {
			parts = append(parts, triRequired(sub))
		}

		return triOrOf(parts)

	case syntax.OpConcat:
		return triConcat(re.Sub)
	}

	// OpEmptyMatch, the anchors and word boundaries (which consume
	// nothing), the character classes and the zero-or-more repetitions
	// all leave the candidate set unconstrained.
	return triQueryAll()
}

// triConcat plans a concatenation, merging adjacent literals first.
//
// Merging is what makes `func (` or `errors.Is(` a usable constraint:
// the parser splits a pattern into literal and non-literal atoms, and a
// pair of literals that sit next to each other is contiguous in the
// matched text, so their trigrams include the ones spanning the join.
// Merging stops at any non-literal atom, which is the conservative
// choice — nothing is assumed about what a class or a repetition
// contributes.
func triConcat(subs []*syntax.Regexp) *triQuery {
	atoms := make([]*syntax.Regexp, 0, len(subs))
	triFlattenConcat(subs, &atoms)

	parts := make([]*triQuery, 0, len(atoms))
	var run strings.Builder
	flush := func() {
		if run.Len() > 0 {
			parts = append(parts, triQueryLiteral(run.String()))
			run.Reset()
		}
	}
	for _, atom := range atoms {
		if atom.Op == syntax.OpLiteral {
			if s, ok := triLiteralString(atom); ok {
				run.WriteString(s)

				continue
			}
		}
		flush()
		parts = append(parts, triRequired(atom))
	}
	flush()

	return triAndOf(parts)
}

// triFlattenConcat expands nested concatenations and unwraps capture
// groups, which are transparent to matching, so adjacent literals hidden
// behind them still merge.
func triFlattenConcat(subs []*syntax.Regexp, out *[]*syntax.Regexp) {
	for _, sub := range subs {
		switch sub.Op {
		case syntax.OpConcat, syntax.OpCapture:
			triFlattenConcat(sub.Sub, out)
		default:
			*out = append(*out, sub)
		}
	}
}

// triLiteralString renders a literal node's runes as a string. It fails
// — degrading that literal to no constraint — when the literal is
// case-folded and one of its runes cannot be represented by the
// byte-wise index: either the rune is itself non-ASCII, or its simple
// fold orbit reaches outside ASCII.
//
// The two folds are deliberately asymmetric. The write side folds one
// byte at a time over A-Z (foldASCII), which is what keeps a trigram
// exactly three bytes wide and the index cheap. A case-insensitive
// regexp folds per rune under Unicode simple folding, a strictly larger
// relation: `(?i)k` also matches U+212A KELVIN SIGN and `(?i)s` also
// matches U+017F LATIN SMALL LETTER LONG S. A document spelled with
// those runes therefore matches the regexp while carrying no posting
// for the folded ASCII trigrams, so requiring the literal's trigrams
// would drop a real match.
//
// The query side is the one that has to give way, because it is the
// only side that can be conservative without making every write pay
// for Unicode folding: the literal contributes nothing, the branch it
// sits in is answered by a scan, and the regexp still decides the
// match. That is a loss of selectivity, which the package accepts, and
// not a wrong answer, which it does not.
func triLiteralString(re *syntax.Regexp) (string, bool) {
	fold := re.Flags&syntax.FoldCase != 0
	var b strings.Builder
	for _, r := range re.Rune {
		if fold && triFoldEscapesASCII(r) {
			return "", false
		}
		b.WriteRune(r)
	}

	return b.String(), true
}

// triFoldEscapesASCII reports whether case-folding r can reach a rune
// the byte-wise index cannot spell.
//
// The orbit is walked with unicode.SimpleFold rather than testing for
// 'k' and 's' directly: the pair is a property of the current Unicode
// tables, not of this package, and a hard-coded pair would rot silently
// if the tables ever grew another ASCII letter with a non-ASCII fold.
func triFoldEscapesASCII(r rune) bool {
	if r > unicode.MaxASCII {
		return true
	}
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		if f > unicode.MaxASCII {
			return true
		}
	}

	return false
}

// triQueryLiteral requires the trigrams of one literal run.
func triQueryLiteral(s string) *triQuery {
	tri := triSampleLiteral(s)
	if len(tri) == 0 {
		return triQueryAll()
	}

	return &triQuery{op: triAnd, tri: tri}
}

// triSampleLiteral returns the trigrams to require for a literal run:
// all of them for a short run, a strided sample for a long one.
func triSampleLiteral(s string) []uint32 {
	if len(s) < 3 {
		return nil
	}
	if len(s) <= triLiteralStrideAbove {
		return trigramSet(s)
	}

	out := make([]uint32, 0, triMaxLiteralTrigrams)
	for i := 0; i+3 <= len(s) && len(out) < triMaxLiteralTrigrams; i += 3 {
		out = append(out, uint32(foldASCII(s[i]))<<16|uint32(foldASCII(s[i+1]))<<8|uint32(foldASCII(s[i+2])))
	}
	slices.Sort(out)

	return slices.Compact(out)
}

// triAndOf conjoins parts, flattening nested conjunctions and dropping
// unconstrained ones.
func triAndOf(parts []*triQuery) *triQuery {
	out := &triQuery{op: triAnd}
	for _, p := range parts {
		switch p.op {
		case triAll:
			continue
		case triNone:
			return triQueryNone()
		case triAnd:
			out.tri = append(out.tri, p.tri...)
			out.sub = append(out.sub, p.sub...)
		default:
			out.sub = append(out.sub, p)
		}
	}
	slices.Sort(out.tri)
	out.tri = slices.Compact(out.tri)

	switch {
	case len(out.tri) == 0 && len(out.sub) == 0:
		return triQueryAll()
	case len(out.tri) == 0 && len(out.sub) == 1:
		return out.sub[0]
	}

	return out
}

// triOrOf disjoins parts. A single unconstrained branch makes the whole
// disjunction unconstrained, which is why an alternation with one
// unanchored branch filters nothing.
func triOrOf(parts []*triQuery) *triQuery {
	out := &triQuery{op: triOr}
	for _, p := range parts {
		switch p.op {
		case triAll:
			return triQueryAll()
		case triNone:
			continue
		case triOr:
			out.tri = append(out.tri, p.tri...)
			out.sub = append(out.sub, p.sub...)
		case triAnd:
			// A conjunction of exactly one trigram is that trigram.
			if len(p.sub) == 0 && len(p.tri) == 1 {
				out.tri = append(out.tri, p.tri[0])

				continue
			}
			out.sub = append(out.sub, p)
		default:
			out.sub = append(out.sub, p)
		}
	}
	slices.Sort(out.tri)
	out.tri = slices.Compact(out.tri)

	switch {
	case len(out.tri) == 0 && len(out.sub) == 0:
		return triQueryNone()
	case len(out.tri) == 1 && len(out.sub) == 0:
		return &triQuery{op: triAnd, tri: out.tri}
	case len(out.tri) == 0 && len(out.sub) == 1:
		return out.sub[0]
	}

	return out
}

// String renders a query for tests and diagnostics.
func (q *triQuery) String() string {
	switch q.op {
	case triAll:
		return "ALL"
	case triNone:
		return "NONE"
	}

	parts := make([]string, 0, len(q.tri)+len(q.sub))
	for _, t := range q.tri {
		parts = append(parts, string([]byte{byte(t >> 16), byte(t >> 8), byte(t)}))
	}
	for _, s := range q.sub {
		parts = append(parts, s.String())
	}
	sep := " AND "
	if q.op == triOr {
		sep = " OR "
	}

	return "(" + strings.Join(parts, sep) + ")"
}
