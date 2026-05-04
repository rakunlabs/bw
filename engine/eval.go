package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rakunlabs/query"
)

// Eval reports whether the given record (decoded into a map[string]any)
// satisfies the supplied query expressions. The semantics mirror
// adaptergoqu's translation to SQL but evaluated in Go memory.
//
// An empty Where slice matches everything.
func Eval(record map[string]any, where []query.Expression) (bool, error) {
	if len(where) == 0 {
		return true, nil
	}

	// Multiple top-level expressions are AND-combined, matching how the
	// query parser places them.
	for _, e := range where {
		ok, err := evalExpr(record, e)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func evalExpr(record map[string]any, e query.Expression) (bool, error) {
	switch ex := e.(type) {
	case *query.ExpressionCmp:
		return evalCmp(record, ex)
	case *query.ExpressionLogic:
		return evalLogic(record, ex)
	case nil:
		return true, nil
	default:
		return false, fmt.Errorf("engine: unsupported expression type %T", e)
	}
}

func evalLogic(record map[string]any, e *query.ExpressionLogic) (bool, error) {
	switch e.Operator {
	case query.OperatorAnd:
		for _, sub := range e.List {
			ok, err := evalExpr(record, sub)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}

		return true, nil
	case query.OperatorOr:
		if len(e.List) == 0 {
			return true, nil
		}
		for _, sub := range e.List {
			ok, err := evalExpr(record, sub)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}

		return false, nil
	default:
		return false, fmt.Errorf("engine: unknown logic operator %q", e.Operator)
	}
}

func evalCmp(record map[string]any, e *query.ExpressionCmp) (bool, error) {
	got, present := Lookup(record, e.Field)

	switch e.Operator {
	case query.OperatorIs:
		// IS NULL semantics: field absent or explicitly nil.
		return !present || got == nil, nil
	case query.OperatorIsNot:
		return present && got != nil, nil
	}

	if !present {
		// Missing fields cannot match positive comparisons; for negated
		// operators (ne, nin, nlike, nilike, njin) treat missing as
		// "different from any value", i.e. the predicate succeeds. This
		// matches how SQL's NULL semantics tend to surprise users; we
		// pick the more practical Go-style answer.
		switch e.Operator {
		case query.OperatorNe, query.OperatorNIn, query.OperatorNLike, query.OperatorNILike, query.OperatorNJIn:
			return true, nil
		default:
			return false, nil
		}
	}

	switch e.Operator {
	case query.OperatorEq, query.OperatorEmpty:
		return equalAny(got, e.Value), nil
	case query.OperatorNe:
		return !equalAny(got, e.Value), nil
	case query.OperatorGt, query.OperatorGte, query.OperatorLt, query.OperatorLte:
		return compareOp(got, e.Value, e.Operator)
	case query.OperatorLike:
		return matchLike(got, e.Value, false, false), nil
	case query.OperatorILike:
		return matchLike(got, e.Value, true, false), nil
	case query.OperatorNLike:
		return matchLike(got, e.Value, false, true), nil
	case query.OperatorNILike:
		return matchLike(got, e.Value, true, true), nil
	case query.OperatorIn:
		return inAny(got, e.Value), nil
	case query.OperatorNIn:
		return !inAny(got, e.Value), nil
	case query.OperatorKV:
		// JSONB containment: the field must contain every key/value of the
		// supplied JSON object.
		return jsonContains(got, e.Value)
	case query.OperatorJIn:
		return jArrayHasAny(got, e.Value), nil
	case query.OperatorNJIn:
		return !jArrayHasAny(got, e.Value), nil
	}

	return false, fmt.Errorf("engine: unsupported comparison operator %q", e.Operator)
}

// equalAny compares two values from possibly different sources (decoded
// record vs. parsed query value). Numeric values, booleans and strings are
// compared in their natural ways with cross-type coercion through string
// representation as the last resort.
func equalAny(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}

	// Numeric path: coerce both sides to float64 if possible.
	if af, aok := toFloat(a); aok {
		if bf, bok := toFloat(b); bok {
			return af == bf
		}
	}

	// Boolean path.
	if ab, aok := a.(bool); aok {
		if bb, bok := b.(bool); bok {
			return ab == bb
		}
	}

	// Fall back to string comparison; this also covers the common case
	// where the query parser has the value as string and the record has
	// the value as string.
	return toString(a) == toString(b)
}

func compareOp(got, want any, op query.OperatorCmpType) (bool, error) {
	c, ok := compareValues(got, want)
	if !ok {
		return false, fmt.Errorf("engine: cannot compare %T and %T with %q", got, want, op)
	}
	switch op {
	case query.OperatorGt:
		return c > 0, nil
	case query.OperatorGte:
		return c >= 0, nil
	case query.OperatorLt:
		return c < 0, nil
	case query.OperatorLte:
		return c <= 0, nil
	}

	return false, fmt.Errorf("engine: not a comparison operator: %q", op)
}

// compareValues returns -1/0/+1 if a and b are comparable, and (0, false)
// otherwise. Numbers compare numerically (with cross-coercion), strings
// compare lexicographically.
func compareValues(a, b any) (int, bool) {
	if af, aok := toFloat(a); aok {
		if bf, bok := toFloat(b); bok {
			switch {
			case af < bf:
				return -1, true
			case af > bf:
				return 1, true
			default:
				return 0, true
			}
		}
	}

	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			return strings.Compare(as, bs), true
		}
	}

	// json.Number can show up if the JSON codec is used.
	if an, aok := a.(json.Number); aok {
		if bn, bok := b.(json.Number); bok {
			af, errA := an.Float64()
			bf, errB := bn.Float64()
			if errA == nil && errB == nil {
				switch {
				case af < bf:
					return -1, true
				case af > bf:
					return 1, true
				default:
					return 0, true
				}
			}
		}
	}

	return 0, false
}

func inAny(got, list any) bool {
	switch lst := list.(type) {
	case []string:
		for _, v := range lst {
			if equalAny(got, v) {
				return true
			}
		}

		return false
	case []bool:
		for _, v := range lst {
			if equalAny(got, v) {
				return true
			}
		}

		return false
	case []any:
		for _, v := range lst {
			if equalAny(got, v) {
				return true
			}
		}

		return false
	default:
		return equalAny(got, list)
	}
}

// matchLike implements SQL LIKE semantics with '%' (any) and '_' (one
// character) wildcards. caseI enables case-insensitive comparison; negate
// inverts the result.
func matchLike(got any, pattern any, caseI, negate bool) bool {
	subject := toString(got)
	pat := toString(pattern)
	if caseI {
		subject = strings.ToLower(subject)
		pat = strings.ToLower(pat)
	}
	res := likeMatch(subject, pat)
	if negate {
		return !res
	}

	return res
}

func likeMatch(s, pat string) bool {
	// Standard recursive backtracker with memoization not strictly needed
	// for typical patterns; iterative two-pointer with backtrack handles
	// '%' and '_' in O(n*m) worst case.
	si, pi := 0, 0
	starS, starP := -1, -1
	for si < len(s) {
		switch {
		case pi < len(pat) && pat[pi] == '_':
			si++
			pi++
		case pi < len(pat) && pat[pi] == '%':
			starP = pi
			starS = si
			pi++
		case pi < len(pat) && pat[pi] == s[si]:
			si++
			pi++
		case starP != -1:
			pi = starP + 1
			starS++
			si = starS
		default:
			return false
		}
	}
	for pi < len(pat) && pat[pi] == '%' {
		pi++
	}

	return pi == len(pat)
}

// jsonContains reports whether got "contains" want in the JSONB @>
// containment sense:
//   - if want is a JSON object, every key/value pair must equal the
//     corresponding key in got;
//   - if want is a JSON array, every element of want must be in got
//     (which must also be an array);
//   - otherwise, equality.
func jsonContains(got, want any) (bool, error) {
	wantStr, ok := want.(string)
	if !ok {
		// Already decoded.
		return jsonContainsValue(got, want), nil
	}

	var w any
	if err := json.Unmarshal([]byte(wantStr), &w); err != nil {
		return false, fmt.Errorf("engine: kv operator: invalid JSON: %w", err)
	}

	return jsonContainsValue(got, w), nil
}

func jsonContainsValue(got, want any) bool {
	switch w := want.(type) {
	case map[string]any:
		gm, ok := got.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range w {
			gv, present := gm[k]
			if !present {
				return false
			}
			if !jsonContainsValue(gv, v) {
				return false
			}
		}

		return true
	case []any:
		ga, ok := got.([]any)
		if !ok {
			return false
		}
		for _, want := range w {
			found := false
			for _, gv := range ga {
				if jsonContainsValue(gv, want) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}

		return true
	default:
		return equalAny(got, want)
	}
}

// jArrayHasAny implements PostgreSQL's `?|` operator semantics: returns
// true if any element of the wanted []string matches an element of got
// (where got must be a JSON array of strings or a single string).
func jArrayHasAny(got, want any) bool {
	wantList, ok := want.([]string)
	if !ok {
		// Some adapters might pass a single string.
		if s, ok := want.(string); ok {
			wantList = []string{s}
		} else {
			return false
		}
	}

	switch g := got.(type) {
	case []any:
		for _, gv := range g {
			gs := toString(gv)
			for _, w := range wantList {
				if gs == w {
					return true
				}
			}
		}

		return false
	case []string:
		for _, gs := range g {
			for _, w := range wantList {
				if gs == w {
					return true
				}
			}
		}

		return false
	case string:
		for _, w := range wantList {
			if g == w {
				return true
			}
		}

		return false
	default:
		return false
	}
}
