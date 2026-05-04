package engine

import (
	"encoding/binary"

	"github.com/rakunlabs/query"
)

// PlanKind tells the executor how to fetch candidate primary keys.
type PlanKind int

const (
	// PlanFullScan iterates the bucket data prefix and decodes every
	// value to evaluate the residual filter.
	PlanFullScan PlanKind = iota
	// PlanIndexEq is a point seek on an index entry. Encoded values are
	// in IndexValues; multiple entries are unioned (used for IN lists).
	PlanIndexEq
	// PlanIndexRange iterates a contiguous slice of an index. Lo/Hi are
	// the encoded bounds; LoInclusive/HiInclusive control whether the
	// endpoints match.
	PlanIndexRange
)

// Plan is the execution recipe produced for a query.
type Plan struct {
	Kind PlanKind

	// IndexField is the field name when Kind is PlanIndexEq or
	// PlanIndexRange.
	IndexField string

	// IndexValues holds the byte-encoded equality targets for
	// PlanIndexEq (one per IN element).
	IndexValues [][]byte

	// Lo / Hi are the encoded endpoints for PlanIndexRange.
	Lo, Hi                   []byte
	LoInclusive, HiInclusive bool
	LoOpen, HiOpen           bool // true means unbounded on that side

	// ResidualWhere holds the expressions that the chosen plan does NOT
	// already satisfy; the executor evaluates them against decoded
	// records as a post-filter.
	ResidualWhere []query.Expression

	// SortHandled is true when the plan emits results in q.Sort order
	// already, so the executor can skip the in-memory sort.
	SortHandled bool
}

// IndexedField is the planner's view of an indexable schema field. The
// caller (typically Bucket.Find) supplies one entry per `index`-tagged
// field along with a function that encodes a Go value into index bytes.
type IndexedField struct {
	Name   string
	Encode func(value any) ([]byte, bool)
}

// CompositeIndexedField describes a composite index spanning multiple
// query fields. When all constituent fields have equality conditions in
// the query, the planner encodes each value and concatenates them into
// a single composite key for an index seek.
type CompositeIndexedField struct {
	// Name is the composite group name used as the index key field.
	Name string
	// Fields lists the constituent field names in declaration order.
	Fields []string
	// Encoders parallels Fields: each function encodes a single query
	// value into the sortable byte form for that field position.
	Encoders []func(value any) ([]byte, bool)
}

// PlanQuery picks an execution recipe for q against the supplied set of
// indexed fields. Returns a PlanFullScan plan when no index applies.
//
// Top-level expressions in q.Where are interpreted as an implicit AND
// chain. The first usable index hit becomes the plan; the remaining
// expressions become the residual filter.
func PlanQuery(q *query.Query, idx []IndexedField, composites ...CompositeIndexedField) Plan {
	plan := Plan{Kind: PlanFullScan}
	if q == nil || len(q.Where) == 0 {
		// No filter: a single full-scan iteration is correct.
		plan.SortHandled = sortIsNoop(q)

		return plan
	}

	byName := make(map[string]IndexedField, len(idx))
	for _, f := range idx {
		byName[f.Name] = f
	}

	// --- Composite index check (most selective, try first) ---
	// For each composite, see if ALL constituent fields have eq
	// conditions in the query. If so, encode and concatenate them.
	if len(composites) > 0 {
		// Build a map of field → eq value from the Where clause.
		eqValues := make(map[string]any)
		eqIdxs := make(map[string]int) // field → index in q.Where
		for i, expr := range q.Where {
			cmp, ok := expr.(*query.ExpressionCmp)
			if !ok {
				continue
			}
			switch cmp.Operator {
			case query.OperatorEq, query.OperatorEmpty:
				eqValues[cmp.Field] = cmp.Value
				eqIdxs[cmp.Field] = i
			}
		}

		for _, cf := range composites {
			// Check if all fields in the composite have eq values.
			allFound := true
			for _, fname := range cf.Fields {
				if _, ok := eqValues[fname]; !ok {
					allFound = false
					break
				}
			}
			if !allFound {
				continue
			}

			// Encode each field's value and concatenate with LP.
			var compositeKey []byte
			ok := true
			for i, fname := range cf.Fields {
				b, encoded := cf.Encoders[i](eqValues[fname])
				if !encoded {
					ok = false
					break
				}
				compositeKey = appendLPForPlan(compositeKey, b)
			}
			if !ok {
				continue
			}

			// Build the plan: all matched fields go out of residual.
			residual := make([]query.Expression, 0, len(q.Where))
			for i, expr := range q.Where {
				skip := false
				for _, fname := range cf.Fields {
					if i == eqIdxs[fname] {
						skip = true
						break
					}
				}
				if !skip {
					residual = append(residual, expr)
				}
			}

			return Plan{
				Kind:          PlanIndexEq,
				IndexField:    cf.Name,
				IndexValues:   [][]byte{compositeKey},
				ResidualWhere: residual,
				SortHandled:   len(residual) == 0 && sortIsNoop(q),
			}
		}
	}

	// Look for a usable index hit, prioritising the most selective:
	//   1. eq/in/empty (implicit eq)
	//   2. range (gt/gte/lt/lte chain)
	bestIdx := -1
	bestKind := PlanFullScan
	bestField := ""
	var bestEqValues [][]byte
	var bestRange struct {
		lo, hi                   []byte
		loInclusive, hiInclusive bool
		loOpen, hiOpen           bool
	}

	for i, expr := range q.Where {
		cmp, ok := expr.(*query.ExpressionCmp)
		if !ok {
			continue
		}
		f, ok := byName[cmp.Field]
		if !ok {
			continue
		}

		switch cmp.Operator {
		case query.OperatorEq, query.OperatorEmpty, query.OperatorIn:
			vals := encodeAnyOrList(cmp.Value, f.Encode)
			if vals == nil {
				continue
			}
			// Prefer equality over range; first encountered wins.
			if bestKind == PlanFullScan || bestKind == PlanIndexRange {
				bestIdx = i
				bestKind = PlanIndexEq
				bestField = cmp.Field
				bestEqValues = vals
			}
		case query.OperatorGt, query.OperatorGte, query.OperatorLt, query.OperatorLte:
			b, ok := f.Encode(cmp.Value)
			if !ok {
				continue
			}
			switch {
			case bestKind == PlanIndexEq:
				// Equality always wins; ignore range bounds.
			case bestKind == PlanIndexRange && bestField == cmp.Field:
				// Tighten the existing range with the new bound.
				applyRangeBound(&bestRange, cmp.Operator, b)
			case bestKind == PlanFullScan:
				// First range bound seen — install it.
				bestIdx = i
				bestKind = PlanIndexRange
				bestField = cmp.Field
				bestRange = struct {
					lo, hi                   []byte
					loInclusive, hiInclusive bool
					loOpen, hiOpen           bool
				}{loOpen: true, hiOpen: true}
				applyRangeBound(&bestRange, cmp.Operator, b)
			}
		}
	}

	if bestIdx < 0 {
		// No index applied; full scan with original Where.
		plan.ResidualWhere = q.Where

		return plan
	}

	plan.Kind = bestKind
	plan.IndexField = bestField
	if bestKind == PlanIndexEq {
		plan.IndexValues = bestEqValues
	} else {
		plan.Lo = bestRange.lo
		plan.Hi = bestRange.hi
		plan.LoInclusive = bestRange.loInclusive
		plan.HiInclusive = bestRange.hiInclusive
		plan.LoOpen = bestRange.loOpen
		plan.HiOpen = bestRange.hiOpen
	}

	// Residual: every Where except the chosen one. For range plans, also
	// drop additional bounds on the same field if they ended up baked
	// into bestRange.
	plan.ResidualWhere = make([]query.Expression, 0, len(q.Where)-1)
	for i, expr := range q.Where {
		if i == bestIdx {
			continue
		}
		if bestKind == PlanIndexRange {
			if cmp, ok := expr.(*query.ExpressionCmp); ok && cmp.Field == bestField {
				switch cmp.Operator {
				case query.OperatorGt, query.OperatorGte, query.OperatorLt, query.OperatorLte:
					continue
				}
			}
		}
		plan.ResidualWhere = append(plan.ResidualWhere, expr)
	}

	if len(plan.ResidualWhere) == 0 && len(q.Sort) == 1 && q.Sort[0].Field == bestField && !q.Sort[0].Desc {
		// Index keys are byte-sorted; ascending sort by the indexed field
		// is already in order.
		plan.SortHandled = true
	} else {
		plan.SortHandled = sortIsNoop(q)
	}

	return plan
}

func sortIsNoop(q *query.Query) bool {
	return q == nil || len(q.Sort) == 0
}

// applyRangeBound updates a range struct in-place with one of the four
// bound operators. The range is the in-progress accumulator inside
// PlanQuery; loOpen/hiOpen mean "no bound on that side".
func applyRangeBound(r *struct {
	lo, hi                   []byte
	loInclusive, hiInclusive bool
	loOpen, hiOpen           bool
}, op query.OperatorCmpType, b []byte) {
	switch op {
	case query.OperatorGt:
		r.lo, r.loInclusive, r.loOpen = b, false, false
	case query.OperatorGte:
		r.lo, r.loInclusive, r.loOpen = b, true, false
	case query.OperatorLt:
		r.hi, r.hiInclusive, r.hiOpen = b, false, false
	case query.OperatorLte:
		r.hi, r.hiInclusive, r.hiOpen = b, true, false
	}
}

// encodeAnyOrList accepts either a single value or a slice (typically
// []string from a comma-split IN list) and returns the encoded bytes for
// each member. Returns nil when at least one value cannot be encoded.
func encodeAnyOrList(v any, enc func(any) ([]byte, bool)) [][]byte {
	switch list := v.(type) {
	case []string:
		out := make([][]byte, 0, len(list))
		for _, s := range list {
			b, ok := enc(s)
			if !ok {
				return nil
			}
			out = append(out, b)
		}

		return out
	case []any:
		out := make([][]byte, 0, len(list))
		for _, x := range list {
			b, ok := enc(x)
			if !ok {
				return nil
			}
			out = append(out, b)
		}

		return out
	default:
		b, ok := enc(v)
		if !ok {
			return nil
		}

		return [][]byte{b}
	}
}

// appendLPForPlan appends a uvarint-length-prefixed byte slice to dst.
// This mirrors the appendLP used by the key layer so that composite
// query encodings match the on-disk format.
func appendLPForPlan(dst, b []byte) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(b)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, b...)
	return dst
}
