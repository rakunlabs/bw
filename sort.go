package bw

import (
	"reflect"
	"sort"
	"strings"

	"github.com/rakunlabs/query"
)

// splitSortPaging produces a copy of q with Sort/Offset/Limit cleared,
// plus those three values returned separately so Bucket.Find can apply
// them on the typed slice.
func splitSortPaging(q *query.Query) (scanQ *query.Query, sortSpec []query.ExpressionSort, offset, limit uint64) {
	if q == nil {
		return nil, nil, 0, 0
	}
	clone := *q
	sortSpec = clone.Sort
	offset = clone.GetOffset()
	limit = clone.GetLimit()
	clone.Sort = nil
	clone.Offset = nil
	clone.Limit = nil

	return &clone, sortSpec, offset, limit
}

// sortTyped sorts a slice of *T in place using the given Sort spec. It
// reads field values directly from the typed struct so no codec decode
// or map lookup happens during comparison.
func (b *Bucket[T]) sortTyped(records []*T, spec []query.ExpressionSort) {
	if len(records) <= 1 || len(spec) == 0 {
		return
	}
	cmps := make([]typedSortKey, 0, len(spec))
	for _, s := range spec {
		k := b.lookupSortKey(s.Field)
		k.desc = s.Desc
		cmps = append(cmps, k)
	}
	sort.SliceStable(records, func(i, j int) bool {
		for _, c := range cmps {
			r := c.compare(reflect.ValueOf(records[i]).Elem(), reflect.ValueOf(records[j]).Elem())
			if r == 0 {
				continue
			}
			if c.desc {
				return r > 0
			}

			return r < 0
		}

		return false
	})
}

// applyOffsetLimitTyped applies Offset/Limit to a typed slice.
func applyOffsetLimitTyped[T any](in []*T, offset, limit uint64) []*T {
	if offset == 0 && limit == 0 {
		return in
	}
	if offset >= uint64(len(in)) {
		return nil
	}
	in = in[offset:]
	if limit > 0 && uint64(len(in)) > limit {
		in = in[:limit]
	}

	return in
}

// typedSortKey is a precomputed accessor for one sort field. The field
// path is resolved against the schema once; comparison is then a
// direct reflect.Value.FieldByIndex without map allocation.
type typedSortKey struct {
	// indexPath is the reflect.FieldByIndex path. nil means the field
	// is unknown to the schema and we'll fall back to string-empty
	// comparison (effectively no-op).
	indexPath []int
	// nestedPath holds the remaining dotted segments after the top-level
	// field, used to descend into nested structs/maps post-FieldByIndex.
	nestedPath []string
	// kind is the leaf field's kind, captured once so compare can
	// dispatch without re-reading reflect.Type.
	kind reflect.Kind
	desc bool
}

// lookupSortKey resolves a field path against the bucket schema.
func (b *Bucket[T]) lookupSortKey(path string) typedSortKey {
	first := path
	rest := ""
	if i := strings.IndexByte(path, '.'); i >= 0 {
		first = path[:i]
		rest = path[i+1:]
	}

	f := b.schema.ByName[first]
	if f == nil {
		return typedSortKey{}
	}

	k := typedSortKey{
		indexPath: f.Index,
		kind:      f.Type.Kind(),
	}
	if rest != "" {
		k.nestedPath = strings.Split(rest, ".")
	}

	return k
}

// compare returns -1/0/+1 for a vs b along this key.
func (k typedSortKey) compare(a, b reflect.Value) int {
	if k.indexPath == nil {
		return 0
	}
	av := a.FieldByIndex(k.indexPath)
	bv := b.FieldByIndex(k.indexPath)
	for _, seg := range k.nestedPath {
		av = descend(av, seg)
		bv = descend(bv, seg)
	}

	return compareReflect(av, bv)
}

// descend walks one path segment of a reflect.Value.
func descend(v reflect.Value, seg string) reflect.Value {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Struct:
		// Try a tagged field whose name is `seg`.
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			tag := t.Field(i).Tag.Get("bw")
			name := strings.Split(tag, ",")[0]
			if name == "" {
				name = t.Field(i).Name
			}
			if name == seg {
				return v.Field(i)
			}
		}
	case reflect.Map:
		return v.MapIndex(reflect.ValueOf(seg))
	}

	return reflect.Value{}
}

func compareReflect(a, b reflect.Value) int {
	if !a.IsValid() && !b.IsValid() {
		return 0
	}
	if !a.IsValid() {
		return -1
	}
	if !b.IsValid() {
		return 1
	}
	switch a.Kind() {
	case reflect.String:
		return strings.Compare(a.String(), b.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ai, bi := a.Int(), b.Int()
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		}

		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		ai, bi := a.Uint(), b.Uint()
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		}

		return 0
	case reflect.Float32, reflect.Float64:
		af, bf := a.Float(), b.Float()
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		}

		return 0
	case reflect.Bool:
		ab, bb := a.Bool(), b.Bool()
		switch {
		case !ab && bb:
			return -1
		case ab && !bb:
			return 1
		}

		return 0
	}

	return 0
}
