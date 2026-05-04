package engine

import (
	"strings"

	"github.com/rakunlabs/query"
)

// CollectFields walks where and returns the deduplicated list of
// top-level field names it references. Dotted paths (e.g.
// "address.city") are reduced to their first segment, since a partial
// codec can only deliver whole top-level values.
//
// The returned slice is suitable for passing to
// codec.LazyCodec.UnmarshalFields. Callers that need the full record
// (no Where, or all-fields projection) should pass nil to fall back to
// UnmarshalMap.
func CollectFields(where []query.Expression) []string {
	if len(where) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, 4)
	for _, e := range where {
		collectInto(seen, e)
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}

	return out
}

func collectInto(seen map[string]struct{}, e query.Expression) {
	switch ex := e.(type) {
	case *query.ExpressionCmp:
		if ex == nil || ex.Field == "" {
			return
		}
		seen[topField(ex.Field)] = struct{}{}
	case *query.ExpressionLogic:
		if ex == nil {
			return
		}
		for _, sub := range ex.List {
			collectInto(seen, sub)
		}
	}
}

// topField returns the first dot-segment of a field path.
//
//	"name"             -> "name"
//	"address.city"     -> "address"
//	"items.0.name"     -> "items"
func topField(path string) string {
	if i := strings.IndexByte(path, '.'); i >= 0 {
		return path[:i]
	}

	return path
}
