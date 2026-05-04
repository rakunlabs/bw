// Package engine implements the query → BadgerDB execution pipeline:
// dot-path field lookup on decoded records, evaluation of query.Expression
// against those records, and a full-scan executor.
package engine

import "strings"

// Lookup returns the value at a dot-separated path within a decoded
// map[string]any. Numeric path segments index into []any slices.
//
//	Lookup(m, "user.address.city")
//	Lookup(m, "items.0.name")
//
// Returns (nil, false) if any segment is missing.
func Lookup(root any, path string) (any, bool) {
	if path == "" {
		return root, root != nil
	}

	cur := root
	for {
		dot := strings.IndexByte(path, '.')
		var seg string
		if dot < 0 {
			seg = path
		} else {
			seg = path[:dot]
			path = path[dot+1:]
		}

		next, ok := step(cur, seg)
		if !ok {
			return nil, false
		}
		cur = next

		if dot < 0 {
			return cur, true
		}
	}
}

func step(v any, key string) (any, bool) {
	switch m := v.(type) {
	case map[string]any:
		x, ok := m[key]

		return x, ok
	case map[any]any:
		// Codecs that emit untyped maps (rare for top-level but possible
		// for nested values).
		if x, ok := m[key]; ok {
			return x, true
		}

		return nil, false
	case []any:
		i, ok := atoi(key)
		if !ok || i < 0 || i >= len(m) {
			return nil, false
		}

		return m[i], true
	default:
		return nil, false
	}
}

func atoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}

	return n, true
}
