package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// toFloat coerces v to a float64 if v is any of Go's numeric kinds, a
// numeric string, a json.Number or a bool.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case bool:
		if n {
			return 1, true
		}

		return 0, true
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, false
		}

		return f, true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0, false
		}

		return f, true
	default:
		return 0, false
	}
}

// toString coerces v to its canonical string form for stringly comparisons.
func toString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	case json.Number:
		return s.String()
	case bool:
		if s {
			return "true"
		}

		return "false"
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", s)
	}
}
