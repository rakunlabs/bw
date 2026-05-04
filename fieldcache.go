package bw

import (
	"reflect"
	"strconv"
	"time"

	"github.com/rakunlabs/bw/internal/keyenc"
	"github.com/rakunlabs/bw/schema"
)

// fieldEncoder bundles the precomputed callbacks for one indexable/unique
// field. Encoders never go through reflect.Value.Interface() — that would
// allocate a fresh interface for every record on the write path. Instead,
// each encoder closes over the field's index path and dispatches by kind
// once at registration time.
type fieldEncoder struct {
	Name string
	// Indexed and Unique mirror the schema flags so the maintenance loop
	// can decide what keys to write.
	Indexed bool
	Unique  bool
	// EncodeFromRecord pulls the field out of a typed record (passed as
	// reflect.Value of the struct itself, not a pointer) and produces the
	// sortable byte form.
	EncodeFromRecord func(rv reflect.Value) []byte
	// EncodeFromQuery converts a value coming out of a query expression
	// (typically a string from query.Parse) into the same byte form.
	// Returns (nil, false) if the value cannot be coerced into the
	// field's static type.
	EncodeFromQuery func(v any) ([]byte, bool)
}

// timeType is reflect's view of time.Time; cached to avoid repeated
// reflect.TypeOf calls in the hot path.
var timeType = reflect.TypeOf(time.Time{})

// buildFieldEncoders constructs one fieldEncoder per Indexed/Unique field
// of s. Fields whose Go kind isn't supported by keyenc are skipped (the
// planner will simply not be able to use those indexes).
//
// Composite groups (fields sharing the same IndexGroup or UniqueGroup name)
// produce a single fieldEncoder whose encoded key is the concatenation of
// all constituent fields' sortable encodings. The group name is used as
// the logical "field name" in index/unique keys.
func buildFieldEncoders(s *schema.Schema) []*fieldEncoder {
	var out []*fieldEncoder

	// Track which fields are handled by composite groups so we don't
	// double-register them as standalone encoders.
	compositeIndexFields := make(map[string]bool)
	compositeUniqueFields := make(map[string]bool)

	// Build composite index encoders.
	for _, cg := range s.CompositeIndexes {
		fe := makeCompositeEncoder(cg, true, false)
		if fe != nil {
			out = append(out, fe)
		}
		for _, f := range cg.Fields {
			compositeIndexFields[f.GoName] = true
		}
	}

	// Build composite unique encoders.
	for _, cg := range s.CompositeUniques {
		fe := makeCompositeEncoder(cg, false, true)
		if fe != nil {
			out = append(out, fe)
		}
		for _, f := range cg.Fields {
			compositeUniqueFields[f.GoName] = true
		}
	}

	// Build single-field encoders for fields not covered by a composite.
	for _, f := range s.Fields {
		indexed := f.Indexed && f.IndexGroup == ""
		unique := f.Unique && f.UniqueGroup == ""
		if !indexed && !unique {
			continue
		}
		// Skip if fully handled by a composite group.
		if indexed && compositeIndexFields[f.GoName] {
			indexed = false
		}
		if unique && compositeUniqueFields[f.GoName] {
			unique = false
		}
		if !indexed && !unique {
			continue
		}
		fe := makeEncoderFor(f)
		if fe == nil {
			continue
		}
		fe.Indexed = indexed
		fe.Unique = unique
		out = append(out, fe)
	}

	return out
}

// makeCompositeEncoder builds a single fieldEncoder for a CompositeGroup.
// The encoded value is the concatenation of length-prefixed encoded values
// of each field in declaration order.
func makeCompositeEncoder(cg *schema.CompositeGroup, indexed, unique bool) *fieldEncoder {
	// Build sub-encoders for each field.
	type sub struct {
		encode func(rv reflect.Value) []byte
	}
	subs := make([]sub, 0, len(cg.Fields))
	for _, f := range cg.Fields {
		enc := makeEncoderFor(f)
		if enc == nil {
			return nil // unsupported type in composite → skip entire group
		}
		subs = append(subs, sub{encode: enc.EncodeFromRecord})
	}

	return &fieldEncoder{
		Name:    cg.Name,
		Indexed: indexed,
		Unique:  unique,
		EncodeFromRecord: func(rv reflect.Value) []byte {
			var buf []byte
			for _, s := range subs {
				v := s.encode(rv)
				buf = appendLP(buf, v)
			}
			return buf
		},
		EncodeFromQuery: func(v any) ([]byte, bool) {
			// Composite indexes cannot be queried with a single scalar
			// value — they require all constituent fields. Return false
			// so the planner falls back to a full scan.
			return nil, false
		},
	}
}

func makeEncoderFor(f *schema.Field) *fieldEncoder {
	idx := f.Index
	t := f.Type

	switch t.Kind() {
	case reflect.String:
		return &fieldEncoder{
			Name:    f.Name,
			Indexed: f.Indexed,
			Unique:  f.Unique,
			EncodeFromRecord: func(rv reflect.Value) []byte {
				return keyenc.String(rv.FieldByIndex(idx).String())
			},
			EncodeFromQuery: func(v any) ([]byte, bool) {
				switch x := v.(type) {
				case string:
					return keyenc.String(x), true
				case []byte:
					return keyenc.Bytes(x), true
				}

				return nil, false
			},
		}

	case reflect.Bool:
		return &fieldEncoder{
			Name:    f.Name,
			Indexed: f.Indexed,
			Unique:  f.Unique,
			EncodeFromRecord: func(rv reflect.Value) []byte {
				return keyenc.Bool(rv.FieldByIndex(idx).Bool())
			},
			EncodeFromQuery: func(v any) ([]byte, bool) {
				switch x := v.(type) {
				case bool:
					return keyenc.Bool(x), true
				case string:
					switch x {
					case "true", "1":
						return keyenc.Bool(true), true
					case "false", "0":
						return keyenc.Bool(false), true
					}
				}

				return nil, false
			},
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return &fieldEncoder{
			Name:    f.Name,
			Indexed: f.Indexed,
			Unique:  f.Unique,
			EncodeFromRecord: func(rv reflect.Value) []byte {
				return keyenc.Int64(rv.FieldByIndex(idx).Int())
			},
			EncodeFromQuery: func(v any) ([]byte, bool) {
				n, ok := toInt64(v)
				if !ok {
					return nil, false
				}

				return keyenc.Int64(n), true
			},
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &fieldEncoder{
			Name:    f.Name,
			Indexed: f.Indexed,
			Unique:  f.Unique,
			EncodeFromRecord: func(rv reflect.Value) []byte {
				return keyenc.Uint64(rv.FieldByIndex(idx).Uint())
			},
			EncodeFromQuery: func(v any) ([]byte, bool) {
				n, ok := toUint64(v)
				if !ok {
					return nil, false
				}

				return keyenc.Uint64(n), true
			},
		}

	case reflect.Float32, reflect.Float64:
		return &fieldEncoder{
			Name:    f.Name,
			Indexed: f.Indexed,
			Unique:  f.Unique,
			EncodeFromRecord: func(rv reflect.Value) []byte {
				return keyenc.Float64(rv.FieldByIndex(idx).Float())
			},
			EncodeFromQuery: func(v any) ([]byte, bool) {
				f, ok := toFloat64(v)
				if !ok {
					return nil, false
				}

				return keyenc.Float64(f), true
			},
		}

	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return &fieldEncoder{
				Name:    f.Name,
				Indexed: f.Indexed,
				Unique:  f.Unique,
				EncodeFromRecord: func(rv reflect.Value) []byte {
					return keyenc.Bytes(rv.FieldByIndex(idx).Bytes())
				},
				EncodeFromQuery: func(v any) ([]byte, bool) {
					switch x := v.(type) {
					case []byte:
						return keyenc.Bytes(x), true
					case string:
						return keyenc.String(x), true
					}

					return nil, false
				},
			}
		}

	case reflect.Struct:
		if t == timeType {
			return &fieldEncoder{
				Name:    f.Name,
				Indexed: f.Indexed,
				Unique:  f.Unique,
				EncodeFromRecord: func(rv reflect.Value) []byte {
					t := rv.FieldByIndex(idx).Interface().(time.Time)

					return keyenc.Time(t)
				},
				EncodeFromQuery: func(v any) ([]byte, bool) {
					switch x := v.(type) {
					case time.Time:
						return keyenc.Time(x), true
					case string:
						parsed, err := time.Parse(time.RFC3339Nano, x)
						if err != nil {
							return nil, false
						}

						return keyenc.Time(parsed), true
					}

					return nil, false
				},
			}
		}
	}

	return nil
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	case float32:
		return int64(x), true
	case float64:
		return int64(x), true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err == nil {
			return n, true
		}
		// Allow numeric strings with fractional zero — common in JSON.
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return int64(f), true
		}
	}

	return 0, false
}

func toUint64(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint64:
		return x, true
	case int:
		if x < 0 {
			return 0, false
		}

		return uint64(x), true
	case int64:
		if x < 0 {
			return 0, false
		}

		return uint64(x), true
	case string:
		n, err := strconv.ParseUint(x, 10, 64)
		if err == nil {
			return n, true
		}
	}

	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err == nil {
			return f, true
		}
	}

	return 0, false
}
