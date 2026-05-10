// Package schema parses `bw:"..."` struct tags into a cached schema
// describing the primary key, secondary indexes and unique constraints of
// a record type. It is decoupled from the value codec on purpose: the
// codec serializes the bytes, the schema controls the keys.
package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// Field describes a single tagged field of a record type.
type Field struct {
	// Name is the field name as it appears in serialized records (the
	// first comma-separated segment of the tag, falling back to the Go
	// field name in lowerCamel form). Used by the query engine for
	// dot-path lookups.
	Name string
	// GoName is the Go struct field name.
	GoName string
	// Index is the reflect index path used to reach this field from the
	// root struct (supports embedded structs).
	Index []int
	// Type is the field's reflect type.
	Type reflect.Type
	// PK is true for the primary-key field.
	PK bool
	// Indexed is true if a secondary index should be maintained on this
	// field (planner support is Phase 2; the tag is recognized now).
	Indexed bool
	// Unique is true if the field is unique across the bucket
	// (enforcement is Phase 2).
	Unique bool
	// IndexGroup is non-empty when this field belongs to a named
	// composite index (e.g. `bw:"country,index:location"`).
	IndexGroup string
	// UniqueGroup is non-empty when this field belongs to a named
	// composite unique constraint (e.g. `bw:"code,unique:country_code"`).
	UniqueGroup string
	// FTS is true if a full-text search index should be maintained on
	// this field (only string fields are supported).
	FTS bool
	// Vector is true if a vector index should be maintained on this
	// field (only []float32 is supported).
	Vector bool
	// VectorDim is the embedding dimensionality. Zero means
	// "auto-detect on first insert"; once set in the bucket manifest
	// it is immutable for the lifetime of the bucket.
	VectorDim int
	// VectorMetric is the distance function used at search time.
	// Empty string means "use the bucket default" (cosine).
	// Recognised values: "cosine", "dot", "l2".
	VectorMetric string
}

// CompositeGroup describes a named group of fields that form a composite
// index or unique constraint. Fields are listed in struct declaration order.
type CompositeGroup struct {
	Name   string
	Fields []*Field
}

// Schema is the parsed metadata for a record type T.
type Schema struct {
	// Type is the underlying struct type (T or *T → struct).
	Type reflect.Type
	// PK is the primary-key field, or nil if the bucket uses an external
	// key extractor.
	PK *Field
	// Fields lists every non-skipped field, in declaration order.
	Fields []*Field
	// ByName indexes Fields by their serialized name.
	ByName map[string]*Field
	// CompositeIndexes lists named composite indexes, each containing
	// the ordered fields that form the key. nil when none are declared.
	CompositeIndexes []*CompositeGroup
	// CompositeUniques lists named composite unique constraints. nil
	// when none are declared.
	CompositeUniques []*CompositeGroup
}

// Fingerprint returns a stable SHA-256 hex digest of the schema's index/
// unique/pk surface. Two schemas with the same fingerprint may be stored
// in the same bucket safely; differing fingerprints indicate a schema
// migration is needed.
//
// Field order does not affect the fingerprint; only the set of names and
// their flags do.
func (s *Schema) Fingerprint() string {
	h := sha256.New()
	type entry struct{ Name, Flags string }
	rows := make([]entry, 0, len(s.Fields))
	for _, f := range s.Fields {
		flags := ""
		if f.PK {
			flags += "P"
		}
		if f.Indexed {
			flags += "I"
			if f.IndexGroup != "" {
				flags += ":" + f.IndexGroup
			}
		}
		if f.Unique {
			flags += "U"
			if f.UniqueGroup != "" {
				flags += ":" + f.UniqueGroup
			}
		}
		if f.FTS {
			flags += "F"
		}
		if f.Vector {
			flags += "V"
			if f.VectorDim > 0 {
				flags += fmt.Sprintf(":d=%d", f.VectorDim)
			}
			if f.VectorMetric != "" {
				flags += ":m=" + f.VectorMetric
			}
		}
		rows = append(rows, entry{Name: f.Name, Flags: flags})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	for _, r := range rows {
		_, _ = io.WriteString(h, r.Name)
		_, _ = h.Write([]byte{0})
		_, _ = io.WriteString(h, r.Flags)
		_, _ = h.Write([]byte{0})
	}

	return hex.EncodeToString(h.Sum(nil))
}

// IndexedFields returns the subset of Fields tagged with `index`.
func (s *Schema) IndexedFields() []*Field {
	out := make([]*Field, 0)
	for _, f := range s.Fields {
		if f.Indexed {
			out = append(out, f)
		}
	}

	return out
}

// UniqueFields returns the subset of Fields tagged with `unique`.
func (s *Schema) UniqueFields() []*Field {
	out := make([]*Field, 0)
	for _, f := range s.Fields {
		if f.Unique {
			out = append(out, f)
		}
	}

	return out
}

// FTSFields returns the subset of Fields tagged with `fts`.
func (s *Schema) FTSFields() []*Field {
	out := make([]*Field, 0)
	for _, f := range s.Fields {
		if f.FTS {
			out = append(out, f)
		}
	}

	return out
}

// VectorFields returns the subset of Fields tagged with `vector`.
func (s *Schema) VectorFields() []*Field {
	out := make([]*Field, 0)
	for _, f := range s.Fields {
		if f.Vector {
			out = append(out, f)
		}
	}
	return out
}

var (
	cacheMu sync.RWMutex
	cache   = make(map[reflect.Type]*Schema)
)

// Of returns the Schema for the given record value (T, *T, or reflect.Type).
// Schemas are cached per type.
func Of(v any) (*Schema, error) {
	t, ok := v.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(v)
	}

	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t == nil || t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("schema: %v is not a struct", t)
	}

	cacheMu.RLock()
	s, ok := cache[t]
	cacheMu.RUnlock()
	if ok {
		return s, nil
	}

	s, err := parse(t)
	if err != nil {
		return nil, err
	}

	cacheMu.Lock()
	cache[t] = s
	cacheMu.Unlock()

	return s, nil
}

func parse(t reflect.Type) (*Schema, error) {
	s := &Schema{
		Type:   t,
		ByName: make(map[string]*Field),
	}

	if err := walkStruct(t, nil, s); err != nil {
		return nil, err
	}

	for _, f := range s.Fields {
		if f.PK {
			if s.PK != nil {
				return nil, fmt.Errorf("schema %s: multiple pk fields (%s and %s)", t.Name(), s.PK.GoName, f.GoName)
			}
			pk := f
			s.PK = pk
		}
	}

	// Build composite groups (declaration order within each group is
	// guaranteed because Fields is in declaration order).
	idxGroups := make(map[string]*CompositeGroup)
	uniqGroups := make(map[string]*CompositeGroup)

	for _, f := range s.Fields {
		if f.IndexGroup != "" {
			g, ok := idxGroups[f.IndexGroup]
			if !ok {
				g = &CompositeGroup{Name: f.IndexGroup}
				idxGroups[f.IndexGroup] = g
			}
			g.Fields = append(g.Fields, f)
		}
		if f.UniqueGroup != "" {
			g, ok := uniqGroups[f.UniqueGroup]
			if !ok {
				g = &CompositeGroup{Name: f.UniqueGroup}
				uniqGroups[f.UniqueGroup] = g
			}
			g.Fields = append(g.Fields, f)
		}
	}

	// Stable order for deterministic fingerprints.
	for _, g := range idxGroups {
		s.CompositeIndexes = append(s.CompositeIndexes, g)
	}
	sort.Slice(s.CompositeIndexes, func(i, j int) bool {
		return s.CompositeIndexes[i].Name < s.CompositeIndexes[j].Name
	})
	for _, g := range uniqGroups {
		s.CompositeUniques = append(s.CompositeUniques, g)
	}
	sort.Slice(s.CompositeUniques, func(i, j int) bool {
		return s.CompositeUniques[i].Name < s.CompositeUniques[j].Name
	})

	return s, nil
}

func walkStruct(t reflect.Type, indexPrefix []int, s *Schema) error {
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		// Skip unexported fields entirely (cannot be set via reflect from
		// outside the package).
		if !sf.IsExported() {
			continue
		}

		// Recurse into anonymous embedded structs to surface their tagged
		// fields as if they were on the outer struct (matches encoding/json
		// behaviour and what users expect for "promoted" fields).
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			if _, ok := sf.Tag.Lookup("bw"); !ok {
				idx := append(append([]int{}, indexPrefix...), sf.Index...)
				if err := walkStruct(sf.Type, idx, s); err != nil {
					return err
				}

				continue
			}
		}

		raw, hasTag := sf.Tag.Lookup("bw")
		if hasTag && raw == "-" {
			continue
		}

		f := &Field{
			GoName: sf.Name,
			Index:  append(append([]int{}, indexPrefix...), sf.Index...),
			Type:   sf.Type,
			Name:   sf.Name, // default
		}

		if hasTag {
			parts := splitTagFlags(raw)
			if name := strings.TrimSpace(parts[0]); name != "" {
				f.Name = name
			}
			for _, flag := range parts[1:] {
				flag = strings.TrimSpace(flag)
				switch {
				case flag == "pk":
					f.PK = true
				case flag == "index":
					f.Indexed = true
				case strings.HasPrefix(flag, "index:"):
					f.Indexed = true
					f.IndexGroup = strings.TrimPrefix(flag, "index:")
				case flag == "unique":
					f.Unique = true
				case strings.HasPrefix(flag, "unique:"):
					f.Unique = true
					f.UniqueGroup = strings.TrimPrefix(flag, "unique:")
				case flag == "fts":
					f.FTS = true
				case flag == "vector":
					f.Vector = true
				case strings.HasPrefix(flag, "vector(") && strings.HasSuffix(flag, ")"):
					f.Vector = true
					params := flag[len("vector(") : len(flag)-1]
					if err := parseVectorParams(params, f); err != nil {
						return fmt.Errorf("schema %s: field %s: %w", t.Name(), sf.Name, err)
					}
				case flag == "":
					// trailing comma, ignore
				default:
					return fmt.Errorf("schema %s: unknown tag flag %q on field %s", t.Name(), flag, sf.Name)
				}
			}
		}

		if existing, ok := s.ByName[f.Name]; ok {
			return fmt.Errorf("schema %s: duplicate field name %q (on %s and %s)", t.Name(), f.Name, existing.GoName, f.GoName)
		}

		s.Fields = append(s.Fields, f)
		s.ByName[f.Name] = f
	}

	return nil
}

// PKValue extracts the primary-key value from a record. v must be assignable
// to s.Type or *s.Type. Returns the field's reflect.Value and true if a PK
// field exists.
func (s *Schema) PKValue(v any) (reflect.Value, bool) {
	if s.PK == nil {
		return reflect.Value{}, false
	}

	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}, false
		}
		rv = rv.Elem()
	}

	if rv.Type() != s.Type {
		return reflect.Value{}, false
	}

	return rv.FieldByIndex(s.PK.Index), true
}

// FieldValue returns the reflect.Value of the named field on record. The
// record may be T or *T. (nil, false) is returned if the field is unknown
// or the record is nil.
func (s *Schema) FieldValue(record any, name string) (reflect.Value, bool) {
	f, ok := s.ByName[name]
	if !ok {
		return reflect.Value{}, false
	}

	rv := reflect.ValueOf(record)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return reflect.Value{}, false
		}
		rv = rv.Elem()
	}

	if rv.Type() != s.Type {
		return reflect.Value{}, false
	}

	return rv.FieldByIndex(f.Index), true
}

// splitTagFlags splits a `bw:"…"` tag body on commas, but skips
// commas that appear inside balanced parentheses. This lets a single
// flag carry its own comma-separated parameter list, e.g.
// `bw:"embed,vector(dim=4,metric=cosine)"`.
func splitTagFlags(raw string) []string {
	var (
		parts []string
		buf   strings.Builder
		depth int
	)
	for _, r := range raw {
		switch r {
		case '(':
			depth++
			buf.WriteRune(r)
		case ')':
			if depth > 0 {
				depth--
			}
			buf.WriteRune(r)
		case ',':
			if depth == 0 {
				parts = append(parts, buf.String())
				buf.Reset()
				continue
			}
			buf.WriteRune(r)
		default:
			buf.WriteRune(r)
		}
	}
	parts = append(parts, buf.String())
	return parts
}

// parseVectorParams parses the "key=value,key=value" body of a
// vector(...) tag flag and writes it to f. Recognised keys: dim,
// metric. Unknown keys produce a tag-parse error so typos are caught
// at registration rather than silently ignored.
func parseVectorParams(params string, f *Field) error {
	if params == "" {
		return nil
	}
	for _, kv := range strings.Split(params, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			return fmt.Errorf("vector param %q must be key=value", kv)
		}
		key := strings.TrimSpace(kv[:eq])
		val := strings.TrimSpace(kv[eq+1:])
		switch key {
		case "dim":
			n, err := parseUint(val)
			if err != nil || n == 0 {
				return fmt.Errorf("vector dim %q: must be positive integer", val)
			}
			f.VectorDim = n
		case "metric":
			switch val {
			case "cosine", "dot", "l2":
				f.VectorMetric = val
			default:
				return fmt.Errorf("vector metric %q: want cosine|dot|l2", val)
			}
		default:
			return fmt.Errorf("vector param %q: unknown key", key)
		}
	}
	return nil
}

func parseUint(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not a number")
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}
