// Package codec defines the Codec interface used by bw to serialize and
// deserialize records stored in BadgerDB, plus the default implementations.
package codec

// Codec is the interface for marshalling and unmarshalling records to and
// from raw bytes. It is also responsible for decoding into a generic
// map[string]any used by the query engine for filter evaluation.
type Codec interface {
	// Name returns a stable identifier (e.g. "msgpack", "json").
	Name() string
	// Marshal serializes v into bytes.
	Marshal(v any) ([]byte, error)
	// Unmarshal deserializes data into v (typically a *T).
	Unmarshal(data []byte, v any) error
	// UnmarshalMap deserializes data into a map[string]any using the same
	// field-name semantics as Unmarshal. This is used by the query
	// evaluator for dynamic dot-path lookups.
	UnmarshalMap(data []byte) (map[string]any, error)
}

// LazyCodec is an optional fast-path interface a Codec may implement to
// support partial decoding. When the query engine needs only a handful
// of fields (the residual filter's referenced top-level keys), it calls
// UnmarshalFields instead of UnmarshalMap and the codec returns a map
// containing only the requested top-level keys.
//
// Codecs are free to ignore the hint — the default behaviour is to fall
// back to UnmarshalMap. Implementors that decode token-by-token (like
// msgpack v5) can stop early once every requested key has been seen.
type LazyCodec interface {
	Codec
	// UnmarshalFields decodes data and returns a map containing only
	// the supplied top-level keys (and any whose first dot-path segment
	// matches an entry). Keys not present in data are simply absent
	// from the returned map. If fields is empty the codec may return an
	// empty map without doing any decoding.
	UnmarshalFields(data []byte, fields []string) (map[string]any, error)
}
