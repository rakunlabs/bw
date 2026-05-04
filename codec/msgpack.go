package codec

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgPack returns the default msgpack codec, backed by
// vmihailenco/msgpack/v5. It is reflect-based, so record types do not
// need any code generation: bw.RegisterBucket[T] just works on any
// struct.
//
// The codec honours the `bw:"…"` struct tag bw already uses for
// the schema layer (pk/index/unique flags + field name). The first
// comma-separated segment of the tag is the on-wire key; "-" skips the
// field. This means no second `msgpack:"…"` tag is needed.
//
// Lazy decode is implemented by walking the encoded msgpack map
// token-by-token, decoding only the values whose top-level key the
// residual filter referenced and Skip()-ping every other length-prefixed
// value.
func MsgPack() Codec {
	return msgpackCodec{}
}

type msgpackCodec struct{}

func (msgpackCodec) Name() string { return "msgpack" }

// newEncoder returns an encoder configured to honour bw's `bw` tag.
func newEncoder(buf *bytes.Buffer) *msgpack.Encoder {
	enc := msgpack.NewEncoder(buf)
	enc.SetCustomStructTag("bw")
	enc.UseCompactInts(true)

	return enc
}

// newDecoder returns a decoder configured to honour bw's `bw` tag
// and to decode untyped maps into map[string]any (matching what the
// engine evaluator expects).
func newDecoder(b []byte) *msgpack.Decoder {
	dec := msgpack.NewDecoder(bytes.NewReader(b))
	dec.SetCustomStructTag("bw")
	dec.SetMapDecoder(func(d *msgpack.Decoder) (any, error) {
		return d.DecodeUntypedMap()
	})

	return dec
}

func (msgpackCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := newEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("msgpack marshal: %w", err)
	}

	return buf.Bytes(), nil
}

func (msgpackCodec) Unmarshal(data []byte, v any) error {
	if err := newDecoder(data).Decode(v); err != nil {
		return fmt.Errorf("msgpack unmarshal: %w", err)
	}

	return nil
}

// UnmarshalMap fully decodes the record's top-level map into a generic
// map[string]any.
func (msgpackCodec) UnmarshalMap(data []byte) (map[string]any, error) {
	v, err := newDecoder(data).DecodeInterface()
	if err != nil {
		return nil, fmt.Errorf("msgpack unmarshal map: %w", err)
	}
	out, ok := normalizeAny(v).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("msgpack: top-level value is %T, want map", v)
	}

	return out, nil
}

// normalizeAny converts every map[any]any subtree into map[string]any so
// the engine's path lookup behaves uniformly.
func normalizeAny(v any) any {
	switch m := v.(type) {
	case map[string]any:
		for k, vv := range m {
			m[k] = normalizeAny(vv)
		}

		return m
	case map[any]any:
		out := make(map[string]any, len(m))
		for k, vv := range m {
			out[fmt.Sprint(k)] = normalizeAny(vv)
		}

		return out
	case []any:
		for i, vv := range m {
			m[i] = normalizeAny(vv)
		}

		return m
	default:
		return v
	}
}

// UnmarshalFields satisfies LazyCodec. Walks the top-level map keys
// once; decodes only the values whose key is in fields, Skip()s every
// other value. Returns early once every requested field has been seen.
func (c msgpackCodec) UnmarshalFields(data []byte, fields []string) (map[string]any, error) {
	if len(fields) == 0 {
		return map[string]any{}, nil
	}

	dec := newDecoder(data)
	mapLen, err := dec.DecodeMapLen()
	if err != nil {
		// Not a map at the root; fall back to whole-record decode.
		return c.UnmarshalMap(data)
	}

	want := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		want[f] = struct{}{}
	}

	out := make(map[string]any, len(fields))
	remaining := len(want)

	for i := 0; i < mapLen; i++ {
		key, kerr := dec.DecodeString()
		if kerr != nil {
			return nil, fmt.Errorf("msgpack lazy: key: %w", kerr)
		}
		if _, ok := want[key]; !ok {
			if serr := dec.Skip(); serr != nil {
				return nil, fmt.Errorf("msgpack lazy: skip: %w", serr)
			}

			continue
		}
		v, verr := dec.DecodeInterface()
		if verr != nil {
			return nil, fmt.Errorf("msgpack lazy: value for %q: %w", key, verr)
		}
		out[key] = normalizeAny(v)
		remaining--
		if remaining == 0 {
			return out, nil
		}
	}

	return out, nil
}

// Sentinel kept for callers that previously imported it.
var ErrMissingMsgpMethods = errors.New("bw codec: legacy sentinel — no longer used (msgp codegen requirement removed)")
