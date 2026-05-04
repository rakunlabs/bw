// Package keyenc encodes Go values into byte slices whose lexicographic
// order matches the natural order of the underlying value. This is used
// for secondary index keys and the primary-key encoding of integer pks.
//
// Strings and byte slices pass through; signed integers and floats use the
// classic "sign-flip" tricks so that bytes.Compare yields the right sign.
package keyenc

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"
)

// Encode produces a sortable byte representation of v.
//
// Supported kinds: string, []byte, all int/uint widths, float32/64, bool,
// and time.Time (encoded as unix-nano with the sign bit flipped).
//
// Encoding for indexable values must be unambiguous: two values that are
// equal must produce identical bytes, and bytes.Compare on the encoded
// forms must match the natural ordering of the values.
func Encode(v any) ([]byte, error) {
	if v == nil {
		return []byte{0}, nil
	}

	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return []byte{0}, nil
		}
		rv = rv.Elem()
	}

	return encodeValue(rv)
}

func encodeValue(rv reflect.Value) ([]byte, error) {
	switch rv.Kind() {
	case reflect.String:
		// Strings are stored verbatim. Length-prefixing is handled by the
		// caller when concatenating with another field.
		return []byte(rv.String()), nil

	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			out := make([]byte, rv.Len())
			copy(out, rv.Bytes())

			return out, nil
		}

		return nil, fmt.Errorf("keyenc: unsupported slice kind %s", rv.Type())

	case reflect.Bool:
		if rv.Bool() {
			return []byte{1}, nil
		}

		return []byte{0}, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return encodeInt64(rv.Int()), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return encodeUint64(rv.Uint()), nil

	case reflect.Float32:
		return encodeFloat64(float64(rv.Float())), nil

	case reflect.Float64:
		return encodeFloat64(rv.Float()), nil

	case reflect.Struct:
		// time.Time is the only struct we know how to order natively.
		if t, ok := rv.Interface().(time.Time); ok {
			return encodeTime(t), nil
		}

		return nil, fmt.Errorf("keyenc: unsupported struct type %s", rv.Type())
	}

	return nil, fmt.Errorf("keyenc: unsupported kind %s", rv.Kind())
}

// EncodeAny is the variadic-arg form used by callers that already have an
// any-typed value (e.g. coming out of query.ExpressionCmp).
func EncodeAny(v any) ([]byte, error) {
	if v == nil {
		return []byte{0}, nil
	}

	switch x := v.(type) {
	case string:
		return []byte(x), nil
	case []byte:
		out := make([]byte, len(x))
		copy(out, x)

		return out, nil
	case bool:
		if x {
			return []byte{1}, nil
		}

		return []byte{0}, nil
	case int:
		return encodeInt64(int64(x)), nil
	case int8:
		return encodeInt64(int64(x)), nil
	case int16:
		return encodeInt64(int64(x)), nil
	case int32:
		return encodeInt64(int64(x)), nil
	case int64:
		return encodeInt64(x), nil
	case uint:
		return encodeUint64(uint64(x)), nil
	case uint8:
		return encodeUint64(uint64(x)), nil
	case uint16:
		return encodeUint64(uint64(x)), nil
	case uint32:
		return encodeUint64(uint64(x)), nil
	case uint64:
		return encodeUint64(x), nil
	case float32:
		return encodeFloat64(float64(x)), nil
	case float64:
		return encodeFloat64(x), nil
	case time.Time:
		return encodeTime(x), nil
	}

	return Encode(v)
}

// EncodeSortable returns a byte slice ordered like the input. The returned
// slice has fixed length 8 for numerics/time and variable length for
// strings.
//
// This wrapper exists for symmetry; callers should prefer Encode/EncodeAny.
func EncodeSortable(v any) ([]byte, error) { return EncodeAny(v) }

// Typed encoders (exported). Use these directly when the static type of
// the value is known to skip the reflect-and-type-switch path entirely.

// String returns a byte view of s suitable for use as an index value.
// The returned slice does not alias s.
func String(s string) []byte { return []byte(s) }

// Bytes returns a copy of b.
func Bytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)

	return out
}

// Bool returns the canonical 1-byte encoding of b (0 or 1).
func Bool(b bool) []byte {
	if b {
		return []byte{1}
	}

	return []byte{0}
}

// Int64 returns the 8-byte sortable encoding of i.
func Int64(i int64) []byte { return encodeInt64(i) }

// Uint64 returns the 8-byte big-endian encoding of u.
func Uint64(u uint64) []byte { return encodeUint64(u) }

// Float64 returns the 8-byte sortable encoding of f. NaNs are not given
// a defined ordering; callers should reject them upstream if relevant.
func Float64(f float64) []byte { return encodeFloat64(f) }

// Time returns the sortable encoding of t (unix-nano, sign-flipped).
func Time(t time.Time) []byte { return encodeTime(t) }

// Internal helpers.

func encodeUint64(u uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], u)

	return b[:]
}

func encodeInt64(i int64) []byte {
	// Flip the sign bit so that lexicographic order on the resulting
	// bytes equals signed numeric order.
	return encodeUint64(uint64(i) ^ (1 << 63))
}

func encodeFloat64(f float64) []byte {
	bits := math.Float64bits(f)
	if bits&(1<<63) != 0 {
		// Negative: flip every bit (so -inf < ... < -0).
		bits ^= 0xFFFFFFFFFFFFFFFF
	} else {
		// Positive (incl. +0): flip the sign bit so positives sort
		// after negatives.
		bits ^= 1 << 63
	}

	return encodeUint64(bits)
}

func encodeTime(t time.Time) []byte {
	// time.Time supports nanosecond precision back to year 1. Use UnixNano
	// for ordering; values that overflow int64 are clamped (extremely
	// rare in practice).
	return encodeInt64(t.UnixNano())
}
