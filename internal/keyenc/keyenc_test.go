package keyenc_test

import (
	"bytes"
	"sort"
	"testing"
	"time"

	"github.com/rakunlabs/bw/internal/keyenc"
)

func TestEncode_OrderPreservation(t *testing.T) {
	cases := []struct {
		name string
		in   []any
	}{
		{"int64", []any{int64(-1000), int64(-1), int64(0), int64(1), int64(1000)}},
		{"uint64", []any{uint64(0), uint64(1), uint64(255), uint64(1 << 16), uint64(1 << 40)}},
		{"float64", []any{-1e9, -1.5, -0.0, 0.0, 0.5, 1.5, 1e9}},
		{"string", []any{"", "a", "ab", "abc", "b"}},
		{"bool", []any{false, true}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			encoded := make([][]byte, len(c.in))
			for i, v := range c.in {
				b, err := keyenc.EncodeAny(v)
				if err != nil {
					t.Fatalf("encode %v: %v", v, err)
				}
				encoded[i] = b
			}

			// Encoded slice must already be sorted.
			sorted := make([][]byte, len(encoded))
			copy(sorted, encoded)
			sort.Slice(sorted, func(i, j int) bool {
				return bytes.Compare(sorted[i], sorted[j]) < 0
			})

			for i := range encoded {
				if !bytes.Equal(encoded[i], sorted[i]) {
					t.Fatalf("%s order mismatch at %d: encoded=%v", c.name, i, encoded)
				}
			}
		})
	}
}

func TestEncode_Time(t *testing.T) {
	a := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	b := time.Date(2020, 1, 1, 0, 0, 0, 1, time.UTC)
	c := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	ea, _ := keyenc.EncodeAny(a)
	eb, _ := keyenc.EncodeAny(b)
	ec, _ := keyenc.EncodeAny(c)

	if !(bytes.Compare(ea, eb) < 0 && bytes.Compare(eb, ec) < 0) {
		t.Fatalf("time order broken: %x %x %x", ea, eb, ec)
	}
}

func TestEncode_NilStable(t *testing.T) {
	a, err := keyenc.EncodeAny(nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := keyenc.EncodeAny(nil)
	if !bytes.Equal(a, b) {
		t.Fatalf("nil encoding not stable: %v vs %v", a, b)
	}
}

func TestEncode_UnsupportedType(t *testing.T) {
	if _, err := keyenc.EncodeAny([]int{1, 2}); err == nil {
		t.Fatalf("expected error for []int")
	}
}
