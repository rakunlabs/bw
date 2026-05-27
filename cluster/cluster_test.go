package cluster

import (
	"bytes"
	"testing"
)

// Tests live in the cluster package (not _test) so we can poke at the
// unexported wire helpers directly. These guard the multi-DB protocol
// against accidental breakage; integration testing is handled by the
// example program.

func TestEncodeDecodeNameMsg_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		tag  byte
		tail []byte
	}{
		{"users", msgVersionRequest, nil},
		{"orders", msgPullRequest, []byte{0, 0, 0, 0, 0, 0, 0, 42}},
		{"a", msgPullRequest, []byte{1}},
		{
			name: stringOfLen(255),
			tag:  msgVersionRequest,
			tail: []byte("hello"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := encodeNameMsg(tt.tag, tt.name, tt.tail)
			if buf[0] != tt.tag {
				t.Fatalf("tag = 0x%02x, want 0x%02x", buf[0], tt.tag)
			}
			gotName, gotRest, ok := decodeNameMsg(buf)
			if !ok {
				t.Fatalf("decodeNameMsg returned !ok for %q", tt.name)
			}
			if gotName != tt.name {
				t.Errorf("name = %q, want %q", gotName, tt.name)
			}
			if !bytes.Equal(gotRest, tt.tail) {
				t.Errorf("tail = %x, want %x", gotRest, tt.tail)
			}
		})
	}
}

func TestDecodeNameMsg_Malformed(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{0x01},             // tag but no length byte
		{0x01, 0x00},       // zero-length name
		{0x01, 0x05, 'a'},  // name length 5 but only 1 byte present
		{0x01, 0xFF, 'a'},  // name length 255 but only 1 byte present
	}
	for i, data := range cases {
		_, _, ok := decodeNameMsg(data)
		if ok {
			t.Errorf("case %d: decodeNameMsg returned ok for malformed input %x", i, data)
		}
	}
}

func TestNameHeader_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writeNameHeader(&buf, "orders"); err != nil {
		t.Fatalf("writeNameHeader: %v", err)
	}
	// Append some "body" bytes to verify the reader stops at the header
	// boundary.
	buf.WriteString("BODY")

	got, err := readNameHeader(&buf)
	if err != nil {
		t.Fatalf("readNameHeader: %v", err)
	}
	if got != "orders" {
		t.Errorf("name = %q, want %q", got, "orders")
	}
	if remaining := buf.String(); remaining != "BODY" {
		t.Errorf("body = %q, want %q", remaining, "BODY")
	}
}

func TestValidateName(t *testing.T) {
	if err := validateName(""); err == nil {
		t.Error("empty name should be rejected")
	}
	if err := validateName(stringOfLen(256)); err == nil {
		t.Error("256-byte name should be rejected")
	}
	if err := validateName("users"); err != nil {
		t.Errorf("valid name rejected: %v", err)
	}
	if err := validateName(stringOfLen(255)); err != nil {
		t.Errorf("255-byte name rejected: %v", err)
	}
}

func stringOfLen(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}
