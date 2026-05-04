package bw

import (
	"encoding/binary"
	"fmt"
)

// Key layout:
//
//   data:        <bucket>\x00<pk>
//   index:       \x00idx\x00<bucket>\x00<field>\x00<encoded-value>\x00<pk>
//   unique:      \x00uniq\x00<bucket>\x00<field>\x00<encoded-value>     (value=pk)
//   meta:        \x00meta\x00<bucket>
//
// All separators are a single zero byte. Field/bucket names are validated
// at registration time to forbid embedded \x00, but record PK and indexed
// values are length-prefixed with a uvarint to remain unambiguous if they
// happen to contain \x00.

const sep = 0x00

var (
	idxPrefix  = []byte{sep, 'i', 'd', 'x', sep}
	uniqPrefix = []byte{sep, 'u', 'n', 'i', 'q', sep}
	metaPrefix = []byte{sep, 'm', 'e', 't', 'a', sep}
)

// dataPrefix returns "<bucket>\x00", the prefix used for full-bucket scans.
func dataPrefix(bucket string) []byte {
	out := make([]byte, 0, len(bucket)+1)
	out = append(out, bucket...)
	out = append(out, sep)

	return out
}

// dataKey returns "<bucket>\x00<pk>".
func dataKey(bucket string, pk []byte) []byte {
	out := make([]byte, 0, len(bucket)+1+len(pk))
	out = append(out, bucket...)
	out = append(out, sep)
	out = append(out, pk...)

	return out
}

// metaKey returns the schema-meta key for a bucket.
func metaKey(bucket string) []byte {
	out := make([]byte, 0, len(metaPrefix)+len(bucket))
	out = append(out, metaPrefix...)
	out = append(out, bucket...)

	return out
}

// indexFieldPrefix returns "\x00idx\x00<bucket>\x00<field>\x00", the
// shared prefix for every index entry on (bucket, field).
func indexFieldPrefix(bucket, field string) []byte {
	out := make([]byte, 0, len(idxPrefix)+len(bucket)+1+len(field)+1)
	out = append(out, idxPrefix...)
	out = append(out, bucket...)
	out = append(out, sep)
	out = append(out, field...)
	out = append(out, sep)

	return out
}

// indexValuePrefix returns the prefix shared by every index entry whose
// encoded field value equals `value`. Length-prefixing of the value
// guarantees no ambiguity when value happens to contain \x00.
func indexValuePrefix(bucket, field string, value []byte) []byte {
	pfx := indexFieldPrefix(bucket, field)
	out := make([]byte, 0, len(pfx)+10+len(value))
	out = append(out, pfx...)
	out = appendLP(out, value)

	return out
}

// indexKey returns "\x00idx\x00<bucket>\x00<field>\x00<lp(value)><pk>".
func indexKey(bucket, field string, value, pk []byte) []byte {
	pfx := indexValuePrefix(bucket, field, value)
	out := make([]byte, 0, len(pfx)+len(pk))
	out = append(out, pfx...)
	out = append(out, pk...)

	return out
}

// pkFromIndexKey extracts the pk segment from an index key produced by
// indexKey. fieldPrefix must be the result of indexFieldPrefix(bucket,
// field). Returns nil if the key is malformed.
func pkFromIndexKey(key, fieldPrefix []byte) []byte {
	if len(key) < len(fieldPrefix) {
		return nil
	}
	rest := key[len(fieldPrefix):]
	valueLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil
	}
	rest = rest[n:]
	if uint64(len(rest)) < valueLen {
		return nil
	}

	return rest[valueLen:]
}

// uniqueKey returns "\x00uniq\x00<bucket>\x00<field>\x00<value>".
func uniqueKey(bucket, field string, value []byte) []byte {
	out := make([]byte, 0, len(uniqPrefix)+len(bucket)+1+len(field)+1+len(value))
	out = append(out, uniqPrefix...)
	out = append(out, bucket...)
	out = append(out, sep)
	out = append(out, field...)
	out = append(out, sep)
	out = append(out, value...)

	return out
}

// appendLP appends a length-prefixed (uvarint) byte slice.
func appendLP(dst, b []byte) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(b)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, b...)

	return dst
}

// validateBucketName rejects names that would break key parsing.
func validateBucketName(name string) error {
	if name == "" {
		return fmt.Errorf("bucket name must not be empty")
	}
	for i := 0; i < len(name); i++ {
		if name[i] == sep {
			return fmt.Errorf("bucket name %q contains forbidden NUL byte", name)
		}
	}

	return nil
}
