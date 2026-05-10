package bw

import (
	"encoding/binary"
	"fmt"
)

// Key layout:
//
//   data:         <bucket>\x00<pk>
//   index:        \x00idx\x00<bucket>\x00<field>\x00<encoded-value>\x00<pk>
//   unique:       \x00uniq\x00<bucket>\x00<field>\x00<encoded-value>      (value=pk)
//   meta:         \x00meta\x00<bucket>
//
// Full-text search keys (all under the same Badger transaction as the
// data write, so backup/restore is automatically portable):
//
//   fts-posting:  \x00fts\x00<bucket>\x00p\x00<field>\x00<lp(term)><pk>   value=varint(tf)
//   fts-doclen:   \x00fts\x00<bucket>\x00d\x00<pk>                        value=varint(totalLen)
//   fts-stats:    \x00fts\x00<bucket>\x00s\x00                            value=varint(docCount)+varint(sumLen)
//   fts-terms:    \x00fts\x00<bucket>\x00t\x00<pk>\x00<field>\x00<lp(term)>   value=varint(tf)
//
// The fts-terms key lets a per-pk update find every term it previously
// emitted (so we can decrement postings on overwrite/delete) without
// scanning the whole posting list.
//
// All separators are a single zero byte. Field/bucket names are
// validated at registration time to forbid embedded \x00, but record
// PK and term values are length-prefixed with a uvarint.

const sep = 0x00

var (
	idxPrefix  = []byte{sep, 'i', 'd', 'x', sep}
	uniqPrefix = []byte{sep, 'u', 'n', 'i', 'q', sep}
	metaPrefix = []byte{sep, 'm', 'e', 't', 'a', sep}
	ftsPrefix  = []byte{sep, 'f', 't', 's', sep}
	vecPrefix  = []byte{sep, 'v', 'e', 'c', sep}
	migPrefix  = []byte{sep, 'm', 'i', 'g', sep}
)

// Vector sub-namespace markers (the byte after the field name).
var (
	vecRawMark      = []byte{'v', sep} // raw vector bytes
	vecManifestMark = []byte{'m', sep} // per-field manifest
	vecTombMark     = []byte{'x', sep} // soft-delete tombstone
	vecLevelMark    = []byte{'l', sep} // per-pk graph max level
	vecNeighMark    = []byte{'n', sep} // per-(level, pk) neighbour list
	vecEntryMark    = []byte{'e', sep} // singleton entry-point key
)

// FTS sub-namespace markers (the byte after the bucket name).
var (
	ftsPostingMark = []byte{'p', sep}
	ftsDoclenMark  = []byte{'d', sep}
	ftsStatsMark   = []byte{'s', sep}
	ftsTermsMark   = []byte{'t', sep}
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

// ftsBucketPrefix returns "\x00fts\x00<bucket>\x00", the namespace
// shared by every FTS key for a bucket.
func ftsBucketPrefix(bucket string) []byte {
	out := make([]byte, 0, len(ftsPrefix)+len(bucket)+1)
	out = append(out, ftsPrefix...)
	out = append(out, bucket...)
	out = append(out, sep)
	return out
}

// ftsPostingFieldPrefix returns "\x00fts\x00<bucket>\x00p\x00<field>\x00",
// the prefix shared by every posting key for (bucket, field).
func ftsPostingFieldPrefix(bucket, field string) []byte {
	bp := ftsBucketPrefix(bucket)
	out := make([]byte, 0, len(bp)+len(ftsPostingMark)+len(field)+1)
	out = append(out, bp...)
	out = append(out, ftsPostingMark...)
	out = append(out, field...)
	out = append(out, sep)
	return out
}

// ftsPostingTermPrefix returns the prefix shared by every posting key
// whose term equals `term`. The term is length-prefixed so the prefix
// is unambiguous even when term contains separator bytes.
func ftsPostingTermPrefix(bucket, field string, term []byte) []byte {
	pfx := ftsPostingFieldPrefix(bucket, field)
	out := make([]byte, 0, len(pfx)+10+len(term))
	out = append(out, pfx...)
	out = appendLP(out, term)
	return out
}

// ftsPostingKey returns the full posting key for (bucket, field, term, pk).
func ftsPostingKey(bucket, field string, term, pk []byte) []byte {
	pfx := ftsPostingTermPrefix(bucket, field, term)
	out := make([]byte, 0, len(pfx)+len(pk))
	out = append(out, pfx...)
	out = append(out, pk...)
	return out
}

// pkFromPostingKey extracts the pk segment from a posting key. termPrefix
// must equal ftsPostingTermPrefix(bucket, field, term).
func pkFromPostingKey(key, termPrefix []byte) []byte {
	if len(key) < len(termPrefix) {
		return nil
	}
	return key[len(termPrefix):]
}

// ftsDoclenKey returns the per-doc length key.
func ftsDoclenKey(bucket string, pk []byte) []byte {
	bp := ftsBucketPrefix(bucket)
	out := make([]byte, 0, len(bp)+len(ftsDoclenMark)+len(pk))
	out = append(out, bp...)
	out = append(out, ftsDoclenMark...)
	out = append(out, pk...)
	return out
}

// ftsStatsKey returns the per-bucket corpus stats key.
func ftsStatsKey(bucket string) []byte {
	bp := ftsBucketPrefix(bucket)
	out := make([]byte, 0, len(bp)+len(ftsStatsMark))
	out = append(out, bp...)
	out = append(out, ftsStatsMark...)
	return out
}

// ftsTermsPKPrefix returns "\x00fts\x00<bucket>\x00t\x00<pk>\x00", the
// prefix listing every (field, term) emitted for the given pk. Used
// by the update path to clean up stale postings on overwrite/delete.
func ftsTermsPKPrefix(bucket string, pk []byte) []byte {
	bp := ftsBucketPrefix(bucket)
	out := make([]byte, 0, len(bp)+len(ftsTermsMark)+len(pk)+1)
	out = append(out, bp...)
	out = append(out, ftsTermsMark...)
	out = append(out, pk...)
	out = append(out, sep)
	return out
}

// ftsTermsKey returns the per-(pk, field, term) memo key.
func ftsTermsKey(bucket string, pk []byte, field string, term []byte) []byte {
	pfx := ftsTermsPKPrefix(bucket, pk)
	out := make([]byte, 0, len(pfx)+len(field)+1+10+len(term))
	out = append(out, pfx...)
	out = append(out, field...)
	out = append(out, sep)
	out = appendLP(out, term)
	return out
}

// parseTermsKey extracts (field, term) from a key produced by ftsTermsKey.
// pkPrefix must equal ftsTermsPKPrefix(bucket, pk). Returns false on
// malformed input.
func parseTermsKey(key, pkPrefix []byte) (field string, term []byte, ok bool) {
	if len(key) < len(pkPrefix) {
		return "", nil, false
	}
	rest := key[len(pkPrefix):]
	// rest = <field>\x00<lp(term)>
	idx := -1
	for i := 0; i < len(rest); i++ {
		if rest[i] == sep {
			idx = i
			break
		}
	}
	if idx < 0 {
		return "", nil, false
	}
	field = string(rest[:idx])
	rest = rest[idx+1:]
	termLen, n := binary.Uvarint(rest)
	if n <= 0 || uint64(len(rest)-n) < termLen {
		return "", nil, false
	}
	term = rest[n : n+int(termLen)]
	return field, term, true
}

// vecFieldPrefix returns "\x00vec\x00<bucket>\x00<field>\x00", the
// namespace shared by every vector key for (bucket, field).
func vecFieldPrefix(bucket, field string) []byte {
	out := make([]byte, 0, len(vecPrefix)+len(bucket)+1+len(field)+1)
	out = append(out, vecPrefix...)
	out = append(out, bucket...)
	out = append(out, sep)
	out = append(out, field...)
	out = append(out, sep)
	return out
}

// vecRawPrefix returns the prefix used to range-scan every raw vector
// for (bucket, field).
func vecRawPrefix(bucket, field string) []byte {
	pfx := vecFieldPrefix(bucket, field)
	out := make([]byte, 0, len(pfx)+len(vecRawMark))
	out = append(out, pfx...)
	out = append(out, vecRawMark...)
	return out
}

// vecRawKey returns the per-pk raw vector key.
func vecRawKey(bucket, field string, pk []byte) []byte {
	pfx := vecRawPrefix(bucket, field)
	out := make([]byte, 0, len(pfx)+len(pk))
	out = append(out, pfx...)
	out = append(out, pk...)
	return out
}

// pkFromVecRawKey extracts the pk segment from a raw-vector key.
// rawPrefix must equal vecRawPrefix(bucket, field).
func pkFromVecRawKey(key, rawPrefix []byte) []byte {
	if len(key) < len(rawPrefix) {
		return nil
	}
	return key[len(rawPrefix):]
}

// vecManifestKey returns the per-(bucket, field) manifest key.
func vecManifestKey(bucket, field string) []byte {
	pfx := vecFieldPrefix(bucket, field)
	out := make([]byte, 0, len(pfx)+len(vecManifestMark))
	out = append(out, pfx...)
	out = append(out, vecManifestMark...)
	return out
}

// vecTombKey returns the soft-delete tombstone key for (bucket, field, pk).
func vecTombKey(bucket, field string, pk []byte) []byte {
	pfx := vecFieldPrefix(bucket, field)
	out := make([]byte, 0, len(pfx)+len(vecTombMark)+len(pk))
	out = append(out, pfx...)
	out = append(out, vecTombMark...)
	out = append(out, pk...)
	return out
}

// vecBucketPrefix returns "\x00vec\x00<bucket>\x00", the prefix shared
// by every vector key in the bucket regardless of field.
func vecBucketPrefix(bucket string) []byte {
	out := make([]byte, 0, len(vecPrefix)+len(bucket)+1)
	out = append(out, vecPrefix...)
	out = append(out, bucket...)
	out = append(out, sep)
	return out
}

// vecLevelKey holds the per-pk graph max level (1 byte).
func vecLevelKey(bucket, field string, pk []byte) []byte {
	pfx := vecFieldPrefix(bucket, field)
	out := make([]byte, 0, len(pfx)+len(vecLevelMark)+len(pk))
	out = append(out, pfx...)
	out = append(out, vecLevelMark...)
	out = append(out, pk...)
	return out
}

// vecNeighKey holds the neighbour list of pk at the given graph level.
//
// Layout: \x00vec\x00<bucket>\x00<field>\x00n\x00<level:1><pk>
//
// The level is fixed-width 1 byte (HNSW levels rarely exceed 16) so a
// range scan on the n\x00<level:1> prefix yields every node-at-level
// in pk-sorted order — handy for diagnostics, not used by the hot
// path which only reads single keys.
func vecNeighKey(bucket, field string, level uint8, pk []byte) []byte {
	pfx := vecFieldPrefix(bucket, field)
	out := make([]byte, 0, len(pfx)+len(vecNeighMark)+1+len(pk))
	out = append(out, pfx...)
	out = append(out, vecNeighMark...)
	out = append(out, level)
	out = append(out, pk...)
	return out
}

// vecEntryKey is the singleton holding the graph entry point for a
// (bucket, field). Value layout: <maxLevel:1> <lp(pk)>.
func vecEntryKey(bucket, field string) []byte {
	pfx := vecFieldPrefix(bucket, field)
	out := make([]byte, 0, len(pfx)+len(vecEntryMark))
	out = append(out, pfx...)
	out = append(out, vecEntryMark...)
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

// migProgressKey returns the per-bucket key holding the in-flight
// user migration's resume cursor. The value is the last successfully
// migrated pk, or absent when no migration is running.
//
// Layout: \x00mig\x00<bucket>\x00<fromV>\x00<toV>\x00progress
func migProgressKey(bucket string, fromV, toV uint64) []byte {
	out := make([]byte, 0, len(migPrefix)+len(bucket)+1+2*binary.MaxVarintLen64+len("progress"))
	out = append(out, migPrefix...)
	out = append(out, bucket...)
	out = append(out, sep)
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], fromV)
	out = append(out, buf[:n]...)
	out = append(out, sep)
	n = binary.PutUvarint(buf[:], toV)
	out = append(out, buf[:n]...)
	out = append(out, sep)
	out = append(out, "progress"...)
	return out
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
