package bw

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"github.com/dgraph-io/badger/v4"
)

// fieldEnc carries a field name with its encoded byte form, plus a flag
// indicating whether the field is unique. Built per-record by encoding
// each indexable/unique field via the precomputed fieldEncoder list, so
// the hot path never invokes reflect.Type / Kind switches.
type fieldEnc struct {
	Name    string
	Encoded []byte
	Indexed bool
	Unique  bool
}

// indexEntries returns the encoded values for every Indexed/Unique field
// of a record. The record argument may be a struct value or a pointer;
// either way it is dereferenced to a struct reflect.Value once and then
// every encoder reads the field directly with no further allocation.
func indexEntries(encoders []*fieldEncoder, record any) ([]fieldEnc, error) {
	if len(encoders) == 0 {
		return nil, nil
	}

	rv := reflect.ValueOf(record)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("bw: nil record")
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("bw: record must be a struct, got %s", rv.Kind())
	}

	out := make([]fieldEnc, 0, len(encoders))
	for _, fe := range encoders {
		out = append(out, fieldEnc{
			Name:    fe.Name,
			Encoded: fe.EncodeFromRecord(rv),
			Indexed: fe.Indexed,
			Unique:  fe.Unique,
		})
	}

	return out, nil
}

// writeIndexes maintains all index/unique keys for a record. If oldRecord
// is non-nil, stale keys derived from it are deleted first.
//
// All operations are issued on the same *badger.Txn so they share atomicity
// with the data write.
func writeIndexes(btx *badger.Txn, bucket string, encoders []*fieldEncoder, pk []byte, oldRecord, newRecord any) error {
	var oldEntries []fieldEnc
	if oldRecord != nil {
		var err error
		oldEntries, err = indexEntries(encoders, oldRecord)
		if err != nil {
			return err
		}
	}

	newEntries, err := indexEntries(encoders, newRecord)
	if err != nil {
		return err
	}

	// Indexed maintenance.
	for i, ne := range newEntries {
		if !ne.Indexed {
			continue
		}
		var prev []byte
		if oldEntries != nil {
			prev = oldEntries[i].Encoded
		}
		if prev != nil && bytes.Equal(prev, ne.Encoded) {
			continue
		}
		if prev != nil {
			if err := btx.Delete(indexKey(bucket, ne.Name, prev, pk)); err != nil {
				return err
			}
		}
		if err := btx.Set(indexKey(bucket, ne.Name, ne.Encoded, pk), nil); err != nil {
			return err
		}
	}

	// Unique maintenance.
	for i, ne := range newEntries {
		if !ne.Unique {
			continue
		}
		var prev []byte
		if oldEntries != nil {
			prev = oldEntries[i].Encoded
		}
		if prev != nil && bytes.Equal(prev, ne.Encoded) {
			continue
		}

		ukey := uniqueKey(bucket, ne.Name, ne.Encoded)
		if item, err := btx.Get(ukey); err == nil {
			ownerPK, errCopy := item.ValueCopy(nil)
			if errCopy != nil {
				return errCopy
			}
			if !bytes.Equal(ownerPK, pk) {
				return fmt.Errorf("%w: field %q", ErrConflict, ne.Name)
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if prev != nil {
			if err := btx.Delete(uniqueKey(bucket, ne.Name, prev)); err != nil {
				return err
			}
		}
		if err := btx.Set(ukey, append([]byte(nil), pk...)); err != nil {
			return err
		}
	}

	return nil
}

// deleteIndexes removes all index/unique keys for the given record.
func deleteIndexes(btx *badger.Txn, bucket string, encoders []*fieldEncoder, pk []byte, record any) error {
	entries, err := indexEntries(encoders, record)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Indexed {
			if err := btx.Delete(indexKey(bucket, e.Name, e.Encoded, pk)); err != nil {
				return err
			}
		}
		if e.Unique {
			if err := btx.Delete(uniqueKey(bucket, e.Name, e.Encoded)); err != nil {
				return err
			}
		}
	}

	return nil
}
