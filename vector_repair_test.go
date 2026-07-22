package bw

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

type vectorRepairDoc struct {
	ID    string    `bw:"id,pk"`
	Embed []float32 `bw:"embed,vector(metric=cosine)"`
}

func TestVectorParamsArePersistedAndImmutable(t *testing.T) {
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucket, err := RegisterBucket[vectorRepairDoc](db, "vec_params", WithVectorParams[vectorRepairDoc](4, 17))
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Insert(context.Background(), &vectorRepairDoc{ID: "a", Embed: []float32{1, 0, 0}}); err != nil {
		t.Fatal(err)
	}

	err = db.bdb.View(func(tx *badger.Txn) error {
		man, err := bucket.vecIdx.readManifest(tx)
		if err != nil {
			return err
		}
		if man.M != 4 || man.EfConstruction != 17 {
			t.Fatalf("persisted params = M:%d ef:%d, want M:4 ef:17", man.M, man.EfConstruction)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := RegisterBucket[vectorRepairDoc](db, "vec_params", WithVectorParams[vectorRepairDoc](8, 17)); err == nil {
		t.Fatal("changing persisted HNSW M unexpectedly succeeded")
	}
}

func TestVectorCorruptValuesReturnErrors(t *testing.T) {
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bucket, err := RegisterBucket[vectorRepairDoc](db, "vec_corrupt")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := bucket.Insert(ctx, &vectorRepairDoc{ID: "a", Embed: []float32{1, 0, 0}}); err != nil {
		t.Fatal(err)
	}

	err = db.bdb.Update(func(tx *badger.Txn) error {
		return tx.Set(vecRawKey(bucket.name, bucket.vecIdx.field.Name, []byte("a")), encodeVector([]float32{1}))
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bucket.SearchVector(ctx, []float32{1, 0, 0}); !errors.Is(err, ErrDimMismatch) {
		t.Fatalf("short persisted vector: want ErrDimMismatch, got %v", err)
	}

	err = db.bdb.Update(func(tx *badger.Txn) error {
		return tx.Set(vecRawKey(bucket.name, bucket.vecIdx.field.Name, []byte("a")), encodeVector([]float32{float32(math.NaN()), 0, 0}))
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bucket.SearchVector(ctx, []float32{1, 0, 0}); err == nil {
		t.Fatal("non-finite persisted vector did not return an error")
	}
}

func TestVectorCorruptGraphMetadataReturnsErrors(t *testing.T) {
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bucket, err := RegisterBucket[vectorRepairDoc](db, "vec_corrupt_graph")
	if err != nil {
		t.Fatal(err)
	}
	pk := []byte("a")

	err = db.bdb.Update(func(tx *badger.Txn) error {
		if err := tx.Set(vecNeighKey(bucket.name, bucket.vecIdx.field.Name, 0, pk), []byte{127}); err != nil {
			return err
		}
		return tx.Set(vecLevelKey(bucket.name, bucket.vecIdx.field.Name, pk), []byte{255})
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.bdb.View(func(tx *badger.Txn) error {
		if _, err := bucket.vecIdx.readNeighbours(tx, pk, 0); err == nil {
			t.Fatal("oversized neighbour count did not return an error")
		}
		if _, err := bucket.vecIdx.readLevel(tx, pk); err == nil {
			t.Fatal("invalid graph level did not return an error")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var malformed [binary.MaxVarintLen64 + 2]byte
	n := binary.PutUvarint(malformed[:], 3)
	n += binary.PutUvarint(malformed[n:], uint64(Cosine))
	malformed[n] = 0x80
	if _, err := decodeManifest(malformed[:n+1]); err == nil {
		t.Fatal("truncated manifest count did not return an error")
	}
}

func TestVectorGraphEdgesRemainReciprocalAndUniqueAfterUpdates(t *testing.T) {
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bucket, err := RegisterBucket[vectorRepairDoc](db, "vec_graph_invariants", WithVectorParams[vectorRepairDoc](4, 20))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		v := []float32{float32(i + 1), float32((i % 7) + 1), 1}
		if err := bucket.Insert(ctx, &vectorRepairDoc{ID: fmt.Sprintf("%03d", i), Embed: v}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	for i := 0; i < 10; i++ {
		var entry []byte
		if err := db.bdb.View(func(tx *badger.Txn) error {
			var err error
			entry, _, err = bucket.vecIdx.readEntry(tx)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := bucket.Update(ctx, &vectorRepairDoc{
			ID:    string(entry),
			Embed: []float32{1, float32(i + 1), float32(i + 2)},
		}); err != nil {
			t.Fatalf("update entry %q: %v", entry, err)
		}
	}

	err = db.bdb.View(func(tx *badger.Txn) error {
		prefix := append([]byte{}, vecFieldPrefix(bucket.name, bucket.vecIdx.field.Name)...)
		prefix = append(prefix, vecLevelMark...)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := tx.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			pk := append([]byte(nil), it.Item().Key()[len(prefix):]...)
			level, err := bucket.vecIdx.readLevel(tx, pk)
			if err != nil {
				return err
			}
			for graphLevel := uint8(0); graphLevel <= level; graphLevel++ {
				neighbours, err := bucket.vecIdx.readNeighbours(tx, pk, graphLevel)
				if err != nil {
					return err
				}
				seen := make(map[string]struct{}, len(neighbours))
				for _, neighbour := range neighbours {
					if _, ok := seen[string(neighbour)]; ok {
						t.Fatalf("duplicate edge %q -> %q at level %d", pk, neighbour, graphLevel)
					}
					seen[string(neighbour)] = struct{}{}
					reverse, err := bucket.vecIdx.readNeighbours(tx, neighbour, graphLevel)
					if err != nil {
						return err
					}
					found := false
					for _, candidate := range reverse {
						if vecBytesEqual(candidate, pk) {
							found = true
							break
						}
					}
					if !found {
						t.Fatalf("edge %q -> %q at level %d is not reciprocal", pk, neighbour, graphLevel)
					}
				}
				if graphLevel == 31 {
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestVector_InsertRepairsLegacyMissingEntryVector(t *testing.T) {
	ctx := context.Background()
	db, err := Open("", WithInMemory(true), WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bucket, err := RegisterBucket[vectorRepairDoc](db, "vec_repair_entry")
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Insert(ctx, &vectorRepairDoc{ID: "a", Embed: []float32{1, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Insert(ctx, &vectorRepairDoc{ID: "b", Embed: []float32{0, 1, 0}}); err != nil {
		t.Fatal(err)
	}
	var deletedEntry []byte
	if err := db.bdb.View(func(tx *badger.Txn) error {
		var err error
		deletedEntry, _, err = bucket.vecIdx.readEntry(tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Delete(ctx, string(deletedEntry)); err != nil {
		t.Fatal(err)
	}

	// v0.3.0 removed this raw value but retained the tombstone, level,
	// and entry-point keys while another live graph node still existed.
	err = db.bdb.Update(func(tx *badger.Txn) error {
		return tx.Delete(vecRawKey(bucket.name, bucket.vecIdx.field.Name, deletedEntry))
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := bucket.Insert(ctx, &vectorRepairDoc{ID: "c", Embed: []float32{0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	hits, err := bucket.SearchVector(ctx, []float32{0, 0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].Record.ID != "c" {
		t.Fatalf("want repaired graph to return c and the survivor, got %+v", hits)
	}
}

func TestVector_SearchFallsBackWhenLegacyEntryVectorIsMissing(t *testing.T) {
	ctx := context.Background()
	db, _ := Open("", WithInMemory(true), WithLogger(nil))
	defer db.Close()
	bucket, err := RegisterBucket[vectorRepairDoc](db, "vec_search_missing_entry")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := bucket.Insert(ctx, &vectorRepairDoc{
			ID:    fmt.Sprintf("%03d", i),
			Embed: []float32{float32(i + 1), 1, 1},
		}); err != nil {
			t.Fatal(err)
		}
	}
	var entry []byte
	if err := db.bdb.View(func(tx *badger.Txn) error {
		var err error
		entry, _, err = bucket.vecIdx.readEntry(tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Delete(ctx, string(entry)); err != nil {
		t.Fatal(err)
	}
	if err := db.bdb.Update(func(tx *badger.Txn) error {
		return tx.Delete(vecRawKey(bucket.name, bucket.vecIdx.field.Name, entry))
	}); err != nil {
		t.Fatal(err)
	}
	hits, err := bucket.SearchVector(ctx, []float32{100, 1, 1}, SearchVectorOptions{K: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 5 {
		t.Fatalf("fallback hits=%d, want 5", len(hits))
	}
}
