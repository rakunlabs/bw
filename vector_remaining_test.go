package bw

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

type vectorMigrationV1 struct {
	ID    string    `bw:"id,pk"`
	Embed []float32 `bw:"embed"`
}

type vectorMigrationV2 struct {
	ID    string    `bw:"id,pk"`
	Embed []float32 `bw:"embed,vector(dim=3,metric=cosine)"`
}

type vectorMigrationV3 struct {
	ID    string    `bw:"id,pk"`
	Embed []float32 `bw:"embed"`
}

type vectorMetricCosine struct {
	ID    string    `bw:"id,pk"`
	Embed []float32 `bw:"embed,vector(dim=2,metric=cosine)"`
}

type vectorMetricDot struct {
	ID    string    `bw:"id,pk"`
	Embed []float32 `bw:"embed,vector(dim=2,metric=dot)"`
}

type vectorDimV1 struct {
	ID    string    `bw:"id,pk"`
	Embed []float32 `bw:"embed,vector(dim=2,metric=cosine)"`
}

type vectorDimV2 struct {
	ID    string    `bw:"id,pk"`
	Embed []float32 `bw:"embed,vector(dim=3,metric=cosine)"`
}

func TestConcurrentVectorWritesWithConflictsDisabled(t *testing.T) {
	db, err := Open("", WithInMemory(true), WithLogger(nil), WithBadgerTune(func(options *badger.Options) {
		options.DetectConflicts = false
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bucket, err := RegisterBucket[vectorRepairDoc](db, "vec_concurrent")
	if err != nil {
		t.Fatal(err)
	}

	const count = 40
	start := make(chan struct{})
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs <- bucket.Insert(context.Background(), &vectorRepairDoc{
				ID:    fmt.Sprintf("%03d", i),
				Embed: []float32{float32(i + 1), 1, 1},
			})
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	err = db.bdb.View(func(tx *badger.Txn) error {
		man, err := bucket.vecIdx.readManifest(tx)
		if err != nil {
			return err
		}
		if man.Count != count {
			t.Fatalf("manifest count=%d, want %d", man.Count, count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceGateAllowsNestedReadWhileWriterWaits(t *testing.T) {
	var gate maintenanceGate
	gate.init()
	gate.RLock()
	writerAcquired := make(chan struct{})
	go func() {
		gate.Lock()
		close(writerAcquired)
	}()
	for {
		gate.mu.Lock()
		waiting := gate.waiting
		gate.mu.Unlock()
		if waiting > 0 {
			break
		}
		runtime.Gosched()
	}

	// This models a standalone read called from an existing read callback.
	gate.RLock()
	gate.RUnlock()
	gate.RUnlock()
	<-writerAcquired
	gate.Unlock()
}

func TestVectorTxRejectsForeignDatabase(t *testing.T) {
	db1, _ := Open("", WithInMemory(true), WithLogger(nil))
	defer db1.Close()
	db2, _ := Open("", WithInMemory(true), WithLogger(nil))
	defer db2.Close()
	bucket, err := RegisterBucket[vectorRepairDoc](db1, "foreign_tx")
	if err != nil {
		t.Fatal(err)
	}
	tx := db2.Begin()
	defer tx.Discard()
	if err := bucket.InsertTx(tx, &vectorRepairDoc{ID: "a", Embed: []float32{1, 0, 0}}); err == nil {
		t.Fatal("foreign transaction unexpectedly accepted")
	}
	if err := bucket.DeleteTx(tx, "a"); err == nil {
		t.Fatal("foreign delete transaction unexpectedly accepted")
	}
}

func TestCompactVectorsRemovesTombstonesAndRebuilds(t *testing.T) {
	db, _ := Open("", WithInMemory(true), WithLogger(nil))
	defer db.Close()
	bucket, err := RegisterBucket[vectorRepairDoc](db, "vec_compact")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 80; i++ {
		if err := bucket.Insert(ctx, &vectorRepairDoc{ID: fmt.Sprintf("%03d", i), Embed: []float32{float32(i + 1), 1, 1}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 30; i++ {
		if err := bucket.Delete(ctx, fmt.Sprintf("%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bucket.CompactVectors(ctx); err != nil {
		t.Fatal(err)
	}

	err = db.bdb.View(func(tx *badger.Txn) error {
		man, err := bucket.vecIdx.readManifest(tx)
		if err != nil {
			return err
		}
		if man.Count != 50 {
			t.Fatalf("manifest count=%d, want 50", man.Count)
		}
		tombPrefix := append(vecFieldPrefix(bucket.name, bucket.vecIdx.field.Name), vecTombMark...)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = tombPrefix
		it := tx.NewIterator(opts)
		defer it.Close()
		it.Seek(tombPrefix)
		if it.ValidForPrefix(tombPrefix) {
			t.Fatal("compaction left vector tombstones")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	hits, err := bucket.SearchVector(ctx, []float32{80, 1, 1}, SearchVectorOptions{K: 50, EfSearch: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 50 {
		t.Fatalf("hits=%d, want 50", len(hits))
	}

	// A marker left by an interrupted compaction is resumed during
	// registration before the bucket is returned.
	if err := db.bdb.Update(func(tx *badger.Txn) error {
		return tx.Set(vecMaintenanceKey(bucket.name, bucket.vecIdx.field.Name), []byte{1})
	}); err != nil {
		t.Fatal(err)
	}
	resumed, err := RegisterBucket[vectorRepairDoc](db, "vec_compact")
	if err != nil {
		t.Fatal(err)
	}
	pending, err := resumed.vecIdx.maintenancePending(db)
	if err != nil {
		t.Fatal(err)
	}
	if pending {
		t.Fatal("registration left compaction maintenance pending")
	}
}

func TestVectorSchemaMigrations(t *testing.T) {
	ctx := context.Background()
	db, _ := Open("", WithInMemory(true), WithLogger(nil))
	defer db.Close()

	v1, err := RegisterBucket[vectorMigrationV1](db, "vec_schema_migration", WithVersion[vectorMigrationV1](1))
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Insert(ctx, &vectorMigrationV1{ID: "a", Embed: []float32{1, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := v1.Insert(ctx, &vectorMigrationV1{ID: "b", Embed: []float32{0, 1, 0}}); err != nil {
		t.Fatal(err)
	}

	v2, err := RegisterBucket[vectorMigrationV2](db, "vec_schema_migration", WithVersion[vectorMigrationV2](2))
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Insert(ctx, &vectorMigrationV1{ID: "stale", Embed: []float32{0, 0, 1}}); !errors.Is(err, ErrStaleBucket) {
		t.Fatalf("old schema handle: want ErrStaleBucket, got %v", err)
	}
	hits, err := v2.SearchVector(ctx, []float32{1, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].Record.ID != "a" {
		t.Fatalf("added vector index returned %+v", hits)
	}
	if err := db.bdb.Update(func(tx *badger.Txn) error {
		return tx.Set(vectorReconcileKey("vec_schema_migration"), []byte{1})
	}); err != nil {
		t.Fatal(err)
	}
	v2, err = RegisterBucket[vectorMigrationV2](db, "vec_schema_migration", WithVersion[vectorMigrationV2](2))
	if err != nil {
		t.Fatalf("resume vector reconciliation: %v", err)
	}
	pending, err := v2.vectorReconcilePending()
	if err != nil {
		t.Fatal(err)
	}
	if pending {
		t.Fatal("vector reconciliation marker was not cleared")
	}

	v3, err := RegisterBucket[vectorMigrationV3](db, "vec_schema_migration", WithVersion[vectorMigrationV3](3))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v3.SearchVector(ctx, []float32{1, 0, 0}); !errors.Is(err, ErrNoVector) {
		t.Fatalf("removed vector field: want ErrNoVector, got %v", err)
	}
	fields, err := vectorFieldsOnDisk(db, "vec_schema_migration")
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 0 {
		t.Fatalf("removed vector field left namespaces %v", fields)
	}
}

func TestVectorMetricAndDimensionMigrations(t *testing.T) {
	ctx := context.Background()
	db, _ := Open("", WithInMemory(true), WithLogger(nil))
	defer db.Close()

	cosineBucket, err := RegisterBucket[vectorMetricCosine](db, "vec_metric_migration", WithVersion[vectorMetricCosine](1))
	if err != nil {
		t.Fatal(err)
	}
	_ = cosineBucket.Insert(ctx, &vectorMetricCosine{ID: "magnitude", Embed: []float32{10, 0}})
	_ = cosineBucket.Insert(ctx, &vectorMetricCosine{ID: "direction", Embed: []float32{1, 1}})
	dotBucket, err := RegisterBucket[vectorMetricDot](db, "vec_metric_migration", WithVersion[vectorMetricDot](2))
	if err != nil {
		t.Fatal(err)
	}
	hits, err := dotBucket.SearchVector(ctx, []float32{1, 1}, SearchVectorOptions{K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Record.ID != "magnitude" {
		t.Fatalf("dot metric migration returned %+v", hits)
	}

	dim1, err := RegisterBucket[vectorDimV1](db, "vec_dim_migration", WithVersion[vectorDimV1](1))
	if err != nil {
		t.Fatal(err)
	}
	_ = dim1.Insert(ctx, &vectorDimV1{ID: "a", Embed: []float32{1, 0}})
	dim2, err := RegisterBucket[vectorDimV2](db, "vec_dim_migration",
		WithVersion[vectorDimV2](2),
		WithVectorReembed[vectorDimV2](1, 2, func(_ context.Context, record *vectorDimV2) ([]float32, error) {
			return []float32{0, 0, 1}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	dimHits, err := dim2.SearchVector(ctx, []float32{0, 0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(dimHits) != 1 || dimHits[0].Record.ID != "a" {
		t.Fatalf("dimension migration returned %+v", dimHits)
	}
}

func TestVectorDimensionMigrationRequiresReembed(t *testing.T) {
	ctx := context.Background()
	db, _ := Open("", WithInMemory(true), WithLogger(nil))
	defer db.Close()
	v1, err := RegisterBucket[vectorDimV1](db, "vec_dim_reject", WithVersion[vectorDimV1](1))
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Insert(ctx, &vectorDimV1{ID: "a", Embed: []float32{1, 0}}); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterBucket[vectorDimV2](db, "vec_dim_reject", WithVersion[vectorDimV2](2)); !errors.Is(err, ErrDimMismatch) {
		t.Fatalf("dimension change without reembed: want ErrDimMismatch, got %v", err)
	}

	// Failed preflight must leave the original schema and graph usable
	// after re-registration.
	v1, err = RegisterBucket[vectorDimV1](db, "vec_dim_reject", WithVersion[vectorDimV1](1))
	if err != nil {
		t.Fatal(err)
	}
	hits, err := v1.SearchVector(ctx, []float32{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Record.ID != "a" {
		t.Fatalf("failed dimension migration damaged old index: %+v", hits)
	}
}

func TestSameSchemaReembedInvalidatesOldHandle(t *testing.T) {
	ctx := context.Background()
	db, _ := Open("", WithInMemory(true), WithLogger(nil))
	defer db.Close()
	v1, err := RegisterBucket[vectorDimV1](db, "vec_same_reembed", WithVersion[vectorDimV1](1))
	if err != nil {
		t.Fatal(err)
	}
	if err := v1.Insert(ctx, &vectorDimV1{ID: "a", Embed: []float32{1, 0}}); err != nil {
		t.Fatal(err)
	}
	_, err = RegisterBucket[vectorDimV1](db, "vec_same_reembed",
		WithVersion[vectorDimV1](2),
		WithVectorReembed[vectorDimV1](1, 2, func(_ context.Context, _ *vectorDimV1) ([]float32, error) {
			return []float32{0, 1}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v1.SearchVector(ctx, []float32{1, 0}); !errors.Is(err, ErrStaleBucket) {
		t.Fatalf("old handle after same-schema reembed: want ErrStaleBucket, got %v", err)
	}
}
