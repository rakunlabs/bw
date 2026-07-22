package bw

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/rakunlabs/bw/codec"
	"github.com/rakunlabs/query"
)

type snapshotVectorDoc struct {
	ID    string    `bw:"id,pk"`
	Tag   string    `bw:"tag"`
	Embed []float32 `bw:"embed,vector(dim=3,metric=cosine)"`
}

type blockingMapCodec struct {
	codec.Codec
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (c *blockingMapCodec) UnmarshalMap(data []byte) (map[string]any, error) {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.release
	}
	return c.Codec.UnmarshalMap(data)
}

func TestSearchVectorUsesSingleSnapshot(t *testing.T) {
	blocking := &blockingMapCodec{
		Codec:   codec.MsgPack(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	db, err := Open("", WithInMemory(true), WithLogger(nil), WithCodec(blocking))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bucket, err := RegisterBucket[snapshotVectorDoc](db, "vec_snapshot")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := bucket.Insert(ctx, &snapshotVectorDoc{ID: "a", Tag: "keep", Embed: []float32{1, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	filter, err := query.Parse("tag=keep")
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		hits []VectorHit[snapshotVectorDoc]
		err  error
	}
	resultCh := make(chan result, 1)
	blocking.armed.Store(true)
	go func() {
		hits, err := bucket.SearchVector(ctx, []float32{1, 0, 0}, SearchVectorOptions{Filter: filter})
		resultCh <- result{hits: hits, err: err}
	}()
	<-blocking.entered
	if err := bucket.Update(ctx, &snapshotVectorDoc{ID: "a", Tag: "drop", Embed: []float32{0, 1, 0}}); err != nil {
		t.Fatal(err)
	}
	close(blocking.release)
	resultValue := <-resultCh
	if resultValue.err != nil {
		t.Fatal(resultValue.err)
	}
	if len(resultValue.hits) != 1 {
		t.Fatalf("hits=%d, want 1", len(resultValue.hits))
	}
	hit := resultValue.hits[0]
	if hit.Record.Tag != "keep" || hit.Score < 0.99 {
		t.Fatalf("mixed snapshots: record=%+v score=%f", hit.Record, hit.Score)
	}
}
