package bw_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/bw/codec"
	"github.com/rakunlabs/query"
)

// countingCodec wraps another codec and tallies how many times each
// decode entry-point is hit. Used by the lazy-decode test below to
// prove the engine prefers the partial path.
type countingCodec struct {
	inner          codec.Codec
	mapCalls       atomic.Int64
	fieldsCalls    atomic.Int64
	fullDecodeCnt  atomic.Int64
	lastFieldsArg  []string
	lastFieldsArgM atomic.Pointer[[]string]
}

func (c *countingCodec) Name() string                  { return c.inner.Name() }
func (c *countingCodec) Marshal(v any) ([]byte, error) { return c.inner.Marshal(v) }
func (c *countingCodec) Unmarshal(data []byte, v any) error {
	c.fullDecodeCnt.Add(1)

	return c.inner.Unmarshal(data, v)
}
func (c *countingCodec) UnmarshalMap(data []byte) (map[string]any, error) {
	c.mapCalls.Add(1)

	return c.inner.UnmarshalMap(data)
}

// Implement LazyCodec.
func (c *countingCodec) UnmarshalFields(data []byte, fields []string) (map[string]any, error) {
	c.fieldsCalls.Add(1)
	if lc, ok := c.inner.(codec.LazyCodec); ok {
		return lc.UnmarshalFields(data, fields)
	}
	// Fallback: full decode + project.
	full, err := c.inner.UnmarshalMap(data)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for _, f := range fields {
		if v, ok := full[f]; ok {
			out[f] = v
		}
	}

	return out, nil
}

func TestLazyDecode_PrefersFieldsPath(t *testing.T) {
	cc := &countingCodec{inner: codec.MsgPack()}
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil), bw.WithCodec(cc))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	users, err := bw.RegisterBucket[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}
	seed(t, users)

	// Reset counters after the seed phase (which goes through Marshal,
	// not the decode path, but be defensive).
	cc.mapCalls.Store(0)
	cc.fieldsCalls.Store(0)

	// Indexed eq with a residual ILike on name — engine should:
	//   1. seek country=US (none of these has country, but pretend by
	//      using a real indexed field — name=Alice). Just use eq on the
	//      indexed name.
	// Use a residual that requires only "name" so the lazy path returns
	// early on a small map.
	q := mustParse(t, "name=Alice&age[gt]=20")
	got, err := users.Find(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("expected at least one match")
	}

	if cc.fieldsCalls.Load() == 0 {
		t.Fatalf("expected UnmarshalFields to be called, got %d (UnmarshalMap=%d)", cc.fieldsCalls.Load(), cc.mapCalls.Load())
	}
	if cc.mapCalls.Load() != 0 {
		t.Fatalf("UnmarshalMap should not be used when LazyCodec is available, got %d", cc.mapCalls.Load())
	}
}

func TestLazyDecode_NoResidualMeansNoMapDecode(t *testing.T) {
	// When the entire query is satisfied by an index seek (no residual),
	// the engine should not need any UnmarshalMap or UnmarshalFields call.
	cc := &countingCodec{inner: codec.MsgPack()}
	db, err := bw.Open("", bw.WithInMemory(true), bw.WithLogger(nil), bw.WithCodec(cc))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	users, _ := bw.RegisterBucket[User](db, "users")
	seed(t, users)

	cc.mapCalls.Store(0)
	cc.fieldsCalls.Store(0)

	// Pure indexed seek, no residual, no sort.
	q := mustParse(t, "name=Alice")
	if _, err := users.Find(context.Background(), q); err != nil {
		t.Fatal(err)
	}

	if cc.mapCalls.Load() != 0 || cc.fieldsCalls.Load() != 0 {
		t.Fatalf("expected zero map decodes, got map=%d fields=%d",
			cc.mapCalls.Load(), cc.fieldsCalls.Load())
	}
}

// keep query import used for parser parity
var _ = query.Parse
