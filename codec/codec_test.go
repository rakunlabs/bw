package codec_test

import (
	"reflect"
	"testing"

	"github.com/rakunlabs/bw/codec"
)

func newSample() Sample {
	return Sample{
		ID:    "u1",
		Name:  "Alice",
		Age:   30,
		Score: 1.5,
		Tags:  []string{"a", "b"},
		Address: SampleAddress{
			City: "Berlin",
		},
	}
}

func TestMsgPack_UnmarshalFields_Subset(t *testing.T) {
	c := codec.MsgPack().(codec.LazyCodec)
	s := newSample()
	data, err := c.Marshal(&s)
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.UnmarshalFields(data, []string{"name", "age"})
	if err != nil {
		t.Fatalf("UnmarshalFields: %v", err)
	}
	if got["name"] != "Alice" {
		t.Fatalf("name = %v, want Alice", got["name"])
	}
	if got["age"] == nil {
		t.Fatalf("age missing")
	}
	if _, present := got["score"]; present {
		t.Fatalf("score should not be present in subset: got %v", got)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 keys, got %d: %v", len(got), got)
	}
}

func TestMsgPack_UnmarshalFields_NestedTopLevel(t *testing.T) {
	// Asking for a nested path lookup like address.city only needs the
	// top-level "address" value, which the engine's CollectFields already
	// reduces to. Make sure that delivers the nested map intact.
	c := codec.MsgPack().(codec.LazyCodec)
	s := newSample()
	data, err := c.Marshal(&s)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.UnmarshalFields(data, []string{"address"})
	if err != nil {
		t.Fatal(err)
	}
	addr, ok := got["address"].(map[string]any)
	if !ok {
		t.Fatalf("address not map: %T %v", got["address"], got["address"])
	}
	if addr["city"] != "Berlin" {
		t.Fatalf("city = %v, want Berlin", addr["city"])
	}
}

func TestMsgPack_UnmarshalFields_EmptyFields(t *testing.T) {
	c := codec.MsgPack().(codec.LazyCodec)
	s := newSample()
	data, _ := c.Marshal(&s)
	got, err := c.UnmarshalFields(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map, got %v", got)
	}
}

func TestMsgPack_UnmarshalFields_MissingKey(t *testing.T) {
	c := codec.MsgPack().(codec.LazyCodec)
	s := newSample()
	data, _ := c.Marshal(&s)
	got, err := c.UnmarshalFields(data, []string{"name", "nope"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["nope"]; ok {
		t.Fatalf("missing key shouldn't be present")
	}
	if got["name"] != "Alice" {
		t.Fatalf("name not decoded: %v", got)
	}
}

func TestJSON_UnmarshalFields(t *testing.T) {
	c := codec.JSON().(codec.LazyCodec)
	data, _ := c.Marshal(JSONSample{Name: "Bob", Age: 25})
	got, err := c.UnmarshalFields(data, []string{"name"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, map[string]any{"name": "Bob"}) {
		t.Fatalf("got %v", got)
	}
}

func TestMsgPack_PlainStructWorks(t *testing.T) {
	// Confirms the new reflect-based codec accepts arbitrary structs
	// without any codegen step.
	type Plain struct {
		X int
		Y string
	}
	c := codec.MsgPack()
	data, err := c.Marshal(Plain{X: 7, Y: "hi"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Plain
	if err := c.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.X != 7 || got.Y != "hi" {
		t.Fatalf("round-trip failed: %+v", got)
	}
}
