package codec_test

// Package-scoped types for the codec test suite. The default codec is
// reflect-based (vmihailenco/msgpack) configured to honour bw's
// `bw:"…"` struct tag, so no codegen step is required and no extra
// `msgpack:"…"` tag is needed.

// SampleAddress is a nested sub-struct used by Sample.
type SampleAddress struct {
	City string `bw:"city"`
}

// Sample exercises a representative scalar / nested / slice mix for the
// codec round-trip and partial-decode tests.
type Sample struct {
	ID      string        `bw:"id"`
	Name    string        `bw:"name"`
	Age     int           `bw:"age"`
	Score   float64       `bw:"score"`
	Tags    []string      `bw:"tags"`
	Address SampleAddress `bw:"address"`
}

// JSONSample is decoupled from Sample so the JSON codec test can use plain
// json tags without dragging in msg-encoded values.
type JSONSample struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}
