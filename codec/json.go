package codec

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// JSON returns a JSON codec backed by encoding/json.
//
// JSON honours the standard `json:"..."` struct tag. The bw schema layer is
// independent and parses the `bw` tag for keys/indexes; the codec is
// only responsible for value encoding, so users who pick the JSON codec
// should typically tag fields with both `bw:"name,pk"` and
// `json:"name"`.
func JSON() Codec {
	return jsonCodec{}
}

type jsonCodec struct{}

func (jsonCodec) Name() string { return "json" }

func (jsonCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}

	return b, nil
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}

	return nil
}

func (jsonCodec) UnmarshalMap(data []byte) (map[string]any, error) {
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("json unmarshal map: %w", err)
	}

	return out, nil
}

// UnmarshalFields satisfies codec.LazyCodec. The JSON path doesn't get a
// real performance win from token-streaming (encoding/json doesn't expose
// cheap value-skipping), so we fall back to a full decode and project
// down. Correct behaviour for any caller that wants a uniform interface.
func (c jsonCodec) UnmarshalFields(data []byte, fields []string) (map[string]any, error) {
	if len(fields) == 0 {
		return map[string]any{}, nil
	}
	full, err := c.UnmarshalMap(data)
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		if v, ok := full[f]; ok {
			out[f] = v
		}
	}

	return out, nil
}
