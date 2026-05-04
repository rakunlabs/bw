package schema_test

import (
	"testing"

	"github.com/rakunlabs/bw/schema"
)

type combo struct {
	ID       string `bw:"id,pk"`
	Username string `bw:"username,index,unique"` // both flags
	Email    string `bw:"email,unique,index"`    // order shouldn't matter
	Tag      string `bw:"tag,index"`
	Plain    string `bw:"plain"`
	Skip     string `bw:"-"`
}

func TestIndexAndUniqueTogether(t *testing.T) {
	s, err := schema.Of(combo{})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if s.PK == nil || s.PK.GoName != "ID" {
		t.Fatalf("pk = %+v", s.PK)
	}
	cases := []struct {
		field         string
		index, unique bool
	}{
		{"username", true, true},
		{"email", true, true},
		{"tag", true, false},
		{"plain", false, false},
	}
	for _, c := range cases {
		f := s.ByName[c.field]
		if f == nil {
			t.Fatalf("missing field %q", c.field)
		}
		if f.Indexed != c.index || f.Unique != c.unique {
			t.Fatalf("%s: indexed=%v unique=%v, want indexed=%v unique=%v",
				c.field, f.Indexed, f.Unique, c.index, c.unique)
		}
	}
	if _, ok := s.ByName["Skip"]; ok {
		t.Fatalf("- field should be skipped")
	}
}

func TestUnknownFlag(t *testing.T) {
	type bad struct {
		ID string `bw:"id,pk,bogus"`
	}
	if _, err := schema.Of(bad{}); err == nil {
		t.Fatalf("expected error for unknown flag")
	}
}

func TestDuplicatePK(t *testing.T) {
	type bad struct {
		A string `bw:"a,pk"`
		B string `bw:"b,pk"`
	}
	if _, err := schema.Of(bad{}); err == nil {
		t.Fatalf("expected error for duplicate pk")
	}
}

func TestCompositeIndexGroup(t *testing.T) {
	type loc struct {
		ID      string `bw:"id,pk"`
		Country string `bw:"country,index:region"`
		City    string `bw:"city,index:region"`
		Code    string `bw:"code,unique:cc"`
		Prefix  string `bw:"prefix,unique:cc"`
		Name    string `bw:"name,index"` // plain single-field index
	}
	s, err := schema.Of(loc{})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}

	// Individual fields should still be marked.
	country := s.ByName["country"]
	if !country.Indexed || country.IndexGroup != "region" {
		t.Fatalf("country: indexed=%v group=%q", country.Indexed, country.IndexGroup)
	}
	code := s.ByName["code"]
	if !code.Unique || code.UniqueGroup != "cc" {
		t.Fatalf("code: unique=%v group=%q", code.Unique, code.UniqueGroup)
	}

	// Composite groups.
	if len(s.CompositeIndexes) != 1 || s.CompositeIndexes[0].Name != "region" {
		t.Fatalf("CompositeIndexes = %+v", s.CompositeIndexes)
	}
	if len(s.CompositeIndexes[0].Fields) != 2 {
		t.Fatalf("region fields = %d", len(s.CompositeIndexes[0].Fields))
	}
	if len(s.CompositeUniques) != 1 || s.CompositeUniques[0].Name != "cc" {
		t.Fatalf("CompositeUniques = %+v", s.CompositeUniques)
	}
	if len(s.CompositeUniques[0].Fields) != 2 {
		t.Fatalf("cc fields = %d", len(s.CompositeUniques[0].Fields))
	}

	// Plain index should still work.
	name := s.ByName["name"]
	if !name.Indexed || name.IndexGroup != "" {
		t.Fatalf("name: indexed=%v group=%q", name.Indexed, name.IndexGroup)
	}
}
