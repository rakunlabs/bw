package bw_test

// Package-scoped record types shared by the test/bench suites. With the
// reflect-based default codec there's no codegen step — these are just
// plain Go structs.

// User is the canonical bucket type used by the bw_test.go suite.
type User struct {
	ID    string `bw:"id,pk"`
	Name  string `bw:"name,index"`
	Email string `bw:"email,unique"`
	Age   int    `bw:"age,index"`
	Bio   string `bw:"-"`
}

// PersonAddress is used by TestNestedDotPath.
type PersonAddress struct {
	City string `bw:"city"`
}

// NestedPerson is used by TestNestedDotPath.
type NestedPerson struct {
	ID      string        `bw:"id,pk"`
	Address PersonAddress `bw:"address"`
}

// Thing is used by TestNoPKBucket.
type Thing struct {
	Name string `bw:"name"`
}

// SchemaV1 / SchemaV2 are used by TestSchema_FingerprintMismatch.
type SchemaV1 struct {
	ID   string `bw:"id,pk"`
	Name string `bw:"name,index"`
}

type SchemaV2 struct {
	ID    string `bw:"id,pk"`
	Name  string `bw:"name,index"`
	Email string `bw:"email,unique"`
}

// PlainTag is used by TestPlanner_NoIndexFallsBackToFullScan.
type PlainTag struct {
	ID  string `bw:"id,pk"`
	Tag string `bw:"tag"`
}

// PersonAddressBench is the nested address struct used by the bench
// types. Promoted to package scope alongside the others for symmetry.
type PersonAddressBench struct {
	City string `bw:"city"`
	Zip  string `bw:"zip"`
}

// Person is the indexed bench bucket type.
type Person struct {
	ID      string             `bw:"id,pk"`
	Name    string             `bw:"name,index"`
	Email   string             `bw:"email,unique"`
	Age     int                `bw:"age,index"`
	Country string             `bw:"country,index"`
	Score   float64            `bw:"score"`
	Address PersonAddressBench `bw:"address"`
	Tags    []string           `bw:"tags"`
}

// PersonPlain mirrors Person without index/unique tags so the bench can
// measure full-scan cost on identical data.
type PersonPlain struct {
	ID      string             `bw:"id,pk"`
	Name    string             `bw:"name"`
	Email   string             `bw:"email"`
	Age     int                `bw:"age"`
	Country string             `bw:"country"`
	Score   float64            `bw:"score"`
	Address PersonAddressBench `bw:"address"`
	Tags    []string           `bw:"tags"`
}

// CodecSample is used by codec/codec_test.go.
type CodecSample struct {
	ID      string             `bw:"id"`
	Name    string             `bw:"name"`
	Age     int                `bw:"age"`
	Score   float64            `bw:"score"`
	Tags    []string           `bw:"tags"`
	Address PersonAddressBench `bw:"address"`
}

// Location tests composite indexes and unique constraints.
type Location struct {
	ID      string `bw:"id,pk"`
	Country string `bw:"country,index:region"`
	City    string `bw:"city,index:region"`
	Code    string `bw:"code,unique:country_code"`
	Prefix  string `bw:"prefix,unique:country_code"`
}
