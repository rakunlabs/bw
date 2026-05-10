// User migration demo.
//
//  1. Open a fresh database, register the bucket at v1, insert a few
//     records under the v1 schema (`Name` is a single field).
//  2. Close the DB, then reopen it under the v2 schema where `Name`
//     has been split into `First` and `Last`. A typed migration step
//     handles the conversion.
//  3. Read the records back through the v2 bucket and confirm the
//     fields ended up where they should.
//
// Run from the repo root:
//
//	go run ./example/migration
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/rakunlabs/bw"
)

// Schema v1 — original shape.
type UserV1 struct {
	ID   string `bw:"id,pk"`
	Name string `bw:"name"`
}

// Schema v2 — Name split into First / Last; First is now indexed.
type UserV2 struct {
	ID    string `bw:"id,pk"`
	First string `bw:"first,index"`
	Last  string `bw:"last"`
}

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "bw-mig-demo-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// --- Phase 1: write some records under the v1 schema. ---
	{
		db, err := bw.Open(dir, bw.WithLogger(nil))
		if err != nil {
			log.Fatal(err)
		}
		users, err := bw.RegisterBucket[UserV1](db, "users",
			bw.WithVersion[UserV1](1),
		)
		if err != nil {
			log.Fatal(err)
		}
		for _, n := range []string{"Ali Veli", "Ayse Yilmaz", "Mehmet Demir Kaya"} {
			id := strings.ToLower(strings.SplitN(n, " ", 2)[0])
			if err := users.Insert(ctx, &UserV1{ID: id, Name: n}); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("v1 insert id=%-7s name=%q\n", id, n)
		}
		db.Close()
	}

	// --- Phase 2: reopen at v2 with a typed migration step. ---
	db, err := bw.Open(dir, bw.WithLogger(nil))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	users, err := bw.RegisterBucket[UserV2](db, "users",
		bw.WithVersion[UserV2](2),
		bw.WithTypedMigration[UserV1, UserV2](1, 2,
			func(_ context.Context, old *UserV1) (*UserV2, error) {
				// "First Last" → split on the first whitespace.
				// Anything beyond the first space goes into Last,
				// so "Mehmet Demir Kaya" becomes ("Mehmet", "Demir Kaya").
				parts := strings.SplitN(old.Name, " ", 2)
				first := parts[0]
				last := ""
				if len(parts) == 2 {
					last = parts[1]
				}
				return &UserV2{ID: old.ID, First: first, Last: last}, nil
			},
		),
		bw.WithMigrationProgress[UserV2](func(bucket string, fromV, toV uint64, processed, total uint64) {
			fmt.Printf("migration %s v%d->v%d: %d / %d\n", bucket, fromV, toV, processed, total)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// --- Phase 3: read back. ---
	all, err := users.Find(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nv2 records:")
	for _, u := range all {
		fmt.Printf("  id=%-7s first=%-7s last=%q\n", u.ID, u.First, u.Last)
	}

	// Re-running RegisterBucket at v2 is a no-op — the migration
	// already completed and the bucket version is now 2.
	_, err = bw.RegisterBucket[UserV2](db, "users",
		bw.WithVersion[UserV2](2),
		bw.WithTypedMigration[UserV1, UserV2](1, 2,
			func(_ context.Context, _ *UserV1) (*UserV2, error) {
				log.Fatal("migration unexpectedly re-ran")
				return nil, nil
			},
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nsecond Open at v2 was a no-op — migration is idempotent.")
}
