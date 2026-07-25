package bw

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

// BenchmarkFTSRanking measures a query on the shape that hurts: a corpus with a
// single-domain vocabulary, where every query term appears in nearly every
// document (a JIRA project of one team's tickets, say).
//
// postings_keyonly is the floor for the current index layout — iterating the
// same posting keys and doing nothing with them. The gap between it and
// rank_full is the scoring work; the floor itself is Badger's LSM iterator, at
// roughly 150ns per posting, and only walking fewer postings can move it.
func BenchmarkFTSRanking(b *testing.B) {
	var docs []*Doc
	for i := range 10000 {
		docs = append(docs, &Doc{
			ID:    fmt.Sprintf("web:jira/PAY-%05d.md#0", i),
			Repo:  "web:jira",
			Title: fmt.Sprintf("PAY-%d payment gateway timeout capture", i),
			Body:  strings.Repeat("payment gateway timeout capture. ", 4),
		})
	}
	bucket, ctx := newDocBucket(b, docs)
	query := "payment gateway timeout capture"

	b.Run("rank_full", func(b *testing.B) {
		for b.Loop() {
			if _, _, err := bucket.Search(ctx, query, 5, 0); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("postings_keyonly", func(b *testing.B) {
		fi := bucket.ftsIdx
		var walked int
		for b.Loop() {
			walked = 0
			err := bucket.db.View(func(tx *Tx) error {
				for _, term := range strings.Fields(query) {
					for _, f := range fi.fields {
						prefix := ftsPostingTermPrefix(fi.bucket, f.Name, []byte(term))
						opts := badger.DefaultIteratorOptions
						opts.Prefix = prefix
						opts.PrefetchValues = false
						it := tx.btx.NewIterator(opts)
						for it.Seek(prefix); it.Valid(); it.Next() {
							walked++
						}
						it.Close()
					}
				}

				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(walked), "postings/op")
	})
}
