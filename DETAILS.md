## Performance

Synthetic 10 000-record dataset, in-memory Badger, AMD Ryzen 7 5800X,
default codec (vmihailenco/msgpack/v5, reflect-based, no codegen),
single goroutine.

The `_FullScan` rows below are run on a parallel `PersonPlain` bucket
that has the same shape as `Person` but no `index` tags, so the same
query is forced down the residual-only path. They are the
apples-to-apples baseline for the index-driven rows immediately above
them.

```
                                       ns/op       B/op   allocs/op   note
BenchmarkInsert                          6,236      1,846       40   no indexes
BenchmarkInsert_Indexed                 12,646      3,918      101   ~2× write cost of index maintenance
BenchmarkGet                             2,160      1,230       27   pk lookup

BenchmarkFind_EqIndexed              2,203,737  1,389,592   24,839   } 4.8× faster
BenchmarkFind_EqFullScan            10,676,773  8,169,171  175,180   }

BenchmarkFind_RangeIndexed           6,275,854  4,045,633   69,077   } 1.9× faster
BenchmarkFind_RangeFullScan         11,826,207  8,950,903  150,985   }

BenchmarkFind_InListIndexed         11,067,271  7,306,713  122,231   5 point seeks
BenchmarkFind_EqIndexedPlusResidual  1,883,360  1,293,111   21,343   index seek + ILike on candidates
BenchmarkFind_ComplexAndOr           9,852,006  6,332,668  123,199   one index hit, big residual
BenchmarkFind_SortLimit              2,583,401  1,389,592   24,842   indexed seek + typed-slice sort
BenchmarkWalk_Indexed                2,385,073  1,258,477   24,826   streaming form of EqIndexed

BenchmarkFind_EqUnindexed            9,939,978  6,754,650  140,930   no index → lazy borrowed-slice scan
BenchmarkFind_OrAcrossFields        15,556,697 11,348,802  225,697   OR can't share one index
BenchmarkFind_LikePrefix             8,194,592  6,999,213  117,240   wildcard not index-friendly
BenchmarkFind_ILikeContains          8,781,939  7,108,834  127,240   leading wildcard, full scan
BenchmarkFind_NestedDotPath         18,086,797 15,772,537  315,182   nested field, top-level subtree
BenchmarkFind_FullScan              17,513,370  9,485,698  160,953   no Where, sort all 10k typed
```

### What makes bw fast

**1. Index seeks** for top-level `eq`/`in`/range comparisons on
`index`-tagged fields. Eliminates the bucket-wide iterator pass for the
vast majority of practical queries.

**2. Cached field encoders.** `RegisterBucket` builds one closure per
indexable field, dispatched on the static field kind. The write and
planner paths reuse those closures, so they never go through
`interface{}` boxing or per-record `reflect.Kind` switches.

**3. Lazy partial decode.** When a residual filter only touches a few
fields, the codec walks the encoded record token-by-token, decodes the
values it needs, and `Skip()`s every other length-prefixed value.

**4. Borrowed-slice scan.** The engine evaluates the residual filter
against Badger's mmap'd value bytes directly. The bytes are copied into
a fresh slice only when the record actually survives the filter — so
records that fail residual never pay a value-copy.

**5. Typed-slice sort.** `Bucket.Find` decodes each match into `*T` once
and sorts the typed slice using a precomputed `reflect.FieldByIndex`
accessor. No `map[string]any` round-trip for sort keys.

Headline takeaways:

- **Indexed equality is ~5× faster** than the same query on a non-indexed
  bucket on identical data.
- **Index seek + unindexable residual** runs in ~1.9 ms — the seek
  narrows the candidate set, then the residual filter evaluates lazily
  against the survivors.
- **Insert overhead with all 4 maintained indexes is ~2×** vs an
  unindexed bucket. Cost scales linearly with the number of `index` /
  `unique` fields and includes the read-old-record step.
- **Operations that cannot use an index** (OR across fields, leading-`%`
  ILike, predicates on un-indexed fields, nested dot-paths) still full
  scan, but lazy decode + borrowed-slice keep them under ~18 ms.

### Reproduce

```sh
go test -bench=. -benchmem -run=^$ -benchtime=1s ./...
```

Tune dataset size with `BW_BENCH_N`:

```sh
BW_BENCH_N=100000 go test -bench=. -benchtime=3s -run=^$ ./...
```

---

## Storage layout

```
data:    <bucket>\x00<pk>                                       → encoded record
index:   \x00idx\x00<bucket>\x00<field>\x00<lp(value)><pk>      → empty
unique:  \x00uniq\x00<bucket>\x00<field>\x00<value>             → pk
meta:    \x00meta\x00<bucket>                                   → schema fingerprint
fts:     \x00fts\x00<bucket>\x00…                               → BM25 postings, doc lengths, corpus stats
trigram: \x00tri\x00<bucket>\x00p\x00<field>\x00<tri:3><id:4>   → empty
         \x00tri\x00<bucket>\x00i\x00<pk>                       → interned document id
         \x00tri\x00<bucket>\x00r\x00<id:4>                     → primary key
         \x00tri\x00<bucket>\x00f\x00<field>\x00<id:4>           → field-indexed marker
```

- `\x00` separators keep prefix scans cheap.
- Indexed values are uvarint length-prefixed so they may safely contain
  `\x00`.
- Indexed values use a sortable encoding (`internal/keyenc`): integers
  flip the sign bit, floats flip a sign-aware mask, time uses unix-nano
  big-endian. Lexicographic byte order then matches natural value order,
  which is what makes index range scans correct.
- All index/unique writes happen inside the same `*badger.Txn` as the
  data write, so a crash either commits both or neither.

### Schema fingerprint

`RegisterBucket` writes a SHA-256 of `(field name, flags)` for every
indexed/unique/pk field to `\x00meta\x00<bucket>` on first use. Subsequent
opens compare and refuse to proceed if the fingerprint differs — adding,
removing, or renaming an index requires a deliberate migration so we
don't silently leave dangling index keys behind.

`vector(inline=…)` is deliberately **not** part of the fingerprint: it
changes neither the index keyspace nor the vector keyspace, only how much
of a record the encoder writes from that point on. Flipping it is
forward-only — records written before the change keep their inline copy
until they are rewritten.

---

## Regular-expression search

A `trigram`-tagged string field carries a byte-trigram index that turns a
regular expression into a candidate set. `RegexSearch` / `RegexWalk` then
run the compiled regexp over each candidate's stored value: the index only
removes reading, it never decides a match, so the answer is exactly what a
linear scan would have produced.

```go
type Chunk struct {
    ID      string `bw:"id,pk"`
    Repo    string `bw:"repo,index"`
    Snippet string `bw:"snippet,fts,trigram"`
}

hits, total, err := chunks.RegexSearch(ctx, `func \(m \*Manager\)`, bw.RegexOptions{
    CaseSensitive: true,
    KeyFilter:     func(id string) bool { return strings.HasPrefix(id, "acme/app/") },
})
```

### Planning

`regexTrigramQuery` walks the parsed `regexp/syntax` tree and returns a
boolean expression over trigrams that every match must satisfy. Literals
contribute their trigrams; adjacent literals inside a concatenation are
merged first, which is what makes `errors\.Is\(` selective even though the
parser splits it into four atoms. Alternation becomes a disjunction, `+`
and `{n,}` with `n ≥ 1` pass through to their operand, and everything else
— character classes, `*`, `?`, anchors — widens to "no constraint".

Widening is always safe: a pattern the planner cannot constrain costs a
scan of the bucket, never a wrong answer. That is also the escape hatch
for a pattern with no three-byte literal run at all (`ab.cd`, `[a-z]+`).

### Case folding

Trigrams are folded per byte over ASCII on both the write and the query
side. Folding per byte rather than per rune is what keeps a trigram
exactly three bytes, which is what makes the posting key fixed-width. The
consequence is that case-sensitive and case-insensitive searches share one
index and cost the same — only the compiled regexp differs. A
case-*insensitive* literal containing a non-ASCII rune is the one shape
the planner refuses to constrain, because Unicode folding does not
preserve byte length.

### Document ids

A posting names a 4-byte interned document id, not the primary key. A code
chunk emits on the order of a thousand trigrams, so the posting key *is*
the index's cost: interning makes consecutive postings under one trigram
differ only in their last four bytes, which is what lets Badger's
per-block key diffing store each in a handful of bytes. The pk↔id tables
outlive a document's deletion so re-indexing the same pk reuses its id —
the normal shape for a refresh that deletes and rewrites a corpus.

A write diffs the record's new trigrams against its previous ones, which is
only correct against a term set that was actually written — so the index
keeps one marker key per (document, tagged field) recording that the field
has postings. Indexedness cannot be per document: a bucket that gains the
`trigram` tag on a *second* field would rewrite each record, find the
document already interned, diff the second field's unchanged value against
itself, and leave that field permanently unsearchable with no error. One
marker beside a thousand postings does not move the size story.

### Intersection

An AND is answered by materialising the *rarest* trigram's posting list
and point-reading the others for each candidate. Rarity comes from a
capped probe (512 keys): the exact length of a long posting list cannot
change the choice, and walking it to find out would cost more than the
plan saves. At most eight trigrams are verified per candidate; dropping
the rest only admits false candidates, which the regexp rejects.

Beyond `MaxCandidates` (250 000 by default) the prefilter stops paying for
itself and the search falls back to a bucket scan — the same reasoning as
the vector index's brute-force threshold.

### Cost

Measured over 1.9 MiB of Go source (the krabby repository) chunked the way
a code index chunks it, the trigram keyspace adds this much LSM:

| chunk size | added index | ratio to source |
| --- | --- | --- |
| 3 000 chars | 8.9 MiB | 4.5× |
| 12 000 chars | 5.6 MiB | 2.9× |

The ratio is driven by document granularity, not by the corpus: the number
of *distinct* trigrams in a document grows far more slowly than its size,
so a posting amortises over more source bytes the larger the document is.
A 3 000-character chunk holds roughly one posting per two source bytes.
Chunk larger when index size matters more than match locality.

Trigram writes also dominate a record's transaction footprint — a chunk
expands into ~1 500 keys — so a writer batching records should be prepared
to halve on `ErrTxnTooBig` rather than assume a fixed batch size.

---

## Vector search

### Filtered search

A filter is resolved to the set of primary keys that survive it, and the
vector pass only ranks those keys.

Resolving it never decodes a record when it doesn't have to. An index
entry already embeds the primary key it points at, so a plan that is a
pure index seek with no residual predicate reads its key set straight off
the index keyspace (`engine.IndexScanKeys`). Only a full scan, a residual
predicate, or paging falls back to materialising records.

This matters far more on a vector bucket than elsewhere. A record on such
a bucket is mostly embedding — msgpack spends five bytes per `float32`, so
at 3072 dimensions the vector is ~15 KB of a ~17 KB record — and the
filter pass would decode all of it only to throw everything but the key
away. Resolving a filter over a tenant of *N* chunks cost *N* Badger gets
and *N* full record decodes; now it costs one index scan.

### Brute force vs the graph

HNSW applies a filter *after* a node has been fetched and scored. Rejected
nodes never enter the result heap, so the early-termination test stays
false and the search keeps expanding: a selective filter makes the graph
walk longer, not shorter, and costs recall on top.

So when the allow-set is small relative to the corpus — currently an
eighth, floor 64 — the query is answered by scoring the allow-set
directly. That is exact, and it reads members by key so the decoded-vector
cache can serve them. The previous rule scanned the entire vector
keyspace and *filtered* it, which made a query over a small tenant cost as
much as a query over the whole corpus.

Measured on 20 000 records at 768 dimensions, on disk, `K=10`:

```
filter selects        before              after            change
none  (20 000)    5.27 ms /  11 380   4.84 ms /  8 113     1.1× 
49%   ( 9 800)  343.31 ms / 372 652  12.44 ms / 34 378    27.6×
10%   ( 2 000)  102.63 ms / 156 083   2.07 ms /  6 441    49.5×
1%    (   200)  174.03 ms / 739 632   0.59 ms /  1 008   295.0×
0.05% (    10)   21.19 ms /  73 696   0.35 ms /    407    59.9×
```

(ns/op and allocs/op.) Recall is unchanged: 0.792/0.932/0.994 at
efSearch 100/200/400 unfiltered, and 1.000/1.000/1.000/0.986 across the
filter widths above.

Note the shape of the "before" column: the more selective the filter, the
slower the query got. That inversion is what these two changes remove.

### Distance kernels

`cosine` is the default metric and the inner cost of every search, so the
search path scores through a `queryScorer` that computes the query's own
norm once per query instead of once per candidate, and uses a kernel
unrolled by 2.

Unrolling by 2 rather than 4 is deliberate: computing the dot product and
the candidate norm together needs two accumulator sets, and at four-way
unrolling the eight accumulators plus eight loaded values exceed the XMM
register file. Measured at 3072 dimensions: 2021 ns plain, **1031 ns at
four accumulators**, 1982 ns at eight — the spills give back the entire
gain.

```
                        plain kernel    queryScorer
cosine dim=768              509 ns         266 ns
cosine dim=1536            1015 ns         519 ns
cosine dim=3072            2021 ns        1032 ns
```

Summation order differs from the plain kernel, so scores differ in the
last bits — measured worst case 1.15e-07 relative. `dot` and `l2` are
untouched and remain bit-identical.

### Storing the vector once

A vector is always written to its own keyspace. Keeping a second copy
inside the record is pure duplication, and at embedding widths it is the
*larger* copy. `vector(inline=false)` drops it from the encoded record:

```go
type Chunk struct {
    ID   string    `bw:"id,pk"`
    Repo string    `bw:"repo,index"`
    Text string    `bw:"text"`
    Emb  []float32 `bw:"emb,vector(metric=cosine,inline=false)"`
}
```

At 768 dimensions this makes stored records ~170× smaller. Search,
filtering and ranking are unaffected because they read the vector
keyspace; the cost is that reads return the field empty. It is off by
default, and worth turning on when the vector is written but never read
back — the usual shape for a chunk store.
