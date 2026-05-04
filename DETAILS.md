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
