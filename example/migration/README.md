# User migration demo

A 3-phase walkthrough of bw's user-defined migration framework. The
bucket evolves from a single `Name` field (v1) to split `First` /
`Last` fields (v2); a typed migration callback handles the rewrite,
and bw drives it across every record in the bucket.

## Run

From the repo root:

```bash
go run ./example/migration
```

## Phases

1. **Insert under v1.** A `UserV1` struct with a single `Name` field
   is registered at `WithVersion(1)`. Three users are inserted.
2. **Reopen as v2.** The bucket type changes to `UserV2` (split into
   `First` and `Last`, with `First` indexed). A
   `WithTypedMigration[UserV1, UserV2](1, 2, fn)` describes how to
   convert one shape into the other and automatically makes `2` the
   target version; there is no duplicate `WithVersion(2)` to keep in
   sync. bw walks every record, runs the callback, and writes the
   result back through the ordinary `Insert` path so secondary
   indexes stay coherent. A progress hook reports how many records
   have been processed.
3. **Verify.** The same bucket handle now reads `UserV2` records.
   Re-running `RegisterBucket` at v2 is a no-op — the migration is
   idempotent because the persisted version key already says `2`.

When deployments may skip releases, keep every migration step registered. A
database at v3 can reach v6 through `3->4`, `4->5`, and `5->6` (or through one
explicit `3->6` migration). bw validates the complete path before changing the
first record, so a missing intermediate step fails without leaving the bucket
half-upgraded.

## Schema-only vs data migration

Do not write a record callback when only the model metadata changes. For
example, adding a field whose zero value is acceptable or changing an
index/unique tag only needs a version bump:

```go
bw.RegisterBucket[UserV6](db, "users", bw.WithVersion[UserV6](6))
```

With no data migration options, this can move directly from stored v3 to v6;
no `3->4`, `4->5`, or `5->6` callbacks are required. Existing records are not
rewritten.

Use a data migration when existing encoded values must change: renaming a
serialized `bw` field, changing a field type, splitting/merging fields,
backfilling values, or recomputing embeddings.

If schema-only and data-changing releases are mixed, a missing data step is
not assumed to be a no-op. Register a complete conversion path for every
stored version that may still exist. For a `3->4` data change, schema-only
`4->5`, and a `5->6` data change, a v6 binary can register `3->4`, `4->6`, and
`5->6`. This lets v3 follow `3->4->6` while v4 and v5 each have a direct path,
without an identity migration that rewrites all records.

## Other migration shapes

For the same plumbing without a typed Old struct, use
`WithRawMigration` and decode bytes yourself (e.g. via
`codec.UnmarshalMap` for "JSON-like" edits):

```go
bw.WithRawMigration[User](1, 2, func(_ context.Context, raw []byte) ([]byte, error) {
    m, _ := codec.MsgPack().UnmarshalMap(raw)
    m["email"] = strings.ToLower(m["email"].(string))
    return codec.MsgPack().Marshal(m)
})
```

When the only thing that changes between versions is the vector
field (different embedding model, same data), reach for
`WithVectorReembed`:

```go
bw.WithVectorReembed[Doc](2, 3, func(ctx context.Context, d *Doc) ([]float32, error) {
    return newModel.Embed(ctx, d.Title + "\n" + d.Body)
})
```

## Resume after crash

If the process dies mid-migration, the next `RegisterBucket` picks up
where it left off. The runner persists its cursor (`last successfully
migrated pk`) between batches; on restart it skips everything up to
and including that pk and processes the rest. The bucket's version
key is only advanced once a step finishes cleanly, so a partial run
never lies about what shape the data is in.

## Failure semantics

A user-supplied migration that returns an error aborts the run. The
batch in flight is rolled back via Badger's transaction discard,
nothing prior to that point is touched, and the bucket's stored
version stays at its previous value. Re-running `RegisterBucket` will
restart the failed step from its last cursor, so a fix-and-retry
cycle is safe.
