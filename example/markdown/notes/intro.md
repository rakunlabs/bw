# Introduction to bw

bw is a small embedded database built on top of BadgerDB. It speaks the
language of typed buckets, secondary indexes, and full-text search.

## What it does well

- Atomic writes: every Insert touches data, indexes, FTS, and vectors
  inside a single Badger transaction.
- Portable backups: a Backup taken on one machine restores cleanly on
  another. Search and vectors keep working without a rebuild step.
- Cluster-friendly: the leader pushes incremental backups to followers
  on a tick and they catch up automatically.

## What's next

Stay tuned for vector search and HNSW.
