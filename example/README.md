# bw examples

Three runnable demos covering different facets of the library. Each
lives in its own subdirectory; run them from the repository root so
relative paths resolve.

| Path | What it shows |
|------|---------------|
| [`./cluster`](./cluster) | Multi-node cluster with leader election and forwarding (uses [alan](https://github.com/rakunlabs/alan)). |
| [`./markdown`](./markdown) | Full-text search over markdown files: load, search, edit, re-search. |
| [`./vector`](./vector) | Vector search with `WithEmbedder`, `query.Query` filter, update, and Backup/Restore portability. |
| [`./migration`](./migration) | User-driven schema migration: typed v1 → v2 step, progress hook, idempotent re-run. |

Each subdirectory has its own README with the run command and a
walkthrough of what to expect.
