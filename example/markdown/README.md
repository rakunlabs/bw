# Markdown FTS demo

A 100-line walkthrough of bw's full-text search showing what happens
when a record's content changes underneath an already-built index.

## Run

```bash
go run ./example/markdown
```

(must be run from the repository root so the `notes/` directory is
found at `example/markdown/notes`)

## What it does

1. Opens a fresh on-disk database under `os.TempDir()` and registers a
   `Note` bucket whose `Title` and `Body` fields are FTS-tagged.
2. Walks `example/markdown/notes/*.md`, inserting each file. The FTS
   posting lists are written inside the same Badger transaction as
   the record itself.
3. Searches for the term `vector` and prints the hits.
4. Rewrites `notes/intro.md` so it now mentions vectors prominently,
   re-Inserts it (overwriting the previous version), and runs the
   same search again. The score climbs because BM25 sees more
   matching terms in the new body, and the printed snippet is the new
   content.
5. On exit, the database directory is removed and the original
   `intro.md` is restored so the next run starts clean.

## Why it matters

The point isn't the search itself — it's the atomicity. If a process
crashes between the data write and the FTS update, neither would be
visible because both are part of the same Badger transaction. The
same property keeps backups portable: the FTS index is just a set of
Badger keys, so `Backup` carries it along with the data and `Restore`
on a different machine has search working immediately, no rebuild
required.
