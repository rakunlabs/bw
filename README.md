# bw

[![License](https://img.shields.io/github/license/rakunlabs/bw?color=red&style=flat-square)](https://raw.githubusercontent.com/rakunlabs/bw/main/LICENSE)
[![Coverage](https://img.shields.io/sonar/coverage/rakunlabs_bw?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=flat-square)](https://sonarcloud.io/summary/overall?id=rakunlabs_bw)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/rakunlabs/bw/test.yml?branch=main&logo=github&style=flat-square&label=ci)](https://github.com/rakunlabs/bw/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/rakunlabs/bw?style=flat-square)](https://goreportcard.com/report/github.com/rakunlabs/bw)
[![Go PKG](https://raw.githubusercontent.com/rakunlabs/.github/main/assets/badges/gopkg.svg)](https://pkg.go.dev/github.com/rakunlabs/bw)


`bw` is a thin wrapper around [BadgerDB](https://github.com/dgraph-io/badger)
that exposes a typed bucket API, plus a query engine that consumes
[`github.com/rakunlabs/query`](https://github.com/rakunlabs/query)
expressions directly. URL query strings translate into Go-side filtering,
sorting and pagination over Badger key prefixes.

```mermaid
graph LR
    A["URL query<br/>name=A&age>30"] --> B["query.Parse<br/>*query.Query"] --> C["bw engine<br/>Badger scan"]
```

## Install

```sh
go get github.com/rakunlabs/bw
```

---

## Quick start

```go
package main

import (
    "context"
    "log"

    "github.com/rakunlabs/bw"
    "github.com/rakunlabs/query"
)

// One tag set drives both the bw schema (pk/index/unique flags) and
// the on-wire field name. The codec honours `bw:"-"` to skip a
// field. No codegen needed.
type User struct {
    ID    string `bw:"id,pk"`
    Name  string `bw:"name,index"`
    Email string `bw:"email,unique"`
    Age   int    `bw:"age,index"`
    Bio   string `bw:"-"`             // never serialized
}

func main() {
    db, err := bw.Open("/var/lib/myapp")
    if err != nil { log.Fatal(err) }
    defer db.Close()

    users, err := bw.RegisterBucket[User](db, "users")
    if err != nil { log.Fatal(err) } // fails clearly if you forgot `go generate`

    ctx := context.Background()

    err = users.InsertMany(ctx,
      []*User{
        {ID: "1", Name: "Kemal Sunal", Email: "a@x", Age: 30},
        {ID: "2", Name: "Tarık Akan", Email: "b@x", Age: 25},
      },
    )
    if err != nil { log.Fatal(err) }

    u, _ := users.Get(ctx, "1")
    log.Println(u.Name) // Kemal Sunal

    q, _ := query.Parse("name=Tarık Akan|age[gt]=29&_sort=-age&_limit=10")
    got, _ := users.Find(ctx, q)
    log.Println(got)
}
```

### Tag flags

```go
ID    string `bw:"id,pk"`           // primary key
Name  string `bw:"name,index"`      // ordered index, range/sort friendly
Email string `bw:"email,unique"`    // uniqueness constraint (lookup-style)
User  string `bw:"user,index,unique"` // both: indexed AND unique (allowed)
Tag   string `bw:"-"`               // skip
```

`pk`, `index` and `unique` parse independently; combine them freely.

### Composite indexes and unique constraints

Use `index:groupname` or `unique:groupname` to combine multiple fields
into a single index or unique constraint. Fields sharing the same group
name are concatenated in struct declaration order.

```go
type Location struct {
    ID      string `bw:"id,pk"`
    Country string `bw:"country,index:region"`         // composite index "region"
    City    string `bw:"city,index:region"`             // same group → key is (country, city)
    Code    string `bw:"code,unique:country_code"`      // composite unique "country_code"
    Prefix  string `bw:"prefix,unique:country_code"`    // same group → (code, prefix) must be unique together
    Name    string `bw:"name,index"`                    // plain single-field index (unchanged)
}
```

**Composite indexes** are used by the query planner when all constituent
fields appear as equality conditions:

```
country=TR&city=Istanbul  → composite index seek on "region"
country=TR                → full scan (only 1 of 2 fields supplied)
country=TR&city=Istanbul&name=foo → composite seek + residual filter on name
```

**Composite unique constraints** enforce that the combination of all
grouped fields is unique across the bucket. Individual field values may
repeat as long as the full tuple is distinct:

```
(Code="TR", Prefix="34") + (Code="TR", Prefix="06")  → OK
(Code="TR", Prefix="34") + (Code="TR", Prefix="34")  → ErrConflict (on different PKs)
```

### Schema evolution (adding/removing fields)

When you change a struct (add new fields, add/remove indexes), use
`WithVersion` to tell `RegisterBucket` to auto-migrate:

```go
// V1 — original schema.
type User struct {
    ID   string `bw:"id,pk"`
    Name string `bw:"name,index"`
}

// V2 — added Email (unique) and Age (indexed).
type User struct {
    ID    string `bw:"id,pk"`
    Name  string `bw:"name,index"`
    Email string `bw:"email,unique"`
    Age   int    `bw:"age,index"`
}
```

```go
// Bump the version number each time you change the index/unique surface.
// RegisterBucket auto-migrates when stored version < provided version.
users, err := bw.RegisterBucket[User](db, "users", bw.WithVersion[User](2))
if err != nil {
    log.Fatal(err)
}
```

That's it. No manual two-step `MigrateBucket` call needed — just bump
the version number when you change the struct.

**What happens under the hood (incremental):**

It does NOT drop all indexes and rebuild everything. It diffs the old
schema against the new one and only touches what changed:

1. Fields that **lost** their `index` tag → only those index keys are deleted.
2. Fields that **lost** their `unique` tag → only those unique keys are deleted.
3. Fields that are **newly** indexed/unique → scans data and builds entries
   only for those fields.
4. Fields whose flags are **unchanged** → left completely alone (no I/O).
5. Updates the stored fingerprint, version, and manifest.

**Rules:**

- Additive changes (new fields) are safe — old records get zero values.
- Removing an index is safe — the stale index keys are cleaned up.
- Changing the primary key field or its `bw` tag name requires a manual
  data migration (you'd need to re-key every record).
- Zero-value unique fields (empty string, nil slice) are skipped during
  migration to avoid false conflicts on old records that lack the new
  field.
- If you don't provide `WithVersion`, the old strict behavior applies
  (fingerprint mismatch = error). You can still call `MigrateBucket`
  explicitly in that case.

### Defaults

```go
bw.DefaultCacheSize int64 = 100 << 20   // 100 MiB block cache (Badger default is 256 MiB)
bw.DefaultLogSize   int64 = 100 << 20   // 100 MiB value-log file size (Badger default is 1 GiB)
```

These are package-level vars. Either change them at process start or use
`WithBadgerOptions` to take full control.

### Tuning individual Badger fields

If you only need to change a few Badger fields (`NumVersionsToKeep`,
`NumGoroutines`, `NumCompactors`, etc.) without rebuilding the whole
`badger.Options`, use `WithBadgerTune`. It hands you a `*badger.Options`
that already has bw's defaults (the lighter cache/log sizes above) and
the `path` argument applied — you only supply the delta:

```go
db, err := bw.Open("/var/lib/myapp",
    bw.WithBadgerTune(func(bo *badger.Options) {
        bo.NumVersionsToKeep = 3
        bo.NumGoroutines     = 8
        bo.NumCompactors     = 4
    }),
)
```

Badger's `With*` methods use value receivers, so the chained form must
be reassigned through the pointer:

```go
bw.WithBadgerTune(func(bo *badger.Options) {
    *bo = bo.WithNumVersionsToKeep(3).WithNumGoroutines(8)
})
```

`WithBadgerTune` runs on top of whichever base was chosen (path,
`WithInMemory`, or a full `WithBadgerOptions` override), so it composes
with all three. The logger from `WithLogger` is reapplied AFTER the
tune callback, so set the logger via `WithLogger` rather than mutating
`bo.Logger` inside the tune.

| Option | When to reach for it |
| --- | --- |
| `WithBadgerTune(fn)` | Tweak a few badger fields on top of bw's defaults — keeps `path`, cache/log sizes, etc. |
| `WithBadgerOptions(bo)` | Take full control. `path` is ignored, bw defaults are skipped. |
| `bw.DefaultCacheSize` / `DefaultLogSize` | Change bw's process-wide defaults before any `Open` call. |

The most commonly tuned Badger fields and the values you see inside the
tune callback before you touch anything (effective defaults: Badger's
own values, with bw overriding only `BlockCacheSize` and
`ValueLogFileSize`):

| Field | Effective default | What it controls |
| --- | --- | --- |
| `BlockCacheSize` | `100 << 20` (100 MiB) — **bw override** (Badger: 256 MiB) | LSM block cache size in bytes |
| `ValueLogFileSize` | `100 << 20` (100 MiB) — **bw override** (Badger: 1 GiB) | Max value-log file size before rollover |
| `NumVersionsToKeep` | `1` | Versions kept per key (raise for time-travel / longer incremental backups) |
| `NumGoroutines` | `8` | Worker pool size used by `Stream` (backup, GC, etc.) |
| `NumCompactors` | `4` | Concurrent LSM compaction workers (one dedicated to L0→L1) |
| `NumMemtables` | `5` | In-memory tables kept before stalling writes |
| `NumLevelZeroTables` | `5` | L0 tables before compaction kicks in |
| `NumLevelZeroTablesStall` | `15` | L0 tables before writes stall |
| `Logger` | `bw.newLogger()` — **bw override** | Use `WithLogger` instead; tune-time changes get overwritten |

Run `go doc github.com/dgraph-io/badger/v4.Options` for the full list.

---

## Query syntax

`bw` consumes whatever `query.Parse` produces, so the operator set is
identical to the upstream package:

| Operator | Meaning | Example |
| --- | --- | --- |
| `eq` (default) | equal | `name=Alice` |
| `ne` | not equal | `name[ne]=Alice` |
| `gt`, `gte`, `lt`, `lte` | numeric / lexicographic comparison | `age[gte]=18` |
| `like`, `ilike` | SQL-style `%`/`_` wildcards (i = case-insensitive) | `name[like]=A%25` (URL-encode `%`) |
| `nlike`, `nilike` | negated `LIKE` / `ILIKE` | |
| `in` (implicit on `,`) | membership | `country=US,DE,FR` |
| `nin` | not in | `status[nin]=banned,deleted` |
| `is`, `not` | IS NULL / IS NOT NULL | `deleted_at[is]=` |
| `kv` | JSONB-style containment | `meta[kv]=eyJhIjoxfQ` |
| `jin`, `njin` | array has any / none | `tags[jin]=admin,editor` |

Logical: `&` for AND, `|` for OR, `()` to group.
Pagination: `_sort=field,-other`, `_limit=N`, `_offset=N`.
Projection: `_fields=id,name` (returned as `Query.Select`).

Dot-paths work for nested values and slice indexing:

```
address.city=Berlin
items.0.name[like]=foo%25
```

---

## Backup & restore

Every `*bw.DB` exposes backup, restore and version methods backed by
Badger's streaming backup format.

```go
// Current database version (monotonically increasing uint64).
ver := db.Version()

// Full backup.
var buf bytes.Buffer
since, _ := db.Backup(&buf, 0, false)

// Incremental backup (only entries newer than `since`).
db.Backup(&buf, since, false)

// Backup with deleted data (preserves delete markers for point-in-time).
db.Backup(&buf, 0, true)

// Point-in-time backup: only entries with version <= savedVersion.
db.BackupUntil(&buf, savedVersion)

// Restore into a (typically fresh) database.
db2.Restore(&buf)
```

| Method | Description |
| --- | --- |
| `Backup(w, since, deletedData)` | Incremental backup; set `deletedData=true` to preserve delete markers |
| `BackupUntil(w, until)` | Point-in-time backup up to a given version |
| `Restore(r)` | Load a backup into the database |
| `Version()` | Current max transaction version |

---

## Cluster mode

The `cluster` sub-package adds multi-node replication on top of bw using
[alan](https://github.com/rakunlabs/alan) for UDP peer discovery and
leader election.

```sh
go get github.com/rakunlabs/bw/cluster
```

### How it works

```mermaid
graph LR
    F1["Follower<br/>(read-only)<br/>local read"] -- "write" --> L["Leader<br/>(read-write)<br/>backup diff"]
    L -- "Push (stream)" --> F2["Follower<br/>(read-only)<br/>local read"]
    L -- "Push (stream)" --> F1
    F1 -- "PullReq" --> L
```

- **Leader election**: alan's distributed lock (`LeaderLoop`). If the
  leader crashes, another node acquires the lock automatically.
- **Writes**: only the leader writes. The application routes writes via
  `IsLeader()` check and its own transport, or uses the built-in
  `Forward()` helper (see below).
- **Reads**: always local. Every node serves reads from its own database.
- **Sync after write**: leader calls `NotifySync(ctx)`, which pushes the
  incremental diff to every behind follower over a QUIC stream and blocks
  until each one has finished restoring it. Stream completion is the
  acknowledgement, so when `NotifySync` returns nil all reachable
  followers are caught up.
- **Periodic sync**: followers pull from the leader every N minutes
  (default 5) as a safety net for missed pushes.
- **Leader catch-up**: a newly elected leader asks all peers for their
  version and pulls the diff from whichever peer is furthest ahead.
- **Stream transfer**: diff data uses alan's `SendToStream` /
  `HandleStream`, so there is no `MaxMessageSize` cap and the receiver
  pipes the body directly into `bw.DB.Restore`.

### Usage

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/rakunlabs/alan"
    "github.com/rakunlabs/bw"
    "github.com/rakunlabs/bw/cluster"
)

type User struct {
    ID   string `bw:"id,pk"`
    Name string `bw:"name,index"`
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 1. Open the database.
    db, err := bw.Open("/var/lib/myapp")
    if err != nil { log.Fatal(err) }
    defer db.Close()

    // 2. Create an alan instance (do NOT call Start yourself).
    a, err := alan.New(alan.Config{
        DNSAddr:  "myapp-headless.default.svc.cluster.local",
        Port:     7946,
        Replicas: 3,
    })
    if err != nil { log.Fatal(err) }

    // 3. Create and start the cluster.
    c := cluster.New(db, a,
        cluster.WithSyncInterval(5*time.Minute),
        cluster.WithLockKey("myapp-leader"),
        cluster.WithOnLeaderChange(func(isLeader bool) {
            log.Println("leader:", isLeader)
        }),
    )
    if err := c.Start(ctx); err != nil { log.Fatal(err) }
    defer c.Stop()

    // 4. Register buckets as usual.
    users, _ := bw.RegisterBucket[User](db, "users")

    // 5. Reads — always local.
    u, _ := users.Get(ctx, "u1")
    _ = u

    // 6. Writes — leader only. NotifySync blocks until followers
    //    have applied the diff (or ctx is cancelled).
    if c.IsLeader() {
        _ = users.Insert(ctx, &User{ID: "u1", Name: "Elif"})
        if err := c.NotifySync(ctx); err != nil {
            log.Printf("notify sync: %v", err)
        }
    }
}
```

### Options

| Option | Default | Description |
| --- | --- | --- |
| `WithLockKey(key)` | `"bw-leader"` | Distributed lock name for leader election |
| `WithSyncInterval(d)` | `5m` | How often followers poll the leader |
| `WithOnLeaderChange(fn)` | `nil` | Callback when leadership changes |
| `WithPrefix(s)` | `"bw"` | Message namespace prefix (see below) |
| `WithForwardHandler(fn)` | `nil` | Handler for forwarded requests on the leader |
| `WithDBName(name)` | `"default"` | Name registered for the primary DB passed to `New` (see multi-DB below) |
| `WithExternalAlan()` | off | Alan's lifecycle is managed by the caller; `Stop` only deregisters handlers |

### Multiple databases under one cluster

A single `Cluster` can manage **more than one** `*bw.DB` under one leader
election. Each database is registered under a unique name (1..255 bytes),
and the wire protocol carries that name in every per-DB message so peers
route the diff into the matching database. Leader election remains
global — whichever node holds the lock is the leader for every
registered DB — but version probing, catch-up, and pushes run in
parallel per DB, so a slow database does not stall the others.

```go
users,  _ := bw.Open("/var/lib/myapp/users")
orders, _ := bw.Open("/var/lib/myapp/orders")
defer users.Close()
defer orders.Close()

// Primary DB is passed to New and named via WithDBName; additional
// DBs are attached with AddDB.
c := cluster.New(users, a,
    cluster.WithLockKey("myapp-leader"),
    cluster.WithDBName("users"),
)
if err := c.AddDB("orders", orders); err != nil {
    log.Fatal(err)
}
if err := c.Start(ctx); err != nil { log.Fatal(err) }
defer c.Stop()

// Sync everything in parallel after a multi-DB write...
_ = c.NotifySync(ctx)

// ...or scope the broadcast to one DB when only that one changed:
_ = c.NotifySyncDB(ctx, "users")

// Status carries per-DB versions.
for name, ver := range c.Status().Versions {
    log.Printf("%s @ v%d", name, ver)
}
```

Helpers:

| Method | Description |
| --- | --- |
| `AddDB(name, db) error` | Register an additional DB. Safe to call before or after `Start`. |
| `DBByName(name) *bw.DB` | Look up a registered DB, or `nil` if the name is unknown. |
| `DBNames() []string` | Sorted snapshot of every registered DB name. |
| `NotifySyncDB(ctx, name) error` | Push the diff for a single named DB; returns `ErrUnknownDB` if not registered. |
| `Status().Versions` | `map[string]uint64` of every DB's local version. |

**Rules of the road:**

- Every node in the cluster MUST register the same set of DB names.
  A peer that doesn't know a name replies with version `0` and gets
  silently skipped by the leader — it will be permanently stale on
  that DB until you fix the configuration.
- DB names must be 1..255 bytes (single-byte length prefix on the wire).
- `Forward` is DB-agnostic: the cluster does not inspect the payload, so
  if your handler can touch multiple DBs encode that routing yourself
  (for example, prefix the payload with a one-byte tag). See
  [`example/cluster/main.go`](example/cluster/main.go) for a working
  two-DB demo.
- If you prefer fully separate leader elections per DB, use multiple
  `Cluster` instances with distinct `WithLockKey` and `WithPrefix`
  instead — share one alan via `WithExternalAlan()`.

### Message prefix

If the same alan instance is shared by multiple subsystems (e.g. your app
uses alan for its own protocol alongside `bw/cluster`), messages can
collide. Every cluster message is prefixed with a namespace string
(default `"bw"`). Messages without the expected prefix are silently
ignored.

```go
// Two independent clusters on the same alan instance — each with its
// own leader election. For one cluster that manages several DBs under
// a single election, see "Multiple databases under one cluster" above.
c1 := cluster.New(db1, a,
    cluster.WithPrefix("users"),
    cluster.WithLockKey("users-leader"),
    cluster.WithExternalAlan(),
)
c2 := cluster.New(db2, a,
    cluster.WithPrefix("orders"),
    cluster.WithLockKey("orders-leader"),
    cluster.WithExternalAlan(),
)
```

### Forwarding writes to the leader

`Forward` sends an application-level request to the current leader over
alan's request-reply and returns the response. If the calling node is
already the leader, the handler runs locally without a network hop.

This eliminates the need for a separate HTTP/gRPC forwarding layer for
simple or moderate-sized writes.

```go
c := cluster.New(db, a,
    cluster.WithForwardHandler(func(ctx context.Context, data []byte) []byte {
        var req CreateUserRequest
        json.Unmarshal(data, &req)

        _ = users.Insert(ctx, &User{ID: req.ID, Name: req.Name})
        _ = c.NotifySync(ctx) // wait for followers to catch up

        resp, _ := json.Marshal(CreateUserResponse{OK: true})
        return resp
    }),
)

// In your HTTP handler (works on any node):
func (s *Server) CreateUser(w http.ResponseWriter, r *http.Request) {
    body, _ := io.ReadAll(r.Body)
    resp, err := s.cluster.Forward(r.Context(), body)
    if err != nil {
        http.Error(w, err.Error(), 502)
        return
    }
    w.Write(resp)
}
```

> **Note:** Forward uses alan's QUIC-based transport, so there is no payload size limit.
