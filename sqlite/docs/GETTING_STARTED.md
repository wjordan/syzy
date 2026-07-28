# SQLite getting started

The quickest way to try Syzy is through the standard `sqlite3` client. This
example creates two writable SQLite files, writes through both, and watches the
first replica receive the second replica's row. No application code is needed.

Syzy is pre-1.0 and experimental. Read [Limitations](LIMITATIONS.md) before
using it outside an evaluation environment.

## Prerequisites

You need the `sqlite3` CLI, plus the `syzy` command and its loadable extension:

```console
$ curl -fsSL https://github.com/wjordan/syzy/releases/latest/download/install.sh | sh
```

To build them from source instead, see [CONTRIBUTING.md](../../CONTRIBUTING.md)
and use `.load ./ext/syzy` in place of `.load syzy` below.

The host SQLite library must be built with `SQLITE_ENABLE_PREUPDATE_HOOK`; most
Debian, Ubuntu, and Homebrew packages include it. Loading the extension reports
a clear error when the hook is unavailable.

## Run two replicas

Create the first database and write one row. Loading `ext/syzy` attaches capture
hooks to this SQLite connection and starts the local replication daemon:

```console
$ SYZY_LISTEN=127.0.0.1:7000 \
    sqlite3 -cmd '.load syzy' a.db \
    "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, text TEXT NOT NULL);
     INSERT INTO notes VALUES ('a', 'from a');"
```

Clone that replica, then write through the second database. The seed address
connects its daemon directly to the first:

```console
$ syzy clone a.db b.db
cloned a.db → b.db
$ SYZY_LISTEN=127.0.0.1:7002 SYZY_SEEDS=127.0.0.1:7000 \
    sqlite3 -cmd '.load syzy' b.db \
    "INSERT INTO notes VALUES ('b', 'from b');"
```

That write committed locally without waiting for the network. `syzy wait`
blocks until every peer has it — name the database that was *written*, not the
one you are about to read:

```console
$ syzy wait b.db
$ sqlite3 a.db 'SELECT id, text FROM notes ORDER BY id'
a|from a
b|from b
```

`syzy wait` is a convenience for scripts and demos where a read has to observe
a specific earlier write. Applications do not call it: replication runs on its
own and a replica serves reads at whatever point it has converged to.

Each replica is an ordinary SQLite file with its own `<db>-syzy/` replication
state. The extension must be loaded into every connection that writes to a
replicated database; reads can use standard SQLite tooling without it. The
daemon runs separately and continues exchanging committed changes after the
client exits.

This local example uses fixed loopback ports and a file-backed cluster rooted
in `a.db-syzy/`. Real deployments use stable listen addresses and normally a
shared object-store cluster URL; see [Operations](OPERATIONS.md).

## Schema essentials

Every replicated table needs an explicitly non-NULL primary key. Text and UUID
keys work as usual. For generated integer keys, use:

```sql
id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('notes'))
```

`gen_id` uses the local random-partition allocator and produces sparse positive
int63 values; use
`INSERT ... RETURNING id` rather than `last_insert_rowid()`.

The linked Go API and `LD_PRELOAD` shim can rewrite a simple `INTEGER PRIMARY
KEY` to this safe form. Direct `.load` clients must declare a safe key
themselves, as the example does. See [SQL preprocessing](DDL.md#sql-preprocessing)
for the rule and [`ext/README.md`](../../ext/README.md) for preload setup.

Two optional declarations select specialized conflict behavior:

```sql
quantity INTEGER COUNTER NOT NULL DEFAULT 0
email    TEXT NOT NULL UNIQUE
```

Counter updates merge as relative adjustments. An all-`NOT NULL` unique key is
reserved synchronously before commit and requires shared reservation storage in
a multi-writer cluster. Nullable unique keys instead converge without
coordination by clearing the losing key. See [Limitations](LIMITATIONS.md) for
the concise rules.

## Embed it in Go

Go applications can run Syzy in-process instead of loading the
extension and running a daemon. The maintained example creates the same
two-replica topology:

```console
$ cd sqlite
$ go run ./examples/getting-started
a.db: from a, from b
b.db: from a, from b
```

Read the [example source](../examples/getting-started/main.go) when you are ready
to configure `sqlite.Open`, transport, and a schema log. For multi-host wiring,
continue to [Operations](OPERATIONS.md).

## Next steps

- [Operations](OPERATIONS.md) — configure linked nodes or extension daemons
  across hosts, with shared schema and durable history.
- [Concepts](../../docs/CONCEPTS.md) — understand changesets, convergence, and
  catch-up.
- [Limitations](LIMITATIONS.md) — review current SQL and durability boundaries.
- [Package map](../../docs/PACKAGES.md) — find the application and provider APIs.
