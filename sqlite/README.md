# Syzy for SQLite

`github.com/wjordan/syzy/sqlite` provides Syzy's multi-writer SQLite API. Every
replica owns an ordinary local SQLite file, accepts reads and writes without
routing ordinary commits through a primary, and converges with other replicas
through Syzy's changeset protocol.

Application rows remain readable with standard SQLite tooling. Replication
state lives in a `<db>-syzy/` sidecar directory rather than hidden application
columns or log tables. Every writing connection must use the linked API or
load the Syzy extension so its transactions can be captured.

> [!IMPORTANT]
> Syzy is pre-1.0 and experimental. Read the current
> [limitations](docs/LIMITATIONS.md), including uncaptured writes, foreign-key
> behavior, and host-crash durability, before using it with important data.

## Capabilities

- Embedded Go API or loadable SQLite extension plus daemon.
- Row- or cell-level last-writer-wins conflict clocks.
- Additive `INTEGER COUNTER` columns.
- Eventual nullable unique keys and coordinated all-`NOT NULL` unique keys.
- Replicated SQLite DDL with stable table and column identities.
- Incremental `sqlite3_blob_write` replication as byte-range patches.
- Peer and object-store changeset catch-up.
- Litestream-readable LTX physical history, full restore, and optional lazy
  page restore.

The repository-root [documentation](../docs/README.md) specifies the
changeset, convergence, schema-chain, transport, and recovery contracts.

## Try it

The shortest path uses the standard `sqlite3` client. From the repository root:

```console
$ make build ext
$ export PATH="$PWD/bin:$PATH"
$ SYZY_LISTEN=127.0.0.1:7000 \
    sqlite3 -cmd '.load ./ext/syzy' a.db \
    "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, text TEXT NOT NULL);
     INSERT INTO notes VALUES ('a', 'hello');"
```

The [getting-started guide](docs/GETTING_STARTED.md) continues by cloning a
second writable replica, connecting the daemons, and showing the replicated
rows. It also covers primary-key rules and the optional embedded Go API.

## Application API

Go applications open a `Node` and use the `database/sql`-shaped `DB` facade:

```go
node, err := sqlite.Open(ctx, sqlite.Config{
	Path:          "app.db",
	InProcessOnly: true,
})
if err != nil {
	return err
}
defer node.Close()

db := sqlite.NewDB(node)
```

`InProcessOnly` is required for a long-lived linked node when no extension
processes also write the database. Multi-node deployments additionally provide
a transport, schema log, and normally an object backend; see
[Operations](docs/OPERATIONS.md).

Non-Go applications load the extension into every writer connection and run a
`syzy daemon` beside each database. See [`ext/README.md`](../ext/README.md) for
extension packaging and environment configuration.

## Conflict behavior

Rows use last-writer-wins within a causal live/deleted generation by default.
Tables can opt into finer behavior:

- `INTEGER COUNTER` updates replicate as signed adjustments and sum concurrent
  work.
- Cell-clock tables arbitrate ordinary columns independently.
- Incremental blobs arbitrate overlapping byte ranges.
- Nullable unique keys select one owner and clear losing key values.
- All-`NOT NULL` unique keys reserve their values synchronously before commit.

Only the coordinated unique path waits on another cluster member. Other DML
commits locally and replicates asynchronously.

## Physical recovery

With an object backend, an elected publisher maintains coupled LTX histories
for the application and metadata databases. New nodes can restore both files
fully before open or use the optional Linux lazy-restore module to fault
application pages on demand.

The application `db/` stream is readable by Litestream and restores a plain
SQLite file without Syzy metadata. That file must be cloned or joined before it
can participate in the logical cluster again.

## Relationship to adjacent SQLite tools

- [Litestream](https://litestream.io) provides SQLite backup and restore from a
  single writer's WAL. Syzy accepts writes at multiple replicas and also
  publishes a Litestream-readable physical stream.
- [rqlite](https://rqlite.io) presents SQLite through a Raft-replicated database
  service with a leader on the write path. Syzy keeps ordinary reads and writes
  in each application's local file and converges asynchronously.
- [cr-sqlite](https://github.com/vlcn-io/cr-sqlite) is also a CRDT-based SQLite
  extension. Syzy differs in packaging, sidecar state, object-store history,
  restore paths, coordinated uniqueness, counters, and blob ranges.

These are different consistency and deployment choices rather than drop-in
substitutes.

## Documentation

- [Getting started](docs/GETTING_STARTED.md)
- [Operations](docs/OPERATIONS.md)
- [Limitations](docs/LIMITATIONS.md)
- [SQLite architecture](docs/ARCHITECTURE.md)
- [SQLite DDL realization](docs/DDL.md)
- [System documentation](../docs/README.md)
