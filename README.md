# Syzy

Syzy is a local-first, multi-writer replication system for SQLite and
Postgres. Applications read and write a standard local SQLite file, or an
ordinary Postgres database, without any network round trip. Syzy replicates
committed transactions and resolves concurrent writes so every replica
converges on the same result.

Syzy automatically replicates to durable object storage, so individual replicas or the entire compute layer can scale to zero.

Syzy is a good fit for:

- latency-sensitive services that need the database close to the application;
- multi-region and edge deployments with writable replicas in every location;
- elastic workloads that scale to zero without giving up database durability;
- applications that want database-level conflict handling instead of a custom
  synchronization protocol; and
- nodes that continue working through temporary disconnection and catch up
  later.

Syzy is not a fit when every transaction must be globally ordered before it
commits, or when a write cannot commit until remote replicas accept it.

> [!IMPORTANT]
> Syzy is pre-1.0 and under active development. APIs, supported engine features,
> and recovery behavior may change.

## Features

- **Fast local operations.** Applications execute reads and writes against a
  local SQLite file or Postgres database. A network round trip is only needed
  to coordinate `NOT NULL UNIQUE` constraints.
- **Writable replicas everywhere.** Every replica accepts changes and shares
  them directly with its peers. There is no permanent database primary.
- **Consistent conflict handling.** Changes remain safe when they arrive late,
  more than once, or in different orders. Replicas converge after receiving the
  same changes.
- **Replicated DDL.** Supported `CREATE`, `ALTER`, and `DROP` statements
  replicate with row changes. Schema changes stay ordered, and writes made
  against an older schema remain valid.
- **Live sync.** Peers exchange committed changes directly for fast
  replication.
- **Durable scale-to-zero.** Syzy continuously backs up the database and its
  replication history to object storage. New or returning replicas can restore
  and catch up without relying on a permanent peer.
- **Native database integration.** SQLite applications use an embedded Go API
  or loadable extension; Postgres applications use stock clients beside a
  `syzy-pg` sidecar.

## Install

```console
$ curl -fsSL https://github.com/wjordan/syzy/releases/latest/download/install.sh | sh
```

This installs `syzy`, `syzy-pg`, and the SQLite loadable extension into
`/usr/local`. The SQLite command and extension refuse to talk across versions,
so always install them together. Set `SYZY_PREFIX` to install somewhere else
(`SYZY_PREFIX=$HOME/.local`), or see [CONTRIBUTING.md](CONTRIBUTING.md) to build
from source.

Linux and macOS, amd64 and arm64.

## Try it with SQLite

Use the standard `sqlite3` client to create the database and write the first
row. Loading the extension is the only change to an otherwise ordinary SQLite
session:

```console
$ sqlite3 -cmd '.load syzy' a.db \
    "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, text TEXT NOT NULL);
     INSERT INTO notes VALUES ('a', 'from a');"
```

Clone a second replica and write another row into it:

```console
$ syzy clone a.db b.db
cloned a.db → b.db
$ sqlite3 -cmd '.load syzy' b.db \
    "INSERT INTO notes VALUES ('b', 'from b');"
```

The write committed locally without waiting for anyone. `syzy wait` blocks
until it has reached every peer, which is what makes the next command's output
deterministic:

```console
$ syzy wait b.db
```

The row is now on the first replica, which never had to be told about it:

```console
$ sqlite3 a.db 'SELECT id, text FROM notes ORDER BY id'
a|from a
b|from b
```

Note that the last command does not load the extension: replication is the
daemon's job, so any SQLite client reads a replicated database as a plain file.

Continue with the [getting-started guide](sqlite/docs/GETTING_STARTED.md) for
an explanation of each command, object-storage clusters, and the embedded Go
API.

## Postgres

The same replication model runs on stock Postgres 17+ — no fork, no extension.
A `syzy-pg` sidecar per database captures committed transactions through
logical decoding and applies peers' changes back, so every node is writable
and **there is no failover to run**: no promotion runbook, no Patroni, no etcd.

Concurrent writes resolve by last-writer-wins per row, with two per-table
opt-ins that narrow it — column-level merge for tables with `REPLICA IDENTITY
FULL`, and counter columns whose concurrent increments all accumulate. Every
discarded value lands in a `syzy_conflicts` table.

SQLite clusters and Postgres clusters are separate meshes; there is no
mixed-engine replication. Start with [Syzy for Postgres](pg/README.md).

## Replication model

Syzy records each committed transaction durably, then shares it with other
replicas. A replica can receive missing changes from a peer or recover them
from object storage.

Changes may reach replicas at different times or in different orders. Syzy's
conflict rules select the same result everywhere, while duplicate delivery is
safe. Reads and writes commit without waiting for the network unless the schema
explicitly requires coordination for a constraint such as global uniqueness.

Read [Concepts](docs/CONCEPTS.md) for the short mental model,
[CRDT.md](docs/CRDT.md) for the consistency and conflict rules, and
[Schema replication](docs/SCHEMA.md) for supported schema changes.

## Repository layout

The repository is a Go workspace containing the root replication module and a
separate Postgres module:

- `sqlite`: application API, loadable extension, command-line tools, and
  recovery support.
- `lazyrestore`: optional sparse bootstrap and page-fault recovery for
  object-backed databases.
- `pg`: the Postgres engine and its `syzy-pg` sidecar (a separate Go module).
- Repository root: replication, conflict handling, peer communication, and
  durable storage.

SQLite applications begin with the SQLite package or loadable extension;
Postgres deployments run the sidecar. See
the [package map](docs/PACKAGES.md) for implementation details and extension
points.

## Documentation

- [Documentation map](docs/README.md)
- [Concepts](docs/CONCEPTS.md)
- [System architecture](docs/ARCHITECTURE.md)
- [CRDT model](docs/CRDT.md)
- [Schema replication](docs/SCHEMA.md)
- [Transport](docs/TRANSPORT.md)
- [Package map](docs/PACKAGES.md)
- [SQLite API and operations](sqlite/README.md)
- [Postgres engine](pg/README.md)

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow. Syzy is
licensed under the [Apache License 2.0](LICENSE).
