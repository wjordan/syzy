# Syzy for Postgres

Multi-writer replication for Postgres 17+ on stock servers — no fork, no
extension, no patched binary. Every node accepts writes, changes replicate
directly between peers and through object storage, and concurrent writes
converge by the same rules the SQLite engine uses.

**There is no failover to run.** No promotion runbook, no Patroni, no etcd, no
witness node. A node that dies is a node that stopped; the others were already
writable and keep going. Multi-writer is how failover is removed, not the
headline on its own.

`pg/` is a separate Go module. It builds on the engine-neutral core (changeset
format, durable catalog, clocks, schema log), so both engines share one wire
format and one operational model — but SQLite clusters and Postgres clusters
are separate meshes. There is no mixed-engine replication.

> [!IMPORTANT]
> The Postgres engine is newer than the SQLite one and has had less production
> exposure. Read [Limitations](../docs/postgres.md#12-limitations) before
> committing to it — particularly conflict semantics, the DDL subset, and the
> unique-constraint model.

## What you need

- **Postgres 17 or newer**, with `wal_level = logical`.
- `max_replication_slots` and `max_wal_senders` sized for the cluster — one
  slot per sidecar. Postgres's default of 10 is too low for a real fleet; 64 is
  a sane floor.
- A **superuser-adjacent role**: the sidecar installs event triggers, sets the
  replica role, and creates a replication slot.
- **Object storage** (`-bucket`). Formally optional, effectively required: it
  is how an offline peer catches up and how local journals stay bounded.
  Without it they grow without limit.

One sidecar per database, one database per node.

## Try it

Two nodes, each a Postgres database with a `syzy-pg` sidecar beside it. Create
the table on both first — without `-ddl`, the replicated set has to exist
before the sidecar starts:

```console
$ psql app_a -c "CREATE TABLE notes (id bigint PRIMARY KEY, body text)"
$ psql app_b -c "CREATE TABLE notes (id bigint PRIMARY KEY, body text)"
```

Then give every node a cluster id and a permanent origin number:

```console
$ syzy-pg -genid
6f1c2b9a4d8e470fa3c5b81d6e2f9074
```

Start the first node. `-origin` must be unique per node and is never reused —
it also slices the `bigint` id space, so `bigserial` primary keys minted on
different nodes cannot collide:

```console
$ syzy-pg -conn postgres://localhost:5432/app_a -origin 1 \
    -cluster-id 6f1c2b9a4d8e470fa3c5b81d6e2f9074 \
    -data-dir /var/lib/syzy/a -listen 127.0.0.1:7000 \
    -bucket s3://my-bucket/app -tables public.notes
```

Start the second node pointing at the first:

```console
$ syzy-pg -conn postgres://localhost:5433/app_b -origin 2 \
    -cluster-id 6f1c2b9a4d8e470fa3c5b81d6e2f9074 \
    -data-dir /var/lib/syzy/b -listen 127.0.0.1:7001 -seeds 127.0.0.1:7000 \
    -bucket s3://my-bucket/app -tables public.notes
```

These listen on loopback. A listener reachable from anywhere else needs
`-tls-cert` / `-tls-key` / `-tls-ca`, or an explicit `-insecure` to acknowledge
plaintext — the sidecar refuses to start otherwise rather than quietly
exposing the mesh.

Now write to either one with any Postgres client. The sidecar is not in the
commit path — it reads the WAL after the fact — unless a table declares a
coordinated unique key, which is the one case a commit waits on it:

```console
$ psql app_a -c "INSERT INTO notes VALUES (1, 'from a')"
$ psql app_b -c "INSERT INTO notes VALUES (2, 'from b')"
$ psql app_a -c "SELECT * FROM notes ORDER BY id"
 id |  body
----+--------
  1 | from a
  2 | from b
(2 rows)
```

Both writes committed locally without waiting for the other node.

### Joining a database that already has data

`-adopt` publishes the existing rows once, so a database with history can join
a cluster. It is idempotent and safe to leave set:

```console
$ syzy-pg -conn postgres://localhost:5433/app_b -origin 2 ... -adopt
```

It is deliberately an explicit flag. A node that adopted when it should have
cloned would republish a stale database into a live cluster.

## What replicates

`-tables` names the replicated set explicitly. `-ddl` instead replicates
`CREATE` / `ALTER` / `DROP` themselves, with tables created by cluster DDL —
see the [allow/reject matrix](../docs/postgres.md#11-ddl-allowreject-matrix)
for exactly which statements are admitted, and why a schema change may only
ever relax the schema.

Only the `public` schema replicates.

**Large values: use `bytea`, not large objects.** Large objects live in
`pg_largeobject`, a system table outside the replicated set, and their
modifications do not decode as row changes — a replicated row would carry an
OID pointing at data no peer has. `bytea` values replicate as ordinary column
values. They replicate whole: there is no incremental byte-range patching on
this engine, so a large `bytea` rewritten often will ship in full each time.

## Conflict behavior

Concurrent writes to the same row resolve by last-writer-wins on a hybrid
logical clock, deterministically on every node. By default the unit is the
whole row, so the losing row image is discarded.

Two opt-ins narrow that, per table:

- `REPLICA IDENTITY FULL` puts a table in the **cell clock group**: concurrent
  writes to *different columns* of one row merge instead of one losing.
- A column typed `public.syzy_counter` is a **counter**: concurrent increments
  all accumulate rather than one winning.

Every discarded value is recorded in `public.syzy_conflicts`, in the same
transaction that discarded it:

```sql
SELECT at, tbl, pk, kind, loser_side, cols, lost_values
FROM syzy_conflicts ORDER BY seq DESC LIMIT 20;
```

See [merge semantics](../docs/postgres.md#8-merge-semantics-clock-groups-and-counters)
and [conflict observability](../docs/postgres.md#9-conflict-observability).

## Operating it

- **The replication slot is the durable resume position.** If it is dropped or
  invalidated, the node cannot resume and must be re-cloned from a peer — the
  sidecar refuses to start rather than skipping the commits it held. Set
  `max_slot_wal_keep_size` so a dead sidecar invalidates its slot instead of
  filling the disk.
- **Clock skew** is bounded by `-max-clock-skew` (default 30s): a peer's clock
  cannot drag this node's forward past it. Writes are never refused over it.
- **Throughput**: a node absorbs peer traffic at roughly 18k rows/sec in the
  project's test environment — size against that, not the much higher capture
  rate, and measure on your own hardware. Split large
  backfills; a transaction is buffered whole. See
  [performance](../docs/postgres.md#13-performance).

## Documentation

- [Postgres engine](../docs/postgres.md) — the full model: capture, apply,
  durability, DDL, unique keys, merge semantics, limitations.
- [CRDT model](../docs/CRDT.md) — the shared consistency rules.
- [Changeset protocol](../docs/PROTOCOL.md) — the shared wire format.
