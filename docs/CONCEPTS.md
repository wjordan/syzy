# Concepts

Syzy turns each committed database transaction into a deterministic changeset,
makes that changeset durable, sends the same bytes to other nodes, and applies
it idempotently. Concurrent changes may arrive in different orders; the merge
rules select the same logical result everywhere.

This page supplies the mental model. The complete lifecycle is in
[ARCHITECTURE.md](ARCHITECTURE.md); formal consistency claims and invariants
are in [CRDT.md](CRDT.md).

## Key terms

- **Replica or node** — one application database plus local Syzy state
  needed to produce, apply, and recover replicated changes.
- **Origin** — the durable identity of one writer. Each origin numbers its
  changesets consecutively so receivers can detect duplicates and gaps.
- **Changeset** — the canonical replicated form of one committed transaction,
  containing identity, timestamp, schema dependency, and typed row records.
- **Stamp** — a hybrid logical timestamp plus an origin tie-breaker. Stamps give
  concurrent writes a deterministic order where last-writer-wins applies.
- **Frontier** — a replica's per-origin record of how far it has durably
  accepted changes. Frontiers drive duplicate detection and catch-up.
- **Schema log** — the ordered authority for replicated DDL. A changeset names
  the schema sequence under which it was produced.

## The replication path

```text
database commit
  -> engine capture
  -> canonical changeset
  -> durable self-origin history
  -> live broadcast and optional object-store sealing
  -> transactional native apply on peers
  -> durable frontier advance
```

SQLite hooks or Postgres logical decoding preserve the originating transaction
boundary. The exact encoded changeset becomes durable
before source history can be released. The public [`crdt`](../crdt) package
owns canonical values and encoding; transports, journals, and object epochs
carry those bytes unchanged.

## Identity, order, and duplicates

A changeset's dot `(origin, seq)` is globally unique. Sequence numbers are
dense within one origin but independent across origins. The HLC captures causal
time and supplies deterministic ordering for concurrent values.

Receivers persist frontiers and gaps. Re-delivery of an accepted dot is a
no-op; out-of-order dots can apply while their missing ranges remain visible to
anti-entropy. Correctness therefore does not require ordered or exactly-once
transport.

## Merge layers

Row-level last-writer-wins is the base. A causal row generation distinguishes
an update from a deletion followed by re-insertion. Within one generation the
higher `(HLC, origin)` stamp wins.

Tables or columns may opt into more specific layers:

- cell-level last-writer-wins;
- additive counters;
- unique-key ownership arbitration or coordinated reservation; and
- blob byte-range clocks and patches.

These layers compose with changeset identity and row generation. Their exact
semantics are specified in [CRDT.md](CRDT.md).

## Coordinated operations

Most commits are local and asynchronous. Coordination is reserved for an
invariant with no convergent loser state. A `NOT NULL UNIQUE` key is the
canonical example: clearing the losing value would violate the schema, so the
current leaseholder reserves the value before the originating commit completes.

Coordination intentionally reduces availability for that operation. Loss of
the coordinator cannot make an unsafe write look committed.

## Delivery, history, and recovery

Live transport is the fast path, not the sole copy of history. A receiver may
miss a broadcast, restart, or remain offline. It compares frontiers, pulls
missing origin/sequence ranges from peers, and can fall back to object-store
epochs when configured.

The SQLite engine uses coupled LTX histories and clone for physical recovery.
The Postgres engine uses native base backups or bucket restore; both replay
above the restored frontier to reach the current logical tip.

## Schema

Replicated DDL does not travel as ordinary row changes. It is serialized
through a compare-and-swap schema log. Catalog operations assign stable table,
column, and key identities, and every changeset records the schema sequence it
requires. Receivers catch the catalog up before applying dependent data.

Each engine translates catalog operations into native DDL and rejects features
it cannot replicate safely. A terminal inability to follow the schema chain is
persisted as schema-unhealthy and requires a fresh clone. See
[SCHEMA.md](SCHEMA.md), [SQLite DDL](../sqlite/docs/DDL.md), and the
[Postgres engine](postgres.md).
