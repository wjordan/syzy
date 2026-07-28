# Syzy Architecture

Syzy captures committed SQLite transactions as durable CRDT changesets,
distributes them through peers and optional object storage, and applies them
transactionally at every replica.

This document defines the end-to-end lifecycle and its ordering requirements.
SQLite hook details, metadata storage, DDL admission, and physical recovery are
specified in the [SQLite architecture](../sqlite/docs/ARCHITECTURE.md).

The [CRDT model](CRDT.md), [changeset protocol](PROTOCOL.md),
[schema contract](SCHEMA.md), and [transport protocol](TRANSPORT.md) are
spec-authoritative where they define externally consumed formats, interfaces,
or consistency claims.

## System overview

```text
SQLite application
  commit hooks | DDL admission | transactional apply
        |
canonical changesets and stable schema identities
        |
origin history | live transport | anti-entropy | object storage
        |
peer SQLite replicas
```

Each node combines local transaction capture, per-origin history, deterministic
conflict arbitration, schema sequencing, peer delivery, and recovery. Public
provider interfaces allow deployments to choose transport, schema-log, object
storage, and coordination implementations without changing the replication
model.

## Transaction lifecycle

One local SQLite transaction follows this ordering:

1. **Hook observation.** SQLite statement and preupdate hooks collect the
   transaction's replicated row effects without losing its atomic boundary.
2. **Materialization.** Stable catalog identities replace native table and
   column names; SQLite values become canonical value classes.
3. **Identity allocation.** The local origin allocates the next sequence and
   HLC stamp, computes row generations, and builds one changeset.
4. **Commit and durable self history.** SQLite commits, then the exact encoded
   bytes become durable before Syzy releases the source history position or
   reports the entry available for pruning.
5. **Publication.** Live transport broadcasts the retained payload; an
   optional sealer also batches it into immutable object-store epochs.
6. **Inbound admission.** A receiver validates cluster identity, schema
   dependency, per-origin sequence state, and record semantics.
7. **Arbitration and apply.** The broker resolves records against persisted
   CRDT state and applies accepted SQLite DML in one transaction.
8. **Durable frontier advance.** Apply metadata is persisted before the origin
   frontier records the sequence as accepted.

Syzy pipelines these phases, but the ordering obligations are invariant. See
[PROTOCOL.md](PROTOCOL.md) for wire and durability requirements.

## Replica state

Every replica tracks:

- cluster identity and writer origin;
- next local sequence and last HLC value;
- a per-origin applied frontier and out-of-order gaps;
- row generations and arbitration clocks;
- stable schema identities and the applied schema sequence;
- quarantined changesets durably accepted but not yet materialized; and
- self-origin and remote-origin history needed for replay and catch-up.

SQLite persists this state in its companion metadata database. Public meaning
and ordering are contractual; internal Go structs are code-authoritative unless
the SQLite architecture explicitly makes an on-disk field part of recovery.

## Convergence

The base layer is row-level last-writer-wins within a causal row generation. A
delete or resurrection advances the generation; concurrent operations within
one generation arbitrate by `Stamp (HLC, origin)`.

A schema may additionally admit:

- per-column last-writer-wins;
- additive counter contributions;
- unique-key ownership arbitration or coordinated reservation; and
- per-range blob clocks.

Unsupported catalog operations and record forms are rejected explicitly, never
silently reinterpreted. [CRDT.md](CRDT.md) defines the layer semantics.

## Schema ordering

DDL uses a separate globally ordered schema chain rather than the DML stream.
A schema event carries a dense sequence, parent sequence, typed catalog
operation, and native SQL context. Compare-and-swap append gives the chain one
total order.

Each DML changeset records the schema sequence it requires. A receiver catches
its stable catalog up before decoding IDs into SQLite objects. DDL admission,
SQLite rendering, terminal schema-health policy, and recovery are described in
[SCHEMA.md](SCHEMA.md) and [SQLite DDL](../sqlite/docs/DDL.md).

## Distribution and anti-entropy

Live transport is a latency optimization. Correctness does not depend on every
broadcast arriving once or in order.

Per-origin sequences let a receiver distinguish a duplicate, an out-of-order
arrival above a gap, and a missing range that must be fetched. The anti-entropy
planner discovers tips and fills ranges from a chain of sources, normally peers
first and object storage second. Sources may over-deliver; idempotent apply and
frontier checks discard duplicates.

The bundled TCP mux carries multiple logical database topics over one peer
connection and exposes catch-up, frontier, bundle, and coordination operations.
Its authoritative framing is in [TRANSPORT.md](TRANSPORT.md).

## Durability and recovery

The exact changeset history and persisted CRDT/frontier state provide logical
durability. SQLite physical recovery publishes coupled LTX histories for the
application and metadata databases. A clone restores both streams to a valid
point, then replays retained changesets above the restored frontier. Optional
lazy restore changes when pages are fetched, not the recovery authority.

SQLite clone and LTX restore are the bootstrap authority, preserving the
schema, extension state, and database bytes that row export cannot represent.

## Implementation map

See [PACKAGES.md](PACKAGES.md) for internal package ownership, dependency
direction, and public extension contracts.
