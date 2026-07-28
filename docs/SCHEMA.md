# Schema Replication

This document defines Syzy's schema chain, stable catalog identity, and the
schema dependency carried by changesets. DDL capture, admission, rendering,
and recovery are detailed in [SQLite DDL](../sqlite/docs/DDL.md).

Catalog-operation meaning and schema-chain ordering are
**spec-authoritative**. Internal metadata tables and Go shapes are
code-authoritative unless the SQLite specification explicitly makes an on-disk
format part of recovery.

## Why DDL has its own chain

DML changesets address tables and columns by stable IDs. A receiver must know
what those IDs mean before applying a dependent transaction. Sending SQL
through ordinary per-origin DML streams would permit incompatible schema
orders and ambiguous renames.

Syzy therefore serializes DDL through one schema chain per logical database.
The chain establishes a total order while DML remains per-origin and
asynchronous.

## Schema log

A schema event contains:

```text
schema_seq   dense sequence allocated by the schema authority
parent_seq   head observed before compare-and-swap append
catalog_op   typed SQLite catalog mutation
raw_sql      native statement context for diagnostics or replay
```

`SchemaLog.Append(parentSeq, catalogOp, rawSQL)` succeeds only when
`parentSeq` is the current head. Competing DDL writers race through that
compare-and-swap; a loser refreshes the catalog before retrying admission.

The public [`schemalog.Log`](../schemalog/log.go) interface is the extension
contract. Built-in implementations provide in-process, shared-file, TCP-hosted,
and S3-backed authorities. Provider behavior and failure ordering are part of
the schema-chain contract; a particular provider's storage layout is not.

## Stable catalog identity

Replicated objects are addressed independently of SQLite names:

- `table_id` — 16-byte stable table identity;
- `column_id` — 16-byte stable column identity scoped by its table;
- `key_id` — stable primary or unique-key identity; and
- schema sequence — the point at which an identity becomes active or
  tombstoned.

Renaming an object changes its SQLite name while preserving its stable ID. A
changeset created before the rename still resolves to the same logical object
after catch-up. Dropping an object tombstones its identity; IDs are never
recycled while retained history may still address them.

The public [`catalog`](../catalog) package owns ID allocation, canonical key
tuples, and catalog helpers. Operations are encoded through
[`crdt.CatalogOp`](../crdt/catalog_op.go).

Catalog-operation bytes are durable schema-log records, so the native SQLite
operation set retains its established key layouts. The high bits of the kind
byte select the coordinated-key and partial-key extensions; operations that do
not need those extensions keep the original layout. Decoders accept all three
layouts so an existing schema log remains replayable after a binary upgrade.

## Catalog operations

The replicated catalog operation vocabulary covers:

- create, rename, and drop table;
- add, rename, and drop column;
- create and drop primary/unique key metadata;
- native create/drop index, view, virtual table, and trigger operations;
- bundles used when one admitted SQLite statement has several catalog effects;
  and
- clock-group selection for replicated conflict behavior.

Structural operations carry stable identities and the SQLite attributes needed
to decode later DML. Auxiliary objects retain admitted SQLite SQL where their
semantics do not affect record decoding. The operation deliberately models only
the catalog state needed for SQLite replication; it is not a universal SQL AST.

## Changeset dependency

The changeset header may carry `(SchemaChain, required_schema_seq)`. Before
applying its records, a receiver must bring the local catalog to at least that
sequence. A schema-behind changeset waits; it must not be decoded against an
older catalog.

DDL itself does not appear as a DML record. The SQLite broker fetches schema
events, applies native structure before metadata, records the event, reloads the
runtime catalog, and advances its local schema sequence.

## Runtime obligations

Syzy must:

1. classify native DDL before admitting dependent DML;
2. build a deterministic catalog operation from the stable catalog;
3. append against the schema head with compare-and-swap semantics;
4. ensure a rejected append cannot silently commit divergent native DDL;
5. apply each event idempotently in dense schema-sequence order;
6. map stable IDs to SQLite objects for DML capture and apply;
7. classify temporary lock, cancellation, I/O, and resource failures as
   retryable without advancing schema state; and
8. durably mark the node schema-unhealthy when it misses the retention horizon,
   receives an invalid chain/operation, or deterministically cannot realize a
   structural event.

The terminal marker is fail-closed and requires a fresh clone. There is no
skip-event or force-advance operation. [SQLite DDL](../sqlite/docs/DDL.md)
specifies hook ordering and the durable marker encoding.

## Unique keys

Unique keys are catalog identities; an eventual key is additionally backed by
a SQLite index, while a coordinated key is catalog metadata alone on every
node. Syzy admits two logical modes:

- **Eventual ownership.** A key with a representable loser state allows
  concurrent claims. Deterministic arbitration selects one row and clears the
  losing value according to the admitted clock layer.
- **Coordinated ownership.** A key with no valid loser state, notably
  `NOT NULL UNIQUE`, requires a serialized reservation before the originating
  commit becomes visible.

The coordinator leaseholder serves canonical per-value reservations and the
SQLite commit hook vetoes conflicts or unavailable coordination. Handoff waits
through the configured release quarantine before a new holder admits claims.
This availability tradeoff is intentional: an operation with no valid loser
state must fail closed when coordination is unavailable.

SQLite collation, partial-index, blob, and DDL details are specified in the
[DDL guide](../sqlite/docs/DDL.md) rather than duplicated here.
