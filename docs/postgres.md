# Postgres engine

The Postgres engine (`pg/`, a separate Go module) replicates a Postgres
database under the same CRDT model the SQLite engine uses — multi-master,
eventually convergent, last-writer-wins per the layered clock in
[CRDT.md](CRDT.md). It builds on the engine-neutral core (`crdt` changesets
and catalog ops, `internal/metadata` durable catalog, `internal/nodestate`
clocks, `schemalog`, `unique`, `transport`), so both engines share one
wire format, one object layout, and one operational model.

**Scope:** SQLite clusters and Postgres clusters are separate meshes. There
is no mixed-engine replication and no cross-engine portability profile; each
engine presents its own native surface. The shared core is what the two have
in common, not a lowest common denominator imposed on either.

**Positioning:** the point is that there is *no failover to run*. Object
storage is the durability and replication layer, any node can die, and there
is no promotion runbook, no Patroni, no etcd. Multi-writer is the mechanism
that removes failover, not the headline.

Authority: on-wire and on-disk formats and convergence invariants are
spec-authoritative here and in [CRDT.md](CRDT.md) / [SCHEMA.md](SCHEMA.md).
Go shapes are described by intent and invariant, not restated line for line.

---

## 1. Why Postgres differs from SQLite

SQLite captures synchronously in a pre-commit `preupdate` hook, so a local
write enters the shared `Cache` before it commits. Postgres has no such hook:
changes are observed only through **logical decoding** (`pglogrepl`, pgoutput
text mode), strictly *after* commit. Two consequences shape the design:

1. **Capture is asynchronous and post-commit.** A local write's CRDT stamp is
   assigned when capture decodes it, not when it commits. The single-writer
   orchestrator (§3) and the capture catch-up gate exist to keep this from
   diverging under concurrency.
2. **DDL leaves no logical-decoding trace.** Postgres does not stream DDL
   through the WAL the way it streams row changes. The engine reconstructs
   schema changes from an event-trigger spool (§6).

A third difference — no pre-commit veto point for coordinated uniqueness —
is answered in §7.

---

## 2. Capture (decode-only)

One replication slot in pgoutput **text** mode, decode-only: capture never
folds into the Cache. For each streamed transaction it builds the
per-transaction net effect, collapsing multiple writes to one row, turning a
PK change into delete+insert, and merging TOAST-unchanged columns into the
latest image. On `Commit` it hands the draft to the orchestrator and only
then advances its processed LSN.

Values carry the canonical typed form: pgoutput text is parsed per column
class into `ColInt`/`ColReal`/`ColBlob` canonical bytes; everything else
stays canonical text under `ColText`. Apply renders typed values back to cast
text literals.

PK identity is the canonical typed encoding shared with the core, so the same
logical row has the same bytes on every node.

Loopback is filtered by replication origin: applied writes (§4) run under a
syzy origin that the slot's `origin='none'` filter drops, so a node never
re-captures its own applied changes.

---

## 3. The orchestrator (single serialized writer)

The orchestrator is the **sole** mutator of the Cache. `Engine.Run` starts
capture as a child (decode, enqueue drafts) and runs one goroutine that folds
local drafts, applies inbound peer changesets, and advances checkpoints. A
capture catch-up gate drains pending local commits to the WAL target before
remote arbitration, so a local write that has committed in Postgres but not
yet been decoded cannot lose an arbitration it should have won.

---

## 4. Apply

A decoded peer changeset is written in one Postgres transaction under
`session_replication_role = replica` and the syzy origin. Per record:

1. **Idempotency** — skip if the changeset's `Dot` is already in the applied
   frontier.
2. **Schema gate** — refuse if the changeset's schema dependency exceeds the
   local schema head (the orchestrator catches the schema log up first; this
   is the backstop).
3. **Row LWW** — apply iff the record's `(CL, Stamp)` dominates the row's
   current state. Tables default to the **row clock group**; per-column
   arbitration is opt-in (§8).
4. **Unique arbitration** (§7), then the DML: an upsert for an image, a
   delete for a tombstone.

Cache mutations are applied **after** the transaction commits, so a
rolled-back apply leaves no clock the SQL did not also roll back.

Foreign keys are **not** enforced on inbound apply — `replica` role disables
triggers, which is how all logical replication behaves, including Postgres's
own. See [LIMITATIONS](#12-limitations).

**Failure handling:** a deterministic integrity-constraint failure (SQLSTATE
class 23 — retrying the same bytes yields the same error) is quarantined to
the metadata store and the frontier advanced past it, capped per origin. The
Run loop re-applies quarantined entries on a sweep and clears the ones that
now land. Every other apply failure — connection loss, schema-behind, disk —
is transient and fatal to Run: restart plus redelivery is the recovery path.

`crdt.BlobPatch` records are rejected deterministically: incremental blob
patching is a SQLite-engine capability with no Postgres counterpart in v1
(bytea columns replicate as whole values). The rejection quarantines rather
than silently skipping, so divergence is impossible.

---

## 5. Durability — self-origin log and async publisher

The **exact shipped changeset bytes** are the durability boundary. A local
fold appends the changeset (and the originating WAL LSN) to a journal and
fsyncs it *before* the slot advances `confirmed_flush`. Recovery replays the
exact bytes — it never re-derives a changeset, so a stamp cannot drift across
restart.

A separate async publisher tails the log and broadcasts, tracking a
`delivered` cursor and retrying transient transport errors. Checkpoint
retention is bounded by `delivered`, not the log head, so an undelivered
entry is never collected even after `confirmed_flush` passes it; on restart
the retained tail re-ships and peers dedupe by `Dot`.

Checkpoints persist a Cache snapshot plus the capture LSN in one metadata
transaction, then ack the slot — the snapshot covers exactly the commits at
or below the acked LSN.

**Object storage and journal GC.** With `-bucket`, the publisher feeds each
shipped changeset to the sealer, which uploads this origin's epochs under the
same object layout the SQLite node uses. That makes the cluster's data
object-storage-backed, lets an offline or brand-new peer converge from the
bucket, and gives journal GC its safety predicate: retention follows the
**sealed** tip, never mere delivery. Without a bucket both journals are
retained unboundedly, because truncating would strand an offline peer —
so a bucket is effectively required in production.

**Peer catch-up and anti-entropy.** The engine serves any (origin, seq) it
produced or applied — own bytes from the self-log, peer bytes from the mirror
— over the mesh catch-up endpoint. The consuming side plans missing ranges
from the Cache frontier plus applied gaps, pulls them from a peer or the
bucket, and routes each fetched changeset through the orchestrator so apply
stays single-writer. Live broadcast is best-effort; anti-entropy is what
makes a missed delivery a latency blip instead of a permanent gap.

---

## 6. DDL replication

DDL has no logical-decoding trace, so the engine reconstructs it from an
event-trigger spool. A `ddl_command_end` event trigger writes a **structured
descriptor** — read from `pg_catalog`, never parsed from SQL text — into a
spool table in the same transaction as the DDL itself. Capture decodes those
spool rows, and the orchestrator turns them into typed `crdt.CatalogOp`s
appended to the ordered schema log under a DDL lease and a parent-CAS.
Receivers apply the typed op, never the originator's SQL.

Unsupported DDL is rejected **before commit** — a migration typo fails the
migration instead of halting the node — and DDL that is simply out of scope
(extensions, functions, triggers, anything outside `public`) is left local to
the node rather than treated as an error. The allow/reject matrix is in §11.

Schema health is fail-closed: a node that cannot interpret a schema event
stops rather than serving a schema it cannot prove.

---

## 7. Unique keys

Two classes, exactly as in the SQLite engine:

**Eventual (nullable) unique keys** converge by loser-null arbitration on the
apply path: the losing row's key columns are set to NULL. A local insert
succeeding is not a global guarantee.

**Coordinated (`NOT NULL UNIQUE`) keys** are CP: uniqueness is guaranteed by
construction through reserve-before-commit against the cluster's key
registry. Enforcement is **gate-only** — no node holds a physical UNIQUE
index for a coordinated key, because a receiver-side index would fail the
apply transaction before arbitration could run. The taken-set is derived, not
stored redundantly.

Postgres has exactly one pre-commit veto point available to a sidecar without
a server extension: a `DEFERRABLE INITIALLY DEFERRED` constraint trigger,
whose `ERROR` aborts the commit. The engine uses it: `BEFORE` row triggers
accumulate the transaction's net key values in an unlogged scratch table, and
one deferred constraint trigger makes a single batched reservation call at
commit. The call reaches the sidecar over `dblink` — the trigger's only way
out of its own transaction — so the sidecar answers as a Postgres server for
exactly one verb. A denial raises `23505` and aborts, which is precisely what
a `UNIQUE` index would have done.

The batch carries the transaction's **net** effect. A value inserted and then
deleted in one transaction is never reserved; a value moved between rows
reserves once, naming the prior owner so the registry transfers it rather than
reporting a conflict; an update that does not touch the key does not call out
at all.

Key values cross the wire as text and are re-encoded into canonical key bytes
in the sidecar's Go, sharing the encoder with capture. Canonical byte equality
*is* row identity, so a second implementation in PL/pgSQL could split one
logical value into two. The trigger renders text under pinned per-function
GUCs (`DateStyle`, `TimeZone`, `extra_float_digits`, `bytea_output`) so a
writer's session settings cannot change the bytes.

The taken-set is **derived, never a ledger**: the registry leaseholder
enumerates the rows that actually hold each key. That is what makes a
reservation survive a leaseholder restart, and it means a released value
re-enters the free pool only after an enumeration observes the row gone and
the release hold expires.

Consequences, stated plainly: a coordinated write's commit includes one mesh
round trip while holding locks (inherent to CP uniqueness); a crash between
reservation and commit leaks a hold bounded by the quarantine window, never a
duplicate; if the sidecar is unreachable, coordinated commits abort with
`40001` — fail-closed and retryable, scoped to coordinated tables only; and
**the engine drops the physical unique index** backing a coordinated key,
including the one the originator's own `CREATE TABLE` built (see
[LIMITATIONS](#12-limitations)).

---

## 8. Merge semantics: clock groups and counters

Tables default to the **row clock group**: a record carries a whole-row image
and arbitration is per row. Two rows written concurrently converge; two
*columns* of one row written concurrently do not — the losing whole-row image
is dropped. That loss is deterministic, bounded by replication lag, and
recorded (§9).

Tables can opt into the **cell clock group**, where arbitration is per column
so concurrent writes to disjoint columns of one row both survive. Postgres
derives per-column changes from `REPLICA IDENTITY FULL`: the old and new
images are diffed at capture. The cost is WAL amplification — roughly a full
old row per update — so it is per-table opt-in, and wide or TOAST-heavy
tables should stay in the row group.

**Counter columns** (`INTEGER COUNTER NOT NULL`) merge by summation rather
than by LWW, so concurrent increments all survive. They require the cell
clock group — a whole-row image would stomp concurrent contributions — and
the same `REPLICA IDENTITY FULL` old image that yields per-column diffs also
yields the counter delta (new minus old). Deletes stay row-level in every
clock group.

---

## 9. Conflict observability

Every arbitration that overrides a committed write is recorded to a bounded
audit table: table, primary key, columns, winner and loser origin and stamp,
and the losing values. Because the model carries causal length, the log can
state *precisely* whether two writes were genuinely concurrent (equal causal
length, different origins) or one was simply stale — a distinction
timestamp-only conflict logs cannot make.

---

## 10. Bootstrap and adoption

Joining a node is deliberately boring: take a physical base backup from a
peer (or restore from the bucket), create the slot, initialize the frontier.

Adopting an **existing** database publishes a consistent initial snapshot
coordinated with the slot's starting LSN, so no write is missed or
double-counted between snapshot and stream.

**Slot and WAL retention** is the operational hazard to understand: a dead
sidecar pins its replication slot and the WAL accumulates. Size
`max_slot_wal_keep_size` so Postgres invalidates the slot rather than filling
the disk; an invalidated slot puts the node into the fail-closed path and it
re-clones, which is strictly better than the database going down.

---

## 11. DDL allow/reject matrix

Decided, not discovered. Every DDL statement lands in exactly one of three
buckets, and the bucket is chosen **before the statement commits**.

**The rule behind the table:** a replicated schema change may only *relax* the
schema. Rows written on a peer before it applied the change are already in
flight, carrying values that were legal under the old shape, and a changeset's
schema dependency only orders writes made *after* the change. So a receiver
that had tightened the column could never apply them, and would halt forever.

### Replicated

| Statement | Notes |
|---|---|
| `CREATE TABLE` | Permanent, in `public`, with a `PRIMARY KEY` |
| `DROP TABLE`, `ALTER TABLE … RENAME` | |
| `ADD COLUMN` / `DROP COLUMN` / `RENAME COLUMN` | |
| `ALTER COLUMN … TYPE` | **Widening only**: `smallint`→`integer`→`bigint`, `real`→`double precision`, `varchar(n)`→`varchar(m>n)`/`text`, `numeric(p,s)`→`numeric(p'≥p,s)` |
| `ALTER COLUMN … SET/DROP DEFAULT` | Defaults are evaluated only at the origin (apply supplies every column), so they carry no convergence risk |
| `ALTER COLUMN … DROP NOT NULL` | A relaxation |
| `CREATE UNIQUE INDEX` / `UNIQUE` constraint | Total keys only; becomes a §7 unique key |
| `CREATE INDEX` (non-unique), `DROP INDEX` | Ships as opaque SQL |
| `CREATE VIEW`, `DROP VIEW` | Ships as opaque SQL |
| `bigserial` / `IDENTITY` primary keys | Each node mints from its own slice of the id space (§6) |

### Local to the node — not replicated, and never a halt

Run them on every node yourself. Their *effects* still replicate where that
makes sense: a trigger or function fires on the originator and the rows it
writes are captured as ordinary DML.

`CREATE EXTENSION` · `CREATE FUNCTION` / `PROCEDURE` · `CREATE TRIGGER` ·
`CREATE TYPE` · `CREATE SCHEMA` · standalone `CREATE SEQUENCE` · `CREATE RULE`
· `GRANT` / `REVOKE` · `COMMENT` · `ALTER TABLE … ADD CHECK` / foreign keys /
storage parameters · anything on a `TEMP` or `UNLOGGED` relation · **anything
outside the `public` schema**.

### Rejected before commit

The statement fails with `SQLSTATE 0A000` and the transaction rolls back; the
node stays healthy.

| Statement | Why |
|---|---|
| `CREATE TABLE` without a `PRIMARY KEY` | Rows are identified by it; without one no write could be merged |
| `CREATE TABLE … PARTITION BY` / a partition | Not supported in v1 |
| A column of a user-defined type (enum, domain, composite, extension type) | Would replicate as text into a type the receiver may not have; an enum that gains a value on one node only would fail apply there forever |
| `CREATE TABLE AS`, `SELECT INTO`, `CREATE MATERIALIZED VIEW` | Materializes a node-local query result |
| `ALTER COLUMN … SET NOT NULL` | A `NULL` written on a peer before it applied the change is already in flight. Declare the column `NOT NULL` when it is created |
| `ALTER COLUMN … TYPE` that narrows | Values in flight under the old type could not be applied |
| Adding/dropping a `GENERATED` expression, changing `IDENTITY` | Recomputed per node |
| Changing `PRIMARY KEY` membership, dropping a PK column | Changes row identity |
| `SET DEFAULT nextval(…)`, `ADD COLUMN` that is serial/identity | Names a node-local sequence / mints divergent values for existing rows |
| `UNIQUE` that is partial (`WHERE`), on an expression, or `NULLS NOT DISTINCT` | Not a plain column tuple, or a predicate whose truth varies per replica |
| `UNIQUE` mixing `NOT NULL` and nullable members | No convergent loser state |

Two layers enforce this. A `ddl_command_start` trigger records the
pre-command column shape; `ddl_command_end` diffs the finished catalog against
it and raises. The same rules run again in the sidecar after commit, as the
floor for a node that has no snapshot to judge against — one whose DDL support
was installed after the table already existed. That second layer halts
schema-unhealthy, which is the outcome the first layer exists to prevent.

---

## 12. Limitations

- **Foreign keys are not enforced on inbound apply.** Standard for all
  logical replication, Postgres's own included.
- **Row-group tables lose the losing whole-row image** of genuinely
  concurrent same-row writes. Deterministic winner; loss window is
  replication lag; every instance is recorded (§9). Cell groups narrow this
  to per column; counters eliminate it for accumulation.
- **Eventual unique keys converge by loser-null.** A local insert's success
  is not a global guarantee.
- **A coordinated key's physical UNIQUE index is dropped**, on every node
  including the one whose `CREATE TABLE` declared it. This is not an
  optimization: every node is a receiver, and a local index would fail an
  apply transaction with `23505` before arbitration could run — turning a
  legitimate transfer, or a value whose release this node has not yet seen,
  into permanent divergence. Enforcement moves entirely to the pre-commit
  reservation. A direct `psql` write with the sidecar down therefore has no
  index to stop it, which is the same posture as any other write that
  bypasses the gate.
- **Only the `public` schema replicates.** Tables elsewhere are node-local;
  their DDL is skipped rather than rejected (§11).
- **`SET NOT NULL` and narrowing type changes are refused** (§11). A schema
  change may only relax the schema, because writes made before a peer applied
  it are already in flight under the old shape.
- **Incremental blob patching is not supported.** Use `bytea` and replicate
  whole values.
- **Postgres 17+**, one sidecar per database, and a superuser-adjacent
  install (event triggers, replica role, replication slot).
- **A bucket is effectively required** in production: without one, journals
  grow unboundedly (§5).
