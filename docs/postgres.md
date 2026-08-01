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

**Counter columns** merge by summation rather than by LWW, so concurrent
increments all survive. They require the cell clock group — a whole-row image
would stomp concurrent contributions — and the same `REPLICA IDENTITY FULL`
old image that yields per-column diffs also yields the counter delta (new
minus old). Deletes stay row-level in every clock group.

### The declaration is the capability

```sql
ALTER TABLE public.doc REPLICA IDENTITY FULL;                   -- cell group
ALTER TABLE public.hits ADD COLUMN n public.syzy_counter NOT NULL DEFAULT 0;
```

There is no separate registry to drift out of sync with the database: the
physical capability per-column capture *needs* is the declaration. A table is
in the cell group exactly when `pg_class.relreplident = 'f'`, and a column is
a counter exactly when its type is the `public.syzy_counter` domain (`bigint`
underneath, so it arithmetics and indexes like one). Declaring a counter
column implies the group — the admission gate sets `REPLICA IDENTITY FULL` in
the same transaction — so the two can never be half-configured.

Both facts replicate: `CREATE TABLE` ships its columns' clock groups and a
following clock-group op, and a later `ALTER TABLE … REPLICA IDENTITY` ships
that flip. A node applying the schema gets the same merge semantics without
anyone running DDL on it.

### Admission rules

Rejected before commit (`SQLSTATE 0A000`), because each would make
contributions unmergeable:

| Rejected | Why |
|---|---|
| A nullable counter | `NULL + delta` is `NULL`; there is no identity element |
| A counter in the `PRIMARY KEY` | Summation would move row identity |
| A `GENERATED` / `IDENTITY` counter | Recomputed per node, not accumulated |
| A counter in any unique key, physical or coordinated | Two nodes' sums both land; the constraint has no convergent loser |
| `REPLICA IDENTITY` away from `FULL` on a table with counters | Leaves the group its counters require |
| A composite coordinated key on a cell-group table | Its members arbitrate as a unit, which per-column merge would split |
| `ALTER COLUMN … TYPE` into or out of `syzy_counter` | Existing rows' values are absolute on one side and contributions on the other |

### Costs and guarantees

`REPLICA IDENTITY FULL` writes the whole old tuple to WAL on every `UPDATE`
and `DELETE` — roughly a doubling for a narrow table, far worse for a wide or
TOAST-heavy one. Opt in per table, and prefer narrow tables. Capture spends
that image immediately: an `UPDATE` that changes nothing replicates nothing,
and one that changes one column replicates one column.

Counter apply is **exactly-once**, not idempotent — a re-delivered
contribution would double-count. Each apply transaction that carries a
contribution writes `(origin, seq)` into `public.syzy_applied` *inside that
same transaction*, so the marker is exactly as durable as the sum it
certifies. On redelivery the marker is found and the contributions are
stripped; only the idempotent remainder of the changeset re-applies. Markers
are pruned strictly *behind* the persisted frontier — never up to the seq the
same transaction is certifying, which a retry of a quarantined changeset would
otherwise delete. Summation itself runs in Postgres
(`SET n = target.n + excluded.n`), so it inherits row locking and needs no
read-modify-write in the sidecar; `bigint` overflow surfaces as `22003` and
quarantines the changeset deterministically.

**A contribution is never arbitrated away.** Two places would otherwise drop
one. An `INSERT` that establishes a row's generation but lands on a row this
node's clock does not cover yet — an undrained local commit (capture folds a
transaction well after Postgres committed it) or a row adopted from before
replication started — merges its counter cells additively instead of
overwriting content the rest of the cluster is summing. And when winner-repair
(§9) finds a local write lost, the contribution it carried still ships: the
registers lose, the counter does not.

The admission rules above are enforced by the `ddl_command_end` gate, which a
table listed in `Config.Tables` never passed — it was created before this node
had event triggers. Introspection re-checks them at `Open` and refuses to bind
a table whose counter columns could not merge, rather than replicating absolute
values that silently overwrite concurrent increments.

---

## 9. Conflict observability

Every arbitration that discards a committed write is recorded to
`public.syzy_conflicts` — **in the transaction that discards it**, so the
audit row is exactly as durable as the overwrite and can never describe a
loss that rolled back.

```sql
SELECT at, tbl, pk, kind, loser_side, cols, lost_values
FROM syzy_conflicts ORDER BY seq DESC LIMIT 20;
```

Each row carries the table, primary key, affected columns, the discarded
values, and both writes' origin, stamp, and causal length. `loser_side` says
whose write lost: `local` when an inbound write overrode values this node had
committed, `inbound` when this node's state overrode a peer's. Both nodes in a
conflict record it, from their own side.

`kind` is where the causal length earns its place. A loser from an *older
generation* (`superseded`) was overridden by a delete-and-recreate of the row;
no policy could have preserved it, and it is not evidence of contention. A
loser at the *same* generation from a different origin (`concurrent`) is a
genuine clobber of a write nothing orders. A timestamp-only conflict log
cannot separate those two — every override looks alike.

The log is node-local (never replicated), bounded to the newest 10,000 rows by
the writer, and read by nobody in the engine. Losses to the same origin are
not recorded: an origin overwriting its own earlier value is ordinary
sequential history, not a conflict.

Three paths write to it, which together cover every place a value is dropped:
an inbound record that loses arbitration, an inbound record that wins over
locally committed values, and a local write dropped by winner-repair on the
fold path. The pre-image read that identifies the *specific* overwritten
values is taken only on rows another origin has actually written, so
uncontended apply keeps its single round trip.

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
| `ALTER COLUMN … TYPE` | **Widening only**, and without `USING`: `smallint`→`integer`→`bigint`, `real`→`double precision`, `varchar(n)`→`varchar(m>n)`/`text`, `numeric(p,s)`→`numeric(p'≥p,s)` |
| `ALTER COLUMN … SET/DROP DEFAULT` | Defaults are evaluated only at the origin (apply supplies every column), so they carry no convergence risk |
| `ALTER COLUMN … DROP NOT NULL` | A relaxation |
| `CREATE UNIQUE INDEX` / `UNIQUE` constraint | Total keys only; becomes a §7 unique key |
| `ALTER TABLE … REPLICA IDENTITY FULL` / `DEFAULT` | Joins or leaves the cell clock group; the group replicates with the table (§8) |
| A `public.syzy_counter` column | A counter (§8); implies the cell group, which is set in the same transaction |
| `CREATE INDEX` (non-unique), `DROP INDEX` | Ships as opaque SQL |
| `CREATE VIEW`, `DROP VIEW` | Ships as opaque SQL |
| `bigserial` / `IDENTITY` primary keys | Each node mints from its own slice of the id space (§6) |

### Local to the node — not replicated, and never a halt

Run them on every node yourself. Their *effects* still replicate where that
makes sense: a trigger or function fires on the originator and the rows it
writes are captured as ordinary DML.

`CREATE EXTENSION` · `CREATE FUNCTION` / `PROCEDURE` · `CREATE TRIGGER` ·
`CREATE TYPE` · `CREATE SCHEMA` · standalone `CREATE SEQUENCE` · `CREATE RULE`
· `GRANT` / `REVOKE` · `COMMENT` · `FOREIGN KEY` constraints · `ALTER TABLE …
SET` storage parameters · anything on a `TEMP` or `UNLOGGED` relation ·
**anything outside the `public` schema**.

Declare foreign keys on every node that should enforce them locally. They are
never enforced on applied rows (§12), so they cannot quarantine a peer's write
or diverge the cluster — which is why they are admitted where `CHECK` is not.

### Rejected before commit

The statement fails with `SQLSTATE 0A000` and the transaction rolls back; the
node stays healthy.

| Statement | Why |
|---|---|
| `CREATE TABLE` without a `PRIMARY KEY` | Rows are identified by it; without one no write could be merged |
| `CREATE TABLE … PARTITION BY` / a partition | Not supported in v1 |
| A column of a user-defined type (enum, domain, composite, extension type) | Would replicate as text into a type the receiver may not have; an enum that gains a value on one node only would fail apply there forever. `public.syzy_counter` is exempt — the sidecar creates it on every node (§8) |
| `CREATE TABLE AS`, `SELECT INTO`, `CREATE MATERIALIZED VIEW` | Materializes a node-local query result |
| `ALTER COLUMN … SET NOT NULL` | A `NULL` written on a peer before it applied the change is already in flight. Declare the column `NOT NULL` when it is created |
| `CHECK` and `EXCLUDE` — at `CREATE TABLE` or `ADD CONSTRAINT` | Neither replicates, and both are enforced on applied rows, so the node that declares one quarantines its peers' writes. See below |
| `ALTER COLUMN … TYPE` that narrows | Values in flight under the old type could not be applied |
| Adding/dropping a `GENERATED` expression, changing `IDENTITY` | Recomputed per node |
| Changing `PRIMARY KEY` membership, dropping a PK column | Changes row identity |
| `SET DEFAULT nextval(…)`, `ADD COLUMN` that is serial/identity | Names a node-local sequence / mints divergent values for existing rows |
| `ALTER COLUMN … TYPE` on a serial/`IDENTITY` column | Its type travels as the `CREATE`-shaped `bigserial` / `… GENERATED … AS IDENTITY`, neither spellable in `ALTER COLUMN … TYPE` |
| `ALTER COLUMN … TYPE … USING` | `USING` rewrites this node's rows only; the op carries just the target type, so peers keep their own values. Widen without `USING`, then `UPDATE` as ordinary DML |
| `ENABLE ALWAYS` / `ENABLE REPLICA` on a trigger | It would also fire on applied peer writes, adding local-only mutations no peer sees |
| `SET UNLOGGED` / `SET SCHEMA` on a replicated table | Would take it out of replication scope with no error while the catalog kept serving it |
| `UNIQUE` that is partial (`WHERE`), on an expression, or `NULLS NOT DISTINCT` | Not a plain column tuple, or a predicate whose truth varies per replica |
| `UNIQUE` mixing `NOT NULL` and nullable members | No convergent loser state |
| A counter column that is nullable, in the PK, `GENERATED`, or in a unique key | Contributions could not merge (§8) |
| `REPLICA IDENTITY` away from `FULL` on a table with counters | Counters require the cell group (§8) |
| `ALTER COLUMN … TYPE` into or out of `public.syzy_counter` | Existing values are absolute on one side, contributions on the other |

#### Why `CHECK` and `EXCLUDE` are refused, and `FOREIGN KEY` is not

A `CREATE TABLE` op carries columns and keys and nothing else, and an `ALTER`
that adds a constraint produces no op at all. So a node that declares one is
enforcing a rule its peers do not have, against rows they have already
committed under their own shape. What that costs depends on whether apply
enforces it:

- **`CHECK` and `EXCLUDE` are enforced on applied rows.** A peer's row fails
  with an integrity error on every redelivery. That does not halt the node —
  it quarantines (§4) — which is exactly the problem: the write is dropped on
  this node alone while every other node keeps it, and the cluster silently
  diverges. Hence the refusal.
- **`FOREIGN KEY` is not enforced on applied rows.** It runs on system
  triggers, and apply runs under `session_replication_role = replica`, which
  disables them, so peer rows land unchecked. Nothing quarantines and nothing
  diverges; what the node loses is the constraint being *true* of its own
  table. That is how all logical replication behaves, Postgres's own included,
  so it stays admitted and documented (§12) — a foreign key still constrains
  the local writes the application makes here, which is why people declare
  them.

Declaring a `CHECK` on every node does not rescue it. The write that breaks it
was made on a peer *before* that peer had the constraint, or against a row this
node does not have yet; identical DDL everywhere does not make a cross-node
invariant enforceable from one node's snapshot. Enforce those rules in the
application, where they can be checked against the state the writer has.

The gate only runs under `-ddl`. With an explicit `-tables` set the schema is
yours to manage, and a constraint you keep identical on every node is your
call — the hazards above are unchanged, and quarantined writes are the cost.

Two layers enforce this. A `ddl_command_start` trigger records the
pre-command column shape and constraint set; `ddl_command_end` diffs the
finished catalog against it and raises. The same rules run again in the sidecar after commit, as the
floor for a node that has no snapshot to judge against — one whose DDL support
was installed after the table already existed. That second layer halts
schema-unhealthy, which is the outcome the first layer exists to prevent.

---

## 12. Limitations

- **Foreign keys are not enforced on inbound apply.** `replica` role disables
  their triggers, so a peer's row lands whether or not it satisfies them —
  standard for all logical replication, Postgres's own included. They are
  still enforced against writes made locally.
- **`CHECK` and `EXCLUDE` constraints are not available on replicated
  tables.** Neither replicates, and both *are* enforced on applied rows, so
  under `-ddl` they are refused before commit rather than left to quarantine
  peer writes and diverge the cluster (§11).
- **Row-group tables lose the losing whole-row image** of genuinely
  concurrent same-row writes. Deterministic winner; loss window is
  replication lag; every instance is recorded (§9). Cell groups narrow this
  to per column; counters eliminate it for accumulation.
- **The cell clock group costs WAL.** `REPLICA IDENTITY FULL` logs the whole
  old tuple on every `UPDATE` and `DELETE`. It is per-table opt-in for that
  reason; wide and TOAST-heavy tables should stay in the row group (§8).
- **Counter apply depends on `public.syzy_applied`.** Contributions are
  exactly-once, certified by a marker written in the apply transaction.
  Truncating that table by hand can double-count a redelivery.
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
