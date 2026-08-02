# SQLite DDL and Schema Catalog Realization

This document specifies how Syzy captures, admits, applies, and
recovers replicated DDL. Stable catalog identities, catalog-operation meaning,
and schema-chain ordering are defined in
[SCHEMA.md](../../docs/SCHEMA.md). CRDT dependencies are in
[CRDT.md](../../docs/CRDT.md).

This document owns the SQLite hooks, metadata realization, supported native SQL
surface, and failure handling. For the surrounding SQLite lifecycle see
[ARCHITECTURE.md](ARCHITECTURE.md); for
frontier and offline-peer rules see [PRUNING.md](../../docs/PRUNING.md).

## Implementation Status

The producer and broker support direct DDL when a `SchemaLog` is
configured. Public `sqlite.Open` and `syzy daemon` expose that wiring,
including schema-chain catch-up. The loadable extension configures a
schema log at attach (`sqlite/syzyext.OpenSchemaLog`: `SYZY_SCHEMA_LOG*` /
`SYZY_CLUSTER` env, defaulting to the file backend under the DB's
`-syzy/` state directory), so extension-attached clients can issue
direct DDL too. Attach only resolves and constructs the schema-log handle;
it does not call `Head`, `Read`, or `Append`, and stream transports remain
disconnected. Schema-authority operations begin only when admitting DDL or
catching up a schema-gated changeset, so opening an application connection
does not synchronously reach the remote schema authority. The extension's
LD_PRELOAD shim also interposes the
`sqlite3_prepare` family and `sqlite3_exec` to apply the SQL rewrites
(see [Transparently rewritten](#supported-ddl)) to host-app statements,
which never pass through syzy's own Prepare/Exec.

## Goals

- In schema-log-backed deployments, DDL can be issued directly through a
  SQLite client running the Syzy producer. No wrapper command is required.
- DDL does not globally fence ordinary DML.
- Concurrent DDL cannot create divergent schema branches.
- Destructive DDL has deterministic late-DML behavior: renames map by stable
  ID, drops become tombstones, and late writes to dropped objects become
  acknowledged no-ops instead of remote apply failures.
- Schema-evolution metadata is compact in steady state. Fine-grained clocks
  are sparse and are folded back when migration safety no longer needs them.

## Model

Every replicated schema has a single global schema chain —
`SchemaChain` (chain id 0) in CRDT.md vocabulary:

```text
schema_seq = 0, 1, 2, ...
```

Every replicated DDL event commits cluster-wide via a single CAS append
to the schema log before the originator's SQLite execution proceeds.
DML Changesets carry `Deps[SchemaChain] = required_schema_seq` under
which they were produced. Receivers apply a Changeset only after the
local catalog has reached that sequence; otherwise the broker keeps the
delivered payload in hand and retries it after schema-chain catch-up
advances `meta.schema_seq` via `schemalog.Read`.

Schema-gated apply is a broker-held retry loop today: the subscribe loop
does not acknowledge the delivered payload until catch-up makes the local
catalog sufficient. A future refinement attaches gating to
`applied_gaps` so a gated seq doesn't block the contiguous frontier
(see [ARCHITECTURE.md](ARCHITECTURE.md#not-yet-implemented)).

DDL is represented as a typed catalog mutation plus the original SQL text
for audit/debug. DML references stable `table_id` and `column_id` values
rather than mutable names.

## Schema Log

DDL admission is the only synchronous distributed step in the normal
write path. The schema log is a CAS log of schema events, not a
sequence allocator: events are durable from the moment `Append`
returns success, and the originator's local SQLite execution is not
load-bearing for cluster-wide schema progression.

The Go shapes live in [`schemalog/log.go`](../../schemalog/log.go)
(`schemalog.Event`, `schemalog.Log`); in sketch:

```go
type Event struct {
    SchemaSeq uint64
    ParentSeq uint64
    CatalogOp []byte
    RawSQL    string
}

type Log interface {
    // Append commits event at head+1 iff head == parentSeq.
    // ErrHeadMoved on CAS conflict.
    Append(ctx context.Context, parentSeq uint64, op []byte, raw string) (
        schemaSeq uint64, err error)

    // Read returns events strictly above fromSeq, up to limit.
    // ErrBelowHorizon if fromSeq is below the log's retention;
    // recovery is syzy_clone.
    Read(ctx context.Context, fromSeq uint64, limit int) (
        []Event, err error)

    // Head returns the current schema_seq head, or 0 when empty.
    Head(ctx context.Context) (schemaSeq uint64, err error)
}
```

Rules:

- `Append` is the cluster-wide commit point for a DDL. Once it returns
  success, every node will eventually see the event via `Read`.
- `ParentSeq` must equal the log's current head. Concurrent calls
  serialize via CAS; losers receive `ErrHeadMoved`, catch up via `Read`,
  and retry. Autocommit admission performs one catch-up + retry on the
  app's behalf (see the pipeline below); transactional DDL aborts the
  COMMIT instead, because the statement already executed against the
  stale schema.
- The log retains events under a deployment-policy retention window
  aligned with the offline-deadline contract. Below-horizon recovery is
  `syzy_clone`.

Backends:

- **Local** (`schemalog.NewLocal`): in-process; dev/test and single
  process.
- **File** (`schemalog.OpenFile`): SQLite-file CAS log; single host or
  shared filesystem.
- **Stream RPC** (`schemalog.Serve` / `schemalog.DialFunc`): hosts any
  backend over a reliable `net.Conn` stream. `ListenTCP` / `DialTCP`
  are the standard TCP wrappers; embedders may carry the same framed
  protocol over Unix sockets or VM-to-host transports. An AF_VSOCK
  adapter implements the deadline-capable stream directly; it must not
  pass the socket through Go's unsupported `net.FileConn` family lookup.
  The backend, rather than the stream transport, defines failover and
  durability.
- **S3** (`schemalog.OpenS3`): one object per `schema_seq` under
  `events/<seq:016x>.bin` via conditional `PutObject`.
  `Append(parent=N)` writes key `N+1` with `If-None-Match: *`.
  `Read(fromSeq, limit)` fetches exact keys starting at `fromSeq+1`
  and stops at the first missing key, so the catch-up poll path never
  lists the `events/` prefix. `Head` may list to find the highest key
  for startup and diagnostics. No leader, no failover — bucket
  durability is the HA story. Requires a provider that supports
  `If-None-Match: *` (AWS S3, R2, MinIO ≥ recent, Tigris, GCS S3 API).
- **Elected leader / linearizable KV**: extension point.

## Operating the Schema Log

Four deployment shapes:

```
# Single process:
syzy daemon --db app.db

# Single host, multi-process DDL:
syzy daemon --db app.db --schema-log /var/lib/syzy/schemalog.db

# Multi-host cluster, DDL leader (TCP):
syzy daemon --db app.db --listen :7000 --seeds peerA:7000,peerB:7000 \
  --schema-log /var/lib/syzy/schemalog.db --schema-log-listen :7100

# Multi-host cluster, follower (TCP):
syzy daemon --db app.db --listen :7000 --seeds peerA:7000,peerB:7000 \
  --schema-log-dial leader.local:7100

# Multi-host cluster, leaderless via S3:
AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… \
syzy daemon --db app.db --listen :7000 --seeds peerA:7000,peerB:7000 \
  --schema-log-s3 'https://my-bucket.s3.us-east-1.amazonaws.com?prefix=syzy/foo&region=us-east-1'
```

`--schema-log`, `--schema-log-dial`, `--schema-log-s3` are mutually
exclusive. `--schema-log-listen` requires `--schema-log`.

### Failure model

- **Schema log reachable**: DDL admission proceeds; followers apply new
  events on the next catchup tick.
- **Schema log unreachable**: `Append` returns an error; DML is
  unaffected (the apply path doesn't touch the schema log). Followers
  back off and retry.
- **Schema log data lost**: cluster integrity event. Peers hold only
  the *applied* catalog, not the event chain — recovery is to
  rebootstrap every node via `syzy clone` from a healthy peer onto a
  fresh schema log.

### HA

The S3 backend has no SPOF: bucket durability is the HA story, no
failover step exists. The TCP backend is single-leader with no
automatic failover; replicate the leader's file with
[Litestream](https://litestream.io) and fail over manually:

1. Detect leader loss (out-of-band).
2. `litestream restore` onto the standby.
3. Start the daemon there with the same `--schema-log --schema-log-listen`.
4. Repoint each follower's `--schema-log-dial` and restart it.

Future extensions (etcd lease, Raft) preserve the client API; the wire
protocol need not change.

## Metadata Catalog

The metadata stores stable schema identity:

```sql
-- meta key: schema_seq uint64

CREATE TABLE syzy_table (
  table_id BLOB PRIMARY KEY,
  name TEXT NOT NULL,
  state TEXT NOT NULL,             -- active | dropped
  default_clock_group TEXT NOT NULL, -- row | cell
  create_seq INTEGER NOT NULL,
  drop_seq INTEGER
) STRICT, WITHOUT ROWID;

CREATE TABLE syzy_column (
  table_id BLOB NOT NULL,
  column_id BLOB NOT NULL,
  name TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  state TEXT NOT NULL,             -- active | dropped
  clock_group TEXT NOT NULL,       -- row | cell | counter
  collation INTEGER NOT NULL DEFAULT 0, -- 0 = BINARY, 1 = NOCASE, 2 = RTRIM
  create_seq INTEGER NOT NULL,
  drop_seq INTEGER,
  PRIMARY KEY (table_id, column_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE syzy_key (
  table_id    BLOB NOT NULL,
  key_id      BLOB NOT NULL,        -- 0x00…00 = PK; otherwise a unique key
  column_id   BLOB NOT NULL,
  ordinal     INTEGER NOT NULL,     -- position within the key tuple
  state       TEXT NOT NULL,        -- active | dropped
  coordinated INTEGER NOT NULL,     -- 1 = CP (reserved); 0 = eventual (loser-null)
  create_seq  INTEGER NOT NULL,
  drop_seq    INTEGER,
  PRIMARY KEY (table_id, key_id, ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE syzy_schema_event (
  schema_seq    INTEGER PRIMARY KEY,
  parent_seq    INTEGER NOT NULL,
  catalog_op    BLOB    NOT NULL,
  raw_sql       TEXT,
  applied_at_us INTEGER NOT NULL,
  apply_state   TEXT    NOT NULL  -- 'applied' | 'failed_local'
) STRICT;

CREATE TABLE syzy_synth_trigger (
  child_table_id BLOB NOT NULL,   -- table that owned the cascade FK
  trigger_name   TEXT NOT NULL,   -- _syzy_fkcascade_<child>_<idx>_<d|u>
  parent_table   TEXT NOT NULL,   -- table the trigger lives on
  PRIMARY KEY (child_table_id, trigger_name)
) STRICT, WITHOUT ROWID;
```

Names are presentation metadata. IDs are stable:

- `RENAME TABLE` keeps the same `table_id`.
- `RENAME COLUMN` keeps the same `column_id`.
- `DROP TABLE` tombstones the `table_id`.
- `DROP COLUMN` tombstones the `column_id`.
- Recreating the same name allocates a new ID.

`syzy_key` records both the PK and any unique keys defined on a table.
The PK lives at the reserved sentinel `key_id = 0x00…00`; every other
row corresponds to a unique key declared via `UNIQUE` constraint or
`CREATE UNIQUE INDEX`. All rows sharing `(table_id, key_id)` carry
identical `(state, coordinated, create_seq, drop_seq)` — keys
transition lifecycle as a unit, not per member column. `coordinated`
distinguishes the CP reservation mode from the eventual loser-null mode
(see [Unique Keys](#unique-keys)). PK ordering for `pk_blob` encoding
comes from `syzy_key WHERE key_id = PK_SENTINEL ORDER BY ordinal`; the
same canonical encoder produces the arbitration tuple for unique keys.

The catalog is part of `syzy_clone`; new peers receive a catalog snapshot and
read forward from the schema log via `Read(fromSeq=local_schema_seq)`.

## Catalog Ops

```text
CreateTable(table_id, name, columns..., keys...)
AddColumn(table_id, column_id, column_def)
RenameTable(table_id, new_name)
RenameColumn(table_id, column_id, new_name)
DropColumn(table_id, column_id)
DropTable(table_id)
AddUniqueKey(table_id, key_id, columns...)
DropUniqueKey(table_id, key_id)
CreateIndex(raw_sql)                  -- non-unique only
DropIndex(raw_sql)                    -- non-unique only
CreateView(raw_sql)
DropView(raw_sql)
CreateVirtualTable(raw_sql)
DropVirtualTable(raw_sql)
CreateTrigger(name, raw_sql)
DropTrigger(name, raw_sql)
Bundle(sub_ops...)                    -- ordered list, applied atomically
```

`CreateTable` and `AddUniqueKey` are the two paths that populate
`syzy_key`. Both carry the per-key `coordinated` flag (derived from
member-column nullability at admission; see [Unique Keys](#unique-keys)).
`CREATE UNIQUE INDEX` admits as `AddUniqueKey` (typed), while plain
`CREATE INDEX` remains opaque SQL. `DropUniqueKey` flips the index's
`syzy_key` rows to `dropped`; `DROP TABLE` drops all of the table's
keys transitively.

The typed catalog op is authoritative; receivers issue the equivalent
SQLite schema change against their current catalog names. Each column
carries its declared type, nullability, default, generated flag, and
**collation** (`BINARY`/`NOCASE`/`RTRIM`), so a receiver reconstructs the
column with the same collation the origin declared — otherwise text
comparisons, ordering, and unique semantics would silently differ across
replicas. A custom (registered) collation cannot be replayed on a peer
and is rejected at admission (`CREATE TABLE` and `ADD COLUMN`).
`raw_sql` is audit/debug text only. DDL events live in the schema log and are mirrored
in the metadata `syzy_schema_event` table; they do not traverse the peer
transport (`Transport.Broadcast`/`Subscribe`/`Fetch` is DML-only).

## Apply Catalog Op

`apply_catalog_op` is the idempotent helper used by `wal_hook`
on the originator and by inbound apply (receivers, startup
recovery). Implementation in
[`internal/metadata/catalog_apply.go`](../../internal/metadata/catalog_apply.go)
(`Tx.ApplyCatalogOp`, the metadata side) and
[`internal/broker/broker_schema.go`](../../internal/broker/broker_schema.go)
(`applyCatalogStructural`, the SQLite side).

**Contract.**

- Reads the live SQLite structural post-state via `sqlite_schema` and
  pragmas (`table_xinfo`, `index_list`, `index_xinfo`,
  `foreign_key_list`) — never raw `sqlite_master` SQL text. If the
  post-state already matches the op, the SQLite DDL is skipped.
- Otherwise executes the SQLite DDL once on this connection. Failure
  (constraint violation, resource limit, etc.) aborts the catch-up
  pass with an error: no `syzy_schema_event` row is written and
  `meta.schema_seq` does NOT advance, so the next catch-up tick
  re-fetches and retries the same event (see
  [Failure Mode](#failure-mode)).
- Then UPSERTs `syzy_table` / `syzy_column` / `syzy_key` to match the
  op, in one metadata txn with the `syzy_schema_event` append and the
  `meta.schema_seq` advance. A metadata failure on the inbound path
  aborts that txn and the next tick retries; the structural precheck
  makes the retry idempotent. Only the originator's `resolve_intent`
  path — where the SQLite DDL has already committed and cannot be
  rolled back — records `syzy_schema_event(apply_state='failed_local')`
  on a failing catalog UPSERT and still advances `meta.schema_seq`;
  broker startup drains such rows by re-running the idempotent apply
  (`drainFailedLocalSchemaEvents`).

`wal_hook` on the originator never executes a SQLite DDL — the writer
connection just ran it, so the structural check always finds the
post-state present. The "execute SQLite DDL" branch fires only from
the inbound-apply path.

## Direct Local DDL Flow

The originator's writer connection runs `trace_v2` at statement
start, SQLite executes the DDL body, then `commit_hook` and
`wal_hook` complete the txn. Implementation in
[`internal/producer/ddl.go`](../../internal/producer/ddl.go) (`handleStmt`,
`buildCatalogOp`) and
[`internal/producer/producer.go`](../../internal/producer/producer.go)
(`commitHook`, `walHook`'s DDL branch via `maybeResolveDDL`).

**`trace_v2` admission**, in order:

1. Classify the SQL. Non-replicated objects skip; unsupported forms
   set the txn reject flag and `sqlite3_interrupt` the statement. DDL
   inside an explicit `BEGIN` takes the transactional path below
   instead.
2. Build `catalog_op` from the metadata catalog. SQLite-no-op cases
   (`IF NOT EXISTS` on an existing object, `IF EXISTS` on a missing
   one) skip Append entirely. **Data-dependent forms**
   (`ddlBodyFallible`: `CREATE UNIQUE INDEX`, `DROP TABLE`) stop here
   and take the deferred path — see [Fallible
   bodies](#fallible-bodies).
3. Pre-reserve this producer's intent slot
   (`meta."intent:<origin>" = LocalDDL{schema_seq=parent+1, ...}`)
   *before* calling `SchemaLog.Append`. Intent slots are origin-scoped:
   N producers share one metadata (app connections, sidecars, a
   host-side node) and must never overwrite or clear each other's
   in-flight slot. The pre-reservation ordering is required —
   the broker's catch-up loop yields when it sees a LocalDDL intent
   matching a schema-log event; without the pre-reservation, the
   broker could run `CREATE TABLE` on AppApply between Append and
   SetIntent and the originator's writer-connection DDL would then
   fail "table already exists."
4. Call the schema log's `Append(parentSeq, catalog_op, raw_sql)`. On
   `ErrHeadMoved` (CAS loss: a peer appended since `schema_seq` was
   read), clear the intent, catch the local catalog up to the log
   head, rebuild the op against the fresh schema, and retry steps 2–4
   once. Catch-up is the broker's synchronous pass when one runs
   in-process (full node); producer-only deployments wait — bounded,
   2s default — for the external catch-up authority sharing the
   metadata (the daemon, or a host-side node) to advance
   `meta.schema_seq`. After catch-up an identical peer DDL degrades
   to the step-2 no-op (`IF NOT EXISTS`) or a precise "already
   exists" rejection. A second CAS loss, or any other error, clears
   the intent and rejects the statement. Success returns the
   assigned `schema_seq`.
5. SQLite executes the DDL body.
6. `commit_hook` returns nonzero iff the reject flag is set — the
   codegen-evolution backstop.
7. `wal_hook` runs `resolve_intent` (below) and appends a `KindEmpty`
   journal record so the self-origin sequence stays dense.

If SQLite rolls back the DDL body in-process after Append committed
cluster-wide, `rollback_hook` clears the intent. The originator
catches up via the inbound path on the next schema-log Read.

### Fallible bodies

`trace_v2` admits a statement *before* SQLite runs it, so step 4's
Append describes a schema change that has not happened yet. Almost
always that is safe: bad DDL is rejected by SQLite's compiler, which
runs before the trace hook, so the statement never reaches admission
at all. The exceptions are forms whose validity depends on table
**data** and can therefore only fail during execution:

- `CREATE UNIQUE INDEX` scans existing rows and fails on duplicates.
- `DROP TABLE` fails a foreign-key check against dependent rows when
  `PRAGMA foreign_keys` is `ON`.

Appending these at trace time would publish a schema change the
originator does not have — an event no node can heal from, since the
local data is exactly what rejected it. Both forms instead **defer the
Append to `commit_hook`** (`ddlBodyFallible`, `stashPendingDDL`), which
makes SQLite's own execution the thing that decides whether the event
is published: a failed body rolls the implicit transaction back,
`commit_hook` never fires, and nothing reaches the log. The cost is
that a CAS loss on these forms surfaces as a failed statement instead
of a transparent catch-up + retry — the same trade the transactional
path makes.

Inside an explicit transaction a failed statement does *not* abort the
transaction, so an application that ignores the error can still
`COMMIT`. `verifyPendingDDL` closes that hole: at the next statement's
`trace_v2` — `COMMIT` is itself traced, and `commit_hook` must never
touch the writer connection — the stashed admission is checked against
`app.db`'s real post-state and dropped if its statement left no trace.
Local and cluster state then agree that nothing happened.

Failures outside this class (a disk fault mid-`CREATE TABLE`) are not
covered on the autocommit path; the transactional path's post-state
check catches them regardless of form.

### Transactional DDL

Any number of DDL statements are admitted per explicit
`BEGIN ... COMMIT` transaction, and the whole transaction publishes as
**one** schema event — a single `OpBundle` when it carries more than one
statement — so receivers apply a migration atomically or not at all. No
`disable_ddl_transaction!`-style opt-out is needed. Implementation in
[`internal/producer/ddl.go`](../../internal/producer/ddl.go)
(`handleTxnDDL`, `stashPendingDDL`, `commitPendingTxnDDL`).

Statements after the first routinely name objects the earlier ones
created (`CREATE TABLE t; CREATE INDEX ON t(v)`), which the committed
catalog cannot resolve — it does not learn about the transaction's DDL
until COMMIT. Admission therefore builds each op against a
**transaction-local catalog overlay** (`catalog.Overlay`), a
copy-on-write view of the catalog with the transaction's own admitted
ops folded in; an abandoned transaction leaves the catalog untouched.
The receiver's catch-up uses the same overlay when applying a bundle,
for the same reason (`applyCatalogStructuralVia`).

All the ops share the `parentSeq` pinned at the first one, so the
commit-time CAS validates the transaction as a unit.

The flow inverts the autocommit ordering twice: validation +
`catalog_op` construction still happen at `trace_v2` (a bad statement
fails at the statement), but the admission writes are deferred to
`commit_hook` — COMMIT is the cluster-wide commit point — and there
the order is `Append` BEFORE `SetIntent`. The autocommit path's
pre-reservation (intent before Append) exists so the broker can't
structurally apply a just-Appended event before the originator's DDL
has executed; in a transaction the DDL already executed under the
transaction's write lock, which blocks structural applies until
COMMIT, so that race can't arise — and Append-first means no crash
window can leave a metadata intent pointing past the log head.
Consequences:

- An explicit `ROLLBACK` (or any mid-transaction failure) replicates
  nothing: no schema event, no intent, no reconciliation debt.
- A CAS loss at commit (another writer's DDL landed while the
  transaction was open — the Append CAS at the trace-time parent is
  the freshness check) fails the COMMIT with
  `SQLITE_CONSTRAINT_COMMITHOOK` and rolls the whole transaction back;
  the application retries against the new schema.
- `wal_hook` then resolves the intent before journaling the
  transaction's touch records, so same-transaction DML drains under the
  new `schema_seq` and receivers schema-gate correctly.

- A statement that fails mid-transaction contributes nothing even if
  the application ignores the error and commits anyway — see [Fallible
  bodies](#fallible-bodies). The statements that did take effect still
  publish.

Constraints (each a precise admission rejection):

- all DDL must precede any DML in the transaction (touch-journal
  emptiness is the check; trailing DML like `schema_migrations`
  bookkeeping is fine),
- no DDL under `SAVEPOINT` scope — `ROLLBACK TO` could partially undo
  it in ways the schema chain cannot model,
- no cascade-FK `CREATE TABLE` bundles inside a transaction (synth
  triggers are installed via the helper connection, which would block
  on the transaction's write lock).

Local-only DDL (`_*`, `sqlite_*`, non-`main` schemas) is exempt and
runs freely inside transactions — it never touches the schema log.

### `resolve_intent` (DDL branch)

Run by `wal_hook` on the live path and by producer startup recovery
(../../docs/CRDT.md#intents). Each producer resolves only its own origin's slot.
Because the autocommit path writes the intent *before* `Append`, an
intent's presence is NOT sufficient evidence the event is durable:
resolution first verifies the log really holds this event at
`intent.schema_seq` with matching `catalog_op` bytes. An unappended or
seq-lost intent is cleared without resolving — resolving it would
advance `schema_seq` past the log head (or onto someone else's event),
an unrepairable fork.

In one metadata txn: run `apply_catalog_op(intent.catalog_op)`,
UPSERT `syzy_schema_event` with the resulting state (`applied` or
`failed_local`), advance `meta.schema_seq`, and clear the origin's
intent slot. Implementation in
[`internal/producer/ddl_resolve.go`](../../internal/producer/ddl_resolve.go)
(`resolveLocalDDL`).

Producer startup runs `resolve_intent` **before** accepting writes —
unconditional. Local SQLite catalog and metadata catalog could be out
of sync if SQLite executed the DDL but `wal_hook` never ran, and
accepting DML in that state would silently diverge.

### Catch-up

Run by the broker's `schemaCatchupLoop`, by inbound DML carrying
`Deps[SchemaChain] > meta.schema_seq`, and by producer startup after
`resolve_intent`. Implementation in
[`internal/broker/broker_schema.go`](../../internal/broker/broker_schema.go)
(`runSchemaCatchup`).

For each batch from `schemalog.Read(meta.schema_seq, batch)`:

- `ErrBelowHorizon` ⇒ operator must run `syzy_clone`.
- If a FRESH LocalDDL intent (any origin, started within the last
  60s) matches `e.schema_seq`, the originator's `wal_hook` is
  finalizing it — yield. A STALE intent marks a dead originator
  (crashed guest, killed process); yielding to it forever would wedge
  the schema pipeline, so catch-up applies the event itself and clears
  the dead slot in the same txn. A slow-but-alive originator judged
  stale stays consistent: its `resolve_intent` already-applied fast
  path clears its own slot.
- Otherwise apply each event in one metadata txn (same shape as
  `resolve_intent`).

## Failure Mode

DDL apply failure on a receiving node's inbound catch-up:

- The catch-up pass aborts without advancing `meta.schema_seq` and
  without writing a `syzy_schema_event` row. Transient failures
  (`SQLITE_BUSY`, `SQLITE_LOCKED`, cancellation, I/O, or temporary
  resource exhaustion) leave no health marker; the next tick
  re-fetches and retries the same event.
- A terminal failure means this node can no longer prove that it can
  follow the durable schema chain: it fell below the log retention
  horizon, received a non-contiguous or undecodable event, or SQLite
  deterministically rejected the structural operation. Before
  stopping catch-up, the broker atomically writes the first failing
  sequence and reason to `meta['schema_unhealthy']`. It does not
  advance `meta.schema_seq` or write an applied schema-event row.
- Once the marker exists, catch-up returns `ErrSchemaUnhealthy`
  without reading or applying more schema events. The running node
  exposes the marker through `InboundHealth`; a subsequent `Open`
  refuses to start. `syzy status` remains available because it opens
  metadata read-only and reports the recorded sequence and reason.
- A bad-on-every-node failure (the operator fat-fingered a DDL that
  fails uniformly) blocks every receiver identically — including the
  originator: on the autocommit path `Append` precedes the DDL body,
  so a failing body rolls back locally (`rollback_hook` clears the
  intent) and the originator meets the same event, and the same
  failure, on its own inbound catch-up. The schema log stays
  consistent cluster-wide, and the app saw the SQLite error. Recovery
  is fixing the condition so the event applies; as a last resort,
  rebootstrap via `syzy clone` onto a fresh schema log (same shape as
  schema-log data loss).

There is no operator skip call, marker-clear call, or local
escape hatch. `syzy_clone` is the only divergence repair; an operator
cannot make a broken schema chain appear healthy by deleting or
advancing local metadata.

## Schema Health

`meta['schema_unhealthy']` is the durable, fail-closed schema-health
record. Its value is an 8-byte big-endian failing schema sequence
followed by a non-empty UTF-8 diagnostic reason. The write is
first-failure-wins and idempotent: later catch-up attempts cannot
overwrite the event that originally made the node unsafe.

Absence of the key means no terminal schema divergence has been
observed. It does not replace ordinary retry/error reporting: transient
catch-up failures remain visible through the broker's last error and do
not create the key. Presence means the node requires a fresh clone,
regardless of whether the local environmental condition later changes.

`apply_state='failed_local'` is not schema health. Such rows are
originator-side metadata recovery records (see
[`resolve_intent`](#resolve_intent-ddl-branch)) and are drained by an
idempotent re-apply at broker startup.

## ID-Addressed DML

DML Changesets carry:

```text
Dot{Origin, Seq}
Stamp{Clock, Origin}
Deps{SchemaChain: required_schema_seq}
table_id
pk_blob                    -- encoded by stable PK column IDs
changed column_id -> value -- updates carry changed columns only
```

Apply rules:

- A changeset whose `Deps[SchemaChain]` is ahead of `meta.schema_seq`,
  or that references a `table_id` not yet in the local catalog, is
  schema-gated; one whose every record targets a dropped table is
  deterministically ignored — see [Apply Outcomes](#apply-outcomes)
  for the outcome classification.
- If a changed `column_id` is dropped, ignore that column and apply any
  surviving changed columns (deterministic across replicas — not a
  failure).
- Renamed tables/columns map through the catalog to current SQLite
  names. Renaming a PK column is safe because the column ID is
  unchanged.

This makes destructive DDL deterministic without DML fencing. A stale
writer can still express confused application intent, but Syzy
converges: writes to dropped objects become bounded loss/no-op rather
than corruption.

## Unique Keys

Replicated `UNIQUE` constraints (column-level, table-level, or
`CREATE UNIQUE INDEX`) come in two modes, selected by member-column
nullability:

- **Eventual (loser-null).** A key with any **nullable** member column
  converges with no coordination: concurrent writers may both commit
  the same value locally, then per-value LWW on the cell-LWW layer
  keeps the highest-Stamp row and nulls the losers' member columns. A
  successful local insert is therefore *not* a guarantee of global
  uniqueness — the AP default, zero added latency.

- **Coordinated (by construction).** A key whose members are all
  **`NOT NULL`** cannot express a loser-null state, so it is enforced
  *before* the local commit through a synchronous global reservation.
  The second writer to claim a value never commits: its commit is
  vetoed by the commit hook and fails with
  `SQLITE_CONSTRAINT_COMMITHOOK`, wrapped as a coordinated conflict
  (`sqlite.IsCoordinatedConflict`). A leaseholder handover or partition
  that outlasts the in-commit retry budget carries the same SQLite code
  but is wrapped as unavailable (`sqlite.IsCoordinatedUnavailable`).
  Conflict is final; only unavailable is retryable off the writer. This
  is a CP operation: one round-trip per write that touches the key, and
  unavailability never degrades into a silent conflict. The mechanism lives in
  [ARCHITECTURE.md#coordinated-uniqueness](ARCHITECTURE.md#coordinated-uniqueness).

`NOT NULL UNIQUE` selects the coordinated mode automatically — there is
no separate syntax, and ORM-emitted schemas work unchanged. In a
single-process deployment the reservation backend is in-process (no
round-trip); a multi-writer cluster requires a shared reservation
backend, without which the DDL is rejected at admission.

**Index normalization.** A coordinated key has no physical `UNIQUE`
index on any node — enforcement lives in the reservation gate alone, so
SQLite-level enforcement anywhere would misfire on legal cross-writer
interleavings (see [Reservation](#reservation-coordinated-keys)).
Immediately after the originator's DDL commits, and before the key
activates, the originator normalizes its own physical schema to the
index-free shape receivers materialize:

- An inline `NOT NULL UNIQUE` / table-level `UNIQUE(...)` is stripped
  from the stored `CREATE TABLE` text (a table rebuild preserving rows,
  indexes, and triggers) — the constraint no longer appears in
  `sqlite_master`; `syzy_key` is its record.
- A standalone `CREATE UNIQUE INDEX` executes verbatim (keeping the
  create-time duplicate scan), then is downgraded in place to a plain
  index of the same name.
- A database created before normalization existed converges at the next
  `Open` (a one-time rebuild, logged as such) — unless some trigger
  writes the table, in which case the index is kept (it is the only
  thing holding that ungated channel honest) and the condition is
  logged; drop the trigger and reopen.

`DROP INDEX` on the originator maps an index matching an active
coordinated key to key removal, replicated to every node. The match is
by column list **and** `WHERE` predicate: several coordinated keys may
share a column tuple with different predicates, and a plain lookup index
carries no predicate, so a column-only match would remove an arbitrary
one of them. To keep a *total* key's match unambiguous, admission allows
at most one index over its exact columns — `CREATE INDEX` over them is
rejected, and so is a coordinated key over columns an unfiltered index
already covers. An inline-declared key has no index name to drop; remove
it via `DROP TABLE` or by recreating the table without the constraint.

A `UNIQUE` index may be **partial** (`CREATE UNIQUE INDEX … WHERE
<predicate>`): uniqueness is enforced only over the rows the predicate
admits. This is the soft-delete idiom — `UNIQUE(email) WHERE deleted_at
IS NULL` lets any number of soft-deleted rows share an email while
keeping it unique among the live ones. Partial is supported in
**coordinated mode only** (all members `NOT NULL`); the predicate widens
the reservation's participation gate from "the key tuple is non-NULL" to
"non-NULL *and* the predicate holds" (see
[Reservation](#reservation-coordinated-keys)). An eventual partial index
is rejected (see [DDL Rules](#ddl-rules)).

Each unique constraint allocates a stable `key_id` recorded in
`syzy_key` with the per-key `coordinated` flag. The same canonical
encoder used for `pk_blob` produces the arbitration tuple (eventual) or
reservation tuple (coordinated) for any unique key. A partial key
additionally records its predicate, resolved to column IDs (so it
survives column renames) and carried on the key's catalog op so every
replica reconstructs the same participation test.

### DDL Rules

A `UNIQUE` constraint or `CREATE UNIQUE INDEX` is accepted iff:

- A **partial** index (`CREATE UNIQUE INDEX … WHERE <predicate>`) has
  all members `NOT NULL` (coordinated). An *eventual* partial index is
  rejected: loser-null arbitration runs on every receiver's apply path,
  where the predicate column converges by independent cell-LWW, so "does
  this row participate?" would be evaluated against a transiently-
  divergent row — unsound. A coordinated partial index evaluates its
  predicate only at the originating writer (reserve) and the leaseholder
  (rebuild), never on a receiver, so that timing hazard does not arise.
- A partial predicate references only replicated, active, non-generated
  columns and uses only deterministic operators (SQLite already forbids
  non-deterministic functions in an index `WHERE`). The supported grammar
  is `IS [NOT] NULL`, comparisons (`= <> < <= > >=`) and `[NOT] IN`
  against literals, and `AND`/`OR`/`NOT`. A comparison literal's class
  must match the column's affinity (numeric ↔ numeric, text ↔ text;
  blob literals are unsupported), so no affinity coercion happens.
  Text comparisons honor the
  column's collation: the predicate column's collation
  (`BINARY`/`NOCASE`/`RTRIM`) is captured at admission and compiled into
  the predicate, so the writer's Go-side reserve gate, the rebuilt
  enumerate (which emits an explicit `COLLATE`), and SQLite's own partial
  index all agree on participation. A column with a *custom* (registered)
  collation cannot be replicated and is rejected at column admission.
- Every replicated unique key's **member** columns — coordinated or
  eventual — must have `BINARY` collation. The reservation tuple
  (coordinated) and the arbitration tuple (eventual) are the canonical
  byte encoding of the value, which does not fold case or trim — so a
  `NOCASE`/`RTRIM` member would let two values that the SQLite index
  considers equal reserve (or arbitrate) as distinct, violating
  uniqueness cross-node. Collation-folding member keys are future work;
  until then a non-`BINARY` unique-key member is rejected. (A
  non-`BINARY` *predicate* column is fine — only members encode into
  the tuple.)
- **Eventual** keys (any nullable member) have no member column of
  SQLite `BLOB` affinity. The inbound apply connection cannot disable
  SQLite's UNIQUE enforcement, and `blob_patch` on an eventual UNIQUE
  BLOB column can transit through values that transiently collide with
  another row mid-txn.
- **Coordinated** keys reject `BLOB` members as well. A coordinated key
  has no physical `UNIQUE` index on any node (see
  [Reservation](#reservation-coordinated-keys)), so nothing at the
  SQLite layer stops `sqlite3_blob_open` from incrementally rewriting a
  key column — and blob-write fires carry no whole-value image for the
  reservation scan, so an incremental write would bypass the gate
  entirely. Whole-value keys over `TEXT`/numeric columns are unaffected.
- No key member — coordinated or eventual — is a generated column: a
  generated value is derived per-replica, never captured as a cell, so
  neither the reservation tuple nor the arbitration tuple could carry
  it.
- No trigger writes a coordinated-key table, enforced in both creation
  orders: `CREATE TRIGGER` whose body INSERTs or UPDATEs such a table
  is rejected, and coordinated-key creation is rejected while an
  existing trigger writes the table. Trigger bodies run at apply depth
  on every replica and bypass the reservation gate; DELETE-only bodies
  are fine (a vacated value is observed and freed by the leaseholder).
  FK actions that synthesize an UPDATE-child cascade trigger
  (`ON DELETE SET NULL`/`SET DEFAULT`, `ON UPDATE CASCADE`) are
  rejected the same way; `ON DELETE CASCADE` is fine.
- A transaction may create a coordinated key or write the table it
  covers, not both: the commit hook rejects same-transaction DDL + DML
  (the claims capture began before the key existed).
- A **composite or partial** coordinated key requires the table to use
  the row clock group (and `SetClockGroup('cell')` is refused while one
  exists): under cell-level merge, two origins' gated writes to
  different columns of one row could converge to a key tuple nobody
  reserved. Single-column total keys are safe under cell merge.
- A coordinated key requires a configured reservation backend in a
  multi-writer cluster. Without one, the `NOT NULL UNIQUE` DDL is
  rejected at admission.

### Reservation (coordinated keys)

Coordinated uniqueness rests on one idea: **the replicated rows are the
durable source of truth for who owns a value; the leader is soft state
— a serialization cache over the rows.** Full replication means every
node already holds every row, so the leader derives the taken-set by
enumerating each coordinated key's participating rows on its own
replica rather than keeping a private durable store.

- **Reserve-before-commit.** At `commit_hook` the originating node
  computes the txn's net coordinated values and reserves each against
  the leaseholder in one batched round-trip. Success → the commit
  proceeds; conflict → the commit is rejected as
  `SQLITE_CONSTRAINT_COMMITHOOK` wrapping `unique.ErrConflict`; an
  unavailable leaseholder rejects with the same SQLite code wrapping
  `unique.ErrUnavailable`. A crash between reservation and commit
  leaves at most a row-less grant — a safe leak (a value blocked, never
  a duplicate), reclaimed by GC.
- **Participation predicate (partial keys).** A total key's row
  participates — holds a reservation — exactly when its key tuple is
  non-NULL. A partial key narrows that to non-NULL *and* the index
  predicate holding. The writer evaluates the predicate against the
  transaction's pre- and post-images, which are full rows, so a
  predicate column the statement never touched is still present: it
  reserves the post-value when the post-image participates and releases
  the pre-value when the pre-image participated and the post-image does
  not (or holds a different value). A soft-delete (setting `deleted_at`)
  is thus an ordinary release; an undelete that collides with the live
  owner fails with the same `SQLITE_CONSTRAINT_COMMITHOOK` commit
  rejection. The leaseholder's rebuild-from-rows applies the same
  predicate as a `WHERE` filter when reconstructing the taken-set.
- **Apply does not coordinate.** Only the originating writer reserves;
  every replica applies the row directly. The reservation already
  guaranteed cluster-wide exclusivity, so no apply has a competing row
  and `NOT NULL` applies cleanly. **No node holds a physical `UNIQUE`
  index for a coordinated key** — the originator normalizes its own DDL
  to the same index-free shape receivers materialize (see
  [Unique Keys](#unique-keys)) — so apply can never wedge on an index
  that a legal cross-writer interleaving (e.g. a same-transaction value
  transfer) would violate statement-by-statement. The apply hot path is
  unchanged.
- **Reclaim — release hold.** A value freed by a delete, a
  value-change, or a partial key's predicate flip (e.g. a soft-delete)
  is held under a release hold — unrelated to the apply-quarantine
  mechanism in [Apply Outcomes](#apply-outcomes) — and reclaimable by a
  *different* row only after a
  conservative window ≥ the cluster's bounded-staleness deadline
  ([PRUNING.md](../../docs/PRUNING.md)) — by then a lagging node has either observed
  the release or been evicted, so no replica can see two rows claiming
  the value even transiently. (The owning row may reclaim its own value
  immediately.) The reclaim machinery is indifferent to *what* released
  the value, so a predicate flip needs no special handling. This keeps
  the apply path free of unique-collision gating; the cost is that a
  just-freed value cannot be re-registered by a new row until the window
  elapses (typically seconds).

### Apply Algorithm (eventual keys)

For each record `R` writing `pk = P` to a table that has active
eventual unique keys, after the standard cell-LWW pass determines which
of `R`'s columns win:

```text
for each active unique key K on R.table:
  if K's columns ∩ R's accepted columns is empty: continue
  v = canonical(R, K's columns)             -- post-cell-LWW values
  if v has any NULL: continue               -- multi-NULL allowed
  Q = SELECT pk_cols FROM <table> WHERE <K cols> = v
  if Q is null or Q == P: continue          -- no contention
  s_Q = MAX over c ∈ K of effective_stamp(Q, c)
  if R.stamp > s_Q:                         -- R wins, steal v from Q
    stage <table> SET <K cols> = NULL WHERE pk = Q
    stage cell_clock[Q, c ∈ K] := R.stamp   -- standard cell-LWW write
  else:                                     -- R loses, cede v to Q
    rewrite R's K-column writes to NULL at R.stamp
```

`effective_stamp(pk, c)` is the structural fall-through defined in
CRDT.md#layer-composition: `cell_clock[pk, c]` if present, else
`row_clock[pk]`'s baseline Stamp. `MAX` over the key's columns selects
the latest moment at which the row asserted the tuple.

The arbitration runs inline within the inbound-apply transaction — see
[ARCHITECTURE.md](ARCHITECTURE.md#inbound-apply). SQLite's own unique
index is the materialization of "who currently owns `v`"; cell_clock
provides the arbitration stamp. No new clock table is introduced.

The same path runs for local commits at wal_hook materialization: if a
remote write later wins ownership of a value the local row holds, the
loser-null fires through ordinary cell-LWW on the next inbound apply.
SQLite's own UNIQUE constraint still rejects same-replica duplicates at
write time, so contention reaches the apply path only on genuine
concurrency.

### Invariant

Unique-key exclusivity is CRDT.md invariant 10 (see
[CRDT.md#invariants](../../docs/CRDT.md#invariants)): coordinated keys hold it by
construction, eventual keys post-convergence. The PK is exempt — PK
conflicts cannot arise because PK is the row identity.

## Triggers

`CREATE TRIGGER` / `DROP TRIGGER` replicate as opaque SQL via
`OpCreateTrigger` / `OpDropTrigger` (same shape as views/vtables).
Trigger bodies fire on every replica — on the originator's local writes
and on receivers' inbound apply. The apply connection runs with
triggers enabled; only FKs are deferred. Trigger-induced preupdate
events are filtered via `sqlite3_preupdate_depth() > 0`, so derived
writes never enter the captured changeset; each replica re-derives them
from the source row.

Triggers must be deterministic functions of `OLD` / `NEW` and currently-
replicated state — same rule as generated-column expressions.
`random()`, `datetime('now')`, reads of non-replicated tables, and
reads of unsettled cluster state diverge.

Operator rules:

- Cells maintained by triggers should not also receive direct replicated
  writes; direct writes win cell-LWW and transiently clobber the
  trigger-derived value until the next source-row write re-derives.
- Top-level writes to replicated tables only — writes routed through
  `INSTEAD OF` triggers on views, or fired from triggers on non-
  replicated source tables, are not captured.
- Triggers don't fire retroactively. After `CREATE TRIGGER`, run the
  equivalent of `INSERT INTO fts(fts) VALUES('rebuild')` on each replica
  to reconcile derived state for rows that pre-date the trigger.

## Cascade Rewriting

`FOREIGN KEY` clauses bearing cascade actions (`ON DELETE CASCADE`,
`ON DELETE SET NULL`, `ON DELETE SET DEFAULT`, `ON UPDATE CASCADE`)
are rewritten at the originator into `BEFORE DELETE` / `BEFORE UPDATE`
triggers on the referenced parent table. The rewritten `CreateTable`
plus one `CreateTrigger` per cascade action ship as a single `Bundle`
schema event, applied atomically on receivers.

The originator's local SQLite executes the user's original DDL with
the FK + cascade clauses intact. The synthesized `BEFORE` trigger
fires before SQLite's internal cascade trigger on each parent
delete/update; by the time SQLite's cascade runs there are no children
left to act on, so it's a no-op. Both depth-1 trigger paths are
elided by the depth filter, so the wire carries only the parent
write.

FKs themselves remain local-only (the catalog has no FK fields, so
receivers' reconstructed `CREATE TABLE` carries no FK clause). The
synthesized trigger is what makes cascades replicate: it ships via
`CreateTrigger`, installs on every replica, and fires on the apply
connection.

Synthesized triggers are named `_syzy_fkcascade_<child>_<idx>_<d|u>`
and live in the `_*` reserved namespace. Each is recorded in
`syzy_synth_trigger` so that `DropTable` on the child can emit the
matching `DropTrigger` ops in the same `Bundle` (the triggers live
on the parent, not the child, so SQLite's `DROP TABLE` on the child
does not cascade-drop them).

Self-referential cascades and chained cascade-of-cascade chains
work via SQLite's normal trigger recursion (`PRAGMA recursive_triggers`
defaults on; the depth cap applies). `SET DEFAULT` on a column with
no declared default degrades to `SET NULL` per SQLite semantics.

## Apply Outcomes

The apply path has no explicit FSM (see
[ARCHITECTURE.md#inbound-apply](ARCHITECTURE.md#inbound-apply)).
Possible outcomes per inbound DML changeset:

| Outcome | Notes |
|---|---|
| applied | One BEGIN IMMEDIATE / DML / COMMIT against AppApply, then `cache.MarkApplied` advances frontier; mirror journal appended. |
| ignored (deterministic) | Every record targets a catalog table already known to be dropped — apply loop skips every record and the cache still records the seq as applied. |
| schema-gated | Required `schema_seq` is ahead, or a record's `table_id` is not yet in the local catalog. The broker-held retry loop waits for catch-up; cache state is unchanged. |
| quarantined | Deterministic, payload-specific failure: a SQLite constraint violation (the canonical shape is a partial cross-origin record — e.g. an UPDATE-shaped Insert missing a NOT NULL column — arriving before the INSERT that creates its row; the wire carries no row-level causal deps, `Deps` covers the schema chain only), a counter wire-contract/overflow failure, or a row-group update outrunning its row's INSERT. The changeset's exact payload bytes are durably parked in the `apply_quarantine` metadata table and only then does the origin's frontier advance past the seq, so later records from that origin keep flowing instead of head-of-line blocking. Auto-re-applied every fetcher round (adaptive interval, ~30s base, plus wakes on new records); the entry is deleted once the missing dependency lands and the deferred apply succeeds. Residency is capped per origin (128): at the cap the broker stops advancing and hard-blocks that origin — a flood of deterministic failures signals real corruption or a schema-divergence bug, not a delivery gap. |
| error (transient) | Transient/environmental failure (I/O, `SQLITE_BUSY`, disk full). Returned to transport for retry in place; cache state is unchanged — advancing past an error that will resolve would be wrong. |

The frontier is a **local-durability watermark, not an applied
watermark**: for every origin, every seq ≤ frontier is either applied
to the database or resident in `apply_quarantine` with its exact
payload bytes. The store-then-advance ordering makes a crash in
between re-quarantine idempotently, never silently drop; and because
the payload is retained locally before the frontier advances, draining
the quarantine never needs redelivery from peers or the object store.
Convergence is unaffected — cell-level LWW arbitration makes the order
in which a deferred write finally applies irrelevant to the converged
state, so quarantine changes *when* a record's effect appears, never
*what* the converged result is. `InboundHealth` exposes
`QuarantineResident`, `QuarantineOldest`, and `QuarantineMaxAttempts`:
transient non-zero residency during cross-origin delivery races is
normal and drains automatically; steady residency with a climbing
attempt count since `QuarantineOldest` means an entry can never apply
and needs operator attention.

A schema-gated changeset that's stuck typically means the schema log is
unreachable or the local node fell below the schema log's retention
horizon. Operators investigate the schema-log connection or run
`syzy_clone`. There is no operator-escape state equivalent to a
"dead-lettered" terminal; once schema-gated apply attaches to
`applied_gaps`, the operator escape can take the form of forcing a
particular seq into the gap-set (acknowledging it as permanently
absent, so the contiguous frontier can advance past it).

## Sparse Clock Groups

Row clocks are the steady-state default:

```text
row_clock(table_id, pk_blob) = row existence/deletion clock
```

Fine-grained clocks are sparse overrides:

```text
cell_clock(table_id, pk_blob, column_id)
blob_range_clock(table_id, pk_blob, column_id, byte_range)
```

For a column without a `cell_clock`, the effective value clock is the row
clock. Cell clocks are created only when a column needs independent ordering:
schema evolution, explicit column-LWW policy, or blob-patch interaction.

DELETE is always row-level. A row tombstone at clock `T` dominates every
cell/blob clock below `T`. A later insert/update with a greater clock can
resurrect the row.

`cell_clock` is not age-pruned. Entries are retained indefinitely; they
are sparse (bounded by the per-migration scope of schema evolutions)
and harmless when retained. Opportunistic local collapse deletes a
`cell_clock` override whose `(hlc, hlc_origin)` is identical to the
row's `row_clock`. Full frontier-driven stabilization is a future
addition; see [PRUNING.md](../../docs/PRUNING.md).

## Counter Columns

A **counter column** merges concurrent writes by summation instead of
LWW, so no concurrent increment is ever lost
(`UPDATE inventory SET quantity = quantity + 3 WHERE id = ?` from two
nodes at once nets `+6` everywhere). The math is CRDT.md's `F_counter`
layer; this section is the declaration surface, the admission rules,
and the payload/apply contract.

### Declaration

A column is a counter **from birth**, declared by the `COUNTER` token
in its type at `CREATE TABLE` or `ALTER TABLE … ADD COLUMN`:

```sql
CREATE TABLE inventory (
  id       TEXT PRIMARY KEY,
  name     TEXT,
  quantity INTEGER COUNTER NOT NULL DEFAULT 0
);
```

The declared type travels verbatim through the catalog op and is
re-emitted on receivers (SQLite treats `INTEGER COUNTER` as an
ordinary type name with INTEGER affinity). The column's
`syzy_column.clock_group` is `'counter'`. There is no way to flip an
existing column to (or from) counter: born-counter means every record
ever produced for the column was produced under counter semantics, so
no record can straddle a semantic change — the divergence class that
would otherwise require per-record epoch gating simply cannot occur.

Admission rules (each a precise rejection at DDL admission):

- The declared type must contain both `COUNTER` and `INT` (INTEGER
  affinity). Real-valued counters are not supported.
- The column must be `NOT NULL` (`NULL + delta` is `NULL` in SQL;
  summation needs a total value). `DEFAULT 0` is recommended.
- The column must not be a PK member, `GENERATED`, or a member of any
  unique key (a summed value has no stable identity to reserve or
  arbitrate).
- `CREATE TABLE` with a counter column sets the table's
  `default_clock_group` to `'cell'` (counter payloads are per-column
  diffs; whole-row images would stomp concurrent contributions).
  `ADD COLUMN … COUNTER` on a row-group table is rejected — flip the
  table with `SetClockGroup(table, 'cell')` first. `SetClockGroup(…,
  'row')` on a table with counter columns is rejected.
- `STRICT` tables cannot declare counter columns (SQLite rejects the
  type name natively).

### Payload

Capture is unchanged (preupdate OLD/NEW images). At materialize:

- **UPDATE**: a counter column ships the signed delta `NEW − OLD`
  (netted across the transaction, like every diff), marked on the wire
  with `ColValue.Format = FormatDelta`. Every UPDATE is a relative
  adjustment — `SET quantity = 0` ships `−OLD` ("subtract what I
  observed"), which converges without erasing concurrent increments.
  An absolute reset is expressed by DELETE + re-INSERT (a new CL
  generation).
- **INSERT / DELETE / PK-change**: unchanged (full image / tombstone).
- A non-integer value observed in a counter column at materialize is a
  hard error (same failure mode as an unencodable PK): admission makes
  it unreachable without deliberate type abuse, and a loud stop beats
  silent divergence.

### Apply

Counter cells carry no Stamps and are never stamp-gated; records
carrying counter contributions gate on CL only (their register columns
still arbitrate per column as usual):

- Update delta at the row's current CL → summed into the current cell
  value. The arithmetic runs in Go with the current value read inside
  the apply transaction — SQLite's `+` silently promotes to REAL on
  int64 overflow, an order-dependent (non-convergent) result — and an
  overflowing contribution fails deterministically into quarantine
  (retried; a compensating decrement can bring the cell back in range).
- Insert image landing on a live row at the same CL (a concurrent
  same-PK insert) → the counter column *adds* its image value; within
  a generation the cell converges to the sum of all contributions,
  regardless of arrival order. Likewise, a generation-establishing
  Insert that finds a physical row the row clock doesn't cover (a
  local INSERT committed but not yet drained; an adopted pre-existing
  row) merges counter columns additively instead of erasing the
  contribution the row already carries.
- CL-bumping record (resurrection insert, or an update racing ahead of
  its insert) → applies its contribution absolutely, resetting the
  cell for the new generation.
- Delete dominates same-generation contributions row-level, as always.
- Wire contract, enforced at apply: `FormatDelta` is honored only on a
  declared counter column and only as an 8-byte integer, and a counter
  column's update values are only ever deltas. A violating value (a
  buggy or hostile peer) fails deterministically into quarantine — it
  can neither bypass stamp arbitration nor run SQL arithmetic on a
  register column.

### Exactly-once

`col = col + ?` is not idempotent, so the applied-frontier
short-circuit is load-bearing. The in-memory frontier is snapshotted
asynchronously; the crash window registers cover by idempotent
re-apply is closed for counter-bearing Changesets by an **applied
marker** — a row in the app-db-internal `_syzy_applied` table written
inside the same transaction as the DML, and therefore exactly as
durable as the DML itself. On (re)delivery of a Changeset whose marker
is present but whose frontier entry is not, the apply strips the
counter contributions and re-applies only the idempotent remainder
(journal append and clock advance proceed as usual). Markers are
pruned once the frontier persisted by an earlier snapshot pass covers
them — the retention invariant is that any restorable `(app.db,
metadata)` pairing must find a marker for every seq present in the
app.db bytes but absent from the paired frontier.

`_syzy_applied` is local bookkeeping: created lazily on the apply
connection, absent from the replicated catalog, never journaled.

## Pruning Relationship

The durable schema source of truth is the metadata catalog
(`syzy_table`/`syzy_column`); `syzy_schema_event` is a local mirror of
the schema log's events, kept indefinitely. DDL events do not flow
through the per-origin journals, so journal segment retention (see
[PRUNING.md](../../docs/PRUNING.md)) is unaffected by schema activity. The
offline-deadline contract for tombstone GC and journal-segment GC
applies equally to schema-chain catch-up: a peer offline past the
deadline may fall below the schema log's retention horizon and need
`syzy_clone` to recover schema state.

## Supported DDL

Replicated forms (`Append` to schema log, apply on every node):

- `CREATE TABLE` (including inline `UNIQUE` and `NOT NULL UNIQUE`
  constraints and `WITHOUT ROWID`; see [Unique Keys](#unique-keys))
- `ALTER TABLE ... ADD COLUMN`
- `ALTER TABLE ... RENAME TO`
- `ALTER TABLE ... RENAME COLUMN`
- `ALTER TABLE ... DROP COLUMN` when the SQLite version supports it
- `DROP TABLE`
- `CREATE INDEX` / `DROP INDEX`
- `CREATE UNIQUE INDEX` / `DROP INDEX` on a unique index — typed via
  `AddUniqueKey` / `DropUniqueKey`
- `CREATE VIEW`
- `DROP VIEW`
- `CREATE VIRTUAL TABLE`
- `DROP VIRTUAL TABLE` (issued as `DROP TABLE <vtab>`)
- `CREATE TRIGGER` / `DROP TRIGGER` (see [Triggers](#triggers))
- `FOREIGN KEY` cascade actions (`ON DELETE CASCADE`, `ON DELETE SET NULL`,
  `ON DELETE SET DEFAULT`, `ON UPDATE CASCADE`) — rewritten to triggers
  per [Cascade Rewriting](#cascade-rewriting). The FK constraint itself
  remains local-only on the originator.

Views and virtual tables replicate as opaque SQL text — catalog ops do not
rewrite their bodies through stable IDs, so renaming a referenced table or
column breaks them on every replica until the name is restored. Virtual
tables additionally require module symmetry: every replica must have the
named module and configuration options available, or the DDL apply fails
on that replica and its schema catch-up blocks retrying the event (see
[Failure Mode](#failure-mode)). Vtable DML is not captured: preupdate
doesn't fire for the vtab's own row operations, and the shadow-table
writes it triggers (which do fire preupdate) are dropped at drain time
because shadow tables never acquire typed-catalog entries. Use vtables
as derived indexes over replicated source rows and rebuild per replica.

Local-only DDL (committed to SQLite, not replicated, no schema-log `Append`):

- DDL targeting any non-`main` schema (`ATTACH`ed databases, `temp.*`).
- Vtable shadow tables. No special tracking needed: SQLite delivers the
  nested shadow `CREATE TABLE`s run by a module's `xCreate` to trace_v2
  with a `-- ` comment prefix, which classifies as non-DDL and passes
  through local-only. (A user-issued `DROP TABLE` on the vtab itself is
  the opposite case: admission resolves the name via schema lookup and
  replicates it as a vtab drop.)
- `_*` and `sqlite_*` objects.

### SQL preprocessing

Transparently rewritten (admitted, but the SQL SQLite compiles
differs from what the user wrote):

- bare `INTEGER PRIMARY KEY` and `INTEGER PRIMARY KEY AUTOINCREMENT`
  are rewritten to `INT PRIMARY KEY NOT NULL DEFAULT (gen_id('<table>'))`
  on a rowid table. The rewrite preserves multi-writer collision-
  freedom; `last_insert_rowid()` no longer recovers the inserted PK
  (use `RETURNING id`). See
  [ARCHITECTURE.md#schema-rules](ARCHITECTURE.md#schema-rules) for the
  full behavior contract.
- a bare `DEFAULT (gen_id())` (CREATE TABLE columns and ADD COLUMN) is
  table-qualified to `gen_id('<table>')` — the runtime function
  allocates per-table and cannot learn its table at call time. Lets
  schema templates and framework primary-key overrides declare one default.
  A bare call that bypasses the preprocessor (comment-prefixed statements) is
  rejected at admission.

The rewrite point depends on how the client reaches SQLite:

- Syzy's own connections (`sqlite.Open`, `sqlitebridge.Conn`): a SQL
  preprocessor on `Conn.Prepare`/`Conn.Exec`
  (`internal/producer/ddl_rewrite.go`). Multi-statement `Conn.Exec`
  rewrites only the first statement.
- Preload mode: interposers on `sqlite3_prepare{,_v2,_v3}` and
  `sqlite3_exec` (`sqlite/cmd/syzy-ext/autoload_shim.c`) run the same
  preprocessor on statements targeting an attached connection. The
  prepare path rewrites the first statement and repoints `*pzTail`
  into the caller's original buffer, so drivers that loop on the
  tail get every statement rewritten; the exec path rewrites all
  statements up front. Not rewritten (pass through to the admission
  backstop): `sqlite3_prepare16*` (UTF-16), comment-prefixed DDL,
  prepares issued mid-`sqlite3_step` on the same thread
  (SQLite-internal DDL such as fts5 shadow tables — these must keep
  their rowid aliases), and statically-linked SQLite (which the shim cannot
  interpose). Shim packaging and lazy runtime loading:
  [ext/README.md](../../ext/README.md).
- Direct `.load` mode: the host process has already resolved its
  `sqlite3_prepare*` calls before loading the extension, so the interposers
  cannot enter that call path. The extension still validates and replicates
  DDL through the admission hook, but SQL that requires a rewrite is rejected;
  callers must use the explicit rewritten form.

Rejected by default:

- DML *preceding* DDL in a transaction, and DDL under `SAVEPOINT`
  scope (see [Transactional DDL](#transactional-ddl); any number of DDL
  statements followed by DML in one `BEGIN ... COMMIT` is supported),
- `CREATE TABLE AS SELECT`,
- hidden columns (vtable-only construct) on replicated tables,
- eventual (nullable) `UNIQUE` on `BLOB` columns, *eventual* partial
  unique indexes (`CREATE UNIQUE INDEX ... WHERE ...` with a nullable
  member — coordinated partial indexes are supported), and `NOT NULL
  UNIQUE` when no reservation backend is configured in a multi-writer
  cluster — see [Unique Keys](#unique-keys),
- arbitrary table-rebuild migrations expressed as mixed SQL/data movement.
