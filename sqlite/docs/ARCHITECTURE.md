# SQLite engine architecture

This document specifies Syzy's SQLite hook capture, transactional apply,
sidecar state, commit-gated uniqueness, extension/daemon lifecycle, and LTX
recovery.

System-wide contracts are authoritative in the root documentation:

- [System architecture](../../docs/ARCHITECTURE.md)
- [CRDT model](../../docs/CRDT.md)
- [Changeset protocol](../../docs/PROTOCOL.md)
- [Schema replication](../../docs/SCHEMA.md)
- [Transport](../../docs/TRANSPORT.md)

This document owns SQLite-specific ordering and storage. For the product
introduction and current limitations, see [README.md](../README.md) and
[LIMITATIONS.md](LIMITATIONS.md). SQLite DDL admission is in [DDL.md](DDL.md).

## Architecture Overview

Syzy is one Go library with a small C hot-path. Replicated DML state is
split between the user's `app.db`, a set of append-only mmap journals
(one per origin), and an in-memory cache that's the runtime source of
truth for CRDT bookkeeping. A metadata SQLite file holds a periodic
checkpoint of the cache so restarts don't replay history from genesis.

- **Producer**: preupdate/commit/wal/trace_v2 hooks, journal append in
  `wal_hook`, drainer goroutine that builds Changesets and advances
  `nodestate.Cache`, and intent recovery. C owns the preupdate hot loop and
  per-connection buffering; Go owns HLC/`Seq` allocation through the
  Cache, journal append, materialization, and the drainer. C crosses
  into Go once in `commit_hook` and once in `wal_hook`; row-level
  preupdate fires do not cross cgo. Mandatory in any process opening
  `app.db` for replicated writes.
- **Broker**: three loops — the transport `Subscribe` loop, the
  gap-fill fetcher loop, and the schema catch-up loop (the latter
  two spawned only when a `GapFiller` / schema log is configured) —
  all serialized onto the single apply connection by one apply
  mutex. Inbound apply runs through `applyPayloadCache`: cache
  idempotency check, LWW vs `cache.RowState`, app.db DML in one
  BEGIN IMMEDIATE / COMMIT, cache state advance, mirror-journal
  append. The fetcher loop plans missing ranges and pulls them
  through the `GapFiller` chain (peers, then object store — see
  [TRANSPORT.md](../../docs/TRANSPORT.md#peer-catchup-op-0x01)). Pure Go.
  Required for cluster participation; not required for local commit
  correctness.
- **Snapshotter**: a goroutine owned by the node, not the broker. Wakes
  on a tick or explicit `Trigger` and writes the cache's dirty state
  (frontier, row_clock, applied_gaps, sender_next_seq, hlc_last,
  per-origin journal markers) to the metadata inside one `WithTx`. This
  is the only metadata writer for replicated state. Off the hot path.
- **Peer frontiers**: frontier exchange is pull-based. A peer
  answers an `opFrontier` request on the shared mesh listener
  with its per-origin applied frontier;
  `tcpmesh.PeerFrontierSource` aggregates responses and serves as both
  a `TipSource` (peer-mesh tip discovery) and the reaper's
  all-peers-applied liveness signal. See
  [PRUNING.md](../../docs/PRUNING.md#peer-frontiers).

Locally-produced changesets are broadcast directly: the producer's
drainer fires `OnEncoded` on each freshly built changeset, which the
node wires straight to `transport.Broadcast`. There is no broker outbox
loop and no commit-thread metadata write on the publish path.

**Public wiring status.** The public `sqlite.Open`, `syzy daemon`, and
loadable extension wire DML replication and schema-log-backed DDL. In the
linked Go path, `sqlite.Open` owns the in-process control plane and a single
producer writer pool; all `sqlite.NewDB(node)` facades share that pool. In
the extension path, each host SQLite connection is a producer-only writer
and the daemon drains those per-origin journals.

Public APIs expose public value types only. SQLite configuration and status
types belong to `syzy/sqlite`; transport contracts use `crdt` identities and
sequences; `sqlite/syzyext.Attached` exposes only the origin and lifecycle needed by
extension hosts. Producer, metadata, catalog, controller, object-layout, and
publisher implementation types remain internal. Helpers that operate on those
internal representations are internal too rather than presenting signatures
that outside callers cannot implement or construct.

The producer and broker share an in-memory `nodestate.Cache` (one
`sync.Mutex` covering `senderNextSeq`, `hlcLast`, per-origin frontier,
per-origin `applied_gaps`, the `(table, pk) → RowState` map, and
per-origin snapshot markers). The cache is the runtime source of truth; the
metadata is its checkpoint. Producer-to-broker wakeups are best-effort
latency hints; correctness relies on the cache being seeded from the
last metadata snapshot at startup and the journals being replayed past
the snapshot markers. If the broker is unavailable, local reads and
writes continue (producer + cache + journal); remote apply, publish,
gap repair, and pruning pause until it returns.

### Deploy Modes

Same library, two supported packagings:

- **Linked**: producer and broker run in the application process. The
  recommended default for long-lived Go services. The `Node` owns one
  producer writer pool over its hooked SQLite connection; application
  facades are lightweight handles that share that pool.
- **Extension + metadata**: the producer loaded as a SQLite extension into
  any client (Python, CLI, polyglot apps); the broker run separately as
  `syzy daemon` against the same `app.db`/`app.db-syzy`. Use when you don't
  own the writer process, when replication must outlive app restarts, or
  when several writers per host share one broker.

Recovery, durability, and crash semantics are identical in both modes —
they're determined by the metadata protocol, not the packaging.

The local commit pipeline in one breath: the C preupdate hook
records touch evidence per row; the app commits and SQLite fsyncs
the WAL; `wal_hook` (the one cgo crossing) stamps the HLC and
appends one durable record to the self-origin journal; the drainer
goroutine decodes the evidence off-thread, builds Changesets, fires
`OnEncoded` → `transport.Broadcast`, and advances the cache. No
metadata transaction on the DML path. Full hook-by-hook detail and
the ordering invariants: [Local Commit](#local-commit).

All writers — local commits and inbound apply — serialize through the
in-memory cache mutex (briefly: a few microseconds of map updates) plus
SQLite's own writer lock for app.db. The DML pipeline takes no metadata
intent: the journal record is the durability primitive, and the cache +
snapshotter handle persistence. The `LocalDDL` intent kind (see
CRDT.md#intents) covers DDL admission. Clone uses an implicit
WAL-writer-slot barrier on the source instead of a persistent intent
record — see [Bootstrap & Repair](#bootstrap--repair).

A crash after a journal record is durable but before the snapshotter
catches up leaves the journal containing records past the last
`markers[origin]`. On restart the cache is seeded from the metadata
snapshot, then the producer's drainer replays the self journal past
`markers[self]` (advancing the cache without re-broadcasting; see
[Recovery](#recovery)) and `nodestate.RecoverMirror` replays each peer
mirror journal. App.db UPSERTs are idempotent; cache state catches up
without divergence. Default `synchronous=NORMAL` on both `app.db` and
the metadata prioritizes commit performance; use `synchronous=FULL` on
`app.db` for stronger crash durability. See
[Host-Level Desync](#host-level-desync).

## Storage

### File Layout

```text
app.db                            -- user data (SQLite WAL)
app.db-wal
app.db-shm

app.db-syzy/
  daemon.lock                     -- daemon-role flock
  metadata.db                      -- metadata checkpoint (SQLite WAL)
  metadata.db-wal
  metadata.db-shm
  notify.feed                     -- daemon-published invalidation feed
  origins/
    <origin-hex>/                 -- origin-role flock
      journal/
        seg-00000000.bin          -- append-only waitable records
        seg-00000001.bin
        ...
  mirror/
    origin_<origin>/              -- one mirror journal per origin
      seg-00000000.bin            --   (decimal origin; the self origin's
      ...                         --   entry is the self-log)
```

`app.db` must run in WAL mode. `syzy_open` enables WAL or fails clearly.
Litestream-class WAL tailers remain compatible with `app.db`. When a
publisher tails a database, it owns WAL recycling: the coordinated pass
drains the tailer, verifies a PASSIVE checkpoint's frame counts, then runs
the recycle write as one write transaction that revalidates the drained
generation under SQLite's write lock before committing. SQLite restarts
the fully-backfilled WAL inside that commit — rewinding the write position
without shrinking the file (`journal_size_limit` stays unset: commit-tail
truncation runs while readers are live, and a stale wal-index view of a
truncated file reads sparse holes as zero pages that a checkpoint would
backfill over page 1; only `wal_checkpoint(TRUNCATE)`, which holds every
read slot exclusively, may shrink a WAL) — and the commit's recorded frame
count plus the header salts prove the outcome, because nothing can move
between the validation and the commit (`ltxstream.CheckpointUnderLock` is
the contract). Any uncoordinated restart is caught by that validation or the
tailer's resume salt check and forces a loud rebaseline — safe, but not
free — so auto-checkpointing on published databases is disabled (host
writer) or demoted to a high emergency backstop (other openers).

Metadata pragmas: `journal_mode=WAL`, `synchronous=NORMAL`, and
`wal_autocheckpoint` set to a high backstop threshold (the host process
owns metadata WAL recycling; see above). `journal_size_limit` stays unset
for the same no-commit-tail-truncation reason as `app.db`. The
metadata holds the periodic snapshot of in-memory CRDT state and is not on
the commit hot path; NORMAL is enough.

The metadata schema keeps released columns as inert reserved fields until a
coordinated file-format migration can rebuild the table. Removing a runtime
feature does not by itself make an existing metadata file unreadable or prevent
the immediately preceding binary from reopening it during a failed handoff.

`metadata.db` is the only recognized metadata filename; Open creates a
missing store and does not rename older files. Additive nullable/defaulted
columns are migrated in place via one-shot `ALTER TABLE` on Open (dormant
stores of arbitrary age can wake onto a current build), with no
`schema_version` bump; a `schema_version` mismatch fails startup and the
store must be rebuilt through clone/bootstrap.

Each origin has one append-only journal directory. The self-origin
journal records local `wal_hook` evidence (KindLocalDML or KindEmpty);
mirror records use the same segment format with KindMirror payloads.
Each segment is a fixed-size mmap with its size stored in the segment
header. New segments use a 1 MiB target by default, but segment files may
vary in size: if a single journal record would not fit in the target
segment plus the required trailing seal reserve, the writer rotates to a
one-record segment sized for that record. Records are one-shot published
cells: the first 4-byte word is zero while the writer fills the record,
then a final atomic store publishes the nonzero kind word.
Cross-process drainers wait on that word with futex; the in-process
drainer also receives the existing coalesced Go-channel wake. Rotation is
represented by a published KindSeal record, so readers follow the record
stream across segments instead of polling directory state as the live
protocol. The writer creates the successor segment before publishing the
seal, so readers never allocate journal segment files. Recovery still
scans segments from disk and stops at the first zero, torn, or invalid
tail record. Segments before a durable snapshot marker are unlinked by
the snapshotter when GC is enabled.

Replicated table discovery ignores `sqlite_*` and `_*` tables and vtable
shadow tables (see [DDL.md](DDL.md#supported-ddl) for shadow detection).

**Why a separate metadata (decided, do not relitigate).** Collapsing
`_syzy_*` into `app.db` would buy single-WAL crash atomicity (app +
metadata in one commit) but pushes replication churn into `app.db` and
breaks the "nothing extra injected into your db" property. Syzy leaves
`app.db`'s schema alone with one narrow exception:
the `_syzy_applied` counter applied-marker table
([DDL.md](DDL.md#counter-columns)), created lazily only when a counter-bearing
changeset first applies — local bookkeeping, never replicated or
journaled. All other replication state lives in the metadata. Recovery
from metadata loss is `syzy_clone`.

### Metadata Schema

The metadata is the periodic checkpoint of CRDT state. The full DDL is
in [`internal/metadata/schema.go`](../../internal/metadata/schema.go); this
section describes the surface. There is no log table — replicated DML
payloads live in per-origin journals on disk (see
[File Layout](#file-layout)).

**Replicated state** — snapshots of in-memory
`nodestate.Cache` written periodically by the snapshotter:

- `meta(key TEXT PRIMARY KEY, value BLOB)` — keyed scalars and packed
  blobs: `cluster_id` (16-byte UUID), `node_id`, `hlc_last`,
  `schema_seq`, `clean_shutdown`, plus packed blobs for
  `applied_gaps` (`map[origin]SeqSet` of seqs above the contiguous
  frontier) and `snapshot_markers` (`map[origin]offset` per-origin
  journal offset reflected in this snapshot). DDL intents live in
  origin-scoped slots — one `intent:<origin-hex>` key per origin,
  not a single global key; encoding is
  `kind || started_at_us || payload` (see CRDT.md#intents). The
  `IntentClone` kind is reserved but not currently persisted by any
  code path — clone uses an in-memory WAL writer-slot barrier instead
  (see [Bootstrap & Repair](#bootstrap--repair)).
- `sender_seq(origin, next_seq)` — next sequence to allocate for every
  locally drained origin.
- `frontier(origin, last_seq, last_hlc)` — highest contiguous applied
  sequence and its Stamp.Clock per origin. Precisely, the frontier is
  a local-durability watermark, not an applied watermark: for every
  origin, every seq ≤ frontier is either **applied to the database**
  or **resident in `apply_quarantine` with its exact payload bytes**,
  so no redelivery from peers or object store is ever needed to drain
  the quarantine.
- `row_clock(table_id, pk_blob, cl, base_hlc, base_origin)` — per-row
  CL + baseline Stamp. CL parity encodes liveness (odd=live,
  even=tombstoned, 0=never existed); see
  CRDT.md#causal-length-cl. Tombstones share the table; no boolean.
- `cell_clock(table_id, pk_blob, column_id, hlc, hlc_origin)` — sparse
  per-column Stamp overrides on `row_clock`'s baseline. Effective
  Stamp falls through cell_clock → row_clock baseline; absent entries
  inherit the row baseline. Written when a column needs independent
  ordering — UNIQUE arbitration loser-null and blob-patch interaction
  in the current build; explicit column-LWW emission and schema
  evolution are future additions. See
  [DDL.md](DDL.md#sparse-clock-groups).

**Apply quarantine** —
`apply_quarantine(origin, seq, payload, err, first_seen, attempts)`
holds inbound changesets whose apply failed deterministically, parked
with their exact payload bytes. Written by the broker at quarantine
time, not a cache snapshot; see
[Localized Failures](#localized-failures) for the semantics.

**Schema catalog** — four tables committed synchronously by DDL apply
(DDLs are rare; a metadata tx on the writer thread is acceptable).
Their layout is specified in [DDL.md](DDL.md#metadata-catalog):
`syzy_table`, `syzy_column`, `syzy_key` (PK lives at the all-zero
key_id sentinel; unique keys at their own key_ids), and
`syzy_schema_event` (local mirror of the schema log's events). Plus
`syzy_synth_trigger` for the cascade-rewrite bookkeeping.

**Implicit defaults.** A row with no `row_clock` entry has implicit
`(cl=0, base_hlc=0, base_origin=0)` (never existed); the first write
produces an explicit entry. `syzy_init` does not seed pre-existing
rows. Columns with no `cell_clock` entry inherit the row's baseline
Stamp.

**Specified, not yet implemented.** The diagnostics table exists on
paper but the schema doesn't carry it yet (no consumer). Listed
verbatim here so a future implementation matches:

```sql
CREATE TABLE apply_issue (
  id INTEGER PRIMARY KEY,
  detected_at_us INTEGER NOT NULL,
  origin INTEGER,                  -- nullable for local-only failures
  sender_seq INTEGER,              -- nullable for local-only failures
  reason TEXT NOT NULL,            -- 'ddl_apply_failed' |
                                   -- 'record_apply_failed'
  table_id BLOB,                   -- nullable for changeset-wide failures
  pk_blob BLOB,                    -- nullable for changeset-wide failures
  detail TEXT                      -- SQLite error text or operator note
) STRICT;
CREATE INDEX apply_issue_recent ON apply_issue(detected_at_us);
```

Opportunistic local collapse deletes `cell_clock` overrides redundant
with `row_clock`'s baseline; the rest are retained indefinitely. Full
frontier-driven stabilization is a future addition.

SQLite `INTEGER` is signed 64-bit. All ID fields (`node_id`,
`sender_next_seq`, `last_seq`, `last_hlc`, `hlc`, `origin`, `cl`) are
restricted to `[0, 2^63)`; wire format reserves the high bit zero. HLC
fits by construction (47-bit ms ≈ 4458 years from Unix epoch + 16-bit
logical counter, packed as `(ms << 16) | logical`); `node_id`
generation masks the high bit.

`row_clock` stores per-row CL plus baseline Stamp; tombstones are encoded
as even `cl` (no separate boolean). The convergence state for a remote
seq is "applied" iff it lies within the contiguous frontier or in
`applied_gaps[origin]`; idempotency lives entirely in the cache (see
[Apply Path](#apply-path)). Tombstone GC, journal segment GC, and the
peer-frontier safe_seq mechanism are in [PRUNING.md](../../docs/PRUNING.md).

Incremental BLOB replication adds a `blob_range_clock` metadata table —
[BLOB_PATCH.md](BLOB_PATCH.md).

## Changeset Protocol

Syzy emits and consumes the canonical changeset bytes specified in
[the protocol](../../docs/PROTOCOL.md). SQLite hook evidence is
materialized into those records without a SQLite session-extension encoding,
and inbound records map stable catalog IDs back to the current SQLite names.

The protocol defines the changeset header, DML record layout, canonical value
classes, dependency encoding, and HLC packing. This document defines SQLite
capture and apply ordering around those bytes.

## Primary Keys

### Canonical Encoding

`row_clock.pk_blob` is a stable canonical encoding of the PK column IDs and
values. It is independent of column names, so PK column renames do not change
row identity:

```text
for each PK column in create-time PK order:
  column_id (16 bytes)
  type tag (1 byte)   -- 1=INTEGER, 2=REAL, 3=TEXT, 4=BLOB
  varint byte length
  canonical bytes
```

There is no header — the blob is just the concatenated per-column
entries; decoding walks entries until the bytes are exhausted.

- **INTEGER:** 8-byte two's-complement big-endian.
- **REAL:** IEEE 754 binary64, big-endian.
- **TEXT:** UTF-8 bytes.
- **BLOB:** raw bytes.
- **NULL:** rejected.

Recreating a table with the same visible name allocates a new
`table_id`, so its PK space is distinct from the old table's
tombstoned ID. (SQLite itself disallows in-place PK changes — no
`ALTER` form drops a PK column or adds a column with a column-level
`PRIMARY KEY` — so "change the PK" means the user-level rebuild dance
against a fresh `table_id`.)

### Schema Rules

Auto-allocated INTEGER PKs would collide under multi-writer LWW:

- **Bare `INTEGER PRIMARY KEY` (rowid alias)** — SQLite hole-fills
  `max(rowid)+1`; independent writers collide silently.
- **`INTEGER PRIMARY KEY AUTOINCREMENT`** — `OP_NewRowid` floors
  auto-allocation at `max(rowid)+1` (SQLite `src/vdbe.c`), so once a remote
  rowid is applied locally, the next local auto-allocation lands in the
  remote node's partition. Pinning `sqlite_sequence` does not fix this; the
  floor is unconditional.

Syzy's SQL preprocessor avoids those collisions by rewriting
`<col> INTEGER PRIMARY KEY [AUTOINCREMENT]` into
`<col> INT PRIMARY KEY NOT NULL DEFAULT (gen_id('<table>'))` on a
rowid table — switching from `INTEGER` to `INT` breaks the rowid-alias
rule, so the `gen_id` DEFAULT actually fires for omitted-id inserts.
The exact eligible syntax and the integration paths that run the preprocessor
are defined in [SQL preprocessing](DDL.md#sql-preprocessing). The implementation
lives in [`ddl_rewrite.go`](../../internal/producer/ddl_rewrite.go).

Behavior contract for rewritten tables:

- `INSERT ... RETURNING id` returns the assigned `gen_id` value
  (recommended; modern ORMs use this by default).
- `sqlite3_last_insert_rowid()` returns the locally allocated rowid,
  not the assigned `id`. Apps depending on `last_insert_rowid` to
  recover the inserted PK must switch to `RETURNING`.
- IDs are sparse partitioned int63s, not a global insertion sequence, and
  allocation starts at or above 2³³.
- `PRAGMA table_info` reflects the rewritten column type (`INT`) and
  the injected `DEFAULT (gen_id('<table>'))`.
- AUTOINCREMENT is silently dropped; `sqlite_sequence` is not created.
- `blob_patch` still works on rewritten tables (they remain rowid
  tables).

All other explicit PKs are fine, including on rowid tables — the implicit
rowid is local-only and may diverge across nodes; replication keys off the
explicit PK columns. `blob_patch` looks up the local rowid via PK at apply
time.

### Recommended Patterns

Default on paths with SQL preprocessing — ordinary ORM-compatible auto-integer
PK:

```sql
CREATE TABLE event (
  id      INTEGER PRIMARY KEY,
  ts      INTEGER NOT NULL,
  payload TEXT NOT NULL
);
```

The SQL preprocessor rewrites this to the collision-free `INT PRIMARY KEY NOT NULL
DEFAULT (gen_id('event'))` form described above. `AUTOINCREMENT` is accepted
too, but is dropped by the rewrite. Use `INSERT ... RETURNING id`; the assigned
IDs are sparse partitioned int63 values, and `last_insert_rowid()` reports the
local rowid rather than this PK.

Explicit integer form for clients that do not preprocess SQL:

```sql
CREATE TABLE doc (
  id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('doc')),
  body BLOB
);
```

Compact integer PK:

```sql
CREATE TABLE inode (
  ino INTEGER PRIMARY KEY DEFAULT (gen_id('inode')),
  ...
) WITHOUT ROWID;
```

Use `INT PRIMARY KEY NOT NULL` on rowid tables when `blob_patch` is needed.
Use `WITHOUT ROWID` for compact integer-keyed rows.

Use `BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7())` when IDs should be opaque,
time-ordered, or usable outside Syzy's integer allocator.

### `uuidv7()`

A custom SQL function (RFC 9562: 48-bit ms time prefix + version/variant
bits + 62 bits random) registered on every connection opened through
`sqlitebridge.Open` (which includes the producer's writer connection and
the apply connection). Time-ordered for b-tree locality, collision-resistant
across nodes without coordination — 62 bits of entropy per ms gives ~2^31
ids/ms before birthday collision. The ms prefix is the local wall-clock
millisecond — DEFAULTs run during statement execution, before `commit_hook`
reserves the changeset HLC, so the HLC isn't available here. A per-process
12-bit `rand_a` counter keeps ids monotonic within a millisecond and borrows
from the future on overflow. No metadata state. Apply paths carry explicit
values and never invoke the function.

Implementation: [sqlitebridge/funcs.c](../../sqlitebridge/funcs.c) +
[funcs.go](../../sqlitebridge/funcs.go).

### `gen_id()`

`gen_id(table_name)` uses a local 30-bit random partition plus a 33-bit
in-memory counter. The allocator probes the table before selecting a partition,
so concurrently active nodes occupy disjoint positive-int63 ranges without a
network round trip. The table name is qualified by preprocessing because the
SQLite scalar function cannot infer which DEFAULT invoked it.

Implementation: [sqlitebridge/funcs.c](../../sqlitebridge/funcs.c) +
[funcs.go](../../sqlitebridge/funcs.go).

The native-schema reader recognizes the strict forms `gen_id('t')` and
`uuidv7()` in `PRAGMA table_xinfo(t).dflt_value` and stamps them onto
`catalog.Column.PKDefault` (not persisted; re-derived after metadata-only
reloads). The SQL preprocessor qualifies bare `gen_id()` calls with the table
name; DDL admission rejects an unprocessed bare call or a `gen_id('X')` whose
literal does not match the table being defined.

## Write Path

### Capture

Per-connection touch journal in C, populated by the preupdate callback. It
records enough evidence to materialize the transaction's final row effects
after commit; it is not the wire changeset. Each preupdate fire:

1. If `zDb != "main"`, skip buffering (replication covers the `main` database
   only; ATTACHed and `temp` databases are not replicated).
2. If table name starts with `sqlite_` or `_`, skip buffering (non-replicated
   table). Return without setting the reject flag — the write commits as a
   local-only mutation.
3. If `sqlite3_preupdate_depth() > 0`, skip buffering. Trigger-induced
   and cascade-induced writes are derived effects; every replica
   re-derives them when the source row is applied.
4. If `sqlite3_preupdate_blobwrite() >= 0`, copy the OLD blob bytes into the
   blob-write buffer for deferred encoding in Go (see
   [BLOB_PATCH.md](BLOB_PATCH.md#capture)). Skip the normal DML touch path.
5. If op = UPDATE and any PK column's OLD vs NEW bytes differ, buffer
   the row as DELETE-touch at OLD pk_blob (full OLD row) plus
   INSERT-touch at NEW pk_blob (full NEW row), in that order. Skip the
   normal single-touch path. (`WITHOUT ROWID` PK updates already fire
   as separate DELETE+INSERT preupdates; this branch makes rowid
   tables behave the same.)
6. Encode the PK. If this is the first touch for `(table, pk)`, store:
   INSERT → PK only; UPDATE/DELETE → OLD full row. Skip generated
   (STORED or VIRTUAL) columns from the captured image — receivers
   recompute them locally from the row's source columns.
7. If the table has a **coordinated** unique key and this op writes one
   of its member columns, copy the `NEW` member values (and, for the
   prior value, the `OLD`) into the per-txn coordination buffer keyed by
   `(table_id, key_id, pk)`. `commit_hook` folds this buffer to net
   claims and reserves them (see [Coordinated Uniqueness](#coordinated-uniqueness)).
   `blob_write` against a coordinated-unique column sets the reject flag
   — coordinated keys are whole-value.

`commit_hook` checks the reject flag at entry and returns nonzero if set,
aborting the txn before any metadata work.

At wal-hook time, Go reads the touch/blob journals and current app rows for the
touched PKs:

- INSERT-first + row exists → full-row insert/upsert image.
- INSERT-first + row absent → no-op.
- UPDATE/DELETE-first + row exists and differs from OLD → changed-column
  update image.
- UPDATE/DELETE-first + row exists unchanged → no-op.
- UPDATE/DELETE-first + row absent → delete.

Rows that materialize no effect are omitted (irredundant deltas, per
CRDT.md#materialization). This deliberately replicates net row-state
changes, so SQLite statement rollbacks, savepoint rollbacks, and no-op
updates collapse naturally. Every record that remains shares one Stamp;
receivers treat equal Stamps as already applied.

**Column metadata** at capture: column count and values come from
`sqlite3_preupdate_count()` and `sqlite3_preupdate_old`/`_new`. Column
names, stable `table_id`/`column_id`, PK indices, BLOB indices, and
generated-column flags come from the metadata schema catalog plus
`pragma_table_xinfo`, invalidated on DDL commit. Generated columns
(SQLite enforces deterministic expressions) are excluded from the
captured image and the wire record; receivers' SQLite recomputes them
from the source columns. Virtual-table DML is silently uncaptured
because preupdate doesn't fire for it (see [Sharp Edges](#sharp-edges)).

**Savepoints:** rollbacked touch records are harmless because materialization
compares against current app rows. Syzy may still truncate touch/blob buffers
on `ROLLBACK TO` as an optimization.

Rollback hook clears the touch/blob buffers entirely.

### DDL Capture

DDL does not fire `preupdate_hook`. The producer installs
`sqlite3_trace_v2(SQLITE_TRACE_STMT)` on the writer connection. The
trace_v2 hook is the entire local-DDL admission path: it classifies the
statement against the metadata catalog, calls `SchemaLog.Append`
synchronously, writes the `LocalDDL` intent, and then allows SQLite to
execute the DDL body. On any failure (`ErrHeadMoved`, schema-unhealthy
node, metadata I/O error), trace_v2 sets the txn reject flag and calls
`sqlite3_interrupt(db)`, aborting the statement before its body runs
(verified empirically against SQLite 3.46.1: constant ~3 VDBE opcodes
for every supported DDL form). See
[DDL.md](DDL.md#direct-local-ddl-flow) for the full flow.

DDL is classified by affected object. Non-`main` schemas (ATTACHed,
`temp`), `sqlite_*`/`_*` objects, and vtable shadow tables commit
locally without invoking the schema log. Shadow-table DDL needs no
special tracking: SQLite delivers trace_v2 STMT callbacks for nested
statements (the shadow `CREATE TABLE`s run by a vtab module's
`xCreate`) with a `-- ` comment prefix, which classifies as non-DDL
and passes through local-only. Indexes on replicated tables are handled
by the table they index, not by index name. `CREATE VIEW`/`DROP VIEW`
and `CREATE VIRTUAL TABLE`/`DROP TABLE`-on-vtable replicate as opaque
SQL text (a vtab drop arrives as plain `DROP TABLE`, so admission
upgrades it via schema lookup — vtabs never enter the typed catalog). DDL inside an explicit `BEGIN` is admitted — any number of statements,
all before any DML, resolved against a transaction-local catalog
overlay and published as one schema event (an `OpBundle` past the
first); the schema-log Append is deferred to `commit_hook`, so an
explicit ROLLBACK replicates nothing and a CAS loss aborts the COMMIT.
DDL under a SAVEPOINT scope, DDL after DML, and other unsupported forms
set the reject flag without calling Append.

`commit_hook` is the codegen-evolution backstop: reject flag → return
nonzero. On the happy DDL path the intent is already present;
commit_hook returns 0 and the wal_hook DDL branch runs `resolve_intent`.

`rollback_hook` clears the `LocalDDL` intent if SQLite rolls back the
statement after it was written. Append has already committed
cluster-wide; the originator converges via the catch-up path.

### Local Commit

The committer thread runs the SQLite hook stack; the drainer
goroutine builds and broadcasts changesets off-thread. Implementation
in [`internal/producer/producer.go`](../../internal/producer/producer.go)
(`walHook`, `traceHook`, `commitHook`) and
[`internal/syncer/sink.go`](../../internal/syncer/sink.go) (`Apply`).

**Hook stack, per row / per statement / per commit.**

1. `preupdate_cb` (C, no cgo) appends evidence to the per-connection
   touch/blob buffers. See [Capture](#capture) for filter rules.
2. `trace_v2` (statement start, replicated DDL only) classifies the
   SQL, calls `SchemaLog.Append`, and writes a `LocalDDL` intent
   into this origin's slot (`meta."intent:<origin>"`). On any failure
   it sets the txn reject flag and
   `sqlite3_interrupt`s the statement. See
   [DDL.md#direct-local-ddl-flow](DDL.md#direct-local-ddl-flow).
3. `commit_hook` (C): if the txn touched a **coordinated** unique key,
   cross into Go, fold the per-txn coordination buffer to net
   `(pk, key) → value` claims, and `Reserve` them against the
   leaseholder in one batched round-trip. A conflict or unavailable
   leaseholder sets the reject flag. Then return nonzero iff the reject
   flag is set, surfacing as `SQLITE_CONSTRAINT_COMMITHOOK` to the app.
   The Go error also wraps `unique.ErrConflict` or
   `unique.ErrUnavailable`, preserving the reason that SQLite's commit
   hook return value cannot encode; DDL rejects share the SQLite code
   but carry neither coordinated cause.
   DML without a coordinated key never crosses into Go here and never
   sets the flag. See [Coordinated Uniqueness](#coordinated-uniqueness).
4. App commits; SQLite fsyncs the WAL.
5. `wal_hook` (one cgo crossing into Go):
   - **DDL branch**: `resolve_intent` runs idempotent
     `apply_catalog_op` against the metadata, advances
     `meta.schema_seq`, and clears this origin's intent slot — all in
     one metadata tx. A `KindEmpty` journal record keeps the self-origin sequence
     dense.
   - **DML branch**: `cache.StampHLC` allocates the HLC under the
     cache mutex; `journal[self].Append` writes one record carrying
     the raw touch buffer (kind = `KindLocalDML` if non-empty, else
     `KindEmpty`); the record's kind word is published last, which
     wakes in-process and cross-process drainers.
6. `rollback_hook` clears the touch/blob buffers. If trace_v2 wrote a
   `LocalDDL` intent and SQLite then rolls back the DDL body in-process,
   the intent is cleared (Append already committed cluster-wide; the
   originator catches up via the inbound path).

**Drainer goroutine** walks records from `drainedOffset`; if it reaches
a zero publish word it parks on that mmap word, and if it sees KindSeal
it advances to the next segment. For each non-empty record it decodes the
touch evidence into `crdt.Record`s, allocates `cache.AllocSelfSeq()`,
calls `crdt.Build`, fires `OnEncoded(cs.Encoded())` (the node has wired
this to `transport.Broadcast`), and writes the per-row
`cache.PutRowState` updates. Finally it advances
`cache.SetSnapshotMarker(self, endOffset)` and fires `OnCommit`
listeners. The drainer never unlinks journal segments — `RetainAfter` is
the snapshotter's job.

**Invariants the order preserves.**

- Journal record durability ≤ one OS-flush quantum behind app.db
  (wal_hook fires after WAL fsync). Recovery replays records past the
  last snapshot marker.
- The DML path holds no metadata intent. The journal record is the
  durability primitive; the cache + snapshotter handle persistence.
- `cache.hlcLast` is monotonic — reusing a wall-clock ms increments
  the logical counter rather than going backward.
- Checkpointing is WAL housekeeping, not recovery evidence; Syzy may
  PASSIVE-checkpoint the metadata past a frame-count threshold.
  Litestream-style WAL tailers remain compatible.

Local-only `main` commits (non-replicated `_`/`sqlite_` tables) flow
through wal_hook as a `KindEmpty` journal record; the drainer skips
them.

## Apply Path

### Inbound Apply

Apply runs on a dedicated connection (`AppApply`) with no producer
hooks installed. Public `Open` configures it with `PRAGMA
synchronous=NORMAL` and `PRAGMA busy_timeout=5000`. It does not enable
foreign-key enforcement; SQLite's default is normally OFF. Triggers
remain enabled on inbound apply so replicated source rows re-derive the
same deterministic trigger/cascade side effects on every replica. Local
app connections may still enforce FKs for local commits.

The broker's subscribe loop calls `applyPayload` per delivered
payload. Implementation in
[`internal/broker/apply.go`](../../internal/broker/apply.go) (`applyPayload`,
`applyPayloadCache`, `applyRecordsLWW`); the subscribe, gap-fill
fetcher, and schema catch-up loops all serialize on the broker's
apply mutex, so inbound apply is single-threaded per node. No
metadata tx on the hot path — idempotency, LWW, and frontier advance
all happen in `nodestate.Cache`; persistence is the snapshotter's
responsibility.

**Phases per changeset**, in order:

1. **Decode.** `crdt.Decode` parses the payload.
2. **Reject defensive cases.** `cluster_id` mismatch is an error;
   self-origin payloads are dropped.
3. **Idempotency check.** `cache.IsAppliedRemote(origin, seq)` returns
   true iff the seq lies within the contiguous frontier or
   `applied_gaps[origin]`. Hit ⇒ return nil (already applied).
4. **Schema-chain gate.** If `cs.Deps[SchemaChain] > meta.schema_seq`,
   keep the delivered payload in the subscribe loop and retry after
   catch-up. (See [Not Yet Implemented](#not-yet-implemented) for the
   planned `applied_gaps` extension.)
5. **DML.** One `BEGIN IMMEDIATE / COMMIT` against AppApply. Per
   record: schema-gate if `table_id` is unknown, skip if the table is
   known dropped, LWW skip if `cache.RowState` dominates the record's
   `(CL, Stamp)`; otherwise stmt-step the cached UPSERT or DELETE
   (UPDATE shapes vary so they are prepared one-shot).
6. **Cache advance** under the cache mutex: `PutRowState` for each
   winning record, then `MarkApplied(origin, seq, hlc)` (adds seq to
   `applied_gaps`, promotes the contiguous prefix to the frontier,
   pulls `hlc_last` forward to MAX).
7. **Mirror append.** Bounded chan send into the per-origin writer
   goroutine; on success advance `cache.SetSnapshotMarker(origin,
   journal[origin].Head())`.
8. **Notify.** `fireApplied(origin, seq)` and
   `fireApplyRecords(origin, seq, records)`; the node converts records
   into table-name keyed notify slots for `Node.Subscribe`.

**Mirror-then-applied ordering.** The sequence above (app.db commit →
cache advance → mirror append → notify) survives a crash between
app.db commit and mirror append: the cache loaded from the next
snapshot won't include this seq, and the peer's re-broadcast lands as
an idempotent app.db UPSERT (`ON CONFLICT DO UPDATE` preserves
receiver-only columns) plus a fresh mirror append. No corruption; on
a sustained crash loop the mirror journal of an origin can lag
app.db.

There is no apply-state FSM. An apply either succeeds, returns nil
(idempotently skipped, deterministically ignored, or durably parked
in `apply_quarantine` — see
[Localized Failures](#localized-failures)), or returns an error for
the transport to retry. Hot path: one app.db WAL fsync, two short
cache-mutex sections, one bounded chan send. ~12–15 µs per apply on
the bench fixture.

Inbound DML applies out of `Dot.Seq` order by design. Missing base rows
auto-create via column-wise UPSERT for insert/update; DELETEs against
missing rows still issue the DML statement (idempotent on absent rows)
and `cache.PutRowState` records the tombstone. `blob_patch` records
apply through the per-byte range-clock path, and full DML against a
row with active `blob_range_clock` entries reconciles through the
same `IntervalMap` (see [BLOB_PATCH.md](BLOB_PATCH.md#apply)). `Dot.Seq` is for
idempotency, retention, and frontier advance; semantic conflict
resolution is `(CL, Stamp)` plus the sparse cell/blob layers.

`ON CONFLICT DO UPDATE` (not `INSERT OR REPLACE`) preserves receiver
columns the originator did not carry; stable column IDs keep
old-schema DML on a newer schema deterministic (see [DDL.md](DDL.md)).

Apply outcomes (success, deterministic ignore, quarantine, error →
retry) are detailed in [Failure Handling](#localized-failures).

### Conflict Resolution

Per CRDT.md: incoming `(CL, Stamp)` vs the effective local
`(CL, Stamp)`, lex on `(CL, Stamp.Clock, Stamp.Origin)`. The effective
local Stamp is `Ranges[col].At(range)` if present, else `Cells[col]`,
else `Base` (RowState fall-through; see CRDT.md#layer-composition).
Strictly greater wins; ties and lower lose. Implemented inline (no
`xConflict` callback abstraction).

DELETE bumps `cl` to the next-higher even value (tombstone); the
`(CL, Stamp)` order means the tombstone dominates lower-CL writes.
A future INSERT bumps `cl` again to the next-higher odd value
(resurrection). Per-cell and per-byte-range overrides are scoped to a
specific CL value; bumping CL implicitly tombstones prior-generation
overrides without explicit GC (../../docs/CRDT.md#causal-length-cl).

Blob columns layer per-byte LWW on top — see [BLOB_PATCH.md](BLOB_PATCH.md).

Counter columns (DDL.md#counter-columns) opt out of Stamp arbitration
entirely: within a row generation their contributions sum, gated on CL
only (../../docs/CRDT.md `F_counter`).

Foreign keys are local-only — see [Sharp Edges](#sharp-edges).

## Coordinated Uniqueness

A `NOT NULL UNIQUE` key (see [DDL.md](DDL.md#unique-keys)) is enforced
*before* the local commit by a synchronous global reservation, so the
value is unique cluster-wide **by construction** rather than reconciled
after the fact. This is syzy's one CP operation; every other write stays
AP.

### The model

One idea carries the design: **the replicated rows are the durable
source of truth for who owns a value; the leaseholder is soft state — a
serialization cache over the rows.** Because replication is full (every
node holds every row), the taken-set for a coordinated key is exactly
the union of that key's SQLite UNIQUE index across the cluster, so the
leaseholder reconstructs it from rows and never needs a private durable
store.

Safety rests on three independently-simple facts:

1. **One leaseholder per generation.** The `unique.Registry`
   leaseholder role is held through a lease object mutated by ETag-CAS,
   carrying a monotonic `Generation` fencing token and a
   heartbeat-renewed expiry. A successor takes over once the lease expires;
   the `Generation` bump fences the prior holder.
2. **Reserve-before-commit.** `commit_hook` reserves the txn's net
   coordinated values against the leaseholder, then the commit proceeds;
   the grant becomes durable as the committed, replicated row. A crash
   between reserve and commit leaves at most a row-less grant — a safe
   leak (a value blocked, never a duplicate), reclaimed by GC.
3. **Rebuild-from-rows on takeover.** A new leaseholder fences the old
   generation, waits the bounded-staleness drain so every committed
   grant from the prior generation has replicated in, rebuilds `taken`
   from its local indexes, and resumes. Coordinated writes pause for
   that window (bounded, rare), surfacing a retryable "unavailable"
   error — never a silent conflict.

The commit hook preserves that decision at the application boundary. A
reservation conflict wraps `unique.ErrConflict` and is final; backend
unavailability wraps `unique.ErrUnavailable` and may be retried after the
writer has been released. Both retain `SQLITE_CONSTRAINT_COMMITHOOK` as the
underlying SQLite error, so low-level SQLite diagnostics remain accurate.

### Registry interface

`unique.Registry` is the contract every backend implements, mirroring
[`schemalog.Log`](../../schemalog/log.go)'s backend pluralism:

```go
type Registry interface {
    // Reserve atomically claims every (Table, Key, Value) in claims. Each
    // Claim carries its own Owner (PKBlob), so one batch covers a
    // transaction touching several rows. ok=false names the first
    // conflicting claim; a non-nil err is an unavailable leaseholder
    // (retryable). A claim already held by the same owner is an idempotent
    // success (replay / re-assert).
    Reserve(ctx context.Context, claims []Claim) (ok bool, conflict *Claim, err error)
    // Release relinquishes claims when the owning row is deleted or its
    // value changes. A released value re-enters the free pool only after a
    // release hold (held until the release is cluster-stable).
    Release(ctx context.Context, claims []Claim) error
}
```

Backend selection is by deployment: with object storage a node elects a
**leaseholder** (one mesh round-trip, no object-store I/O on the hot
path); without a bucket, an in-process backend (`unique.Local`) serves
the single-writer case. A secondary producer without lease-store or mesh access
may instead set `SYZY_UNIQUE_DIAL` to a `unix:` or `vsock:`
`unique.ServeProxy`; the proxy carries only claims, leaving lease discovery and
fencing in the serving node. Its `UniqueProxy` net/rpc stream is capped at 64
MiB per call, and transport/backend failures become `ErrUnavailable`. The
extension probes the service before attach; an absent endpoint keeps new
coordinated DDL disabled, while an existing coordinated catalog without a
registry makes producer creation fail because no physical UNIQUE index remains.

### Leaseholder

The holder is a mutex-serialized reservation table — every grant/release
decision is made under one lock, so arbitration is correct by
construction. `Reserve`/`Release` reach it as `net/rpc` request-response;
clients discover the holder's address and generation from the lease
object and re-resolve on a handover. On acquisition it rebuilds the
taken-set from its local replica's coordinated-key rows (after a failover
drain — skipped for the first generation, which has no predecessor) and
prunes leaked reservations by reconciling against the rows. A released
value is held under a **release hold** and reclaimable by a different owner only
after a conservative window ≥ the cluster's bounded-staleness deadline
(../../docs/CRDT.md) — by then an unresponsive node is evicted, so every remaining
member has observed the release and no receiver can see two live rows
claim the value. The prior owner may reclaim its own value immediately.

### Apply path is untouched

Receivers apply a coordinated row directly; only the originating writer
reserves. The reservation already guaranteed cluster-wide exclusivity
(and the release hold prevents even a transient cross-row collision),
so no apply ever has a competing row — `NOT NULL` applies cleanly. The
apply hot path keeps its ~12–15µs cost. Receivers reconstruct the table
*without* a SQLite UNIQUE index (the typed `CreateTable` carries only
columns + PK; unique keys live in `syzy_key`), and the originator
normalizes away its physical index after DDL admission. The reservation
gate therefore handles same-node and cross-node conflicts uniformly;
coordinated correctness never depends on physical UNIQUE enforcement.

### Partial keys

A partial coordinated key (`CREATE UNIQUE INDEX … WHERE <predicate>`)
generalizes the single gate that decides whether a row reserves a value.
For a total key that gate is "the key tuple is non-NULL"; a partial key
makes it "non-NULL *and* the predicate holds." Nothing else moves — the
lease, the serialized reservation table, the release hold,
rebuild-from-rows, and GC reconcile are unchanged.

The predicate is evaluated in exactly two places, both single-sited on an
authoritative full-row image, never on a mid-convergence replica:

1. **Reserve.** The writer evaluates the predicate against the
   transaction's pre- and post-image rows (the touch buffer carries every
   column, so a predicate column the statement left untouched is still
   present), reserving the post-value when the post-image participates
   and releasing the pre-value when the pre-image participated and the
   post-image no longer does (or holds a different value). A soft-delete
   is an ordinary release; the release hold then covers the freed
   value as it would a delete, since reclaim is indifferent to whether the
   release came from a `DELETE`, a value change, or a predicate flip.
2. **Rebuild.** A new leaseholder reconstructs the taken-set by
   enumerating rows under the same predicate as a `WHERE` filter.

Receivers never enforce a coordinated key (apply is direct), so the
predicate is never evaluated on the apply path — which is exactly why
partial is admissible for coordinated keys but not eventual ones, whose
loser-null arbitration *does* run on every receiver against
independently-converging cells. The predicate must reference only
replicated, deterministic, collation-independent columns so the writer's
reservation gate and SQLite's own partial index agree on participation
([DDL.md](DDL.md#unique-keys)).

### Limits

- **Availability.** Coordinated writes are unavailable during a lease
  handover, or a partition that isolates the writer from the
  leaseholder — a retryable error, by CAP necessity.
- **Latency.** One mesh round-trip per write touching a coordinated key
  (batched per txn), versus ~3.5µs for an uncoordinated insert.
- **Commit gate.** Pre-commit reservation uses SQLite's `commit_hook`, allowing
  a conflicting reservation to veto the application commit.

## Replication

### Publish

Locally-produced changesets broadcast off the producer's drainer
goroutine. Per drain batch the sink captures every built changeset in
the self-log and fsyncs once, then fires `OnEncoded(payload)` per
changeset; the node's `OnEncoded` listener is wired to
`transport.Broadcast` (see [Self-log](#self-log)). There is no broker
outbox loop — the publish path never enters the broker.
`transport.Broadcast` may queue, retry, or fail; Syzy treats
Broadcast as fire-and-forget — a peer that misses live delivery repairs
via anti-entropy below.

Changesets built by a daemon's secondary drainer are also appended to
that node's per-origin mirror journal before broadcast. The writer's raw
origin journal remains their durable source, while the encoded mirror
entry makes the exact published bytes immediately available from the
peer catch-up endpoint. A peer that opens the topic after the live
broadcast therefore need not wait for the object-store sealer's age
flush.

Receivers do not re-broadcast. The transport handles dissemination
between peers.

### Self-log

The self origin's mirror journal (`origin_<self>/`) is the self-log:
the durability boundary for locally-produced changesets and the single
source of truth for their republish. Broadcast is a latency
optimization layered on already-durable bytes.

Invariant: the self consumption marker (and the `senderNextSeq` it
implies) never advances past a commit whose wire bytes are not durably
captured for verbatim republish. Verbatim matters because a `BlobPatch`
self-journal record captures the written range, not the bytes — the
drainer reads content live from app.db at build time, so re-deriving a
seq later can yield different bytes for the same `Dot`, which
`blob_range_clock` arbitration cannot reconcile (one `Dot`, two
contents). Recovery must replay the originally-built bytes.

Per drain batch, capture precedes publish: append each built changeset
to the self-log (inline, `SyncOn`; the record header's `hlc` field
carries the source self-journal `endOffset`), one group-commit fsync,
and only then broadcast, feed the sealer, and commit cache state
(marker, `senderNextSeq`, clocks). Fsync failure is fatal — the drainer
stops with nothing published, and restart decides durability by what
recovery reads back: a surviving record is canonical; a torn tail
truncates and those seqs are re-derived as the only version that ever
existed.

On startup, `recoverSelf` replays the self-log into the cache (no
broadcast) before the drainer is built, then advances the marker to the
max `endOffset` seen, so the drainer derives only the undrained tail
fresh. All replay effects are idempotent, so re-runs and snapshot
overlap fold to no-ops. The startup-drain tail is captured but not
broadcast live; peers pull it via anti-entropy. The sealer's contiguous
watermark is rebuilt each start by re-feeding the retained self-log
(idempotent `IfAbsent` against S3), which also re-seals anything
dropped at a prior shutdown.

The self-log truncates on the reap pass via
`RetainSealed(self, ContiguousSealedSeq)` — segment-granular, always
keeping the active tail, never ahead of what S3 serves.

Pre-self-log records (`endOffset==0`) are skipped before payload decoding and
waive the promotion guard. A kept origin follows a clean shutdown whose final
snapshot already includes those records, while idle or holed production origins
can retain them indefinitely. Greenfield nodes never write this form. A database
first opened single-node (no capture) then reopened replicated still fails fast
in `recoverSelf` when no legacy records exist and the self-log covers fewer seqs
than the persisted `sender_seq` claims, pointing at re-provision instead of
seeding a permanent peer wedge.

### Anti-Entropy

Each origin's wire history lives in exactly one journal per node: the
self origin's mirror journal (the self-log) for the node's own
commits, and one mirror journal per remote or locally-drained secondary
origin — all under
`<app.db>-syzy/mirror/origin_<origin>/` (see
[File Layout](#file-layout)). All journals share the same on-disk
format and recovery rules; the `transport.CatchupSource` contract
serves replay ranges from these journals.

Changesets may arrive out of `Dot.Seq` order. The cache's contiguous
frontier per origin advances only when the prefix is dense; out-of-order
seqs populate `applied_gaps[origin]` (a `crdt.SeqSet`) and promote into
the frontier when the gap fills. `cache.IsAppliedRemote(origin, seq)`
returns true iff `seq` lies within the contiguous frontier or
`applied_gaps[origin]` — that's the apply-path idempotency check.

The gap-repair loop: per fetcher round, the shared planner
(`internal/antientropy.Plan`) derives missing `(origin, seq)` ranges from
the cache's frontiers and `applied_gaps` (+ optional `TipSource`
discovery), and a `GapFiller` chain pulls them — peers first (the TCP
transport's catchup endpoint replays mirror/self journal bytes), then
S3 — through the same apply path as live deliveries. Each round also
force-re-applies quarantined entries
(see [Localized Failures](#localized-failures)). Rounds back off
exponentially while nothing progresses; an out-of-order apply or a peer
connect wakes the loop. Ranges intersecting none of the origin's
surviving S3 epoch intervals (`transport.CoverageSource`, derived from
the same complete bucket listing as tip discovery — absence from the
complete walk is itself a claim) are unserveable by the bucket and
demote to a slow probe (`antientropy.Prober`, re-armed on peer connect
and by any probe that fills) instead of retrying every round; each
round reclassifies from a fresh snapshot, so a fresh origin's ranges
return to the normal plan as soon as its first epoch lands.

**Schema-gated apply** is held by the broker: when an inbound changeset's
`Deps[SchemaChain]` exceeds local `meta.schema_seq`, or a record references
a table id not yet present in the local catalog, the subscribe loop keeps
that delivered payload in hand and retries it while schema-chain catch-up
advances `meta.schema_seq` via `schemalog.Read`. There is no separate
`schema_gated` state today. A future revision will attach gating to
`applied_gaps` so a gated seq doesn't advance the contiguous frontier —
see [Not Yet Implemented](#not-yet-implemented).

Frontier exchange for GC and reaping is pull-based
(`tcpmesh.PeerFrontierSource` over the `opFrontier` bundle op) — see
[PRUNING.md](../../docs/PRUNING.md#peer-frontiers).

### Transport Interface

The broker does not ship with a transport; operators bind one. The
minimal contract is two methods, with optional sibling contracts a
transport implements for richer behavior (all defined in
[`transport/transport.go`](../../transport/transport.go)):

```go
type ApplyFunc func(context.Context, []byte) error

// Transport ships changeset bytes between peers.
type Transport interface {
    // Broadcast publishes one local changeset for live dissemination.
    // Returns when the transport has accepted the publish attempt;
    // not when peers have applied.
    Broadcast(ctx context.Context, changeset []byte) error

    // Subscribe delivers live changesets. The callback returns nil
    // once Syzy has durably accepted the changeset: applied,
    // idempotently skipped, or blocked on schema.
    Subscribe(ctx context.Context, apply ApplyFunc) error
}

// GapFiller backfills ranges the live Transport may not have
// delivered.
type GapFiller interface {
    Fetch(ctx context.Context, ranges []Range, apply ApplyFunc) error
}

type Range struct {
    Origin crdt.Origin
    Lo, Hi crdt.Seq // inclusive; Hi=0 = "everything past Lo"
}
```

Optional siblings, discovered by interface assertion in `sqlite.Open`:
`TipSource` (highest known seq per origin from a source the broker
hasn't observed live), `CoverageSource` (which seq intervals a
source can currently serve), `PeerStatter` (per-peer RTT stats),
`CatchupSource` (serve peer-pull requests from local journals), and
`PeerFrontierBuilder` (aggregate peers' applied frontiers). The mux
channel satisfies `PeerStatter` and `PeerFrontierBuilder` directly
and registers the rest (`CatchupSource` is `mirror.Manager`; the
frontier builder yields the peer `TipSource`); `CoverageSource` is
the object store's capability. A minimal custom transport needs
only `Broadcast`/`Subscribe` plus an object-store `GapFiller`.

Required transport guarantees:

- If `Broadcast` returns an error, the broadcaster keeps the
  changeset in its self-origin journal and recovery flows through
  catchup.
- `ApplyFunc` returning nil means Syzy durably accepted the
  Changeset. A non-nil error means Syzy did not accept those bytes.
- `Subscribe` may deliver duplicates and out-of-order Changesets. A
  production transport that may drop live deliveries must be paired
  with a `GapFiller` or an equivalent replay channel.
- `Fetch` makes a best effort to cover requested ranges and may
  over-deliver adjacent Changesets. Success means the attempt
  completed, not that every requested sequence was applied; Syzy
  checks the frontier afterward. `ErrUnfilled` marks a CLEAN empty
  fetch (every source answered, none held the ranges),
  distinguishable from substantive failures.
- **Self-origin Fetch** (`Range.Origin == our origin`) is supported;
  the transport routes to any peer holding the Changesets.
- `Range.Origin` is the Changeset origin, not a transport peer. The
  transport decides whether to fetch from the origin, another
  replica, a relay, or an object store.
- No ordering requirement: Syzy treats `Dot.Seq` as
  idempotency, retention, gap-fill, and frontier metadata, not
  semantic apply order.

Out of scope: general causal ordering beyond schema dependencies. DML
carries `required_schema_seq` and blocks behind missing DDL, but
application-level causal barriers still require explicit frontier
checks.

## Lifecycle

### Producer Startup

1. **Open files.** Open `app.db` and `app.db-syzy`. SQLite's WAL recovery
   on each file runs to completion before any of the steps below. Failure
   to open either file → exit with structured error; operator runs
   `syzy_clone` (metadata gone) or addresses the underlying SQLite/disk
   issue.

2. **Validate cluster identity.** `metadata.meta.cluster_id` must match
   the operator-supplied cluster_id. Mismatch → exit with structured
   error.

3. **Validate metadata schema.** Open verifies the metadata schema
   matches the canonical version (meta, frontier, row_clock, and the
   DDL catalog tables). Mismatch or corruption → exit with structured
   error; operator runs `syzy_clone`.

4. **Seed cache from metadata snapshot.** `nodestate.Cache.LoadFromMeta`
   reads the `sender_seq`, `frontier`, `row_clock`, and `cell_clock`
   tables plus `meta.hlc_last`, `meta.applied_gaps`, and
   `meta.snapshot_markers` into the in-memory Cache. The cache is now
   consistent with the last metadata snapshot.

5. **Open journals.** Open the self evidence journal at
   `<app.db>-syzy/origins/<origin-hex>/journal/` and the mirror
   manager at `<app.db>-syzy/mirror/`. `mirror.Manager.LoadExisting`
   walks `mirror/origin_*` to make every known origin reachable for
   replay even before any new Append.

6. **Replay journals past the snapshot markers.** The producer's
   drainer replays the self journal (`prod.WaitForDrain`, without
   re-broadcasting) and `nodestate.RecoverMirror` replays each mirror
   journal — see [Recovery](#recovery) for the replay semantics.
   Replay is bounded by the snapshot interval times the commit/apply
   rate, independent of cluster age or total disk usage.

7. **Resolve any DDL intent.** Read `was_clean = meta.clean_shutdown`,
   then set `meta.clean_shutdown = false`. If this origin's intent slot
   (`meta."intent:<origin>"`) is present (the only kind that survives
   across restarts is `LocalDDL`):

   Resolution runs `resolve_intent` (DDL branch — see
   [DDL.md](DDL.md#resolve_intent-ddl-branch)). The autocommit path
   pre-reserves the intent *before* `Append`, so an intent's presence
   is not proof the event is durable: resolution verifies the schema
   log holds the event at `intent.schema_seq` before applying, and
   clears an unappended intent without resolving. DML has no intent
   kind — its durability lives in the
   per-origin journals. Clone has no intent kind either — the source
   coordinates via the WAL writer-slot barrier (see [Bootstrap &
   Repair](#bootstrap--repair)) and the receiver's installation is an
   atomic file rename, so there is no half-completed clone state to
   resolve at startup.

8. **Origin rotation after unclean shutdown.** If `was_clean=false`,
   allocate a fresh local origin (`node_id`), reset
   seed `sender_seq(node_id, next_seq=1)`, and seed `frontier(node_id, last_seq=0,
   last_hlc=hlc_last)` before accepting local writes. Clean restarts
   keep the existing origin.

9. **Install hooks** on the writer connection (preupdate, commit, wal,
   trace_v2, rollback). Register the OnEncoded listener last so it only
   fires for new commits.

10. **Wake or start the broker and snapshotter.** Both are
    goroutines scoped to the node's context. The snapshotter ticks
    every `Interval`.

### Broker Startup

The broker opens the same `app.db` (a separate `AppApply` connection
without producer hooks) and reuses the node's `nodestate.Cache` +
`MirrorJournals`. It validates `cluster_id` from the
metadata, then starts its loops (subscribe always; gap-fill fetcher
and schema catch-up when configured). There is no outbox loop and
no metadata scan at startup — all CRDT state was already seeded into
the Cache by the producer's startup path.

### Clean Shutdown

`syzy_close` on a producer connection stops accepting local writes, lets
any in-flight commit finish, drains the journal, takes one final
metadata snapshot via `Snapshotter.SnapshotOnce`, and sets
`meta.clean_shutdown = true`. Shutdown that misses this step is treated
as unclean on the next producer startup and rotates the local origin.

Broker shutdown cancels the subscribe loop's context, lets the in-flight
apply finish, and exits without changing local commit durability state.
The mirror manager closes per-origin writer goroutines after they drain
their bounded chans.

### Bootstrap & Repair

**`syzy_init`** (cluster genesis): allocate `cluster_id` UUID and `node_id`,
`sender_seq(node_id, next_seq=1)`, `hlc_last=0`, `clean_shutdown=true`,
`schema_seq=0`, seed `frontier(node_id, last_seq=0, last_hlc=0)`, and
create metadata tables including the initial schema catalog for existing
replicated tables. Today this genesis flow runs implicitly on the first
`sqlite.Open` against a database with no metadata; an explicit
`syzy_init` surface is not yet implemented
(see [Not Yet Implemented](#not-yet-implemented)).

**Safety check:** refuse if `app.db-syzy*` files already exist at the
target path — this prevents accidentally re-initializing a Syzy-managed
database under a different cluster identity. Operator must `rm` the
existing metadata files explicitly to re-init.

Works against any `app.db` (empty or populated) once that check passes.
Pre-existing rows have implicit `(hlc=0, origin=0)` clocks — the first
write produces an explicit `row_clock` entry.

**`syzy clone <src> <dst>`** is the canonical recovery primitive for
"I lost the metadata," "I want a known-good snapshot from a peer," or
"bootstrap fresh node into an existing cluster." It produces a *bundle*
— a wire/file format containing `metadata.db` and `app.db` — and adopts
that bundle at a fresh `<dst>` path with rewritten identity.

Source forms:

- `tcp://host:port` — pull from a running daemon's `--listen` port.
  Live; the source serves a writer-barrier-pinned bundle.
- `s3://bucket/prefix` / `file:///abs/path` — pull the latest
  HEAD-pinned snapshot from object storage. See
  [Object-store clone](#object-store-clone-s3--file).
- `path/to/app.db` — copy a stopped local syzy database. Refuses if the
  source's `daemon.lock` is held; the local path bypasses any in-memory
  state, so a live daemon would produce an inconsistent bundle.

#### Consistency: the two-file problem

`app.db` and `metadata.db` are independent SQLite files. No single
SQLite transaction spans both. To produce a *consistent* bundle the
source needs the receiver's `metadata.db` to be a strict subset of the
state in the receiver's `app.db`:

- **Extras in app.db** (rows present in `app.db` whose `row_clock`
  entries are missing from `metadata.db`): receiver treats them as
  pre-existing implicit `(hlc=0, origin=0)` rows. Any concurrent peer
  write beats them. Stale-data risk.
- **Extras in metadata.db** (frontier or row_clock entries for rows that
  aren't in `app.db`): receiver's frontier dominance check
  *deduplicates* peer rebroadcasts of those rows. Permanent silent data
  loss.

Both directions are unsafe. The fix is a writer-barrier window on the source
that copies both files to completion at the same logical commit boundary.

#### Live clone (tcp://): the writer-barrier protocol

The source daemon serves the bundle over its TCP listener. To produce
a consistent snapshot without a long writer pause:

1. **Pre-drain (no barrier held).** `WaitForDrain` on the producer's
   drainer and every attached `SecondaryDrainer`. Concurrent commits
   may still land — the drainer is async and converges to whatever
   journal head exists at the moment. This soaks up the bulk of the
   drain latency *before* we stall any other writer.
2. **Open a writer barrier on `app.db`.** `BEGIN IMMEDIATE` on the
   daemon's barrier connection takes the SQLite WAL writer slot, which
   serializes against every writer on the database file (other
   connections, the apply path on `appApply`, and any extension-process
   writer). The Go `Node.Exec` convenience writer is also guarded by a
   per-node mutex, so the daemon does not enter the barrier while its
   own writer connection is between SQLite commit visibility and
   producer journal append. Concurrent external writers hit
   `SQLITE_BUSY` and retry under their `busy_timeout` — they don't fail.
3. **Tail-drain.** Re-run `WaitForDrain` to catch records that landed
   between step 1 and step 2. The barrier has frozen all journal heads
   so this converges in microseconds in steady state.
4. **Flush the cache to `metadata.db`.** `Snapshotter.SnapshotOnce()` —
   incremental, scales with dirty rows since the last periodic snapshot,
   not with cache size.
5. **Copy both backup snapshots.** Open `sqlite3_backup_init` on
   `metadata.db` and on `app.db` (each through its own fresh source
   connection), and run both backups to completion while the barrier remains
   held. A partial `sqlite3_backup` is not a durable snapshot boundary:
   source writes between `backup_step` calls can restart the backup and appear
   in later steps.
6. **Release the barrier.** `COMMIT` on the writer connection.
   Concurrent writers proceed. The completed staged files are independent of
   subsequent source writes and checkpoints.
7. **Stream the staged files.** The bundle wire format is emitted as
   `magic | version | (metadata.db) | (app.db)` regardless of how the
   pages were copied.

The held window is bounded by `tail-drain + SnapshotOnce + two complete local
SQLite backups`. The copy cost scales with the database sizes, so baseline
creation can pause writers longer than the old partial-copy scheme. This is a
deliberate correctness tradeoff: publishing a mixed physical image can make a
cold restore unrecoverable. The bulk journal drain is still amortized in step
1 outside the barrier, and object encoding/upload happens after release. There
is no source-side intent record persisted — the barrier is implicit in the
SQLite WAL writer slot, not in the intent slots.

Streaming does not hold source read transactions and therefore does not pin
the source WAL. The staged files are removed when the bundle is closed.

#### Clone installation (receiver)

1. Refuse if `<dst>` or `<dst>-syzy/` already exists. Stage everything
   under sibling `.tmp` paths so a partial clone leaves visible cruft.
2. Materialize `metadata.db` and `app.db` from the stream into the
   staged metadata dir.
3. Mint a fresh `node_id` (`layout.MintOrigin`).
4. **Identity reset on the staged metadata** (one transaction):
   - Write `node_id = newOrigin`.
   - Wipe `sender_seq`; seed `(newOrigin, 1)`.
   - `clean_shutdown = true`.
   - `hlc_last = max(seed.hlc_last, HLC.now())` where `HLC.now()` is the
     local wall-clock physical-millisecond field with logical counter
     zero, so the clock cannot regress relative to either the seed or
     the local node.
   - Seed `frontier(newOrigin, last_seq=0, last_hlc=hlc_last)`.
   - Clear all `meta."intent:*"` slots and `meta.snapshot_markers`
     (they belong to the source's own journals, which we don't ship).
   - `applied_gaps` deliberately survives — those remote seqs are
     already represented in the cloned `app.db`.
5. Pre-create `origins/<newOrigin>/journal/` under the staged metadata so
   the next `sqlite.Open`'s `layout.Acquire(pinned=0)` recycles our
   minted origin instead of minting a different one.
6. Atomic rename: metadata dir first, then `app.db`. An interrupted
   clone that leaves the metadata dir but not the app.db lets the
   operator retry without manual cleanup, since `Adopt` refuses on a
   pre-existing metadata dir.

Any local writes not yet broadcast or applied on the source are
already covered by the source's own journals; the adopter's frontier
inherits the source's view, so peer rebroadcasts of those writes that
arrive at the new node after `Open` will be deduplicated by the
receiver's frontier dominance check.

#### Object-store clone (s3:// / file://)

The bucket carries immutable streams plus one mutable HEAD — see
`internal/objstore` for the canonical layout and key grammar. Two
physical LTX streams (`db/` for app.db, `metadata/` for metadata.db)
are produced by a single elected publisher. The logical streams
(`origins/<origin>/` changeset epochs, `events/` schema events) are
produced by each node into its own per-origin or per-event slot.
S3-compatible endpoints may omit optional response checksum headers;
the object-storage client suppresses SDK checksum-skip noise while preserving
validation when supported headers are present.

**Read liveness.** Large object reads are striped across independent
connections; slow connections are replaced up to a budget, a stalled
read terminates, and callers verify the reassembled object's size and
digest.

**Coordination invariant.** Publisher-coupled metadata.db snapshot
transactions stamp `meta.parent_app_txid` after draining the app.db
LTX tailer (idle ticks that would rewrite an unchanged stamp are
skipped). Restore replays `metadata/` to its tip, reads that parent
app TXID, and caps `db/` replay at that value, so app.db is never
materialized past the metadata pin. A db chain short of the pin still
restores to its tip; the resulting metadata↔app orphans are detected
and logged after restore, not fatal. This is the only cross-stream
ordering rule.

**Lock-order invariant.** Operations that need both a database writer
fence and the corresponding LTX tailer's position mutex acquire the
writer fence first and the tailer second: `app.db` uses `writeMu →
app tailer`, while `metadata.db` uses `Store.mu → metadata tailer`.
A coordinated WAL recycle may pre-drain the tailer before taking the
fence, then, while holding the fence, takes the tailer mutex and performs
the last-mile drain, checkpoint, and position reset as one atomic unit.
It must never request a writer fence while holding a tailer mutex. This
ordering lets a coupled baseline drain a tailer while its writer barrier
is open without cycling against the checkpoint loop, while the atomic
last-mile sequence prevents a concurrent tail pass from observing a
recycled WAL with the old position or salt.

**LTX compaction.** The publisher periodically compacts contiguous L0
runs into L1 for both `db/` and `metadata/`, off the commit path, in
bounded work-idempotent passes: each pass derives the uncovered-L0
scan bound from the active baseline plus contiguous L1 coverage,
streams inputs through the compactor (memory bounded per chunk, not
by backlog), and emits a limited number of L1 chunks; later ticks
resume. Retention deletes covered L0 and below-baseline L1 only after
the covering object has aged past the grace window. Retention also
reclaims superseded baselines (below HEAD's active baseline pointer)
and origin epochs whose records are durable at/below the applied
frontier — both likewise only after grace.

**Full restore (`Restore`, `RestoreFromBucket`).** Read HEAD → app and metadata
baseline LTX FileRefs. Both baselines are required. Apply each baseline, then a
non-overlapping LTX chain that prefers L1 over covered L0 retained for grace.
Both databases are completely materialized and verified before installation, so
the opened replica has no read dependency on object storage. A HEAD without
either baseline has no restorable state.

**Lazy bootstrap (`lazyrestore` optional package).** `lazyrestore.Prepare`
restores `metadata.db` completely, pins the matching app TXID, builds a
page-number → LTX-object index, and creates `app.db` as a correctly sized sparse
backing file with page 1 present. `lazyrestore.NewMount` exposes the database
through a FUSE filesystem. Reads fault absent pages from immutable LTX objects;
writes first fault partial pages, then update the backing file. Presence and
cleanliness tracking permit a matching clean page from another live mount to be
reflink-cloned before falling back to object storage.

The sparse backing file must remain outside application-visible filesystems.
Reading one of its holes directly returns zeroes and bypasses page hydration;
only the mounted path is safe for SQLite. The preparation step rejects SQLite
page sizes smaller than the backing filesystem block size because sparse extent
discovery cannot then distinguish adjacent pages. Runtimes without the mount
use full restore.

Lazy bootstrap is optional and Linux-specific. It reduces bytes needed before
open at the cost of an object-store dependency and cold-read latency until pages
are local. Its manifest sidecar is an internal restart artifact, not part of the
replication or object-storage protocol.

The `db/` layout is byte-compatible with Litestream's LTX schema, so
`litestream restore` against `db/` produces a valid `app.db` as an
eject hatch (without `metadata.db` — recover via `syzy clone` from a
peer to rejoin the cluster).

**HEAD as cluster-id beacon.** HEAD is also the cluster identity
beacon: a fresh bucket gets a stub HEAD via If-None-Match CAS
carrying a minted `cluster_id`. Concurrent first opens linearize
through that CAS. The publisher lease is embedded in the same HEAD
object (one mutable artifact, one CAS protocol).

**Publisher shutdown handoff.** Clean publisher shutdown cancels and joins its
generation work, makes a best-effort final drain/checkpoint, stops the tailers,
then CAS-expires `HEAD.publisher` if it still owns the same
`(node_id,generation)` lease. Node shutdown may still be quiescing other local
producers during that checkpoint, so it is WAL hygiene rather than a durability
boundary. The retained holder identity distinguishes a coordinated same-node
handoff from a foreign takeover in HEAD, but every claim re-anchors with a fresh
coupled baseline: WAL continuity with the bucket chain cannot be proven across
a restart. Crashes leave the lease in place for normal expiry.

**Publisher-generation fencing.** Each successful claim owns one leadership
context keyed by the exact `(node_id,generation)` recorded in HEAD. Every
leader-only goroutine — both LTX tailers, compaction, retention, coordinated
checkpointing, and recycle-triggered rebaseline work — derives from that
context. Before the publisher run returns it cancels the context and joins the
maintenance goroutines and admitted external operations. On clean shutdown it
then makes a best-effort final drain/checkpoint against the retained tailer
objects, joins and clears those tailers, and only then releases or retains the
lease. This final checkpoint is WAL-size/startup hygiene, not a correctness
boundary: shutdown begins before the Node's broker and sealer have fully
quiesced, and the next claimant always takes a fresh coupled baseline.

Every lease-scoped HEAD mutation re-reads HEAD and verifies that exact,
**unexpired** `(node_id,generation)` before treating an existing baseline as
idempotently covered or attempting CAS. Coupled app+metadata baseline rotation and
metadata-only rebaseline both return ownership loss when the identity differs;
that result cancels the claim context, and they never move a successor's
pointers. Expiry is also ownership loss. Immutable baseline objects uploaded
before that check may be left unreferenced; a later higher baseline makes them
eligible for ordinary retention.
An explicit offline/operator snapshot is a separate exclusive operation. It
holds the source daemon claim through capture, then CAS-acquires a short-lived,
randomly identified publisher generation in the target HEAD before it scans
bucket TXIDs, stages data, or uploads immutable objects. Acquisition rejects
any unexpired target publisher and retries CAS contention; the resulting exact
`(node_id,generation)` owns the coupled-baseline promotion through the same
active-lease checks as a live publisher. The operation context and every remote
Put are bounded by that temporary lease's expiry, so a paused or overlong tool
cannot begin another mutation after its reservation expires. Every exit
best-effort CAS-releases only that exact generation; a failed cleanup leaves a
bounded lease to expire naturally and can never release a successor. There is
no read-only-preflight offline publication path.

Online/operator-triggered publication through a live node is admitted by the
same generation gate: teardown first closes admission, then cancels and joins
every admitted baseline mutation before it releases or retains the lease. A
request arriving after teardown starts is rejected, and an admitted request's
object-store calls derive from the generation context as well as the caller's
context. Thus a released generation cannot continue mutating HEAD merely
because the retained holder fields still name its `(node_id,generation)`.

Baseline capture and promotion are serialized across coupled, metadata-only,
structural, and online/operator requests, and every path allocates its
baseline TXID **before** the writer-barrier pin. Allocate-before-pin is the
ordering invariant: any L0 the tailers drain after the allocation sorts above
the baseline, and any L0 numbered at or below it was drained before the
allocation and therefore holds only commits the later pin also captures, so
restore filtering (which discards LTX at or below the baseline TXID) can never
hide a commit. The reverse order — pin, release the writer fence, then
allocate — would let a post-pin commit drain into an L0 numbered below the
baseline and silently vanish from the restore chain; the publisher owns the
allocation precisely so no caller can reintroduce that ordering. One allocator
mutex covers app, metadata, and baseline TXIDs, so an incremental L0 cannot
collide with that root allocation. Serialization makes allocation order,
capture order, and HEAD promotion order agree instead of allowing an older
delayed capture to overwrite newer state under a numerically larger TXID.
Online publication is not exposed as ready until both tailers are installed.

Baseline promotion is monotonic per stream. A coupled candidate may fill or
advance both pointers, or be idempotently covered by both; it must fail closed
when one current pointer is already ahead while the other is not, rather than
regressing the ahead stream. Immutable LTX keys are also byte-identifying in
practice: an If-Absent collision is success only when the stored object's size
and SHA-256 equal the proposed bytes. Foreign bytes at the same TXID key are a
publication conflict and can never be recorded in HEAD under the proposer's
digest. An immutable-key or pointer-order conflict is a generation-fatal
physical integrity failure, not periodic warning noise: the publisher
self-fences and leaves its lease to expire.

Lease renewal is coupled to forward progress in both physical pipelines. On
every heartbeat tick the publisher consistent-reads HEAD and verifies its exact
`(node_id,generation)`, even when it is not eligible to renew. It may extend the
lease only after both the app and metadata tailers have completed at least one
new successful public Sync pass since the preceding successful renewal. An idle
pass with no WAL or no new commit counts: this proves the executor can inspect
the stream, not that the workload is writing. Failed passes and coordinated
checkpoint-internal drains do not count.

The two Sync proofs are consumed only after the HEAD renewal CAS succeeds, so a
transient object-store failure cannot discard evidence needed by the retry. If
either proof is absent, renewal is skipped while another heartbeat opportunity
remains before the locally tracked expiry. The same safety window applies to a
transient renewal failure. `lease_expiry` must exceed two heartbeat intervals,
leaving at least one scheduled renewal before the safety deadline. Once
`now + heartbeat_interval >= lease_expiry`, the publisher treats the generation
as fatally unhealthy regardless of whether proof is available: it cannot renew
from inside its self-fence window. A per-generation watchdog cancels work at the
same deadline, and every leader-generation LTX Put or retention Delete
synchronously re-checks the wall clock immediately before handing the mutation
to the backend. Claim, release, and exclusive offline maintenance use their own
CAS rules. The synchronous guard covers a process that was paused while an
operation was in flight, even if its watchdog callback has not yet run on
resume.

An unhealthy publisher cancels and joins the generation but deliberately does
not release or CAS-expire the lease. Natural expiry is the quarantine window
for immutable uploads that the backend may already have accepted; such a
request is irrevocable once handed off, so collision verification and the next
claimant's fresh coupled baseline contain that residual. Only after expiry may
a foreign successor claim and re-anchor. A coordinated same-node handoff may
claim a retained lease immediately; daemon/origin flock transfer is the
exclusivity proof for that path, not the publisher lease by itself.

The expiry protocol assumes wall-clock skew and backward movement across hosts
remain smaller than the heartbeat safety interval. Without a storage-provider
server-time primitive, a node whose clock is farther behind can still consider
its immutable-write window open after a correctly timed foreign takeover.
Deployments must synchronize and monitor host clocks; generation-qualified
immutable manifests or server-time fencing would be required to remove this
assumption entirely.

### Recovery

Recovery time is bounded by snapshot interval × commit-or-apply rate,
not by total cluster history. The full sequence is described in
[Producer Startup](#producer-startup); the short version is:

1. `cache.LoadFromMeta(sc)` seeds the cache with the last snapshot's
   frontier, row_clock, cell_clock, applied_gaps, sender_seq, hlc_last, and
   per-origin journal markers.
2. The producer's drainer replays the self journal from
   `markers[self]` to head. Because OnEncoded is registered AFTER
   `WaitForDrain` returns, these historical records advance the cache
   (PutRowState, AllocSelfSeq via the sink, mark new SnapshotMarker)
   without re-broadcasting.
3. `nodestate.RecoverMirror(cache, mirrorMgr, cat)` walks each mirror
   journal from `markers[origin]` to head and runs each KindMirror
   payload through the LWW dominance check + `MarkApplied`. App.db DML
   is **not** re-run — the apply path that wrote the mirror entry
   committed app.db before the journal append, so post-snapshot mirror
   records imply post-snapshot app.db rows already. Replay is
   clock-group-aware: cell-group tables re-derive the live apply
   path's per-column bookkeeping (cell stamps, opportunistic collapse,
   zero-Base generation advance) so a partial-column update never
   inflates the row `Base` over columns it didn't carry; a table
   missing from the catalog degrades to row-level replay.

The fixed point: cache is consistent with app.db and with the journals'
heads. New commits and applies start from there.

## Performance

Hot path per local commit: preupdate first-touch bookkeeping per row
(C, no cgo), one cache mutex for `StampHLC` (~3µs), one mmap memcpy +
atomic head publish for the journal append. No SQL on the writer
thread. `wal_hook` returns and the user thread sees the COMMIT as
completed; the broadcast happens on the drainer goroutine some
microseconds later.

Hot path per inbound apply: cache idempotency check (~100ns under
mutex), per-record cache LWW + DML stmt step, BEGIN IMMEDIATE / COMMIT
on AppApply (one WAL fsync), cache state advance under mutex, bounded
chan send to the mirror writer. No metadata I/O.

Snapshotter cost is amortized over many commits: one metadata `WithTx`
per tick (default user-tunable; testcluster fixture omits the default)
batching all dirty rows + frontier + applied_gaps + markers + meta
fields. The metadata is the only thing taking SQL writes for replicated
state, and it does so off the hot path. The snapshotter does not hold
the cache mutex while doing metadata I/O. It captures the dirty set under
the cache mutex, writes that immutable snapshot, then clears only dirty
entries whose current in-memory value still matches the snapshot it just
persisted. Commits or applies that advance the cache while the metadata
transaction is running remain dirty for the next snapshot; this preserves
the clone/recovery invariant without adding metadata I/O or broad locks to
the write path.

End-to-end round-trip benchmark
(`internal/testcluster/BenchmarkRoundTripInsert`, in-process two-node
memtransport, 8-byte BLOB PK INSERT) lands at ~30 µs median. The floor
is dominated by one app.db WAL fsync on the writer node, the
apply-side cgo triple (BEGIN/DML/COMMIT) on the receiver, and three
goroutine wakeups per round-trip.

**Multi-writer serialization:** local commits serialize through SQLite's
writer lock on `app.db`; the cache mutex adds bounded contention with
the apply goroutine but is held only for in-memory work. Inbound
applies serialize on the broker's apply mutex (shared by the
subscribe, fetcher, and schema catch-up loops); the
per-origin mirror writers are concurrent on disk but bounded by their
own chans.

Mitigations: keep journals on fast local storage (the mmap path is
sensitive to page cache pressure); small transactions; prefer short
DDL statements (DDL apply still runs synchronously through the
metadata — DDLs are rare so a metadata tx on the writer thread is
acceptable).

## Failure Handling

Two narrow categories: localized failures (return an error to the
transport so it retries; emit a diagnostic) and startup integrity errors
(exit with structured error; operator runs `syzy_clone`).

### Localized Failures

A Changeset or DDL statement failed without invalidating the node. An
inbound apply either succeeds (cache advances, mirror journal
appended, broker fires applied), quarantines a deterministic failure
(below), or returns an error from `applyPayload`; the transport
retries per its own backoff.

Cache state advance and mirror append are atomic with respect to the
caller: if `applyRecordsLWW` errors, BEGIN IMMEDIATE rolls back
and no cache mutation happens; if `MirrorJournals.Append` errors, the
cache has already been advanced (rare — the mirror writer is async
with backpressure, so an error here means the writer goroutine died)
and the broker logs the error. Any per-record failure rolls back the
whole app.db tx; what happens next depends on whether the failure is
deterministic. The `apply_issue` diagnostic table specified below is
not yet written from the apply path.

**Apply quarantine.** Syzy carries no row-level causal dependencies on
the wire (`Deps` covers the schema chain only — a deliberate design
choice to keep records small and the protocol simple). A dependent
write can therefore arrive before the write it depends on: the
canonical shape is a partial cross-origin record (e.g. an
UPDATE-shaped Insert missing a NOT NULL column) arriving before the
INSERT that creates its row. Applying it raises a *deterministic*
failure — one that retrying in place can never fix, and that would
otherwise pin the origin's frontier forever, silently starving every
later record from that origin (head-of-line blocking).

Rule: on a deterministic apply failure (SQLite constraint violation,
counter wire-contract/overflow failure, or a row-group update
outrunning its row's INSERT), the broker durably parks the changeset's
**exact payload bytes** in the `apply_quarantine` metadata table and
only then advances the origin's frontier past it. The ordering
matters: a crash in between leaves the entry stored but the frontier
un-advanced, so the record re-arrives and re-quarantines idempotently
— never the reverse (advanced but lost), which would silently drop the
changeset. Later records from that origin keep flowing.

Quarantined entries are force-re-applied every fetcher round (adaptive
interval, ~30s base, plus wakes when new records arrive). Once the
missing dependency lands, the deferred apply succeeds and the entry is
deleted. Convergence is unaffected: cell-level LWW arbitration makes
the *order* in which the deferred write finally applies irrelevant to
the converged state. Quarantine changes *when* a record's effect
appears, never *what* the converged result is.

The frontier invariant, restated: for every origin, every seq ≤
frontier is either **applied to the database** or **resident in
`apply_quarantine` with its exact payload bytes**. The frontier is a
local-durability watermark, not an applied watermark. Because the
payload is retained locally *before* the frontier advances, no
redelivery from peers or object store is ever needed to drain the
quarantine.

**The backstop.** Residency is capped per origin (128). At the cap the
broker stops advancing and hard-blocks that origin: a *flood* of
deterministic failures from one origin no longer looks like an
isolated delivery gap — it signals real corruption or a
schema-divergence bug, and bounding the damage beats limping forward.

**Observability.** `InboundHealth` exposes `QuarantineResident`,
`QuarantineOldest`, and `QuarantineMaxAttempts`. Transient non-zero
residency is normal during cross-origin delivery races and drains
automatically; steady-state residency with a climbing attempt count
since `QuarantineOldest` means an entry can never apply and needs
operator attention.

**Transient vs deterministic.** Transient/environmental errors (I/O,
`SQLITE_BUSY`, disk full) are NOT quarantined — they hard-block and
retry in place, because they are expected to resolve and advancing
past them would be wrong. Quarantine is exclusively for failures that
are a deterministic function of (payload, current state).

The "ignored" outcome (every record's table tombstoned, deterministic
across replicas) is implicit: the apply loop skips records whose
`table_id` isn't in the catalog, so an all-dropped changeset advances
frontier with zero DML. There is no "rejected" or "dead_lettered"
terminal state — a transient error retries until it lands, a
deterministic failure re-applies from quarantine until it lands, and
the operator escape hatch is `syzy_clone`.

Behavior:

- Return error to transport on transient apply failure; rely on
  transport retry.
- Park deterministic failures in `apply_quarantine` and advance the
  frontier (above); the fetcher round drains them.
- Continue processing subsequent changesets (the broker's subscribe
  loop doesn't block on individual apply errors).

Issue codes:

| Reason                | Trigger                                                                                         | Resolution                                                              |
| --------------------- | ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `ddl_apply_failed`    | Schema catch-up cannot continue because the log horizon was missed, the event chain/encoding is invalid, or SQLite deterministically rejected the structural operation | Broker durably records `meta['schema_unhealthy']`, stops schema catch-up, and requires `syzy_clone` (see [DDL.md](DDL.md#schema-health)). |
| `record_apply_failed` | Column/type/non-FK constraint/reject failure within a Changeset; whole Changeset rolled back, then quarantined if deterministic | Quarantine re-apply converges once the missing dependency lands; future higher-`(CL, Stamp)` writes apply normally. |

### Startup Integrity Errors

Syzy cannot proceed safely. The process exits with a structured error.
Recovery is operator-run `syzy_clone` or addressing the underlying
issue. Triggers:

- Cannot open `app.db` or `app.db-syzy` or the journal directories
  (file missing, SQLite reports corruption, permission error, mmap
  fails).
- Metadata schema validation failure (wrong `schemaVersion`, missing
  meta/frontier/row_clock tables).
- Metadata `cluster_id` does not match the operator-supplied cluster.
- Durable `meta['schema_unhealthy']` marker from a prior terminal
  schema catch-up failure.
- Journal segment header corruption that the journal package can't
  tail-truncate past.
- DDL intent recovery cannot apply the catalog op against the current
  SQLite structural state.

There is no degraded half-running mode. The node either runs
correctly or refuses to start.

## Sharp Edges

Known and intentional. Do not propose runtime checks for these.

### Host-Level Desync

WAL is excluded from SQLite's super-journal (vdbeaux.c `aMJNeeded[]`), so
app and metadata commits aren't strictly crash-atomic across the two
files. Cache state advances atomically in memory; the snapshotter
persists later; recovery replays journals. A crash between the cache
state advance and the next snapshot leaves the journal containing
records past `markers[origin]`, which recovery replays. After an
unclean shutdown, Syzy rotates the local origin before accepting new
writes, so future commits cannot reuse a sequence already visible
under the previous origin. Clean restarts keep the existing origin.

Under the default WAL `synchronous=NORMAL`, a hard OS crash can
theoretically persist `app.db`'s final commit while losing the
trailing journal append, or vice versa (the journal append runs in
`wal_hook` after the WAL fsync, so it is at most one OS-flush quantum
behind). Both straddle cases leave the cluster consistent:

- **Journal record lost, app.db row kept.** No Changeset was ever
  built or broadcast; the row is a local-only orphan, invisible to
  the cache and the cluster, until the operator re-issues the write.
  Origin rotation keeps the lost seq from colliding.
- **App.db row lost, journal record kept.** Recovery replays the
  record into the cache without re-broadcasting; the cluster already
  saw the original broadcast, and this node re-converges via peer
  re-delivery.

Operators close the window by setting `app.db` to `synchronous=FULL`
(or `EXTRA`) **before** constructing the producer. The pragma is read
once at construction and derives the journal sync mode to match:
`FULL`/`EXTRA` → `msync(MS_SYNC)` after each journal `Append` (plus
fdatasync at segment rotation), `NORMAL`/`OFF` → kernel page cache
only. One knob, both files. `Config.JournalSync`
(`Auto`/`ForceOn`/`ForceOff`) overrides the derivation for
measurement or deliberately asymmetric modes; both asymmetric
combinations are safe, just not symmetric. Per-commit cost ranges
from ~15 µs (`NORMAL`, journal sync off) to ~6 ms (`FULL`, journal
sync on); see `internal/producer/journalsync_bench_test.go`.

Mirror journals are intentionally never synced: a trailing mirror
append lost on host crash is recoverable via peer re-delivery — an
availability concern, not a durability one.

Out-of-band writes to replicated tables or schema (manual `sqlite3`
shell without the producer loaded, naive backup restore that mixes a
fresh app.db with a stale metadata or stale journals, direct file edits)
bypass these assumptions entirely. Don't do them. Direct DDL through a
producer-loaded SQLite client is replicated only when that producer was
configured with a schema log; public `sqlite.Open`, `syzy daemon`,
and extension paths all expose schema-log-backed DDL. Its contract is defined
in [DDL.md](DDL.md). App-local metadata tables whose names start with `_` remain
non-replicated. Backup discipline: capture `app.db`, `app.db-syzy`, and both
journal directories together (filesystem snapshot, stopped-node copy).
Recovery from metadata-only loss is `syzy_clone` from a peer.

### DML & Schema

- Replicated tables must declare `PRIMARY KEY`. `UPDATE` modifying a
  PK column is replicated as `DELETE` of old_pk + `INSERT` of new_pk
  in the same Changeset (one Stamp). The two paths are equivalent;
  unifies rowid and `WITHOUT ROWID` behavior. Footgun: a concurrent
  non-PK column update against the old_pk lands on the tombstoned row
  and is not forwarded to new_pk — there is no row identity beyond
  the PK.
- `UNIQUE` constraints (column, table, or `CREATE UNIQUE INDEX`)
  converge across replicas via per-value LWW: SQLite enforces UNIQUE
  locally as usual, and on concurrent inserts the row with the lower
  cell-LWW stamp has its UNIQUE columns nulled at the winner's stamp.
  Member columns must be nullable; UNIQUE on `BLOB` columns and
  *eventual* partial unique indexes are rejected at DDL apply
  (coordinated partial indexes are supported). See
  [DDL.md](DDL.md#unique-keys). A successful local insert
  is **not** a globally-exclusive reservation — apps that need
  exclusive ownership should derive the PK from the unique field or
  use an external reservation service.
- Foreign keys are useful local write guards, not a convergence guarantee.
  Local app commits may enforce them; inbound apply does not rely on FK
  enforcement, and public `Open` does not enable it on the apply
  connection. Remote convergence can still produce FK violations, so
  operators audit with `PRAGMA foreign_key_check` if they rely on
  referential integrity.
- User triggers replicate as schema objects in the schema-log-backed DDL
  path and fire on inbound apply. Trigger-induced DML is not captured in
  the source changeset; each replica re-derives it locally, so trigger
  bodies must be deterministic and depend only on replicated state.
  Cascade FK actions are represented by synthesized triggers when the
  DDL schema-log/AppHelper path is wired.
- Generated columns (STORED and VIRTUAL) replicate as derived state:
  the wire never carries the generated value; receivers' SQLite
  recomputes from the source columns. Relies on SQLite's
  `SQLITE_DETERMINISTIC` contract — a UDF that lies about determinism
  causes silent divergence.
- `CREATE VIRTUAL TABLE` / `DROP VIRTUAL TABLE` replicate as opaque SQL;
  every replica needs the named module and matching options, or apply
  fails (`ddl_apply_failed`). Shadow tables are local-only per replica.
  Vtable DML is not captured (preupdate doesn't fire) — use vtables as
  derived indexes, not primary storage.
- Explicit transactions admit any number of DDL statements, but they
  must all precede any DML in that transaction; DDL after DML, DDL
  under a `SAVEPOINT` scope, and cascade-FK `CREATE TABLE` inside a
  transaction are rejected.
- Massive transactions hold the journal append on the writer thread
  for the duration of the touch buffer's encoding work in wal_hook
  (proportional to row count). They are not capped by the default
  segment target; an oversized record gets its own larger segment.
- `main` database only; ATTACHed and `temp` writes are local-only.

### Blob Writes

`sqlite3_blob_write()` replicates as per-range LWW on every public
surface. `sqlite.Open` allocates a per-node read-only `BlobRead` aux
conn for the in-process drainer, and the daemon allocates one per
attached secondary drainer to materialize patches from extension
writers. The row-LWW length constraint, the `WITHOUT ROWID` exclusion,
the NULL/DEFAULT-on-non-blob-columns rule, the unbounded-growth caveat
for pure-patch workloads, and the recovery caveat for standalone
patches are documented in [BLOB_PATCH.md](BLOB_PATCH.md).

### DDL

- Direct DDL through the producer is supported when a schema log is
  configured, but every replicated DDL reserves the next global
  `schema_seq` before local commit. If reservation fails, the local DDL
  rolls back. `sqlite.Open` and `syzy daemon` expose this schema-log path;
  the extension configures it from the cluster or schema-log environment.
- DDL is typed catalog mutation plus raw SQL for audit, not opaque SQL replay.
  Stable table/column IDs make renames map late DML forward and drops become
  deterministic tombstones. See [DDL.md](DDL.md).
- DML that arrives before its required schema is held by the broker's
  subscribe loop until schema catch-up pulls the catalog forward. A
  future refinement attaches gating to `applied_gaps` so a gated seq
  doesn't advance the contiguous frontier — see
  [Not Yet Implemented](#not-yet-implemented).
- DDL support and rejection rules are defined in
  [DDL.md#supported-ddl](DDL.md#supported-ddl).

### Hooks

- App-installed `preupdate`/`commit`/`rollback`/`wal`/`trace_v2` hooks on
  the writer silently disable replication (one hook each per connection;
  DDL capture uses `trace_v2`).
- `wal_hook` install disables autocheckpoint on the writer conn (the
  trampoline's threshold checkpoint stands in, unless the embedder disables
  it because a publisher owns WAL bounding). `PRAGMA wal_autocheckpoint=N`
  on that conn re-enables SQLite's own hook and silently uninstalls ours —
  don't. Other connections can still checkpoint `app.db`; on a published
  database that forces a loud rebaseline (see the WAL recycle ownership
  rule above).

### Operational

- `syzy_clone` is heavy (full `app.db` copy); plan as a maintenance op for
  10–100 GB databases.
- Segment-level GC gates on the durable snapshot marker, the sealer
  watermark (drained origins), and a retention age floor; whole-journal
  reaping consults peer frontiers. See [PRUNING.md](../../docs/PRUNING.md).

## Not Yet Implemented

Specified behavior not implemented yet:

- **Schema-gated `applied_gaps` extension.** Gating is currently a
  broker-held retry loop; folding it into `applied_gaps` keeps the
  contiguous frontier from blocking on a deferred seq.
- **`apply_issue` diagnostics.** The diagnostic table is specified
  but not written by the apply path.
- **SQL admin functions.** `syzy clone`, `syzy status`, and `syzy check`
  exist as CLI commands. SQL-callable `syzy_status()` / `syzy_prune()`,
  a pruning CLI, and an explicit `syzy_init` surface are not implemented.
- **Tombstone GC and `blob_range_clock` compaction.** The snapshotter
  performs segment-level GC only; HLC-driven metadata tombstone
  pruning per [PRUNING.md](../../docs/PRUNING.md) is not yet implemented.
- **Per-segment `max_seq` for finer-grained GC.** Segment GC is
  per-origin all-or-nothing past the snapshot marker. Tracking per-
  segment `max_seq` would let GC unlink earlier segments even when a
  later one must be retained.
- **Coordinated-uniqueness reclaim dependencies.** Reclaim uses a
  conservative time-window cooldown (≥ the bounded-staleness deadline)
  rather than an exact causal-stability signal (the PRUNING.md horizon
  design); the precise variant would free a just-released value the
  instant it is cluster-stable instead of after the window. A leaderless per-value
  object-store CAS backend is admitted by the `Registry` interface but not
  built. See [Coordinated Uniqueness](#coordinated-uniqueness).

## Implementation Footprint

The hot path is C because preupdate must avoid cgo per row. C owns hook
callbacks, DML payload buffering, schema-cache lookup, and the `uuidv7`
SQL function. The `gen_id` SQL function is a thin C trampoline that
delegates to Go for partition selection state (Go owns the per-(db,table)
partition map and atomic counter). Go in the producer owns HLC/`Seq`
allocation through `nodestate.Cache`, journal append, the drainer
goroutine that builds Changesets and advances cache state, and intent
recovery for DDL. Go in the broker owns transport, inbound apply
through `applyPayloadCache`, gap-fetch planning, and schema catch-up
when configured. The snapshotter lives in `internal/nodestate` and is
owned by the node lifecycle (not the broker).

Build dependency: SQLite. No external hashing library.
