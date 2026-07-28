# SQLite Incremental BLOB Replication

This document specifies how Syzy captures
`sqlite3_blob_write()` mutations, materializes compact `blob_patch` records,
and applies winning byte ranges through SQLite. The record and range-LWW
contract is authoritative in [BLOB_PATCH.md](../../docs/BLOB_PATCH.md); the
layer composition is in [CRDT.md](../../docs/CRDT.md#layer-composition).

The public [`crdt.IntervalMap`](../../crdt/interval.go) implements the
range primitive. This document owns SQLite hook evidence, `BlobRead`,
`sqlite3_blob_open`, placeholder behavior, and crash ordering. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the surrounding lifecycle and
[DDL.md](DDL.md) for stable SQLite catalog IDs.

Full-row UPDATE replication is fine for most workloads but reships the whole
blob on every patch — unworkable when the blob is a file body and writes are
sparse (SyzyFS). `blob_patch` carries only mutated ranges; per-range LWW
resolves concurrent writers without coordination.

## Implementation Status

End-to-end on the public surface. `sqlite.Open` allocates a per-node
read-only `BlobRead` aux conn, the daemon allocates one per attached
secondary drainer, and the in-process producer drainer materializes
`blob_patch` records via `sqlite3_blob_open`. Loadable-extension writers
emit blob-write evidence through the cross-process journal; the daemon's
secondary drainer materializes patches against its own `BlobRead`
connection. Full-row BLOB `INSERT`/`UPDATE` values still replicate as
ordinary DML; per-byte patches and full DML reconcile against active
`blob_range_clock` entries (see [Apply](#apply)).

## `blob_range_clock`

Metadata table — at most one row per `(table_id, pk_blob)` while concurrent
writers race on a row's blob columns:

```sql
CREATE TABLE blob_range_clock (
  table_id   BLOB NOT NULL,
  pk_blob    BLOB NOT NULL,
  intervals  BLOB NOT NULL,        -- packed: (column_id, n_intervals u16,
                                   --   [start u64, end u64, hlc u64, origin u64]+)+
  PRIMARY KEY (table_id, pk_blob)
) STRICT, WITHOUT ROWID;
```

`intervals` lists, per blob column, sorted non-overlapping byte ranges
tagged with the `Stamp` `(hlc, hlc_origin)` of the winning patch.
**Invariant:** every entry strictly dominates the effective parent
Stamp — `Cells[col]` if present, else `Base`
([CRDT.md](../../docs/CRDT.md#layer-composition)).
Absent ranges inherit that parent Stamp. Entries are absorbed when a
full-column/full-row write advances the parent Stamp (`Prune(floor)`);
frontier-driven compaction is in [PRUNING.md](../../docs/PRUNING.md). Empty for
typical INSERT/UPDATE workloads.

## Wire Format

`blob_patch` is operation 4 in the canonical
[DML record format](../../docs/PROTOCOL.md#dml-records):
the record carries `table_id`, `pk_blob`, the writer's view of the row's
`cl` (must be odd — patches only apply on live rows), then one trailing
block:

```text
16 bytes blob_column_id
varint   range_count
[range_count]:
  varint  offset
  varint  byte_len
  N bytes range_bytes
```

A `blob_patch` and a full-row DML record for the same `(table_id, pk_blob)` are
never paired in one changeset — wal-hook materialization emits the final
full row when ordinary DML also touched the row.

## Capture

`sqlite3_blob_write()` fires `preupdate_hook` with `op=DELETE` and
`sqlite3_preupdate_blobwrite() >= 0` returning the column index. OLD bytes
are reachable via `sqlite3_preupdate_old`; the NEW blob is not (no `_new()`
for blob_write). C copies OLD bytes; Go diffs and encodes in the wal-hook —
cgo hot path stays identical to a normal DML row.

**Preupdate (C).** Append `(table_id, pk_blob, blob_column_id, rowid,
old_blob_bytes)` to a per-connection blob-write buffer; skip the normal
DML touch path for this preupdate fire.

**Wal-hook (Go), per blob-write entry:**

0. Dedupe entries by `(table_id, pk_blob, column_id)`, keeping the *earliest* OLD
   bytes (multiple `sqlite3_blob_write` calls on one column in one txn
   each appended an entry; only the pre-txn OLD is a valid diff baseline
   against the post-commit NEW).
1. Open the post-commit blob (`sqlite3_blob_open`, read-only) and read NEW
   bytes. This requires the producer's `BlobRead` connection; if it is
   nil, current producer code drops standalone blob-write entries.
2. If ordinary DML for the same `(table_id, pk_blob)` materializes a full-row record,
   drop the entry; the full-row payload carries the post-blob_write state.
3. Otherwise, diff old vs new (linear, small number of contiguous ranges)
   and append a `blob_patch` record to the changeset payload.

One blob-write entry → 0 or 1 `blob_patch` record.

The blob-write buffer participates in
[savepoint discipline](ARCHITECTURE.md#capture) alongside the DML buffer.

### Capture paths

There are two capture paths into the touch journal, one chosen by call
site:

**Intent path** (Syzy-owned surfaces). Triggered by `tx.BlobWriteAt` and
the `syzy_blob_write(table, rowid, col, offset, bytes)` SQL function.
The wrapper appends a `SYZY_OP_BLOB_INTENT` record carrying only
`(table, rowid, column, offset, length)` to the touch journal, sets a
per-conn `suppress_blob_capture` flag, and runs the underlying
`sqlite3_blob_write`. The preupdate trampoline observes the flag and
skips its OLD-image emission. The drainer reads NEW bytes for the
recorded range from the post-commit DB via `s.blobRead` and emits a
`blob_patch` record. Touch-journal cost is proportional to write count,
not column size.

**OLD-image fallback** (raw `sqlite3_blob_write`). Any blob_write that
didn't go through one of the wrappers above fires the preupdate hook
without the suppress flag, so `SYZY_OP_BLOB_WRITE` stores the row's OLD
column values — including the full pre-write target BLOB. The drainer
diffs OLD vs post-commit NEW into ranges and emits the same `blob_patch`
record. Cost scales with stored BLOB row length, not write length, so
an oversized row amplifies a small append; the journal segment format
sizes the segment to fit oversized records, so this is latency/storage
pressure rather than a correctness limit. Used by third-party SQLite
extensions, raw C-API callers, and the loadable-extension shape where
the host's `sqlite3_blob_write` is unwrappable.

Both paths converge on the same on-the-wire `blob_patch` format. Within
a single transaction, dedup keeps a row's earliest OLD (image-path) and
appends per-fire intent ranges (intent-path); a row also touched by full
DML drops both — the DML record's NEW image carries the post-write
state.

## Public API

Go callers use `sqlite.NewDB(node)` for a facade over the node-owned
single writer pool, with explicit incremental blob writes:
`tx.BlobWriteAt(table, column, rowid, offset, data)` inside an
ordinary transaction (see [`sqlite/db.go`](../db.go)).

`BlobWriteAt` is intentionally
rowid-based, matching `sqlite3_blob_open`. Callers that address rows by
logical primary key should `SELECT rowid` in the same transaction before
calling it; Syzy's capture hook maps the blob write back to the stable
catalog table/column IDs and PK blob for replication.

## Crash Recovery

Standalone blob writes have one narrow recovery wrinkle: the compact
`blob_patch` bytes are materialized by diffing captured OLD bytes
against the current post-commit blob. The OLD bytes captured by
preupdate live only in process memory; the journal record carries the
raw touch buffer (which references those OLD bytes), and the drainer
reads it post-commit when the app blob is already at its NEW value. A
host-crash window between WAL fsync and the journal record's flush
leaves the `blob_patch` unmaterialized; the row's content is still
consistent because `app.db` retains the new bytes. Only an
unreadable/missing blob from disk corruption requires manual repair
or `syzy_clone`. (The journal append happens inside `wal_hook` after
the WAL fsync, so this is a host-crash window only — process death
between WAL fsync and journal append cannot occur.)

Blob writes that materialize as a full-row DML record are replayable
from the self-origin journal — the wire-format Changeset bytes (with
the full image) live there until peer-aware GC unlinks the segment.

## Apply

Slots into [Inbound Apply](ARCHITECTURE.md#inbound-apply). The app.db
write commits first; then the resulting `blob_range_clock` side effect
is persisted in a metadata transaction. Row/cell parent Stamps are
advanced only by full DML records, not by `blob_patch`.

`new_rc = changeset.Stamp`; `rc` is the row's current effective parent
Stamp for the blob column (`Cells[col]` if present, else `Base`,
defaulting to `Stamp{}` if absent); `map` is the row's
`blob_range_clock` loaded once.

### `blob_patch` Records

```text
if RowState.CL is even (tombstoned): skip   -- patches don't resurrect; see below
ensureRow(table, pk_cols, pk_vals)
rowid = SELECT rowid FROM <table> WHERE <pk> = ?
ensureBlobLen(row, col, max(range.end))
for each range in record:
  won = map[col].Apply(range.start, range.end, c=new_rc, baseline=rc)
  for w in won:
    blob.WriteAt(range.bytes[w.Start-range.start:w.End-range.start], w.Start)
stage blob_range_clock save (DELETE if every column's map is empty, else UPSERT)
-- row/cell parent Stamp unchanged
```

`IntervalMap.Apply` performs the parent-Stamp LWW check. If `new_rc`
does not dominate `rc`, uncovered gaps do not fill; any overlapping
entries that `new_rc` fails to dominate survive. A fully dominated patch
is still accepted for idempotency/frontier progress, but writes no bytes
and leaves no range-clock entry behind. Overrides accumulate above the
row/cell parent Stamp until a full write absorbs them. Patches arriving
before the base row create a placeholder via `ensureRow`; concurrent
patches converge regardless of arrival order.

**Tombstones are terminal for `blob_patch`.** A patch arriving while the
row's `CL` is even (tombstoned) is dropped, even if `new_rc > rc` —
`blob_patch` carries only partial row data and can't legitimately
resurrect. Required for convergence: a DELETE unconditionally clears
`blob_range_clock`, so without this rule a patch arriving after the
tombstone on one replica would resurrect bytes that the other replica
(which applied the patch *before* the DELETE) had cleared. Resurrection
must come via a full-row INSERT/UPDATE that bumps `CL` to the next-odd
generation.

### `insert`/`update` With Active `blob_range_clock`

When the row has interval-map entries, blob columns route through
`IntervalMap` instead of UPSERTing wholesale:

```text
UPSERT non-blob columns via INSERT ... ON CONFLICT DO UPDATE
  (omitting blob columns)
for each blob column in record:
  new_len = len(record.value)
  cur_len = SELECT length(<col>) FROM <table> WHERE <pk> = ?

  -- Entries dominating new_rc survive past record.length.
  surviving = [e in map[col] where e.Stamp > new_rc]
  effective_len = max(new_len, max(e.end for e in surviving, default=0))

  if cur_len > effective_len:
    UPDATE <table> SET <col> = substr(<col>, 1, effective_len) WHERE <pk> = ?
  elif cur_len < effective_len:
    ensureBlobLen(row, col, effective_len)
  map[col].Clip(effective_len)

  -- Zero-fill gaps in [new_len, effective_len) not covered by `surviving`,
  -- so substr-shrunk and ensureBlobLen-extended replicas agree byte-for-byte.
  for gap in [new_len, effective_len) minus {e ∩ [new_len, effective_len) for e in surviving}:
    blob.WriteAt(zeros[0:gap.len], gap.start)

  won = map[col].Apply(0, new_len, c=new_rc, baseline=rc)
  for w in won:
    blob.WriteAt(record.value[w.Start:w.End], w.Start)
  map[col].Prune(new_rc)
stage blob_range_clock save (DELETE if every column's map is empty, else UPSERT)
stage parent row/cell Stamp = new_rc
```

`effective_len` makes a shrinking UPDATE order-independent against a
higher-Stamp patch at higher offsets: both replicas converge on the same
length and bytes regardless of arrival order. `Prune(new_rc)` drops
entries the full-row update absorbed.

`delete` drops the `blob_range_clock` row alongside bumping `row_clock.cl`
to the next-higher even value ([CRDT.md](../../docs/CRDT.md#causal-length-cl)).

### Helpers

`ensureRow` inserts a placeholder row (PK + empty blobs + schema
defaults; its `row_clock` stays absent so the canonical INSERT wins
row-LWW) and `ensureBlobLen` zero-extends a column to the needed
length — see [`internal/broker/apply_reconcile.go`](../../internal/broker/apply_reconcile.go).

### Per-Record Failure

A `blob_patch` whose apply fails aborts the Changeset's single apply
transaction, preserving Read Atomic visibility: none of the Changeset's
records become visible. There is no "continue-with-overlay" path. A
transient failure (I/O, `SQLITE_BUSY`) is returned to the transport and
retried in place. A deterministic failure — one a retry can never fix —
parks the changeset's exact payload bytes in the `apply_quarantine`
metadata table and advances the origin's frontier past it, so one bad
record cannot starve later records from that origin; quarantined
entries are force-re-applied every fetcher round and drain
automatically once the missing dependency lands.

### Local Commit

The producer drainer computes `blob_range_clock` side effects after
materializing the transaction's records. Row/cell clocks advance in
`nodestate.Cache` and are snapshotted later; `blob_range_clock` updates
are persisted immediately in a metadata transaction:

- For each row touched by a DML record: `DELETE FROM blob_range_clock
  WHERE table_id=? AND pk_blob=?` — the local Stamp dominates every prior
  interval entry for row-clocked columns.
- For each row touched only by a standalone `blob_patch`: load the row's
  map, run `IntervalMap.Apply(start, end, c=local_stamp,
  baseline=effective_parent_stamp(row, column))` per range, save (DELETE
  if empty, UPSERT otherwise); row/cell Stamps unchanged. The local
  Stamp is freshly allocated and strictly dominates the baseline, so no
  LWW skip fires.

## Per-Range LWW

`IntervalMap` is the per-byte-range layer's primitive (interface in
[`crdt/interval.go`](../../crdt/interval.go)). **Invariant: every entry
strictly dominates the parent row/cell Stamp**; entries that fail this
are pruned at apply time. **Run-coalescing invariant:** byte-contiguous
adjacent entries with equal Stamps merge into a single entry.

Operations:

- **`Apply(start, end, c, baseline)`** integrates a write at Stamp `c`
  over `[start, end)`. Existing entries overlapping the range: if `c`
  strictly dominates the entry's Stamp, overwrite the overlap with `c`
  and emit it as a "won" range; otherwise keep the existing Stamp for
  the overlap. Uncovered gaps inside `[start, end)`: if `c` strictly
  dominates `baseline`, emit a won range and add an entry at `c`.
  Adjacent entries merge per the run-coalescing invariant. The caller
  writes only won sub-ranges via `sqlite3_blob_write`. `baseline` is the
  current effective parent Stamp.
- **`Prune(floor)`** drops entries with `Stamp <= floor`. Called after
  the parent row/cell Stamp advances on a full write.
- **`Clip(maxEnd)`** drops entries with `start >= maxEnd` and truncates
  `end > maxEnd` to `maxEnd`. Called by full-row UPDATE with
  `maxEnd = max(record.length, max-end of entries with Stamp above new_rc)`,
  so entries strictly dominating the UPDATE are never clipped (see
  [Sharp Edges](#sharp-edges)).

After Apply (and any Prune/Clip), if the map is empty across all
columns, DELETE the `blob_range_clock` row; otherwise UPSERT.

Idempotent replay and concurrent-origin convergence fall out of the
algorithm: replay finds entries at exact equal Stamps (`Dominates` false
→ no-op); concurrent patches commute on disjoint ranges and resolve by
per-range LWW on overlap.

## Compaction

`blob_range_clock` shrinks via two mechanisms:

- **Full-write absorption.** A full-row or full-blob-column `insert`/`update`
  advances the relevant row/cell parent clock and `Prune(floor)` drops every
  entry it dominates; `delete` drops the row's `blob_range_clock` entry
  entirely. Most workloads compact this way on ordinary writes.
- **Frontier-driven compaction.** The metadata prune loop in
  [PRUNING.md](../../docs/PRUNING.md) consolidates entries below a cluster-stable
  HLC horizon. Convergence-safe: a future patch with a competing clock
  cannot arrive below that horizon.

There is no automatic time-based compaction. A row written only via
`blob_patch` accumulates one entry per disjoint un-absorbed range —
even with a single writer producing sparse patches over time.
Operators with persistently patch-only workloads should issue periodic
full-row UPDATEs on hot rows to compact (DELETE+INSERT with the same
PK is incompatible with blob-patch convergence — see Sharp Edges).
Row-level LWW is not windowed.

## Sharp Edges

- **Blob length is row-LWW with per-range overrides.** A higher-Stamp
  patch past a shrinking UPDATE survives, so the visible blob may exceed
  the UPDATE's stated length. Resolve object size via a separate
  row-LWW field, not `length(blob)` (SyzyFS uses `fs_inode.size`).
- **DELETE is terminal for concurrent `blob_patch` records** at the same
  CL generation. A DELETE bumps `cl` to the next-higher even value and
  drops `blob_range_clock`; patches arriving while `cl` is even are
  dropped (they cannot legitimately resurrect — `blob_patch` carries
  partial row data only). To preserve patches through row-content
  removal, use `UPDATE … SET <blob> = x''` and reserve DELETE for
  permanent removal. SyzyFS truncate uses the empty-blob UPDATE;
  unlink uses DELETE.
- **PK reuse across CL generations is convergent but lossy.** A DELETE
  bumps `cl` to even; a subsequent INSERT with the same PK bumps to the
  next-higher odd `cl`, establishing a fresh row generation. Per-cell
  and per-byte-range overrides scoped to the prior generation are
  implicitly tombstoned ([CRDT.md](../../docs/CRDT.md#causal-length-cl)). A `blob_patch`
  generated against the prior generation may still apply against the
  new generation if its Stamp dominates the new `Base` — convergent but
  semantically a write to the wrong row. Prefer globally-unique PKs
  (`uuidv7`, or `gen_id`). A `gen_id` post-restart partition probe can in principle
  pick a fully-deleted partition and recycle IDs at ~2⁻³⁰ per restart per
  table, low enough to ignore for most workloads.
- Requires rowid tables; `WITHOUT ROWID` has no rowid for
  `sqlite3_blob_open` and replicates via full-row UPDATEs only. Use
  `INT PRIMARY KEY NOT NULL DEFAULT (gen_id('t'))` for integer IDs with
  `blob_patch`.
- Tables receiving `blob_patch` must allow NULL/DEFAULT on every non-PK
  non-blob column. `BLOB NOT NULL` columns are compatible because
  placeholders seed them with `x''`.
