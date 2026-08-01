# Pruning and offline-peer contract

This document specifies the SQLite retention realization and the shared
offline-peer contract. Postgres journal retention is described in the
[Postgres engine architecture](postgres.md).

Bounded retention for Syzy's logical history and CRDT state. Per-origin
journal files are pruned by segment-level `unlink()`; metadata
tombstones and `blob_range_clock` overrides are pruned by HLC
horizon. The object-store seal — and, in no-bucket mode, the
peer-frontier applied signal — gates journal GC so a peer that
needs catch-up can still find the bytes.

Syzy's journal, metadata, snapshot, and anti-entropy services implement the
rules described here. Physical backup has separate retention rules. The CRDT
primitives this doc references (the stability horizon,
the offline-deadline contract, CL-driven tombstone collapse) are
defined in [CRDT.md](CRDT.md). The HLC-horizon tombstone pruner in
this doc is design-stage: the horizon rules below are the spec for
code that has not been built yet.

## Problem

Three durable stores grow unboundedly without intervention:

| Store | Growth driver | Bounded by |
|---|---|---|
| Per-origin journals (`<dir>/jrn/`, `<dir>/mirror/origin_*/`) | cluster write rate × time | peer-aware segment GC |
| Metadata `row_clock` tombstones | delete rate × time | HLC-horizon tombstone prune |
| Metadata `blob_range_clock` | concurrent overlapping or sparse `blob_patch` writes | full-row writes (immediate) + frontier-driven compaction |

`row_clock` live entries are bounded by current row count. `cell_clock`
and the future `apply_issue` diagnostics follow the same retention
patterns as the row-level tables — see [Schema replication](SCHEMA.md) and the
current [SQLite-backed metadata realization](../sqlite/docs/ARCHITECTURE.md#metadata-schema).

`syzy_schema_event` mirrors the schema log's events locally;
it is small (one row per replicated DDL, KB-scale per row) and kept
indefinitely.

Keeping everything forever is unsustainable past a few days for any
non-trivial write rate.

## Two boundedness properties

The two retention concerns decouple cleanly:

- **Recovery time** ≤ `snapshot_interval × (commit_rate + apply_rate)`.
  Tunable via `Interval` on `nodestate.SnapshotterConfig`.
  Independent of cluster age.
- **Disk usage** ≈ `retention window × write_rate` per origin.
  Tunable via `Config.SnapshotRetention` and the snapshotter's tick
  rate.

Neither grows unboundedly while snapshots keep landing.

Drain completion is not a retention boundary. The producer drainer may
advance `cache.snapshot_markers[self]` after it has decoded records and
updated in-memory state, but it must not unlink journal segments. A
segment becomes eligible for physical deletion only after the
snapshotter has durably written the marker that covers it and the
peer-safety gate below is satisfied.

## Safe-to-unlink rules — journal segments

Two paths unlink journal segments, with different gates.

**Snapshotter segment GC** (`nodestate.Snapshotter.gcSegments`, the
post-checkpoint pass, all origins): a segment is unlinked only when
every record it contains is:

1. Reflected in this node's last durable metadata snapshot
   (`segment.max_offset ≤ snapshot_markers[origin]`), AND
2. for drained origins (self, and extension origins this node
   drains): sealed to the object store through our head
   (`Sealer.ContiguousSealedSeq ≥ head`). The journal is the
   pre-seal buffer — before the seal these records exist nowhere
   else. No sealer (no bucket) ⇒ drained origins are never
   segment-GC'd. AND
3. older than the retention age floor (`Config.SnapshotRetention`,
   default 72h). The floor lets a peer offline for less than the
   window catch up incrementally instead of rebaselining.

In-flight readers are protected by the journal package's segment
reference counting. Unlinking is segment-granular and all-or-nothing
per origin: `journal.RetainAfterAged(off, olderThan)` drops segments
strictly before the one containing the marker AND older than the
floor. See `nodestate.Snapshotter.gcSegments`.

**Reaper self-log trim** (`sqlite/reaper.go` →
`mirror.RetainSealed`): on each reaper tick, the self-journal is
additionally trimmed on the seal gate ALONE — segments whose
seek-index seq ceiling is at or below
`Sealer.ContiguousSealedSeq(self)` are unlinked, always keeping the
newest (active) segment. Rules 1 and 3 do not apply on this path:
neither the snapshot marker nor the retention age floor is
consulted. Everything at or under the contiguous sealed tip is
durable in the bucket, so a peer asking for a trimmed seq falls
back to the object store via its gap-filler chain. No sealer (no
bucket) ⇒ no durable floor, and this path never trims.

Whole mirror journals of retired origins are dropped by the reaper.
A non-self journal is reapable once the origin is sealed to the
bucket through our applied tip — a behind peer then re-fetches from
the bucket, so the mirror copy is redundant. Without a bucket there
is no durable tier to wait for, so best-effort mode instead requires
every currently-connected peer to have applied through our tip
(`PeerFrontier.AllPeersApplied`). That signal is liveness-only — it
says nothing about absent peers, which is why it is trusted only
when no bucket exists; see `reapable` in `sqlite/reaper.go`.

## Safe-to-prune rules — metadata tombstones

For HLC-based pruning, the stability horizon is the minimum
`last_hlc` across our local frontier and every peer frontier for every
known origin, computed over the *current* member set. If any required
origin report is missing, skip HLC-based pruning for this pass. Margins
compare the millisecond component of packed HLCs.

A `row_clock` tombstone (`cl` even) is safe to drop when:

1. `tombstone.hlc.ms + T_threshold ≤ Horizon().Clock.ms`, AND
2. `tombstone.hlc.ms < now_ms - prune_safety_margin`.

The HLC-based check approximates frontier coverage without storing
`sender_seq` per `row_clock` entry; `T_threshold` is the safety margin
beyond the stable horizon.

A `blob_range_clock` entry (see
[BLOB_PATCH.md](BLOB_PATCH.md)) is safe to **consolidate** when:

1. `entry.hlc.ms + T_threshold ≤ Horizon().Clock.ms`, AND
2. `entry.hlc.ms < now_ms - prune_safety_margin`.

Consolidation collapses adjacent qualifying runs per
`(table_id, pk_blob, column_id)` into one entry at its max `Stamp`.
Bytes are unchanged; metadata collapses. Convergence-safe because all
known members have advanced past the stable horizon, so no future patch
can carry a Stamp below it.

`cell_clock` is not pruned by age. Opportunistic collapse is applied at
write time: a winning update that covers every live non-PK column
re-absorbs the row into its `row_clock` baseline (`Base` advances to
the write's Stamp, the row's overrides are dropped), so rows carry
`cell_clock` entries only while they have outstanding partial-column
writes. CL bumps (resurrection / tombstone) clear a generation's
overrides wholesale. Full frontier-driven stabilization is a future
addition.

## Peer frontiers

Retention decisions that involve peers use the transport's
pull-based frontier exchange: a peer answers an `opFrontier`
request on the shared mesh listener with its per-origin applied
frontier, and `tcpmesh.PeerFrontierSource` aggregates the responses
(see [TRANSPORT.md](TRANSPORT.md#bundle-listener) for the listener framing).
`AllPeersApplied(origin, seq)` reports whether every
currently-connected peer has applied through `seq` — a liveness
signal only; it says nothing about peers that are down or
unreachable. That is why the reaper trusts it only in no-bucket
mode, and why the offline-deadline contract below exists.

A custom Transport supplies the same signal by satisfying
`transport.PeerFrontierBuilder`.

## Runtime GC loop

The snapshotter (`internal/nodestate.Snapshotter`) runs the GC pass
after each successful checkpoint: it durably writes the snapshot
(rows + frontier + markers) and then unlinks, per origin, the
segments covered by the marker, subject to the sealer and age gates
above. Syzy enables GC whenever an object backend is configured,
defaulting the age floor to `DefaultSnapshotRetention`.

Tombstone GC and `blob_range_clock` compaction are planned for this
same goroutine — they share the cache's view of `Horizon()` and
don't need a separate prune timer.

No producer/drainer path unlinks journal segments. That keeps
crash-recovery ordering simple: records past the last durable
metadata snapshot remain present even if the drainer had already
processed them in memory before the crash.

## Performance properties

Segment unlink is one syscall per segment — no SQL writer-lock
contention, no WAL growth, no checkpoint pressure. Tombstone
DELETEs are batched on the PK index with yields between batches. No
VACUUM is required for normal metadata operation: WAL truncation
reclaims pages and SQLite reuses the freelist.

## Operator contract — offline-peer resurrection

Tombstone GC creates one sharp edge worth flagging loudly. This is the
offline-deadline contract from CRDT.md, in Depot bounded-staleness
form (`T_offline_deadline = T_announce + T_propagate + Δ`):

> An origin offline longer than `T_threshold + prune_interval +
> prune_safety_margin` whose writes precede a
> deletion applied during their absence may **resurrect the deleted
> row** when their late writes finally arrive. The tombstone they would
> have lost to has been GC'd; their late INSERT (CL bumped from 0 to 1
> on the receiver) looks like "first write to a never-seen key" and
> applies.

This is the cost of bounded tombstone storage. Per Bauwens MPLR'20,
stability and membership are separate concerns: pruning safety
requires explicit eviction of silent peers from the member set before
their absence can stop blocking the horizon. Operators with workloads
where extended offline periods are normal must either:

- Disable tombstone GC (set `T_threshold` very high, or skip tombstones
  in the prune loop entirely), accepting unbounded `row_clock` growth,
  OR
- Evict peers that exceed the offline window before they reconnect
  (a design-stage `syzy_evict` operation — the tombstone pruner it
  gates is itself not built yet). An evicted peer must be recovered
  via `syzy_clone` before reconnecting.

Journal retention has a similar but milder failure mode: an offline
peer whose missing changesets have been segment-GC'd cluster-wide can
no longer gap-fill via `Fetch` — the only recovery is `syzy_clone`.
This is fine if expected; surprising if not. Document the cluster's
effective "offline deadline" as a deployment parameter.

`blob_range_clock` compaction shares the same offline-window
contract: a late `blob_patch` from a peer offline past the window may
carry a Stamp below an entry the cluster has already consolidated.
The patch loses against the consolidated baseline (lost-write /
precision loss — opposite direction from tombstone resurrection but
the same root cause). Same `syzy_evict` + `syzy_clone` recovery
applies.

Schema-chain catch-up shares this contract via the schema log's
retention horizon: a peer offline past the deadline may fall below
the horizon and need `syzy_clone` to recover schema state. The
schema log's retention window should be set to comfortably exceed the
deployment's offline deadline.

## Config knobs

| Knob | Default | Purpose |
|---|---|---|
| `nodestate.SnapshotterConfig.Interval` | caller picks | Snapshot + GC tick cadence. |
| `sqlite.Config.SnapshotRetention` | `DefaultSnapshotRetention` (72h) | SQLite age floor for post-snapshot segment unlink; bounds disk to ~retention × write rate per origin while letting briefly-offline peers gap-fill incrementally. |

## Operator surface

The SQLite CLI exposes a limited status/check surface:

```bash
syzy status --db app.db
syzy check --db app.db
```

SQL admin functions are specified but not yet implemented:

```sql
SELECT syzy_prune();
-- Forces an immediate snapshot + GC pass. Returns a one-row result:
-- (segments_unlinked, tombstones_pruned, journal_size_bytes, row_clock_size_bytes)

SELECT * FROM syzy_status();
-- Reports per-origin journal sizes, oldest segment age, last
-- snapshot marker, peer frontier coverage, etc.
```

The test harness in `internal/testcluster` exposes
`Snapshotter.Trigger()` for forcing a checkpoint, and the cache's
internal state is queryable via the FrontierMap / SenderNextSeq /
SnapshotMarker getters.

## Implementation pointers

Segment GC lives in `internal/nodestate` (`Snapshotter.gcSegments`)
over `internal/journal` (`RetainAfterAged`); the self-log trim and
whole-journal reap in `sqlite/reaper.go` over `internal/mirror`
(`RetainSealed` / `Reap`). Tombstone GC and
`blob_range_clock` compaction are future work attached to the same
snapshotter goroutine. None of it touches the hot path — snapshots,
GC, and reaping run on dedicated goroutines.
