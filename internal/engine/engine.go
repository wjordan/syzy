// Package engine defines narrow ports between Syzy's engine-neutral core and a
// concrete database adapter. Dependencies point inward: adapters import the
// core, while the core never imports an adapter.
//
// Two seams carry the replication traffic:
//
//   - Capture streams local (non-syzy-origin) commits as fully-built
//     Changesets, in commit order, for the orchestrator to broadcast.
//   - Applier writes one decoded peer Changeset in a single transaction.
//
// Both seams share the node's nodestate.Cache (Seq/HLC allocation, per-row
// RowState arbitration, frontier + idempotency), so the CRDT logic is reused
// unchanged across engines; only the SQL/decoding differs per adapter.
//
// Why Capture emits a *crdt.Changeset (not a half-built record batch the core
// finishes): Seq and HLC must be allocated atomically with the adapter-owned
// replication checkpoint (a SQLite app_txid, a Postgres confirmed_flush LSN).
// Splitting Dot/Stamp assignment into the orchestrator would let a crash
// reorder them against the checkpoint and re-derive a different Seq for a
// redelivered transaction.
package engine

import (
	"context"

	"github.com/wjordan/syzy/crdt"
)

// Marker is an opaque, engine-specific resume position: a SQLite app_txid, a
// Postgres confirmed_flush LSN. The core treats it as bytes.
type Marker []byte

// Sink receives one fully-built local Changeset in commit order. Returning an
// error stops the capture loop; the changeset is not acked, so it is
// redelivered on resume.
type Sink func(ctx context.Context, cs *crdt.Changeset) error

// Capture streams local commits as Changesets. Run drives the loop until ctx
// is cancelled; Checkpoint/Ack manage the durable resume position, which the
// orchestrator advances only after a changeset is durably broadcast/journaled.
type Capture interface {
	Run(ctx context.Context, sink Sink) error
	Checkpoint() (Marker, error)
	Ack(Marker) error
}

// Applier writes one decoded peer Changeset. Implementations are idempotent
// (IsAppliedRemote dedupes by Dot) and arbitrate each record by (CL, Stamp)
// LWW against the shared Cache, in one engine transaction.
type Applier interface {
	Apply(ctx context.Context, cs *crdt.Changeset) error
}

// Engine bundles the seams a sidecar binary wires to one database.
type Engine interface {
	Capture() Capture
	Applier() Applier
	Close() error
}
