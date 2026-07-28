// Package quarantine is the replication-core policy for deterministic inbound
// apply failures: a
// changeset whose apply fails deterministically (same bytes → same error) is
// persisted to the metadata store and the per-origin frontier advanced past it
// (liveness), capped per origin (data-safety: a flood of failures for one
// origin more likely signals real corruption than an isolated cross-origin
// gap). A later sweep re-applies each entry with the caller's force semantics
// and clears the ones that now land. What counts as "deterministic" and how a
// force-apply runs are the broker's to define.
//
// Unrelated to the coordinated-uniqueness "quarantine-until-stable"
// release window (unique/reservation.go), which shares only the word.
package quarantine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/syzylog"
)

// DefaultCap bounds resident quarantine entries per origin.
const DefaultCap = 128

// Policy binds the shared behavior to one node's stores. Zero Cap means
// DefaultCap; nil Log discards.
type Policy struct {
	Meta  *metadata.Store
	Cache *nodestate.Cache
	Cap   int
	Log   *slog.Logger
}

func (p Policy) cap() int {
	if p.Cap > 0 {
		return p.Cap
	}
	return DefaultCap
}

func (p Policy) log() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return syzylog.Default()
}

func originHex(o crdt.Origin) string { return fmt.Sprintf("%016x", uint64(o)) }

// Quarantine persists a deterministically-failing changeset, advances the
// per-origin frontier past it (so later seqs flow), and returns true. Returns
// false when quarantine is unavailable (no Meta) or the per-origin cap is
// exceeded — the caller then keeps its hard failure.
//
// The frontier advances AFTER the durable store write: a crash in between
// leaves the entry stored but the frontier un-advanced, so the poison
// re-arrives and re-quarantines idempotently — never the reverse (advanced but
// lost), which would silently drop the changeset.
func (p Policy) Quarantine(cs *crdt.Changeset, payload []byte, applyErr error) bool {
	if p.Meta == nil {
		return false // no durable store; cannot quarantine safely, keep the block
	}
	origin, seq := cs.Dot.Origin, cs.Dot.Seq
	n, err := p.Meta.CountQuarantineByOrigin(origin)
	if err != nil {
		p.log().Error("quarantine: count failed; keeping hard failure",
			"origin", originHex(origin), "seq", uint64(seq), "err", err)
		return false
	}
	if n >= p.cap() {
		p.log().Error("quarantine: cap exceeded; halting origin (likely real corruption, not a cross-origin gap)",
			"origin", originHex(origin), "seq", uint64(seq), "cap", p.cap(), "err", applyErr)
		return false
	}
	if err := p.Meta.PutQuarantine(origin, seq, payload, applyErr.Error(), time.Now().UnixMicro()); err != nil {
		p.log().Error("quarantine: store failed; keeping hard failure",
			"origin", originHex(origin), "seq", uint64(seq), "err", err)
		return false
	}
	p.Cache.MarkApplied(origin, seq, cs.Stamp.Clock)
	p.log().Warn("quarantine: advanced past a deterministic apply failure, deferring re-apply",
		"origin", originHex(origin), "seq", uint64(seq), "err", applyErr)
	return true
}

// Retry re-applies every quarantined changeset once via apply (which must
// bypass the applied-frontier short-circuit — the frontier already advanced at
// quarantine time). An entry that now applies cleanly is removed; one that
// still fails deterministically (per deterministic) is kept for a later round
// with its attempt count bumped; a transient failure keeps it untouched; an
// undecodable entry is dropped (it can never apply). Best-effort.
func (p Policy) Retry(ctx context.Context, deterministic func(error) bool,
	apply func(ctx context.Context, cs *crdt.Changeset, payload []byte) error) {
	if p.Meta == nil {
		return
	}
	entries, err := p.Meta.ListQuarantine()
	if err != nil {
		p.log().Error("quarantine: list failed", "err", err)
		return
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		cs, derr := crdt.Decode(e.Payload)
		if derr != nil {
			p.log().Error("quarantine: dropping undecodable entry",
				"origin", originHex(e.Origin), "seq", uint64(e.Seq), "err", derr)
			_ = p.Meta.DeleteQuarantine(e.Origin, e.Seq)
			continue
		}
		aerr := apply(ctx, cs, e.Payload)
		switch {
		case aerr == nil:
			p.log().Info("quarantine: entry applied cleanly on retry; cleared",
				"origin", originHex(e.Origin), "seq", uint64(e.Seq))
			_ = p.Meta.DeleteQuarantine(e.Origin, e.Seq)
		case deterministic(aerr):
			_ = p.Meta.BumpQuarantineAttempt(e.Origin, e.Seq)
		default:
			// Transient (locked / conn / schema-behind); keep, retry next round.
		}
	}
}
