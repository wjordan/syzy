// Package transport defines the network-layer contract for shipping
// changeset bytes between syzy peers. Transport is two methods;
// optional sibling contracts (GapFiller, TipSource) live alongside.
package transport

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/wjordan/syzy/crdt"
)

// ApplyFunc accepts one changeset payload. Returning nil means the
// engine durably accepted the bytes (or idempotently skipped a dup).
type ApplyFunc func(ctx context.Context, changeset []byte) error

// ErrClosed is returned by Transport methods once the transport (or the
// topic channel backing it) has been closed. A Subscribe returning
// ErrClosed will never deliver again; resuming delivery requires
// obtaining a fresh Transport for the topic, not retrying the old one.
var ErrClosed = errors.New("transport: closed")

// Transport ships changeset bytes between peers. Engine dedupes via
// per-(origin, seq) idempotency, so duplicates and reordering are fine.
type Transport interface {
	// Broadcast publishes one local changeset; returns once accepted
	// for dispatch (not once peers have applied). The slice is safe
	// to reuse after return.
	Broadcast(ctx context.Context, changeset []byte) error

	// Subscribe invokes apply per delivered changeset until ctx is
	// cancelled, the transport hits an unrecoverable error, or the
	// transport closes (returns ErrClosed; delivery on this handle is
	// permanently over). Implementations must not return nil;
	// consumers treat a legacy nil return as ErrClosed.
	Subscribe(ctx context.Context, apply ApplyFunc) error
}

// Range names a span of one origin's sequences. Hi=0 means "everything
// past Lo, inclusive."
type Range struct {
	Origin crdt.Origin
	Lo     crdt.Seq
	Hi     crdt.Seq
}

func (r Range) OpenEnded() bool { return r.Hi == 0 }

func (r Range) Contains(seq crdt.Seq) bool {
	if seq < r.Lo {
		return false
	}
	if r.OpenEnded() {
		return true
	}
	return seq <= r.Hi
}

// GapFiller backfills sequence ranges the live Transport may not have
// delivered. apply routes through the engine's single-writer path;
// implementations may over-deliver and the engine dedupes.
type GapFiller interface {
	Fetch(ctx context.Context, ranges []Range, apply ApplyFunc) error
}

// ErrUnfilled marks a CLEAN empty Fetch: every source answered and none
// held the requested ranges. Distinguishable (errors.Is) from substantive
// failures — decode, apply, dial, corruption — so callers that expect
// emptiness (the unserveable-range probe) demote only this, never a real
// error.
var ErrUnfilled = errors.New("no source delivered the requested ranges")

// TipSource reports the highest known seq per origin from a source the
// broker hasn't observed live (typically the cluster's object store).
// Returning seq=0 for an origin means "tip unknown — skip."
type TipSource interface {
	DiscoverTips(ctx context.Context) (map[crdt.Origin]crdt.Seq, error)
}

// CoverageSource is an optional TipSource capability: per origin, the
// merged seq intervals (sorted, non-overlapping) the source can currently
// serve — for a bucket, its surviving epochs. A range intersecting no
// interval is provably unavailable from THIS source right now: not
// necessarily from peers, and not necessarily forever (a fresh origin's
// first epoch, or an upload that lands between snapshots, shows up on the
// next call — consumers reclassify every round). A non-nil map is
// authoritative for absence: an origin missing from it holds nothing at
// this source. It must therefore come from a complete listing snapshot —
// a truncated LIST would fabricate absence. Returned intervals are shared
// read-only; callers must not mutate them.
type CoverageSource interface {
	Coverage(ctx context.Context) (map[crdt.Origin][]Range, error)
}

// MergeIntervals sorts ranges by Lo and coalesces overlapping or adjacent
// ones in place, returning a CoverageSource-shaped interval list. Epochs
// seal contiguously, so a loss-free origin merges to a single interval;
// each surviving gap adds one.
func MergeIntervals(rs []Range) []Range {
	slices.SortFunc(rs, func(a, b Range) int {
		return cmp.Compare(a.Lo, b.Lo)
	})
	out := rs[:0]
	for _, r := range rs {
		if n := len(out); n > 0 && r.Lo <= out[n-1].Hi+1 {
			if r.Hi > out[n-1].Hi {
				out[n-1].Hi = r.Hi
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

// PeerStat is one directly-connected peer's transport-level locality
// and health signal. RTT/RTTVar are zero when unavailable (non-TCP
// socket, OS without TCP_INFO, fresh connection with no samples yet).
// SinceLastRecv is the kernel's last-data-recv age — a freshness
// proxy for the RTT estimate.
type PeerStat struct {
	Addr          string
	RTT           time.Duration
	RTTVar        time.Duration
	SinceLastRecv time.Duration

	// Outbound-queue health for this peer's current connection:
	// queued DATA frames/bytes not yet written, and the count of
	// frames dropped because the queue was full (recovered by the
	// receiver's catch-up chain).
	QueuedFrames  int
	QueuedBytes   int64
	OutboundDrops uint64
}

// PeerStatter is an optional sibling contract for Transports that can
// report kernel-level per-peer stats. tcpmesh.Channel satisfies it;
// in-memory and Unix-socket transports may return an empty slice.
type PeerStatter interface {
	PeerStats() []PeerStat
}

// CatchupRequest is a peer-pull request: serve every changeset whose
// (origin, seq) lands in any of Ranges, capped by MaxRecords / MaxBytes.
// A zero cap means unbounded. Server-side iteration honours both;
// clients re-request whatever the planner still wants on the next round.
type CatchupRequest struct {
	Ranges     []Range
	MaxRecords uint32
	MaxBytes   uint64
}

// CatchupSource serves CatchupRequests from a node-local journal of
// wire-format changeset payloads. Mirror.Manager (with self-mirror
// wired) satisfies this — one journal per origin we've ever produced or
// applied. write delivers each matching payload; returning a non-nil
// error from write halts the scan.
type CatchupSource interface {
	Serve(ctx context.Context, req CatchupRequest, write func(payload []byte) error) error
}

// Optional capability interfaces. Concrete Transport implementations
// (tcpmesh.Channel, etc.) satisfy as many of these as they
// can. sqlite.Open discovers capabilities via interface assertion
// instead of switching on concrete types, so new transports can
// be wired in without touching sqlite.Open.

// CatchupRegistrar accepts a CatchupSource for peers to pull from
// (typically the node's mirror.Manager). nil unregisters.
type CatchupRegistrar interface {
	SetCatchupSource(CatchupSource)
}

// PeerConnectNotifier installs a callback that fires when a peer
// becomes "interested" in this transport's scope — for tcp this is
// any new connection; for tcpmesh.Channel it is per-topic.
// nil clears.
type PeerConnectNotifier interface {
	SetOnPeerConnect(func())
}

// PeerCatchupBuilder constructs a GapFiller that pulls missing
// ranges from connected peers, dialed at their gossip address.
type PeerCatchupBuilder interface {
	PeerCatchupBuilder() GapFiller
}

// FrontierSource provides this node's applied-frontier (origin -> highest
// contiguous applied seq) to peers. Served over the peer endpoint so peers can
// discover origins they never received live and decide when an origin is fully
// replicated. The node's nodestate.Cache (AppliedTipMap) satisfies it.
type FrontierSource interface {
	Frontier() map[crdt.Origin]crdt.Seq
}

// FrontierRegistrar accepts a FrontierSource that peers query. nil unregisters
// (the node then refuses frontier queries, as an un-upgraded peer does).
type FrontierRegistrar interface {
	SetFrontierSource(FrontierSource)
}

// FrontierObservationState classifies one peer's frontier observation.
type FrontierObservationState string

const (
	// FrontierOK: the last refresh fetched this peer's frontier.
	FrontierOK FrontierObservationState = "ok"
	// FrontierError: the last refresh failed for this peer (down,
	// refusing the op, or timing out). Err carries the cause.
	FrontierError FrontierObservationState = "error"
	// FrontierUnknown: the peer is connected but no refresh has
	// queried it yet.
	FrontierUnknown FrontierObservationState = "unknown"
)

// FrontierObservation is one currently-connected peer's applied-frontier
// as last observed. Connected peers whose observation is unknown, old, or
// errored MUST appear with that state rather than be omitted — omission
// turns fetch failures into false-healthy readings. Consumers judge
// staleness from Age.
type FrontierObservation struct {
	Addr     string
	State    FrontierObservationState
	Frontier map[crdt.Origin]crdt.Seq // last fetched frontier; nil unless State == FrontierOK
	Age      time.Duration            // since the observation was made; zero when State == FrontierUnknown
	Err      string                   // cause when State == FrontierError
}

// PeerFrontier aggregates connected peers' applied-frontiers. It is a TipSource
// (so the fetcher discovers + pulls origins this node never saw live, without
// waiting on the object-store seal+LIST backstop) and additionally reports
// whether every live peer holds an origin fully, the GC-safety predicate.
type PeerFrontier interface {
	TipSource
	// AllPeersApplied reports whether every currently-known peer holds origin
	// o at seq >= head. False when no peers are known.
	AllPeersApplied(o crdt.Origin, head crdt.Seq) bool
	// Refresh re-queries every connected peer's frontier.
	Refresh(ctx context.Context)
	// Observations reports one entry per currently-connected peer —
	// including unknown and errored observations, never omitting them.
	Observations() []FrontierObservation
}

// PeerFrontierBuilder constructs a PeerFrontier over the transport's
// connected peers, dialed at their gossip address.
type PeerFrontierBuilder interface {
	PeerFrontierBuilder() PeerFrontier
}
