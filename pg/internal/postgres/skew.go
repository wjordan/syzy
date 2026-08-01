package postgres

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/wjordan/syzy/crdt"
)

// Bounded-skew clock admission (§10). Applying a changeset merges its HLC into
// this node's clock, which is what keeps stamps monotonic across the cluster —
// and also what makes one broken clock everyone's problem. A peer whose wall
// time is a year ahead would drag every node's HLC there permanently: HLC never
// moves backwards, so every subsequent local write on every node would carry the
// bad time, and the damage outlives the peer that caused it.
//
// The guard is deliberately narrow. It does NOT refuse the changeset — the
// record's stamp is what LWW arbitrates on, and dropping peer writes over a
// clock complaint would trade a clock problem for a data problem — and it does
// not halt the node, because one misconfigured NTP client should not stop a
// cluster. It only refuses to let a far-future stamp become OUR clock: the value
// merged into the local HLC is capped at now + bound. The skewed peer still wins
// the rows it wrote (unavoidable under last-writer-wins), but the effect stays
// bounded and ends when the peer's clock is fixed, instead of being permanent
// and cluster-wide.

// defaultMaxClockSkew is the bound when Config.MaxClockSkew is unset. Generous
// enough that ordinary NTP drift and cross-region latency never trip it, tight
// enough that a mis-set clock cannot push the cluster years ahead.
const defaultMaxClockSkew = 30 * time.Second

// skewGuard caps the clock a remote changeset can push this node's HLC to.
type skewGuard struct {
	bound time.Duration
	now   func() time.Time // overridable in tests
	// warned rate-limits the operator warning to one per bound-length window;
	// a skewed peer sends continuously and the log must stay readable.
	warnedUntilMs atomic.Int64
}

func newSkewGuard(bound time.Duration) *skewGuard {
	if bound == 0 {
		bound = defaultMaxClockSkew
	}
	return &skewGuard{bound: bound, now: time.Now}
}

// admit returns the clock to merge into the local HLC for a changeset stamped
// clk. Unskewed stamps pass through untouched; a stamp beyond the bound is
// capped and reported.
func (g *skewGuard) admit(origin crdt.Origin, clk crdt.Clock) crdt.Clock {
	if g == nil || g.bound < 0 {
		return clk // explicitly disabled
	}
	nowMs := g.now().UnixMilli()
	capMs := nowMs + g.bound.Milliseconds()
	if clk.WallTime <= capMs {
		return clk
	}
	if g.warnedUntilMs.Load() <= nowMs {
		g.warnedUntilMs.Store(nowMs + g.bound.Milliseconds())
		slog.Warn("postgres: peer clock is ahead of this node beyond the skew bound; "+
			"its writes will win arbitration until local time catches up — check NTP on both nodes",
			"origin", origin, "ahead_ms", clk.WallTime-nowMs, "bound_ms", g.bound.Milliseconds())
	}
	// Cap the wall time and drop the logical counter with it: the pair is only
	// meaningful together, and a capped stamp must not claim ticks at a wall
	// time it no longer names.
	return crdt.Clock{WallTime: capMs}
}
