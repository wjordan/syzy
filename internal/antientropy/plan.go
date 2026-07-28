// Package antientropy holds the replication-core gap-repair planner. From the
// cache's applied state (frontier + gaps) merged with externally reported
// origin tips, it computes the missing (origin, seq) ranges worth pulling this
// round. The SQLite broker owns pacing, wake signals, and delivery into its
// single-writer apply path.
package antientropy

import (
	"context"
	"sync"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/transport"
)

// Result is one planning round's output.
type Result struct {
	// Ranges is the round's normal fetch plan: missing ranges some source
	// may plausibly serve.
	Ranges []transport.Range
	// Unserveable holds missing ranges that intersect no surviving
	// bucket-epoch interval (including every range of an origin the bucket
	// holds nothing for): the bucket cannot serve them this round, so
	// fetching them every round is pure noise. A peer journal may still
	// hold the frames (coverage proves nothing about peers), and the
	// bucket may gain the range later (each round reclassifies from a
	// fresh snapshot), so these are not dropped — the engine probes them
	// through its full gap-filler chain on a slow fixed cadence instead of
	// the per-round plan.
	Unserveable []transport.Range
	// Before snapshots each planned origin's applied tip so the caller can
	// detect progress after its fetch (Progressed).
	Before map[crdt.Origin]crdt.Seq
	// Discovered is the TipSource's raw report (nil without a TipSource or
	// on its error).
	Discovered map[crdt.Origin]crdt.Seq
	// TipErr reports a TipSource failure; planning still proceeds on
	// cache-known tips alone.
	TipErr error
}

// Plan computes at most maxRanges missing ranges across all known origins
// (excluding self), split into the normal plan and the unserveable probe set
// (each capped at maxRanges independently). Coverage comes from the
// TipSource's optional transport.CoverageSource capability; without it (or
// on its error) nothing is classified unserveable — the conservative
// direction.
func Plan(ctx context.Context, cache *nodestate.Cache, self crdt.Origin, tipSource transport.TipSource, maxRanges int) Result {
	var res Result

	tips := cache.AppliedTipMap()
	delete(tips, self)
	// Merge externally-reported tips so a node returning from offline pulls
	// origins it never saw live — without this, MissingRangesUpTo only fires
	// for origins the cache already applied a record from.
	if tipSource != nil {
		if d, err := tipSource.DiscoverTips(ctx); err == nil {
			res.Discovered = d
			for o, t := range d {
				if o == self || t == 0 {
					continue
				}
				if cur, ok := tips[o]; !ok || t > cur {
					tips[o] = t
				}
			}
		} else {
			res.TipErr = err
		}
	}
	if len(tips) == 0 {
		return res
	}

	// A nil map (no capability, or a failed/incomplete walk) classifies
	// nothing: an unclassifiable range stays in the normal plan rather than
	// being wrongly demoted. A non-nil map is a complete snapshot, so an
	// origin absent from it holds nothing in the bucket.
	var coverage map[crdt.Origin][]transport.Range
	if cs, ok := tipSource.(transport.CoverageSource); ok {
		coverage, _ = cs.Coverage(ctx)
	}

	res.Before = map[crdt.Origin]crdt.Seq{}
	for o, t := range tips {
		miss := cache.MissingRangesUpTo(o, t)
		if len(miss) == 0 {
			continue
		}
		res.Before[o] = cache.AppliedTip(o)
		for _, r := range miss {
			tr := transport.Range{Origin: o, Lo: r.Lo, Hi: r.Hi}
			// No overlap with any surviving epoch: the bucket cannot serve
			// any of it. A range partially covered stays normal; once its
			// servable part is fetched, the remainder re-plans as wholly
			// uncovered next round. Open-ended ranges (unknown Hi) stay
			// normal too.
			if coverage != nil && !tr.OpenEnded() && !intersectsAny(coverage[o], tr) {
				if len(res.Unserveable) < maxRanges {
					res.Unserveable = append(res.Unserveable, tr)
				}
			} else if len(res.Ranges) < maxRanges {
				res.Ranges = append(res.Ranges, tr)
			}
			if len(res.Ranges) >= maxRanges && len(res.Unserveable) >= maxRanges {
				return res
			}
		}
	}
	return res
}

// intersectsAny reports whether r overlaps any interval in iv (sorted,
// non-overlapping — a CoverageSource's per-origin value). Linear scan:
// merged interval lists are short (one entry per surviving gap).
func intersectsAny(iv []transport.Range, r transport.Range) bool {
	for _, c := range iv {
		if c.Lo > r.Hi {
			return false
		}
		if c.Hi >= r.Lo {
			return true
		}
	}
	return false
}

// NextInterval is the fetch loop's pacing rule, shared by both engines: an
// idle round (nothing missing) keeps the current interval — a healthy quiet
// cluster isn't penalized; a round that progressed resets to base; a round
// that issued but made no progress doubles, capped at max.
func NextInterval(cur, base, max time.Duration, issued, progressed bool) time.Duration {
	switch {
	case !issued:
		return cur
	case progressed:
		return base
	default:
		return min(cur*2, max)
	}
}

// Progressed reports whether any planned origin's applied tip advanced past
// its pre-fetch snapshot.
func Progressed(cache *nodestate.Cache, before map[crdt.Origin]crdt.Seq) bool {
	for o, b := range before {
		if cache.AppliedTip(o) > b {
			return true
		}
	}
	return false
}

// defaultProbeInterval paces unserveable-range re-probes. Fixed cadence,
// deliberately decoupled from the progress-driven round interval: unrelated
// live traffic resets that interval to base every round, which is exactly
// what made bucket-unfillable ranges re-fetch (and WARN) every ~30s forever.
const defaultProbeInterval = 15 * time.Minute

// Prober paces slow-cadence probes of a round's Unserveable ranges through
// the engine's full gap-filler chain (a peer journal may hold frames the
// bucket lacks). The zero value probes immediately, giving every unserveable
// range one normal attempt before slow pacing starts. Safe for concurrent
// Kick (e.g. from a transport's peer-connect callback).
type Prober struct {
	Interval time.Duration // 0 → defaultProbeInterval
	mu       sync.Mutex
	next     time.Time
}

// Kick makes the next Probe due immediately — used on peer connect, since a
// new peer's journal is exactly where bucket-lost frames might surface.
func (p *Prober) Kick() {
	p.mu.Lock()
	p.next = time.Time{}
	p.mu.Unlock()
}

// Probe fetches res.Unserveable via fetch when due. Returns probed=false when
// there was nothing to do (not due, or no unserveable ranges). filled reports
// whether a probed origin's contiguous frontier advanced (unserveable ranges
// are typically holes under the applied tip, so a fill shrinks the hole from
// the bottom; AppliedTip would not move) — a source evidently holds the range,
// so the prober re-arms itself to due and keeps draining at round pace. Coming
// back empty is the EXPECTED outcome; the caller logs the result on its own
// channel, never as a fetch-round error.
func (p *Prober) Probe(ctx context.Context, cache *nodestate.Cache, res Result,
	fetch func(context.Context, []transport.Range) error) (probed, filled bool, err error) {
	if len(res.Unserveable) == 0 {
		return false, false, nil
	}
	now := time.Now()
	p.mu.Lock()
	if now.Before(p.next) {
		p.mu.Unlock()
		return false, false, nil
	}
	interval := p.Interval
	if interval <= 0 {
		interval = defaultProbeInterval
	}
	p.next = now.Add(interval)
	p.mu.Unlock()

	before := map[crdt.Origin]crdt.Seq{}
	for _, r := range res.Unserveable {
		f, _ := cache.FrontierFor(r.Origin)
		before[r.Origin] = f.LastSeq
	}
	err = fetch(ctx, res.Unserveable)
	for o, b := range before {
		if f, _ := cache.FrontierFor(o); f.LastSeq > b {
			filled = true
			break
		}
	}
	if filled {
		p.Kick()
	}
	return true, filled, err
}
