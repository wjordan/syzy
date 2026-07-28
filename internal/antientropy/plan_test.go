package antientropy

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/transport"
)

// fakeTips is a TipSource with optional coverage claims and injectable
// errors, standing in for s3fetch.Source.
type fakeTips struct {
	tips     map[crdt.Origin]crdt.Seq
	coverage map[crdt.Origin][]transport.Range
	tipsErr  error
	covErr   error
}

func (f *fakeTips) DiscoverTips(context.Context) (map[crdt.Origin]crdt.Seq, error) {
	if f.tipsErr != nil {
		return nil, f.tipsErr
	}
	return maps.Clone(f.tips), nil
}

func (f *fakeTips) Coverage(context.Context) (map[crdt.Origin][]transport.Range, error) {
	if f.covErr != nil {
		return nil, f.covErr
	}
	return maps.Clone(f.coverage), nil
}

// gapCache builds a cache for origin 7 with applied seq=5 only:
// frontier 0, gap-set {5}, so MissingRangesUpTo(7, 5) = [1,4].
func gapCache(t *testing.T) *nodestate.Cache {
	t.Helper()
	c := nodestate.New(1)
	c.MarkApplied(7, 5, crdt.Clock{WallTime: 1})
	return c
}

func planRanges(t *testing.T, c *nodestate.Cache, ts transport.TipSource) Result {
	t.Helper()
	return Plan(context.Background(), c, c.Self(), ts, 32)
}

// TestPlanDemotesUncoveredRange: a missing range intersecting no surviving
// epoch interval moves to Unserveable, out of the normal per-round plan.
func TestPlanDemotesUncoveredRange(t *testing.T) {
	c := gapCache(t)
	res := planRanges(t, c, &fakeTips{
		tips:     map[crdt.Origin]crdt.Seq{7: 5},
		coverage: map[crdt.Origin][]transport.Range{7: {{Origin: 7, Lo: 5, Hi: 10}}}, // [1,4] is gone
	})
	if len(res.Ranges) != 0 {
		t.Errorf("Ranges = %+v; want empty", res.Ranges)
	}
	if len(res.Unserveable) != 1 || res.Unserveable[0] != (transport.Range{Origin: 7, Lo: 1, Hi: 4}) {
		t.Errorf("Unserveable = %+v; want [{7 1 4}]", res.Unserveable)
	}
	if _, ok := res.Before[7]; !ok {
		t.Errorf("Before missing origin 7 (probe progress needs its snapshot)")
	}
}

// TestPlanDemotesMidCoverageHole: the production shape — epochs survive on
// BOTH sides of the hole (older epoch pins a low floor), so a floor test
// would never fire; coverage intersection does.
func TestPlanDemotesMidCoverageHole(t *testing.T) {
	c := gapCache(t)
	c.MarkApplied(7, 1, crdt.Clock{WallTime: 1}) // missing shrinks to [2,4]
	res := planRanges(t, c, &fakeTips{
		tips:     map[crdt.Origin]crdt.Seq{7: 5},
		coverage: map[crdt.Origin][]transport.Range{7: {{Origin: 7, Lo: 1, Hi: 1}, {Origin: 7, Lo: 5, Hi: 9}}},
	})
	if len(res.Unserveable) != 1 || res.Unserveable[0] != (transport.Range{Origin: 7, Lo: 2, Hi: 4}) {
		t.Errorf("Unserveable = %+v; want [{7 2 4}]", res.Unserveable)
	}
	if len(res.Ranges) != 0 {
		t.Errorf("Ranges = %+v; want empty", res.Ranges)
	}
}

// TestPlanKeepsIntersectingRange: a range overlapping any epoch interval
// stays in the normal plan — the bucket can serve at least part of it, and
// the uncovered remainder re-plans next round.
func TestPlanKeepsIntersectingRange(t *testing.T) {
	c := gapCache(t)
	res := planRanges(t, c, &fakeTips{
		tips:     map[crdt.Origin]crdt.Seq{7: 5},
		coverage: map[crdt.Origin][]transport.Range{7: {{Origin: 7, Lo: 3, Hi: 10}}}, // [3,4] servable
	})
	if len(res.Unserveable) != 0 {
		t.Errorf("Unserveable = %+v; want empty", res.Unserveable)
	}
	if len(res.Ranges) != 1 || res.Ranges[0] != (transport.Range{Origin: 7, Lo: 1, Hi: 4}) {
		t.Errorf("Ranges = %+v; want [{7 1 4}]", res.Ranges)
	}
}

// TestPlanAbsentOriginDemotes: a non-nil coverage map comes from a complete
// walk, so an origin absent from it holds nothing in the bucket — its
// ranges demote. The probe still gives them an immediate full-chain (peers
// first) attempt, and a fill re-arms it to round pace, so a fresh origin's
// catch-up is not starved; each round reclassifies once its first epoch
// lands.
func TestPlanAbsentOriginDemotes(t *testing.T) {
	c := gapCache(t)
	res := planRanges(t, c, &fakeTips{
		tips:     map[crdt.Origin]crdt.Seq{7: 5},
		coverage: map[crdt.Origin][]transport.Range{}, // complete walk, no epochs for 7
	})
	if len(res.Unserveable) != 1 || len(res.Ranges) != 0 {
		t.Errorf("Ranges=%+v Unserveable=%+v; want 0 normal, 1 demoted", res.Ranges, res.Unserveable)
	}
}

// TestPlanCoverageErrorIsConservative: a Coverage failure must not demote
// anything.
func TestPlanCoverageErrorIsConservative(t *testing.T) {
	c := gapCache(t)
	res := planRanges(t, c, &fakeTips{
		tips:     map[crdt.Origin]crdt.Seq{7: 5},
		coverage: map[crdt.Origin][]transport.Range{7: {{Origin: 7, Lo: 5, Hi: 10}}},
		covErr:   errors.New("bucket down"),
	})
	if len(res.Unserveable) != 0 || len(res.Ranges) != 1 {
		t.Errorf("Ranges=%+v Unserveable=%+v; want 1 normal, 0 demoted on Coverage error", res.Ranges, res.Unserveable)
	}
}

// TestPlanWithoutCoverageCapability: a plain TipSource (no CoverageSource)
// plans exactly as before the classifier existed.
func TestPlanWithoutCoverageCapability(t *testing.T) {
	c := gapCache(t)
	type plain struct{ transport.TipSource }
	res := planRanges(t, c, plain{&fakeTips{tips: map[crdt.Origin]crdt.Seq{7: 5}}})
	if len(res.Unserveable) != 0 || len(res.Ranges) != 1 {
		t.Errorf("Ranges=%+v Unserveable=%+v; want 1 normal, 0 demoted without CoverageSource", res.Ranges, res.Unserveable)
	}
}

// TestMergeIntervals: sort, overlap-coalesce, adjacency-coalesce, gaps kept.
func TestMergeIntervals(t *testing.T) {
	got := transport.MergeIntervals([]transport.Range{
		{Origin: 7, Lo: 12, Hi: 15},
		{Origin: 7, Lo: 1, Hi: 3},
		{Origin: 7, Lo: 4, Hi: 6}, // adjacent to [1,3] → coalesce
		{Origin: 7, Lo: 5, Hi: 8}, // overlaps → extend
		{Origin: 7, Lo: 20, Hi: 20},
	})
	want := []transport.Range{
		{Origin: 7, Lo: 1, Hi: 8},
		{Origin: 7, Lo: 12, Hi: 15},
		{Origin: 7, Lo: 20, Hi: 20},
	}
	if len(got) != len(want) {
		t.Fatalf("merged = %+v; want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("merged[%d] = %+v; want %+v", i, got[i], want[i])
		}
	}
}

// TestProber: zero value probes immediately, then not again within the
// interval; Kick makes it due; a probe that fills frames re-arms itself.
func TestProber(t *testing.T) {
	c := gapCache(t)
	res := Result{
		Unserveable: []transport.Range{{Origin: 7, Lo: 1, Hi: 4}},
		Before:      map[crdt.Origin]crdt.Seq{7: c.AppliedTip(7)},
	}
	var p Prober
	calls := 0
	noop := func(context.Context, []transport.Range) error { calls++; return nil }

	if probed, _, _ := p.Probe(context.Background(), c, res, noop); !probed || calls != 1 {
		t.Fatalf("first probe: probed=%v calls=%d; want immediate probe", probed, calls)
	}
	if probed, _, _ := p.Probe(context.Background(), c, res, noop); probed || calls != 1 {
		t.Fatalf("second probe: probed=%v calls=%d; want paced out", probed, calls)
	}
	// A concurrent NEW high seq (6, above tip 5) raises AppliedTip but not
	// the frontier: an empty probe must NOT report filled off unrelated
	// live traffic (that false signal would re-arm the probe every round,
	// recreating the noise the classifier exists to remove).
	p.Kick()
	liveOnly := func(context.Context, []transport.Range) error {
		c.MarkApplied(7, 6, crdt.Clock{WallTime: 2})
		return nil
	}
	if probed, filled, _ := p.Probe(context.Background(), c, res, liveOnly); !probed || filled {
		t.Fatalf("live-traffic probe: probed=%v filled=%v; want probed, not filled", probed, filled)
	}
	// Filling the hole beneath the existing tip advances the contiguous
	// frontier (AppliedTip would NOT move): this is the real-repair shape
	// and must report filled=true.
	p.Kick()
	fill := func(context.Context, []transport.Range) error {
		c.MarkApplied(7, 1, crdt.Clock{WallTime: 3}) // probe delivered a frame
		return nil
	}
	if probed, filled, _ := p.Probe(context.Background(), c, res, fill); !probed || !filled {
		t.Fatalf("kicked probe: probed=%v filled=%v; want probe that reports fill", probed, filled)
	}
	// filled re-armed the prober: next probe is due without waiting.
	if probed, _, _ := p.Probe(context.Background(), c, res, noop); !probed {
		t.Fatalf("probe after fill: not due; want re-armed")
	}
	// Nothing unserveable → never probes.
	if probed, _, _ := p.Probe(context.Background(), c, Result{}, noop); probed {
		t.Fatalf("probe with empty Unserveable: probed=true")
	}
}
