package sqlite

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

type stubTips struct{ tips map[crdt.Origin]crdt.Seq }

func (s stubTips) DiscoverTips(context.Context) (map[crdt.Origin]crdt.Seq, error) {
	return maps.Clone(s.tips), nil
}

type stubTipsCoverage struct {
	stubTips
	coverage map[crdt.Origin][]transport.Range
	covErr   error
}

func (s stubTipsCoverage) Coverage(context.Context) (map[crdt.Origin][]transport.Range, error) {
	if s.covErr != nil {
		return nil, s.covErr
	}
	return maps.Clone(s.coverage), nil
}

// TestMergedTipsForwardsCoverage: the merged TipSource must expose the
// CoverageSource capability of its members, or the planner's classifier
// silently never engages on the broker path, where the s3 source is
// always wrapped together with the peer frontier.
func TestMergedTipsForwardsCoverage(t *testing.T) {
	peerish := stubTips{tips: map[crdt.Origin]crdt.Seq{7: 100}} // no coverage claims
	s3ish := stubTipsCoverage{
		stubTips: stubTips{tips: map[crdt.Origin]crdt.Seq{7: 90}},
		coverage: map[crdt.Origin][]transport.Range{7: {{Origin: 7, Lo: 40, Hi: 90}}},
	}
	merged := mergeTipSources(peerish, s3ish)

	cs, ok := merged.(transport.CoverageSource)
	if !ok {
		t.Fatalf("merged TipSource does not implement CoverageSource")
	}
	cov, err := cs.Coverage(context.Background())
	if err != nil {
		t.Fatalf("Coverage: %v", err)
	}
	if len(cov[7]) != 1 || cov[7][0] != (transport.Range{Origin: 7, Lo: 40, Hi: 90}) {
		t.Errorf("coverage[7] = %v; want [{7 40 90}]", cov[7])
	}

	// Two coverage-bearing members union (and coalesce where adjacent).
	s3b := stubTipsCoverage{coverage: map[crdt.Origin][]transport.Range{
		7: {{Origin: 7, Lo: 10, Hi: 39}},
		9: {{Origin: 9, Lo: 1, Hi: 5}},
	}}
	cov2, _ := mergeTipSources(s3ish, s3b).(transport.CoverageSource).Coverage(context.Background())
	if len(cov2[7]) != 1 || cov2[7][0] != (transport.Range{Origin: 7, Lo: 10, Hi: 90}) {
		t.Errorf("coverage2[7] = %v; want [{7 10 90}]", cov2[7])
	}
	if len(cov2[9]) != 1 {
		t.Errorf("coverage2[9] = %v; want one interval", cov2[9])
	}

	// A capable member whose walk fails invalidates the whole claim —
	// absence is authoritative, so a partial union would fabricate it.
	s3bad := stubTipsCoverage{covErr: errors.New("bucket down")}
	cov3, _ := mergeTipSources(s3ish, s3bad).(transport.CoverageSource).Coverage(context.Background())
	if cov3 != nil {
		t.Errorf("coverage with failing member = %v; want nil (no claims)", cov3)
	}
}
