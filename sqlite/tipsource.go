package sqlite

import (
	"context"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/transport"
)

// frontierFromCache adapts nodestate.Cache to transport.FrontierSource: the
// frontier we serve to peers is our applied-tip-per-origin map.
type frontierFromCache struct{ cache *nodestate.Cache }

func (f frontierFromCache) Frontier() map[crdt.Origin]crdt.Seq { return f.cache.AppliedTipMap() }

// mergeTipSources combines TipSources; DiscoverTips returns the max seq per
// origin across all of them. Callers must pass only non-nil sources (a nil
// concrete pointer as an interface is a non-nil interface). Returns nil for
// none, the lone source for one, else a merging wrapper.
func mergeTipSources(srcs ...transport.TipSource) transport.TipSource {
	live := make([]transport.TipSource, 0, len(srcs))
	for _, s := range srcs {
		if s != nil {
			live = append(live, s)
		}
	}
	switch len(live) {
	case 0:
		return nil
	case 1:
		return live[0]
	default:
		return mergedTips(live)
	}
}

type mergedTips []transport.TipSource

func (m mergedTips) DiscoverTips(ctx context.Context) (map[crdt.Origin]crdt.Seq, error) {
	out := map[crdt.Origin]crdt.Seq{}
	var firstErr error
	anyOK := false
	for _, s := range m {
		tips, err := s.DiscoverTips(ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		anyOK = true
		for o, t := range tips {
			if t > out[o] {
				out[o] = t
			}
		}
	}
	if !anyOK {
		return nil, firstErr
	}
	return out, nil
}

// Coverage implements transport.CoverageSource by unioning members'
// interval claims. Members without the capability (e.g. peer frontiers)
// contribute nothing; but because a non-nil map claims authoritative
// ABSENCE, a capable member whose Coverage call fails must invalidate the
// whole result (nil, no claims) — skipping it would fabricate absence,
// the anti-conservative direction.
func (m mergedTips) Coverage(ctx context.Context) (map[crdt.Origin][]transport.Range, error) {
	var out map[crdt.Origin][]transport.Range
	for _, s := range m {
		cs, ok := s.(transport.CoverageSource)
		if !ok {
			continue
		}
		cov, err := cs.Coverage(ctx)
		if err != nil || cov == nil {
			return nil, err
		}
		if out == nil {
			out = map[crdt.Origin][]transport.Range{}
		}
		for o, iv := range cov {
			out[o] = append(out[o], iv...)
		}
	}
	for o := range out {
		out[o] = transport.MergeIntervals(out[o])
	}
	return out, nil
}
