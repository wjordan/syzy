package postgres

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/antientropy"
)

// Anti-entropy gap repair — the SQLite broker's fetcher, restated for the
// single-writer orchestrator. Live broadcast is best-effort (a tcp send to a
// disconnected peer is simply lost), so a node that misses a delivery holds a
// permanent gap unless someone asks for it: per round, plan missing ranges
// from the Cache (+ optional TipSource), pull them from a peer's catchup
// endpoint via the GapFiller, and route every fetched changeset through the
// orchestrator goroutine so apply stays single-writer.
const (
	fetcherInterval    = 30 * time.Second
	fetcherMaxInterval = 5 * time.Minute
	fetcherMaxRanges   = 32
)

// fetchReq hands one fetched changeset to the orchestrator goroutine and
// waits for its apply verdict, so GapFiller.Fetch observes per-payload errors
// (and its stream halts on the first hard failure) exactly as if the bytes
// had arrived on the inbox.
type fetchReq struct {
	cs  *crdt.Changeset
	err chan error
}

// kickFetch nudges the fetcher after an out-of-order apply left a gap below
// the just-applied seq. Non-blocking: a pending wake already covers it.
func (o *orchestrator) kickFetch() {
	select {
	case o.fetchWake <- struct{}{}:
	default:
	}
}

// fetcherLoop runs on its own goroutine (started by Run when a GapFiller is
// configured). Timer-paced with exponential backoff while rounds make no
// progress; a gap observed by applyRemote wakes it immediately.
func (o *orchestrator) fetcherLoop(ctx context.Context) {
	interval := fetcherInterval
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.fetchWake:
		case <-time.After(interval):
		}
		issued, progressed := o.runFetchRound(ctx)
		if ctx.Err() != nil {
			return
		}
		interval = antientropy.NextInterval(interval, fetcherInterval, fetcherMaxInterval, issued, progressed)
	}
}

// runFetchRound plans this node's missing ranges and issues at most one
// GapFiller.Fetch covering up to fetcherMaxRanges of them. Returns (issued,
// progressed) with the broker fetcher's meaning: issued = something was
// missing and Fetch ran; progressed = some tracked origin's applied tip
// advanced during the round.
func (o *orchestrator) runFetchRound(ctx context.Context) (issued, progressed bool) {
	cache := o.appl.cfg.Cache
	plan := antientropy.Plan(ctx, cache, o.selfOrigin, o.tipSource, fetcherMaxRanges)
	if plan.TipErr != nil && !errors.Is(plan.TipErr, context.Canceled) {
		slog.Warn("postgres: fetch tip discovery failed", "err", plan.TipErr)
	}
	if plan.Discovered != nil {
		// Stash the bucket's sealed tips for checkpoint-time mirror GC: a
		// mirror segment wholly at-or-below an origin's sealed tip is
		// bucket-durable and safe to truncate.
		o.tipsMu.Lock()
		o.bucketTips = plan.Discovered
		o.tipsMu.Unlock()
	}
	if len(plan.Ranges) == 0 {
		return false, false
	}

	err := o.gapFiller.Fetch(ctx, plan.Ranges, func(ctx context.Context, payload []byte) error {
		cs, derr := crdt.Decode(payload)
		if derr != nil {
			return derr // a peer streaming garbage halts this fetch, not the node
		}
		req := fetchReq{cs: cs, err: make(chan error, 1)}
		select {
		case o.fetched <- req:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case aerr := <-req.err:
			return aerr
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("postgres: gap fetch round failed", "err", err)
	}

	return true, antientropy.Progressed(cache, plan.Before)
}
