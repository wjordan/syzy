package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wjordan/syzy/internal/antientropy"
	"github.com/wjordan/syzy/transport"
)

// Default tuning for the gap planner.
const (
	defaultFetcherInterval    = 30 * time.Second
	defaultFetcherMaxInterval = 5 * time.Minute
	defaultFetcherMaxRanges   = 32
)

// fetcherLoop drives anti-entropy gap repair: per-round, plan missing
// ranges from the cache + TipSource, dispatch to GapFiller.Fetch, and
// adjust the interval based on observed AppliedTip progress. Wakes on
// timer or fetchWake (signalled by applyPayload on out-of-order seqs).
// No-op when cfg.GapFiller is nil.
func (b *Broker) fetcherLoop(ctx context.Context) {
	defer b.wg.Done()

	if b.cfg.GapFiller == nil {
		return
	}

	base := b.fetcherInterval
	if base <= 0 {
		base = defaultFetcherInterval
	}
	maxInterval := b.fetcherMaxInterval
	if maxInterval <= 0 {
		maxInterval = defaultFetcherMaxInterval
	}
	maxRanges := b.fetcherMaxRanges
	if maxRanges <= 0 {
		maxRanges = defaultFetcherMaxRanges
	}

	interval := base
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.fetchWake:
		case <-time.After(interval):
		}
		issued, progressed := b.runFetchRound(ctx, maxRanges)
		// Re-apply quarantined constraint failures: a missing cross-origin
		// dependency may have just landed via this round's gap-fill.
		b.RetryQuarantined(ctx)
		if ctx.Err() != nil {
			return
		}
		interval = antientropy.NextInterval(interval, base, maxInterval, issued, progressed)
	}
}

// fetchErrLogEvery caps the fetch-round WARN rate for an unchanged
// error: at most one log per interval unless the error string changes.
const fetchErrLogEvery = time.Minute

// warnFetchError surfaces a fetch-round failure (GapFiller.Fetch /
// DiscoverTips) at WARN, rate-limited. These were previously recorded
// only via setLastSubscribeError, which nothing polls — a fetcher
// failing every round for hours was invisible at the default log level.
// Suppress ctx-cancellation noise from shutdown/interrupt teardown.
func (b *Broker) warnFetchError(stage string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	msg := stage + ": " + err.Error()
	now := time.Now()
	b.fetchErrMu.Lock()
	if msg == b.fetchErrMsg && now.Sub(b.fetchErrAt) < fetchErrLogEvery {
		b.fetchErrMu.Unlock()
		return
	}
	b.fetchErrMsg, b.fetchErrAt = msg, now
	b.fetchErrMu.Unlock()
	b.log.Warn("broker: fetch round error", "stage", stage, "err", err)
}

// runFetchRound issues at most one GapFiller.Fetch call covering up to
// maxRanges total ranges across all origins, plus (when due) one probe
// Fetch for bucket-unserveable ranges. Returns (issued, progressed):
//
//   - issued: true iff the normal plan found anything and Fetch ran. A
//     probe-only round is not "issued" — an empty probe must not double
//     the round interval.
//   - progressed: true iff any tracked origin's AppliedTip advanced
//     between the pre- and post-Fetch snapshot.
//
// Any progress (gap-fill driven or coincidental live-apply) keeps the
// loop responsive; the planner doesn't try to attribute per-seq.
func (b *Broker) runFetchRound(ctx context.Context, maxRanges int) (issued, progressed bool) {
	if b.cfg.GapFiller == nil || b.cfg.Cache == nil {
		return false, false
	}
	cache := b.cfg.Cache

	// Plan the round via the replication-core anti-entropy planner. Cap total
	// range count to maxRanges; the next round picks up any remainder.
	res := antientropy.Plan(ctx, cache, cache.Self(), b.cfg.TipSource, maxRanges)
	if res.TipErr != nil {
		b.setLastSubscribeError(fmt.Errorf("tip discovery: %w", res.TipErr))
		b.warnFetchError("tip discovery", res.TipErr)
	}

	if len(res.Ranges) > 0 {
		// Route fetched bytes through the single-writer apply path; cache
		// mutex + sqlite writer slot serialize against the live Subscribe.
		if err := b.cfg.GapFiller.Fetch(ctx, res.Ranges, b.applyPayloadWithRetry); err != nil {
			// GapFiller-level error: still useful to compute progress in
			// case some payloads were applied before the error. The
			// progress check below covers that case.
			b.setLastSubscribeError(err)
			b.warnFetchError("gap fetch", err)
		}
	}
	// Clean-empty is the expected probe outcome and logs INFO (the
	// recurrence monitor); a substantive failure (decode/apply/dial) keeps
	// its WARN visibility.
	probed, filled, perr := b.gapProbe.Probe(ctx, cache, res, func(ctx context.Context, rs []transport.Range) error {
		return b.cfg.GapFiller.Fetch(ctx, rs, b.applyPayloadWithRetry)
	})
	if probed {
		if perr == nil || errors.Is(perr, transport.ErrUnfilled) {
			b.log.Info("broker: unserveable-range probe (ranges the bucket cannot serve)",
				"ranges", len(res.Unserveable), "filled_any", filled, "err", perr)
		} else {
			b.warnFetchError("unserveable-range probe", perr)
		}
	}

	return len(res.Ranges) > 0, antientropy.Progressed(cache, res.Before)
}
