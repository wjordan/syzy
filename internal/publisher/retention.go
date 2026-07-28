package publisher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
)

// deleteConcurrency bounds parallel object deletes within a sweep. Object-store
// DELETE is round-trip-bound, so serial deletion of the tens of thousands of
// objects a sweep can reclaim takes hours; fanning out cuts that to minutes.
const deleteConcurrency = 32

// deleteKeys removes keys with bounded concurrency, counting ErrNotFound as a
// successful delete (idempotent — a concurrent sweep may have removed it).
// Returns the number deleted and the first error; on error it stops launching
// new deletes and waits for in-flight ones to finish.
func deleteKeys(ctx context.Context, be objectstore.Bucket, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		deleted  atomic.Int64
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	sem := make(chan struct{}, deleteConcurrency)
	for _, key := range keys {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := be.Delete(ctx, k); err != nil && !errors.Is(err, objectstore.ErrNotFound) {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("delete %s: %w", k, err)
					cancel()
				}
				mu.Unlock()
				return
			}
			deleted.Add(1)
		}(key)
	}
	wg.Wait()
	return int(deleted.Load()), firstErr
}

// Retention deletes superseded objects from the bucket. Per-stream
// rules — applied independently to db/ (app.db) and metadata/
// (metadata.db) using each stream's own baseline TXID:
//
//  1. L0 files at <stream>0000/ are eligible when an L1 file at
//     <stream>0001/ covers their TXID range AND the L1 has been in
//     the bucket longer than Grace, or when the active baseline covers
//     them AND that baseline has aged past Grace.
//  2. L1 files at <stream>0001/ are eligible when their max TXID is
//     at/below the latest baseline's TXID AND that baseline has aged
//     past Grace.
//  3. Baseline files at <stream>0009/ are eligible when their TXID is
//     strictly below the active (HEAD) baseline's TXID AND aged past
//     Grace. HEAD references only the current baseline, so every prior
//     rebaseline is otherwise orphaned forever.
//
// Grace lets in-flight readers (a Litestream-follower mid-stream, a
// syzy restore that just LISTed) finish before the file vanishes.
type Retention struct {
	Backend objectstore.Bucket
	Grace   time.Duration
	DryRun  bool
}

// Result summarizes what one Sweep did or would have done.
type Result struct {
	L0Deleted       int
	L1Deleted       int
	BaselineDeleted int // superseded <stream>0009/ baselines below the active one
	MetadataDeleted int // retained in the public status shape; always zero
}

// Sweep runs one retention pass over both streams.
func (r *Retention) Sweep(ctx context.Context) (Result, error) {
	if r.Grace <= 0 {
		r.Grace = 24 * time.Hour
	}
	now := time.Now()
	cutoff := now.Add(-r.Grace)

	head, _, err := objstore.LoadHEAD(ctx, r.Backend)
	if err != nil && !errors.Is(err, objstore.ErrNoHEAD) {
		return Result{}, err
	}

	res := Result{}

	// LTX-stream rules come from HEAD.
	if head != nil {
		// Rules 1+2 per stream.
		for _, sweep := range []struct {
			prefix   string
			baseline *objstore.Baseline
		}{
			{objstore.DBPrefix, head.Baseline},
			{objstore.MetadataPrefix, head.MetaBaseline},
		} {
			l0, l1, bl, err := r.streamSweep(ctx, sweep.prefix, sweep.baseline, cutoff)
			if err != nil {
				return res, err
			}
			res.L0Deleted += l0
			res.L1Deleted += l1
			res.BaselineDeleted += bl
		}
	}

	return res, nil
}

func baselineOrZero(b *objstore.Baseline) uint64 {
	if b == nil {
		return 0
	}
	return b.TXID
}

func baselineReady(b *objstore.Baseline, cutoff time.Time) (txid uint64, ready bool) {
	if b == nil || b.TXID == 0 {
		return 0, false
	}
	if b.BuiltAtUS == 0 {
		return b.TXID, true
	}
	return b.TXID, !time.UnixMicro(b.BuiltAtUS).After(cutoff)
}

// streamSweep runs L0-covered-by-L1 + L1-below-baseline rules for
// one stream prefix.
func (r *Retention) streamSweep(ctx context.Context, streamPrefix string, baseline *objstore.Baseline, cutoff time.Time) (l0Deleted, l1Deleted, baselineDeleted int, err error) {
	baselineTXID, baselineAged := baselineReady(baseline, cutoff)

	l1, err := objstore.ListLTX(ctx, r.Backend, streamPrefix, objstore.L1Level)
	if err != nil {
		return 0, 0, 0, err
	}
	sortLTXFiles(l1)
	agedL1Coverage := newLTXCoverage(l1, func(f objstore.LTXFile) bool {
		return !f.Modified.After(cutoff)
	})

	l0, err := objstore.ListLTX(ctx, r.Backend, streamPrefix, objstore.L0Level)
	if err != nil {
		return 0, 0, 0, err
	}

	var l0Keys []string
	for _, f := range l0 {
		deleteL0 := baselineAged && f.MaxTXID <= baselineTXID
		if !deleteL0 {
			deleteL0 = agedL1Coverage.Covers(f.MinTXID, f.MaxTXID)
		}
		if deleteL0 {
			l0Keys = append(l0Keys, f.Key)
		}
	}
	if r.DryRun {
		l0Deleted = len(l0Keys)
	} else if l0Deleted, err = deleteKeys(ctx, r.Backend, l0Keys); err != nil {
		return l0Deleted, 0, 0, err
	}

	var l1Keys []string
	for _, f := range l1 {
		if baselineAged && f.MaxTXID <= baselineTXID {
			l1Keys = append(l1Keys, f.Key)
		}
	}
	if r.DryRun {
		l1Deleted = len(l1Keys)
	} else if l1Deleted, err = deleteKeys(ctx, r.Backend, l1Keys); err != nil {
		return l0Deleted, l1Deleted, 0, err
	}

	// Rule 3: superseded baselines. Each rebaseline writes a new <stream>0009/
	// object; HEAD references only the current one, so every prior baseline is
	// orphaned forever — no other rule covers the baseline level (observed: tens
	// of thousands of stale baselines, multi-GiB). Delete those strictly below
	// the active baseline TXID and aged past grace. (== keeps the active
	// baseline; > keeps a newer one written but not yet promoted into HEAD; grace
	// lets an in-flight restore that already LISTed finish before bytes vanish.)
	if baselineTXID > 0 {
		baselines, lerr := objstore.ListLTX(ctx, r.Backend, streamPrefix, objstore.BaselineLevel)
		if lerr != nil {
			return l0Deleted, l1Deleted, 0, lerr
		}
		var blKeys []string
		for _, f := range baselines {
			if f.MaxTXID >= baselineTXID || f.Modified.After(cutoff) {
				continue
			}
			blKeys = append(blKeys, f.Key)
		}
		if r.DryRun {
			baselineDeleted = len(blKeys)
		} else if baselineDeleted, err = deleteKeys(ctx, r.Backend, blKeys); err != nil {
			return l0Deleted, l1Deleted, baselineDeleted, err
		}
	}

	return l0Deleted, l1Deleted, baselineDeleted, nil
}

// retentionSweepInterval picks how often the retention loop runs:
// hourly, but never longer than the grace window (so a short grace —
// e.g. in tests — still sweeps promptly).
func retentionSweepInterval(grace time.Duration) time.Duration {
	const def = time.Hour
	if grace > 0 && grace < def {
		return grace
	}
	return def
}

// runRetentionLoop drives Sweep on a tick until ctx cancels. Wired
// into Publisher; default tick = Grace duration (so the loop runs
// once per grace window).
func (p *Publisher) runRetentionLoop(ctx context.Context, grace, interval time.Duration) {
	if interval <= 0 || grace <= 0 {
		return
	}
	r := &Retention{Backend: p.mutationBackend(), Grace: grace}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			mutationCtx, done, err := p.leaseMutationContext(ctx)
			if err != nil {
				return
			}
			t0 := time.Now()
			res, err := r.Sweep(mutationCtx)
			if err != nil && errors.Is(err, context.Canceled) {
				done()
				return
			}
			p.stats.recordRetention(res, time.Since(t0), err)
			if err != nil {
				p.cfg.Logger.Warn("retention: sweep failed", "err", err)
			} else if res.L0Deleted+res.L1Deleted+res.BaselineDeleted+res.MetadataDeleted > 0 {
				p.cfg.Logger.Info("retention: swept",
					"l0_deleted", res.L0Deleted,
					"l1_deleted", res.L1Deleted,
					"baseline_deleted", res.BaselineDeleted,
					"metadata_deleted", res.MetadataDeleted)
			}
			done()
		}
	}
}
