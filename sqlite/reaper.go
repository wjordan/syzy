package sqlite

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/s3fetch"
)

// Reaper tuning.
const (
	defaultReapInterval = 60 * time.Second
	// reapBudgetPerPass bounds rmdir/goroutine-join work per tick so a large
	// accumulated backlog (hundreds of dead origins) clears over several
	// passes instead of one stall.
	reapBudgetPerPass = 64
)

// seedSealerFromSelfLog replays the durable self-log through the sealer so
// its in-memory ContiguousSealedSeq is rebuilt from durable state on
// restart: each retained record is re-confirmed against S3 (idempotent
// IfAbsent), re-sealing anything dropped at a prior shutdown. Runs after the
// sealer goroutine starts so OnEncoded's bounded queue drains instead of
// deadlocking. Non-fatal throughout — a partial seed only defers coverage to
// the live feed and the next restart.
//
// Known minor inefficiency: UploadedSeq resets to 0 each start and this seed
// re-feeds already-sealed records, which the sealer re-groups into epochs by
// the current run's boundaries. Those epoch keys don't collide with the prior
// run's, so IfAbsent doesn't dedup — a frequently-restarting node accumulates
// overlapping epoch objects for the retained (untruncated) tail. Tolerated:
// DiscoverTips/Fetch are overlap-safe and the retained tail is bounded, so
// it's redundant S3 storage, not a correctness bug. A fix would prime the
// sealer's per-origin UploadedSeq from S3 tips before seeding, but that must
// preserve the re-seal-the-gaps purpose (a max tip can't skip a mid-range
// queue-drop), so it's deferred rather than bolted on here.
func (n *Node) seedSealerFromSelfLog(self crdt.Origin) {
	if n.sealer == nil || n.mirror == nil {
		return
	}
	j, err := n.mirror.Journal(self)
	if err != nil {
		return
	}
	head := j.Head()
	it := j.Iterate(0)
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) || err != nil {
			return
		}
		if it.Offset() > head {
			return
		}
		if rec.Kind != journal.KindMirror || rec.Aborted() {
			continue
		}
		n.sealer.OnEncoded(rec.Payload)
	}
}

// reaperLoop periodically reaps fully-sealed mirror journals to bound origin
// proliferation. Exits on ctx cancel. See the launch site in Open.
func (n *Node) reaperLoop(ctx context.Context, src *s3fetch.Source) {
	interval := defaultReapInterval
	// Debug override for fast iteration/validation (e.g. SYZY_REAP_INTERVAL=2s).
	if v := os.Getenv("SYZY_REAP_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			interval = d
		}
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n.reapOrigins(ctx, src)
	}
}

// reapOrigins reaps every non-self mirror journal that is safe to drop. "Safe"
// means no cluster member can still need our copy as a catch-up source — see
// reapable for the exact predicate. The durable signal is the object-store seal
// (a behind member always re-fetches a sealed origin from the bucket, and the
// secondary drainer seals even retired/dead origins, so the seal is reached for
// every origin). The all-peers-applied signal is only LIVENESS (currently-
// connected, responsive peers; Refresh drops absent/errored peers), so it is
// trusted to reap an UNSEALED origin ONLY in best-effort no-bucket mode — never
// when a bucket exists, where reaping an unsealed origin on liveness alone would
// strand a transiently-absent member permanently (its records gone from every
// mirror and never sealed). The cache's applied frontier is left intact so the
// fetcher never re-pulls a reaped origin (no thrash). Bounded by
// reapBudgetPerPass per pass; refreshing the peer frontier each pass drops
// departed peers from the (no-bucket-only) all-peers-applied predicate.
func (n *Node) reapOrigins(ctx context.Context, src *s3fetch.Source) {
	if n.mirror == nil || n.cache == nil || n.originClaim == nil {
		return
	}
	var bucket map[crdt.Origin]crdt.Seq
	hasBucket := src != nil
	tipsOK := false
	if hasBucket {
		var err error
		bucket, err = src.DiscoverTips(ctx)
		tipsOK = err == nil
	}
	if n.peerFrontier != nil {
		n.peerFrontier.Refresh(ctx)
	}
	local := n.cache.AppliedTipMap()
	self := n.originClaim.Origin
	origins := n.mirror.Origins()
	reaped, kept := 0, 0
	for _, o := range origins {
		if ctx.Err() != nil {
			return
		}
		if o == self {
			// The self-log is truncated (not reaped) below the sealed
			// watermark: everything at/under ContiguousSealedSeq is durable
			// in S3, so a peer needing a trimmed seq gap-fills from the
			// bucket. RetainSealed keeps the active tail. No sealer (no
			// bucket) ⇒ no durable floor, so leave it.
			if n.sealer != nil {
				sealedTip := crdt.Seq(n.sealer.ContiguousSealedSeq(uint64(self)))
				if sealedTip > 0 {
					if err := n.mirror.RetainSealed(self, sealedTip); err != nil {
						n.log.Debug("syzy: reaper self-log trim", "err", err)
					}
				}
			}
			continue
		}
		lt := local[o]
		if lt == 0 {
			continue
		}
		sealed := bucket[o] >= lt
		replicated := n.peerFrontier != nil && n.peerFrontier.AllPeersApplied(o, lt)
		if !reapable(sealed, replicated, hasBucket) {
			kept++
			continue // keep as a catch-up source — see reapable
		}
		if err := n.mirror.Reap(o); err != nil {
			n.log.Debug("syzy: reaper reap", "origin", layout.OriginHex(o), "err", err)
			continue
		}
		reaped++
		if reaped >= reapBudgetPerPass {
			break
		}
	}
	if reaped > 0 || kept > 0 {
		n.log.Info("syzy: reaper pass",
			"mirror_origins", len(origins), "reaped", reaped, "kept", kept)
	}
	n.forgetDeadOrigins(ctx, bucket, hasBucket, tipsOK, self, origins)
}

// forgetDeadOrigins is the frontier half of origin GC: the reap loop above
// unlinks dead origins' mirror JOURNALS, but the per-origin frontier entry
// they leave behind is never revisited (the reap loop only walks live mirror
// journals), so retired origins accumulate in the frontier — inherited in
// full by every new node and iterated on every gossip/fetch pass. This pass
// evicts a frontier origin when it is (1) not self, (2) not a live mirror
// journal, and (3) absent from the bucket tips, i.e. retention has swept its
// epochs so it is dead cluster-wide and cannot be re-fetched or re-inherited.
//
// Guards: bucket mode only (no durable tier ⇒ no safe "dead" signal), and
// only when DiscoverTips succeeded — a failed or partial listing must never
// be read as "origin absent" (that would evict live origins and thrash). See
// EvictOrigin for the straggler/resurrection contract.
func (n *Node) forgetDeadOrigins(ctx context.Context, bucket map[crdt.Origin]crdt.Seq, hasBucket, tipsOK bool, self crdt.Origin, liveJournals []crdt.Origin) {
	if !hasBucket || !tipsOK {
		return
	}
	live := make(map[crdt.Origin]struct{}, len(liveJournals))
	for _, o := range liveJournals {
		live[o] = struct{}{}
	}
	forgot := 0
	for o := range n.cache.FrontierMap() {
		if ctx.Err() != nil {
			return
		}
		if o == self {
			continue
		}
		if _, hasJournal := live[o]; hasJournal {
			continue // still draining live — not dead
		}
		if _, inBucket := bucket[o]; inBucket {
			continue // epochs still durable — a behind node may re-fetch it
		}
		if n.cache.EvictOrigin(o) {
			forgot++
			if forgot >= reapBudgetPerPass {
				break
			}
		}
	}
	if forgot > 0 {
		n.log.Info("syzy: origin GC", "forgotten", forgot, "frontier_origins", n.cache.FrontierLen())
	}
}

// reapable decides whether a non-self mirror journal is safe to drop, given
// whether the origin is sealed to the object store up to our applied tip, whether
// every connected peer has applied it up to our tip, and whether a durable bucket
// exists at all.
//
//   - sealed: a behind member re-fetches the origin from the bucket, so dropping
//     our mirror copy loses nothing. Always safe.
//   - replicated (AllPeersApplied) is LIVENESS only: it covers currently-
//     connected, responsive peers, not absent ones (Refresh drops a peer that is
//     down or times out). Reaping an UNSEALED origin on this signal strands any
//     transiently-absent member permanently — the records are then gone from every
//     mirror and were never sealed, so neither a peer fetch nor a bucket fetch can
//     recover them; only a full re-clone can. So it is trusted ONLY in best-effort
//     no-bucket mode, where there is no durability tier to wait for anyway. When a
//     bucket exists, require the seal (the secondary drainer seals even retired/
//     dead origins, so every origin reaches it — no unbounded retention).
func reapable(sealed, replicated, hasBucket bool) bool {
	if sealed {
		return true
	}
	return replicated && !hasBucket
}
