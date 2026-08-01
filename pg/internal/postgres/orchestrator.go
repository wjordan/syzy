package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/engine"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/transport"
)

// draftBuffer bounds the in-flight decoded drafts capture may queue ahead of the
// orchestrator's fold. It only smooths bursts: a full buffer backpressures
// capture (it pauses decoding) and is drained as the orchestrator loops, so the
// value is a throughput knob, never a correctness one.
const draftBuffer = 256

// catchUpTimeout bounds the orchestrator's wait for capture to reach a WAL
// target during a remote apply's catch-up drain, so a stalled capturer surfaces
// an error rather than hanging the apply.
const catchUpTimeout = 15 * time.Second

// publishPoll is the publisher's safety timeout when it has caught up to the
// self-log head: it normally wakes on journal.Notify, but polls this often to
// catch a missed wake (the notify channel coalesces). broadcastBackoff is the
// retry pause after a transport error — the publisher retries the same entry
// rather than tearing down, keeping the network send off the actor's path.
// Vars, not consts, so tests can shrink them.
var (
	publishPoll      = 50 * time.Millisecond
	broadcastBackoff = 100 * time.Millisecond
)

// orchestrator is the single serialized actor (§9 / pg-coordination-model §2):
// the one goroutine that mutates the Cache. Capture decodes the WAL
// concurrently and only enqueues drafts; the orchestrator folds those local
// drafts (alloc Dot/HLC/CL, build changeset, persist row state, broadcast) and
// applies inbound peer changesets — both on its own goroutine. Collapsing the
// two former writers (capture-fold and apply) into one removes the
// cross-goroutine race by construction: there is no concurrent
// read-modify-write of the Cache left.
type orchestrator struct {
	capt   *capturer
	appl   *applier
	prog   *progress
	drafts chan *txnAccum

	// Self-origin durability (pg-coordination-model §3), nil when disabled
	// (no Meta/JournalDir). selfLog holds the exact bytes of every folded local
	// changeset; shipped is the highest commit LSN whose changeset is fsynced
	// there (capture reports it as confirmed_flush); skipThrough is the self-log
	// head at Open — re-delivered commits at or below it were already folded
	// (and recovered via recoverSelf) and must not be re-folded.
	//
	// delivered is the self-log offset past the last changeset the transport
	// durably accepted (broadcast returned nil). The publisher goroutine
	// (publish) ships from the log asynchronously and advances it; checkpoint
	// retention holds the log until delivered, so an undelivered entry is never
	// GC'd. shipped (confirmed_flush) may run ahead of delivered: once a commit
	// is fsynced in the log the slot can release its WAL, because the log — not
	// the slot — is what the publisher ships from and recovery replays.
	selfLog     *journal.Journal
	shipped     atomic.Uint64
	delivered   atomic.Uint64
	skipThrough pglogrepl.LSN
	sinceCkpt   int

	// Schema-log integration (§6), nil when no SchemaLog is configured. Safe to
	// touch the catalog from here because capture no longer reads it (D4):
	// schemaSeq is the node's schema head (shared with capture, which stamps it
	// as a changeset's Deps); catchUp applies pending schema events before a
	// gated remote DML is arbitrated.
	schemaSeq *atomic.Uint64
	catchUp   func(context.Context) error

	// DDL lease gate (§6 increment E), nil when no Lease is configured. The gate
	// manager runs on its own goroutine and serializes cross-node DDL; it requests
	// schema catch-up over catchUpReq so catchUpSchema still runs only here (the
	// sole catalog writer). gate is started/stopped by Run.
	gate       *gateManager
	catchUpReq chan chan error

	// Anti-entropy (fetcher.go), active when gapFiller is set. fetched routes
	// each pulled changeset onto this goroutine; fetchWake nudges the loop
	// when applyRemote observes an out-of-order seq.
	gapFiller transport.GapFiller
	tipSource transport.TipSource
	fetched   chan fetchReq
	fetchWake chan struct{}

	// Object-store sealing (Config.OnPublished / SealedSelfSeq), nil/zero when
	// no bucket is configured. The publish goroutine hands each shipped
	// changeset to onPublished (the sealer) and records (seq, end offset)
	// under sealMu; checkpoint retention then never truncates the self-log
	// past the last entry the sealer has made bucket-durable. bucketTips is
	// the fetcher's latest DiscoverTips snapshot, consumed by checkpoint to
	// truncate mirror segments that are fully sealed in the bucket.
	onPublished   func(payload []byte)
	sealedSelfSeq func() crdt.Seq
	sealMu        sync.Mutex
	sealPending   []sealMark // ascending by seq/offset
	sealedOffset  journal.Offset
	tipsMu        sync.Mutex
	bucketTips    map[crdt.Origin]crdt.Seq

	// Peer catchup mirror: each applied REMOTE changeset's wire bytes are
	// journaled per-origin so this node can serve future peer gap-fill requests
	// (Engine.CatchupSource). Own-origin bytes stay in the self-log, served via
	// the same source. nil ⇒ no remote-origin peer catchup.
	mirror     *mirror.Manager
	selfOrigin crdt.Origin
}

// sealMark records that the self-log entry ending at off carries seq; once
// the sealer reports seq durable, retention may advance to off.
type sealMark struct {
	seq crdt.Seq
	off journal.Offset
}

func newOrchestrator(capt *capturer, appl *applier, prog *progress) *orchestrator {
	return &orchestrator{
		capt:       capt,
		appl:       appl,
		prog:       prog,
		drafts:     make(chan *txnAccum, draftBuffer),
		catchUpReq: make(chan chan error),
		fetched:    make(chan fetchReq),
		fetchWake:  make(chan struct{}, 1),
	}
}

// setShipped advances the highest-shipped commit LSN (a monotonic max). Capture
// reports it as the slot's confirmed_flush, so the slot never releases WAL for a
// commit whose changeset is not yet durable in the self-log.
func (o *orchestrator) setShipped(lsn pglogrepl.LSN) {
	for {
		cur := o.shipped.Load()
		if uint64(lsn) <= cur || o.shipped.CompareAndSwap(cur, uint64(lsn)) {
			return
		}
	}
}

// Run starts capture (decode-only, enqueuing drafts) and then runs the actor
// loop on this goroutine until ctx is cancelled. A capture failure cancels the
// loop and is surfaced as Run's error.
func (o *orchestrator) Run(ctx context.Context, inbox <-chan *crdt.Changeset, broadcast engine.Sink) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// DDL lease gate (§6 E): close the gate and start the watcher before capture,
	// so a user DDL that arrives the instant we start is serialized, not admitted
	// ungated. Stopped (lease + gate released, conn closed) on Run exit.
	if o.gate != nil {
		o.gate.start(runCtx) // gate already shut in Open; this launches the admit watcher
		defer o.gate.stop()
	}

	// Capture decodes and enqueues; it never folds. Folding happens only on the
	// orchestrator goroutine, so the Cache keeps a single writer.
	//
	// With a self-log, capture reports the orchestrator's shipped LSN as
	// confirmed_flush (confirmedLSN) — the slot advances only past commits whose
	// changeset is fsynced, and the orchestrator owns checkpointing. Without one
	// (no Meta/JournalDir), capture runs noAck: it must not advance the slot,
	// because a draft is shipped only after the orchestrator folds/broadcasts it,
	// so a per-commit ack could release WAL for a commit not yet broadcast.
	opts := runOpts{noAck: true}
	if o.selfLog != nil {
		opts = runOpts{confirmedLSN: &o.shipped}
	}

	// Each background goroutine writes only its OWN error var; both are read on
	// this goroutine after wg.Wait() (happens-before), so there is no shared
	// write to synchronize.
	var wg sync.WaitGroup
	var captErr, pubErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := o.capt.run(runCtx, o.enqueue, opts); err != nil && runCtx.Err() == nil {
			captErr = err
			cancel() // surface a capture failure by stopping the loop
		}
	}()

	// With a self-log the publisher ships downstream of the durability boundary:
	// the actor (fold) only appends+fsyncs, and this goroutine tails the log and
	// broadcasts. A transport stall/error never blocks folding or holds back
	// confirmed_flush, and a crash between append and broadcast loses nothing —
	// the publisher re-ships from the retained log on restart (peers dedup by
	// Dot). Without a self-log there is no outbox, so fold broadcasts inline.
	if o.selfLog != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := o.publish(runCtx, broadcast); err != nil && runCtx.Err() == nil {
				pubErr = err
				cancel()
			}
		}()
	}

	// Anti-entropy fetcher (fetcher.go): plans missing ranges and pulls them
	// from peers; each fetched changeset arrives on o.fetched in loop().
	if o.gapFiller != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.fetcherLoop(runCtx)
		}()
	}

	loopErr := o.loop(runCtx, inbox, broadcast)
	cancel()
	wg.Wait()
	if loopErr != nil {
		return loopErr
	}
	if captErr != nil {
		return captErr
	}
	if pubErr != nil {
		return pubErr
	}
	// Persist a final snapshot so a clean restart replays little or no self-log.
	return o.checkpoint()
}

// enqueue is capture's draftProcess in live mode: hand the draft to the
// orchestrator goroutine. It never folds (so emitted is always false — stopAfter
// is unused in the live loop). Blocking on a full channel backpressures capture.
func (o *orchestrator) enqueue(ctx context.Context, t *txnAccum) (bool, error) {
	select {
	case o.drafts <- t:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return false, nil
}

func (o *orchestrator) loop(ctx context.Context, inbox <-chan *crdt.Changeset, broadcast engine.Sink) error {
	// Quarantine retry cadence (no-op without Meta): re-apply deterministic
	// failures whose missing dependency may since have landed.
	retryTick := time.NewTicker(quarantineRetryInterval)
	defer retryTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-o.drafts:
			if err := o.fold(ctx, t, broadcast); err != nil {
				return err
			}
		case cs := <-inbox:
			if err := o.applyRemote(ctx, cs, broadcast); err != nil {
				return err
			}
		case f := <-o.fetched:
			f.err <- o.applyRemote(ctx, f.cs, broadcast)
		case <-retryTick.C:
			o.retryQuarantined(ctx)
		case reply := <-o.catchUpReq:
			// The DDL gate (another goroutine) asks the sole catalog writer to
			// apply pending schema events before it opens the gate for a waiting
			// local DDL transaction. Errors are non-fatal to the loop — they go
			// back to the gate, which keeps the gate closed and retries.
			var err error
			if o.catchUp != nil {
				err = o.catchUp(ctx)
			}
			reply <- err
		}
	}
}

// fold turns one queued local draft into a changeset and makes it durable, on
// the orchestrator goroutine (the sole Cache writer). With a self-log the
// append+fsync IS the durability commit and the boundary the publisher ships
// from: build → append exact bytes + fsync → prune → mark shipped. fold does
// NOT broadcast — the publisher goroutine does, downstream of the log — so a
// transport stall never blocks the actor and a crash before delivery loses
// nothing. Without a self-log there is no outbox, so fold broadcasts inline
// (the legacy non-durable live path).
func (o *orchestrator) fold(ctx context.Context, t *txnAccum, broadcast engine.Sink) error {
	if o.selfLog != nil && t.endLSN <= o.skipThrough {
		// Already folded before a restart and recovered from the self-log; the
		// slot re-delivered it because a standby ack lagged the append. Re-folding
		// would build a duplicate Dot and re-derive its stamp — drop it, just let
		// confirmed_flush move past it. The publisher still ships it from the log.
		o.setShipped(t.endLSN)
		return nil
	}
	// DDL first: append the transaction's schema Bundle (onDDLIntents =
	// appendDDLBundle, which adds the table to the catalog) and prune the consumed
	// intent rows BEFORE foldCommit. So the catalog has the new table before
	// foldCommit resolves any DML on it (schema-then-DML, even mixed in one txn),
	// and foldCommit stamps the now-advanced schemaSeq as the changeset's
	// Deps[SchemaChain]. Safe on this goroutine because capture no longer reads
	// the catalog (D4). (Restart idempotency of the append — a crash between
	// append and prune — is increment F's batch_id.)
	if len(t.ddlIntents) > 0 && o.capt.onDDLIntents != nil {
		if err := o.capt.onDDLIntents(ctx, t.ddlIntents); err != nil {
			return err
		}
		o.capt.pruneDDLIntents(ctx, t.ddlIntentSeqs)
	}
	cs, lf, err := o.capt.foldCommit(t)
	if err != nil {
		return err
	}
	// Winner-repair (§9 Option A): execute any pending self-correct UPSERTs that
	// foldCommit deferred when a local record's (CL, Stamp) lost to a stashed
	// peer winner. Runs on the apply conn (same goroutine), before broadcasting
	// the changeset. Capture decodes the UPSERTed bytes on a later WAL pass and
	// re-folds at a higher stamp that dominates the stash — convergent.
	if lf != nil && len(lf.selfCorrect) > 0 {
		if err := o.appl.applySelfCorrect(ctx, lf.selfCorrect); err != nil {
			return err
		}
	}
	if cs != nil {
		if o.selfLog != nil {
			payload := encodeSelfLogPayload(t.endLSN, cs.Encoded())
			if _, _, err := o.selfLog.Append(journal.KindLocalDML, cs.Stamp.Clock.Pack(), uint64(cs.Dot.Origin), payload); err != nil {
				return fmt.Errorf("postgres: self-log append: %w", err)
			}
			if err := o.selfLog.Sync(); err != nil {
				return fmt.Errorf("postgres: self-log sync: %w", err)
			}
		} else if err := broadcast(ctx, cs); err != nil {
			return err
		}
	}
	o.setShipped(t.endLSN)
	return o.maybeCheckpoint()
}

// publish is the async self-log publisher (pg-coordination-model §2/§3). It
// tails the self-log and broadcasts each local changeset to the transport,
// off the actor's goroutine. delivered advances only after the transport
// durably accepts an entry (broadcast returned nil), and checkpoint retention
// holds the log until delivered — so an entry is never GC'd before delivery,
// and a crash before delivery just means it is re-shipped from the retained
// log on the next run (idempotent: peers dedup by Dot). It re-creates the
// iterator from delivered each pass (matching syncer.Drainer) so a retained-log
// GC never strands it.
func (o *orchestrator) publish(ctx context.Context, broadcast engine.Sink) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		it := o.selfLog.Iterate(journal.Offset(o.delivered.Load()))
		progressed := false
		for {
			rec, _, err := it.Next()
			if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
				break
			}
			if err != nil {
				return fmt.Errorf("postgres: publisher read self-log: %w", err)
			}
			next := it.Offset()
			if rec.Kind != journal.KindLocalDML || rec.Aborted() {
				o.delivered.Store(uint64(next))
				progressed = true
				continue
			}
			_, encoded, err := decodeSelfLogPayload(rec.Payload)
			if err != nil {
				return err
			}
			cs, err := crdt.Decode(encoded)
			if err != nil {
				return fmt.Errorf("postgres: publisher decode self-log changeset: %w", err)
			}
			if o.onPublished != nil {
				o.onPublished(encoded)
				o.sealMu.Lock()
				o.sealPending = append(o.sealPending, sealMark{seq: cs.Dot.Seq, off: next})
				o.sealMu.Unlock()
			}
			if err := o.broadcastWithRetry(ctx, broadcast, cs); err != nil {
				return nil // ctx cancelled mid-retry: clean stop
			}
			o.delivered.Store(uint64(next))
			progressed = true
		}
		if !progressed {
			if err := o.waitPublish(ctx); err != nil {
				return nil
			}
		}
	}
}

// broadcastWithRetry hands one changeset to the transport, retrying on a
// transient error rather than tearing down the publisher (so the actor keeps
// folding and confirmed_flush keeps advancing through a transport hiccup).
// Returns ctx.Err() only when cancelled, which publish treats as a clean stop.
func (o *orchestrator) broadcastWithRetry(ctx context.Context, broadcast engine.Sink, cs *crdt.Changeset) error {
	for {
		if err := broadcast(ctx, cs); err == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(broadcastBackoff):
		}
	}
}

// waitPublish blocks until the self-log grows or ctx is cancelled, with a poll
// fallback for a coalesced/missed wake.
func (o *orchestrator) waitPublish(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.selfLog.Notify():
		return nil
	case <-time.After(publishPoll):
		o.selfLog.Refresh()
		return nil
	}
}

// maybeCheckpoint persists a Cache snapshot every checkpointEvery folds and
// compacts the self-log. Checkpoints are pure compaction now (the self-log, not
// the snapshot, is the durability boundary): a fresher snapshot just means less
// self-log to replay on restart.
func (o *orchestrator) maybeCheckpoint() error {
	if o.selfLog == nil {
		return nil
	}
	o.sinceCkpt++
	if o.sinceCkpt < o.capt.checkpointEvery() {
		return nil
	}
	return o.checkpoint()
}

// checkpoint snapshots the Cache (covering all folds so far) and unlinks the
// self-log segments before the last-delivered offset — those entries are
// redundant with the snapshot for recovery AND already handed to the transport,
// so neither recovery nor the publisher needs them. Retaining at delivered (not
// Head) is the §3 peer-delivery bound: an entry the publisher has not yet
// shipped is never GC'd, even though its Cache effect is already in the
// snapshot and confirmed_flush may have passed it.
func (o *orchestrator) checkpoint() error {
	if o.selfLog == nil || o.capt.cfg.Meta == nil {
		return nil
	}
	o.sinceCkpt = 0
	if err := o.capt.checkpoint(pglogrepl.LSN(o.shipped.Load())); err != nil {
		return err
	}
	retain := journal.Offset(o.delivered.Load())
	// With a sealer, retention is additionally gated on bucket durability:
	// never truncate past the last self-log entry whose seq the sealer has
	// uploaded, so the exact shipped bytes survive locally until the bucket
	// holds them.
	if o.sealedSelfSeq != nil {
		sealed := o.sealedSelfSeq()
		o.sealMu.Lock()
		for len(o.sealPending) > 0 && o.sealPending[0].seq <= sealed {
			o.sealedOffset = o.sealPending[0].off
			o.sealPending = o.sealPending[1:]
		}
		if o.sealedOffset < retain {
			retain = o.sealedOffset
		}
		o.sealMu.Unlock()
	}
	if err := o.selfLog.RetainAfter(retain); err != nil {
		return err
	}
	// Mirror GC: truncate peer-origin segments that are fully sealed in the
	// bucket (per the fetcher's latest tip snapshot). Peers below the mirror
	// horizon fall back to the object store via their gap-filler chain.
	if o.mirror != nil {
		o.tipsMu.Lock()
		tips := o.bucketTips
		o.bucketTips = nil
		o.tipsMu.Unlock()
		for org, tip := range tips {
			if org == o.selfOrigin {
				continue
			}
			if err := o.mirror.RetainSealed(org, tip); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyRemote folds every pending local draft up to the current WAL head into
// the Cache before arbitrating the peer changeset, then applies it. This is the
// restructured capture-catch-up gate: rather than a separate apply goroutine
// waiting on capture's progress (the old waitCaptureCaughtUp), the one
// orchestrator goroutine folds the pending drafts itself.
func (o *orchestrator) applyRemote(ctx context.Context, cs *crdt.Changeset, broadcast engine.Sink) error {
	if err := o.drainToWALTarget(ctx, broadcast); err != nil {
		return err
	}
	// Peer-catchup mirror: journal the exact bytes BEFORE apply so a peer asking
	// for this (origin, seq) gets them whether or not THIS apply commits — but
	// only the first time. Skip a changeset we've already applied: redelivery is
	// routine (the publisher re-ships its retained log on restart; peers dedup by
	// Dot), and mirror.Append does NOT dedup nor GC by seq, so re-appending an
	// applied changeset would grow the per-origin journal without bound. The
	// bytes are already mirrored from the first delivery. Own-origin entries are
	// served from the self-log, never re-mirrored, so the journals stay one-writer.
	if o.mirror != nil && cs.Dot.Origin != o.selfOrigin &&
		!o.appl.cfg.Cache.IsAppliedRemote(cs.Dot.Origin, cs.Dot.Seq) {
		if err := o.mirror.Append(cs.Dot.Origin, cs.Encoded()); err != nil {
			return fmt.Errorf("postgres: mirror append: %w", err)
		}
	}
	// Schema gate (§6): hold the DML until the local catalog reaches the schema
	// event it was produced under. Catch up from the shared log; if the dep is
	// still unmet the originator has not yet published that event, so surface it
	// rather than apply against a catalog missing the table.
	if o.catchUp != nil && o.schemaSeq != nil {
		if need := uint64(cs.Deps[crdt.SchemaChain]); need > o.schemaSeq.Load() {
			if err := o.catchUp(ctx); err != nil {
				return err
			}
			if need > o.schemaSeq.Load() {
				return fmt.Errorf("postgres: changeset schema dep %d exceeds local head %d", need, o.schemaSeq.Load())
			}
		}
	}
	// A deterministic integrity-constraint failure (SQLSTATE class 23) would
	// re-fail identically on every redelivery and pin this origin's frontier
	// forever. Quarantine it and advance (the SQLite broker's policy); every
	// other failure stays fatal to Run — restart + redelivery is the recovery
	// path for transients.
	if err := o.appl.Apply(ctx, cs); err != nil {
		if isDeterministicApplyErr(err) && o.quarantineApplyFailure(cs, err) {
			return nil
		}
		return err
	}
	// An out-of-order apply leaves a gap below cs.Dot.Seq (the frontier
	// stalled behind it) — wake the fetcher instead of waiting out its timer.
	if o.gapFiller != nil {
		if fr, ok := o.appl.cfg.Cache.FrontierFor(cs.Dot.Origin); !ok || fr.LastSeq < cs.Dot.Seq {
			o.kickFetch()
		}
	}
	return nil
}

// drainToWALTarget folds local drafts until every commit at or before the
// current WAL head is in the Cache. Correctness rests on capture enqueuing a
// commit's draft BEFORE advancing prog past that commit's LSN (the post-switch
// c.prog.advance in capture's run loop): once prog reaches target, every draft
// for a commit ≤ target is already queued, so the FIFO drain folds them all
// before the caller arbitrates. Folding a draft slightly past target is harmless
// (a valid local commit, folded in order). No deadlock: the loop is actively
// receiving, so capture's enqueue never blocks the drain.
func (o *orchestrator) drainToWALTarget(ctx context.Context, broadcast engine.Sink) error {
	target, err := o.appl.currentWALLSN(ctx)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(catchUpTimeout)
	for o.prog.load() < target {
		if time.Now().After(deadline) {
			return fmt.Errorf("postgres: capture catch-up timeout (progress=%s target=%s)", o.prog.load(), target)
		}
		select {
		case t := <-o.drafts:
			if err := o.fold(ctx, t, broadcast); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
			// capture advanced prog via a keepalive (no draft to fold); re-check.
		}
	}
	// prog ≥ target ⇒ the draft at target (if any) is already queued; fold the
	// drafts the blocking loop didn't consume, then arbitrate.
	for {
		select {
		case t := <-o.drafts:
			if err := o.fold(ctx, t, broadcast); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}
