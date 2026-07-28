package nodestate

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/metadata"
)

// metaKeyParentAppTXID must match publisher.MetaKeyParentAppTXID.
const metaKeyParentAppTXID = "parent_app_txid"

// JournalProvider is the optional resolver Snapshotter consults at GC
// time to map an origin id → its journal. Returning (nil, nil) means
// "no journal for this origin; skip"; returning a non-nil Journal
// makes it eligible for segment GC. Errors halt the per-origin GC
// pass for that snapshot but don't roll back the snapshot itself.
type JournalProvider interface {
	JournalFor(origin crdt.Origin) (*journal.Journal, error)
}

// Snapshotter periodically writes the Cache's dirty state to the
// metadata. The metadata tables (frontier, row_clock, meta) are the
// recovery source; the journal segments past Markers[origin] are what
// recovery replays on top of the snapshot.
//
// Behavior: on each tick or trigger, take a SnapshotIncremental, write it
// inside one metadata.WithTx, and on success clear only dirty entries still
// covered by that snapshot. Errors are returned via the channel passed to
// Run; production wiring should route them to a logger.
//
// Cross-stream coupling (publisher mode only): when SetCoupling has
// been called, every snapshot tx first drains the app.db LTX tailer
// (beforeTx → tailer.Sync) so bucket_txid reflects every app commit
// reachable from the cache state being snapshotted, then stamps that
// TXID inside the same metadata.db tx under "parent_app_txid".
// Restore replays metadata.db's chain to its tip, reads the stamp,
// and waits for the app.db chain to cover it before considering the
// restore consistent.
type Snapshotter struct {
	cache *Cache
	sc    *metadata.Store

	interval      time.Duration
	trigger       chan struct{}
	gc            bool
	provider      JournalProvider
	sealer        SealerProvider
	self          crdt.Origin
	retention     time.Duration
	beforeTx      func(context.Context) error
	parentAppTXID func() uint64

	// runMu serializes SnapshotOnceCtx calls. The Run loop and any
	// out-of-band caller (e.g. publisher.takeCoupledBaselines via
	// Node.snapshotPinned) both invoke SnapshotOnceCtx, and concurrent
	// calls would race on lastStampedAppTXID and double-apply
	// snapshot work into the metadata tx.
	runMu sync.Mutex

	// lastStampedAppTXID skips re-writing parent_app_txid when neither
	// the app stream nor the cache moved since the last snapshot tx —
	// avoids dirtying a metadata.db page (and emitting a meta LTX
	// frame) on idle ticks where only the stamp would change.
	// Guarded by runMu.
	lastStampedAppTXID uint64
}

// SealerProvider tells the snapshotter the highest CONTIGUOUS seq of
// origin that has been durably uploaded to object storage — every seq
// up to it is sealed, with no gap. The sealer GC gate applies to
// origins this node drains (own writes, plus any writer-process origins
// drained in daemon mode); their journal is the pre-seal buffer and
// segments must be retained until the records are durable elsewhere.
// A contiguous (not max) watermark is required: the sealer can upload
// epochs on both sides of an input-stream hole, and gating on a max
// watermark would let GC unlink the never-sealed source records behind
// the hole. sealer.Sealer.ContiguousSealedSeq satisfies this. Returning
// 0 means "nothing contiguously sealed yet" — refuse to GC drained
// origins.
type SealerProvider interface {
	ContiguousSealedSeq(origin uint64) uint64
}

// SnapshotterConfig configures a Snapshotter.
type SnapshotterConfig struct {
	// Interval is the wakeup period. <=0 disables periodic ticks; the
	// snapshotter only runs when explicitly triggered.
	Interval time.Duration
	// GC enables segment-level GC after each successful snapshot.
	// Calls journal.RetainAfter(snapshot_marker[origin]) on every
	// origin reachable via the JournalProvider — segments wholly
	// before the snapshotted marker are unlinked.
	//
	// Two gates apply per origin:
	//   - Drained origins (own self plus any locally-drained writer
	//     origins): the journal is the pre-seal buffer, so GC requires
	//     Sealer.ContiguousSealedSeq[origin] >= our head. With no
	//     Sealer set, drained origins are never pruned.
	//   - Mirror origins (purely remote): the marker alone suffices —
	//     the metadata reflects the cache state, and recovery's
	//     RecoverMirror walks only forward of the marker.
	GC bool
	// Journals resolves origin → *journal.Journal for GC. Required
	// when GC is true; otherwise unused.
	Journals JournalProvider
	// Sealer reports per-origin S3-uploaded watermarks. Required for
	// GC of drained origins; optional otherwise. Mirror origins are
	// GC'd marker-only and don't consult Sealer.
	Sealer SealerProvider
	// Self is this node's origin id. Used to identify the drained-by-
	// us origin that the producer feeds. Defaults to cache.Self() when
	// zero.
	Self crdt.Origin
}

// NewSnapshotter returns a Snapshotter ready to run.
func NewSnapshotter(c *Cache, sc *metadata.Store, cfg SnapshotterConfig) *Snapshotter {
	self := cfg.Self
	if self == 0 {
		self = c.Self()
	}
	return &Snapshotter{
		cache:    c,
		sc:       sc,
		interval: cfg.Interval,
		trigger:  make(chan struct{}, 1),
		gc:       cfg.GC,
		provider: cfg.Journals,
		sealer:   cfg.Sealer,
		self:     self,
	}
}

// SetCoupling installs the cross-stream coupling hooks used in
// publisher mode (BeforeTx and ParentAppTXID). Safe to call once,
// before Run starts. Single-node mode leaves these unset; passing nil
// values disables coupling.
func (s *Snapshotter) SetCoupling(beforeTx func(context.Context) error, parentAppTXID func() uint64) {
	s.beforeTx = beforeTx
	s.parentAppTXID = parentAppTXID
}

// EnableGC turns on post-snapshot segment GC. provider resolves
// origin→journal (self plus mirror origins); sealer gates drained-origin
// pruning on the S3-uploaded watermark; retention is the age floor below
// which segments are kept on disk even once snapshotted, so a peer offline
// less than retention can still gap-fill incrementally rather than
// rebaselining. A zero retention prunes purely by marker (no offline grace).
// Call once before Run.
func (s *Snapshotter) EnableGC(provider JournalProvider, sealer SealerProvider, retention time.Duration) {
	s.gc = true
	s.provider = provider
	s.sealer = sealer
	s.retention = retention
}

// Trigger schedules an immediate snapshot. Coalesces multiple triggers
// into one.
func (s *Snapshotter) Trigger() {
	select {
	case s.trigger <- struct{}{}:
	default:
	}
}

// Run drives the snapshot loop until ctx is cancelled. Returns nil on
// clean shutdown. The final snapshot before exit is best-effort: if
// the context is cancelled mid-write, the partially-applied tx rolls
// back via WithTx's defer.
func (s *Snapshotter) Run(ctx context.Context) error {
	var ticker *time.Ticker
	if s.interval > 0 {
		ticker = time.NewTicker(s.interval)
		defer ticker.Stop()
	}
	for {
		var tickC <-chan time.Time
		if ticker != nil {
			tickC = ticker.C
		}
		select {
		case <-ctx.Done():
			// One final snapshot on the way out so the next start has
			// minimal replay. Best-effort; ignore errors.
			finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.SnapshotOnceCtx(finalCtx)
			cancel()
			return nil
		case <-s.trigger:
		case <-tickC:
		}
		if err := s.SnapshotOnceCtx(ctx); err != nil {
			return fmt.Errorf("snapshotter: %w", err)
		}
	}
}

// SnapshotOnce runs one snapshot pass synchronously with a background
// context. Tests and clean-shutdown callers without publisher-mode
// coupling can use this; production wiring should prefer
// SnapshotOnceCtx so BeforeTx (app.db tailer.Sync) inherits cancel.
func (s *Snapshotter) SnapshotOnce() error {
	return s.SnapshotOnceCtx(context.Background())
}

// SnapshotOnceCtx runs one snapshot pass with the given context.
// beforeTx (if configured) sees this ctx; the metadata tx is not
// itself cancelable, but beforeTx can return ctx.Err() to abort.
// Concurrent callers serialize on runMu — two interleaved snapshots
// would race on lastStampedAppTXID and double-apply the cache's
// incremental dirty set.
func (s *Snapshotter) SnapshotOnceCtx(ctx context.Context) error {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	snap := s.cache.SnapshotIncremental()
	hasWork := snapshotHasWork(snap)
	if s.beforeTx != nil {
		if err := s.beforeTx(ctx); err != nil {
			return fmt.Errorf("snapshotter: beforeTx: %w", err)
		}
	}
	// Resolve parentAppTXID *before* opening the metadata tx — the
	// callback typically reads bucket_txid via Store.GetMeta, which
	// takes the same Store mutex WithTx holds.
	//
	// beforeTx is the SyncAppStream coupling: it drains pending app.db
	// WAL frames into LTX, which can bump LastBucketTXID. Resolving
	// parentAppTXID *after* beforeTx ensures the stamp covers any
	// frames drained by this call — without that ordering, a fork that
	// races with steady-state writes could stamp a stale TXID.
	var parentAppTX uint64
	if s.parentAppTXID != nil {
		parentAppTX = s.parentAppTXID()
	}
	stampParent := s.parentAppTXID != nil && parentAppTX != s.lastStampedAppTXID
	// Open a tx if there is either cache work to flush or a stamp to
	// write. Skipping when stampParent is true would prevent Fork from
	// reading a parent_app_txid for an idle source that never accrued
	// cache deltas between its last snapshot and the fork's
	// PublishSnapshot.
	if !hasWork && !stampParent {
		return nil
	}
	if err := s.sc.WithTx(func(tx *metadata.Tx) error {
		if err := WriteSnapshot(tx, snap); err != nil {
			return err
		}
		if stampParent {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], parentAppTX)
			if err := tx.SetMeta(metaKeyParentAppTXID, buf[:]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if stampParent {
		s.lastStampedAppTXID = parentAppTX
	}
	s.cache.ClearSnapshotDirty(snap)
	if s.gc {
		s.gcSegments(snap.Markers)
	}
	return nil
}

// gcSegments unlinks per-origin journal segments wholly before the
// snapshotted marker. Failures are silent here — GC is best-effort.
//
// Two gates apply:
//   - Drained origins (cache has a SenderNextSeq entry, including
//     self): the journal is the pre-seal buffer; require
//     Sealer.ContiguousSealedSeq[o] ≥ our head before pruning. No
//     sealer ⇒ never prune drained origins.
//   - Mirror origins (purely remote): the snapshot marker alone
//     suffices. Meta reflects cache state past the marker;
//     recovery's RecoverMirror walks only forward of it.
func (s *Snapshotter) gcSegments(markers map[crdt.Origin]journal.Offset) {
	if s.provider == nil {
		return
	}
	var olderThan time.Time
	if s.retention > 0 {
		olderThan = time.Now().Add(-s.retention)
	}
	for o, off := range markers {
		if off == 0 {
			continue
		}
		if !s.gcSafeFor(o) {
			continue
		}
		j, err := s.provider.JournalFor(o)
		if err != nil || j == nil {
			continue
		}
		// RetainAfterAged unlinks segments strictly before the segment
		// containing off AND older than the retention floor; the segment
		// with `off`, and any younger than the floor, are preserved (the
		// former may hold records past the marker we still need for
		// replay, the latter for offline-peer gap-fill).
		_ = j.RetainAfterAged(off, olderThan)
	}
}

func (s *Snapshotter) gcSafeFor(o crdt.Origin) bool {
	// Drained origins: gate on the sealer.
	next := s.cache.SenderNextSeq(o)
	drained := o == s.self || next > 1
	if !drained {
		return true // mirror journal: marker-only.
	}
	if s.sealer == nil {
		return false
	}
	var ourHead crdt.Seq
	if next > 0 {
		ourHead = next - 1
	} else if f, ok := s.cache.FrontierFor(o); ok {
		ourHead = f.LastSeq
	}
	if ourHead == 0 {
		// Marker is non-zero (caller's gate) but our local view shows
		// no emitted records. Inconsistent — refuse to GC.
		return false
	}
	// Contiguous, not max: everything we produced must be sealed with no
	// hole before we unlink any source segment. A max watermark that
	// sailed past an unsealed hole would let GC destroy the only
	// remaining copy of the un-republished records behind it.
	//
	// Scope: the watermark reflects only holes the sealer OBSERVED on its
	// OnEncoded feed. A hole minted below the sealer's first observation
	// of an origin (e.g. records consumed by the producer drain before the
	// publication listeners are wired) is invisible here — no seed, in-
	// memory or persisted, recovers it, because the sealer never saw the
	// gap. Closing that class requires the producer to wire its listeners
	// (self-log/broadcast/sealer) BEFORE the startup drain so every
	// produced record flows through the sealer; this gate is
	// defense-in-depth on top of that, not a substitute (see
	// ARCHITECTURE.md "Self-log").
	return crdt.Seq(s.sealer.ContiguousSealedSeq(uint64(o))) >= ourHead
}

func snapshotHasWork(s Snapshot) bool {
	if len(s.Rows) > 0 || len(s.Cells) > 0 || len(s.ClearedRows) > 0 {
		return true
	}
	if s.MetaDirty {
		return true
	}
	return false
}
