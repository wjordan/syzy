package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/clone"
	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/internal/nodestate"
)

type pinnedSnapshot struct {
	bundle    *clone.PinnedBundle
	frontier  map[crdt.Origin]nodestate.FrontierEntry
	schemaSeq uint64
	timings   pinTimings
}

// pinTimings is a per-phase breakdown of one snapshotPinnedLocked call.
// Sum of phases ≈ total wall time of the function; BarrierHold is the
// portion during which the WAL writer slot is held (i.e., concurrent
// writers see SQLITE_BUSY).
type pinTimings struct {
	PreDrain      time.Duration // outside barrier
	BarrierAcq    time.Duration // sqlite open + BEGIN IMMEDIATE
	TailDrain     time.Duration // inside barrier
	SnapshotFlush time.Duration // SnapshotOnce → metadata.db
	PinInit       time.Duration // backup_init + step(1) on both DBs
	BarrierRel    time.Duration // closeWriterBarrier
}

func (t pinTimings) BarrierHold() time.Duration {
	return t.BarrierAcq + t.TailDrain + t.SnapshotFlush + t.PinInit + t.BarrierRel
}

func (t pinTimings) Total() time.Duration {
	return t.PreDrain + t.BarrierHold()
}

func (s *pinnedSnapshot) Close() {
	if s == nil {
		return
	}
	if s.bundle != nil {
		s.bundle.Close()
		s.bundle = nil
	}
}

// snapshotPinned takes n.writeMu and runs the barrier-pin protocol.
// It is the public-API surface for callers (PublishSnapshot) that do
// not need writeMu held for the byte-drain phase. ServeBundle holds
// writeMu through Stream itself, so it uses snapshotPinnedLocked
// directly.
func (n *Node) snapshotPinned(ctx context.Context) (*pinnedSnapshot, error) {
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	return n.snapshotPinnedLocked(ctx)
}

// snapshotPinnedLocked runs the writer-barrier protocol described in
// ARCHITECTURE.md "Bootstrap & Repair". Caller must hold n.writeMu.
//
//  1. Pre-drain producer + secondaries OUTSIDE the barrier. This is
//     the bulk of the wait time and doesn't need the WAL writer slot
//     held — the drainer is already async and racing with concurrent
//     commits is fine; we just want to absorb most of the latency
//     before stalling other writers.
//  2. Hold a WAL writer-slot barrier on app.db (BEGIN IMMEDIATE).
//  3. Re-drain to catch the small tail of records that landed between
//     pre-drain and barrier acquisition. Converges quickly because
//     the barrier has frozen all journal heads.
//  4. Flush the cache to metadata.db (Snapshotter.SnapshotOnce).
//  5. Pin sqlite3_backup reads on metadata.db and app.db (one
//     backup_step each, opening read transactions on both).
//  6. Release the barrier. Concurrent app.db writers (gated only by
//     the WAL slot, not by writeMu) resume; the pinned read txns
//     survive new commits.
//
// On success, the returned pinnedSnapshot holds backup handles whose
// remaining bytes can be drained later (via bundle.Stream or
// bundle.Files). Caller must Close.
func (n *Node) snapshotPinnedLocked(ctx context.Context) (*pinnedSnapshot, error) {
	var t pinTimings
	mark := time.Now()

	if err := n.waitAllDrained(ctx); err != nil {
		return nil, fmt.Errorf("syzy: pre-barrier drain: %w", err)
	}
	t.PreDrain = time.Since(mark)
	mark = time.Now()

	barrier, err := openWriterBarrier(n.appPath)
	if err != nil {
		return nil, fmt.Errorf("syzy: acquire writer barrier: %w", err)
	}
	t.BarrierAcq = time.Since(mark)
	released := false
	defer func() {
		if !released {
			_ = closeWriterBarrier(barrier, true)
		}
	}()

	mark = time.Now()
	if err := n.waitAllDrained(ctx); err != nil {
		return nil, fmt.Errorf("syzy: in-barrier drain: %w", err)
	}
	t.TailDrain = time.Since(mark)
	mark = time.Now()

	if err := n.snap.SnapshotOnceCtx(ctx); err != nil {
		return nil, fmt.Errorf("syzy: flush cache to metadata.db: %w", err)
	}
	t.SnapshotFlush = time.Since(mark)
	mark = time.Now()

	pb, err := clone.PinSnapshots(n.appPath)
	if err != nil {
		return nil, fmt.Errorf("syzy: pin snapshots: %w", err)
	}
	t.PinInit = time.Since(mark)
	mark = time.Now()

	if err := closeWriterBarrier(barrier, false); err != nil {
		pb.Close()
		return nil, fmt.Errorf("syzy: release writer barrier: %w", err)
	}
	released = true
	t.BarrierRel = time.Since(mark)

	n.log.Debug("syzy: snapshot pinned",
		slog.Duration("total", t.Total()),
		slog.Duration("barrier_hold", t.BarrierHold()),
		slog.Duration("pre_drain", t.PreDrain),
		slog.Duration("barrier_acq", t.BarrierAcq),
		slog.Duration("tail_drain", t.TailDrain),
		slog.Duration("snapshot_flush", t.SnapshotFlush),
		slog.Duration("pin_init", t.PinInit),
		slog.Duration("barrier_rel", t.BarrierRel),
	)

	// Capture metadata. After barrier release, additional commits may
	// land on app.db, but the pinned read txns inside pb were opened
	// inside the barrier and survive concurrent commits. The cache's
	// FrontierMap and the metadata's schema_seq advanced contiguously
	// up to the same boundary.
	frontier := n.cache.FrontierMap()
	schemaSeq, _, err := n.meta.GetSchemaSeq()
	if err != nil {
		pb.Close()
		return nil, fmt.Errorf("syzy: read schema_seq: %w", err)
	}

	return &pinnedSnapshot{
		bundle:    pb,
		frontier:  frontier,
		schemaSeq: schemaSeq,
		timings:   t,
	}, nil
}

// ServeBundle writes a clone-bundle of this node to w. Used by the
// daemon's TCP transport as a BundleHandler so a fresh peer can
// bootstrap from this one with `syzy clone tcp://here:port new.db`.
// See ARCHITECTURE.md "Bootstrap & Repair" for the protocol.
//
// Holds n.writeMu for the entire call: the barrier-pin orchestration
// AND the byte streaming. Holding writeMu through Stream prevents
// Node.Exec writes from racing with the pinned reads' WAL backing
// (PinSnapshots' read txns survive new commits, but the app's writer
// pipeline still sees a paused appearance).
//
// Returns ErrClosed if Close has begun: the transport's bundle
// handler may fire after Close on a quiescing node, and dereferencing
// a closed metadata would panic.
func (n *Node) ServeBundle(w io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	// Acquire writeMu first, then re-check closed under it. Close
	// flips closed=true outside writeMu, then takes writeMu briefly to
	// drain any in-flight ServeBundle/PublishSnapshot before tearing
	// down the metadata. This ordering guarantees that once we observe
	// closed=false here, the rest of the call runs against a live
	// metadata (Close cannot reach the teardown phase while we hold
	// writeMu).
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	n.closeMu.Lock()
	closed := n.closed
	n.closeMu.Unlock()
	if closed {
		return ErrClosed
	}

	snap, err := n.snapshotPinnedLocked(ctx)
	if err != nil {
		return err
	}
	defer snap.Close()
	return snap.bundle.Stream(w)
}

// PublishSnapshot publishes writer-barrier-consistent app and metadata
// baseline LTXes through this node's active publisher generation, then advances
// both HEAD baseline pointers in one CAS. Equivalent immutable uploads and an
// already-covered HEAD are idempotent; a stale or expired generation cannot
// move the pointers.
//
// Errors with ErrNoObjectBackend if Config.ObjectBackend was nil.
//
// Holds n.writeMu only across the writer-barrier-pin window
// (snapshotPinnedLocked). Once the pin is established, writeMu is
// released and the bundle drain + object upload + HEAD CAS run free of
// the lock — they only touch the pinned bundle's independent
// SQLite conns and immutable Node state (objectBackend, clusterID).
// Concurrent Node.Exec / db.BeginTx callers therefore block only
// for the brief barrier-pin phase, not for the duration of the S3
// round-trip.
func (n *Node) PublishSnapshot(ctx context.Context) error {
	if n.objectBackend == nil {
		return ErrNoObjectBackend
	}
	if n.publisher == nil {
		return ErrNoObjectBackend
	}
	tCall := time.Now()
	var tPredrained, tLocked, tReleased, tUpload time.Time
	var filesDur time.Duration
	var snapTimings pinTimings
	var baselineTXID uint64
	pubErr := n.publisher.PublishCoupledBaseline(ctx, func(opCtx context.Context, txid uint64) ([]byte, []byte, func(), error) {
		baselineTXID = txid
		// Pre-drain off writeMu. The generation operation context cancels this
		// work during lease teardown before database connections are closed.
		if err := n.waitAllDrained(opCtx); err != nil {
			return nil, nil, nil, fmt.Errorf("syzy: pre-barrier drain: %w", err)
		}
		tPredrained = time.Now()

		snap, err := func() (*pinnedSnapshot, error) {
			n.writeMu.Lock()
			tLocked = time.Now()
			defer func() {
				tReleased = time.Now()
				n.writeMu.Unlock()
			}()
			n.closeMu.Lock()
			closed := n.closed
			n.closeMu.Unlock()
			if closed {
				return nil, ErrClosed
			}
			return n.snapshotPinnedLocked(opCtx)
		}()
		if err != nil {
			return nil, nil, nil, err
		}
		snapTimings = snap.timings

		tFiles := time.Now()
		metaPath, appPath, err := snap.bundle.Files()
		filesDur = time.Since(tFiles)
		if err != nil {
			snap.Close()
			return nil, nil, nil, fmt.Errorf("syzy: drain pinned backups: %w", err)
		}
		tUpload = time.Now()
		var appBuf, metaBuf bytes.Buffer
		if _, err := ltxstream.EncodeBaseline(opCtx, &appBuf, appPath, txid); err != nil {
			snap.Close()
			return nil, nil, nil, fmt.Errorf("syzy: encode app baseline LTX: %w", err)
		}
		if _, err := ltxstream.EncodeBaseline(opCtx, &metaBuf, metaPath, txid); err != nil {
			snap.Close()
			return nil, nil, nil, fmt.Errorf("syzy: encode meta baseline LTX: %w", err)
		}
		return appBuf.Bytes(), metaBuf.Bytes(), snap.Close, nil
	})
	if tUpload.IsZero() {
		return pubErr
	}
	uploadDur := time.Since(tUpload)

	n.log.Debug("syzy: publish snapshot",
		slog.Duration("total", time.Since(tCall)),
		slog.Duration("predrain", tPredrained.Sub(tCall)),
		slog.Duration("writemu_wait", tLocked.Sub(tPredrained)),
		slog.Duration("writemu_hold", tReleased.Sub(tLocked)),
		slog.Duration("pin_total", snapTimings.Total()),
		slog.Duration("pin_barrier_hold", snapTimings.BarrierHold()),
		slog.Duration("bundle_files", filesDur),
		slog.Duration("obj_publish", uploadDur),
		slog.Uint64("baseline_txid", baselineTXID),
	)
	return pubErr
}

// ErrNoObjectBackend is returned by PublishSnapshot when no
// ObjectBackend was configured at Open time.
var ErrNoObjectBackend = errors.New("syzy: no ObjectBackend configured")

// FrontierEntry is the public form of one origin's apply state:
// LastSeq is the contiguous-applied head; AppliedTip is the highest
// seq the broker has accepted (≥ LastSeq when applied_gaps is
// non-empty, e.g. when an early seq from this origin was never seen).
// LastHLC is the HLC at LastSeq, packed as the wire encoding.
