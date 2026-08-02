package sqlite

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/internal/publisher"
)

// startPublisher wires Node into the publisher controller (lease,
// two LTX tailers, coupled baseline). Caller must have ObjectBackend set.
func (n *Node) startPublisher() error {
	walPath := n.appPath + "-wal"
	metaWALPath := layout.MetaDB(n.appPath) + "-wal"
	cidHex := hex.EncodeToString(n.clusterID[:])
	nodeID := n.OriginHex()

	pub, err := publisher.New(publisher.Config{
		Backend:         n.objectBackend,
		ClusterID:       cidHex,
		NodeID:          nodeID,
		WALPath:         walPath,
		MetaWALPath:     metaWALPath,
		Baseline:        n.publisherBaseline,
		MetaBaseline:    n.publisherMetaBaseline,
		AppCheckpoint:   n.appCheckpoint,
		MetaCheckpoint:  n.metaCheckpoint,
		LTXSyncInterval: n.ltxSyncInterval,
		ClaimSettle:     n.leaseClaimSettle,
		// Only single-node deployments risk the empty-DB clobber: a peer
		// transport means the broker catches a fresh node up before it
		// could take over the lease, so its rebaseline reflects real data.
		LocalFreshAtOpen: n.freshAtOpen && n.transport == nil,
		Logger:           n.log,
	})
	if err != nil {
		return err
	}
	n.publisher = pub
	n.snap.SetCoupling(pub.SyncAppStream, pub.LastBucketTXID)
	ctx, cancel := context.WithCancel(context.Background())
	n.publisherCancel = cancel
	n.publisherDone = make(chan struct{})
	go func() {
		defer close(n.publisherDone)
		if err := pub.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			n.log.Warn("publisher exited", "err", err)
		}
	}()
	return nil
}

// publisherBaseline is the Node-side BaselineFunc: takes a writer-barrier
// capture, encodes both app.db and metadata.db as snapshot LTXes
// stamped with MaxTXID=txid (allocated by the publisher before this
// call). Used on initial claim, lease takeover, and rebaseline.
func (n *Node) publisherBaseline(ctx context.Context, txid uint64) (publisher.EncodedBaseline, publisher.EncodedBaseline, func(), error) {
	snap, app, meta, err := n.encodeBaselines(ctx, txid, true)
	if err != nil {
		return publisher.EncodedBaseline{}, publisher.EncodedBaseline{}, func() {}, err
	}
	return app, meta, snap.Close, nil
}

// publisherMetaBaseline is the Node-side MetaBaselineFunc: encodes
// metadata.db alone at txid. Used on an out-of-band meta WAL recycle.
func (n *Node) publisherMetaBaseline(ctx context.Context, txid uint64) (publisher.EncodedBaseline, func(), error) {
	snap, _, meta, err := n.encodeBaselines(ctx, txid, false)
	if err != nil {
		return publisher.EncodedBaseline{}, func() {}, err
	}
	return meta, snap.Close, nil
}

// appCheckpoint runs PRAGMA wal_checkpoint(<mode>) on appWrite under writeMu.
// underFence runs after writeMu is acquired and receives the checkpoint
// hooks (Recycle is sqlitebridge.RecycleCommit on appWrite), allowing the
// publisher to acquire the app tailer only after this serialization and
// keep its last drain, checkpoint, and position reset atomic.
func (n *Node) appCheckpoint(_ context.Context, mode string, underFence func(hooks ltxstream.CheckpointHooks) error) error {
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if n.writerDB == nil {
		return ErrClosed
	}
	checkpoint := func() (ltxstream.CheckpointResult, error) {
		var busy, nLog, nCkpt int64
		if err := n.writerDB.QueryRow(fmt.Sprintf(`PRAGMA wal_checkpoint(%s)`, mode)).Scan(&busy, &nLog, &nCkpt); err != nil {
			return ltxstream.CheckpointResult{}, err
		}
		// The backfill this checkpoint just ran is the only writer of
		// page 1 in WAL mode; a zeroed on-disk header here means WAL
		// state is corrupt. Fail the fence rather than recycle over it.
		if err := verifyDBHeader(n.appPath); err != nil {
			return ltxstream.CheckpointResult{}, err
		}
		return ltxstream.CheckpointResult{Busy: busy != 0, NLog: nLog, NCkpt: nCkpt}, nil
	}
	if underFence == nil {
		_, err := checkpoint()
		return err
	}
	return underFence(ltxstream.CheckpointHooks{
		Checkpoint: checkpoint,
		// The bracket runs directly on appWrite (the single conn behind
		// writerDB); writeMu serializes all writers around it.
		Recycle: n.appWrite.RecycleCommit,
	})
}

// metaCheckpoint composes Store.Checkpoint: the metadata connection's
// wal_checkpoint runs under Store.mu, before the under-fence hook acquires the
// metadata tailer. The same page-1 header gate as appCheckpoint rides on the
// checkpoint hook.
func (n *Node) metaCheckpoint(_ context.Context, mode string, underFence func(hooks ltxstream.CheckpointHooks) error) error {
	metaPath := layout.MetaDB(n.appPath)
	if underFence == nil {
		if err := n.meta.Checkpoint(mode, nil); err != nil {
			return err
		}
		return verifyDBHeader(metaPath)
	}
	return n.meta.Checkpoint(mode, func(hooks ltxstream.CheckpointHooks) error {
		inner := hooks.Checkpoint
		hooks.Checkpoint = func() (ltxstream.CheckpointResult, error) {
			res, err := inner()
			if err != nil {
				return res, err
			}
			if err := verifyDBHeader(metaPath); err != nil {
				return ltxstream.CheckpointResult{}, err
			}
			return res, nil
		}
		return underFence(hooks)
	})
}

// encodeBaselines pins the node and encodes metadata.db (always) plus
// app.db (when includeApp), each with its seeded checksum state.
// Caller owns snap.Close on success; on error snap is already closed.
func (n *Node) encodeBaselines(ctx context.Context, txid uint64, includeApp bool) (*pinnedSnapshot, publisher.EncodedBaseline, publisher.EncodedBaseline, error) {
	var app, meta publisher.EncodedBaseline
	snap, err := n.snapshotPinned(ctx)
	if err != nil {
		return nil, app, meta, err
	}
	metaPath, appPath, err := snap.bundle.Files()
	if err != nil {
		snap.Close()
		return nil, app, meta, fmt.Errorf("read staged backups: %w", err)
	}
	// Never encode a baseline whose page 1 is not a SQLite header: a
	// corrupted staged copy must fail here, not propagate to the bucket.
	if err := verifyDBHeader(metaPath); err != nil {
		snap.Close()
		return nil, app, meta, fmt.Errorf("staged meta baseline: %w", err)
	}
	if includeApp {
		if err := verifyDBHeader(appPath); err != nil {
			snap.Close()
			return nil, app, meta, fmt.Errorf("staged app baseline: %w", err)
		}
	}
	var metaBuf bytes.Buffer
	if _, meta.Checksums, err = ltxstream.EncodeBaseline(ctx, &metaBuf, metaPath, txid); err != nil {
		snap.Close()
		return nil, app, meta, fmt.Errorf("encode meta baseline: %w", err)
	}
	meta.LTX = metaBuf.Bytes()
	if includeApp {
		var appBuf bytes.Buffer
		if _, app.Checksums, err = ltxstream.EncodeBaseline(ctx, &appBuf, appPath, txid); err != nil {
			snap.Close()
			return nil, app, meta, fmt.Errorf("encode app baseline: %w", err)
		}
		app.LTX = appBuf.Bytes()
	}
	return snap, app, meta, nil
}

// PublisherStats returns a snapshot of the publisher's local state.
// The bool is true only when this node currently runs the publisher
// controller (i.e. holds the lease); when false, callers should
// look at the bucket HEAD to find the active holder.
func (n *Node) PublisherStats() (PublisherStats, bool) {
	if n.publisher == nil {
		return PublisherStats{}, false
	}
	return publicPublisherStats(n.publisher.Stats()), true
}

// HoldsPublisherLease reports whether this node currently holds its topic's
// publisher lease (and so owns the live app tailer). False on a standby.
func (n *Node) HoldsPublisherLease() bool {
	return n.publisher != nil && n.publisher.Leading()
}

// errStandbyStandDown aborts a standby checkpoint from its under-fence hook when
// the node has just become the publisher; not a real error.
var errStandbyStandDown = errors.New("syzy: standby checkpoint stood down (now leading)")

// runStandbyWALCheckpoint periodically TRUNCATE-checkpoints app.db and
// metadata.db while this node is NOT the publisher. The publisher's own
// coordinated checkpoint loop recycles both WALs while it leads; a standby
// has no such loop, so its physical WALs otherwise climb to their busiest
// burst's high-water and never shrink until a restart. The checkpoint is
// data-safe: wal_checkpoint(TRUNCATE) writes WAL frames into the DB before
// truncating, and a standby has no LTX tailer whose unread frames it could
// strand. The under-fence hook re-checks leadership under the connection's
// serialization so a checkpoint can't slip in right after this node wins the
// lease and starts its tailers.
func (n *Node) runStandbyWALCheckpoint(ctx context.Context, interval time.Duration) {
	standDownOrCheckpoint := func(hooks ltxstream.CheckpointHooks) error {
		if n.HoldsPublisherLease() {
			return errStandbyStandDown
		}
		// No tailer to coordinate with on a standby: run the bare
		// checkpoint.
		res, err := hooks.Checkpoint()
		if err == nil && res.Busy {
			return ltxstream.ErrCheckpointBusy
		}
		return err
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if n.HoldsPublisherLease() {
				continue // publisher's coordinated checkpoint owns the WALs
			}
			for label, fence := range map[string]publisher.CheckpointFunc{
				"app": n.appCheckpoint, "meta": n.metaCheckpoint,
			} {
				err := fence(ctx, "TRUNCATE", standDownOrCheckpoint)
				if err != nil && !errors.Is(err, errStandbyStandDown) {
					// BUSY (an apply held the writer) or transient: retry
					// next tick. Debug, not warn — it self-corrects.
					n.log.Debug("standby WAL checkpoint", "stream", label, "err", err)
				}
			}
		}
	}
}

// stopPublisher tears down the publisher loop. Idempotent.
func (n *Node) stopPublisher() {
	if n.publisherCancel == nil {
		return
	}
	n.publisherCancel()
	if n.publisherDone != nil {
		<-n.publisherDone
	}
	n.publisherCancel = nil
	n.publisherDone = nil
}
