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

// publisherBaseline is the Node-side BaselineFunc: takes a writer-
// barrier-pin, encodes both app.db and metadata.db as snapshot LTXes
// stamped with MaxTXID=txid (allocated by the publisher before this
// call). Used on initial claim, lease takeover, and rebaseline.
func (n *Node) publisherBaseline(ctx context.Context, txid uint64) ([]byte, []byte, func(), error) {
	snap, appLTX, metaLTX, err := n.encodeBaselines(ctx, txid, true)
	if err != nil {
		return nil, nil, func() {}, err
	}
	return appLTX, metaLTX, snap.Close, nil
}

// publisherMetaBaseline is the Node-side MetaBaselineFunc: encodes
// metadata.db alone at txid. Used on an out-of-band meta WAL recycle.
func (n *Node) publisherMetaBaseline(ctx context.Context, txid uint64) ([]byte, func(), error) {
	snap, _, metaLTX, err := n.encodeBaselines(ctx, txid, false)
	if err != nil {
		return nil, func() {}, err
	}
	return metaLTX, snap.Close, nil
}

// appCheckpoint runs PRAGMA wal_checkpoint(<mode>) on appWrite under writeMu.
// underFence runs after writeMu is acquired and receives the checkpoint
// operation, allowing the publisher to acquire the app tailer only after the
// writer fence and keep its last drain, checkpoint, and position reset atomic.
func (n *Node) appCheckpoint(_ context.Context, mode string, underFence func(checkpoint func() error) error) error {
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if n.writerDB == nil {
		return ErrClosed
	}
	checkpoint := func() error {
		_, err := n.writerDB.Exec(fmt.Sprintf(`PRAGMA wal_checkpoint(%s)`, mode))
		return err
	}
	if underFence != nil {
		return underFence(checkpoint)
	}
	return checkpoint()
}

// metaCheckpoint composes Store.Checkpoint: the metadata connection's
// wal_checkpoint runs under Store.mu, before the under-fence hook acquires the
// metadata tailer.
func (n *Node) metaCheckpoint(_ context.Context, mode string, underFence func(checkpoint func() error) error) error {
	return n.meta.Checkpoint(mode, underFence)
}

// encodeBaselines pins the node and encodes metadata.db (always) plus
// app.db (when includeApp). Caller owns snap.Close on success; on
// error snap is already closed.
func (n *Node) encodeBaselines(ctx context.Context, txid uint64, includeApp bool) (*pinnedSnapshot, []byte, []byte, error) {
	snap, err := n.snapshotPinned(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	metaPath, appPath, err := snap.bundle.Files()
	if err != nil {
		snap.Close()
		return nil, nil, nil, fmt.Errorf("drain pinned backups: %w", err)
	}
	var metaBuf bytes.Buffer
	if _, err := ltxstream.EncodeBaseline(ctx, &metaBuf, metaPath, txid); err != nil {
		snap.Close()
		return nil, nil, nil, fmt.Errorf("encode meta baseline: %w", err)
	}
	var appLTX []byte
	if includeApp {
		var appBuf bytes.Buffer
		if _, err := ltxstream.EncodeBaseline(ctx, &appBuf, appPath, txid); err != nil {
			snap.Close()
			return nil, nil, nil, fmt.Errorf("encode app baseline: %w", err)
		}
		appLTX = appBuf.Bytes()
	}
	return snap, appLTX, metaBuf.Bytes(), nil
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

// runStandbyWALCheckpoint periodically TRUNCATE-checkpoints app.db while this
// node is NOT the publisher. The publisher's own coordinated checkpoint loop
// recycles the WAL while it leads; a standby has no such loop, so its physical
// app.db-wal otherwise climbs to its busiest burst's high-water and never
// shrinks until a restart. The checkpoint is data-safe: wal_checkpoint(TRUNCATE)
// writes WAL frames into the DB before truncating, and a standby has no LTX
// tailer whose unread frames it could strand. The under-fence hook re-checks
// leadership under writeMu so a checkpoint can't slip in right after this node
// wins the lease and starts its tailer.
func (n *Node) runStandbyWALCheckpoint(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if n.HoldsPublisherLease() {
				continue // publisher's coordinated checkpoint owns the WAL
			}
			err := n.appCheckpoint(ctx, "TRUNCATE", func(checkpoint func() error) error {
				if n.HoldsPublisherLease() {
					return errStandbyStandDown
				}
				return checkpoint()
			})
			if err != nil && !errors.Is(err, errStandbyStandDown) {
				// BUSY (an apply held the writer) or transient: retry next
				// tick. Debug, not warn — it self-corrects.
				n.log.Debug("standby WAL checkpoint", "err", err)
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
