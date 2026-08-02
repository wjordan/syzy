package sqlite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/syncer"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/transport"
)

func openWriterBarrier(appPath string) (*sqlitebridge.Conn, error) {
	c, err := sqlitebridge.Open(appPath, 0)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if err := c.Exec(`PRAGMA busy_timeout = 30000; BEGIN IMMEDIATE`); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("begin immediate: %w", err)
	}
	return c, nil
}

// closeWriterBarrier ends the barrier transaction (COMMIT on the happy
// path, ROLLBACK on error) and closes the connection. Idempotent.
func closeWriterBarrier(c *sqlitebridge.Conn, rollback bool) error {
	if c == nil {
		return nil
	}
	stmt := "COMMIT"
	if rollback {
		stmt = "ROLLBACK"
	}
	err := c.Exec(stmt)
	if cerr := c.Close(); err == nil {
		err = cerr
	}
	return err
}

// waitAllDrained blocks until the producer's drainer and every attached
// secondary drainer report DrainedOffset >= journal head as of call
// time. Convergence requires no concurrent commits on the drained
// origins; callers that need that guarantee must serialize against
// writers separately (the writer-barrier path will own this).
func (n *Node) waitAllDrained(ctx context.Context) error {
	// Watchdog: a drain that stalls is otherwise invisible — callers
	// like the publisher's takeover baseline sit in this wait while
	// HOLDING writeMu, so name the stuck origin and its offsets
	// instead of hanging silently.
	wait := func(name string, fn func(context.Context) error, progress func() (uint64, uint64)) error {
		for {
			sub, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := fn(sub)
			// Classify BEFORE cancel: afterwards sub.Err() is always
			// Canceled, which misreads real errors as stalls and
			// hot-loops on them.
			stalled := errors.Is(sub.Err(), context.DeadlineExceeded)
			cancel()
			if err == nil || ctx.Err() != nil {
				return err
			}
			if stalled {
				drained, head := progress()
				n.log.Warn("syzy: drain stalled; still waiting",
					"which", name, "drained", drained, "head", head)
				continue
			}
			return err
		}
	}
	if err := wait("producer", n.producer.WaitForDrain, n.producer.DrainProgress); err != nil {
		return fmt.Errorf("producer drain: %w", err)
	}
	n.secMu.Lock()
	secs := make([]*syncer.SecondaryDrainer, 0, len(n.secondaries))
	for _, sd := range n.secondaries {
		secs = append(secs, sd)
	}
	n.secMu.Unlock()
	for _, sd := range secs {
		origin := layout.OriginHex(sd.Origin)
		if err := wait("origin "+origin, sd.WaitForDrain, func() (uint64, uint64) {
			return uint64(sd.Drainer.DrainedOffset()), uint64(sd.Journal.Head())
		}); err != nil {
			return fmt.Errorf("secondary drain (origin %s): %w", origin, err)
		}
	}
	return nil
}

// ErrClosed is returned by Node methods invoked after Close has begun.
var ErrClosed = errors.New("syzy: node closed")

func (n *Node) isClosed() bool {
	if n == nil {
		return true
	}
	n.closeMu.Lock()
	defer n.closeMu.Unlock()
	return n.closed
}
func (n *Node) Close() error {
	return n.closeWithOpts(true, true)
}

// closeWithOpts is the shared teardown. releaseClaims=false keeps the daemon +
// origin flocks held (for a handoff — see Detach), and markClean=false leaves
// clean_shutdown=false (a live successor still owns the origin). Close passes
// (true, true): the normal full shutdown.
func (n *Node) closeWithOpts(releaseClaims, markClean bool) error {
	n.closeMu.Lock()
	if n.closed {
		n.closeMu.Unlock()
		return nil
	}
	n.closed = true
	n.closeMu.Unlock()

	var errs []error

	// Handoff (releaseClaims=false, a Detach): retain the publisher lease so a
	// same-NodeID successor resumes it with no expiry window for a peer to
	// force-rebaseline through. Must run before publisherCancel fires the
	// release defer. nil on single-node / no-bucket nodes.
	if !releaseClaims && n.publisher != nil {
		n.publisher.RetainLeaseOnStop()
	}

	// Detach every peer-serving registration from the transport so any
	// in-flight accept path stops landing in this node's state after we
	// tear it down. Mirrors the registrations Open makes; the transport
	// is owned by the caller and may outlive the node.
	if r, ok := n.transport.(transport.CatchupRegistrar); ok {
		r.SetCatchupSource(nil)
	}
	if pn, ok := n.transport.(transport.PeerConnectNotifier); ok {
		pn.SetOnPeerConnect(nil)
	}
	if r, ok := n.transport.(transport.FrontierRegistrar); ok {
		r.SetFrontierSource(nil)
	}
	if bs, ok := n.transport.(transport.BundleSource); ok {
		bs.SetBundleHandler(nil)
	}

	// Cancel the publisher BEFORE draining writeMu. A publisher stuck
	// in a lease-takeover baseline (claimOrTakeover → takeCoupledBaselines
	// → snapshotPinned) HOLDS writeMu while it waits — without bound —
	// in waitAllDrained, and only its context cancellation unblocks
	// that wait. Acquiring writeMu first deadlocks Close against it
	// (observed in production: shutdown wedged until SIGKILL, then the
	// exit-time flush against the process's own FUSE mount left an
	// unkillable zombie). stopPublisher below still waits for the
	// goroutine to exit; cancelling twice is harmless.
	if n.publisherCancel != nil {
		n.publisherCancel()
	}

	// Drain the database-touching phase of in-flight ServeBundle /
	// PublishSnapshot. Both re-check n.closed under writeMu. A publisher
	// snapshot that already completed has independent staged files;
	// stopPublisher later joins that generation operation before DB teardown.
	n.writeMu.Lock()
	n.writeMu.Unlock()

	// Drain self + secondaries before cancelling the snapshotter so the
	// final snapshot reflects every record committed up to now. Bound
	// the wait so a wedged drainer can't stall the stop sequence.
	//
	// Fatality splits on the handoff: a real Close (releaseClaims=true) keeps
	// the drain LOAD-BEARING — it gates clean_shutdown + the final snapshot and
	// there is no successor to finish the work, so a stall is an error. A Detach
	// (releaseClaims=false) treats it as BEST-EFFORT: the successor adopts the
	// same origin + on-disk journal and re-drains from the last persisted offset
	// (see openWithAdopt: "the journal under this origin is the resume point"),
	// so handoff correctness derives from the flock transfer + journal
	// durability, never from this drain converging. Failing the Detach on a
	// transient drain stall would force the caller into a cold restart — a full
	// outage — to dodge work the successor does anyway. Drain what we can within
	// the (shorter) handoff budget to shrink the successor's catch-up, then go.
	budget := drainTimeout
	if !releaseClaims {
		budget = n.handoffDrainTimeout
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), budget)
	if err := n.waitAllDrained(drainCtx); err != nil {
		if releaseClaims {
			errs = append(errs, fmt.Errorf("close drain: %w", err))
		} else {
			n.log.Warn("syzy: handoff drain incomplete; successor resumes from the journal",
				"error", err)
		}
	}
	drainCancel()

	if n.syncCancel != nil {
		n.syncCancel()
	}
	// Broker.Close blocks on its own subscribe-loop wg. Do it first so
	// the apply path stops mutating mirror journals / appApply before
	// either is torn down.
	if n.broker != nil {
		if err := n.broker.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	// Sealer drains its in-memory queue and flushes any partial epoch
	// before exiting. syncCancel triggers the shutdown; Stop is called
	// for symmetry but the goroutine has already started its final
	// flush at this point.
	if n.sealer != nil {
		n.sealer.Stop()
	}
	if n.sealerDone != nil {
		<-n.sealerDone
	}
	// Standby checkpoint loop stops on syncCancel above; wait it out before
	// the writer DB is torn down so no checkpoint is mid-flight.
	if n.standbyCkptDone != nil {
		<-n.standbyCkptDone
	}
	// Leaseholder: syncCancel above already signalled its maintenance loop
	// to release the lease; wait for that, then close the RPC listener so a
	// successor can take over immediately.
	if n.leaseholderDone != nil {
		<-n.leaseholderDone
	}
	if n.leaseholder != nil {
		_ = n.leaseholder.Close()
	}
	// The leaseholder's maintenance loop (joined above) was uniqueRead's
	// only user, so close that aux connection now. Leaking it kept the app
	// DB — and any FUSE mount backing it — busy until process exit.
	if n.uniqueRead != nil {
		_ = n.uniqueRead.Close()
	}
	// Join publisher shutdown before tearing down its app/meta dependencies. It
	// may already have attempted its best-effort final WAL checkpoint; the next
	// claim's coupled baseline, not that checkpoint, is the correctness anchor.
	n.stopPublisher()
	// Snapshotter.Run takes one final SnapshotOnce on ctx.Done before
	// returning, so by the time snapDone closes the cache state has
	// been persisted. Wait for that before flipping clean_shutdown.
	if n.snapDone != nil {
		<-n.snapDone
	}
	if n.secScanDone != nil {
		<-n.secScanDone
	}
	// Tear down secondary drainers. syncCancel above already stopped
	// their Run goroutines; Close drains the done channels and
	// releases the journal handles. Unregister each origin from
	// the wake listener so the listener can drop any pending
	// connections for that origin.
	n.secMu.Lock()
	for o, sd := range n.secondaries {
		if err := sd.Close(); err != nil {
			errs = append(errs, err)
		}
		if n.wakeListener != nil {
			n.wakeListener.Unregister(layout.OriginHex(o))
		}
	}
	n.secondaries = nil
	n.secMu.Unlock()
	if n.writerDB != nil {
		if err := n.writerDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := n.producer.Close(); err != nil {
		errs = append(errs, err)
	}
	if n.mirror != nil {
		if err := n.mirror.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	// Stop the notify dispatcher next: every publisher has stopped.
	// Close the writer first to wake any futex-sleeping reader, then
	// wait for the dispatcher to close subscriber channels and exit.
	if n.notifyDispatchCanc != nil {
		n.notifyDispatchCanc()
	}
	if n.notifier != nil {
		if err := n.notifier.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := n.stopNotifyDispatcher(); err != nil {
		errs = append(errs, err)
	}

	// Mark clean shutdown only when the drain + snapshot path completed
	// cleanly. A partial shutdown leaves clean_shutdown=false so the
	// next start rotates the origin — the conservative default. A handoff
	// (markClean=false) also leaves it false: the origin is still live in
	// the successor, so any crash before the successor's own clean Close
	// must rotate.
	if markClean && len(errs) == 0 {
		if err := n.meta.SetCleanShutdown(true); err != nil {
			errs = append(errs, fmt.Errorf("set clean_shutdown=true: %w", err))
		}
	}

	if err := n.appWrite.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := n.appApply.Close(); err != nil {
		errs = append(errs, err)
	}
	if n.appHelper != nil {
		if err := n.appHelper.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if n.appBlobRead != nil {
		if err := n.appBlobRead.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := n.meta.Close(); err != nil {
		errs = append(errs, err)
	}
	// On a handoff (releaseClaims=false) keep the flocks held: the Handoff
	// owns the FDs and passes them to the successor, so the lock is never
	// released and no window opens. Detach extracts them after this returns.
	if releaseClaims {
		if err := n.daemonClaim.Release(); err != nil {
			errs = append(errs, err)
		}
		if err := n.originClaim.Release(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
