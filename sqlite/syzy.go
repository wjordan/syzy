package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/broker"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/producer"
	"github.com/wjordan/syzy/internal/publisher"
	"github.com/wjordan/syzy/internal/sealer"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/internal/syncer"
	"github.com/wjordan/syzy/notify"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/transport"
	"github.com/wjordan/syzy/unique"
	"github.com/wjordan/syzy/wake"
)

// drainTimeout caps how long Open will block waiting for the producer's
// drainer to flush historical records on a re-open. Exceeded only on a
// pathologically large self-journal; fail loud rather than silently
// re-broadcasting old records.
const drainTimeout = 30 * time.Second

// defaultHandoffDrainTimeout caps the drain on a Detach (releaseClaims=false).
// Shorter than drainTimeout because the handoff drain is best-effort, not
// load-bearing: the successor adopts the same origin + on-disk journal and
// re-drains from the last persisted offset, so a stalled drain must yield to
// the successor quickly rather than hold the predecessor in its half-quiesced
// window for the full 30s. Per-node override (Node.handoffDrainTimeout) exists
// for teardown-path tests.
const defaultHandoffDrainTimeout = 5 * time.Second

// defaultReadPoolSize bounds concurrent readers. Readers pin WAL frames
// while they run, so the pool stays finite to keep checkpointing viable.
const defaultReadPoolSize = 16

// DefaultSnapshotRetention is the journal-GC age floor used when
// Config.SnapshotRetention is zero and ObjectBackend is set: segments
// stay on disk this long past being snapshotted, bounding disk while
// giving a returning-from-offline peer this much grace to gap-fill
// incrementally before it must rebaseline.
const DefaultSnapshotRetention = 72 * time.Hour

// standbyCheckpointInterval is how often a non-publisher node
// TRUNCATE-checkpoints its app.db WAL to keep the physical file from drifting
// to a burst high-water (the standby never runs the publisher's coordinated
// checkpoint loop). Matches the publisher's default checkpoint cadence.
const standbyCheckpointInterval = time.Minute

// defaultSchemaCatchupInterval is how often the broker polls the
// schema log for events past meta.schema_seq when DDL replication is
// enabled. Tunable via Config.SchemaCatchupInterval.
const defaultSchemaCatchupInterval = 500 * time.Millisecond

// gcJournals is the snapshotter's JournalProvider: it resolves the self
// origin to the producer's journal and every other origin to its mirror
// journal (non-creating — an origin we hold no mirror for yields nil,
// which the snapshotter skips).
type gcJournals struct {
	self   crdt.Origin
	selfJ  *journal.Journal
	mirror *mirror.Manager
}

func (g gcJournals) JournalFor(o crdt.Origin) (*journal.Journal, error) {
	if o == g.self {
		return g.selfJ, nil
	}
	if j, ok := g.mirror.LookupJournal(o); ok {
		return j, nil
	}
	return nil, nil
}

// Node owns one process's view of a syzy database — the writer
// connection (with hooks installed), the cluster metadata, the
// per-origin journal, and the syncer goroutines. Constructed by
// Open; release with Close.
type Node struct {
	appPath     string
	appWrite    *sqlitebridge.Conn
	appApply    *sqlitebridge.Conn
	appHelper   *sqlitebridge.Conn // non-nil only when DDL replication is enabled
	appBlobRead *sqlitebridge.Conn // read-only conn the producer's drainer uses to materialize blob_patch
	writerDB    *sql.DB            // application-facing pool over appWrite; owned by Node
	readerDB    *sql.DB            // read-only pool over appPath serving DB reads; nil when disabled
	writeMu     sync.Mutex
	meta        *metadata.Store
	catalog     *catalog.Catalog
	cache       *nodestate.Cache
	producer    *producer.Producer
	snap        *nodestate.Snapshotter
	log         *slog.Logger

	// handoffDrainTimeout bounds the best-effort drain on Detach. Defaults to
	// defaultHandoffDrainTimeout; teardown-path tests shrink it to keep a
	// deliberately-stalled handoff drain sub-second.
	handoffDrainTimeout time.Duration

	clusterID crdt.ClusterID
	schemaLog schemalog.Log

	// disableMmap mirrors Config.DisableMmap for connections opened
	// after Open (secondary-origin blob-read conns).
	disableMmap bool

	// freshAtOpen records whether the local schema_seq was 0 when this
	// node opened (an empty DB, before any schema catch-up). The
	// publisher consults it to refuse clobbering an existing bucket
	// baseline with an empty local snapshot on a foreign lease takeover.
	freshAtOpen bool

	// Multi-node fields, populated only when cfg.Transport is set.
	transport transport.Transport
	mirror    *mirror.Manager
	broker    *broker.Broker
	// peerFrontier aggregates connected peers' applied-frontiers (nil when the
	// transport doesn't support it). Drives proactive new-origin discovery and
	// the reaper's all-peers-applied GC-safety predicate.
	peerFrontier transport.PeerFrontier

	// lastApplyNanos is the wall-clock time (UnixNano) of the most recent
	// inbound apply, stamped by signalApplied. WaitApplyQuiescent reads it
	// to detect when an initial catchup burst has drained. Zero until the
	// first apply.
	lastApplyNanos atomic.Int64

	// Secondary drainers for other writer-process origins on this
	// box (typically loadable-extension producers). Populated when
	// cfg.Transport is set; the daemon owns the broadcast pipeline
	// for every local origin's journal.
	secMu       sync.Mutex
	secondaries map[crdt.Origin]*syncer.SecondaryDrainer
	secScanDone chan struct{}

	// wakeListener, when non-nil, supplies per-origin Waiters for
	// cross-kernel secondary producers. secondaryScan registers a
	// Waiter per discovered origin; node.Close unregisters them.
	wakeListener wake.Listener

	// notifier publishes per-record changes to the shared-memory feed
	// at notify.FeedPath. Always non-nil (single-node mode publishes
	// for in-process Subscribe consumers too).
	notifier *notify.Writer

	// In-process Subscribe state. notifyReader consumes the same feed
	// the notifier writes to; the dispatch goroutine fans out to
	// per-subscriber channels. subsMu guards subs / subsNextID.
	notifyReader       *notify.Reader
	notifyDispatchCanc context.CancelFunc
	notifyDispatchDone chan struct{}
	subsMu             sync.Mutex
	subs               map[uint64]*subscription
	subsNextID         uint64

	originClaim *layout.OriginClaim
	daemonClaim *layout.DaemonClaim

	syncCancel context.CancelFunc
	snapDone   chan struct{}

	// objectBackend is cfg.ObjectBackend, kept for PublishSnapshot to
	// reach the same backend used by the sealer + cluster_id rendezvous.
	// nil when no ObjectBackend was configured.
	objectBackend objectstore.Bucket

	// ltxSyncInterval mirrors cfg.LTXSyncInterval; passed to the
	// publisher when the lease is claimed.
	ltxSyncInterval  time.Duration
	leaseClaimSettle time.Duration

	// sealer, when non-nil, uploads per-origin Changeset epochs to
	// the configured object-storage backend. Lifecycle is owned by
	// Open/Close: it runs from before producer hooks are wired (so
	// it sees recovery replays via OnEncoded) until shutdown.
	sealer     *sealer.Sealer
	sealerDone chan struct{}

	// standbyCkptDone tracks the standby WAL-checkpoint loop (runs only when
	// ObjectBackend is set); closed when the loop exits on syncCancel.
	standbyCkptDone chan struct{}

	// leaseholder runs the coordinated-uniqueness reservation server while
	// this node holds the lease. Non-nil only when ObjectBackend is set;
	// its maintenance loop contends for the lease and rebuilds the
	// reservation index from the replica. See unique.Leaseholder.
	leaseholder     *unique.Leaseholder
	leaseholderDone chan struct{}

	// uniqueReg is the coordinated-uniqueness reservation backend this
	// node's producer claims against (nil when coordinated keys are
	// rejected — see the Config.LoopbackUnique discussion). Exposed via
	// UniqueRegistry so an embedder can front it for secondary producers
	// that cannot resolve a backend themselves (unique.ServeProxy).
	uniqueReg unique.Registry

	// uniqueRead is the leaseholder's aux read connection over the app DB,
	// used by Enumerate to rebuild the reservation index. Closed in Close
	// after the maintenance loop is joined; leaking it keeps the DB file
	// (and any FUSE mount backing it) busy. Non-nil only with ObjectBackend.
	uniqueRead *sqlitebridge.Conn

	// publisher is the elected physical-stream publisher loop. Non-nil
	// when ObjectBackend is set; the lease in HEAD ensures exactly one
	// node per cluster runs the L0 tailer + metadata uploader at a
	// time, so it's safe to start unconditionally on every backed
	// node. See internal/publisher.
	publisher       *publisher.Publisher
	publisherCancel context.CancelFunc
	publisherDone   chan struct{}

	closeMu sync.Mutex
	closed  bool

	// originAddr backs AddrFor; see SetOriginAddrs.
	originAddrMu sync.Mutex
	originAddr   map[crdt.Origin]string
}

// UniqueRegistry returns the coordinated-uniqueness reservation backend
// this node claims against, or nil when this node rejects coordinated
// (NOT NULL UNIQUE) keys. Embedders front it for secondary producers that
// have a stream to this node but no backend access of their own — serve it
// with unique.ServeProxy and point the producer's SYZY_UNIQUE_DIAL at the
// listener. A proxied claim is arbitrated exactly like one of this node's
// own, so serving nil-vs-non-nil also tells the secondary whether
// coordinated keys are supported at all.
func (n *Node) UniqueRegistry() unique.Registry { return n.uniqueReg }

// Exec runs SQL with no result. Suitable for DDL and non-parameterized
// DML against the replicated writer connection.
func (n *Node) Exec(sql string) error {
	if n.writerDB == nil {
		return ErrClosed
	}
	if n.isClosed() {
		return ErrClosed
	}
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if n.isClosed() {
		return ErrClosed
	}
	_, err := n.writerDB.Exec(sql)
	return err
}

// WithWriteLock runs fn with the node's writer ordering lock held,
// passing the replicated writer pool. Any caller that EXECUTES WRITES
// through WriterDB must go through here (or Exec): checkpoint
// recycling and snapshot pinning rely on the lock to keep their
// barrier protocols atomic against application writes — a write that
// bypasses it can land between a publisher's tailer drain and its
// checkpoint, desyncing the pinned frontier from the captured pages.
// fn must not retain the *sql.DB past its return.
func (n *Node) WithWriteLock(fn func(w *sql.DB) error) error {
	if n.writerDB == nil {
		return ErrClosed
	}
	if n.isClosed() {
		return ErrClosed
	}
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	if n.isClosed() {
		return ErrClosed
	}
	return fn(n.writerDB)
}

// ClusterID returns the 16-byte cluster identifier this node is bound
// to. Operators copy this value to fresh databases (via JoinCluster)
// to bring new nodes into the same logical cluster.
func (n *Node) ClusterID() [16]byte {
	return [16]byte(n.clusterID)
}

// Origin returns this node's per-process origin id as an opaque
// uint64. Stable across the lifetime of the underlying flock claim
// (which spans process restarts for the same on-disk origin slot).
func (n *Node) Origin() uint64 {
	return uint64(n.originClaim.Origin)
}

// OriginHex returns Origin formatted as the canonical 16-char
// big-endian hex string used for on-disk origin directory names and
// in operational logs.
func (n *Node) OriginHex() string {
	return layout.OriginHex(n.originClaim.Origin)
}

// AppConn returns the writer connection with producer hooks installed.
// Used by companion integrations that need the producer surface directly.
func (n *Node) AppConn() *sqlitebridge.Conn { return n.appWrite }

// AppPath returns the on-disk path of the application database.
func (n *Node) AppPath() string { return n.appPath }

// WriterDB returns the database/sql pool pinned to AppConn. It is the
// same pool *sqlite.DB exposes; share it rather than opening a second
// OpenDB on AppConn (the PinnedConn connector is one-shot).
func (n *Node) WriterDB() *sql.DB { return n.writerDB }

// ReaderDB returns the read-only pool serving *sqlite.DB reads, or nil
// when Config.ReadPoolSize disabled it. Hook-free and write-incapable.
func (n *Node) ReaderDB() *sql.DB { return n.readerDB }

// pinnedSnapshot is the writer-barrier-consistent capture shared by
// ServeBundle (live tcp clone) and PublishSnapshot (live S3 publish).
// Holds staged metadata.db + app.db copies plus the frontier/schema_seq
// metadata captured at the same logical commit boundary. Caller must invoke
// Close exactly once.

// JoinCluster seeds the metadata at appPath with clusterID, so a
// subsequent Open treats that database as a member of the cluster.
// Refuses to overwrite a different existing cluster_id (returns
// ErrClusterMismatch). Idempotent when the existing id already matches.
//
// Use this when bootstrapping a new node into an existing cluster:
// take the cluster_id from a peer's (*Node).ClusterID() (or the
// `syzy status` CLI), then call JoinCluster on the new database
// before the first Open.
func JoinCluster(appPath string, clusterID [16]byte) error {
	if appPath == "" {
		return errors.New("syzy: JoinCluster: empty appPath")
	}
	if err := os.MkdirAll(layout.MetaDir(appPath), 0o755); err != nil {
		return fmt.Errorf("syzy: JoinCluster: ensure metadata dir: %w", err)
	}
	sc, err := metadata.Open(layout.MetaDB(appPath))
	if err != nil {
		return fmt.Errorf("syzy: JoinCluster: open metadata: %w", err)
	}
	defer sc.Close()
	cur, ok, err := sc.GetClusterID()
	if err != nil {
		return fmt.Errorf("syzy: JoinCluster: read cluster_id: %w", err)
	}
	if ok {
		if cur != crdt.ClusterID(clusterID) {
			return ErrClusterMismatch
		}
		return nil
	}
	if err := sc.SetClusterID(crdt.ClusterID(clusterID)); err != nil {
		return fmt.Errorf("syzy: JoinCluster: set cluster_id: %w", err)
	}
	return nil
}

// ErrClusterMismatch is returned by JoinCluster when the target
// database already has a different cluster_id than the one offered.
var ErrClusterMismatch = errors.New("syzy: cluster_id mismatch (database already joined to a different cluster)")

// Close shuts the node down: drains pending journal records, cancels
// syncer goroutines, closes the broker, takes the final snapshot,
// marks the metadata's clean_shutdown flag, then releases all SQLite
// handles and the on-disk locks. The clean_shutdown bit is the
// signal the next startup uses to skip origin rotation; setting it
// last keeps the next start treating us as unclean if any earlier
// step here errored. Idempotent.
