package sqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/broker"
	"github.com/wjordan/syzy/internal/gapfillerchain"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/producer"
	"github.com/wjordan/syzy/internal/s3fetch"
	"github.com/wjordan/syzy/internal/sealer"
	catalog "github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/internal/syncer"
	"github.com/wjordan/syzy/notify"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/transport"
	"github.com/wjordan/syzy/unique"
)

// Open initializes a syzy node at cfg.Path. Auto-mints a random
// cluster id on first open; subsequent opens reuse the persisted one.
//
// Single-node mode (cfg.Transport == nil): producer + snapshotter
// only — local writes are durable but not disseminated. Multi-node
// mode (cfg.Transport != nil): adds the apply pipeline (mirror
// journals, broker) and wires the producer's OnEncoded callback to
// Transport.Broadcast. ObjectBackend is the cluster's durable backstop
// when set; without it, the node runs producer-only over the transport
// — live gossip works, but historical Fetch and sealing are disabled.
// At least one peer in the cluster must hold an ObjectBackend for
// catch-up beyond gossip retention.
func Open(ctx context.Context, cfg Config) (*Node, error) {
	return openWithAdopt(ctx, cfg, nil)
}

// opener carries openWithAdopt's in-flight state across its phases.
// Fields are acquired in phase order; unwind releases them LIFO when
// a later phase fails, so a half-open node never leaks locks, conns,
// or goroutines.
type opener struct {
	cfg   Config
	adopt *Handoff
	log   *slog.Logger

	ok      bool
	unwinds []func()

	daemonClaim *layout.DaemonClaim
	originClaim *layout.OriginClaim
	sc          *metadata.Store

	appWrite    *sqlitebridge.Conn
	appApply    *sqlitebridge.Conn
	appHelper   *sqlitebridge.Conn
	appBlobRead *sqlitebridge.Conn

	clusterID   crdt.ClusterID
	cache       *nodestate.Cache
	cat         *catalog.Catalog
	freshAtOpen bool
	mgr         *mirror.Manager
	notifier    *notify.Writer

	uniqueReg   unique.Registry
	leaseholder *unique.Leaseholder
	uniqueRead  *sqlitebridge.Conn
	prod        *producer.Producer
	node        *Node

	s3Source *s3fetch.Source

	syncCtx    context.Context
	syncCancel context.CancelFunc
}

func (o *opener) push(fn func()) { o.unwinds = append(o.unwinds, fn) }

// unwind releases everything acquired so far, newest first. A no-op
// once ok is set: from then on the node's Close owns the resources.
func (o *opener) unwind() {
	if o.ok {
		return
	}
	for i := len(o.unwinds) - 1; i >= 0; i-- {
		o.unwinds[i]()
	}
}

// openWithAdopt is the shared open path. When adopt is nil it claims a fresh
// daemon role + origin (the normal Open). When adopt is non-nil it ADOPTS a
// predecessor's handed-off lock (see Attach): no re-flock, no origin rotation,
// resuming the predecessor's exact origin from its on-disk journal.
func openWithAdopt(ctx context.Context, cfg Config, adopt *Handoff) (*Node, error) {
	if cfg.Path == "" {
		return nil, errors.New("syzy: Config.Path required")
	}
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if err := os.MkdirAll(layout.MetaDir(cfg.Path), 0o755); err != nil {
		return nil, fmt.Errorf("syzy: ensure metadata dir: %w", err)
	}

	o := &opener{cfg: cfg, adopt: adopt, log: log}
	defer o.unwind()

	// Phase order is load-bearing (locks before state, state before
	// pipelines, pipelines before goroutines); each phase documents its
	// own internal ordering constraints.
	if err := o.claimLocks(); err != nil {
		return nil, err
	}
	if err := o.openConns(); err != nil {
		return nil, err
	}
	if err := o.loadState(ctx); err != nil {
		return nil, err
	}
	if err := o.initUnique(); err != nil {
		return nil, err
	}
	if err := o.startProducer(ctx); err != nil {
		return nil, err
	}
	o.assembleNode()
	if err := o.buildBroker(); err != nil {
		return nil, err
	}
	o.wireSealer()
	o.startBackground()
	o.healCatalog(ctx)
	if err := o.startServices(ctx); err != nil {
		return nil, err
	}

	log.Info("syzy: opened",
		"path", cfg.Path,
		"origin", layout.OriginHex(o.originClaim.Origin),
		"multi_node", cfg.Transport != nil,
		"publisher", cfg.ObjectBackend != nil,
	)

	// A successful Attach transfers FD ownership from the handoff to the
	// node's claims (released by Close). Nil the handoff's copies so a stray
	// Commit on the consumed handoff can't double-close them.
	if adopt != nil {
		adopt.daemonFile = nil
		adopt.originFile = nil
	}

	o.ok = true
	return o.node, nil
}

// claimLocks takes the daemon role, opens metadata, claims an origin,
// and marks the lifecycle unclean.
//
// Daemon claim first: it gates exclusive access to metadata.db, which
// owns the clean_shutdown bit that drives origin-rotation policy.
// Reading that bit before the origin claim lets us decide whether to
// recycle the prior origin or mint a fresh one.
func (o *opener) claimLocks() error {
	var err error
	if o.adopt != nil {
		// On an adopted handoff we already hold the daemon lock via the
		// predecessor's passed FD (shared open file description) — wrap it
		// without a fresh flock.
		o.daemonClaim = layout.AdoptDaemon(o.adopt.daemonFile)
	} else {
		o.daemonClaim, err = layout.ClaimDaemon(o.cfg.Path)
		if err != nil {
			if errors.Is(err, layout.ErrDaemonLocked) {
				return errors.New("syzy: daemon role already held by another process (multi-process not yet supported)")
			}
			return fmt.Errorf("syzy: claim daemon role: %w", err)
		}
	}
	// On adopt the caller (Handoff) owns the lock FDs and may Commit or
	// Attach-to-Resume them, so an Open-error unwind must not close them.
	if o.adopt == nil {
		o.push(func() { _ = o.daemonClaim.Release() })
	}

	o.sc, err = metadata.Open(layout.MetaDB(o.cfg.Path))
	if err != nil {
		return fmt.Errorf("syzy: open metadata: %w", err)
	}
	o.push(func() { _ = o.sc.Close() })
	if health, unhealthy, err := o.sc.GetSchemaHealth(); err != nil {
		return fmt.Errorf("syzy: read schema health: %w", err)
	} else if unhealthy {
		return fmt.Errorf("%w: seq=%d: %s", ErrSchemaUnhealthy, health.Seq, health.Reason)
	}

	if o.adopt != nil {
		// Adopt the predecessor's exact origin via its passed directory FD:
		// no rotation, no re-flock. The journal under this origin is the
		// resume point — startProducer's WaitForDrain recovers it.
		o.originClaim = layout.AdoptOrigin(o.cfg.Path, o.adopt.origin, o.adopt.originFile)
	} else {
		// Origin-rotation policy. clean_shutdown=false (explicit) means the
		// previous lifecycle crashed; the prior origin's seq counter may
		// have leaked seqs that peers have but our journal doesn't, so we
		// must not reuse it for new local writes. Mint a fresh origin and
		// leave the prior origin's directory in place — the secondary-
		// drainer scan picks it up and re-broadcasts any trailing
		// records (idempotent on peers via frontier dedup).
		wasClean, hadCleanFlag, err := o.sc.GetCleanShutdown()
		if err != nil {
			return fmt.Errorf("syzy: read clean_shutdown: %w", err)
		}
		uncleanRestart := hadCleanFlag && !wasClean
		switch {
		case uncleanRestart:
			// Rotate to a fresh origin even when pinned: never reuse a
			// possibly leaked-seq origin after a crash. A fresh mint is still
			// host-owned (never an in-guest writer's), and guests re-read the
			// updated node_id to exclude it, so the collision guard holds.
			o.originClaim, err = layout.MintAndClaim(o.cfg.Path)
			if err != nil {
				return fmt.Errorf("syzy: rotate origin: %w", err)
			}
			o.log.Warn("syzy: unclean prior shutdown — rotating to fresh origin",
				"origin", layout.OriginHex(o.originClaim.Origin),
			)
		case o.cfg.NodeID != 0:
			// Pinned host: claim our stable reserved origin instead of
			// recycling the first unlocked dir, which could be an origin a
			// guest writer is actively producing into (its flock is invisible
			// across the pmem/virtiofs boundary). See Config.NodeID.
			pinned := crdt.Origin(o.cfg.NodeID &^ (uint64(1) << 63))
			if pinned == 0 {
				pinned = 1
			}
			o.originClaim, err = layout.Acquire(o.cfg.Path, pinned, 0)
			if err != nil {
				return fmt.Errorf("syzy: claim pinned origin: %w", err)
			}
		default:
			o.originClaim, err = layout.Acquire(o.cfg.Path, 0, 0)
			if err != nil {
				return fmt.Errorf("syzy: claim origin: %w", err)
			}
		}
	}
	if o.adopt == nil {
		o.push(func() { _ = o.originClaim.Release() })
	}

	// Mark unclean as soon as we hold the origin: any crash from here on
	// out leaves clean_shutdown=false so the next start rotates. Close
	// flips this back to true after a graceful drain + final snapshot. A
	// handoff keeps it false (a live successor still owns the origin).
	if err := o.sc.SetCleanShutdown(false); err != nil {
		return fmt.Errorf("syzy: set clean_shutdown=false: %w", err)
	}
	return nil
}

// openConns opens the app-database connections: the writer, the
// broker's apply conn, the DDL-admission helper, and the blob reader.
func (o *opener) openConns() error {
	var err error
	o.appWrite, err = sqlitebridge.Open(o.cfg.Path, 0)
	if err != nil {
		return fmt.Errorf("syzy: open writer: %w", err)
	}
	o.push(func() { _ = o.appWrite.Close() })
	if err := o.appWrite.Exec(`PRAGMA journal_mode = WAL; ` + connPragmas(o.cfg.DisableMmap)); err != nil {
		return fmt.Errorf("syzy: configure writer: %w", err)
	}
	// Shrink any WAL inherited from a prior run before opening for full
	// operation. Prior to the auto-checkpoint fix, crashes could leave a
	// multi-hundred-MB WAL on disk; without this, recovery starts under
	// memory pressure that can OOM-kill before the runtime checkpoint
	// hook gets a chance to keep up.
	if err := o.appWrite.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return fmt.Errorf("syzy: truncate inherited WAL: %w", err)
	}
	// When the publisher is wired in, it owns WAL recycling: SQLite's
	// auto-checkpoint must NOT race ahead of the LTX tailer's Sync
	// (that's what forces full rebaselines). Disable it here; the
	// publisher's checkpoint loop runs a coordinated TRUNCATE under
	// writer fence + tailer drain.
	if o.cfg.ObjectBackend != nil {
		if err := o.appWrite.Exec(`PRAGMA wal_autocheckpoint = 0`); err != nil {
			return fmt.Errorf("syzy: disable app autocheckpoint: %w", err)
		}
		if err := o.sc.DisableAutoCheckpoint(); err != nil {
			return fmt.Errorf("syzy: disable meta autocheckpoint: %w", err)
		}
	}

	o.appApply, err = openAuxConn(o.cfg.Path, "apply", o.cfg.DisableMmap, o.cfg.ObjectBackend != nil)
	if err != nil {
		return err
	}
	o.push(func() { _ = o.appApply.Close() })
	// CHECK constraints are origin-write-time admission predicates: the
	// originating writer already enforced them, and re-evaluating at
	// apply would let this replica quarantine a row its peers accepted.
	if err := o.appApply.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		return fmt.Errorf("syzy: configure apply conn: %w", err)
	}

	// appHelper is only needed for DDL admission's cascade-trigger
	// synthesis; skip the fd + cgo handle when DDL replication is off.
	if o.cfg.SchemaLog != nil {
		o.appHelper, err = openAuxConn(o.cfg.Path, "helper", o.cfg.DisableMmap, o.cfg.ObjectBackend != nil)
		if err != nil {
			return err
		}
		o.push(func() { _ = o.appHelper.Close() })
	}

	// appBlobRead lets the producer's drainer read post-commit NEW blob
	// bytes via sqlite3_blob_open so sqlite3_blob_write() captures
	// materialize as compact blob_patch records (BLOB_PATCH.md).
	o.appBlobRead, err = openAuxConn(o.cfg.Path, "blobread", o.cfg.DisableMmap, o.cfg.ObjectBackend != nil)
	if err != nil {
		return err
	}
	o.push(func() { _ = o.appBlobRead.Close() })
	return nil
}

// loadState establishes cluster identity and rebuilds in-memory state
// from disk: the clock cache, the catalog, the mirror journals, and
// the notify feed writer.
func (o *opener) loadState(ctx context.Context) error {
	var err error
	o.clusterID, err = ensureClusterID(ctx, o.sc, o.cfg.ObjectBackend)
	if err != nil {
		return err
	}
	if err := o.sc.SetNodeID(o.originClaim.Origin); err != nil {
		return fmt.Errorf("syzy: persist node_id: %w", err)
	}

	o.cache = nodestate.New(o.originClaim.Origin)
	if err := o.cache.LoadFromMeta(o.sc); err != nil {
		return fmt.Errorf("syzy: load cache: %w", err)
	}

	// Capture freshness before any schema catch-up runs: schema_seq==0
	// here means an empty local DB. The publisher uses this to refuse
	// rebaselining over a populated bucket on a foreign takeover.
	schemaSeqAtOpen, _, err := o.sc.GetSchemaSeq()
	if err != nil {
		return fmt.Errorf("syzy: read schema_seq: %w", err)
	}
	o.freshAtOpen = schemaSeqAtOpen == 0

	o.cat, err = catalog.LoadForRuntime(o.appWrite, o.sc)
	if err != nil {
		return fmt.Errorf("syzy: load catalog: %w", err)
	}

	// Mirror manager owns per-origin inbound journals. Built even in
	// single-node mode so a later upgrade to multi-node mode (process
	// restart with cfg.Transport set) finds an empty mirror dir
	// already in place — and so the snapshotter has a JournalProvider
	// to reach for if we ever enable per-origin GC here.
	o.mgr, err = mirror.New(mirror.Config{Root: filepath.Join(layout.MetaDir(o.cfg.Path), "mirror"), Log: o.log, Self: o.originClaim.Origin})
	if err != nil {
		return fmt.Errorf("syzy: open mirror: %w", err)
	}
	o.push(func() { _ = o.mgr.Close() })
	if err := o.mgr.LoadExisting(); err != nil {
		return fmt.Errorf("syzy: load mirror: %w", err)
	}
	if _, err := nodestate.RecoverMirror(o.cache, o.mgr, o.cat); err != nil {
		return fmt.Errorf("syzy: recover mirror: %w", err)
	}

	o.notifier, err = notify.NewWriter(notify.WriterConfig{
		Path: notify.FeedPath(o.cfg.Path),
	})
	if err != nil {
		return fmt.Errorf("syzy: open notify feed: %w", err)
	}
	o.push(func() { _ = o.notifier.Close() })
	return nil
}

// initUnique builds the coordinated-uniqueness reservation backend
// (for NOT NULL UNIQUE keys). With object storage, elect a lease-held
// leaseholder and route reservations to it over RPC; without a bucket
// (single-process), an in-process registry suffices. nil ⇒ NOT NULL
// UNIQUE rejected at DDL.
func (o *opener) initUnique() error {
	if o.cfg.ObjectBackend != nil {
		conn, err := openAuxConn(o.cfg.Path, "unique-read", o.cfg.DisableMmap, o.cfg.ObjectBackend != nil)
		if err != nil {
			return err
		}
		o.uniqueRead = conn
		o.push(func() { _ = o.uniqueRead.Close() })
		leaseStore := unique.OpenLease(o.cfg.ObjectBackend, "unique/lease")
		// Route reservation RPCs over the mesh when the transport carries
		// them (the built-in mesh): the leaseholder then publishes a peer-reachable
		// bundle URL into the lease and every follower's LeaseClient dials
		// it over the already-connected, firewall-open mesh. Without a
		// mesh-routing transport (single-node, or a transport that doesn't
		// carry unique RPCs) the leaseholder falls back to a loopback
		// listener, correct only when client and server share a process —
		// a clustered node MUST provide a TransportProvider or cross-node
		// reservation is impossible (the bug this guards against).
		var uniqueServe unique.ServeTransport
		var uniqueDial unique.DialTransport
		if tp, ok := o.cfg.Transport.(unique.TransportProvider); ok {
			uniqueServe = tp.UniqueServeTransport()
			uniqueDial = tp.UniqueDialTransport()
		} else if o.cfg.Transport != nil && !o.cfg.LoopbackUnique {
			// Fail closed instead of falling back to loopback: on a
			// multi-node cluster the loopback address published into
			// the lease is undialable from followers, breaking
			// cross-node reservation exactly when it matters.
			return fmt.Errorf("syzy: coordinated uniqueness requires a Transport that carries uniqueness RPCs (unique.TransportProvider); set Config.LoopbackUnique only if every writer shares this process")
		}
		quarantineUS := o.cfg.UniqueQuarantine.Microseconds()
		o.leaseholder = unique.NewLeaseholder(unique.LeaseholderConfig{
			Store: leaseStore,
			// Graceful-shutdown handoff: a clean leader publishes its taken-set
			// so a successor serves immediately, no failover drain. Sibling of
			// the lease object.
			Handoff:      unique.OpenHandoff(o.cfg.ObjectBackend, "unique/handoff"),
			Owner:        layout.OriginHex(o.originClaim.Origin),
			Transport:    uniqueServe, // nil ⇒ loopback (single-node / in-process)
			QuarantineUS: quarantineUS,
			Enumerate: func(context.Context) (unique.Snapshot, error) {
				return enumerateCoordinatedClaims(o.cat, o.uniqueRead)
			},
		})
		if err := o.leaseholder.Start(); err != nil {
			return fmt.Errorf("syzy: start unique leaseholder: %w", err)
		}
		o.push(func() { _ = o.leaseholder.Close() })
		if uniqueDial != nil {
			// Co-locate: when this node holds the lease, the client serves in
			// process rather than dialing the address it published for remote
			// peers (which need not be self-reachable under 1:1 NAT).
			o.uniqueReg = unique.NewLeaseClientTransport(leaseStore, uniqueDial).
				UseLocalLeaseholder(o.leaseholder)
		} else {
			o.uniqueReg = unique.NewLeaseClient(leaseStore).
				UseLocalLeaseholder(o.leaseholder)
		}
	} else if o.cfg.Transport == nil {
		// Single-node, no object storage: an in-process registry enforces
		// coordinated uniqueness. It is the ONLY enforcement — no node
		// keeps a physical UNIQUE index for a coordinated key — so it gets
		// the same row enumerator the leaseholder derives from, or it
		// would grant values the rows already hold after every restart. A
		// MULTI-node cluster without a bucket has no shared registry, so
		// leave uniqueReg nil — NOT NULL UNIQUE is then rejected at DDL
		// rather than silently un-coordinated.
		conn, err := openAuxConn(o.cfg.Path, "unique-read", o.cfg.DisableMmap, o.cfg.ObjectBackend != nil)
		if err != nil {
			return err
		}
		o.uniqueRead = conn
		o.push(func() { _ = o.uniqueRead.Close() })
		o.uniqueReg = unique.NewLocal().WithEnumerate(func() (unique.Snapshot, error) {
			return enumerateCoordinatedClaims(o.cat, o.uniqueRead)
		})
	}
	return nil
}

// startProducer builds the producer over the writer conn and drains
// the self-journal to its tip so the cache is current before the
// snapshotter starts and before Close can race shutdown.
func (o *opener) startProducer(ctx context.Context) error {
	// The self-log (durable capture for verbatim republish, ARCHITECTURE.md
	// "Self-log") matters only when this node replicates: a transport to serve
	// peer-pull, or a bucket to seed S3 and to bound the log via truncation.
	// Single-node mode has one source — no re-derive divergence and nothing
	// to publish — so skip the capture cost entirely.
	var selfLog syncer.SelfLog
	var recoverSelf func(nodestate.SelfLogReplayer) error
	if o.cfg.Transport != nil || o.cfg.ObjectBackend != nil {
		selfLog = o.mgr
		recoverSelf = func(blobs nodestate.SelfLogReplayer) error {
			selfJournal, err := o.mgr.Journal(o.originClaim.Origin)
			if err != nil {
				return err
			}
			return nodestate.RecoverSelf(o.cache, selfJournal, o.cat, blobs)
		}
	}
	prod, err := producer.New(o.appWrite, o.sc, o.cat, producer.Config{
		JournalDir:                layout.JournalDir(o.cfg.Path, o.originClaim.Origin),
		Cache:                     o.cache,
		SchemaLog:                 o.cfg.SchemaLog,
		AppHelper:                 o.appHelper,
		BlobRead:                  o.appBlobRead,
		ReplicateUnderscoreTables: o.cfg.ReplicateUnderscoreTables,
		IdempotentDDL:             o.cfg.IdempotentDDL,
		UniqueRegistry:            o.uniqueReg,
		SelfLog:                   selfLog,
		RecoverSelf:               recoverSelf,
	})
	if err != nil {
		return fmt.Errorf("syzy: start producer: %w", err)
	}
	o.prod = prod
	o.push(func() { _ = prod.Close() })
	if o.cfg.Wake != nil {
		// Cross-kernel producer: install the wake transport on the
		// journal in place of the futex-based default. EnableSharedWake
		// is still on; SetWakeFunc takes priority.
		prod.Journal().SetWakeFunc(o.cfg.Wake.Wake)
	}

	// Wait for the drainer to reach the journal tip before the node
	// starts serving. producer.New already replayed the self-log
	// (RecoverSelf) and resumed the drainer at the self-log tip, so this
	// captures only the never-published tail — durably, into the self-log.
	// That tail reaches peers via peer-pull gap-fill and the sealer via
	// seedSealerFromSelfLog, so its live broadcast/seal (listeners wired
	// later) isn't required; we just need the cache current before the
	// snapshotter starts and before Close can race shutdown.
	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()
	if err := prod.WaitForDrain(drainCtx); err != nil {
		return fmt.Errorf("syzy: drain self-journal: %w", err)
	}
	return nil
}

// assembleNode builds the Node from the acquired pieces and wires the
// producer's record feed into the notify path.
func (o *opener) assembleNode() {
	snap := nodestate.NewSnapshotter(o.cache, o.sc, nodestate.SnapshotterConfig{
		Interval: 5 * time.Second,
	})

	o.node = &Node{
		appPath:      o.cfg.Path,
		appWrite:     o.appWrite,
		appApply:     o.appApply,
		appHelper:    o.appHelper,
		appBlobRead:  o.appBlobRead,
		meta:         o.sc,
		catalog:      o.cat,
		cache:        o.cache,
		producer:     o.prod,
		leaseholder:  o.leaseholder,
		uniqueRead:   o.uniqueRead,
		snap:         snap,
		log:          o.log,
		clusterID:    o.clusterID,
		schemaLog:    o.cfg.SchemaLog,
		disableMmap:  o.cfg.DisableMmap,
		freshAtOpen:  o.freshAtOpen,
		mirror:       o.mgr,
		originClaim:  o.originClaim,
		daemonClaim:  o.daemonClaim,
		notifier:     o.notifier,
		wakeListener: o.cfg.WakeListener,
	}
	o.node.handoffDrainTimeout = defaultHandoffDrainTimeout
	writerDB := sqlitebridge.OpenDB(o.node.appWrite)
	o.node.writerDB = writerDB
	o.push(func() { _ = writerDB.Close() })

	// Wire the producer's per-Changeset records into the notify feed.
	// Self-origin commits flow through here; remote applies wire in
	// buildBroker once the broker is built; secondary-process commits
	// wire when scanSecondaries spawns drainers.
	o.prod.OnRecords(o.node.publishRecords)
}

// buildBroker constructs the apply pipeline and its catch-up sources,
// and wires transport dissemination.
//
// The broker is built whenever there's a durable schema log to
// reconcile against, even without a transport. Schema catch-up
// replays the log into the local catalog/schema_seq so a node behind
// the durable head (a peer advanced it, or this node restarted after
// a schema-log Append committed but before the local DDL commit)
// converges instead of livelocking its next DDL on "head moved". With
// a transport we additionally wire peer broadcast + gap-fill. Done
// before goroutines spin up so a startup failure unwinds cleanly
// rather than leaking the broker.
func (o *opener) buildBroker() error {
	node := o.node

	// When the sealer publishes to S3, the broker gets a GapFiller +
	// TipSource over the bucket so a returning-from-offline node can
	// catch up on origins it never received live broadcasts from. Both
	// are satisfied by one s3fetch.Source.
	if o.cfg.Transport != nil && o.cfg.ObjectBackend != nil {
		o.s3Source = s3fetch.NewSource(o.cfg.ObjectBackend)
	}

	if o.cfg.Transport == nil && o.cfg.SchemaLog == nil {
		return nil
	}
	catchup := o.cfg.SchemaCatchupInterval
	if catchup == 0 {
		catchup = defaultSchemaCatchupInterval
	}
	var (
		peerFiller transport.GapFiller
		s3Filler   transport.GapFiller
		tipSource  transport.TipSource
		gapFiller  transport.GapFiller
	)
	if o.cfg.Transport != nil {
		// Peer-pull catchup is available on any transport that
		// satisfies the optional capability interfaces in
		// transport/transport.go. *tcpmesh.Channel qualifies;
		// in-memory and other transports skip.
		if r, ok := o.cfg.Transport.(transport.CatchupRegistrar); ok {
			r.SetCatchupSource(o.mgr)
		}
		// Serve our applied-frontier to peers so they discover origins they
		// never saw live and can decide when an origin is fully replicated.
		if r, ok := o.cfg.Transport.(transport.FrontierRegistrar); ok {
			r.SetFrontierSource(frontierFromCache{o.cache})
		}
		// Clone serving is opt-in and library-owned: registered here,
		// unregistered in Close. Fail closed on a transport that can't
		// serve rather than silently not serving.
		if o.cfg.ServeClones {
			bs, ok := o.cfg.Transport.(transport.BundleSource)
			if !ok {
				return fmt.Errorf("syzy: Config.ServeClones requires a Transport that accepts clone requests (transport.BundleSource)")
			}
			bs.SetBundleHandler(node.ServeBundle)
		}
		if b, ok := o.cfg.Transport.(transport.PeerCatchupBuilder); ok {
			peerFiller = b.PeerCatchupBuilder()
		}
		// Peer-frontier discovery: pull peers' frontiers so a new/returning
		// origin is found in seconds (vs the object-store seal+LIST
		// backstop), and so the reaper can prove an origin fully replicated.
		if b, ok := o.cfg.Transport.(transport.PeerFrontierBuilder); ok {
			node.peerFrontier = b.PeerFrontierBuilder()
		}
		if o.s3Source != nil {
			s3Filler = o.s3Source
		}
		// Compose tip sources without passing typed-nils (a nil
		// *s3fetch.Source as an interface is a non-nil interface).
		var tips []transport.TipSource
		if node.peerFrontier != nil {
			tips = append(tips, node.peerFrontier)
		}
		if o.s3Source != nil {
			tips = append(tips, o.s3Source)
		}
		tipSource = mergeTipSources(tips...)
		gapFiller = gapfillerchain.New(peerFiller, s3Filler)
	}
	br, err := broker.New(broker.Config{
		AppApply:              o.appApply,
		Meta:                  o.sc,
		Catalog:               o.cat,
		Log:                   o.log,
		Transport:             o.cfg.Transport, // nil in schema-catch-up-only mode
		Cache:                 o.cache,
		MirrorJournals:        o.mgr,
		SchemaLog:             o.cfg.SchemaLog,
		SchemaCatchupInterval: catchup,
		GapFiller:             gapFiller,
		TipSource:             tipSource,
	})
	if err != nil {
		return fmt.Errorf("syzy: build broker: %w", err)
	}
	node.broker = br
	// DDL admission retries an Append CAS loss after catching the
	// catalog up to the schema-log head; the broker's synchronous
	// catch-up makes that immediate instead of tick-bound.
	o.prod.SetSchemaCatchup(br.RunSchemaCatchupOnce)
	br.OnApplyRecords(func(orig crdt.Origin, s crdt.Seq, recs []crdt.Record) {
		node.publishRecords(crdt.Dot{Origin: orig, Seq: s}, recs)
	})
	// Stamp the apply clock after every inbound apply so
	// WaitApplyQuiescent observes catch-up activity.
	br.OnApplied(func(crdt.Origin, crdt.Seq) { node.signalApplied() })
	o.prod.SetReassert(node.reassertFn(o.log))

	if o.cfg.Transport != nil {
		// Broadcast must register first so a slow sealer (downstream)
		// can't backpressure peer dissemination. Listeners fire in
		// registration order on the drainer goroutine.
		tx := o.cfg.Transport
		o.prod.OnEncoded(func(payload []byte) {
			cp := append([]byte(nil), payload...)
			_ = tx.Broadcast(context.Background(), cp)
		})
		// (The self-log capture that used to live here as a best-effort
		// OnEncoded listener is now the drainer's durable, fsync-gated
		// capture path — see producer.Config.SelfLog — so peer-pull
		// serves our own writes from durably-captured bytes.)
		// New peer connection (inbound or outbound, or per-topic
		// TOPIC_ADD on the mesh) → kick the gap planner. The broker
		// re-runs MissingRangesUpTo and pulls whatever the live
		// broadcast missed, via peerFiller (if available) then s3.
		if n, ok := o.cfg.Transport.(transport.PeerConnectNotifier); ok {
			n.SetOnPeerConnect(br.KickFetcher)
		}
		if !o.cfg.InProcessOnly {
			node.secondaries = map[crdt.Origin]*syncer.SecondaryDrainer{}
		}
		node.transport = o.cfg.Transport
	}
	return nil
}

// wireSealer registers the sealer last so its backpressure (when the
// queue is full) doesn't stall earlier OnEncoded listeners. Its
// in-memory ContiguousSealedSeq is rebuilt from the durable self-log
// by seedSealerFromSelfLog in startBackground, so restart re-confirms
// everything against S3 (idempotent) rather than depending on
// drain-time wiring.
func (o *opener) wireSealer() {
	if o.cfg.ObjectBackend == nil {
		return
	}
	node := o.node
	node.objectBackend = o.cfg.ObjectBackend
	node.ltxSyncInterval = o.cfg.LTXSyncInterval
	node.leaseClaimSettle = o.cfg.LeaseClaimSettle
	node.sealer = sealer.New(o.cfg.ObjectBackend, sealer.Config{
		MaxBytes:   o.cfg.SealerConfig.MaxBytes,
		MaxAge:     o.cfg.SealerConfig.MaxAge,
		QueueDepth: o.cfg.SealerConfig.QueueDepth,
		Logf:       o.cfg.SealerConfig.Logf,
	})
	o.prod.OnEncoded(node.sealer.OnEncoded)

	// Enable post-snapshot journal GC now that the sealer (the
	// drained-origin watermark) exists. The provider resolves the
	// self-journal and every mirror journal; the age floor keeps a
	// retention window of history for offline-peer gap-fill.
	retention := o.cfg.SnapshotRetention
	if retention <= 0 {
		retention = DefaultSnapshotRetention
	}
	node.snap.EnableGC(gcJournals{
		self:   o.originClaim.Origin,
		selfJ:  o.prod.Journal(),
		mirror: o.mgr,
	}, node.sealer, retention)
}

// startBackground spins up the always-on goroutines (snapshotter,
// sealer, standby checkpoint) under a single cancel, pushed onto the
// unwind stack so a later phase failure stops them.
func (o *opener) startBackground() {
	node := o.node
	o.syncCtx, o.syncCancel = context.WithCancel(context.Background())
	node.syncCancel = o.syncCancel
	node.snapDone = make(chan struct{})
	go func() {
		_ = node.snap.Run(o.syncCtx)
		close(node.snapDone)
	}()
	o.push(func() {
		o.syncCancel()
		<-node.snapDone
	})
	if node.sealer != nil {
		node.sealerDone = make(chan struct{})
		go func() {
			_ = node.sealer.Run(o.syncCtx)
			close(node.sealerDone)
		}()
		// Seed the sealer from the durable self-log now that its goroutine
		// drains the queue: re-confirm every retained record against S3
		// (idempotent IfAbsent), rebuilding ContiguousSealedSeq from durable
		// state and re-sealing anything dropped at a prior shutdown. The
		// live OnEncoded feed covers records appended after this scan; the
		// overlap dedups.
		node.seedSealerFromSelfLog(o.originClaim.Origin)
	}
	// Standby WAL checkpoint: only meaningful in a published deployment (a
	// publisher lease exists for some node to hold or not). Gated at run time
	// on HoldsPublisherLease so the leader's coordinated loop owns the WAL.
	if o.cfg.ObjectBackend != nil {
		node.standbyCkptDone = make(chan struct{})
		go func() {
			node.runStandbyWALCheckpoint(o.syncCtx, standbyCheckpointInterval)
			close(node.standbyCkptDone)
		}()
	}
}

// healCatalog runs the two best-effort open-time repairs. Neither may
// block the node from serving (that would regress a degraded node
// into a crash-loop): failures log loudly and degrade to the prior
// serve-but-maybe-drop behavior, never worse.
func (o *opener) healCatalog(ctx context.Context) {
	node := o.node
	reconciled := true
	if node.broker != nil {
		// Heal any metadata-ahead-of-app.db schema skew before the node
		// serves. A two-stream restore can leave app.db missing a DDL the
		// metadata catalog already records as applied (schema_seq advanced
		// past it); without this the node serves a schema behind its own
		// catalog and silently drops every inbound write touching the absent
		// column. Synchronous on Open so a fresh restore and an existing
		// skewed node both converge before any apply. Idempotent on a
		// healthy node.
		if repaired, err := node.broker.ReconcileSchemaToSQLite(ctx); err != nil {
			o.log.Error("syzy: schema reconcile failed; serving without full heal",
				"err", err, "path", o.cfg.Path)
			reconciled = false
		} else if repaired > 0 {
			o.log.Warn("syzy: healed schema skew on open (app.db was behind metadata catalog)",
				"repaired", repaired, "path", o.cfg.Path)
		}
	}
	// Repair unique-key catalog divergence (duplicate / orphaned /
	// missing keys left by historical admission defects — replaying the
	// schema log rebuilds them forever; see producer.RepairUniqueKeys).
	// Ordering matters twice over: AFTER the schema-skew reconcile so a
	// not-yet-applied index isn't misread as an orphaned key (hence the
	// skip when the reconcile itself failed), and BEFORE the leaseholder
	// maintenance loop starts so its election rebuild enumerates from
	// the repaired catalog. Coordinated-index normalization runs first
	// on the same connection: repair must never interpret the
	// intermediate (still natively enforced) schema, so a normalization
	// failure also skips repair. An un-normalized index is strictly
	// stricter than the reservation gate — a safe, availability-only
	// degradation until the next open retries.
	if !reconciled {
		o.log.Warn("syzy: catalog repair skipped; schema reconcile did not complete", "path", o.cfg.Path)
	} else if repairConn, err := openAuxConn(o.cfg.Path, "catalog-repair", o.cfg.DisableMmap, o.cfg.ObjectBackend != nil); err != nil {
		o.log.Error("syzy: catalog repair skipped; open conn failed", "err", err, "path", o.cfg.Path)
	} else {
		normalized := true
		for _, tab := range o.cat.Tables() {
			sets := producer.CoordinatedMemberSets(tab)
			if len(sets) == 0 {
				continue
			}
			// A database written before the trigger ban existed can carry
			// a trigger that writes this table; its native index is what
			// kept that channel honest. Stripping the index would turn a
			// rejected duplicate into a committed one, so keep the index
			// (stricter, availability-only) and say so.
			if err := producer.CoordinatedTriggerConflict(repairConn, tab.Name); err != nil {
				o.log.Error("syzy: coordinated-index normalization skipped; a trigger writes this table and would bypass the reservation gate — drop the trigger, then reopen",
					"table", tab.Name, "err", err, "path", o.cfg.Path)
				normalized = false
				break
			}
			changed, err := producer.NormalizeCoordinatedIndexes(repairConn, tab.Name, sets)
			if err != nil {
				o.log.Error("syzy: coordinated-index normalization failed; catalog repair skipped",
					"table", tab.Name, "err", err, "path", o.cfg.Path)
				normalized = false
				break
			}
			if changed {
				o.log.Warn("syzy: normalized native UNIQUE enforcement of a coordinated key (one-time migration)",
					"table", tab.Name, "path", o.cfg.Path)
			}
		}
		if normalized {
			stats, err := producer.RepairUniqueKeys(repairConn, o.cat, o.sc, o.log)
			if err != nil {
				o.log.Error("syzy: catalog repair failed; serving without full heal",
					"err", err, "path", o.cfg.Path)
			} else if stats.Dropped > 0 {
				o.log.Warn("syzy: repaired unique-key catalog divergence",
					"dropped", stats.Dropped, "path", o.cfg.Path)
			}
		}
		_ = repairConn.Close()
	}
}

// startServices starts the request-serving machinery: leaseholder
// maintenance, the broker (or schema-catch-up-only loop), the notify
// dispatcher, secondary-origin drainers, the mirror reaper, and the
// publisher. Failures unwind through the stack (goroutines stopped by
// the cancel closure startBackground pushed).
func (o *opener) startServices(ctx context.Context) error {
	node := o.node
	if o.leaseholder != nil {
		node.leaseholderDone = make(chan struct{})
		go func() {
			o.leaseholder.RunMaintenance(o.syncCtx)
			close(node.leaseholderDone)
		}()
	}
	if node.broker != nil {
		if o.cfg.Transport != nil {
			if err := node.broker.Start(o.syncCtx); err != nil {
				return fmt.Errorf("syzy: start broker: %w", err)
			}
		} else if err := node.broker.StartSchemaCatchup(o.syncCtx); err != nil {
			return fmt.Errorf("syzy: start schema catchup: %w", err)
		}
	}
	// Notify dispatcher: independent of syncCtx so Subscribe consumers
	// can drain in-flight notifications during Close (we cancel its
	// context after publishers stop).
	dispatchCtx, dispatchCancel := context.WithCancel(context.Background())
	node.notifyDispatchCanc = dispatchCancel
	o.push(func() { dispatchCancel() })
	if err := node.startNotifyDispatcher(dispatchCtx); err != nil {
		return fmt.Errorf("syzy: start notify dispatcher: %w", err)
	}
	// Multi-origin drainage. Scan origins/*/ on startup, spawn a
	// SecondaryDrainer for every non-self origin we find, then start
	// a periodic rescan so newly-arriving extension processes
	// (whose journals appear under a fresh origins/<hex>/ directory)
	// get drainers spawned without a daemon restart.
	if o.cfg.Transport != nil {
		if err := node.scanSecondaries(o.syncCtx, o.cfg.Path, o.log); err != nil {
			o.log.Warn("initial origin scan", "err", err)
		}
		node.secScanDone = make(chan struct{})
		go func() {
			defer close(node.secScanDone)
			t := time.NewTicker(secondaryRescanInterval)
			defer t.Stop()
			for {
				select {
				case <-o.syncCtx.Done():
					return
				case <-t.C:
				}
				if err := node.scanSecondaries(o.syncCtx, o.cfg.Path, o.log); err != nil {
					o.log.Warn("origin rescan", "err", err)
				}
			}
		}()
	}

	// Periodically reap fully-sealed mirror journals to bound origin
	// proliferation: every unclean restart mints a fresh origin
	// (layout.MintAndClaim), and dead origins' inbound mirror journals
	// otherwise accumulate without bound. Reaping is local + reversible — a
	// reaped origin's rows stay materialized in app.db, its log stays in the
	// bucket, and a later live frame re-creates the journal via handleFor —
	// so it is gated on an object backend (the bucket is the durable
	// re-fetch source).
	if o.cfg.Transport != nil && (node.peerFrontier != nil || o.s3Source != nil) {
		go node.reaperLoop(o.syncCtx, o.s3Source)
	}

	if o.cfg.ObjectBackend != nil {
		if err := node.startPublisher(); err != nil {
			return fmt.Errorf("syzy: start publisher: %w", err)
		}
	}
	return nil
}

// connPragmas is the standard PRAGMA set applied to every connection;
// the writer adds journal_mode=WAL on top. noMmap selects mmap_size=0
// (see Config.DisableMmap).
func connPragmas(noMmap bool) string {
	mmapBytes := 67108864
	if noMmap {
		mmapBytes = 0
	}
	return fmt.Sprintf(`PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000; PRAGMA mmap_size = %d; PRAGMA temp_store = MEMORY`, mmapBytes)
}

// openAuxConn opens a non-writer SQLite connection to the app database
// and applies the standard PRAGMAs used by every aux conn (apply,
// helper). The writer connection adds journal_mode=WAL on top and is
// opened separately. role appears in the error message.
func openAuxConn(path, role string, noMmap, disableAutoCheckpoint bool) (*sqlitebridge.Conn, error) {
	c, err := sqlitebridge.Open(path, 0)
	if err != nil {
		return nil, fmt.Errorf("syzy: open %s conn: %w", role, err)
	}
	if err := c.Exec(connPragmas(noMmap)); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("syzy: configure %s conn: %w", role, err)
	}
	// wal_autocheckpoint is connection-local. Disabling it only on appWrite
	// leaves the inbound apply/helper connections free to recycle the WAL
	// outside the physical publisher's tailer lock. Every connection sharing
	// a publisher-owned WAL must defer recycling to the coordinated checkpoint.
	if disableAutoCheckpoint {
		if err := c.Exec(`PRAGMA wal_autocheckpoint = 0`); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("syzy: disable %s autocheckpoint: %w", role, err)
		}
	}
	return c, nil
}

// ensureClusterID returns the cluster identity this node should run
// under. Resolution order:
//
//  1. Persisted metadata cluster_id wins. The local DB is already
//     joined to a cluster.
//  2. Else, if be is non-nil, defer to the bucket: objstore.ResolveClusterID
//     either reads HEAD's existing id or CAS-creates a stub HEAD with a
//     fresh id (HEAD-as-beacon). Concurrent first opens against the same
//     empty bucket linearize through the CAS.
//  3. Else, mint a fresh local random id.
//
// The chosen id is persisted to the metadata before returning.
func ensureClusterID(ctx context.Context, sc *metadata.Store, be objectstore.Bucket) (crdt.ClusterID, error) {
	if cur, ok, err := sc.GetClusterID(); err != nil {
		return crdt.ClusterID{}, fmt.Errorf("syzy: read cluster_id: %w", err)
	} else if ok {
		return cur, nil
	}
	var cid crdt.ClusterID
	if be != nil {
		hexID, err := objstore.ResolveClusterID(ctx, be)
		if err != nil {
			return crdt.ClusterID{}, fmt.Errorf("syzy: resolve cluster_id from object backend: %w", err)
		}
		raw, err := hex.DecodeString(hexID)
		if err != nil {
			return crdt.ClusterID{}, fmt.Errorf("syzy: decode cluster_id %q: %w", hexID, err)
		}
		if len(raw) != len(cid) {
			return crdt.ClusterID{}, fmt.Errorf("syzy: cluster_id length %d, want %d", len(raw), len(cid))
		}
		copy(cid[:], raw)
	} else {
		if _, err := rand.Read(cid[:]); err != nil {
			return crdt.ClusterID{}, fmt.Errorf("syzy: mint cluster_id: %w", err)
		}
	}
	if err := sc.SetClusterID(cid); err != nil {
		return crdt.ClusterID{}, fmt.Errorf("syzy: persist cluster_id: %w", err)
	}
	return cid, nil
}
