// Package testcluster wires producer + broker + metadata + catalog +
// transport into a single Node for tests and benchmarks. One Node serves
// one app/metadata pair and one transport peer; multiple Nodes share a
// memtransport.Hub to form a test cluster.
package testcluster

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/broker"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/producer"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/transport"
	"github.com/wjordan/syzy/transport/memtransport"
)

// nodeJournals adapts (self journal, mirror manager) to
// nodestate.JournalProvider so the snapshotter can GC segments after
// each successful checkpoint.
type nodeJournals struct {
	self   crdt.Origin
	selfJ  *journal.Journal
	mirror *mirror.Manager
}

func (n *nodeJournals) JournalFor(o crdt.Origin) (*journal.Journal, error) {
	if o == n.self {
		return n.selfJ, nil
	}
	if n.mirror == nil {
		return nil, nil
	}
	return n.mirror.Journal(o)
}

// TestClusterID is the cluster_id every Node uses. Cluster mismatch checks
// are exercised in broker_test, not here.
var TestClusterID = crdt.ClusterID{0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC,
	0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC}

// Node bundles one process's view of the inner replication loop.
//
// AppWrite carries the producer hooks; tests issue local DML against it.
// AppApply is a separate connection on the same app.db file, used by the
// broker's inbound apply path. Two connections are required so inbound
// applies don't re-trigger the producer hooks on the writer connection.
//
// Read is a third connection for test assertions to poll applied state.
// Tests MUST NOT read through AppApply: it is the broker's connection,
// driven concurrently by the apply/catch-up/fetch goroutines, and a SQLite
// connection is not safe for concurrent use — a test SELECT racing an inbound
// apply on the same handle corrupts SQLite's state and crashes in cgo.
type Node struct {
	Origin   crdt.Origin
	AppWrite *sqlitebridge.Conn
	AppApply *sqlitebridge.Conn
	Read     *sqlitebridge.Conn
	Meta     *metadata.Store
	Catalog  *catalog.Catalog
	Producer *producer.Producer
	Broker   *broker.Broker

	// Cache is non-nil when the node was built via NewWithCache. It's
	// the shared in-memory CRDT state for both producer and broker.
	Cache *nodestate.Cache
	// Snapshotter is non-nil when the node was built via NewWithCache.
	// Started by Start; SnapshotOnce is exposed for tests that want to
	// force a checkpoint.
	Snapshotter *nodestate.Snapshotter
	// MirrorJournals owns per-origin inbound journals. Non-nil when
	// built via NewWithCache.
	MirrorJournals *mirror.Manager

	// hasTransport is set when New received a non-nil hub. Start refuses
	// to spawn loops if the broker has no transport (would NPE in the
	// subscribe loop).
	hasTransport bool
	snapCancel   context.CancelFunc
	snapDone     chan struct{}

	// appliedMu protects wakeup. wakeup is closed (and replaced) by the
	// broker's OnApplied listener; WaitApplied snapshots it before
	// checking the frontier so it can't miss a wake.
	appliedMu sync.Mutex
	wakeup    chan struct{}
}

// Start spawns the broker's subscribe goroutine and registers a
// t.Cleanup to stop it. ctx scopes the goroutines' lifetime. Start
// requires that New received a non-nil hub. When the node was built
// via NewWithCache, Start also launches the Snapshotter goroutine
// scoped to ctx.
func (n *Node) Start(t testing.TB, ctx context.Context) {
	t.Helper()
	if !n.hasTransport {
		t.Fatalf("testcluster.Node.Start: Node was constructed with hub=nil")
	}
	if err := n.Broker.Start(ctx); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	t.Cleanup(func() { _ = n.Broker.Close() })
	if n.Snapshotter != nil {
		snapCtx, cancel := context.WithCancel(ctx)
		n.snapCancel = cancel
		n.snapDone = make(chan struct{})
		go func() {
			_ = n.Snapshotter.Run(snapCtx)
			close(n.snapDone)
		}()
		t.Cleanup(func() {
			n.snapCancel()
			<-n.snapDone
		})
	}
}

// WaitShutdown blocks until the goroutines launched by Start have
// stopped. Call after cancelling the ctx passed to Start, and before
// closing dependencies the goroutines hold (Meta, Broker, etc.).
// Idempotent / no-op when Start was never called.
func (n *Node) WaitShutdown() {
	if n.snapDone != nil {
		<-n.snapDone
	}
}

// NewWithCache builds a Node backed by nodestate.Cache + Snapshotter
// (the production architecture). Mirrors New otherwise; the Cache is
// loaded from the freshly-opened metadata (empty on first construction)
// and the Snapshotter runs every snapshotInterval. Setting
// snapshotInterval=0 disables periodic ticks (callers can still
// Trigger).
func NewWithCache(t testing.TB, hub *memtransport.Hub, origin crdt.Origin, schema string, snapshotInterval time.Duration) *Node {
	t.Helper()
	dir := t.TempDir()
	return newWithCacheAt(t, hub, origin, schema, dir, snapshotInterval, "NORMAL")
}

// NewWithCacheJournalSync is NewWithCache with the writer at
// PRAGMA synchronous=FULL. Producer auto-derive then puts the
// self-journal in SyncOn. Used by benchmarks measuring the
// host-crash-symmetric commit path.
func NewWithCacheJournalSync(t testing.TB, hub *memtransport.Hub, origin crdt.Origin, schema string, snapshotInterval time.Duration) *Node {
	t.Helper()
	dir := t.TempDir()
	return newWithCacheAt(t, hub, origin, schema, dir, snapshotInterval, "FULL")
}

// NewWithDDL builds a Node wired to a shared SchemaLog for DDL
// replication tests. schema is empty by convention — DDL is replicated
// through the schema log, not pre-applied. Producer is configured with
// SchemaLog = log; broker runs schema-chain catch-up at the supplied
// interval (use a small value for tests).
func NewWithDDL(t testing.TB, hub *memtransport.Hub, origin crdt.Origin,
	log schemalog.Log, catchupInterval time.Duration) *Node {
	t.Helper()
	dir := t.TempDir()
	return newDDLNodeAt(t, hub, origin, log, catchupInterval, dir)
}

// alwaysUploadedSealer is the test stub for SealerProvider. Tests
// without a real S3 backend behave as if every origin's records are
// already durably uploaded — the production gate that would block
// self-origin GC isn't exercised here.
type alwaysUploadedSealer struct{}

func (alwaysUploadedSealer) ContiguousSealedSeq(uint64) uint64 { return ^uint64(0) }

// NewWithCacheGC is NewWithCache but enables segment-level GC after
// each successful snapshot. The snapshotter is wired with the
// JournalProvider (origin → journal) and a stub sealer that reports
// every drained origin as fully uploaded — sufficient for tests that
// only exercise the marker-based GC path.
func NewWithCacheGC(t testing.TB, hub *memtransport.Hub, origin crdt.Origin, schema string, snapshotInterval time.Duration) *Node {
	t.Helper()
	dir := t.TempDir()
	n := newWithCacheAt(t, hub, origin, schema, dir, snapshotInterval, "NORMAL")
	n.Snapshotter = nodestate.NewSnapshotter(n.Cache, n.Meta, nodestate.SnapshotterConfig{
		Interval: snapshotInterval,
		GC:       true,
		Journals: &nodeJournals{self: origin, selfJ: n.Producer.Journal(), mirror: n.MirrorJournals},
		Sealer:   alwaysUploadedSealer{},
		Self:     origin,
	})
	return n
}

// newDDLNodeAt builds a Node with DDL replication wired (no pre-
// applied schema, SchemaLog on the producer + broker). Used by
// DDL-replication tests; callers must drive the cluster's schema via
// schemalog.Append (typically by issuing DDL through AppWrite, which
// goes through the producer's trace_v2 hook).
func newDDLNodeAt(t testing.TB, hub *memtransport.Hub, origin crdt.Origin,
	log schemalog.Log, catchupInterval time.Duration, dir string) *Node {
	t.Helper()
	appDB := filepath.Join(dir, "app.db")

	appWrite, err := sqlitebridge.Open(appDB, 0)
	if err != nil {
		t.Fatalf("open app (writer): %v", err)
	}
	t.Cleanup(func() { _ = appWrite.Close() })
	if err := appWrite.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("WAL+synchronous: %v", err)
	}

	appApply, err := sqlitebridge.Open(appDB, 0)
	if err != nil {
		t.Fatalf("open app (apply): %v", err)
	}
	t.Cleanup(func() { _ = appApply.Close() })
	if err := appApply.Exec(`PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000; PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("PRAGMA synchronous=NORMAL: %v", err)
	}

	appHelper, err := sqlitebridge.Open(appDB, 0)
	if err != nil {
		t.Fatalf("open app (helper): %v", err)
	}
	t.Cleanup(func() { _ = appHelper.Close() })
	if err := appHelper.Exec(`PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("PRAGMA synchronous=NORMAL (helper): %v", err)
	}

	appRead, err := sqlitebridge.Open(appDB, 0)
	if err != nil {
		t.Fatalf("open app (read): %v", err)
	}
	t.Cleanup(func() { _ = appRead.Close() })
	if err := appRead.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("PRAGMA busy_timeout (read): %v", err)
	}

	if err := os.MkdirAll(layout.MetaDir(appDB), 0o755); err != nil {
		t.Fatalf("mkdir metadata dir: %v", err)
	}
	sc, err := metadata.Open(layout.MetaDB(appDB))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	if err := sc.SetClusterID(TestClusterID); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(origin); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}

	cache := nodestate.New(origin)
	if err := cache.LoadFromMeta(sc); err != nil {
		t.Fatalf("Cache.LoadFromMeta: %v", err)
	}
	mgr, err := mirror.New(mirror.Config{Root: filepath.Join(layout.MetaDir(appDB), "mirror")})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	if err := mgr.LoadExisting(); err != nil {
		t.Fatalf("mirror.LoadExisting: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	cat, err := catalog.LoadFromMeta(sc)
	if err != nil {
		t.Fatalf("LoadFromMeta: %v", err)
	}

	prod, err := producer.New(appWrite, sc, cat, producer.Config{
		JournalDir: layout.JournalDir(appDB, origin),
		Cache:      cache,
		SchemaLog:  log,
		AppHelper:  appHelper,
	})
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	t.Cleanup(func() { _ = prod.Close() })

	var peer transport.Transport
	if hub != nil {
		peer = hub.Peer()
	}
	br, err := broker.New(broker.Config{
		AppApply:              appApply,
		Meta:                  sc,
		Catalog:               cat,
		Transport:             peer,
		Cache:                 cache,
		MirrorJournals:        mgr,
		SchemaLog:             log,
		SchemaCatchupInterval: catchupInterval,
	})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}

	snapshotter := nodestate.NewSnapshotter(cache, sc, nodestate.SnapshotterConfig{Interval: 0})

	n := &Node{
		Origin:         origin,
		AppWrite:       appWrite,
		AppApply:       appApply,
		Read:           appRead,
		Meta:           sc,
		Catalog:        cat,
		Producer:       prod,
		Broker:         br,
		Cache:          cache,
		Snapshotter:    snapshotter,
		MirrorJournals: mgr,
		hasTransport:   hub != nil,
		wakeup:         make(chan struct{}),
	}
	br.OnApplied(n.onApplied)
	prod.SetReassert(br.ReassertLocal)
	if peer != nil {
		prod.OnEncoded(func(payload []byte) {
			cp := append([]byte(nil), payload...)
			_ = peer.Broadcast(context.Background(), cp)
		})
	}
	return n
}

// newWithCacheAt is NewWithCache parameterized by an explicit dir, so
// recovery tests can re-open the same on-disk state across separate
// "process lifecycles".
func newWithCacheAt(t testing.TB, hub *memtransport.Hub, origin crdt.Origin, schema string, dir string, snapshotInterval time.Duration, writerSynchronous string) *Node {
	t.Helper()
	appDB := filepath.Join(dir, "app.db")

	appWrite, err := sqlitebridge.Open(appDB, 0)
	if err != nil {
		t.Fatalf("open app (writer): %v", err)
	}
	t.Cleanup(func() { _ = appWrite.Close() })
	// WAL + synchronous=writerSynchronous on the writer. NORMAL is
	// the production default for replicated workloads (durability
	// lives in the per-origin journals); FULL exercises the
	// host-crash-symmetric commit path so the producer's
	// auto-derive picks SyncOn.
	if err := appWrite.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = ` + writerSynchronous + `; PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("WAL+synchronous: %v", err)
	}
	if schema != "" {
		// CREATE TABLE IF NOT EXISTS — recovery tests re-open with the
		// same schema and shouldn't fail on duplicate-table errors.
		if err := appWrite.Exec(schema); err != nil {
			// Fall through: existing schema (recovery path).
		}
	}

	appApply, err := sqlitebridge.Open(appDB, 0)
	if err != nil {
		t.Fatalf("open app (apply): %v", err)
	}
	t.Cleanup(func() { _ = appApply.Close() })
	if err := appApply.Exec(`PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000; PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("PRAGMA synchronous=NORMAL: %v", err)
	}

	appRead, err := sqlitebridge.Open(appDB, 0)
	if err != nil {
		t.Fatalf("open app (read): %v", err)
	}
	t.Cleanup(func() { _ = appRead.Close() })
	if err := appRead.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("PRAGMA busy_timeout (read): %v", err)
	}

	// blobRead is the producer-drainer's read connection used by the
	// blob_patch materializer to read post-commit NEW bytes via
	// sqlite3_blob_open. Separate from AppWrite (the writer) and
	// AppApply (the inbound apply path) — Conn isn't safe for
	// concurrent use across goroutines.
	blobRead, err := sqlitebridge.Open(appDB, 0)
	if err != nil {
		t.Fatalf("open app (blob read): %v", err)
	}
	t.Cleanup(func() { _ = blobRead.Close() })
	if err := blobRead.Exec(`PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("PRAGMA synchronous=NORMAL (blob read): %v", err)
	}

	if err := os.MkdirAll(layout.MetaDir(appDB), 0o755); err != nil {
		t.Fatalf("mkdir metadata dir: %v", err)
	}
	sc, err := metadata.Open(layout.MetaDB(appDB))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	// SetClusterID/NodeID are idempotent (REPLACE INTO meta) so re-open
	// for recovery tests doesn't break.
	if err := sc.SetClusterID(TestClusterID); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(origin); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}

	cache := nodestate.New(origin)
	if err := cache.LoadFromMeta(sc); err != nil {
		t.Fatalf("Cache.LoadFromMeta: %v", err)
	}
	mgr, err := mirror.New(mirror.Config{Root: filepath.Join(layout.MetaDir(appDB), "mirror")})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	if err := mgr.LoadExisting(); err != nil {
		t.Fatalf("mirror.LoadExisting: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	cat, err := catalog.SeedFromSchema(appWrite, sc)
	if err != nil {
		t.Fatalf("catalog.SeedFromSchema: %v", err)
	}

	// Replay any mirror records past the cache's snapshot markers —
	// brings rowClock + frontier forward for records whose app.db DML
	// was committed before crash but whose state never made it into a
	// metadata snapshot. Catalog-aware so cell-group tables replay
	// per-column stamps, matching the live apply path.
	if _, err := nodestate.RecoverMirror(cache, mgr, cat); err != nil {
		t.Fatalf("nodestate.RecoverMirror: %v", err)
	}
	snapshotter := nodestate.NewSnapshotter(cache, sc, nodestate.SnapshotterConfig{
		Interval: snapshotInterval,
	})

	prod, err := producer.New(appWrite, sc, cat, producer.Config{
		JournalDir: layout.JournalDir(appDB, origin),
		Cache:      cache,
		BlobRead:   blobRead,
	})
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	t.Cleanup(func() { _ = prod.Close() })
	// On a fresh dir the drainer has nothing to do; on a recovery
	// re-open it processes self-journal records past the cache's
	// snapshot marker. We block until that's done so the OnEncoded
	// registration below doesn't accidentally rebroadcast historical
	// records.
	{
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := prod.WaitForDrain(drainCtx); err != nil {
			drainCancel()
			t.Fatalf("WaitForDrain (recovery): %v", err)
		}
		drainCancel()
	}

	var peer transport.Transport
	if hub != nil {
		peer = hub.Peer()
	}
	br, err := broker.New(broker.Config{
		AppApply:       appApply,
		Meta:           sc,
		Catalog:        cat,
		Transport:      peer,
		Cache:          cache,
		MirrorJournals: mgr,
	})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}

	n := &Node{
		Origin:         origin,
		AppWrite:       appWrite,
		AppApply:       appApply,
		Read:           appRead,
		Meta:           sc,
		Catalog:        cat,
		Producer:       prod,
		Broker:         br,
		Cache:          cache,
		Snapshotter:    snapshotter,
		MirrorJournals: mgr,
		hasTransport:   hub != nil,
		wakeup:         make(chan struct{}),
	}
	br.OnApplied(n.onApplied)
	prod.SetReassert(br.ReassertLocal)
	// Wire broadcast: the producer's sink fires OnEncoded on the
	// drainer goroutine after crdt.Build returns. The encoded slice
	// aliases sink-owned scratch — copy before queueing.
	if peer != nil {
		prod.OnEncoded(func(payload []byte) {
			cp := append([]byte(nil), payload...)
			_ = peer.Broadcast(context.Background(), cp)
		})
	}
	return n
}

// WaitApplied blocks until the node's frontier for origin reaches seq.
// Driven by broker.OnApplied; no polling. Fails the test on timeout.
//
// The deadline timer is lazy-allocated — when the apply has already landed
// by the first poll (the common case in tight benchmarks) WaitApplied
// returns without ever calling time.NewTimer.
//
// Reads broker.AppliedSeq (in-memory) — no metadata call per poll.
func (n *Node) WaitApplied(t testing.TB, origin crdt.Origin, seq crdt.Seq, timeout time.Duration) {
	t.Helper()
	var deadline *time.Timer
	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()
	for {
		n.appliedMu.Lock()
		wakeup := n.wakeup
		n.appliedMu.Unlock()

		if cur, ok := n.Broker.AppliedSeq(origin); ok && cur >= seq {
			return
		}
		if deadline == nil {
			deadline = time.NewTimer(timeout)
		}
		select {
		case <-wakeup:
		case <-deadline.C:
			cur, _ := n.Broker.AppliedSeq(origin)
			t.Fatalf("WaitApplied(origin=%d, seq=%d) timed out after %v (applied=%d)", origin, seq, timeout, cur)
			return
		}
	}
}

func (n *Node) onApplied(_ crdt.Origin, _ crdt.Seq) {
	n.appliedMu.Lock()
	close(n.wakeup)
	n.wakeup = make(chan struct{})
	n.appliedMu.Unlock()
}
