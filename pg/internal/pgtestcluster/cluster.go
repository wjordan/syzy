// Package pgtestcluster wires N postgres.Engine peers through an
// in-memory transport for cross-node convergence tests. Each peer owns a
// fresh PG database on the shared test container plus its own metadata
// store, self-log journal, and mirror; all peers share one schemalog.Local
// (so DDL replication composes) and one memtransport.Hub.
//
// Server selection follows the pgtest contract; scripts/pg-test-container.sh
// is the canonical recipe.
//
// Note on running alongside ./internal/postgres: each pgtestcluster node
// holds one logical replication slot for the duration of a test, and
// several postgres-package tests do the same in parallel. The container
// recipe raises max_replication_slots well above Postgres's default of 10
// for exactly this reason; against a server left at the default, run with
// `-p 1` or the quota exhausts and surfaces as "could not find free
// replication state slot" or apply timeouts.
package pgtestcluster

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/pg/internal/pgtest"
	"github.com/wjordan/syzy/pg/internal/postgres"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/transport"
	"github.com/wjordan/syzy/transport/memtransport"
)

// TestClusterID is the cluster id every cluster uses. Cluster-mismatch
// rejection is exercised in postgres-package tests; here every peer is
// in-cluster.
var TestClusterID = crdt.ClusterID{
	0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC,
	0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC, 0xCC,
}

// inboxDepth bounds the per-node decoded-inbox channel. Small enough that a
// stalled apply backpressures the subscribe loop quickly under test load.
const inboxDepth = 64

// RequirePG applies the live-Postgres contract: skip when no server is
// configured, fail when one is configured but unreachable. See pgtest.
func RequirePG(t testing.TB) {
	t.Helper()
	pgtest.BaseURL(t)
}

// Config sets up a cluster.
type Config struct {
	// N is the number of peers.
	N int
	// DBPrefix scopes the per-node database name (e.g. "syzy_clu_kv");
	// each node gets DBPrefix + "_n<i>". Must be a valid PG identifier
	// component.
	DBPrefix string
	// Schema is the SQL run against each fresh per-node DB before the
	// engine opens (pre-DDL phase). Use it to seed the replicated set.
	// Empty when DDL replication is enabled (DDL: true).
	Schema string
	// Tables is the replicated set every peer captures (schema-qualified
	// names, e.g. "public.kv"). Used as Config.Tables on each Engine.
	Tables []string
	// DDL enables cross-node DDL replication via a shared schemalog.Local.
	// When set, Schema is typically empty and tests issue CREATE TABLE
	// through one node's app conn.
	DDL bool
}

// Cluster is N postgres.Engine peers wired through one memtransport.Hub.
type Cluster struct {
	t         testing.TB
	cfg       Config
	Hub       *memtransport.Hub
	SchemaLog schemalog.Log
	Nodes     []*Node
}

// Node is one peer.
type Node struct {
	Origin     crdt.Origin
	DB         string
	ConnURL    string
	ReplURL    string
	Engine     *postgres.Engine
	Cache      *nodestate.Cache
	Meta       *metadata.Store
	Mirror     *mirror.Manager
	JournalDir string

	transport transport.Transport
	inbox     chan *crdt.Changeset
	runDone   chan error
}

// New creates the cluster (databases, engines, transport peers, schemalog)
// and registers tear-down cleanups, but does not start the engines. Call
// Start to drive the actor loops.
func New(t testing.TB, cfg Config) *Cluster {
	t.Helper()
	RequirePG(t)
	if cfg.N < 1 {
		t.Fatalf("pgtestcluster.New: N must be ≥ 1, got %d", cfg.N)
	}
	if cfg.DBPrefix == "" {
		t.Fatalf("pgtestcluster.New: DBPrefix required")
	}
	if !cfg.DDL && len(cfg.Tables) == 0 {
		t.Fatalf("pgtestcluster.New: Tables required when DDL=false")
	}

	ctx := context.Background()
	hub := memtransport.NewHub()
	t.Cleanup(hub.Close)

	var slog schemalog.Log
	if cfg.DDL {
		slog = schemalog.NewLocal()
	}

	c := &Cluster{
		t:         t,
		cfg:       cfg,
		Hub:       hub,
		SchemaLog: slog,
		Nodes:     make([]*Node, cfg.N),
	}
	for i := 0; i < cfg.N; i++ {
		c.Nodes[i] = c.makeNode(ctx, i)
	}
	return c
}

// dbName builds a per-node DB name (DBPrefix + "_n<i>"), in this run's
// fixture namespace so concurrent `go test` runs cannot collide. Lowercase
// and a valid PG identifier component.
func (c *Cluster) dbName(i int) string {
	return pgtest.Name(fmt.Sprintf("%s_n%d", c.cfg.DBPrefix, i))
}

// makeNode builds one Node: fresh DB + schema, fresh metadata + mirror +
// self-log dir, a memtransport peer, and an open postgres.Engine.
func (c *Cluster) makeNode(ctx context.Context, i int) *Node {
	t := c.t
	t.Helper()

	db := c.dbName(i)
	origin := crdt.Origin(uint64(i + 1)) // 1-indexed; 0 is reserved
	connURL := pgtest.BaseURL(t) + db
	replURL := pgtest.BaseURL(t) + db + "?replication=database"

	createTestDB(t, ctx, db, c.cfg.Schema)
	t.Cleanup(func() { dropTestDB(t, db) })

	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.db")
	meta, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta open node %d: %v", i, err)
	}
	t.Cleanup(func() { _ = meta.Close() })
	if err := meta.SetClusterID(TestClusterID); err != nil {
		t.Fatalf("meta SetClusterID node %d: %v", i, err)
	}

	mirrorRoot := filepath.Join(dir, "mirror")
	mgr, err := mirror.New(mirror.Config{Root: mirrorRoot})
	if err != nil {
		t.Fatalf("mirror.New node %d: %v", i, err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	cache := nodestate.New(origin)
	journalDir := filepath.Join(dir, "selflog")

	eng, err := postgres.Open(ctx, postgres.Config{
		Name:        db,
		Origin:      origin,
		Cluster:     TestClusterID,
		Cache:       cache,
		ConnURL:     connURL,
		ReplConnURL: replURL,
		Publication: "syzy_pub",
		Slot:        "syzy_slot_" + db,
		OriginName:  "syzy_origin_" + db,
		Tables:      c.cfg.Tables,
		Meta:        meta,
		JournalDir:  journalDir,
		Mirror:      mgr,
		DDL:         c.cfg.DDL,
		SchemaLog:   c.SchemaLog,
		// CheckpointEvery left default — cluster tests want realistic
		// batching, not per-commit.
	})
	if err != nil {
		t.Fatalf("postgres.Open node %d (%s): %v", i, db, err)
	}
	originName := "syzy_origin_" + db
	t.Cleanup(func() {
		// Drop slot, close engine, then drop the replication origin.
		// Origins are GLOBAL in PG (not per-database), so a missed drop
		// here eventually exhausts max_replication_slots cluster-wide.
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.DropSlot(dropCtx)
		_ = eng.Close()
		admin, err := pgx.Connect(dropCtx, pgtest.BaseURL(t)+"postgres")
		if err != nil {
			return
		}
		defer admin.Close(dropCtx)
		// The session release after Close is async — retry a handful of times.
		for i := 0; i < 50; i++ {
			_, err := admin.Exec(dropCtx,
				`SELECT pg_replication_origin_drop($1) WHERE pg_replication_origin_oid($1) IS NOT NULL`,
				originName)
			if err == nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	})

	return &Node{
		Origin:     origin,
		DB:         db,
		ConnURL:    connURL,
		ReplURL:    replURL,
		Engine:     eng,
		Cache:      cache,
		Meta:       meta,
		Mirror:     mgr,
		JournalDir: journalDir,
		transport:  c.Hub.Peer(),
		inbox:      make(chan *crdt.Changeset, inboxDepth),
		runDone:    make(chan error, 1),
	}
}

// Start launches every node's Engine.Run + transport.Subscribe under ctx.
// Returns once all loops are running. The Subscribe loop decodes payloads
// and filters own-origin (the sole convention for a self-broadcast loop
// over memtransport).
func (c *Cluster) Start(ctx context.Context) {
	c.t.Helper()
	for _, n := range c.Nodes {
		n := n
		broadcast := func(ctx context.Context, cs *crdt.Changeset) error {
			return n.transport.Broadcast(ctx, cs.Encoded())
		}
		go func() {
			n.runDone <- n.Engine.Run(ctx, n.inbox, broadcast)
		}()
		go func() {
			_ = n.transport.Subscribe(ctx, func(ctx context.Context, payload []byte) error {
				cs, err := crdt.Decode(payload)
				if err != nil {
					return err
				}
				if cs.Dot.Origin == n.Origin {
					return nil
				}
				select {
				case n.inbox <- cs:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
	}
}

// InboxDepth returns the current depth of node n's decoded inbox. A
// chronically full inbox indicates the orchestrator's apply loop is stalled.
func (n *Node) InboxDepth() int { return len(n.inbox) }

// RunErr non-blocking-reads any Engine.Run error this node's actor has
// already surfaced; returns (nil, false) when Run is still active.
func (n *Node) RunErr() (error, bool) {
	select {
	case err := <-n.runDone:
		// Requeue so test cleanup can also observe it.
		n.runDone <- err
		return err, true
	default:
		return nil, false
	}
}

// WaitConverge blocks until every node has applied every other node's
// commits up to that node's producer head. Polls; bounded by deadline.
//
// "Have I seen all of node X's commits?" decomposes per peer:
//   - peer's view of OWN origin: SenderNextSeq (producer counter)
//   - my view of peer's origin: FrontierFor (contiguous applied head)
//
// The cluster has converged when, for every (observer, producer) pair,
// observer.frontier(producer.origin) ≥ producer.senderNextSeq - 1.
func (c *Cluster) WaitConverge(deadline time.Duration) error {
	end := time.Now().Add(deadline)
	for {
		producerHeads := make(map[crdt.Origin]crdt.Seq)
		for _, p := range c.Nodes {
			next := p.Cache.SenderNextSeq(p.Origin)
			if next > 1 {
				producerHeads[p.Origin] = next - 1
			}
		}
		converged := true
	outer:
		for _, n := range c.Nodes {
			for origin, want := range producerHeads {
				if origin == n.Origin {
					continue // own commits visible by definition
				}
				front, _ := n.Cache.FrontierFor(origin)
				if front.LastSeq < want {
					converged = false
					break outer
				}
			}
		}
		if converged {
			return nil
		}
		if time.Now().After(end) {
			return fmt.Errorf("pgtestcluster: failed to converge within %s", deadline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// WaitIdle blocks until the cluster has genuinely stopped: every node's inbox
// is drained, every producer head is covered by every peer, and no head has
// moved for idleWindow.
//
// It observes only — no writes. That matters for any test of what the cluster
// settles on after a contention burst, because winner-repair is triggered by
// peer traffic: a probe that writes a marker row to establish a happens-after
// fence would supply exactly the message that repairs the state it is trying
// to measure, and would report convergence the quiet system does not have.
//
// The idle window is what makes this sound. A commit still undecoded in the
// WAL is in no producer head, so a burst's tail is invisible to a counter check
// sampled once; requiring the heads to hold still across a window long enough
// to cover decode-and-fold latency is what separates "finished" from
// "mid-flight".
func (c *Cluster) WaitIdle(idleWindow, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	var prev map[crdt.Origin]crdt.Seq
	stableSince := time.Now()
	for {
		heads := make(map[crdt.Origin]crdt.Seq, len(c.Nodes))
		drained := true
		for _, p := range c.Nodes {
			heads[p.Origin] = p.Cache.SenderNextSeq(p.Origin)
			if p.InboxDepth() > 0 {
				drained = false
			}
		}
		if !sameHeads(prev, heads) {
			stableSince = time.Now()
		}
		prev = heads
		if drained && time.Since(stableSince) >= idleWindow {
			if err := c.WaitConverge(time.Until(end)); err != nil {
				return err
			}
			// Converging can itself publish (a winner-repair fold), so only a run
			// that leaves every head where it was counts as idle.
			settled := true
			for _, p := range c.Nodes {
				if p.Cache.SenderNextSeq(p.Origin) != heads[p.Origin] {
					settled = false
				}
			}
			if settled {
				return nil
			}
			stableSince = time.Now()
		}
		if time.Now().After(end) {
			return fmt.Errorf("pgtestcluster: cluster still moving after %s", deadline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func sameHeads(a, b map[crdt.Origin]crdt.Seq) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// Connect opens a fresh pgx conn against node n. Caller closes.
func (n *Node) Connect(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, n.ConnURL)
}

// AppExec runs sql against node n on a fresh connection. Convenience for
// tests issuing one statement at a time.
func (n *Node) AppExec(t testing.TB, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := n.Connect(ctx)
	if err != nil {
		t.Fatalf("connect %s: %v", n.DB, err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, sql); err != nil {
		t.Fatalf("exec %s %q: %v", n.DB, sql, err)
	}
}

// DumpKV returns SELECT id,val FROM public.kv as a map[int64]string
// (helpful for the canonical KV convergence test). Tests that use a
// different schema can use Connect + manual queries.
func (n *Node) DumpKV(t testing.TB) map[int64]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := n.Connect(ctx)
	if err != nil {
		t.Fatalf("connect %s: %v", n.DB, err)
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `SELECT id,val FROM public.kv`)
	if err != nil {
		t.Fatalf("dump %s: %v", n.DB, err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan %s: %v", n.DB, err)
		}
		out[id] = val
	}
	return out
}

// --- internal helpers (DB lifecycle) ---

// createTestDB recreates db with schemaSQL applied. Mirrors the postgres
// package's helper; duplicated to avoid an internal/_test import.
func createTestDB(t testing.TB, ctx context.Context, db, schemaSQL string) {
	t.Helper()
	admin, err := pgx.Connect(ctx, pgtest.BaseURL(t)+"postgres")
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer admin.Close(ctx)
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, db)
	for i := 0; i < 50; i++ {
		var n int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots WHERE database=$1`, db).Scan(&n); err != nil {
			t.Fatalf("count slots: %v", err)
		}
		if n == 0 {
			break
		}
		_, _ = admin.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database=$1 AND NOT active`, db)
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, db)); err != nil {
		t.Fatalf("drop db: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, db)); err != nil {
		t.Fatalf("create db: %v", err)
	}
	if schemaSQL == "" {
		return
	}
	app, err := pgx.Connect(ctx, pgtest.BaseURL(t)+db)
	if err != nil {
		t.Fatalf("schema connect: %v", err)
	}
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, schemaSQL); err != nil {
		t.Fatalf("schema: %v", err)
	}
}

// dropTestDB is the post-test teardown — DROP DATABASE with FORCE.
func dropTestDB(t testing.TB, db string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, pgtest.BaseURL(t)+"postgres")
	if err != nil {
		return
	}
	defer admin.Close(ctx)
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, db)
	_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, db))
}
