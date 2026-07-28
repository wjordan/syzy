package sqlite_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
)

// testBackend builds a file-backed ObjectBackend rooted in the test's
// TempDir. Multi-node syzy.Open requires a non-nil ObjectBackend
// (sealer is the durable backstop for self-journal GC), and tests
// don't need shared state across nodes — each call gets a fresh dir.
func testBackend(t testing.TB) objectstore.Bucket {
	t.Helper()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("objectstore.OpenFS: %v", err)
	}
	return be
}

func TestOpenCreatesAndCloses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := node.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY DEFAULT (uuidv7()), v INT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := node.Exec(`INSERT INTO t (v) VALUES (1), (2), (3)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-read via a stock sqlitebridge connection to confirm the rows
	// landed in app.db.
	conn, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	defer conn.Close()

	stmt, _, err := conn.Prepare(`SELECT count(*) FROM t`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	if got := stmt.ColumnInt64(0); got != 3 {
		t.Errorf("count = %d, want 3", got)
	}
}

func TestOpenReopensSameDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	first, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := first.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := first.Exec(`INSERT INTO t VALUES (1, 100)`); err != nil {
		t.Fatalf("first INSERT: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	second, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer second.Close()
	if err := second.Exec(`INSERT INTO t VALUES (2, 200)`); err != nil {
		t.Fatalf("second INSERT: %v", err)
	}
}

func TestOpenRefusesDurableSchemaUnhealthyMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("initial Open: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("initial Close: %v", err)
	}

	sc, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	if _, err := sc.MarkSchemaUnhealthy(4, "terminal schema test failure"); err != nil {
		_ = sc.Close()
		t.Fatalf("MarkSchemaUnhealthy: %v", err)
	}
	if err := sc.Close(); err != nil {
		t.Fatalf("close metadata: %v", err)
	}

	_, err = syzy.Open(ctx, syzy.Config{Path: dbPath})
	if !errors.Is(err, syzy.ErrSchemaUnhealthy) {
		t.Fatalf("Open with schema marker = %v; want ErrSchemaUnhealthy", err)
	}
}

// TestCleanShutdownPreservesOrigin asserts the graceful Close path
// flips meta.clean_shutdown to true so the next start recycles the
// same origin instead of rotating.
func TestCleanShutdownPreservesOrigin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	first, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := first.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	firstOrigin := first.Origin()
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	sc, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		t.Fatalf("inspect metadata: %v", err)
	}
	clean, ok, err := sc.GetCleanShutdown()
	_ = sc.Close()
	if err != nil {
		t.Fatalf("GetCleanShutdown: %v", err)
	}
	if !ok || !clean {
		t.Fatalf("clean_shutdown after Close = (clean=%v, ok=%v); want (true, true)", clean, ok)
	}

	second, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer second.Close()
	if got, want := second.Origin(), firstOrigin; got != want {
		t.Errorf("origin after clean reopen = %016x; want %016x", got, want)
	}
}

// TestUncleanShutdownRotatesOrigin asserts that when meta.clean_shutdown
// is explicitly false at startup (the unclean-restart signal), syzy
// allocates a fresh local origin instead of recycling. The prior
// origin's directory must remain so the daemon's secondary-drainer
// scan can flush its trailing journal.
func TestUncleanShutdownRotatesOrigin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	first, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := first.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	firstOrigin := first.Origin()
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// Simulate an unclean prior shutdown: poke the metadata bit that
	// Close would have flipped. The next Open reads false and rotates.
	sc, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	if err := sc.SetCleanShutdown(false); err != nil {
		_ = sc.Close()
		t.Fatalf("set clean_shutdown=false: %v", err)
	}
	if err := sc.Close(); err != nil {
		t.Fatalf("close metadata: %v", err)
	}

	second, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer second.Close()
	if got := second.Origin(); got == firstOrigin {
		t.Fatalf("origin after unclean reopen = %016x; want a different (rotated) origin from %016x",
			got, firstOrigin)
	}

	// Prior origin's directory survives so secondary drain can flush it.
	priorDir := layout.OriginDir(dbPath, crdt.Origin(firstOrigin))
	if _, err := os.Stat(priorDir); err != nil {
		t.Fatalf("prior origin dir %s missing after rotation: %v", priorDir, err)
	}
}

// TestFreshOpenDoesNotRotate asserts that a brand-new database (no
// clean_shutdown flag yet) is treated as clean — first open mints,
// second clean open recycles. Without this guard, every first re-open
// of a pre-existing metadata would gratuitously rotate the origin.
func TestFreshOpenDoesNotRotate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	first, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	firstOrigin := first.Origin()
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	second, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer second.Close()
	if got, want := second.Origin(), firstOrigin; got != want {
		t.Errorf("origin after clean reopen = %016x; want %016x (no rotation expected)", got, want)
	}
}

func TestOpenRequiresPath(t *testing.T) {
	t.Parallel()
	if _, err := syzy.Open(context.Background(), syzy.Config{}); err == nil {
		t.Fatalf("Open with empty path: err = nil, want error")
	}
}

func TestOpenRefusesConcurrentDaemonRole(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	first, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer first.Close()

	if _, err := syzy.Open(ctx, syzy.Config{Path: dbPath}); err == nil {
		t.Fatalf("second Open against same path: err = nil, want refusal")
	}
}

// TestMultiOriginDrainage simulates the loadable-extension topology:
// daemon A holds the syncer pipeline; a separate "extension" producer
// (simulating an in-process loadable extension on a different writer
// process) writes through its own origin slot on the same app.db. The
// daemon's secondary-drainer scan must pick up the extension's origin
// directory and broadcast its writes to remote peer B.
func TestMultiOriginDrainage(t *testing.T) {
	// Mutates package-level intervals: must stay serial (no t.Parallel).
	t.Cleanup(syzy.SetSecondaryIntervalsForTest(25*time.Millisecond, 25*time.Millisecond))
	ctx := context.Background()
	dbA := filepath.Join(t.TempDir(), "app.db")
	dbB := filepath.Join(t.TempDir(), "app.db")
	const schema = `CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT)`
	for _, p := range []string{dbA, dbB} {
		c, err := sqlitebridge.Open(p, 0)
		if err != nil {
			t.Fatalf("seed schema open %s: %v", p, err)
		}
		if err := c.Exec(schema); err != nil {
			c.Close()
			t.Fatalf("seed schema exec %s: %v", p, err)
		}
		c.Close()
	}

	txA, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}
	defer txA.Close()
	txB, err := syzy.NewTestTx(tcpmesh.Config{Seeds: []string{txA.Addr()}, DialRetry: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("transport B: %v", err)
	}
	defer txB.Close()

	nodeA, err := syzy.Open(ctx, syzy.Config{Path: dbA, Transport: txA, ObjectBackend: testBackend(t)})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer nodeA.Close()
	if err := syzy.JoinCluster(dbB, nodeA.ClusterID()); err != nil {
		t.Fatalf("JoinCluster B: %v", err)
	}
	nodeB, err := syzy.Open(ctx, syzy.Config{Path: dbB, Transport: txB, ObjectBackend: testBackend(t)})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer nodeB.Close()

	// Simulate an extension on A's box: claim a fresh origin slot
	// distinct from the daemon's, open a writer connection, run a
	// producer-only that journals but doesn't broadcast. The daemon
	// A's secondary-drainer rescan (shrunk above) should attach.
	ext := openSimulatedExtension(t, dbA)
	defer ext.Close()

	if err := ext.writer.Exec(`INSERT INTO t VALUES (10, 1000), (20, 2000)`); err != nil {
		t.Fatalf("extension INSERT: %v", err)
	}

	read, err := sqlitebridge.Open(dbB, 0)
	if err != nil {
		t.Fatalf("open B reader: %v", err)
	}
	defer read.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		stmt, _, err := read.Prepare(`SELECT count(*) FROM t WHERE id IN (10, 20)`)
		if err != nil {
			t.Fatalf("prepare on B: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			stmt.Finalize()
			t.Fatalf("step on B: %v", err)
		}
		got := stmt.ColumnInt64(0)
		stmt.Finalize()
		if got >= 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("count on B = %d after 10s, want 2 (multi-origin replication failed)", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMultiNodeReplication asserts that two syzy.Open nodes wired to a
// pair of TCP transports converge: a write on A becomes visible on B's
// app.db within a short bound. Exercises the full Open-time wiring —
// broker, mirror, gossip, OnEncoded broadcast.
//
// Schema is pre-applied to both files before Open so SeedFromSchema
// picks the table up with deterministic name-derived table IDs.
// Without DDL replication wired through the public API, this is the
// supported path for shared-schema multi-node deployments today.
func TestMultiNodeReplication(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dbA := filepath.Join(t.TempDir(), "app.db")
	dbB := filepath.Join(t.TempDir(), "app.db")
	const schema = `CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT)`
	for _, p := range []string{dbA, dbB} {
		c, err := sqlitebridge.Open(p, 0)
		if err != nil {
			t.Fatalf("seed schema open %s: %v", p, err)
		}
		if err := c.Exec(schema); err != nil {
			c.Close()
			t.Fatalf("seed schema exec %s: %v", p, err)
		}
		c.Close()
	}

	txA, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}
	defer txA.Close()

	txB, err := syzy.NewTestTx(tcpmesh.Config{
		Seeds:     []string{txA.Addr()},
		DialRetry: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("transport B: %v", err)
	}
	defer txB.Close()

	nodeA, err := syzy.Open(ctx, syzy.Config{Path: dbA, Transport: txA, ObjectBackend: testBackend(t)})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer nodeA.Close()

	// Bring B into A's cluster before opening — otherwise each side
	// auto-mints its own cluster_id and the broker rejects inbound
	// payloads.
	if err := syzy.JoinCluster(dbB, nodeA.ClusterID()); err != nil {
		t.Fatalf("JoinCluster B: %v", err)
	}
	nodeB, err := syzy.Open(ctx, syzy.Config{Path: dbB, Transport: txB, ObjectBackend: testBackend(t)})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer nodeB.Close()

	if err := nodeA.Exec(`INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)`); err != nil {
		t.Fatalf("INSERT on A: %v", err)
	}

	// Poll B's app.db via a third connection. syzy.Node holds the
	// writer + apply conns; WAL allows concurrent readers, so opening
	// one more is safe.
	read, err := sqlitebridge.Open(dbB, 0)
	if err != nil {
		t.Fatalf("open B reader: %v", err)
	}
	defer read.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		stmt, _, err := read.Prepare(`SELECT count(*) FROM t`)
		if err != nil {
			t.Fatalf("prepare on B: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			stmt.Finalize()
			t.Fatalf("step on B: %v", err)
		}
		got := stmt.ColumnInt64(0)
		stmt.Finalize()
		if got >= 3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("count on B = %d after 5s, want 3", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMultiNodeDDL exercises the public DDL replication path: two
// syzy.Open nodes share one schemalog.Local, a CREATE TABLE on A
// reaches B via the broker's schema-catchup loop, and a follow-up DML
// converges. Without Config.SchemaLog wired, A's DDL would be rejected
// at the trace_v2 hook and the test would fail at the CREATE.
func TestMultiNodeDDL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dbA := filepath.Join(t.TempDir(), "app.db")
	dbB := filepath.Join(t.TempDir(), "app.db")

	log := schemalog.NewLocal()

	txA, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}
	defer txA.Close()
	txB, err := syzy.NewTestTx(tcpmesh.Config{Seeds: []string{txA.Addr()}, DialRetry: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("transport B: %v", err)
	}
	defer txB.Close()

	nodeA, err := syzy.Open(ctx, syzy.Config{
		Path:                  dbA,
		Transport:             txA,
		ObjectBackend:         testBackend(t),
		SchemaLog:             log,
		SchemaCatchupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer nodeA.Close()

	if err := syzy.JoinCluster(dbB, nodeA.ClusterID()); err != nil {
		t.Fatalf("JoinCluster B: %v", err)
	}
	nodeB, err := syzy.Open(ctx, syzy.Config{
		Path:                  dbB,
		Transport:             txB,
		ObjectBackend:         testBackend(t),
		SchemaLog:             log,
		SchemaCatchupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer nodeB.Close()

	if err := nodeA.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT)`); err != nil {
		t.Fatalf("CREATE TABLE on A: %v", err)
	}

	read, err := sqlitebridge.Open(dbB, 0)
	if err != nil {
		t.Fatalf("open B reader: %v", err)
	}
	defer read.Close()

	// First wait for the schema to land on B via the broker's catchup
	// loop; then issue DML so the changeset's schema_seq dep is already
	// satisfied on receipt.
	waitForCount(t, read,
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'`,
		1, 5*time.Second,
		"CREATE TABLE on A never replicated to B (schema-catchup loop not wired?)")

	if err := nodeA.Exec(`INSERT INTO t VALUES (1, 100), (2, 200)`); err != nil {
		t.Fatalf("INSERT on A: %v", err)
	}

	waitForCount(t, read, `SELECT count(*) FROM t`, 2, 5*time.Second,
		"DML on A did not converge to B within 5s")
}

// waitForCount polls a count(*) query against conn until the result is
// at least want, or fatally fails the test once timeout elapses. Used
// by replication tests that need to observe convergence on a peer.
func waitForCount(t *testing.T, conn *sqlitebridge.Conn, sql string, want int64, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got int64
	for {
		stmt, _, err := conn.Prepare(sql)
		if err != nil {
			t.Fatalf("prepare %q: %v", sql, err)
		}
		if _, err := stmt.Step(); err != nil {
			stmt.Finalize()
			t.Fatalf("step %q: %v", sql, err)
		}
		got = stmt.ColumnInt64(0)
		stmt.Finalize()
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: got %d, want >= %d", msg, got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
