package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
)

// TestMultiNodeDropColumnWithData exercises replicated ALTER TABLE
// DROP COLUMN on a populated cell-group table: the drop replicates,
// post-drop DML converges, and rows written before the drop survive
// with their remaining columns intact. This is the gate for dropping
// dead columns from production tables.
func TestMultiNodeDropColumnWithData(t *testing.T) {
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
		Path: dbA, Transport: txA, ObjectBackend: testBackend(t),
		SchemaLog: log, SchemaCatchupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer nodeA.Close()
	if err := syzy.JoinCluster(dbB, nodeA.ClusterID()); err != nil {
		t.Fatalf("JoinCluster B: %v", err)
	}
	nodeB, err := syzy.Open(ctx, syzy.Config{
		Path: dbB, Transport: txB, ObjectBackend: testBackend(t),
		SchemaLog: log, SchemaCatchupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer nodeB.Close()

	if err := nodeA.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, keep TEXT, dead INT)`); err != nil {
		t.Fatalf("CREATE TABLE on A: %v", err)
	}
	readB, err := sqlitebridge.Open(dbB, 0)
	if err != nil {
		t.Fatalf("open B reader: %v", err)
	}
	defer readB.Close()
	waitForCount(t, readB,
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'`,
		1, 5*time.Second, "CREATE TABLE never replicated to B")

	// Cell group, matching production tables.
	if err := nodeA.SetClockGroup(ctx, "t", syzy.ClockGroupCell); err != nil {
		t.Fatalf("SetClockGroup: %v", err)
	}

	if err := nodeA.Exec(`INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)`); err != nil {
		t.Fatalf("seed INSERT on A: %v", err)
	}
	waitForCount(t, readB, `SELECT count(*) FROM t`, 3, 5*time.Second,
		"seed rows never replicated to B")
	// Leave an outstanding cell override on the doomed column.
	if err := nodeA.Exec(`UPDATE t SET dead = 99 WHERE id = 2`); err != nil {
		t.Fatalf("UPDATE dead on A: %v", err)
	}
	waitForCount(t, readB, `SELECT count(*) FROM t WHERE dead = 99`, 1, 5*time.Second,
		"dead-column update never replicated to B")

	if err := nodeA.Exec(`ALTER TABLE t DROP COLUMN dead`); err != nil {
		t.Fatalf("DROP COLUMN on A: %v", err)
	}
	// waitForCount is monotone-up (got >= want), so wait for a
	// predicate that flips 0→1 when the column disappears.
	waitForCount(t, readB,
		`SELECT NOT EXISTS(SELECT 1 FROM pragma_table_info('t') WHERE name='dead')`,
		1, 5*time.Second, "DROP COLUMN never replicated to B")

	// Pre-drop data survives with remaining columns intact on both.
	readA, err := sqlitebridge.Open(dbA, 0)
	if err != nil {
		t.Fatalf("open A reader: %v", err)
	}
	defer readA.Close()
	for _, side := range []struct {
		name string
		conn *sqlitebridge.Conn
	}{{"A", readA}, {"B", readB}} {
		waitForCount(t, side.conn,
			`SELECT count(*) FROM t WHERE keep IN ('a','b','c')`,
			3, 5*time.Second, "pre-drop rows damaged on node "+side.name)
	}

	// Post-drop DML from both sides converges.
	if err := nodeA.Exec(`UPDATE t SET keep = 'a2' WHERE id = 1`); err != nil {
		t.Fatalf("post-drop UPDATE on A: %v", err)
	}
	if err := nodeB.Exec(`INSERT INTO t VALUES (4, 'd')`); err != nil {
		t.Fatalf("post-drop INSERT on B: %v", err)
	}
	waitForCount(t, readB, `SELECT count(*) FROM t WHERE keep = 'a2'`, 1, 5*time.Second,
		"post-drop UPDATE never replicated to B")
	waitForCount(t, readA, `SELECT count(*) FROM t WHERE keep = 'd'`, 1, 5*time.Second,
		"post-drop INSERT never replicated to A")
}

// TestMultiNodeDropMiddleColumnSparseOrdinals drops a NON-trailing
// column, leaving the catalog's surviving ordinals sparse (ordinals
// are never renumbered; only the trailing-drop case stays dense).
// Post-drop DML then exercises the apply path's placeholder binding
// against the gapped shape: full-image INSERTs and covering UPDATEs
// from each side must land in the right columns on the other.
func TestMultiNodeDropMiddleColumnSparseOrdinals(t *testing.T) {
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
		Path: dbA, Transport: txA, ObjectBackend: testBackend(t),
		SchemaLog: log, SchemaCatchupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer nodeA.Close()
	if err := syzy.JoinCluster(dbB, nodeA.ClusterID()); err != nil {
		t.Fatalf("JoinCluster B: %v", err)
	}
	nodeB, err := syzy.Open(ctx, syzy.Config{
		Path: dbB, Transport: txB, ObjectBackend: testBackend(t),
		SchemaLog: log, SchemaCatchupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer nodeB.Close()

	if err := nodeA.Exec(`CREATE TABLE m (id INT PRIMARY KEY NOT NULL, mid TEXT, tail INT)`); err != nil {
		t.Fatalf("CREATE TABLE on A: %v", err)
	}
	readB, err := sqlitebridge.Open(dbB, 0)
	if err != nil {
		t.Fatalf("open B reader: %v", err)
	}
	defer readB.Close()
	waitForCount(t, readB,
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='m'`,
		1, 5*time.Second, "CREATE TABLE never replicated to B")

	if err := nodeA.Exec(`INSERT INTO m VALUES (1, 'x', 10)`); err != nil {
		t.Fatalf("seed INSERT on A: %v", err)
	}
	waitForCount(t, readB, `SELECT count(*) FROM m`, 1, 5*time.Second,
		"seed row never replicated to B")

	// Drop the MIDDLE column: survivors keep ordinals (id=0, tail=2).
	if err := nodeA.Exec(`ALTER TABLE m DROP COLUMN mid`); err != nil {
		t.Fatalf("DROP COLUMN on A: %v", err)
	}
	waitForCount(t, readB,
		`SELECT NOT EXISTS(SELECT 1 FROM pragma_table_info('m') WHERE name='mid')`,
		1, 5*time.Second, "DROP COLUMN never replicated to B")

	// Full-image INSERTs from both sides across the gapped shape.
	if err := nodeA.Exec(`INSERT INTO m VALUES (2, 20)`); err != nil {
		t.Fatalf("post-drop INSERT on A: %v", err)
	}
	if err := nodeB.Exec(`INSERT INTO m VALUES (3, 30)`); err != nil {
		t.Fatalf("post-drop INSERT on B: %v", err)
	}
	// A covering UPDATE (every non-PK column) rides the full-image path.
	if err := nodeA.Exec(`UPDATE m SET tail = 11 WHERE id = 1`); err != nil {
		t.Fatalf("post-drop UPDATE on A: %v", err)
	}

	readA, err := sqlitebridge.Open(dbA, 0)
	if err != nil {
		t.Fatalf("open A reader: %v", err)
	}
	defer readA.Close()
	// Values must land in the right column on the receiving side.
	waitForCount(t, readB, `SELECT count(*) FROM m WHERE id = 2 AND tail = 20`, 1, 5*time.Second,
		"A's post-drop INSERT mis-bound on B")
	waitForCount(t, readA, `SELECT count(*) FROM m WHERE id = 3 AND tail = 30`, 1, 5*time.Second,
		"B's post-drop INSERT mis-bound on A")
	waitForCount(t, readB, `SELECT count(*) FROM m WHERE id = 1 AND tail = 11`, 1, 5*time.Second,
		"A's post-drop UPDATE mis-bound on B")
}
