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

// TestSetClockGroupFlipAndMerge exercises the public clock-group flip:
// two nodes share a schema log, A flips a table to the cell group, the
// flip replicates to B via schema catch-up, and concurrent
// disjoint-column updates from both nodes merge instead of one side's
// row image winning whole-row.
func TestSetClockGroupFlipAndMerge(t *testing.T) {
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

	if err := nodeA.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, x TEXT, y TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE on A: %v", err)
	}
	readB, err := sqlitebridge.Open(dbB, 0)
	if err != nil {
		t.Fatalf("open B reader: %v", err)
	}
	defer readB.Close()
	readA, err := sqlitebridge.Open(dbA, 0)
	if err != nil {
		t.Fatalf("open A reader: %v", err)
	}
	defer readA.Close()
	waitForCount(t, readB,
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'`,
		1, 5*time.Second, "CREATE TABLE never replicated to B")

	if err := nodeA.Exec(`INSERT INTO t VALUES (1, 'x0', 'y0')`); err != nil {
		t.Fatalf("seed INSERT on A: %v", err)
	}
	waitForCount(t, readB, `SELECT count(*) FROM t`, 1, 5*time.Second,
		"seed row never replicated to B")

	// Flip to cell group on A; B picks it up via schema catch-up.
	if err := nodeA.SetClockGroup(ctx, "t", syzy.ClockGroupCell); err != nil {
		t.Fatalf("SetClockGroup on A: %v", err)
	}
	if g, err := nodeA.ClockGroup("t"); err != nil || g != syzy.ClockGroupCell {
		t.Fatalf("A clock group = %q, %v; want cell", g, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if g, err := nodeB.ClockGroup("t"); err == nil && g == syzy.ClockGroupCell {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("flip never replicated to B")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Idempotent re-flip is a no-op.
	if err := nodeB.SetClockGroup(ctx, "t", syzy.ClockGroupCell); err != nil {
		t.Fatalf("idempotent SetClockGroup on B: %v", err)
	}

	// Concurrent disjoint-column updates: A writes x, B writes y,
	// back-to-back so neither has applied the other's. Cell-group
	// arbitration must merge both edits on both nodes.
	if err := nodeA.Exec(`UPDATE t SET x = 'xa' WHERE id = 1`); err != nil {
		t.Fatalf("UPDATE x on A: %v", err)
	}
	if err := nodeB.Exec(`UPDATE t SET y = 'yb' WHERE id = 1`); err != nil {
		t.Fatalf("UPDATE y on B: %v", err)
	}
	for _, side := range []struct {
		name string
		conn *sqlitebridge.Conn
	}{{"A", readA}, {"B", readB}} {
		waitForCount(t, side.conn,
			`SELECT count(*) FROM t WHERE x = 'xa' AND y = 'yb'`,
			1, 5*time.Second,
			"disjoint-column updates did not merge on node "+side.name)
	}
}

// TestSetClockGroup_RefusesCellWithCompositeCoordinatedKey: a composite
// coordinated (NOT NULL UNIQUE) key names a row shape; per-cell merge
// could assemble a row from writes that were never reserved together,
// so the flip to 'cell' is refused while such a key exists. A
// single-column coordinated key does not constrain the group.
func TestSetClockGroup_RefusesCellWithCompositeCoordinatedKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          filepath.Join(t.TempDir(), "app.db"),
		SchemaLog:     schemalog.NewLocal(),
		ObjectBackend: testBackend(t),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if err := node.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, a TEXT NOT NULL, b TEXT NOT NULL, UNIQUE(a, b))`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := node.SetClockGroup(ctx, "t", syzy.ClockGroupCell); err == nil {
		t.Fatal("SetClockGroup(cell) accepted on a table with a composite coordinated key; want refusal")
	}
	if err := node.Exec(`CREATE TABLE u (id INT PRIMARY KEY NOT NULL, a TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("CREATE TABLE u: %v", err)
	}
	if err := node.SetClockGroup(ctx, "u", syzy.ClockGroupCell); err != nil {
		t.Fatalf("SetClockGroup(cell) with a single-column coordinated key must succeed: %v", err)
	}
}
