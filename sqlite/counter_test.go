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

// TestCounterColumnConvergence: full-loop counter merge. A table with an
// `INTEGER COUNTER` column replicates its DDL; both nodes increment the
// same row back-to-back (concurrent — neither has applied the other's);
// both nodes converge to the sum with no lost increment
// (sqlite/docs/DDL.md#counter-columns, CRDT.md F_counter).
func TestCounterColumnConvergence(t *testing.T) {
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

	if err := nodeA.Exec(`CREATE TABLE inventory (id INT PRIMARY KEY NOT NULL, quantity INTEGER COUNTER NOT NULL DEFAULT 0, name TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE on A: %v", err)
	}

	readA, err := sqlitebridge.Open(dbA, 0)
	if err != nil {
		t.Fatalf("open A reader: %v", err)
	}
	defer readA.Close()
	readB, err := sqlitebridge.Open(dbB, 0)
	if err != nil {
		t.Fatalf("open B reader: %v", err)
	}
	defer readB.Close()
	waitForCount(t, readB,
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='inventory'`,
		1, 5*time.Second, "CREATE TABLE never replicated to B")

	// The counter declaration must land as cell + counter on both nodes.
	for _, n := range []*syzy.Node{nodeA, nodeB} {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if g, err := n.ClockGroup("inventory"); err == nil && g == syzy.ClockGroupCell {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("counter table never became cell-group")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	if err := nodeA.Exec(`INSERT INTO inventory VALUES (1, 100, 'widget')`); err != nil {
		t.Fatalf("seed INSERT on A: %v", err)
	}
	waitForCount(t, readB, `SELECT count(*) FROM inventory WHERE quantity = 100`, 1,
		5*time.Second, "seed row never replicated to B")

	// Concurrent increments, the additive-counter conflict scenario: A adds
	// 30, B subtracts 50, back-to-back so neither has applied the
	// other's. Both must survive on both nodes: 100+30-50 = 80.
	if err := nodeA.Exec(`UPDATE inventory SET quantity = quantity + 30 WHERE id = 1`); err != nil {
		t.Fatalf("increment on A: %v", err)
	}
	if err := nodeB.Exec(`UPDATE inventory SET quantity = quantity - 50 WHERE id = 1`); err != nil {
		t.Fatalf("decrement on B: %v", err)
	}
	for _, side := range []struct {
		name string
		conn *sqlitebridge.Conn
	}{{"A", readA}, {"B", readB}} {
		waitForCount(t, side.conn,
			`SELECT count(*) FROM inventory WHERE quantity = 80`,
			1, 5*time.Second,
			"concurrent increments did not merge to 80 on node "+side.name)
	}

	// A register write racing an increment: the register column LWWs,
	// the counter keeps summing.
	if err := nodeA.Exec(`UPDATE inventory SET name = 'gadget' WHERE id = 1`); err != nil {
		t.Fatalf("register write on A: %v", err)
	}
	if err := nodeB.Exec(`UPDATE inventory SET quantity = quantity + 5 WHERE id = 1`); err != nil {
		t.Fatalf("increment on B: %v", err)
	}
	for _, side := range []struct {
		name string
		conn *sqlitebridge.Conn
	}{{"A", readA}, {"B", readB}} {
		waitForCount(t, side.conn,
			`SELECT count(*) FROM inventory WHERE quantity = 85 AND name = 'gadget'`,
			1, 5*time.Second,
			"register+counter race did not converge on node "+side.name)
	}
}
