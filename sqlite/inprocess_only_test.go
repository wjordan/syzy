package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
)

// TestInProcessOnly_NoSecondaryDrain is the inverse of
// TestMultiOriginDrainage: with Config.InProcessOnly set, a cross-process
// (extension) origin on node A's box must NOT be picked up by a secondary
// drainer, so its writes never reach B. This is the guard against
// re-draining retired self-origins, which otherwise re-publish their whole
// journal under fresh seqs on every restart (mirror-journal amplification).
func TestInProcessOnly_NoSecondaryDrain(t *testing.T) {
	// Mutates package-level intervals: must stay serial (no t.Parallel).
	t.Cleanup(syzy.SetSecondaryIntervalsForTest(25*time.Millisecond, 25*time.Millisecond))
	ctx := context.Background()
	dbA := filepath.Join(t.TempDir(), "app.db")
	dbB := filepath.Join(t.TempDir(), "app.db")
	const schema = `CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT)`
	for _, p := range []string{dbA, dbB} {
		c, err := sqlitebridge.Open(p, 0)
		if err != nil {
			t.Fatalf("seed open %s: %v", p, err)
		}
		if err := c.Exec(schema); err != nil {
			c.Close()
			t.Fatalf("seed exec %s: %v", p, err)
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

	// InProcessOnly on A: no secondary drainers.
	nodeA, err := syzy.Open(ctx, syzy.Config{Path: dbA, Transport: txA, ObjectBackend: testBackend(t), InProcessOnly: true})
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

	// Wait well past the (test-shrunk 25ms) secondary-scan interval —
	// >10 rescan ticks; the extension's rows must never arrive on B
	// because A never attaches a secondary drainer.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
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
		if got != 0 {
			t.Fatalf("extension origin was drained despite InProcessOnly: B saw %d of ids {10,20}", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
