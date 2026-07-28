package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
)

// TestReapLiveOrigin_InboundApplyContinues reproduces the prod wedge:
// after the reaper reaps a LIVE origin's mirror journal on the receiving
// node (allowed by design when the origin is sealed to the bucket),
// subsequent live broadcasts from that origin must still apply.
func TestReapLiveOrigin_InboundApplyContinues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbA := filepath.Join(t.TempDir(), "app.db")
	dbB := filepath.Join(t.TempDir(), "app.db")
	const schema = `CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT)`
	for _, p := range []string{dbA, dbB} {
		c, err := sqlitebridge.Open(p, 0)
		if err != nil {
			t.Fatalf("seed open: %v", err)
		}
		if err := c.Exec(schema); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
		c.Close()
	}

	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}

	txA, err := NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}
	defer txA.Close()
	nodeA, err := Open(ctx, Config{Path: dbA, Transport: txA, ObjectBackend: be, InProcessOnly: true})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer nodeA.Close()

	if err := JoinCluster(dbB, nodeA.ClusterID()); err != nil {
		t.Fatalf("JoinCluster: %v", err)
	}
	txB, err := NewTestTx(tcpmesh.Config{Seeds: []string{txA.Addr()}, DialRetry: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("transport B: %v", err)
	}
	defer txB.Close()
	nodeB, err := Open(ctx, Config{Path: dbB, Transport: txB, ObjectBackend: be, InProcessOnly: true})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer nodeB.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(txB.PeerAddrs()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if len(txB.PeerAddrs()) == 0 {
		t.Fatal("B never connected")
	}

	waitRow := func(id, want int) error {
		read, err := sqlitebridge.Open(dbB, 0)
		if err != nil {
			return err
		}
		defer read.Close()
		dl := time.Now().Add(5 * time.Second)
		for time.Now().Before(dl) {
			var got int
			found := false
			stmt, _, err := read.Prepare(fmt.Sprintf("SELECT v FROM t WHERE id = %d", id))
			if err == nil {
				if has, err2 := stmt.Step(); err2 == nil && has {
					got = int(stmt.ColumnInt64(0))
					found = true
				}
				stmt.Finalize()
			}
			if found && got == want {
				return nil
			}
			time.Sleep(20 * time.Millisecond)
		}
		return fmt.Errorf("row id=%d v=%d never appeared on B", id, want)
	}

	// Row 1: prove live replication works pre-reap.
	if err := nodeA.Exec(`INSERT INTO t VALUES (1, 100)`); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if err := waitRow(1, 100); err != nil {
		t.Fatalf("pre-reap: %v", err)
	}

	// Reap A's mirror journal on B — exactly what reapOrigins does for a
	// sealed live origin.
	originA := crdt.Origin(nodeA.Origin())
	if err := nodeB.mirror.Reap(originA); err != nil {
		t.Fatalf("Reap: %v", err)
	}

	// Row 2: live apply must continue post-reap.
	if err := nodeA.Exec(`INSERT INTO t VALUES (2, 200)`); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	if err := waitRow(2, 200); err != nil {
		t.Fatalf("POST-REAP WEDGE: %v", err)
	}

	// And a third, after B's snapshotter may have run a GC pass.
	nodeB.snap.Trigger()
	time.Sleep(100 * time.Millisecond)
	if err := nodeA.Exec(`INSERT INTO t VALUES (3, 300)`); err != nil {
		t.Fatalf("insert 3: %v", err)
	}
	if err := waitRow(3, 300); err != nil {
		t.Fatalf("POST-REAP+SNAPSHOT WEDGE: %v", err)
	}

	// Reap again, snapshot (persists the now-stale marker), then hot-restart
	// B via Detach+Attach — the prod wedge sequence.
	if err := nodeB.mirror.Reap(originA); err != nil {
		t.Fatalf("Reap 2: %v", err)
	}
	if err := nodeB.snap.SnapshotOnce(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	ho, err := nodeB.Detach()
	if err != nil {
		t.Fatalf("Detach: %v", err)
	}
	nodeB2, err := Attach(ctx, Config{Path: dbB, Transport: txB, ObjectBackend: be, InProcessOnly: true}, ho)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer nodeB2.Close()
	if err := ho.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := nodeA.Exec(`INSERT INTO t VALUES (4, 400)`); err != nil {
		t.Fatalf("insert 4: %v", err)
	}
	if err := waitRow(4, 400); err != nil {
		t.Fatalf("POST-REAP+ATTACH WEDGE: %v", err)
	}
}
