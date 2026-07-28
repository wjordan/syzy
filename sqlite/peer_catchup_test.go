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

// TestPeerCatchup_WithoutObjectBackend exercises the peer-pull catchup
// path end-to-end with NO object backend wired. A writes row 1 before
// B connects; the broadcast reaches no peers. B then connects, A
// writes row 2 (live broadcast reaches B and tells B about A's
// origin). The broker sees a gap at seq=1, kicks the fetcher, and
// PeerGapFiller pulls the missing payload from A's catchup endpoint —
// satisfied by A's self-mirror (no S3 in the picture).
//
// Success criterion: B's app.db has both rows within a few seconds.
// Without peer catchup the test would hang at row 1 since there is no
// s3fetch.Source to fall back to.
func TestPeerCatchup_WithoutObjectBackend(t *testing.T) {
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

	nodeA, err := syzy.Open(ctx, syzy.Config{Path: dbA, Transport: txA})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer nodeA.Close()

	if err := syzy.JoinCluster(dbB, nodeA.ClusterID()); err != nil {
		t.Fatalf("JoinCluster B: %v", err)
	}

	// A writes row 1 BEFORE B is alive. The broadcast hits no peers
	// (txA has nothing connected). With S3 disabled and no peer at
	// the moment of broadcast, the only path for row 1 to reach a
	// future peer is the peer-pull catchup wired in syzy.Open.
	if err := nodeA.Exec(`INSERT INTO t VALUES (1, 100)`); err != nil {
		t.Fatalf("INSERT row 1 on A: %v", err)
	}

	// Now bring B up with txB seeded at A; PeerGapFiller dials A's
	// mesh address directly for catchup.
	txB, err := syzy.NewTestTx(tcpmesh.Config{
		Seeds:     []string{txA.Addr()},
		DialRetry: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("transport B: %v", err)
	}
	defer txB.Close()

	nodeB, err := syzy.Open(ctx, syzy.Config{
		Path:      dbB,
		Transport: txB,
	})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer nodeB.Close()

	// Wait for B to actually connect, then A writes row 2. The live
	// broadcast of row 2 reaches B, surfaces A's origin into B's
	// cache, and exposes the gap at seq=1 → kickFetcher.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(txB.PeerAddrs()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if len(txB.PeerAddrs()) == 0 {
		t.Fatalf("B never connected to A")
	}

	if err := nodeA.Exec(`INSERT INTO t VALUES (2, 200)`); err != nil {
		t.Fatalf("INSERT row 2 on A: %v", err)
	}

	// Read B's app.db via an independent connection. Both rows must
	// land. The bound is generous: peer catchup should converge within
	// a second; the fetcher's max interval is 5min, so this latency
	// dominates if peer-pull regresses.
	read, err := sqlitebridge.Open(dbB, 0)
	if err != nil {
		t.Fatalf("open B reader: %v", err)
	}
	defer read.Close()
	deadline = time.Now().Add(10 * time.Second)
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
		if got >= 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("count on B = %d after 10s, want 2", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
