package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/tcpmesh"
)

// TestInboundHealth exercises the Node.InboundHealth plumbing: zero
// value without a broker (single-node), and a per-origin applied entry
// on a node receiving inbound writes.
func TestInboundHealth(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Single-node: no broker configured → zero value.
	single, err := syzy.Open(ctx, syzy.Config{Path: filepath.Join(t.TempDir(), "app.db")})
	if err != nil {
		t.Fatalf("Open single: %v", err)
	}
	if h := single.InboundHealth(); len(h.Origins) != 0 || h.ApplyStalled ||
		h.ConsecutiveLocked != 0 || h.SelfHeals != 0 || h.LastSubscribeError != "" {
		single.Close()
		t.Fatalf("single-node health not zero: %+v", h)
	}
	if err := single.Close(); err != nil {
		t.Fatalf("close single: %v", err)
	}

	// Two nodes: B applies A's writes and reports A's origin.
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

	if err := nodeA.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE on A: %v", err)
	}
	if err := nodeA.Exec(`INSERT INTO t VALUES (1, 'v1')`); err != nil {
		t.Fatalf("INSERT on A: %v", err)
	}

	originA := nodeA.Origin()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h := nodeB.InboundHealth()
		var got *syzy.OriginInboundHealth
		for i := range h.Origins {
			if h.Origins[i].Origin == originA {
				got = &h.Origins[i]
			}
		}
		if got != nil && got.AppliedSeq >= 1 {
			if got.AppliedTip < got.AppliedSeq {
				t.Fatalf("AppliedTip %d < AppliedSeq %d", got.AppliedTip, got.AppliedSeq)
			}
			if got.LastApplied.IsZero() {
				t.Fatalf("LastApplied zero on applied origin: %+v", got)
			}
			if h.ApplyStalled || h.ConsecutiveLocked != 0 {
				t.Fatalf("healthy node reports stall: %+v", h)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("B never reported origin A applied; health = %+v", nodeB.InboundHealth())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
