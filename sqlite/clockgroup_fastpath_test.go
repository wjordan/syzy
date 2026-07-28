package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/tcpmesh"
)

// gateLog wraps a schema log and can be told to fail Read, standing in for a
// node that cannot reach the (S3-backed) schema log — e.g. a node far from the
// bucket. Read is the per-call bucket round-trip.
type gateLog struct {
	schemalog.Log
	failReads atomic.Bool
}

func (g *gateLog) Read(ctx context.Context, fromSeq uint64, limit int) ([]schemalog.Event, error) {
	if g.failReads.Load() {
		return nil, errors.New("schema-log read gated off for test")
	}
	return g.Log.Read(ctx, fromSeq, limit)
}

// TestSetClockGroup_AlreadySetSkipsSchemaLog is the regression for a slow
// far-region hot-restart: storage.New flips every replicated table's clock
// group at boot, and SetClockGroup used to run a schema-log catch-up (an S3
// GET) on every call BEFORE checking whether the table was already in the
// target group. On a node far from the bucket that is N serial round-trips for
// what is a no-op (~60s observed from sa-east-1). The fix checks the local
// catalog first; an already-set re-flip must take that fast path and therefore
// succeed even with the schema log unreadable.
func TestSetClockGroup_AlreadySetSkipsSchemaLog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	glog := &gateLog{Log: schemalog.NewLocal()}

	tx, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	defer tx.Close()

	node, err := syzy.Open(ctx, syzy.Config{
		Path: dbPath, Transport: tx, ObjectBackend: testBackend(t),
		SchemaLog: glog, SchemaCatchupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if err := node.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, x TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// First flip does the real work (catch up + append + apply).
	if err := node.SetClockGroup(ctx, "t", syzy.ClockGroupCell); err != nil {
		t.Fatalf("first SetClockGroup: %v", err)
	}
	if g, err := node.ClockGroup("t"); err != nil || g != syzy.ClockGroupCell {
		t.Fatalf("clock group = %q, %v; want cell", g, err)
	}

	// Cut off the schema log: any round-trip from here on fails.
	glog.failReads.Store(true)

	// The table is already in the target group, so this must short-circuit on
	// the local catalog WITHOUT a schema-log read. Without the fix it runs
	// catch-up first and fails on the gated Read.
	if err := node.SetClockGroup(ctx, "t", syzy.ClockGroupCell); err != nil {
		t.Fatalf("already-set re-flip must skip the schema log and succeed, got: %v", err)
	}
}
