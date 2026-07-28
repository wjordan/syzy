package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/mirror"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
)

type selfRec struct {
	seq     uint64
	payload []byte
}

// readSelfLog reads the on-disk self-log (the self origin's mirror journal)
// after the node is closed, returning each captured changeset's seq and exact
// bytes. Closed-node only: the node holds the write handle while running.
func readSelfLog(t *testing.T, appPath string, origin uint64) []selfRec {
	t.Helper()
	dir := filepath.Join(layout.MetaDir(appPath), "mirror", fmt.Sprintf("origin_%d", origin))
	j, err := journal.Open(dir, mirror.DefaultSegmentSize, journal.SyncOff)
	if err != nil {
		t.Fatalf("open self-log %s: %v", dir, err)
	}
	defer j.Close()
	var out []selfRec
	it := j.Iterate(0)
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
			break
		}
		if err != nil {
			t.Fatalf("iterate self-log: %v", err)
		}
		if rec.Kind != journal.KindMirror || rec.Aborted() {
			continue
		}
		cs, err := crdt.Decode(rec.Payload)
		if err != nil {
			t.Fatalf("decode self-log payload: %v", err)
		}
		out = append(out, selfRec{seq: uint64(cs.Dot.Seq), payload: append([]byte(nil), rec.Payload...)})
	}
	return out
}

// assertContiguous checks the self-log carries seqs 1..n with no hole — the
// exact property whose absence was the fleet-replication wedge.
func assertContiguous(t *testing.T, recs []selfRec, n int) {
	t.Helper()
	if len(recs) != n {
		t.Fatalf("self-log has %d records, want %d", len(recs), n)
	}
	for i, r := range recs {
		if r.seq != uint64(i+1) {
			t.Fatalf("self-log seq at index %d = %d, want %d (hole in the seq stream)", i, r.seq, i+1)
		}
	}
}

// TestSelfLog_RestartRepublishesVerbatimNoHoles is the end-to-end regression
// for the motivating bug: after a clean restart, the drainer must resume PAST
// the durably-captured self-log tip (not re-derive already-published seqs),
// so the origin's seq stream stays contiguous and every pre-restart
// changeset's bytes are byte-identical across the restart — the verbatim
// republish §2 requires (a re-derive could produce different bytes for the
// same Dot). A regression here (drainer resuming from a stale marker) would
// re-derive seqs 1..N under new numbers, forking the stream.
func TestSelfLog_RestartRepublishesVerbatimNoHoles(t *testing.T) {
	ctx := context.Background()
	dbA := filepath.Join(t.TempDir(), "app.db")

	c, err := sqlitebridge.Open(dbA, 0)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if err := c.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT)`); err != nil {
		c.Close()
		t.Fatalf("seed schema: %v", err)
	}
	c.Close()

	const n1 = 4
	const n2 = 3

	// First boot: transport-only (enables the self-log without a sealer or
	// reaper that could truncate mid-test). Write n1 rows, each its own txn
	// so each is a distinct seq.
	tx1, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport 1: %v", err)
	}
	nodeA, err := syzy.Open(ctx, syzy.Config{Path: dbA, Transport: tx1})
	if err != nil {
		tx1.Close()
		t.Fatalf("Open A: %v", err)
	}
	origin := nodeA.Origin()
	for i := 1; i <= n1; i++ {
		if err := nodeA.Exec(fmt.Sprintf(`INSERT INTO t VALUES (%d, %d)`, i, i*10)); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	// Close drains the producer fully before returning, so the self-log now
	// holds every captured changeset.
	if err := nodeA.Close(); err != nil {
		t.Fatalf("Close A: %v", err)
	}
	tx1.Close()

	before := readSelfLog(t, dbA, origin)
	assertContiguous(t, before, n1)

	// Second boot: RecoverSelf replays the self-log and resumes the drainer
	// at the tip. Write n2 more rows.
	tx2, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport 2: %v", err)
	}
	nodeA2, err := syzy.Open(ctx, syzy.Config{Path: dbA, Transport: tx2})
	if err != nil {
		tx2.Close()
		t.Fatalf("Reopen A: %v", err)
	}
	if origin2 := nodeA2.Origin(); origin2 != origin {
		t.Fatalf("origin changed across clean restart: %d -> %d", origin, origin2)
	}
	for i := n1 + 1; i <= n1+n2; i++ {
		if err := nodeA2.Exec(fmt.Sprintf(`INSERT INTO t VALUES (%d, %d)`, i, i*10)); err != nil {
			t.Fatalf("post-restart INSERT %d: %v", i, err)
		}
	}
	if err := nodeA2.Close(); err != nil {
		t.Fatalf("Close A2: %v", err)
	}
	tx2.Close()

	after := readSelfLog(t, dbA, origin)
	assertContiguous(t, after, n1+n2)

	// Verbatim: the pre-restart records survive byte-for-byte — recovery
	// replayed them, it never re-derived them.
	for i := 0; i < n1; i++ {
		if string(after[i].payload) != string(before[i].payload) {
			t.Fatalf("self-log record %d (seq %d) changed across restart — not republished verbatim", i, before[i].seq)
		}
	}
}
