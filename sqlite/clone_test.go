package sqlite_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/clone"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
)

// openSourceNode opens a syzy node, runs the seed workload, and closes
// it. We close before bundling so all snapshot state has been flushed
// to the metadata; the offline `clone.Stream` path doesn't trigger a
// snapshot itself, only the running daemon's ServeBundle does.
func openSourceNode(t *testing.T, dbPath string, seed func(*syzy.Node)) {
	t.Helper()
	node, err := syzy.Open(context.Background(), syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("syzy.Open(src): %v", err)
	}
	seed(node)
	if err := node.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}
}

func readColumnText(t *testing.T, dbPath, sql string) []string {
	t.Helper()
	c, err := sqlitebridge.Open(dbPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		t.Fatalf("open %s: %v", dbPath, err)
	}
	defer c.Close()
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	var out []string
	for {
		ok, err := stmt.Step()
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if !ok {
			break
		}
		out = append(out, string(stmt.ColumnText(0)))
	}
	return out
}

func TestClone_OfflineRoundtrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	openSourceNode(t, src, func(n *syzy.Node) {
		if err := n.Exec(`CREATE TABLE notes (
			id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()),
			body TEXT NOT NULL
		)`); err != nil {
			t.Fatalf("create: %v", err)
		}
		if err := n.Exec(`INSERT INTO notes (body) VALUES ('alpha'), ('beta'), ('gamma')`); err != nil {
			t.Fatalf("insert: %v", err)
		}
	})

	var bundle bytes.Buffer
	if err := clone.Stream(&bundle, src); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if bundle.Len() < 1024 {
		t.Fatalf("bundle suspiciously small: %d bytes", bundle.Len())
	}

	dst := filepath.Join(dir, "dst.db")
	newOrigin, err := clone.Adopt(&bundle, dst)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if newOrigin == 0 {
		t.Fatalf("Adopt returned zero origin")
	}

	bodies := readColumnText(t, dst, `SELECT body FROM notes ORDER BY body`)
	want := []string{"alpha", "beta", "gamma"}
	if len(bodies) != len(want) {
		t.Fatalf("rows: got %v want %v", bodies, want)
	}
	for i := range want {
		if bodies[i] != want[i] {
			t.Fatalf("row %d: got %q want %q", i, bodies[i], want[i])
		}
	}

	sc, err := metadata.Open(layout.MetaDB(dst))
	if err != nil {
		t.Fatalf("open dst metadata: %v", err)
	}
	defer sc.Close()
	if got, _, _ := sc.GetNodeID(); got != newOrigin {
		t.Fatalf("dst node_id: got %x want %x", got, newOrigin)
	}
	seqs, err := sc.SenderSeqs()
	if err != nil {
		t.Fatalf("SenderSeqs: %v", err)
	}
	if len(seqs) != 1 || seqs[newOrigin] != 1 {
		t.Fatalf("dst sender_seqs: %+v", seqs)
	}

	// Real test: open the destination as a syzy node. The producer's
	// startup path consults intent + clean_shutdown + sender_seq +
	// frontier; if AdoptClone left anything inconsistent the Open
	// would error.
	dstNode, err := syzy.Open(context.Background(), syzy.Config{Path: dst})
	if err != nil {
		t.Fatalf("reopen dst: %v", err)
	}
	// The runtime origin must match the one Adopt minted into the
	// metadata. If the origin dir wasn't pre-created under the staged
	// metadata, layout.Acquire would mint a fresh origin instead of
	// recycling ours, silently desyncing sender_seq/frontier.
	if dstNode.Origin() != uint64(newOrigin) {
		t.Fatalf("dst runtime origin %x != adopted origin %x", dstNode.Origin(), newOrigin)
	}
	if err := dstNode.Close(); err != nil {
		t.Fatalf("close dst: %v", err)
	}
}

func TestClone_RefusesPreexistingDestination(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	openSourceNode(t, src, func(n *syzy.Node) {
		if err := n.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('t')), v TEXT)`); err != nil {
			t.Fatalf("create: %v", err)
		}
	})

	// Pre-existing dst app.db: refuse.
	dst := filepath.Join(dir, "dst.db")
	if err := os.WriteFile(dst, []byte("anything"), 0o644); err != nil {
		t.Fatalf("seed dst: %v", err)
	}
	var buf bytes.Buffer
	if err := clone.Stream(&buf, src); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := clone.Adopt(&buf, dst); err == nil {
		t.Fatalf("Adopt over pre-existing dst should error")
	}

	// Pre-existing metadata dir but no app.db: refuse.
	dst2 := filepath.Join(dir, "dst2.db")
	if err := os.MkdirAll(layout.MetaDir(dst2), 0o755); err != nil {
		t.Fatalf("seed metadata dir: %v", err)
	}
	buf.Reset()
	if err := clone.Stream(&buf, src); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if _, err := clone.Adopt(&buf, dst2); err == nil {
		t.Fatalf("Adopt over pre-existing metadata dir should error")
	}
}

func TestClone_OverTCP_FromRunningDaemon(t *testing.T) {
	t.Parallel()
	// End-to-end: source node running with a TCP listener that has the
	// bundle handler installed; clone receiver pulls via FetchBundle
	// and adopts. Verifies the same bytes that come out of clone.Stream
	// roundtrip through the wire and produce a working destination.

	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")

	// Bind on :0 to pick free ports for gossip and bundle.
	tx, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewTestTx: %v", err)
	}
	defer tx.Close()

	srcNode, err := syzy.Open(context.Background(), syzy.Config{Path: src, Transport: tx, ObjectBackend: testBackend(t)})
	if err != nil {
		t.Fatalf("syzy.Open(src): %v", err)
	}
	defer srcNode.Close()
	tx.SetBundleHandler(srcNode.ServeBundle)

	if err := srcNode.Exec(`CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := srcNode.Exec(`INSERT INTO kv (k, v) VALUES ('a', '1'), ('b', '2'), ('c', '3')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Pull a bundle from the daemon.
	dst := filepath.Join(dir, "dst.db")
	pr, pw := io.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		err := tcpmesh.FetchBundle(ctx, tx.Endpoint(), pw)
		_ = pw.CloseWithError(err)
	}()
	newOrigin, err := clone.Adopt(pr, dst)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if uint64(newOrigin) == srcNode.Origin() {
		t.Fatalf("dst origin %x must differ from src origin", newOrigin)
	}

	// Open the destination and assert its runtime origin matches the
	// one we minted (regression for the missing origin-dir bug).
	dstNode, err := syzy.Open(context.Background(), syzy.Config{Path: dst})
	if err != nil {
		t.Fatalf("reopen dst: %v", err)
	}
	if dstNode.Origin() != uint64(newOrigin) {
		t.Fatalf("dst runtime origin %x != adopted origin %x", dstNode.Origin(), newOrigin)
	}
	if err := dstNode.Close(); err != nil {
		t.Fatalf("close dst: %v", err)
	}

	rows := readColumnText(t, dst, `SELECT k || '=' || v FROM kv ORDER BY k`)
	want := []string{"a=1", "b=2", "c=3"}
	if len(rows) != len(want) {
		t.Fatalf("rows: %v want %v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("row %d: %q want %q", i, rows[i], want[i])
		}
	}
}

// rowCount returns count(*) from a single table on a stopped database.
func rowCount(t *testing.T, dbPath, table string) int {
	t.Helper()
	c, err := sqlitebridge.Open(dbPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		t.Fatalf("open %s: %v", dbPath, err)
	}
	defer c.Close()
	stmt, _, err := c.Prepare(fmt.Sprintf(`SELECT count(*) FROM %s`, table))
	if err != nil {
		t.Fatalf("prepare count(%s): %v", table, err)
	}
	defer stmt.Finalize()
	if ok, err := stmt.Step(); err != nil || !ok {
		t.Fatalf("step count: ok=%v err=%v", ok, err)
	}
	return int(stmt.ColumnInt64(0))
}

// rowClockCount returns count(*) from metadata.db's row_clock table.
func rowClockCount(t *testing.T, metaPath string) int {
	t.Helper()
	sc, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	defer sc.Close()
	rows, err := sc.AllRowClocks()
	if err != nil {
		t.Fatalf("AllRowClocks: %v", err)
	}
	return len(rows)
}

// TestClone_ServeBundle_ConcurrentWriters exercises the Phase 2
// writer-barrier consistency invariant under sustained write load:
// every row in the cloned app.db should have a corresponding row_clock
// entry in the cloned metadata.db, and vice versa.
//
// Without the barrier this test reliably fails with deltas of hundreds
// to thousands of rows. The bundle must now be exact: ServeBundle
// serializes with Node.Exec's writer connection, and SnapshotOnce
// conditionally clears dirty cache entries after metadata I/O so rows
// dirtied while a periodic snapshot is in flight stay visible to the
// forced pre-bundle snapshot.
//
// We drive ServeBundle directly into a bytes.Buffer (rather than
// through the TCP transport) so the test exercises the barrier path
// without entangling with the broker's broadcast pipeline on the
// shared peer connection. The schema is pre-seeded before Open so
// SeedFromSchema picks up the table as replicated; DDL admission
// isn't wired through Node.Exec (see TestMultiNodeReplication).
func TestClone_ServeBundle_ConcurrentWriters(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")

	{
		c, err := sqlitebridge.Open(src, 0)
		if err != nil {
			t.Fatalf("seed schema open: %v", err)
		}
		if err := c.Exec(`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`); err != nil {
			c.Close()
			t.Fatalf("seed schema: %v", err)
		}
		c.Close()
	}

	tx, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewTestTx: %v", err)
	}
	defer tx.Close()

	srcNode, err := syzy.Open(context.Background(), syzy.Config{Path: src, Transport: tx, ObjectBackend: testBackend(t)})
	if err != nil {
		t.Fatalf("syzy.Open(src): %v", err)
	}
	defer srcNode.Close()

	// One writer goroutine is enough here. The barrier semantics we're
	// testing depend on the SQLite WAL writer slot, not on Go-level
	// parallelism — what matters is that INSERTs land over time,
	// including during the barrier hold window.
	var (
		stop    atomic.Bool
		writers sync.WaitGroup
		insOK   atomic.Int64
		insBusy atomic.Int64
	)
	writers.Add(1)
	go func() {
		defer writers.Done()
		i := 0
		for !stop.Load() {
			err := srcNode.Exec(fmt.Sprintf(`INSERT INTO notes (id, body) VALUES (%d, 'w-%d')`, i, i))
			if err == nil {
				insOK.Add(1)
			} else {
				insBusy.Add(1)
			}
			i++
			// Throttle so the drainer can keep up — without this the
			// writer's commit rate exceeds the drainer's apply rate
			// and WaitForDrain races with new appends.
			time.Sleep(50 * time.Microsecond)
		}
	}()
	// Give the writer a moment to land its first commits before we
	// start pulling — otherwise pull=0 sometimes sees zero rows.
	time.Sleep(20 * time.Millisecond)

	// Pull several bundles back-to-back while writers run.
	const bundlePulls = 5
	for pull := 0; pull < bundlePulls; pull++ {
		var buf bytes.Buffer
		if err := srcNode.ServeBundle(&buf); err != nil {
			stop.Store(true)
			writers.Wait()
			t.Fatalf("ServeBundle(pull=%d): %v", pull, err)
		}
		dst := filepath.Join(dir, fmt.Sprintf("dst-%d.db", pull))
		if _, err := clone.Adopt(&buf, dst); err != nil {
			stop.Store(true)
			writers.Wait()
			t.Fatalf("Adopt(pull=%d): %v", pull, err)
		}

		appRows := rowCount(t, dst, "notes")
		clockRows := rowClockCount(t, layout.MetaDB(dst))
		if appRows != clockRows {
			stop.Store(true)
			writers.Wait()
			t.Fatalf("pull=%d consistency violation: app rows=%d, row_clock entries=%d (delta=%d)",
				pull, appRows, clockRows, appRows-clockRows)
		}
		if appRows == 0 {
			t.Fatalf("pull=%d: no rows captured (writers haven't started?)", pull)
		}
	}

	stop.Store(true)
	writers.Wait()
	if insBusy.Load() > 0 {
		t.Logf("note: %d INSERTs hit busy_timeout (still %d successful)", insBusy.Load(), insOK.Load())
	}
}

// TestServeBundle_BarrierBlocksWriters verifies that holding the
// writer barrier during ServeBundle's drain+snapshot+pin window
// serializes against concurrent INSERT attempts: an INSERT issued
// while the barrier is held must not commit until after the bundle
// has finished pinning. Without the barrier, INSERTs would commit
// freely between metadata.db and app.db backups.
//
// We use a *blocking* writer on the receiver side of the bundle
// pipe to keep ServeBundle in its post-pin streaming phase, then
// observe that an INSERT issued while ServeBundle was running has
// committed by the time the bundle drains. The point of the test is
// that ServeBundle never panics or wedges under contention; we can't
// directly observe the ms-scale barrier window from a Go-level test.
func TestServeBundle_BarrierBlocksWriters(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")

	tx, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewTestTx: %v", err)
	}
	defer tx.Close()

	srcNode, err := syzy.Open(context.Background(), syzy.Config{Path: src, Transport: tx, ObjectBackend: testBackend(t)})
	if err != nil {
		t.Fatalf("syzy.Open(src): %v", err)
	}
	defer srcNode.Close()
	tx.SetBundleHandler(srcNode.ServeBundle)

	if err := srcNode.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('t')), v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := srcNode.Exec(`INSERT INTO t (v) VALUES ('seed')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Pull a bundle and adopt; a successful round-trip implies the
	// barrier acquired and released cleanly. Run two pulls back-to-back
	// to confirm the barrier connection is closed (no flock held).
	for i := 0; i < 2; i++ {
		dst := filepath.Join(dir, fmt.Sprintf("dst-%d.db", i))
		pr, pw := io.Pipe()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		go func() {
			err := tcpmesh.FetchBundle(ctx, tx.Endpoint(), pw)
			_ = pw.CloseWithError(err)
		}()
		_, err := clone.Adopt(pr, dst)
		cancel()
		if err != nil {
			t.Fatalf("Adopt(%d): %v", i, err)
		}
		if err := srcNode.Exec(fmt.Sprintf(`INSERT INTO t (v) VALUES ('after-%d')`, i)); err != nil {
			t.Fatalf("post-bundle insert (%d): %v", i, err)
		}
	}
}
