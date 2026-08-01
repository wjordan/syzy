package postgres

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/pg/internal/pgtest"
)

// Performance baseline (§10). These are manual-run: they take tens of seconds
// and exist to ground launch claims, not to gate every commit. Run with
//
//	SYZY_PG_PERF=1 go test ./internal/postgres/ -run TestPerf -v -p 1
//
// The comparison that matters is against Postgres's OWN logical replication on
// the same server and workload: the engine decodes the same WAL through the
// same slot machinery, so native throughput is the ceiling, and the gap is the
// cost of arbitration plus the row-clock writes. An absolute rows/sec number
// says nothing without it — it would just be measuring the test container.

func requirePerf(t *testing.T) {
	t.Helper()
	requirePG(t)
	if os.Getenv("SYZY_PG_PERF") == "" {
		t.Skip("perf baseline: set SYZY_PG_PERF=1 to run")
	}
}

// perfRows is the workload size. Large enough that neither setup cost nor the
// poll interval of the native comparison dominates, small enough that a run
// stays under a minute.
const perfRows = 100000

// TestPerfApplyThroughput measures inbound apply: how fast this node commits
// peer changesets, including arbitration and row-clock maintenance.
func TestPerfApplyThroughput(t *testing.T) {
	requirePerf(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xbe}
	a := openEngine(t, ctx, "syzy_perf_src", 201, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, a)
	b := openEngine(t, ctx, "syzy_perf_dst", 202, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, b)

	// Build the changesets on A first so the measured window is apply only.
	appExec(t, "syzy_perf_src", fmt.Sprintf(
		`INSERT INTO public.kv SELECT g, 'v'||g FROM generate_series(1,%d) g`, perfRows))
	sets := captureAll(t, ctx, a)
	if len(sets) == 0 {
		t.Fatal("no changesets captured")
	}

	start := time.Now()
	for _, cs := range sets {
		if err := b.appl.Apply(ctx, cs); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	applySec := time.Since(start).Seconds()
	if got := len(dumpKV(t, "syzy_perf_dst")); got != perfRows {
		t.Fatalf("applied %d rows, want %d", got, perfRows)
	}
	t.Logf("apply: %.0f rows/sec (%d rows in %d changeset(s), %.2fs)",
		float64(perfRows)/applySec, perfRows, len(sets), applySec)
}

// TestPerfVersusNativeLogical runs the identical workload through Postgres's
// own logical replication, so the engine's number has a ceiling to be read
// against rather than standing alone.
func TestPerfVersusNativeLogical(t *testing.T) {
	requirePerf(t)
	ctx := context.Background()
	const src, dst = "syzy_perf_nat_src", "syzy_perf_nat_dst"
	createTestDB(t, ctx, src, schemaKV)
	createTestDB(t, ctx, dst, schemaKV)

	// Subscriptions and slots are cluster-wide names, so they need this
	// run's namespace just as the databases do.
	sub := pgtest.Name("perf_sub")

	pubConn := connectDB(t, ctx, src)
	defer pubConn.Close(ctx)
	if _, err := pubConn.Exec(ctx, `CREATE PUBLICATION perf_pub FOR TABLE public.kv`); err != nil {
		t.Fatalf("create publication: %v", err)
	}
	// Publisher and subscriber are the same server here, which makes
	// CREATE SUBSCRIPTION deadlock against itself: it holds a transaction open
	// while its walsender creates the logical slot, and logical slot creation
	// waits for every in-progress transaction — including that one. Create the
	// slot up front and hand it to the subscription instead.
	if _, err := pubConn.Exec(ctx,
		fmt.Sprintf(`SELECT pg_create_logical_replication_slot(%s, 'pgoutput')`, quoteLiteral(sub))); err != nil {
		t.Fatalf("create slot: %v", err)
	}
	subConn := connectDB(t, ctx, dst)
	defer subConn.Close(ctx)
	// Fresh connections: t.Cleanup runs after the test's defers, by which point
	// pubConn/subConn are closed and every teardown statement would fail
	// silently — leaving a subscription behind that blocks the next run from
	// even dropping the database.
	t.Cleanup(func() {
		c := context.Background()
		s, err := pgx.Connect(c, dbURL(dst))
		if err == nil {
			defer s.Close(c)
			_, _ = s.Exec(c, fmt.Sprintf(`ALTER SUBSCRIPTION %s DISABLE`, quoteIdent(sub)))
			_, _ = s.Exec(c, fmt.Sprintf(`ALTER SUBSCRIPTION %s SET (slot_name = NONE)`, quoteIdent(sub)))
			_, _ = s.Exec(c, fmt.Sprintf(`DROP SUBSCRIPTION %s`, quoteIdent(sub)))
		}
		pub, err := pgx.Connect(c, dbURL(src))
		if err == nil {
			defer pub.Close(c)
			_, _ = pub.Exec(c, `DROP PUBLICATION perf_pub`)
			_, _ = pub.Exec(c, fmt.Sprintf(`SELECT pg_drop_replication_slot(%s)`, quoteLiteral(sub)))
		}
	})
	// The subscriber dials from INSIDE the server, so it cannot use the
	// host-side mapped port the tests connect through — it needs the server's own
	// socket directory. copy_data is off: the rows are written after the
	// subscription exists, so this measures streaming apply, which is what the
	// engine's number is.
	if _, err := subConn.Exec(ctx, fmt.Sprintf(
		`CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION perf_pub
		 WITH (create_slot = false, slot_name = %s, copy_data = false)`,
		quoteIdent(sub),
		quoteLiteral("host=/var/run/postgresql user=postgres dbname="+pgDB(src)),
		quoteLiteral(sub))); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	start := time.Now()
	if _, err := pubConn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.kv SELECT g, 'v'||g FROM generate_series(1,%d) g`, perfRows)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		var n int
		if err := subConn.QueryRow(ctx, `SELECT count(*) FROM public.kv`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n >= perfRows {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("native logical replication delivered %d/%d rows in 60s", n, perfRows)
		}
		time.Sleep(20 * time.Millisecond)
	}
	sec := time.Since(start).Seconds()
	t.Logf("native logical: %.0f rows/sec end-to-end (%.2fs)", float64(perfRows)/sec, sec)
}

// TestPerfLargeTransaction characterizes the bulk-backfill shape: one
// transaction far larger than any steady-state write. Postgres buffers a
// transaction's changes until commit before streaming them, so the whole thing
// arrives as one decode burst — the question is what that costs in memory and
// how long the node is unavailable to apply peer traffic while folding it.
func TestPerfLargeTransaction(t *testing.T) {
	requirePerf(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xbf}
	const bulk = 200000
	e := openEngine(t, ctx, "syzy_perf_bulk", 203, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, e)

	appExec(t, "syzy_perf_bulk", fmt.Sprintf(
		`INSERT INTO public.kv SELECT g, 'v'||g FROM generate_series(1,%d) g`, bulk))

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	sets, err := captureOneTxn(ctx, e)
	sec := time.Since(start).Seconds()
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	rows := 0
	for _, cs := range sets {
		rows += len(cs.Records)
	}
	if rows != bulk {
		t.Fatalf("folded %d records, want %d", rows, bulk)
	}
	t.Logf("bulk txn: %d rows in one transaction, decoded+folded in %.2fs (%.0f rows/sec), "+
		"%d changeset(s)", bulk, sec, float64(bulk)/sec, len(sets))
	t.Logf("memory: heap in use +%.0f MiB, total allocated %.0f MiB — the whole "+
		"transaction is buffered as one changeset, so this scales with txn size",
		float64(after.HeapInuse-before.HeapInuse)/(1<<20),
		float64(after.TotalAlloc-before.TotalAlloc)/(1<<20))
	t.Logf("NOTE: the node applies no peer traffic for that %.2fs — a backfill of "+
		"this size is a replication-lag event on every peer", sec)
}

// captureOneTxn decodes and folds exactly one committed transaction and
// returns as soon as it lands. The ordinary capture helpers run until an idle
// timeout, which would make every measurement equal to that timeout.
func captureOneTxn(ctx context.Context, e *Engine) ([]*crdt.Changeset, error) {
	cctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	var out []*crdt.Changeset
	err := e.capt.run(cctx, collectProcess(e, &out), runOpts{stopAfter: 1})
	return out, err
}

func connectDB(t *testing.T, ctx context.Context, db string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect %s: %v", db, err)
	}
	return c
}

// quoteLiteral renders s as a SQL string literal (CREATE SUBSCRIPTION takes the
// connection string as a literal, not a parameter).
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
