package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/pg/internal/pgtest"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/engine"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
)

// These are integration tests against a live Postgres 17 with
// wal_level=logical, selected by the pgtest contract. Run
// scripts/pg-test-container.sh for the canonical server.

// Fixture names are compile-time constants, so every server-side object a
// test creates is mapped into this run's namespace (pgtest.Name) — one
// Postgres server is routinely shared by concurrent `go test` runs.
func pgDB(db string) string       { return pgtest.Name(db) }
func slotName(db string) string   { return "syzy_slot_" + pgtest.Name(db) }
func originName(db string) string { return "syzy_origin_" + pgtest.Name(db) }

func dbURL(db string) string   { return pgtest.URL() + pgDB(db) }
func replURL(db string) string { return pgtest.URL() + pgDB(db) + "?replication=database" }

// TestMain drops this run's databases, slots and origins on the way out;
// per-run names would otherwise pile up on a long-lived shared server.
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.DropRunFixtures()
	os.Exit(code)
}

const schemaKV = `CREATE TABLE public.kv (id bigint PRIMARY KEY, val text)`

func requirePG(t *testing.T) {
	t.Helper()
	pgtest.BaseURL(t)
}

// createTestDB (re)creates a fresh database with the given schema. A logical
// slot pins its database, so leaked slots are dropped before DROP DATABASE.
func createTestDB(t *testing.T, ctx context.Context, db, schemaSQL string) {
	t.Helper()
	admin, err := pgx.Connect(ctx, pgtest.URL()+"postgres")
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer admin.Close(ctx)
	name := pgDB(db)
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, name)
	for i := 0; i < 50; i++ {
		var n int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots WHERE database=$1`, name).Scan(&n); err != nil {
			t.Fatalf("count slots: %v", err)
		}
		if n == 0 {
			break
		}
		_, _ = admin.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database=$1 AND NOT active`, name)
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, name)); err != nil {
		t.Fatalf("drop db: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, name)); err != nil {
		t.Fatalf("create db: %v", err)
	}
	app, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("schema connect: %v", err)
	}
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, schemaSQL); err != nil {
		t.Fatalf("schema: %v", err)
	}
}

func newTestEngine(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID) *Engine {
	return openEngine(t, ctx, db, origin, cluster, schemaKV, []string{"public.kv"})
}

// baseTestConfig is the shared engine Config skeleton every test-open helper
// starts from; callers set their extra fields on the returned value.
func baseTestConfig(db string, origin crdt.Origin, cluster crdt.ClusterID) Config {
	return Config{
		Name:        pgDB(db),
		Origin:      origin,
		Cluster:     cluster,
		Cache:       nodestate.New(origin),
		ConnURL:     dbURL(db),
		ReplConnURL: replURL(db),
		Publication: "syzy_pub",
		Slot:        slotName(db),
		OriginName:  originName(db),
		Tables:      []string{"public.kv"},
	}
}

func openEngine(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, schema string, tables []string) *Engine {
	t.Helper()
	createTestDB(t, ctx, db, schema)
	cfg := baseTestConfig(db, origin, cluster)
	cfg.Tables = tables
	e, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	return e
}

func TestEnsurePublicationValidatesExistingConfiguration(t *testing.T) {
	requirePG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const db = "syzy_publication_validation"
	createTestDB(t, ctx, db, schemaKV)
	conn, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `CREATE PUBLICATION syzy_pub FOR TABLE public.kv`); err != nil {
		t.Fatalf("create limited publication: %v", err)
	}
	if err := ensurePublication(ctx, conn, "syzy_pub"); err == nil {
		t.Fatal("ensurePublication accepted a publication that does not cover all tables")
	}
	if _, err := conn.Exec(ctx, `DROP PUBLICATION syzy_pub`); err != nil {
		t.Fatalf("drop limited publication: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE PUBLICATION syzy_pub FOR ALL TABLES WITH (publish = 'insert')`); err != nil {
		t.Fatalf("create insert-only publication: %v", err)
	}
	if err := ensurePublication(ctx, conn, "syzy_pub"); err == nil {
		t.Fatal("ensurePublication accepted a publication missing updates and deletes")
	}
	if _, err := conn.Exec(ctx, `ALTER PUBLICATION syzy_pub SET (publish = 'insert, update, delete')`); err != nil {
		t.Fatalf("repair publication: %v", err)
	}
	if err := ensurePublication(ctx, conn, "syzy_pub"); err != nil {
		t.Fatalf("ensurePublication rejected a valid publication: %v", err)
	}
}

// closeEngine is test teardown. Engine.Close intentionally keeps the slot and
// origin (the durable resume position) in production, but both count against
// max_replication_slots, so tests must drop them or they accumulate across
// cases. Drop the slot, close (ending the origin session), then drop the
// origin — retrying the drop since the session release after disconnect is
// asynchronous.
func closeEngine(t *testing.T, ctx context.Context, e *Engine) {
	t.Helper()
	if err := e.DropSlot(ctx); err != nil {
		t.Logf("drop slot %s: %v", e.cfg.Slot, err)
	}
	origin := e.cfg.OriginName
	_ = e.Close()
	admin, err := pgx.Connect(ctx, pgtest.URL()+"postgres")
	if err != nil {
		return
	}
	defer admin.Close(ctx)
	for i := 0; i < 50; i++ {
		_, err := admin.Exec(ctx, `SELECT pg_replication_origin_drop($1) WHERE pg_replication_origin_oid($1) IS NOT NULL`, origin)
		if err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// appTxn runs several statements in one application transaction (one logical
// decoding transaction).
func appTxn(t *testing.T, db string, stmts ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("app connect: %v", err)
	}
	defer c.Close(ctx)
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for _, s := range stmts {
		if _, err := tx.Exec(ctx, s); err != nil {
			t.Fatalf("tx exec %q: %v", s, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// collectProcess is the deterministic-capture draftProcess: it folds each draft
// inline (commitTxn → fold + deliver + prune) and appends the built changeset to
// *out. Safe on the capture goroutine because these tests capture-then-apply with
// no concurrent apply, so the inline fold is the single Cache writer; it also
// keeps fold↔checkpoint coherence for the durable-restart test.
func collectProcess(e *Engine, out *[]*crdt.Changeset) draftProcess {
	return func(ctx context.Context, t *txnAccum) (bool, error) {
		return e.capt.commitTxn(ctx, func(_ context.Context, cs *crdt.Changeset) error {
			*out = append(*out, cs)
			return nil
		}, t)
	}
}

// captureAll drains every changeset the node currently has pending into its
// Cache (bounded by a short idle timeout), returning them in commit order.
// Unlike captureBacklog it does not assert a count, so it tolerates
// transactions that collapse to no net effect.
func captureAll(t *testing.T, ctx context.Context, e *Engine) []*crdt.Changeset {
	return captureAllWithin(t, ctx, e, 2*time.Second)
}

// captureAllWithin is captureAll with a caller-chosen idle window. The DDL
// build tests issue one quick local DDL txn per drain, so a tighter window
// (logical decoding delivers a committed txn in well under it) keeps them fast
// without losing the txn.
func captureAllWithin(t *testing.T, ctx context.Context, e *Engine, idle time.Duration) []*crdt.Changeset {
	t.Helper()
	// A prior capture on this engine's slot may still be releasing it (Postgres
	// drops the walsender's slot hold asynchronously after the repl conn closes),
	// so two back-to-back captures on the same engine would race "slot is active
	// for PID X". Same test-sequencing guard captureBacklog uses; production runs
	// one long-lived capture and never re-acquires.
	waitSlotInactive(t, ctx, e.cfg.ConnURL, e.cfg.Slot)
	cctx, cancel := context.WithTimeout(ctx, idle)
	defer cancel()
	var out []*crdt.Changeset
	if err := e.capt.run(cctx, collectProcess(e, &out), runOpts{}); err != nil {
		t.Fatalf("capture %s: %v", e.cfg.Name, err)
	}
	return out
}

func appExec(t *testing.T, db, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("app connect: %v", err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, sql); err != nil {
		t.Fatalf("app exec %q: %v", sql, err)
	}
}

// dropOrigin releases a replication origin by name, retrying because the
// session release after a disconnect is asynchronous.
func dropOrigin(t *testing.T, origin string) {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, pgtest.URL()+"postgres")
	if err != nil {
		return
	}
	defer admin.Close(ctx)
	for i := 0; i < 50; i++ {
		if _, err := admin.Exec(ctx,
			`SELECT pg_replication_origin_drop($1) WHERE pg_replication_origin_oid($1) IS NOT NULL`, origin); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func dumpKV(t *testing.T, db string) map[int64]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("dump connect: %v", err)
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `SELECT id, val FROM public.kv ORDER BY id`)
	if err != nil {
		t.Fatalf("dump query: %v", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var val *string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if val == nil {
			out[id] = "<null>"
		} else {
			out[id] = *val
		}
	}
	return out
}

func mustEqual(t *testing.T, db string, want map[int64]string) {
	t.Helper()
	if got := dumpKV(t, db); !mapsEqual(got, want) {
		t.Fatalf("%s = %v, want %v", db, got, want)
	}
}

func mapsEqual(a, b map[int64]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// captureBacklog drains exactly want committed transactions into the node's
// Cache, returning the built Changesets. A generous ctx timeout guards a
// wrong count rather than hanging.
func captureBacklog(t *testing.T, ctx context.Context, e *Engine, want int) []*crdt.Changeset {
	t.Helper()
	// A prior capture on this engine's slot may still be releasing it (Postgres
	// drops the walsender's slot hold asynchronously after the repl conn closes);
	// back-to-back captures would otherwise hit "slot is active". Production runs
	// one long-lived capture, so this wait is a test-sequencing concern only.
	waitSlotInactive(t, ctx, e.cfg.ConnURL, e.cfg.Slot)
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var out []*crdt.Changeset
	if err := e.capt.run(cctx, collectProcess(e, &out), runOpts{stopAfter: want}); err != nil {
		t.Fatalf("capture %s: %v", e.cfg.Name, err)
	}
	if len(out) != want {
		t.Fatalf("capture %s: got %d changesets, want %d", e.cfg.Name, len(out), want)
	}
	return out
}

// waitSlotInactive polls until the named replication slot is not active, so a
// just-finished capture's walsender has fully released it before the next start.
func waitSlotInactive(t *testing.T, ctx context.Context, connURL, slot string) {
	t.Helper()
	c, err := pgx.Connect(ctx, connURL)
	if err != nil {
		t.Fatalf("slot-wait connect: %v", err)
	}
	defer c.Close(ctx)
	for i := 0; i < 100; i++ {
		var active bool
		err := c.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&active)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && !active) {
			return
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("slot-wait query: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("slot %s still active after wait", slot)
}

func applyAll(t *testing.T, ctx context.Context, e *Engine, css []*crdt.Changeset) {
	t.Helper()
	for _, cs := range css {
		if err := e.Applier().Apply(ctx, cs); err != nil {
			t.Fatalf("apply to %s: %v", e.cfg.Name, err)
		}
	}
}

// TestConvergence: two databases converge on independent inserts and on a
// same-PK conflict resolved identically by (CL, Stamp) LWW. Each node's
// backlog is captured into its Cache before cross-applying, modelling the
// steady state where local capture is caught up.
func TestConvergence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xc0, 0xff, 0xee}

	a := newTestEngine(t, ctx, "syzy_a", 1, cluster)
	defer closeEngine(t, ctx, a)
	b := newTestEngine(t, ctx, "syzy_b", 2, cluster)
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_a", `INSERT INTO public.kv VALUES (1,'a1')`)
	appExec(t, "syzy_a", `INSERT INTO public.kv VALUES (2,'a2')`)
	appExec(t, "syzy_a", `INSERT INTO public.kv VALUES (10,'from-A')`)
	// B writes the conflicting row later (and has the higher origin), so its
	// stamp dominates: from-B wins on both nodes.
	appExec(t, "syzy_b", `INSERT INTO public.kv VALUES (3,'b1')`)
	time.Sleep(10 * time.Millisecond)
	appExec(t, "syzy_b", `INSERT INTO public.kv VALUES (10,'from-B')`)

	csA := captureBacklog(t, ctx, a, 3) // 1, 2, 10:from-A
	csB := captureBacklog(t, ctx, b, 2) // 3, 10:from-B
	// Everything B wrote locally is behind this position, and everything the
	// apply below writes is ahead of it — so the loopback capture can start
	// here and see only applied writes. Without the pin it would restart from
	// the slot's confirmed position, which these Meta-less test engines do not
	// keep in step with their in-memory Cache: an ack that has not landed yet
	// makes B re-read (and re-fold) its OWN commit, which is a harness artifact,
	// not a loopback.
	localHead := walHead(t, ctx, "syzy_b")
	applyAll(t, ctx, b, csA)
	applyAll(t, ctx, a, csB)

	want := map[int64]string{1: "a1", 2: "a2", 3: "b1", 10: "from-B"}
	mustEqual(t, "syzy_a", want)
	mustEqual(t, "syzy_b", want)

	// Loopback: re-capturing B yields nothing — the writes just applied from A
	// are origin-tagged and dropped by the slot's origin='none' filter.
	idleCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	var leaked []*crdt.Changeset
	if err := b.capt.run(idleCtx, collectProcess(b, &leaked), runOpts{startLSN: localHead}); err != nil {
		t.Fatalf("idle capture: %v", err)
	}
	if len(leaked) != 0 {
		// Say WHAT leaked: every id here came from A, so a record means the
		// origin filter missed one of the applied writes.
		t.Fatalf("loopback leak: re-captured %d changesets: %s", len(leaked), describeChangesets(leaked))
	}

	// Update + delete propagate.
	appExec(t, "syzy_a", `UPDATE public.kv SET val='a2-edited' WHERE id=2`)
	appExec(t, "syzy_a", `DELETE FROM public.kv WHERE id=1`)
	applyAll(t, ctx, b, captureBacklog(t, ctx, a, 2))
	want2 := map[int64]string{2: "a2-edited", 3: "b1", 10: "from-B"}
	mustEqual(t, "syzy_a", want2)
	mustEqual(t, "syzy_b", want2)
}

// TestIdempotency: re-delivering an already-applied changeset is a no-op
// (frontier dedupe via IsAppliedRemote), and never errors.
func TestIdempotency(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x1d, 0xe9}

	src := newTestEngine(t, ctx, "syzy_src", 5, cluster)
	defer closeEngine(t, ctx, src)
	dst := newTestEngine(t, ctx, "syzy_dst", 6, cluster)
	defer closeEngine(t, ctx, dst)

	appExec(t, "syzy_src", `INSERT INTO public.kv VALUES (1,'hello')`)

	var captured []*crdt.Changeset
	if err := src.capt.run(ctx, collectProcess(src, &captured), runOpts{stopAfter: 1}); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("expected 1 changeset, got %d", len(captured))
	}
	cs := captured[0]

	if err := dst.Applier().Apply(ctx, cs); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if !dst.cfg.Cache.IsAppliedRemote(cs.Dot.Origin, cs.Dot.Seq) {
		t.Fatalf("frontier not advanced after apply")
	}
	if got := dumpKV(t, "syzy_dst"); got[1] != "hello" {
		t.Fatalf("apply did not land: %v", got)
	}
	// Second apply must be a clean no-op.
	if err := dst.Applier().Apply(ctx, cs); err != nil {
		t.Fatalf("second apply (idempotent) errored: %v", err)
	}
	if got := dumpKV(t, "syzy_dst"); len(got) != 1 || got[1] != "hello" {
		t.Fatalf("idempotent re-apply changed state: %v", got)
	}
}

func TestApplyRejectsMalformedPKWithoutAdvancingFrontier(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xba, 0xdd}
	e := newTestEngine(t, ctx, "syzy_bad_pk", 1, cluster)
	defer closeEngine(t, ctx, e)

	var tableID crdt.TableID
	for id, ti := range e.cat.byID {
		if ti.name == "kv" {
			tableID = id
			break
		}
	}
	if tableID == (crdt.TableID{}) {
		t.Fatal("kv table missing from catalog")
	}
	valid := typedPK(t, e, "kv", "42")
	dot := crdt.Dot{Origin: 2, Seq: 1}
	cs := &crdt.Changeset{
		ClusterID: cluster,
		Dot:       dot,
		Stamp:     crdt.Stamp{Origin: dot.Origin},
		Records: []crdt.Record{crdt.Delete{
			Table: tableID,
			PK:    valid[:len(valid)-1],
			CL:    2,
		}},
	}
	if err := e.Applier().Apply(ctx, cs); err == nil || !strings.Contains(err.Error(), "invalid PK") {
		t.Fatalf("apply malformed PK error = %v, want invalid PK", err)
	}
	if e.cfg.Cache.IsAppliedRemote(dot.Origin, dot.Seq) {
		t.Fatal("malformed PK advanced the remote frontier")
	}
}

// TestLiveConvergence runs two nodes as live serialized actors (Engine.Run):
// capture decodes → enqueues drafts, and the orchestrator folds local commits +
// applies peer changesets on one goroutine, with the peer inbox as the transport.
// This is the path the deterministic TestConvergence avoids; it converges because
// the orchestrator folds every pending local draft up to the WAL head before
// arbitrating a remote write (drainToWALTarget), and is the single Cache writer.
func TestLiveConvergence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x11, 0x7e}

	a := newTestEngine(t, ctx, "syzy_la", 31, cluster)
	defer closeEngine(t, ctx, a)
	b := newTestEngine(t, ctx, "syzy_lb", 32, cluster)
	defer closeEngine(t, ctx, b)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	aInbox := make(chan *crdt.Changeset, 1024)
	bInbox := make(chan *crdt.Changeset, 1024)

	// Each node runs as one serialized actor (Engine.Run): capture decodes +
	// enqueues, and the orchestrator folds local commits and applies peer
	// changesets on a single goroutine — the sole Cache writer. broadcast ships a
	// node's folded changesets to its peer's inbox.
	run := func(node *Engine, inbox <-chan *crdt.Changeset, peerInbox chan<- *crdt.Changeset) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			broadcast := func(ctx context.Context, cs *crdt.Changeset) error {
				select {
				case peerInbox <- cs:
				case <-ctx.Done():
				}
				return nil
			}
			if err := node.Run(runCtx, inbox, broadcast); err != nil && runCtx.Err() == nil {
				t.Errorf("%s orchestrator: %v", node.cfg.Name, err)
			}
		}()
	}
	run(a, aInbox, bInbox) // a applies from aInbox, broadcasts to bInbox
	run(b, bInbox, aInbox)

	// One concurrent write each to a contended PK, plus disjoint rows — the
	// realistic case the gate targets (not pathological same-key hammering).
	var writers sync.WaitGroup
	write := func(db string, base int64) {
		writers.Add(1)
		go func() {
			defer writers.Done()
			appExec(t, db, fmt.Sprintf(`INSERT INTO public.kv VALUES (100,'from-%s')`, db)+
				` ON CONFLICT (id) DO UPDATE SET val=excluded.val`)
			for i := int64(0); i < 5; i++ {
				appExec(t, db, fmt.Sprintf(`INSERT INTO public.kv VALUES (%d,'r%d')`, base+i, i))
			}
		}()
	}
	write("syzy_la", 1000)
	write("syzy_lb", 2000)
	writers.Wait()

	// Both nodes must converge byte-for-byte: the contended row resolves to one
	// winner everywhere; disjoint rows all present (1 + 5 + 5 = 11).
	deadline := time.Now().Add(20 * time.Second)
	var da, db_ map[int64]string
	for time.Now().Before(deadline) {
		da, db_ = dumpKV(t, "syzy_la"), dumpKV(t, "syzy_lb")
		if mapsEqual(da, db_) && len(da) == 11 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if !mapsEqual(da, db_) {
		t.Fatalf("did not converge:\n A=%v\n B=%v", da, db_)
	}
	if len(da) != 11 {
		t.Fatalf("expected 11 rows, got %d: %v", len(da), da)
	}
}

// TestMaterialization exercises per-transaction net-effect collapse: an
// insert+delete of a fresh row in one txn emits nothing (no spurious
// tombstone), and a PK-change UPDATE becomes delete(old)+insert(new) so the
// old key does not survive on peers.
func TestMaterialization(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xab, 0xcd}

	src := openEngine(t, ctx, "syzy_msrc", 11, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, src)
	dst := openEngine(t, ctx, "syzy_mdst", 12, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, dst)

	appExec(t, "syzy_msrc", `INSERT INTO public.kv VALUES (1,'x')`)
	appExec(t, "syzy_msrc", `INSERT INTO public.kv VALUES (5,'keep')`)
	appTxn(t, "syzy_msrc", // insert+delete of a fresh row, one txn → net no-op
		`INSERT INTO public.kv VALUES (99,'tmp')`,
		`DELETE FROM public.kv WHERE id=99`)
	appExec(t, "syzy_msrc", `UPDATE public.kv SET id=2 WHERE id=1`) // PK change

	css := captureAll(t, ctx, src)
	// The no-op txn must contribute nothing; the PK change must carry both a
	// Delete(old) and an Insert(new).
	var dels, ins int
	for _, cs := range css {
		for _, r := range cs.Records {
			switch r.(type) {
			case crdt.Delete:
				dels++
			case crdt.Insert:
				ins++
			}
		}
	}
	if dels != 1 {
		t.Errorf("expected exactly 1 Delete (PK-change old key), got %d", dels)
	}
	if ins != 3 { // id=1, id=5, and the PK-change new id=2
		t.Errorf("expected 3 Inserts, got %d", ins)
	}

	applyAll(t, ctx, dst, css)
	want := map[int64]string{2: "x", 5: "keep"} // id=1 renamed to 2; id=99 never appeared
	mustEqual(t, "syzy_msrc", want)
	mustEqual(t, "syzy_mdst", want)
}

// TestCompositeAndNonLeadingPK regression-guards PK extraction for a
// non-leading single PK and an out-of-column-order composite PK (the pgoutput
// key-tuple alignment and PKBlob ordering verified during review).
func TestCompositeAndNonLeadingPK(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xfe, 0xed}
	schema := `
		CREATE TABLE public.t2 (v text, id bigint PRIMARY KEY);
		CREATE TABLE public.t3 (a int, b int, w text, PRIMARY KEY (b, a));`
	tables := []string{"public.t2", "public.t3"}

	src := openEngine(t, ctx, "syzy_csrc", 13, cluster, schema, tables)
	defer closeEngine(t, ctx, src)
	dst := openEngine(t, ctx, "syzy_cdst", 14, cluster, schema, tables)
	defer closeEngine(t, ctx, dst)

	appExec(t, "syzy_csrc", `INSERT INTO public.t2 VALUES ('hello', 7)`)
	appExec(t, "syzy_csrc", `INSERT INTO public.t3 VALUES (10, 20, 'x')`)
	appExec(t, "syzy_csrc", `UPDATE public.t3 SET w='y' WHERE b=20 AND a=10`)
	appExec(t, "syzy_csrc", `DELETE FROM public.t2 WHERE id=7`) // delete by non-leading PK

	applyAll(t, ctx, dst, captureAll(t, ctx, src))

	// t2 row 7 was inserted then deleted → gone on both. t3 (10,20) → w='y'.
	for _, db := range []string{"syzy_csrc", "syzy_cdst"} {
		assertScalar(t, db, `SELECT count(*) FROM public.t2`, 0)
		assertScalar(t, db, `SELECT count(*) FROM public.t3 WHERE a=10 AND b=20 AND w='y'`, 1)
	}
}

// TestToastImageMerge: an INSERT followed by an UPDATE-of-another-column in
// one txn, where a TOASTed column is unchanged (pgoutput elides it as 'u').
// The collapsed Insert must still carry the full TOASTed value — exercising
// both the per-column image merge and the decode-time byte copy (the value
// must survive buffer reuse across the two messages).
func TestToastImageMerge(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x70, 0xa5}
	schema := `CREATE TABLE public.kvt (id bigint PRIMARY KEY, big text, val text);
		ALTER TABLE public.kvt ALTER COLUMN big SET STORAGE EXTERNAL;` // external ⇒ unchanged update elides as 'u'
	tables := []string{"public.kvt"}

	src := openEngine(t, ctx, "syzy_tsrc", 21, cluster, schema, tables)
	defer closeEngine(t, ctx, src)
	dst := openEngine(t, ctx, "syzy_tdst", 22, cluster, schema, tables)
	defer closeEngine(t, ctx, dst)

	appTxn(t, "syzy_tsrc",
		`INSERT INTO public.kvt VALUES (1, repeat('a',4000), 'a')`,
		`UPDATE public.kvt SET val='b' WHERE id=1`) // big unchanged → TOAST-elided

	applyAll(t, ctx, dst, captureAll(t, ctx, src))
	assertScalar(t, "syzy_tdst", `SELECT count(*) FROM public.kvt WHERE id=1 AND big=repeat('a',4000) AND val='b'`, 1)
}

func assertScalar(t *testing.T, db, query string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	var got int
	if err := c.QueryRow(ctx, query).Scan(&got); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	if got != want {
		t.Errorf("%s: %q = %d, want %d", db, query, got, want)
	}
}

// openDurable opens an engine on an EXISTING database (no recreate) with a
// metadata store and per-commit checkpointing — for restart tests that must
// preserve the slot and replay the persisted recovery state.
func openDurable(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, cache *nodestate.Cache, meta *metadata.Store) *Engine {
	t.Helper()
	e, err := Open(ctx, Config{
		Name:            db,
		Origin:          origin,
		Cluster:         cluster,
		Cache:           cache,
		ConnURL:         dbURL(db),
		ReplConnURL:     replURL(db),
		Publication:     "syzy_pub",
		Slot:            slotName(db),
		OriginName:      originName(db),
		Tables:          []string{"public.kv"},
		Meta:            meta,
		CheckpointEvery: 1, // checkpoint every commit for deterministic restart
	})
	if err != nil {
		t.Fatalf("open durable %s: %v", db, err)
	}
	return e
}

// TestDurableRestart proves §2's recovery checkpoint: after a process
// restart (fresh Cache, reopened metadata store, surviving slot), the Cache
// rehydrates and capture resumes exactly at the persisted coverage LSN — so
// no already-captured txn is re-emitted and Seq continues without reuse.
func TestDurableRestart(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x22, 0x33}
	const db = "syzy_durable"
	const origin = crdt.Origin(41)

	createTestDB(t, ctx, db, schemaKV)
	metaPath := filepath.Join(t.TempDir(), "meta.db")

	// --- run 1: capture 5 commits, checkpointing each ---
	meta1, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta1 open: %v", err)
	}
	cache1 := nodestate.New(origin)
	e1 := openDurable(t, ctx, db, origin, cluster, cache1, meta1)
	for i := 0; i < 5; i++ {
		appExec(t, db, fmt.Sprintf(`INSERT INTO public.kv VALUES (%d,'v%d')`, i, i))
	}
	var got1 []*crdt.Changeset
	if err := e1.capt.run(ctx, collectProcess(e1, &got1), runOpts{stopAfter: 5}); err != nil {
		t.Fatalf("run1: %v", err)
	}
	if len(got1) != 5 {
		t.Fatalf("run1 captured %d changesets, want 5", len(got1))
	}
	if next := cache1.SenderNextSeq(origin); next != 6 {
		t.Fatalf("run1 senderNextSeq=%d, want 6", next)
	}
	_ = e1.Close()                        // keeps the slot (durable resume position)
	if err := meta1.Close(); err != nil { // simulate process exit
		t.Fatalf("meta1 close: %v", err)
	}

	// --- run 2: fresh Cache + reopened metadata, same db/slot ---
	meta2, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta2 open: %v", err)
	}
	cache2 := nodestate.New(origin)
	e2 := openDurable(t, ctx, db, origin, cluster, cache2, meta2)
	// Rehydration: Seq counter restored, so the lost in-memory state can't
	// reuse seqs 1..5.
	if next := cache2.SenderNextSeq(origin); next != 6 {
		t.Fatalf("after restart senderNextSeq=%d, want 6 (Cache not rehydrated)", next)
	}

	// A new local write after restart: capture must resume past the persisted
	// checkpoint (not re-emit the first 5) and allocate Seq 6.
	appExec(t, db, `INSERT INTO public.kv VALUES (5,'v5')`)
	var got2 []*crdt.Changeset
	if err := e2.capt.run(ctx, collectProcess(e2, &got2), runOpts{stopAfter: 1}); err != nil {
		t.Fatalf("run2: %v", err)
	}
	if len(got2) != 1 {
		t.Fatalf("run2 captured %d changesets, want 1 (must resume past checkpoint)", len(got2))
	}
	if got2[0].Dot.Seq != 6 {
		t.Fatalf("run2 Dot.Seq=%d, want 6 (Seq must continue, not reuse)", got2[0].Dot.Seq)
	}

	if err := e2.DropSlot(ctx); err != nil {
		t.Logf("drop slot: %v", err)
	}
	_ = e2.Close()
	_ = meta2.Close()
}

func dumpItems(t *testing.T, db string) map[int64]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("dump connect: %v", err)
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `SELECT id, name FROM public.items ORDER BY id`)
	if err != nil {
		t.Fatalf("dump query: %v", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = name
	}
	return out
}

// onlyKey returns the single key of a one-row map (fails otherwise).
func onlyKey(t *testing.T, m map[int64]string) int64 {
	t.Helper()
	if len(m) != 1 {
		t.Fatalf("expected exactly one row, got %d: %v", len(m), m)
	}
	for k := range m {
		return k
	}
	return 0
}

// TestAutoIncrementPartition proves §6 node-disjoint auto-increment: two nodes
// with the same bigserial-PK schema mint ids from disjoint, ordinal-keyed
// slices, so concurrent DEFAULT inserts never collide and replicate cleanly.
func TestAutoIncrementPartition(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x55, 0x66}
	const schema = `CREATE TABLE public.items (id bigserial PRIMARY KEY, name text)`

	openOrd := func(db string, origin crdt.Origin, ord uint16) *Engine {
		createTestDB(t, ctx, db, schema)
		e, err := Open(ctx, Config{
			Name:        pgDB(db),
			Origin:      origin,
			Cluster:     cluster,
			Cache:       nodestate.New(origin),
			ConnURL:     dbURL(db),
			ReplConnURL: replURL(db),
			Publication: "syzy_pub",
			Slot:        slotName(db),
			OriginName:  originName(db),
			Tables:      []string{"public.items"},
			NodeOrdinal: ord,
		})
		if err != nil {
			t.Fatalf("open %s: %v", db, err)
		}
		return e
	}

	a := openOrd("syzy_pa", 51, 1)
	defer closeEngine(t, ctx, a)
	b := openOrd("syzy_pb", 52, 2)
	defer closeEngine(t, ctx, b)

	// Each node inserts with a DEFAULT id from its own partitioned sequence.
	appExec(t, "syzy_pa", `INSERT INTO public.items (name) VALUES ('from-a')`)
	appExec(t, "syzy_pb", `INSERT INTO public.items (name) VALUES ('from-b')`)
	if err := partitionSequences(ctx, a.appl.conn, a.cat, 1, true); err != nil {
		t.Fatalf("recheck partitioned sequence: %v", err)
	}

	slice := int64(1) << idPartitionBits
	if idA := onlyKey(t, dumpItems(t, "syzy_pa")); idA != 1*slice {
		t.Fatalf("node A first id = %d, want %d", idA, 1*slice)
	}
	if idB := onlyKey(t, dumpItems(t, "syzy_pb")); idB != 2*slice {
		t.Fatalf("node B first id = %d, want %d", idB, 2*slice)
	}

	// Cross-replicate: both converge to the same two rows, no PK collision.
	applyAll(t, ctx, b, captureBacklog(t, ctx, a, 1))
	applyAll(t, ctx, a, captureBacklog(t, ctx, b, 1))

	da, db_ := dumpItems(t, "syzy_pa"), dumpItems(t, "syzy_pb")
	if !mapsEqual(da, db_) {
		t.Fatalf("did not converge:\n A=%v\n B=%v", da, db_)
	}
	if len(da) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(da), da)
	}
}

func TestPartitionRejectsUsedUnpartitionedSequence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_used_seq"
	createTestDB(t, ctx, db, `CREATE TABLE public.items (id bigserial PRIMARY KEY, name text);
		INSERT INTO public.items (name) VALUES ('existing')`)
	e, err := Open(ctx, Config{
		Name:        pgDB(db),
		Origin:      62,
		Cluster:     crdt.ClusterID{0x79},
		Cache:       nodestate.New(62),
		ConnURL:     dbURL(db),
		ReplConnURL: replURL(db),
		Publication: "syzy_pub",
		Slot:        slotName(db),
		OriginName:  originName(db),
		Tables:      []string{"public.items"},
		NodeOrdinal: 1,
	})
	if e != nil {
		closeEngine(t, ctx, e)
	}
	if err == nil || !strings.Contains(err.Error(), "used before node partitioning") {
		t.Fatalf("Open error = %v, want used-sequence rejection", err)
	}
}

// TestPartitionSkipsNarrowColumn guards the column-width check: a bigint
// sequence OWNED BY an int4 column is found by pg_get_serial_sequence, but
// partitioning it would mint ids (ordinal<<47) that overflow int4. The gate
// must skip it — so the sequence stays pristine and the first id is 1.
func TestPartitionSkipsNarrowColumn(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_narrow"
	schema := `CREATE TABLE public.mism (id integer PRIMARY KEY, name text);
		CREATE SEQUENCE public.mism_seq AS bigint OWNED BY public.mism.id;
		ALTER TABLE public.mism ALTER COLUMN id SET DEFAULT nextval('public.mism_seq')`
	createTestDB(t, ctx, db, schema)
	e, err := Open(ctx, Config{
		Name:        pgDB(db),
		Origin:      61,
		Cluster:     crdt.ClusterID{0x77, 0x88},
		Cache:       nodestate.New(61),
		ConnURL:     dbURL(db),
		ReplConnURL: replURL(db),
		Publication: "syzy_pub",
		Slot:        slotName(db),
		OriginName:  originName(db),
		Tables:      []string{"public.mism"},
		NodeOrdinal: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeEngine(t, ctx, e)

	// If the gate failed to skip, nextval would return 1<<47 and this INSERT
	// would error with "integer out of range".
	appExec(t, db, `INSERT INTO public.mism (name) VALUES ('x')`)
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	// Prove the gate is genuinely exercised: pg_get_serial_sequence must find
	// the bigint sequence, so it's the column-type gate — not a NULL lookup —
	// that prevents partitioning. Otherwise this test would pass trivially.
	var seq *string
	if err := c.QueryRow(ctx, `SELECT pg_get_serial_sequence('public.mism','id')`).Scan(&seq); err != nil {
		t.Fatalf("serial seq lookup: %v", err)
	}
	if seq == nil {
		t.Fatal("pg_get_serial_sequence returned NULL — test would not exercise the column gate")
	}
	var id int64
	if err := c.QueryRow(ctx, `SELECT id FROM public.mism`).Scan(&id); err != nil {
		t.Fatalf("select: %v", err)
	}
	if id != 1 {
		t.Fatalf("id = %d, want 1 (narrow column must be left unpartitioned)", id)
	}
}

var _ engine.Engine = (*Engine)(nil)

// describeChangesets renders records as "Insert(pk=…, cl=…)" so a failure says
// which rows a capture produced rather than only how many.
func describeChangesets(css []*crdt.Changeset) string {
	var b strings.Builder
	for _, cs := range css {
		for _, r := range cs.Records {
			if b.Len() > 0 {
				b.WriteString(", ")
			}
			switch v := r.(type) {
			case crdt.Insert:
				fmt.Fprintf(&b, "Insert(pk=%x, cl=%d)", v.PK, v.CL)
			case crdt.Update:
				fmt.Fprintf(&b, "Update(pk=%x, cl=%d)", v.PK, v.CL)
			case crdt.Delete:
				fmt.Fprintf(&b, "Delete(pk=%x, cl=%d)", v.PK, v.CL)
			default:
				fmt.Fprintf(&b, "%T", r)
			}
		}
	}
	return b.String()
}

// walHead returns the database's current WAL insert position, for pinning a
// capture's start so it cannot re-read commits made before this point.
func walHead(t *testing.T, ctx context.Context, db string) pglogrepl.LSN {
	t.Helper()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("wal-head connect: %v", err)
	}
	defer c.Close(ctx)
	var s string
	if err := c.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&s); err != nil {
		t.Fatalf("wal head: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("parse lsn %q: %v", s, err)
	}
	return lsn
}
