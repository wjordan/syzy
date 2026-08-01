package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/schemalog"
)

// Cell clock group + counter columns (§8). The opt-in is REPLICA IDENTITY
// FULL; a syzy_counter column implies it.

func intVal(col crdt.ColumnID, n int64) crdt.ColValue {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	return crdt.ColValue{Column: col, TypeTag: crdt.ColInt, Bytes: b[:]}
}

func textVal(col crdt.ColumnID, s string) crdt.ColValue {
	return crdt.ColValue{Column: col, TypeTag: crdt.ColText, Bytes: []byte(s)}
}

// cellFixture is a three-column table: PK id, register val, counter n.
func cellFixture() (*tableInfo, *colInfo, *colInfo, *colInfo) {
	id := &colInfo{name: "id", typeName: "bigint", cid: crdt.ColumnID{1}, isPK: true, attnum: 1}
	val := &colInfo{name: "val", typeName: "text", cid: crdt.ColumnID{2}, attnum: 2}
	n := &colInfo{name: "n", typeName: counterTypeName, cid: crdt.ColumnID{3}, attnum: 3, notNull: true, counter: true}
	ti := &tableInfo{
		schema: "public", name: "cell", cols: []*colInfo{id, val, n}, pk: []*colInfo{id},
		byName:     map[string]*colInfo{"id": id, "val": val, "n": n},
		clockGroup: metadata.ClockGroupCell,
	}
	return ti, id, val, n
}

// TestCellChanged: the payload unit is the diff, counter columns ship the
// signed contribution, and a column the old tuple did not carry (elided
// unchanged TOAST) is treated as changed rather than assumed equal.
func TestCellChanged(t *testing.T) {
	ti, id, val, n := cellFixture()

	old := []crdt.ColValue{intVal(id.cid, 1), textVal(val.cid, "a"), intVal(n.cid, 5)}
	new := []crdt.ColValue{intVal(id.cid, 1), textVal(val.cid, "b"), intVal(n.cid, 5)}
	got, err := cellChanged(ti, old, new)
	if err != nil {
		t.Fatalf("cellChanged: %v", err)
	}
	if len(got) != 1 || got[0].Column != val.cid || string(got[0].Bytes) != "b" {
		t.Fatalf("want only val changed, got %+v", got)
	}

	// Counter column: NEW − OLD as a contribution, not the absolute value.
	new = []crdt.ColValue{intVal(id.cid, 1), textVal(val.cid, "a"), intVal(n.cid, 12)}
	got, err = cellChanged(ti, old, new)
	if err != nil {
		t.Fatalf("cellChanged counter: %v", err)
	}
	if len(got) != 1 || got[0].Column != n.cid || got[0].Format != crdt.FormatDelta {
		t.Fatalf("want one counter contribution, got %+v", got)
	}
	if d := int64(binary.BigEndian.Uint64(got[0].Bytes)); d != 7 {
		t.Errorf("contribution = %d, want 7", d)
	}

	// PK columns never ride the diff, and an identical row yields nothing.
	if got, err = cellChanged(ti, old, old); err != nil || len(got) != 0 {
		t.Fatalf("unchanged row: got %+v, %v", got, err)
	}

	// Old value elided (unchanged TOAST): carried, since equality is unprovable.
	got, err = cellChanged(ti, []crdt.ColValue{intVal(id.cid, 1), intVal(n.cid, 5)},
		[]crdt.ColValue{intVal(id.cid, 1), textVal(val.cid, "a"), intVal(n.cid, 5)})
	if err != nil {
		t.Fatalf("cellChanged toast: %v", err)
	}
	if len(got) != 1 || got[0].Column != val.cid {
		t.Fatalf("want the un-diffable column carried, got %+v", got)
	}
}

// TestCounterContributionSums: the apply UPSERT adds a contribution onto the
// committed cell instead of overwriting it, and leaves register columns as
// plain assignments.
func TestCounterContributionSums(t *testing.T) {
	ti, id, val, n := cellFixture()
	delta, err := crdt.CounterDelta(intVal(n.cid, 5), intVal(n.cid, 8))
	if err != nil {
		t.Fatalf("CounterDelta: %v", err)
	}
	sql := upsertSQL(ti, []crdt.ColValue{intVal(id.cid, 1), textVal(val.cid, "x"), delta}).sql
	if !strings.Contains(sql, `"n" = "syzy_target"."n" + excluded."n"`) {
		t.Errorf("counter column is not summed:\n%s", sql)
	}
	if !strings.Contains(sql, `"val" = excluded."val"`) {
		t.Errorf("register column is not assigned:\n%s", sql)
	}
	if !strings.Contains(sql, `AS "syzy_target"`) {
		t.Errorf("conflict target is not aliased:\n%s", sql)
	}
}

// TestValidateCounterValues rejects wire payloads that would bypass stamp
// arbitration or run meaningless arithmetic.
func TestValidateCounterValues(t *testing.T) {
	ti, _, val, n := cellFixture()

	// A contribution on a register column would skip arbitration entirely.
	bogus := textVal(val.cid, "x")
	bogus.Format = crdt.FormatDelta
	if err := validateCounterValues(ti, []crdt.ColValue{bogus}); !errors.Is(err, errCounterApply) {
		t.Errorf("register carrying a contribution: err = %v, want errCounterApply", err)
	}
	// An absolute value on a counter column would stomp concurrent sums.
	if err := validateCounterValues(ti, []crdt.ColValue{intVal(n.cid, 3)}); !errors.Is(err, errCounterApply) {
		t.Errorf("counter carrying an absolute value: err = %v, want errCounterApply", err)
	}
	good, _ := crdt.CounterDelta(intVal(n.cid, 1), intVal(n.cid, 2))
	if err := validateCounterValues(ti, []crdt.ColValue{good, textVal(val.cid, "ok")}); err != nil {
		t.Errorf("valid payload rejected: %v", err)
	}
}

const schemaCell = schemaKV + `;
CREATE TABLE public.doc (id bigint PRIMARY KEY, title text, body text);
ALTER TABLE public.doc REPLICA IDENTITY FULL`

// TestCaptureShipsCellDiff: on a cell-group table capture ships only the
// columns the transaction changed, so a concurrent write to another column is
// not stomped. A row-group table still ships the whole image.
func TestCaptureShipsCellDiff(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openEngine(t, ctx, "syzy_celldiff", 71, crdt.ClusterID{0xc1}, schemaCell,
		[]string{"public.kv", "public.doc"})
	defer closeEngine(t, ctx, e)

	if !e.cat.byID[deriveTableID("public", "doc")].cellGroup() {
		t.Fatal("REPLICA IDENTITY FULL did not put public.doc in the cell clock group")
	}
	appExec(t, "syzy_celldiff", `INSERT INTO public.doc VALUES (1,'t','b')`)
	appExec(t, "syzy_celldiff", `UPDATE public.doc SET body = 'b2' WHERE id = 1`)
	// Same value re-written: no column actually changed, so nothing replicates.
	appExec(t, "syzy_celldiff", `UPDATE public.doc SET body = 'b2' WHERE id = 1`)
	css := captureAll(t, ctx, e)
	if len(css) != 2 {
		t.Fatalf("want 2 changesets (insert, real update), got %d", len(css))
	}
	upd, ok := css[1].Records[0].(crdt.Update)
	if !ok {
		t.Fatalf("second record is %T, want Update", css[1].Records[0])
	}
	if len(upd.Changed) != 1 {
		t.Fatalf("cell update carries %d columns, want 1 (the changed one): %+v", len(upd.Changed), upd.Changed)
	}
	if want := deriveColumnID("public", "doc", "body"); upd.Changed[0].Column != want {
		t.Errorf("cell update carries column %x, want body", upd.Changed[0].Column)
	}
}

// TestCellApplyMergesDisjointColumns: two concurrent updates to different
// columns of one row both survive, and a same-column loser does not.
func TestCellApplyMergesDisjointColumns(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xc2}
	a := openEngine(t, ctx, "syzy_cella", 72, cluster, schemaCell, []string{"public.kv", "public.doc"})
	defer closeEngine(t, ctx, a)
	b := openEngine(t, ctx, "syzy_cellb", 73, cluster, schemaCell, []string{"public.kv", "public.doc"})
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_cella", `INSERT INTO public.doc VALUES (1,'t','b')`)
	seed := captureAll(t, ctx, a)
	if err := b.appl.Apply(ctx, seed[0]); err != nil {
		t.Fatalf("B apply seed: %v", err)
	}
	// Concurrent, disjoint: A writes title, B writes body.
	appExec(t, "syzy_cella", `UPDATE public.doc SET title = 'A' WHERE id = 1`)
	appExec(t, "syzy_cellb", `UPDATE public.doc SET body = 'B' WHERE id = 1`)
	csA := captureAll(t, ctx, a)
	csB := captureAll(t, ctx, b)
	if err := b.appl.Apply(ctx, csA[0]); err != nil {
		t.Fatalf("B apply A's update: %v", err)
	}
	if err := a.appl.Apply(ctx, csB[0]); err != nil {
		t.Fatalf("A apply B's update: %v", err)
	}
	for _, db := range []string{"syzy_cella", "syzy_cellb"} {
		title, body := docRow(t, db, 1)
		if title != "A" || body != "B" {
			t.Errorf("%s: doc = (%q,%q), want (A,B) — disjoint-column writes must merge", db, title, body)
		}
	}
}

// TestCounterApplySumsAndIsExactlyOnce: concurrent increments accumulate on
// both nodes, and a re-delivered counter changeset does not double-count.
func TestCounterApplySumsAndIsExactlyOnce(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xc3}
	schema := schemaKV + `;
CREATE TABLE public.hits (id bigint PRIMARY KEY, label text, n bigint NOT NULL DEFAULT 0);
ALTER TABLE public.hits REPLICA IDENTITY FULL`
	a := openCounterEngine(t, ctx, "syzy_cnta", 74, cluster, schema)
	defer closeEngine(t, ctx, a)
	b := openCounterEngine(t, ctx, "syzy_cntb", 75, cluster, schema)
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_cnta", `INSERT INTO public.hits VALUES (1,'x',0)`)
	seed := captureAll(t, ctx, a)
	if err := b.appl.Apply(ctx, seed[0]); err != nil {
		t.Fatalf("B apply seed: %v", err)
	}
	// Concurrent increments: +3 on A, +4 on B. Both must land everywhere.
	appExec(t, "syzy_cnta", `UPDATE public.hits SET n = n + 3 WHERE id = 1`)
	appExec(t, "syzy_cntb", `UPDATE public.hits SET n = n + 4 WHERE id = 1`)
	csA := captureAll(t, ctx, a)
	csB := captureAll(t, ctx, b)
	if got := csA[0].Records[0].(crdt.Update).Changed[0].Format; got != crdt.FormatDelta {
		t.Fatalf("counter update ships format %d, want a contribution", got)
	}
	if err := b.appl.Apply(ctx, csA[0]); err != nil {
		t.Fatalf("B apply A's increment: %v", err)
	}
	if err := a.appl.Apply(ctx, csB[0]); err != nil {
		t.Fatalf("A apply B's increment: %v", err)
	}
	for _, db := range []string{"syzy_cnta", "syzy_cntb"} {
		if n := counterValue(t, db, 1); n != 7 {
			t.Errorf("%s: hits.n = %d, want 7 (3+4 both summed)", db, n)
		}
	}
	// Re-delivery after the frontier was lost (a crash between the apply
	// transaction and the sidecar checkpoint): the applied marker strips the
	// contribution and only the idempotent remainder re-applies.
	if err := b.appl.apply(ctx, csA[0], true); err != nil {
		t.Fatalf("B re-apply: %v", err)
	}
	if n := counterValue(t, "syzy_cntb", 1); n != 7 {
		t.Errorf("re-delivered counter changeset double-counted: n = %d, want 7", n)
	}
}

// TestCellUpdateAppliesWithRequiredColumnAbsent: a cell-group update carries
// only the columns its transaction changed, so on a table with a NOT NULL column
// that has no DEFAULT the image cannot build a whole row. Rendering that as
// INSERT ... ON CONFLICT fails 23502 even though the row exists and the missing
// column is not being written — Postgres checks the proposed tuple before it
// detects the conflict — which quarantines an ordinary update on every receiver.
func TestCellUpdateAppliesWithRequiredColumnAbsent(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xd1}
	schema := schemaKV + `;
CREATE TABLE public.nn (id bigint PRIMARY KEY, a text, b bigint NOT NULL);
ALTER TABLE public.nn REPLICA IDENTITY FULL`
	open := func(db string, origin crdt.Origin) *Engine {
		t.Helper()
		createTestDB(t, ctx, db, schema)
		cfg := baseTestConfig(db, origin, cluster)
		cfg.Tables = []string{"public.kv", "public.nn"}
		e, err := Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open %s: %v", db, err)
		}
		return e
	}
	const dbA, dbB = "syzy_reqcol_a", "syzy_reqcol_b"
	a := open(dbA, 90)
	defer closeEngine(t, ctx, a)
	b := open(dbB, 91)
	defer closeEngine(t, ctx, b)

	appExec(t, dbA, `INSERT INTO public.nn VALUES (1,'x',5)`)
	for _, cs := range captureAll(t, ctx, a) {
		if err := b.appl.Apply(ctx, cs); err != nil {
			t.Fatalf("B apply seed: %v", err)
		}
	}
	appExec(t, dbA, `UPDATE public.nn SET a = 'y' WHERE id = 1`)
	for _, cs := range captureAll(t, ctx, a) {
		if err := b.appl.Apply(ctx, cs); err != nil {
			t.Fatalf("B apply partial cell update: %v", err)
		}
	}
	conn, err := pgx.Connect(ctx, dbURL(dbB))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var gotA string
	var gotB int64
	if err := conn.QueryRow(ctx, `SELECT a, b FROM public.nn WHERE id = 1`).Scan(&gotA, &gotB); err != nil {
		t.Fatalf("read nn on B: %v", err)
	}
	if gotA != "y" || gotB != 5 {
		t.Errorf("B nn = (%q, %d), want (\"y\", 5) — the update lands and the untouched column is left alone", gotA, gotB)
	}
}

// TestCertifiedCounterInsertRecreatesDeletedRow covers the redelivery the
// applied marker certifies when the row it recreates is no longer there.
//
// The marker is committed in the apply transaction; the sidecar's row clock is
// persisted separately, so a crash between the two leaves a node that has the
// certificate but not the clock — the changeset redelivers and arbitration
// judges it against pre-apply state, so it reaches DML instead of being skipped.
// If a local delete removed the row in the meantime and has not folded yet, that
// DML has to recreate it. Counter columns are NOT NULL, so an image with them
// stripped out cannot: the INSERT fails outright, or, where the column has a
// DEFAULT, quietly resurrects the row at zero.
//
// What it must not do is count the contribution again — the marker says it
// already landed — so a row that IS still present keeps its accumulated total.
func TestCertifiedCounterInsertRecreatesDeletedRow(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xc7}
	// No DEFAULT on n: the stripped INSERT then fails loudly rather than
	// resurrecting the row at zero.
	schema := schemaKV + `;
CREATE TABLE public.hits (id bigint PRIMARY KEY, label text, n bigint NOT NULL);
ALTER TABLE public.hits REPLICA IDENTITY FULL`
	const dbA, dbB = "syzy_cntcert_a", "syzy_cntcert_b"
	a := openCounterEngine(t, ctx, dbA, 82, cluster, schema)
	defer closeEngine(t, ctx, a)
	b := openCounterEngine(t, ctx, dbB, 83, cluster, schema)

	appExec(t, dbA, `INSERT INTO public.hits VALUES (1,'x',7)`)
	cs := captureAll(t, ctx, a)
	if len(cs) != 1 {
		t.Fatalf("captured %d changesets, want 1", len(cs))
	}
	if err := b.appl.Apply(ctx, cs[0]); err != nil {
		t.Fatalf("B apply: %v", err)
	}
	if n := counterValue(t, dbB, 1); n != 7 {
		t.Fatalf("B hits.n = %d after first apply, want 7", n)
	}

	// The crash: row and marker are durable, the row clock is not. A fresh
	// Cache on the same database is exactly that state.
	closeEngine(t, ctx, b)
	cfg := baseTestConfig(dbB, 83, cluster)
	cfg.Tables = []string{"public.kv", "public.hits"}
	b2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen B: %v", err)
	}
	defer closeEngine(t, ctx, b2)

	// Move the live cell off the image's opening value, so "keeps the total it
	// accumulated" and "re-opens at a value already counted" are distinguishable.
	appExec(t, dbB, `UPDATE public.hits SET n = n + 4 WHERE id = 1`)

	// Redelivered onto the row that is still there: certified, so the total it
	// already holds stands.
	if err := b2.appl.apply(ctx, cs[0], true); err != nil {
		t.Fatalf("B re-apply onto the live row: %v", err)
	}
	if n := counterValue(t, dbB, 1); n != 11 {
		t.Fatalf("B hits.n = %d after certified re-apply, want 11 (the contribution was already counted)", n)
	}

	// A local delete that has not been folded yet: gone from the table, and the
	// row clock reopened above has never seen it.
	appExec(t, dbB, `DELETE FROM public.hits WHERE id = 1`)
	if err := b2.appl.apply(ctx, cs[0], true); err != nil {
		t.Fatalf("B re-apply onto the deleted row: %v", err)
	}
	if n := counterValue(t, dbB, 1); n != 7 {
		t.Errorf("B hits.n = %d, want 7 — the recreated row must carry the generation's opening value", n)
	}
}

// openCounterEngine opens an engine whose hits.n column is a declared counter.
// The declaration is a DDL-time act, so DDL support is installed first and the
// column is altered into the domain before the engine introspects it.
func openCounterEngine(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, schema string) *Engine {
	t.Helper()
	createTestDB(t, ctx, db, schema)
	appExec(t, db, `DO $$ BEGIN
	    IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
	                   WHERE n.nspname='public' AND t.typname='syzy_counter') THEN
	        CREATE DOMAIN public.syzy_counter AS bigint;
	    END IF;
	END $$`)
	appExec(t, db, `ALTER TABLE public.hits ALTER COLUMN n TYPE public.syzy_counter`)
	cfg := baseTestConfig(db, origin, cluster)
	cfg.Tables = []string{"public.kv", "public.hits"}
	e, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	ti := e.cat.byID[deriveTableID("public", "hits")]
	if !ti.hasCounters() || !ti.cellGroup() {
		t.Fatalf("hits did not bind as a counter table (counters=%v, cell=%v)", ti.hasCounters(), ti.cellGroup())
	}
	return e
}

func docRow(t *testing.T, db string, id int64) (title, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	if err := c.QueryRow(ctx, `SELECT title, body FROM public.doc WHERE id = $1`, id).Scan(&title, &body); err != nil {
		t.Fatalf("read doc %d on %s: %v", id, db, err)
	}
	return title, body
}

func counterValue(t *testing.T, db string, id int64) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	var n int64
	if err := c.QueryRow(ctx, `SELECT n FROM public.hits WHERE id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("read hits %d on %s: %v", id, db, err)
	}
	return n
}

// TestAdmissionRejectsBadCounter: the counter rules are enforced pre-commit, so
// a schema that could not merge never reaches the cluster.
func TestAdmissionRejectsBadCounter(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngineLog(t, ctx, "syzy_cntadm", 76, crdt.ClusterID{0xc4}, 1, schemalog.NewLocal())
	defer closeEngine(t, ctx, e)

	for _, tc := range []struct{ name, sql, want string }{
		{"nullable", `CREATE TABLE public.c1 (id bigint PRIMARY KEY, n public.syzy_counter)`, "must be NOT NULL"},
		{"in the pk", `CREATE TABLE public.c2 (n public.syzy_counter NOT NULL, PRIMARY KEY (n))`, "cannot be part of the PRIMARY KEY"},
		{"generated", `CREATE TABLE public.c3 (id bigint PRIMARY KEY, m bigint NOT NULL, n public.syzy_counter NOT NULL GENERATED ALWAYS AS (m*2) STORED)`, "cannot be GENERATED"},
		{"unique", `CREATE TABLE public.c4 (id bigint PRIMARY KEY, n public.syzy_counter NOT NULL UNIQUE)`, "cannot be part of a UNIQUE key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := appExecErr(t, "syzy_cntadm", tc.sql)
			if err == nil {
				t.Fatalf("%s was admitted; want a pre-commit rejection", tc.sql)
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "0A000" {
				t.Fatalf("error = %v, want SQLSTATE 0A000", err)
			}
			if !strings.Contains(pgErr.Message, tc.want) {
				t.Errorf("message %q does not mention %q", pgErr.Message, tc.want)
			}
		})
	}

	// A counter column declares the cell clock group for its table: the gate
	// installs REPLICA IDENTITY FULL in the same transaction.
	appExec(t, "syzy_cntadm", `CREATE TABLE public.ok (id bigint PRIMARY KEY, n public.syzy_counter NOT NULL DEFAULT 0)`)
	if ri := replIdentOf(t, "syzy_cntadm", "ok"); ri != "f" {
		t.Fatalf("counter table replica identity = %q, want f (FULL)", ri)
	}
	// And it cannot be taken back out from under the counters.
	err := appExecErr(t, "syzy_cntadm", `ALTER TABLE public.ok REPLICA IDENTITY DEFAULT`)
	if err == nil || !strings.Contains(err.Error(), "require REPLICA IDENTITY FULL") {
		t.Fatalf("leaving the cell group with counters: err = %v, want a rejection", err)
	}
}

func replIdentOf(t *testing.T, db, table string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	var ri string
	if err := c.QueryRow(ctx, `SELECT relreplident::text FROM pg_class WHERE oid = format('public.%I',$1::text)::regclass`,
		table).Scan(&ri); err != nil {
		t.Fatalf("read replica identity of %s: %v", table, err)
	}
	return ri
}

// TestBuildCatalogOpClockGroup: the clock group rides the schema log as its own
// op — on the CREATE that declared it, and on a later flip.
func TestBuildCatalogOpClockGroup(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_cgops", 77, crdt.ClusterID{0xc5})
	defer closeEngine(t, ctx, e)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, "syzy_cgops", &ops, &buildErr)

	appExec(t, "syzy_cgops", `CREATE TABLE public.d (id bigint PRIMARY KEY, a text, n public.syzy_counter NOT NULL DEFAULT 0)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_cgops", `CREATE TABLE public.p (id bigint PRIMARY KEY, a text)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_cgops", `ALTER TABLE public.p REPLICA IDENTITY FULL`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	if buildErr != nil {
		t.Fatalf("unexpected build error: %v", buildErr)
	}

	var create *crdt.CatalogOp
	var groups []crdt.CatalogOp
	for i := range ops {
		switch ops[i].Kind {
		case crdt.OpCreateTable:
			if ops[i].TableName == "d" {
				create = &ops[i]
			}
		case crdt.OpSetClockGroup:
			groups = append(groups, ops[i])
		}
	}
	if create == nil {
		t.Fatalf("no create op for the counter table: %+v", ops)
	}
	// One for the counter table's declaration, one for the later flip.
	if len(groups) != 2 {
		t.Fatalf("built %d clock-group ops, want 2: %+v", len(groups), ops)
	}
	for _, g := range groups {
		if g.ClockGroup != metadata.ClockGroupCell {
			t.Errorf("clock group op = %q, want cell", g.ClockGroup)
		}
	}
	if groups[0].TableID != create.TableID {
		t.Errorf("the counter table's clock-group op names a different table")
	}
	var counters int
	for _, c := range create.Columns {
		if c.ClockGroup == metadata.ClockGroupCounter {
			counters++
			if c.Type != counterTypeName {
				t.Errorf("counter column type = %q, want %q", c.Type, counterTypeName)
			}
		}
	}
	if counters != 1 {
		t.Errorf("create op carries %d counter columns, want 1", counters)
	}
}

// TestLiveCellConvergence drives two live sidecars: the cell clock group and a
// counter column are declared on A, replicate as schema, and then concurrent
// per-column writes on both nodes converge — registers merged per column,
// counters summed.
func TestLiveCellConvergence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	cluster := crdt.ClusterID{0xc6}
	a := openDDLEngineLog(t, ctx, "syzy_lcell_a", 78, cluster, 1, log)
	defer closeEngine(t, ctx, a)
	b := openDDLEngineLog(t, ctx, "syzy_lcell_b", 79, cluster, 2, log)
	defer closeEngine(t, ctx, b)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	aInbox := make(chan *crdt.Changeset, 256)
	bInbox := make(chan *crdt.Changeset, 256)
	run := func(node *Engine, inbox <-chan *crdt.Changeset, peer chan<- *crdt.Changeset) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			broadcast := func(ctx context.Context, cs *crdt.Changeset) error {
				select {
				case peer <- cs:
				case <-ctx.Done():
				}
				return nil
			}
			if err := node.Run(runCtx, inbox, broadcast); err != nil && runCtx.Err() == nil {
				t.Errorf("%s orchestrator: %v", node.cfg.Name, err)
			}
		}()
	}
	run(a, aInbox, bInbox)
	run(b, bInbox, aInbox)
	defer func() { cancel(); wg.Wait() }()

	appExec(t, "syzy_lcell_a", `CREATE TABLE public.page (id bigint PRIMARY KEY, title text, body text, views public.syzy_counter NOT NULL DEFAULT 0)`)
	appExec(t, "syzy_lcell_a", `INSERT INTO public.page VALUES (1,'t','b',0)`)
	if !waitPageRow(t, "syzy_lcell_b", "t/b/0", 20*time.Second) {
		t.Fatalf("B never saw the created table (page = %s)", pageRow(t, "syzy_lcell_b"))
	}
	// The declaration carried the cell clock group across.
	if ri := replIdentOf(t, "syzy_lcell_b", "page"); ri != "f" {
		t.Fatalf("B's page replica identity = %q, want f (FULL)", ri)
	}

	// Concurrent per-column writes with no quiescence between them.
	appExec(t, "syzy_lcell_a", `UPDATE public.page SET title = 'A', views = views + 2 WHERE id = 1`)
	appExec(t, "syzy_lcell_b", `UPDATE public.page SET body = 'B', views = views + 5 WHERE id = 1`)

	deadline := time.Now().Add(30 * time.Second)
	var lastA, lastB string
	for time.Now().Before(deadline) {
		lastA, lastB = pageRow(t, "syzy_lcell_a"), pageRow(t, "syzy_lcell_b")
		if lastA == "A/B/7" && lastB == "A/B/7" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("did not converge on merged columns + summed counter: A=%s B=%s, want A/B/7 on both", lastA, lastB)
}

// waitPageRow polls until public.page's row renders as want.
func waitPageRow(t *testing.T, db, want string, within time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if pageRow(t, db) == want {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func pageRow(t *testing.T, db string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	var title, body string
	var views int64
	if err := c.QueryRow(ctx, `SELECT title, body, views FROM public.page WHERE id = 1`).Scan(&title, &body, &views); err != nil {
		return "<none>"
	}
	return title + "/" + body + "/" + strconv.FormatInt(views, 10)
}
