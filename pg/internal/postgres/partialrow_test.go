package postgres

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
)

// TestCellUpdateAppliesWithDomainRequiredColumn: a column can be required
// without saying so in its own attributes. A NOT NULL declared on a DOMAIN
// leaves the column's attnotnull false, so any routing rule that models "which
// columns are required" misses it, sends the partial image back through the
// upsert, and reinstates the 23502 on a row that exists — the original defect,
// just hidden one level down. Routing on whether the image is COMPLETE, rather
// than on whether the missing columns are required, cannot have this blind spot.
func TestCellUpdateAppliesWithDomainRequiredColumn(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xd6}
	schema := schemaKV + `;
CREATE DOMAIN public.req_int AS bigint NOT NULL;
CREATE TABLE public.dom (id bigint PRIMARY KEY, a text, b public.req_int);
ALTER TABLE public.dom REPLICA IDENTITY FULL`
	open := func(db string, origin crdt.Origin) *Engine {
		t.Helper()
		createTestDB(t, ctx, db, schema)
		cfg := baseTestConfig(db, origin, cluster)
		cfg.Tables = []string{"public.kv", "public.dom"}
		e, err := Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open %s: %v", db, err)
		}
		return e
	}
	const dbA, dbB = "syzy_domreq_a", "syzy_domreq_b"
	a := open(dbA, 94)
	defer closeEngine(t, ctx, a)
	b := open(dbB, 95)
	defer closeEngine(t, ctx, b)

	appExec(t, dbA, `INSERT INTO public.dom VALUES (1,'x',5)`)
	for _, cs := range captureAll(t, ctx, a) {
		if err := b.appl.Apply(ctx, cs); err != nil {
			t.Fatalf("B apply seed: %v", err)
		}
	}
	appExec(t, dbA, `UPDATE public.dom SET a = 'y' WHERE id = 1`)
	for _, cs := range captureAll(t, ctx, a) {
		if err := b.appl.Apply(ctx, cs); err != nil {
			t.Fatalf("B apply partial update over a domain-required column: %v", err)
		}
	}
	conn, err := pgx.Connect(ctx, dbURL(dbB))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var gotA string
	var gotB int64
	if err := conn.QueryRow(ctx, `SELECT a, b FROM public.dom WHERE id = 1`).Scan(&gotA, &gotB); err != nil {
		t.Fatalf("read dom on B: %v", err)
	}
	if gotA != "y" || gotB != 5 {
		t.Errorf("B dom = (%q, %d), want (\"y\", 5)", gotA, gotB)
	}
}

// TestCellUpdateOutrunningItsInsertKeepsItsValue: cross-origin delivery is not
// causally gated, so a cell-group update can arrive before the Insert that
// created its row. The row is not physically here yet, but the row clock the
// update writes claims it live.
//
// This is the case a partial image cannot write as a plain UPDATE — it matches
// no row. Reporting success there loses the write permanently, and worse than
// "the value arrives late": the update's per-column cell stamp outranks the
// Insert's, so when the Insert finally lands, ITS value for that column loses
// arbitration too and the column settles empty. Neither record's value survives,
// and nothing errored, so nothing retries.
//
// Failing loudly instead is fine — the changeset quarantines and the sweep
// re-applies it once the Insert has landed. What must not happen is a silent
// success that drops the value, so this drives the whole recovery path and
// asserts the end state.
//
// The SQLite broker resolves the same question by materializing the row from
// the partial image and accepting that a NOT NULL column with no default fails
// into that quarantine/retry machinery (internal/broker/apply_cell.go).
func TestCellUpdateOutrunningItsInsertKeepsItsValue(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xd4}
	// `a` is the column the update writes; `b` is NOT NULL with no default, so
	// the update's image cannot construct a whole row.
	schema := schemaKV + `;
CREATE TABLE public.nn (id bigint PRIMARY KEY, a text, b bigint NOT NULL);
ALTER TABLE public.nn REPLICA IDENTITY FULL`
	const dbA, dbB = "syzy_outrun_a", "syzy_outrun_b"

	createTestDB(t, ctx, dbA, schema)
	cfgA := baseTestConfig(dbA, 92, cluster)
	cfgA.Tables = []string{"public.kv", "public.nn"}
	a, err := Open(ctx, cfgA)
	if err != nil {
		t.Fatalf("open %s: %v", dbA, err)
	}
	defer closeEngine(t, ctx, a)

	// B needs a metadata store: quarantine is what makes a deterministic apply
	// failure recoverable, and it persists there.
	createTestDB(t, ctx, dbB, schema)
	meta, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("meta open: %v", err)
	}
	defer meta.Close()
	cfgB := baseTestConfig(dbB, 93, cluster)
	cfgB.Tables = []string{"public.kv", "public.nn"}
	cfgB.Cache = nodestate.New(93)
	cfgB.Meta = meta
	b, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatalf("open %s: %v", dbB, err)
	}
	defer closeEngine(t, ctx, b)

	appExec(t, dbA, `INSERT INTO public.nn VALUES (1,'x',5)`)
	ins := captureAll(t, ctx, a)
	appExec(t, dbA, `UPDATE public.nn SET a = 'y' WHERE id = 1`)
	upd := captureAll(t, ctx, a)
	if len(ins) != 1 || len(upd) != 1 {
		t.Fatalf("captured %d insert / %d update changesets, want 1 each", len(ins), len(upd))
	}

	// Out of order on purpose: the update first, its Insert second. A
	// deterministic failure is routed to quarantine exactly as applyRemote does
	// (called directly here — applyRemote's WAL catch-up gate needs the capture
	// goroutine, which these engines do not run).
	if err := b.appl.Apply(ctx, upd[0]); err != nil {
		if !isDeterministicApplyErr(err) {
			t.Fatalf("B apply update ahead of its insert: %v", err)
		}
		if !b.orch.quarantineApplyFailure(upd[0], err) {
			t.Fatalf("deterministic failure was not quarantined: %v", err)
		}
		t.Logf("update ahead of its insert failed deterministically and quarantined: %v", err)
	}
	if err := b.appl.Apply(ctx, ins[0]); err != nil {
		t.Fatalf("B apply insert: %v", err)
	}
	b.orch.retryQuarantined(ctx)

	conn, err := pgx.Connect(ctx, dbURL(dbB))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var gotA *string
	var gotB int64
	if err := conn.QueryRow(ctx, `SELECT a, b FROM public.nn WHERE id = 1`).Scan(&gotA, &gotB); err != nil {
		t.Fatalf("read nn on B (the row must exist once its insert arrived): %v", err)
	}
	got := "NULL"
	if gotA != nil {
		got = *gotA
	}
	if got != "y" || gotB != 5 {
		t.Errorf("B nn = (%s, %d), want (\"y\", 5) — the update that outran its insert was lost, "+
			"and its cell stamp then beat the insert's value for that column", got, gotB)
	}
}

// TestApplyAddColumnBindsLocalTruth: an auto-increment column ships as its
// CREATE-shaped pseudo-type with an EMPTY Default — the captured nextval()
// names the originator's sequence and must not cross the wire. The follower's
// ALTER therefore creates a column whose real shape (its own nextval default,
// or attidentity for an IDENTITY column) is not the shape the op declared.
//
// Binding the op's fields instead of the column Postgres actually created
// leaves this node's catalog describing something that does not exist, and
// disagreeing with the originator about the same column — which then feeds
// every decision that reads column metadata, apply rendering included.
func TestApplyAddColumnBindsLocalTruth(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_addcol_truth"
	e := openEngine(t, ctx, db, 96, crdt.ClusterID{0xd7},
		schemaKV+`; CREATE TABLE public.u (id bigint PRIMARY KEY, a text)`,
		[]string{"public.kv", "public.u"})
	defer closeEngine(t, ctx, e)

	ti := e.cat.byID[deriveTableID("public", "u")]
	if ti == nil {
		t.Fatal("public.u not bound in the catalog")
	}
	for _, tc := range []struct {
		name, colName, opType string
		wantDef               string // substring the local default must contain
		wantIdentity          uint8
	}{
		{"serial", "s", "bigserial", "nextval(", 0},
		{"identity", "g", "bigint GENERATED ALWAYS AS IDENTITY", "", 'a'},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := crdt.CatalogOp{
				Kind:    crdt.OpAddColumn,
				TableID: ti.tid,
				Columns: []crdt.CatalogColumn{{
					ID: crdt.ColumnID{0xa0, byte(len(tc.name))}, Name: tc.colName,
					Type: tc.opType, // Default deliberately empty, as the wire carries it
				}},
			}
			if err := applyAddColumn(ctx, e.appl.conn, e.cat, op); err != nil {
				t.Fatalf("applyAddColumn %s: %v", tc.opType, err)
			}
			ci := ti.byName[tc.colName]
			if ci == nil {
				t.Fatalf("column %q not bound after apply", tc.colName)
			}
			// What Postgres actually created, which the binding must match.
			var pgDef string
			var pgIdentity string
			if err := e.appl.conn.QueryRow(ctx, `
				SELECT COALESCE(pg_get_expr(d.adbin, d.adrelid), ''), a.attidentity::text
				FROM pg_attribute a
				LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
				WHERE a.attrelid = $1 AND a.attname = $2`, ti.oid, tc.colName).Scan(&pgDef, &pgIdentity); err != nil {
				t.Fatalf("read local column shape: %v", err)
			}
			if ci.def != pgDef {
				t.Errorf("bound def = %q, but Postgres created %q", ci.def, pgDef)
			}
			if tc.wantDef != "" && !strings.Contains(pgDef, tc.wantDef) {
				t.Fatalf("precondition: local default %q does not contain %q", pgDef, tc.wantDef)
			}
			if ci.identity != tc.wantIdentity {
				t.Errorf("bound identity = %q, want %q (Postgres says %q)", ci.identity, tc.wantIdentity, pgIdentity)
			}
		})
	}
}
