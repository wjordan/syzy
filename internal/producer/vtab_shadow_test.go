package producer

// Tests for virtual-table shadow-table handling in change capture and
// DDL admission. The suppression mechanism under test is indirect:
//
//   - DDL: SQLite delivers trace_v2 STMT callbacks for nested
//     statements (db->nVdbeExec > 1, i.e. the shadow CREATE TABLEs run
//     by a vtab module's xCreate) with a "-- " comment prefix.
//     classifyDDL parses that as nothing -> ddlNone -> local-only.
//   - DML: shadow-table writes fire preupdate at depth 0 and DO land
//     in the touch journal; MetaSink.buildRecordEvidence drops them at
//     drain time because captureTable finds no catalog entry.
//
// Neither path is the stmt-pointer reentrancy tracking sqlite/docs/DDL.md
// describes, so these tests pin the actual behavior.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// queryInt runs a single-row single-column integer query.
func queryInt(t *testing.T, app *sqlitebridge.Conn, sql string) int64 {
	t.Helper()
	stmt, _, err := app.Prepare(sql)
	if err != nil {
		t.Fatalf("prepare %q: %v", sql, err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("step %q: %v", sql, err)
	}
	if !hasRow {
		t.Fatalf("no row for %q", sql)
	}
	return stmt.ColumnInt64(0)
}

// createFTS5 creates an fts5 vtab, skipping the test when the bundled
// SQLite lacks the module.
func createFTS5(t *testing.T, f *ddlFixture, sql string) {
	t.Helper()
	if err := f.app.Exec(sql); err != nil {
		if strings.Contains(err.Error(), "no such module") {
			t.Skipf("fts5 not available in this build: %v", err)
		}
		t.Fatalf("CREATE VIRTUAL TABLE: %v", err)
	}
}

// TestDDL_VirtualTableShadowTablesNotCapturedOrAdmitted exercises the
// full producer path: CREATE VIRTUAL TABLE admits exactly one opaque
// schema-log event (the shadow CREATE TABLEs run by fts5's xCreate are
// not admitted, not rejected, and get no catalog entries), and DML into
// the vtab replicates nothing even though the shadow-table writes flow
// through the touch journal.
func TestDDL_VirtualTableShadowTablesNotCapturedOrAdmitted(t *testing.T) {
	t.Parallel()
	f := newDDLFixture(t)

	var mu sync.Mutex
	var payloads [][]byte
	f.prod.OnEncoded(func(p []byte) {
		mu.Lock()
		payloads = append(payloads, append([]byte(nil), p...))
		mu.Unlock()
	})

	// Baseline replicated table: schema-log event 1.
	if err := f.app.Exec(`CREATE TABLE docs (id BLOB PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE docs: %v", err)
	}
	// Vtab: schema-log event 2. If admission mis-classified the nested
	// shadow DDL ("-- CREATE TABLE 'ft_data'..." etc.), this statement
	// would fail outright (shadow tables use rowid-alias INTEGER
	// PRIMARY KEY, which replicated admission rejects).
	createFTS5(t, f, `CREATE VIRTUAL TABLE ft USING fts5(body)`)

	shadows := []string{"ft_data", "ft_idx", "ft_content", "ft_docsize", "ft_config"}
	for _, name := range shadows {
		exists, err := sqlitebridge.ObjectExists(f.app, "table", name)
		if err != nil {
			t.Fatalf("ObjectExists(%s): %v", name, err)
		}
		if !exists {
			t.Fatalf("shadow table %s missing — fts5 shape changed, test needs updating", name)
		}
	}

	// Schema log: exactly the two user statements, nothing for shadows.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	evts, err := f.log.Read(ctx, 0, 100)
	if err != nil {
		t.Fatalf("schemalog.Read: %v", err)
	}
	if len(evts) != 2 {
		for i, e := range evts {
			t.Logf("event[%d]: seq=%d raw=%q", i, e.SchemaSeq, e.RawSQL)
		}
		t.Fatalf("schema-log events = %d; want 2 (CREATE TABLE docs, CREATE VIRTUAL TABLE ft)", len(evts))
	}
	op, err := crdt.DecodeCatalogOp(evts[1].CatalogOp)
	if err != nil {
		t.Fatalf("DecodeCatalogOp: %v", err)
	}
	if op.Kind != crdt.OpCreateVirtualTable || op.ObjectName != "ft" {
		t.Errorf("event 2 = kind %v object %q; want OpCreateVirtualTable ft", op.Kind, op.ObjectName)
	}

	// Neither the vtab nor its shadow tables may acquire typed catalog
	// entries (a shadow entry would make captureTable admit its rows
	// into the replication stream).
	for _, name := range append([]string{"ft"}, shadows...) {
		if _, ok := f.cat.Table(name); ok {
			t.Errorf("catalog has typed entry for %q; want none", name)
		}
	}

	// Mixed txn: one replicated row + one vtab row. The vtab insert
	// makes fts5 write its shadow tables (nested, preupdate depth 0,
	// so the rows DO enter the touch journal).
	if err := f.app.Exec(`BEGIN;
		INSERT INTO docs (id, body) VALUES (x'01', 'hello world');
		INSERT INTO ft (body) VALUES ('hello world');
	COMMIT`); err != nil {
		t.Fatalf("mixed txn: %v", err)
	}
	// Vtab-only txn: everything it touches is shadow state.
	if err := f.app.Exec(`INSERT INTO ft (body) VALUES ('goodbye moon')`); err != nil {
		t.Fatalf("vtab-only insert: %v", err)
	}

	// Prove the shadow writes actually happened: the FTS index answers.
	if n := queryInt(t, f.app, `SELECT count(*) FROM ft WHERE ft MATCH 'hello'`); n != 1 {
		t.Fatalf("fts match count = %d; want 1", n)
	}
	if n := queryInt(t, f.app, `SELECT count(*) FROM ft_content`); n != 2 {
		t.Fatalf("ft_content rows = %d; want 2", n)
	}

	f.waitDrain(t)

	// Every emitted DML record must target docs; the shadow-table rows
	// (ft_data/ft_idx/ft_content/ft_docsize/ft_config) must have been
	// dropped at drain time.
	docsTab, ok := f.cat.Table("docs")
	if !ok {
		t.Fatalf("docs not in catalog")
	}
	mu.Lock()
	got := append([][]byte(nil), payloads...)
	mu.Unlock()
	dml := 0
	for _, buf := range got {
		cs, err := crdt.Decode(buf)
		if err != nil {
			t.Fatalf("Decode payload: %v", err)
		}
		for _, r := range cs.Records {
			var tab crdt.TableID
			switch rec := r.(type) {
			case crdt.Insert:
				tab = rec.Table
			case crdt.Update:
				tab = rec.Table
			case crdt.Delete:
				tab = rec.Table
			default:
				t.Errorf("unexpected record type %T", r)
				continue
			}
			dml++
			if tab != docsTab.ID {
				t.Errorf("captured record for table %x; only docs (%x) may replicate", tab, docsTab.ID)
			}
		}
	}
	if dml != 1 {
		t.Errorf("replicated DML records = %d; want exactly 1 (the docs insert)", dml)
	}
}

// TestDDL_DropVirtualTable pins the DROP side: a vtab drop arrives as
// plain "DROP TABLE <name>", which reclassifyVtabDrop upgrades to
// OpDropVirtualTable (vtabs are opaque, never in the typed catalog, so
// the typed drop path can't serve them).
func TestDDL_DropVirtualTable(t *testing.T) {
	t.Parallel()
	f := newDDLFixture(t)
	createFTS5(t, f, `CREATE VIRTUAL TABLE ft USING fts5(body)`)

	if err := f.app.Exec(`DROP TABLE ft`); err != nil {
		t.Fatalf("DROP TABLE ft: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	evts, err := f.log.Read(ctx, 0, 100)
	if err != nil {
		t.Fatalf("schemalog.Read: %v", err)
	}
	if len(evts) != 2 {
		t.Fatalf("schema-log events = %d; want 2 (create + drop)", len(evts))
	}
	op, err := crdt.DecodeCatalogOp(evts[1].CatalogOp)
	if err != nil {
		t.Fatalf("DecodeCatalogOp: %v", err)
	}
	if op.Kind != crdt.OpDropVirtualTable || op.ObjectName != "ft" {
		t.Errorf("event 2 = kind %v object %q; want OpDropVirtualTable ft", op.Kind, op.ObjectName)
	}
	if exists, err := sqlitebridge.ObjectExists(f.app, "table", "ft"); err != nil || exists {
		t.Errorf("ft after drop: exists=%v err=%v; want gone", exists, err)
	}
}

// TestDDL_DropVirtualTableIfExists pins the sharpest variant: before
// the reclassification fix, "DROP TABLE IF EXISTS <vtab>" hit the
// typed-catalog miss, took the IF EXISTS no-op branch, and executed
// locally WITHOUT a schema-log append — a silent local-only drop that
// diverged peers. It must append OpDropVirtualTable; a second IF
// EXISTS on the now-missing name must stay a local no-op (no event).
func TestDDL_DropVirtualTableIfExists(t *testing.T) {
	t.Parallel()
	f := newDDLFixture(t)
	createFTS5(t, f, `CREATE VIRTUAL TABLE ft USING fts5(body)`)

	if err := f.app.Exec(`DROP TABLE IF EXISTS ft`); err != nil {
		t.Fatalf("DROP TABLE IF EXISTS ft: %v", err)
	}
	if err := f.app.Exec(`DROP TABLE IF EXISTS ft`); err != nil {
		t.Fatalf("second DROP TABLE IF EXISTS ft: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	evts, err := f.log.Read(ctx, 0, 100)
	if err != nil {
		t.Fatalf("schemalog.Read: %v", err)
	}
	if len(evts) != 2 {
		for i, e := range evts {
			t.Logf("event[%d]: seq=%d raw=%q", i, e.SchemaSeq, e.RawSQL)
		}
		t.Fatalf("schema-log events = %d; want 2 (create + one drop; the redundant IF EXISTS stays local)", len(evts))
	}
	op, err := crdt.DecodeCatalogOp(evts[1].CatalogOp)
	if err != nil {
		t.Fatalf("DecodeCatalogOp: %v", err)
	}
	if op.Kind != crdt.OpDropVirtualTable || op.ObjectName != "ft" {
		t.Errorf("event 2 = kind %v object %q; want OpDropVirtualTable ft", op.Kind, op.ObjectName)
	}
}
