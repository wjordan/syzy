package broker

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// corruptSchemaEventOp overwrites an applied event's catalog_op with bytes
// the current decoder rejects, simulating an op written by an older wire
// format that no longer decodes. In production, pre-collation CREATE TABLE /
// ADD COLUMN ops fail crdt.DecodeCatalogOp with ErrShortBuffer; the reconcile
// pass must skip them rather than abort (which crash-loops the node on Open).
func (f *catchupFixture) corruptSchemaEventOp(t *testing.T, schemaSeq uint64, garbage []byte) {
	t.Helper()
	mc, err := sqlitebridge.Open(filepath.Join(f.dir, "syzy.db"), 0)
	if err != nil {
		t.Fatalf("open metadata conn: %v", err)
	}
	defer mc.Close()
	stmt, _, err := mc.Prepare(`UPDATE syzy_schema_event SET catalog_op = ? WHERE schema_seq = ?`)
	if err != nil {
		t.Fatalf("prepare update: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, garbage); err != nil {
		t.Fatalf("bind blob: %v", err)
	}
	if err := stmt.BindInt64(2, int64(schemaSeq)); err != nil {
		t.Fatalf("bind seq: %v", err)
	}
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("update catalog_op: %v", err)
	}
}

// TestReconcileSchemaToSQLite_SkipsUndecodableEarlierOp reproduces the
// production crash-loop: an applied event early in schema_seq order no longer
// decodes (older catalog-op wire format), while a later applied ADD COLUMN is
// genuinely missing from app.db. The reconcile must skip the undecodable op
// (its table already exists) and still heal the later column, returning no
// error, never aborting the pass (which on Open would crash-loop the node).
func TestReconcileSchemaToSQLite_SkipsUndecodableEarlierOp(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	ctx := context.Background()

	// Seed table t(id) + ADD COLUMN c via the normal path (schema_seq 1, 2).
	tabID, _ := f.appendCreateTable(t, 0, "t")
	f.appendAddColumnNotNull(t, 1, tabID, "c", "''")
	if err := f.br.runSchemaCatchup(ctx); err != nil {
		t.Fatalf("seed catch-up: %v", err)
	}
	if ok, _ := sqlitebridge.ColumnExists(f.app, "t", "c"); !ok {
		t.Fatalf("setup: column c missing after seed catch-up")
	}
	if err := f.app.Exec(`INSERT INTO t (id, c) VALUES (x'01', 'live')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	// Corrupt the EARLIER event (seq=1, CREATE TABLE t) so it no longer
	// decodes — exactly the prod failure ordering (the undecodable op precedes
	// the one that needs healing). Table t still exists, so skipping is safe.
	f.corruptSchemaEventOp(t, 1, []byte{byte(crdt.OpCreateTable), 0xFF})

	// Stage the skew on the LATER event: drop column c from app.db only.
	if err := f.app.Exec(`ALTER TABLE t DROP COLUMN c`); err != nil {
		t.Fatalf("stage skew (drop column): %v", err)
	}

	// Reconcile must skip seq=1 (undecodable) and still heal c from seq=2,
	// with no error.
	repaired, err := f.br.ReconcileSchemaToSQLite(ctx)
	if err != nil {
		t.Fatalf("ReconcileSchemaToSQLite returned error (must skip undecodable, not abort): %v", err)
	}
	if repaired != 1 {
		t.Fatalf("repaired = %d; want 1 (heal c despite undecodable seq=1)", repaired)
	}
	if ok, err := sqlitebridge.ColumnExists(f.app, "t", "c"); err != nil || !ok {
		t.Fatalf("column c not healed past the undecodable op (ok=%v err=%v)", ok, err)
	}
}

// TestIsSupersededDDLErr pins the classifier that decides reconcile log severity:
// a "no such column/table" apply failure is a superseded op (its dependency was
// dropped) and is benign; anything else stays loud.
func TestIsSupersededDDLErr(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("sqlite: no such column: published (code 1)"), true},
		{errors.New("sqlite: no such table: nonexistent_table (code 1)"), true},
		{errors.New("database is locked (code 5)"), false},
		{errors.New("disk I/O error (code 10)"), false},
	}
	for _, c := range cases {
		if got := isSupersededDDLErr(c.err); got != c.want {
			t.Errorf("isSupersededDDLErr(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

// capHandler records emitted slog records so a test can assert log severity.
type capHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capHandler) WithGroup(string) slog.Handler      { return h }

// TestReconcileSchema_SupersededDDLLogsInfoNotError reproduces the prod noise
// (idx_apps_published: a CREATE INDEX whose column a later migration dropped)
// and proves ReconcileSchemaToSQLite skips it at Info, not Error. The target
// table exists only in app.db (no schema-log event), so the reconcile cannot
// heal it — a later DROP makes the applied CREATE INDEX permanently unappliable.
func TestReconcileSchema_SupersededDDLLogsInfoNotError(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	ctx := context.Background()
	cap := &capHandler{}
	br, err := New(Config{
		AppApply: f.app, Meta: f.sc, Catalog: f.cat,
		Cache: nodestate.New(crdt.Origin(7)), SchemaLog: f.log,
		Log: slog.New(cap),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := f.app.Exec(`CREATE TABLE nonexistent_table (col INT)`); err != nil {
		t.Fatalf("create target: %v", err)
	}
	f.appendBadIndex(t, 0, "idx_x") // CREATE INDEX idx_x ON nonexistent_table(col)
	if err := br.runSchemaCatchup(ctx); err != nil {
		t.Fatalf("apply index (becomes an applied event): %v", err)
	}
	if err := f.app.Exec(`DROP TABLE nonexistent_table`); err != nil {
		t.Fatalf("drop target (superseding the index): %v", err)
	}

	if _, err := br.ReconcileSchemaToSQLite(ctx); err != nil {
		t.Fatalf("ReconcileSchemaToSQLite returned error (must skip superseded op): %v", err)
	}

	infoSkip, errSkip := 0, 0
	for _, r := range cap.records {
		switch {
		case r.Level == slog.LevelInfo && strings.Contains(r.Message, "superseded DDL"):
			infoSkip++
		case r.Level == slog.LevelError && strings.Contains(r.Message, "re-apply failed"):
			errSkip++
		}
	}
	if infoSkip != 1 || errSkip != 0 {
		t.Fatalf("want 1 Info superseded-skip + 0 Error re-apply-failed; got info=%d err=%d", infoSkip, errSkip)
	}
}

// appendAddColumnNotNull appends an ADD COLUMN event whose column is
// NOT NULL with a DEFAULT: the shape that must re-render with its
// DEFAULT to be addable to a non-empty table.
func (f *catchupFixture) appendAddColumnNotNull(t *testing.T, parentSeq uint64, tabID crdt.TableID, colName, def string) crdt.ColumnID {
	t.Helper()
	colID := catalog.AllocColumnID()
	op := crdt.CatalogOp{
		Kind:    crdt.OpAddColumn,
		TableID: tabID,
		Columns: []crdt.CatalogColumn{{
			ID: colID, Name: colName, Ordinal: 1, Type: "TEXT",
			NotNull: true, Default: def, ClockGroup: "row",
		}},
	}
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if _, err := f.log.Append(context.Background(), parentSeq, encoded, ""); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	return colID
}

func columnText(t *testing.T, app *sqlitebridge.Conn, table, col string, id []byte) (string, bool) {
	t.Helper()
	stmt, _, err := app.Prepare(`SELECT ` + sqlitebridge.QuoteIdent(col) + ` FROM ` + sqlitebridge.QuoteIdent(table) + ` WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !hasRow {
		return "", false
	}
	return stmt.ColumnText(0), true
}

// TestReconcileSchemaToSQLite_HealsMetadataAheadOfApp reproduces the
// durable two-stream-restore skew: the metadata catalog records an ADD
// COLUMN as applied (schema_seq advanced, syzy_schema_event = 'applied',
// catalog carries the column) while app.db's table lacks it, and asserts
// ReconcileSchemaToSQLite heals it: the column reappears (backfilled via
// its NOT NULL DEFAULT on the existing row), an inbound row-write
// referencing it applies cleanly, and a second reconcile is a no-op.
func TestReconcileSchemaToSQLite_HealsMetadataAheadOfApp(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	ctx := context.Background()

	// Bring the node to a consistent state via the normal catch-up path:
	// CREATE TABLE t(id BLOB PK), then ADD COLUMN c TEXT NOT NULL DEFAULT ''.
	tabID, _ := f.appendCreateTable(t, 0, "t")
	f.appendAddColumnNotNull(t, 1, tabID, "c", "''")
	if err := f.br.runSchemaCatchup(ctx); err != nil {
		t.Fatalf("seed catch-up: %v", err)
	}
	if got := f.localSchemaSeq(t); got != 2 {
		t.Fatalf("schema_seq = %d; want 2 after seed", got)
	}
	if ok, _ := sqlitebridge.ColumnExists(f.app, "t", "c"); !ok {
		t.Fatalf("setup: column c missing after seed catch-up")
	}
	// A live row, so the re-add must backfill via DEFAULT (NOT NULL).
	if err := f.app.Exec(`INSERT INTO t (id, c) VALUES (x'01', 'live')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	// Stage the skew: drop column c from app.db ONLY, leaving the metadata
	// catalog + schema_seq=2 + syzy_schema_event[2]='applied' intact. This
	// is exactly what a metadata-stream-ahead-of-data-stream restore
	// reconstructs.
	if err := f.app.Exec(`ALTER TABLE t DROP COLUMN c`); err != nil {
		t.Fatalf("stage skew (drop column): %v", err)
	}
	if ok, _ := sqlitebridge.ColumnExists(f.app, "t", "c"); ok {
		t.Fatalf("staging failed: column c still present in app.db")
	}

	// Ordinary schema catch-up CANNOT heal this: schema_seq is already at
	// the event's seq, so it is skipped forever.
	if err := f.br.runSchemaCatchup(ctx); err != nil {
		t.Fatalf("runSchemaCatchup (no-op): %v", err)
	}
	if ok, _ := sqlitebridge.ColumnExists(f.app, "t", "c"); ok {
		t.Fatalf("runSchemaCatchup unexpectedly re-added c; the skew premise is invalid")
	}

	// Reconcile heals the skew.
	repaired, err := f.br.ReconcileSchemaToSQLite(ctx)
	if err != nil {
		t.Fatalf("ReconcileSchemaToSQLite: %v", err)
	}
	if repaired != 1 {
		t.Fatalf("repaired = %d; want 1", repaired)
	}
	if ok, err := sqlitebridge.ColumnExists(f.app, "t", "c"); err != nil || !ok {
		t.Fatalf("column c not healed (ok=%v err=%v)", ok, err)
	}
	// The pre-existing row was backfilled with the NOT NULL DEFAULT.
	if got, ok := columnText(t, f.app, "t", "c", []byte{0x01}); !ok || got != "" {
		t.Fatalf("backfilled c = %q (present=%v); want \"\" from DEFAULT", got, ok)
	}

	// An inbound row-write referencing the healed column now applies
	// cleanly (pre-heal it would fail "no such column: c").
	tab, ok := f.cat.Table("t")
	if !ok {
		t.Fatalf("catalog missing table t")
	}
	cCol, ok := tab.Column("c")
	if !ok {
		t.Fatalf("catalog missing column c")
	}
	idCol := tab.PK[0].ID
	pk, err := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x02})})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	src := crdt.Origin(11)
	cs, err := crdt.Build(crdt.Dot{Origin: src, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}, nil, testCluster,
		[]crdt.Record{crdt.Insert{
			Table: tab.ID, PK: pk, CL: 1,
			Image: []crdt.ColValue{textCol(cCol.ID, "remote")},
		}})
	if err != nil {
		t.Fatalf("Build inbound insert: %v", err)
	}
	if err := f.br.applyPayload(ctx, cs.Encoded()); err != nil {
		t.Fatalf("apply inbound write referencing healed column: %v", err)
	}
	if got, ok := columnText(t, f.app, "t", "c", []byte{0x02}); !ok || got != "remote" {
		t.Fatalf("inbound c = %q (present=%v); want \"remote\"", got, ok)
	}

	// Idempotent: a second reconcile on the now-healthy node repairs nothing.
	repaired2, err := f.br.ReconcileSchemaToSQLite(ctx)
	if err != nil {
		t.Fatalf("second ReconcileSchemaToSQLite: %v", err)
	}
	if repaired2 != 0 {
		t.Fatalf("second reconcile repaired = %d; want 0 (no-op on healthy node)", repaired2)
	}
}

// A schema event whose object a LATER op renamed or dropped away must not
// read as "missing" on a healthy node: reconcile would then re-run the
// original CREATE and resurrect an intermediate table no other node has.
// One transaction's DDL arrives as a single bundle, so the same supersede
// can happen between sub-ops of one event.

func TestReconcileSchemaToSQLite_BundleDoesNotResurrectIntermediate(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	create, tabID, _ := createTableOp("t")
	bundle := crdt.CatalogOp{Kind: crdt.OpBundle, SubOps: []crdt.CatalogOp{
		create,
		{Kind: crdt.OpRenameTable, TableID: tabID, TableName: "t2"},
	}}
	encoded, err := crdt.EncodeCatalogOp(bundle)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if _, err := f.log.Append(context.Background(), 0, encoded, "txn"); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup: %v", err)
	}
	if tableExistsInSQLite(t, f.app, "t") || !tableExistsInSQLite(t, f.app, "t2") {
		t.Fatal("setup: want only t2 in app.db after catch-up")
	}

	repaired, err := f.br.ReconcileSchemaToSQLite(context.Background())
	if err != nil {
		t.Fatalf("ReconcileSchemaToSQLite: %v", err)
	}
	if repaired != 0 {
		t.Errorf("repaired = %d on a healthy node; want 0", repaired)
	}
	if tableExistsInSQLite(t, f.app, "t") {
		t.Error("reconcile resurrected the intermediate table t")
	}
	if !tableExistsInSQLite(t, f.app, "t2") {
		t.Error("t2 lost")
	}
}

func TestReconcileSchemaToSQLite_DoesNotResurrectRenamedAwayTable(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	tabID, _ := f.appendCreateTable(t, 0, "t")
	encoded, err := crdt.EncodeCatalogOp(crdt.CatalogOp{
		Kind: crdt.OpRenameTable, TableID: tabID, TableName: "t2",
	})
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if _, err := f.log.Append(context.Background(), 1, encoded, ""); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup: %v", err)
	}

	repaired, err := f.br.ReconcileSchemaToSQLite(context.Background())
	if err != nil {
		t.Fatalf("ReconcileSchemaToSQLite: %v", err)
	}
	if repaired != 0 {
		t.Errorf("repaired = %d on a healthy node; want 0", repaired)
	}
	if tableExistsInSQLite(t, f.app, "t") {
		t.Error("reconcile resurrected the renamed-away table t")
	}
}

func TestReconcileSchemaToSQLite_DoesNotResurrectDroppedTable(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	tabID, _ := f.appendCreateTable(t, 0, "t")
	encoded, err := crdt.EncodeCatalogOp(crdt.CatalogOp{
		Kind: crdt.OpDropTable, TableID: tabID,
	})
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if _, err := f.log.Append(context.Background(), 1, encoded, ""); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup: %v", err)
	}

	repaired, err := f.br.ReconcileSchemaToSQLite(context.Background())
	if err != nil {
		t.Fatalf("ReconcileSchemaToSQLite: %v", err)
	}
	if repaired != 0 {
		t.Errorf("repaired = %d on a healthy node; want 0", repaired)
	}
	if tableExistsInSQLite(t, f.app, "t") {
		t.Error("reconcile resurrected the dropped table t")
	}
}
