package broker

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// catchupFixture drives runSchemaCatchup / drainFailedLocalSchemaEvents
// in isolation, no transport.
type catchupFixture struct {
	dir string
	app *sqlitebridge.Conn
	sc  *metadata.Store
	cat *catalog.Catalog
	log *schemalog.Local
	br  *Broker
}

type readErrorSchemaLog struct{ err error }

func (l readErrorSchemaLog) Append(context.Context, uint64, []byte, string) (uint64, error) {
	return 0, errors.New("unexpected append")
}
func (l readErrorSchemaLog) Read(context.Context, uint64, int) ([]schemalog.Event, error) {
	return nil, l.err
}
func (l readErrorSchemaLog) Head(context.Context) (uint64, error) { return 0, l.err }

func newCatchupFixture(t *testing.T) *catchupFixture {
	t.Helper()
	dir := t.TempDir()

	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		t.Fatalf("WAL: %v", err)
	}

	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	if err := sc.SetClusterID(testCluster); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(crdt.Origin(7)); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}

	cat, err := catalog.SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("SeedFromSchema: %v", err)
	}

	log := schemalog.NewLocal()
	cfg := Config{
		AppApply:  app,
		Meta:      sc,
		Catalog:   cat,
		Cache:     nodestate.New(crdt.Origin(7)),
		SchemaLog: log,
	}
	br, err := New(cfg)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	return &catchupFixture{dir: dir, app: app, sc: sc, cat: cat, log: log, br: br}
}

func (f *catchupFixture) localSchemaSeq(t *testing.T) uint64 {
	t.Helper()
	seq, _, err := f.sc.GetSchemaSeq()
	if err != nil {
		t.Fatalf("GetSchemaSeq: %v", err)
	}
	return seq
}

// createTableOp builds a minimal single-BLOB-PK CREATE TABLE op.
func createTableOp(tableName string) (crdt.CatalogOp, crdt.TableID, crdt.ColumnID) {
	tabID := catalog.AllocTableID()
	colID := catalog.AllocColumnID()
	return crdt.CatalogOp{
		Kind:      crdt.OpCreateTable,
		TableID:   tabID,
		TableName: tableName,
		Columns: []crdt.CatalogColumn{{
			ID: colID, Name: "id", Ordinal: 0, Type: "BLOB",
			NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row",
		}},
		Keys: []crdt.CatalogKey{{
			KeyID: crdt.KeyID{}, Members: []crdt.CatalogKeyMember{{ColumnID: colID}},
		}},
	}, tabID, colID
}

func (f *catchupFixture) appendCreateTable(t *testing.T, parentSeq uint64, tableName string) (crdt.TableID, crdt.ColumnID) {
	t.Helper()
	op, tabID, colID := createTableOp(tableName)
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if _, err := f.log.Append(context.Background(), parentSeq, encoded, ""); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	return tabID, colID
}

func (f *catchupFixture) appendAddColumn(t *testing.T, parentSeq uint64, tabID crdt.TableID, colName string) crdt.ColumnID {
	t.Helper()
	colID := catalog.AllocColumnID()
	op := crdt.CatalogOp{
		Kind:    crdt.OpAddColumn,
		TableID: tabID,
		Columns: []crdt.CatalogColumn{{
			ID: colID, Name: colName, Ordinal: 1, Type: "TEXT", ClockGroup: "row",
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

// appendBadIndex pushes a CREATE INDEX op whose RawSQL is syntactically
// valid but targets a nonexistent table — applyCatalogStructural's Exec
// will return "no such table". Used to drive the failure path without
// having to mock at a lower layer.
func (f *catchupFixture) appendBadIndex(t *testing.T, parentSeq uint64, idxName string) {
	t.Helper()
	op := crdt.CatalogOp{
		Kind:       crdt.OpCreateIndex,
		ObjectName: idxName,
		RawSQL:     "CREATE INDEX " + idxName + " ON nonexistent_table(col)",
	}
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if _, err := f.log.Append(context.Background(), parentSeq, encoded, ""); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
}

func tableExistsInSQLite(t *testing.T, app *sqlitebridge.Conn, name string) bool {
	t.Helper()
	ok, err := sqlitebridge.ObjectExists(app, "table", name)
	if err != nil {
		t.Fatalf("ObjectExists(%q): %v", name, err)
	}
	return ok
}

// TestStartSchemaCatchup_ConvergesWithoutTransport exercises the
// transport-less entry point used by single-node bucket mode: a node
// whose local schema_seq lags a durable schema log (events appended by
// a prior process / peer) must converge to head on its own — replaying
// the log into the catalog — so its next DDL stops losing the CAS.
func TestStartSchemaCatchup_ConvergesWithoutTransport(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	// Durable log already at head=2 while local schema_seq=0, the
	// "fresh local DB against a populated log" shape.
	tabID, _ := f.appendCreateTable(t, 0, "alpha")
	f.appendAddColumn(t, 1, tabID, "extra")

	br, err := New(Config{
		AppApply:              f.app,
		Meta:                  f.sc,
		Catalog:               f.cat,
		Cache:                 nodestate.New(crdt.Origin(7)),
		SchemaLog:             f.log,
		SchemaCatchupInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = br.Close() })

	if err := br.StartSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("StartSchemaCatchup: %v", err)
	}
	// Second start is a programming error.
	if err := br.StartSchemaCatchup(context.Background()); err == nil {
		t.Fatalf("StartSchemaCatchup twice: want error, got nil")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if f.localSchemaSeq(t) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("schema_seq did not reach head=2 (stuck at %d)", f.localSchemaSeq(t))
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Stop the catchup loop before touching f.app directly: each tick ends
	// with a catalog reload that walks pragma_table_xinfo on the AppApply
	// conn, and sqlitebridge.Conn is not safe for concurrent use.
	if err := br.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !tableExistsInSQLite(t, f.app, "alpha") {
		t.Fatalf("catchup loop did not apply table 'alpha'")
	}
}

// TestStartSchemaCatchup_RequiresSchemaLog guards the misconfiguration:
// the transport-less entry point is meaningless without a log to replay.
func TestStartSchemaCatchup_RequiresSchemaLog(t *testing.T) {
	t.Parallel()
	sc, err := metadata.Open(filepath.Join(t.TempDir(), "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	if err := sc.SetClusterID(testCluster); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	br, err := New(Config{Meta: sc, Cache: nodestate.New(crdt.Origin(7))})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = br.Close() })
	if err := br.StartSchemaCatchup(context.Background()); err == nil {
		t.Fatalf("StartSchemaCatchup without SchemaLog: want error, got nil")
	}
}

func TestRunSchemaCatchup_AppliesCreateTable(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	f.appendCreateTable(t, 0, "thing")

	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup: %v", err)
	}
	if got := f.localSchemaSeq(t); got != 1 {
		t.Fatalf("schema_seq = %d; want 1", got)
	}
	if !tableExistsInSQLite(t, f.app, "thing") {
		t.Fatalf("sqlite_master missing table 'thing'")
	}
	if _, ok := f.cat.Table("thing"); !ok {
		t.Fatalf("catalog missing table 'thing'")
	}
}

// TestRunSchemaCatchup_AppliesVirtualTableOps replays the receiver side
// of the vtab lifecycle: OpCreateVirtualTable and OpDropVirtualTable are
// opaque-SQL ops (vtabs never enter the typed catalog), so apply is a
// straight Exec of the RawSQL guarded by the sqlite_master idempotency
// precheck. The drop's RawSQL is the plain DROP TABLE form the
// originator's reclassification captured.
func TestRunSchemaCatchup_AppliesVirtualTableOps(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	appendVtabOp := func(parentSeq uint64, kind crdt.CatalogOpKind, rawSQL string) {
		t.Helper()
		encoded, err := crdt.EncodeCatalogOp(crdt.CatalogOp{
			Kind: kind, ObjectName: "ft", RawSQL: rawSQL,
		})
		if err != nil {
			t.Fatalf("EncodeCatalogOp: %v", err)
		}
		if _, err := f.log.Append(context.Background(), parentSeq, encoded, ""); err != nil {
			t.Fatalf("log.Append: %v", err)
		}
	}

	appendVtabOp(0, crdt.OpCreateVirtualTable, `CREATE VIRTUAL TABLE ft USING fts5(body)`)
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup(create): %v", err)
	}
	if got := f.localSchemaSeq(t); got != 1 {
		t.Fatalf("schema_seq = %d; want 1", got)
	}
	if !tableExistsInSQLite(t, f.app, "ft") {
		t.Fatalf("sqlite_master missing vtab 'ft'")
	}
	if _, ok := f.cat.Table("ft"); ok {
		t.Fatalf("vtab 'ft' must not enter the typed catalog")
	}

	appendVtabOp(1, crdt.OpDropVirtualTable, `DROP TABLE ft`)
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup(drop): %v", err)
	}
	if got := f.localSchemaSeq(t); got != 2 {
		t.Fatalf("schema_seq = %d; want 2", got)
	}
	if tableExistsInSQLite(t, f.app, "ft") {
		t.Fatalf("vtab 'ft' still present after replicated drop")
	}
	// Shadow tables must have been cascade-dropped by SQLite.
	if tableExistsInSQLite(t, f.app, "ft_content") {
		t.Fatalf("shadow table ft_content survived the vtab drop")
	}
}

// TestRunSchemaCatchup_PreservesParameterizedColumnType is the regression
// guard for the type-args-drop bug: a CatalogOp whose CatalogColumn.Type
// carries parenthesized arguments (VARCHAR(255), DECIMAL(10,2)) must
// reconstruct on the receiver with the args intact, so pragma_table_info
// matches what the originator's sqlite_master holds.
func TestRunSchemaCatchup_PreservesParameterizedColumnType(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	tabID := catalog.AllocTableID()
	idCol := catalog.AllocColumnID()
	emailCol := catalog.AllocColumnID()
	amountCol := catalog.AllocColumnID()
	op := crdt.CatalogOp{
		Kind:      crdt.OpCreateTable,
		TableID:   tabID,
		TableName: "users",
		Columns: []crdt.CatalogColumn{
			{ID: idCol, Name: "id", Ordinal: 0, Type: "BLOB",
				NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row"},
			{ID: emailCol, Name: "email", Ordinal: 1, Type: "VARCHAR(255)", ClockGroup: "row"},
			{ID: amountCol, Name: "amount", Ordinal: 2, Type: "DECIMAL(10, 2)", ClockGroup: "row"},
		},
		Keys: []crdt.CatalogKey{{
			KeyID: crdt.KeyID{}, Members: []crdt.CatalogKeyMember{{ColumnID: idCol}},
		}},
	}
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if _, err := f.log.Append(context.Background(), 0, encoded, ""); err != nil {
		t.Fatalf("log.Append: %v", err)
	}
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup: %v", err)
	}

	want := map[string]string{"email": "VARCHAR(255)", "amount": "DECIMAL(10, 2)"}
	stmt, _, err := f.app.Prepare(`SELECT name, type FROM pragma_table_info('users')`)
	if err != nil {
		t.Fatalf("prep: %v", err)
	}
	defer stmt.Finalize()
	got := map[string]string{}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if !hasRow {
			break
		}
		got[stmt.ColumnText(0)] = stmt.ColumnText(1)
	}
	for name, wantType := range want {
		if got[name] != wantType {
			t.Errorf("pragma_table_info: column %q type = %q; want %q", name, got[name], wantType)
		}
	}
}

// TestRunSchemaCatchup_TerminalStructuralFailureMarksUnhealthy verifies that
// deterministic structural rejection halts durably without advancing either
// side of the catalog.
func TestRunSchemaCatchup_TerminalStructuralFailureMarksUnhealthy(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	f.appendBadIndex(t, 0, "idx_x")

	err := f.br.runSchemaCatchup(context.Background())
	if !errors.Is(err, ErrSchemaUnhealthy) {
		t.Fatalf("runSchemaCatchup = %v; want ErrSchemaUnhealthy", err)
	}
	if got := f.localSchemaSeq(t); got != 0 {
		t.Fatalf("schema_seq = %d; want 0 (apply failed, must not advance)", got)
	}
	// schema_event must not have been written.
	events, rerr := f.sc.ReadFailedLocalSchemaEvents()
	if rerr != nil {
		t.Fatalf("ReadFailedLocalSchemaEvents: %v", rerr)
	}
	if len(events) != 0 {
		t.Fatalf("failed_local rows = %d; want 0 (no schema_event row should be written on failure)", len(events))
	}
	health, unhealthy, err := f.sc.GetSchemaHealth()
	if err != nil || !unhealthy {
		t.Fatalf("GetSchemaHealth = (%#v, %v, %v); want unhealthy", health, unhealthy, err)
	}
	if health.Seq != 1 || !strings.Contains(health.Reason, "no such table") {
		t.Fatalf("schema health = %#v; want seq=1 missing-table reason", health)
	}
	status := f.br.InboundHealth()
	if !status.SchemaUnhealthy || status.SchemaUnhealthySeq != health.Seq || status.SchemaUnhealthyReason != health.Reason {
		t.Fatalf("InboundHealth schema state = %#v; want %#v", status, health)
	}
}

func TestRunSchemaCatchup_BelowHorizonMarksUnhealthy(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	f.br.cfg.SchemaLog = readErrorSchemaLog{err: schemalog.ErrBelowHorizon}

	err := f.br.runSchemaCatchup(context.Background())
	if !errors.Is(err, ErrSchemaUnhealthy) {
		t.Fatalf("runSchemaCatchup = %v; want ErrSchemaUnhealthy", err)
	}
	health, unhealthy, err := f.sc.GetSchemaHealth()
	if err != nil || !unhealthy || health.Seq != 1 || !strings.Contains(health.Reason, "retention horizon") {
		t.Fatalf("GetSchemaHealth = (%#v, %v, %v); want seq=1 horizon failure", health, unhealthy, err)
	}
}

func TestRunSchemaCatchup_UndecodableEventMarksUnhealthy(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	if _, err := f.log.Append(context.Background(), 0, []byte{0xff}, "bad event"); err != nil {
		t.Fatalf("append bad event: %v", err)
	}

	err := f.br.runSchemaCatchup(context.Background())
	if !errors.Is(err, ErrSchemaUnhealthy) {
		t.Fatalf("runSchemaCatchup = %v; want ErrSchemaUnhealthy", err)
	}
	health, unhealthy, err := f.sc.GetSchemaHealth()
	if err != nil || !unhealthy || health.Seq != 1 || !strings.Contains(health.Reason, "decode op") {
		t.Fatalf("GetSchemaHealth = (%#v, %v, %v); want seq=1 decode failure", health, unhealthy, err)
	}
	if err := f.br.runSchemaCatchup(context.Background()); !errors.Is(err, ErrSchemaUnhealthy) {
		t.Fatalf("second runSchemaCatchup = %v; want durable ErrSchemaUnhealthy", err)
	}
}

// TestRunSchemaCatchup_RetrySucceedsAfterTransientFailure proves a real
// SQLite writer lock leaves no durable health marker and the same event lands
// after the lock clears.
func TestRunSchemaCatchup_RetrySucceedsAfterTransientFailure(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	f.appendCreateTable(t, 0, "t")

	locker, err := sqlitebridge.Open(filepath.Join(f.dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open locker: %v", err)
	}
	t.Cleanup(func() { _ = locker.Close() })
	if err := locker.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("lock writer: %v", err)
	}
	if err := f.br.runSchemaCatchup(context.Background()); err == nil {
		t.Fatal("runSchemaCatchup under writer lock returned nil")
	}
	if got := f.localSchemaSeq(t); got != 0 {
		t.Fatalf("schema_seq under lock = %d; want 0", got)
	}
	if health, unhealthy, err := f.sc.GetSchemaHealth(); err != nil || unhealthy {
		t.Fatalf("transient failure health = (%#v, %v, %v); want healthy", health, unhealthy, err)
	}
	if err := locker.Exec("ROLLBACK"); err != nil {
		t.Fatalf("unlock writer: %v", err)
	}
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup after unlock: %v", err)
	}
	if got := f.localSchemaSeq(t); got != 1 {
		t.Fatalf("schema_seq = %d; want 1 after retry", got)
	}
}

// TestRunSchemaCatchup_IdempotentWhenSQLiteAlreadyHasState models the
// crash window between successful SQLite apply and the metadata tx:
// SQLite has the change, metadata doesn't. The precheck in
// applyCatalogStructural must skip the redundant DDL and let the
// metadata-side writes land on retry. Without it, the retry would
// fail with "duplicate column" / "table already exists" forever.
func TestRunSchemaCatchup_IdempotentWhenSQLiteAlreadyHasState(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)

	// First apply lays down CREATE TABLE cleanly.
	tabID, _ := f.appendCreateTable(t, 0, "t")
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("initial CREATE TABLE: %v", err)
	}

	// Queue an ADD COLUMN event.
	f.appendAddColumn(t, 1, tabID, "extra")

	// Simulate the crash window: SQLite already has the column, but
	// schema_seq is still at 1 (metadata tx didn't run).
	if err := f.app.Exec(`ALTER TABLE t ADD COLUMN extra TEXT`); err != nil {
		t.Fatalf("manual ALTER: %v", err)
	}

	// Catchup must succeed (precheck skips the redundant ALTER) and
	// advance schema_seq.
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup after crash-window: %v", err)
	}
	if got := f.localSchemaSeq(t); got != 2 {
		t.Fatalf("schema_seq = %d; want 2", got)
	}
	tab, ok := f.cat.Table("t")
	if !ok {
		t.Fatalf("catalog missing 't' after catchup")
	}
	if _, has := tab.Column("extra"); !has {
		t.Fatalf("catalog missing column 'extra'")
	}
}

// TestDrainFailedLocalSchemaEvents drains the residue of an older
// broker binary: a schema_event row marked 'failed_local' with
// schema_seq already advanced.
func TestDrainFailedLocalSchemaEvents(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)

	// Forge the pre-fix half-state: an old binary applied the SQLite
	// CREATE TABLE successfully (so sqlite_master has it), but for some
	// reason ApplyState was recorded as failed_local. schema_seq
	// advanced because the old logic always advanced. The catalog rows
	// were upserted unconditionally by the old code; we mimic that by
	// pre-populating the metadata too.
	tabID := catalog.AllocTableID()
	colID := catalog.AllocColumnID()
	op := crdt.CatalogOp{
		Kind: crdt.OpCreateTable, TableID: tabID, TableName: "stuck",
		Columns: []crdt.CatalogColumn{{
			ID: colID, Name: "id", Ordinal: 0, Type: "BLOB",
			NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row",
		}},
		Keys: []crdt.CatalogKey{{
			KeyID: crdt.KeyID{}, Members: []crdt.CatalogKeyMember{{ColumnID: colID}},
		}},
	}
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	// SQLite-side state from the old binary's successful CREATE TABLE.
	if err := f.app.Exec(`CREATE TABLE stuck (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("seed sqlite: %v", err)
	}
	// Persist the failed_local row + advance schema_seq exactly as the
	// old binary would have.
	if err := f.sc.WithTx(func(tx *metadata.Tx) error {
		return tx.AppendSchemaEvent(metadata.SchemaEventEntry{
			SchemaSeq: 1, ParentSeq: 0, CatalogOp: encoded, RawSQL: "",
			AppliedAtUs: 12345, ApplyState: metadata.ApplyStateFailedLocal,
		})
	}); err != nil {
		t.Fatalf("seed schema_event: %v", err)
	}

	// Sanity: drain finds exactly the one row.
	pre, err := f.sc.ReadFailedLocalSchemaEvents()
	if err != nil {
		t.Fatalf("ReadFailedLocalSchemaEvents pre: %v", err)
	}
	if len(pre) != 1 {
		t.Fatalf("pre-drain failed_local rows = %d; want 1", len(pre))
	}

	if err := f.br.drainFailedLocalSchemaEvents(); err != nil {
		t.Fatalf("drainFailedLocalSchemaEvents: %v", err)
	}

	// Post-drain: no failed_local rows, catalog has the table.
	post, err := f.sc.ReadFailedLocalSchemaEvents()
	if err != nil {
		t.Fatalf("ReadFailedLocalSchemaEvents post: %v", err)
	}
	if len(post) != 0 {
		t.Fatalf("post-drain failed_local rows = %d; want 0", len(post))
	}
	if _, ok := f.cat.Table("stuck"); !ok {
		t.Fatalf("catalog missing 'stuck' after drain")
	}
	if !tableExistsInSQLite(t, f.app, "stuck") {
		t.Fatalf("sqlite_master missing 'stuck' after drain")
	}
}

func TestOpAlreadyAppliedInSQLite(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)

	// Seed a table 't' with column 'a' so the precheck has something
	// to verify against. We use the catchup path itself rather than
	// hand-rolled SQL so the catalog and sqlite_master stay in sync.
	tabID, _ := f.appendCreateTable(t, 0, "t")
	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("seed CREATE TABLE: %v", err)
	}

	t.Run("CreateTable_present", func(t *testing.T) {
		op := crdt.CatalogOp{Kind: crdt.OpCreateTable, TableID: tabID, TableName: "t"}
		got, err := opAlreadyAppliedInSQLite(op, f.app, f.cat)
		if err != nil || !got {
			t.Fatalf("got (%v, %v); want (true, nil)", got, err)
		}
	})
	t.Run("CreateTable_absent", func(t *testing.T) {
		op := crdt.CatalogOp{Kind: crdt.OpCreateTable, TableName: "missing"}
		got, err := opAlreadyAppliedInSQLite(op, f.app, f.cat)
		if err != nil || got {
			t.Fatalf("got (%v, %v); want (false, nil)", got, err)
		}
	})
	t.Run("AddColumn_absent", func(t *testing.T) {
		newColID := catalog.AllocColumnID()
		op := crdt.CatalogOp{
			Kind: crdt.OpAddColumn, TableID: tabID,
			Columns: []crdt.CatalogColumn{{ID: newColID, Name: "newcol"}},
		}
		got, err := opAlreadyAppliedInSQLite(op, f.app, f.cat)
		if err != nil || got {
			t.Fatalf("got (%v, %v); want (false, nil)", got, err)
		}
	})
	t.Run("AddColumn_present", func(t *testing.T) {
		if err := f.app.Exec(`ALTER TABLE t ADD COLUMN side TEXT`); err != nil {
			t.Fatalf("manual ALTER: %v", err)
		}
		op := crdt.CatalogOp{
			Kind: crdt.OpAddColumn, TableID: tabID,
			Columns: []crdt.CatalogColumn{{Name: "side"}},
		}
		got, err := opAlreadyAppliedInSQLite(op, f.app, f.cat)
		if err != nil || !got {
			t.Fatalf("got (%v, %v); want (true, nil)", got, err)
		}
	})
	t.Run("DropTable_alreadyGone", func(t *testing.T) {
		// catalog doesn't have 'ghost', so DropTable is trivially applied.
		op := crdt.CatalogOp{Kind: crdt.OpDropTable, TableID: crdt.TableID{0xAB}}
		got, err := opAlreadyAppliedInSQLite(op, f.app, f.cat)
		if err != nil || !got {
			t.Fatalf("got (%v, %v); want (true, nil)", got, err)
		}
	})
}

// setLocalDDLIntent plants an origin's LocalDDL intent slot directly,
// simulating a producer that appended (or was about to append) seq.
func (f *catchupFixture) setLocalDDLIntent(t *testing.T, origin crdt.Origin, seq uint64, startedUs int64) {
	t.Helper()
	err := f.sc.SetOriginIntent(origin, metadata.EncodeLocalDDL(metadata.LocalDDLIntent{
		StartedAtUs: startedUs, SchemaSeq: seq, ParentSeq: seq - 1,
		CatalogOp: []byte("op"), RawSQL: "CREATE TABLE x",
	}))
	if err != nil {
		t.Fatalf("SetOriginIntent: %v", err)
	}
}

// TestRunSchemaCatchup_YieldsToFreshLocalDDLIntent: a live originator's
// wal_hook owns finalization of its just-appended event; catch-up must
// not race it.
func TestRunSchemaCatchup_YieldsToFreshLocalDDLIntent(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	f.appendCreateTable(t, 0, "pending")
	f.setLocalDDLIntent(t, crdt.Origin(9), 1, time.Now().UnixMicro())

	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup: %v", err)
	}
	if got := f.localSchemaSeq(t); got != 0 {
		t.Fatalf("schema_seq = %d; want 0 (yielded)", got)
	}
	if tableExistsInSQLite(t, f.app, "pending") {
		t.Fatalf("catch-up applied an event a live originator owns")
	}
	if all, _ := f.sc.ListIntents(); len(all) != 1 {
		t.Fatalf("fresh intent destroyed by catch-up")
	}
}

// TestRunSchemaCatchup_RecoversStaleLocalDDLIntent is the dead-guest
// wedge: an originator died between Append and wal_hook resolution,
// leaving a dangling intent at the log head. Catch-up must take over
// (apply the event, clear the dead slot) instead of yielding forever.
func TestRunSchemaCatchup_RecoversStaleLocalDDLIntent(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	f.appendCreateTable(t, 0, "wedged")
	f.setLocalDDLIntent(t, crdt.Origin(9), 1,
		time.Now().UnixMicro()-2*ddlIntentFreshUs)

	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup: %v", err)
	}
	if got := f.localSchemaSeq(t); got != 1 {
		t.Fatalf("schema_seq = %d; want 1 (recovered)", got)
	}
	if !tableExistsInSQLite(t, f.app, "wedged") {
		t.Fatalf("stale-intent event not applied")
	}
	if all, _ := f.sc.ListIntents(); len(all) != 0 {
		t.Fatalf("stale intent not cleared after recovery")
	}
}

// TestRunSchemaCatchup_AppliesMultiDDLBundle is the receiver side of a
// multi-DDL transaction: the originator publishes every statement in one
// BEGIN...COMMIT as a single OpBundle, and catch-up must apply the whole
// set atomically under one schema_seq — including a sub-op that depends
// on a table an earlier sub-op created.
func TestRunSchemaCatchup_AppliesMultiDDLBundle(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)

	tabID, colID := catalog.AllocTableID(), catalog.AllocColumnID()
	extraID := catalog.AllocColumnID()
	bundle := crdt.CatalogOp{Kind: crdt.OpBundle, SubOps: []crdt.CatalogOp{
		{
			Kind: crdt.OpCreateTable, TableID: tabID, TableName: "posts",
			Columns: []crdt.CatalogColumn{{
				ID: colID, Name: "id", Ordinal: 0, Type: "BLOB",
				NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row",
			}},
			Keys: []crdt.CatalogKey{{
				KeyID: crdt.KeyID{}, Members: []crdt.CatalogKeyMember{{ColumnID: colID}},
			}},
		},
		{
			Kind: crdt.OpAddColumn, TableID: tabID,
			Columns: []crdt.CatalogColumn{{
				ID: extraID, Name: "title", Ordinal: 1, Type: "TEXT", ClockGroup: "row",
			}},
		},
		{
			// Depends on both sub-ops above.
			Kind: crdt.OpCreateIndex, ObjectName: "idx_posts_title",
			RawSQL: "CREATE INDEX idx_posts_title ON posts(title)",
		},
	}}
	encoded, err := crdt.EncodeCatalogOp(bundle)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if _, err := f.log.Append(context.Background(), 0, encoded, "multi-DDL txn"); err != nil {
		t.Fatalf("log.Append: %v", err)
	}

	if err := f.br.runSchemaCatchup(context.Background()); err != nil {
		t.Fatalf("runSchemaCatchup: %v", err)
	}
	if got := f.localSchemaSeq(t); got != 1 {
		t.Fatalf("schema_seq = %d; want 1 (one event for the whole transaction)", got)
	}
	if !tableExistsInSQLite(t, f.app, "posts") {
		t.Fatal("sqlite_master missing table 'posts'")
	}
	ok, err := sqlitebridge.ObjectExists(f.app, "index", "idx_posts_title")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("sqlite_master missing index 'idx_posts_title'")
	}
	tab, ok := f.cat.Table("posts")
	if !ok {
		t.Fatal("catalog missing table 'posts'")
	}
	if _, ok := tab.Column("title"); !ok {
		t.Error("catalog missing column 'title' added by the bundle")
	}
	colExists, err := sqlitebridge.ColumnExists(f.app, "posts", "title")
	if err != nil {
		t.Fatal(err)
	}
	if !colExists {
		t.Error("app.db missing column 'title' added by the bundle")
	}
}

// A bundle that fails partway must leave no partial structural state and
// must not advance schema_seq — the SAVEPOINT wrapper is what guarantees
// the receiver never half-applies a transaction's schema change.
func TestRunSchemaCatchup_PartialBundleFailureRollsBack(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)

	tabID, colID := catalog.AllocTableID(), catalog.AllocColumnID()
	bundle := crdt.CatalogOp{Kind: crdt.OpBundle, SubOps: []crdt.CatalogOp{
		{
			Kind: crdt.OpCreateTable, TableID: tabID, TableName: "good",
			Columns: []crdt.CatalogColumn{{
				ID: colID, Name: "id", Ordinal: 0, Type: "BLOB",
				NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row",
			}},
			Keys: []crdt.CatalogKey{{
				KeyID: crdt.KeyID{}, Members: []crdt.CatalogKeyMember{{ColumnID: colID}},
			}},
		},
		{
			Kind: crdt.OpCreateIndex, ObjectName: "idx_bad",
			RawSQL: "CREATE INDEX idx_bad ON nonexistent_table(col)",
		},
	}}
	encoded, err := crdt.EncodeCatalogOp(bundle)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if _, err := f.log.Append(context.Background(), 0, encoded, "bad bundle"); err != nil {
		t.Fatalf("log.Append: %v", err)
	}

	_ = f.br.runSchemaCatchup(context.Background())

	if got := f.localSchemaSeq(t); got != 0 {
		t.Errorf("schema_seq = %d; want 0 (failed bundle must not advance)", got)
	}
	if tableExistsInSQLite(t, f.app, "good") {
		t.Error("partial bundle state survived: table 'good' exists")
	}
}

// The structural half of an event can land while the metadata half fails
// (crash, disk error); catch-up then retries the whole op against a
// catalog that still knows nothing about it. Replaying a bundle sub-op by
// sub-op must not resurrect an object a later sub-op renamed away.
func TestApplyCatalogStructural_BundleRetryIsIdempotent(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	create, tabID, _ := createTableOp("t")
	bundle := crdt.CatalogOp{Kind: crdt.OpBundle, SubOps: []crdt.CatalogOp{
		create,
		{Kind: crdt.OpRenameTable, TableID: tabID, TableName: "t2"},
	}}
	if err := f.br.applyCatalogStructural(bundle); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if tableExistsInSQLite(t, f.app, "t") || !tableExistsInSQLite(t, f.app, "t2") {
		t.Fatal("setup: want only t2 after the first apply")
	}
	// Retry with the catalog still empty — the metadata half never ran.
	if err := f.br.applyCatalogStructural(bundle); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if tableExistsInSQLite(t, f.app, "t") {
		t.Error("retry resurrected the intermediate table t")
	}
	if !tableExistsInSQLite(t, f.app, "t2") {
		t.Error("t2 lost on retry")
	}
}
