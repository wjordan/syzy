package postgres

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/schemalog"
)

// openDDLEngine opens an engine with DDL capture enabled (§6 increment A): the
// syzy_ddl_intent spool + ddl_command_end/sql_drop triggers are installed, so
// ordinary DDL run after Open writes structured intent rows.
func openDDLEngine(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID) *Engine {
	t.Helper()
	createTestDB(t, ctx, db, schemaKV)
	e, err := Open(ctx, Config{
		Name:        db,
		Origin:      origin,
		Cluster:     cluster,
		Cache:       nodestate.New(origin),
		ConnURL:     dbURL(db),
		ReplConnURL: replURL(db),
		Publication: "syzy_pub",
		Slot:        "syzy_slot_" + db,
		OriginName:  "syzy_origin_" + db,
		Tables:      []string{"public.kv"},
		DDL:         true,
	})
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	return e
}

// openDDLEngineLog opens a DDL engine wired to a shared schema log (§6
// increment D): captured DDL is appended as Bundles and a follower applies log
// events via catchUpSchema.
func openDDLEngineLog(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, ordinal uint16, log schemalog.Log) *Engine {
	return openDDLEngineLogMeta(t, ctx, db, origin, cluster, ordinal, log, nil)
}

// openDDLEngineLogMeta is openDDLEngineLog with a durable metadata store, so the
// node persists its schema catalog (§6 F) and a restart can rebuild it.
func openDDLEngineLogMeta(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, ordinal uint16, log schemalog.Log, meta *metadata.Store) *Engine {
	t.Helper()
	createTestDB(t, ctx, db, schemaKV)
	e, err := Open(ctx, Config{
		Name:        db,
		Origin:      origin,
		Cluster:     cluster,
		NodeOrdinal: ordinal,
		Cache:       nodestate.New(origin),
		ConnURL:     dbURL(db),
		ReplConnURL: replURL(db),
		Publication: "syzy_pub",
		Slot:        "syzy_slot_" + db,
		OriginName:  "syzy_origin_" + db,
		Tables:      []string{"public.kv"},
		DDL:         true,
		SchemaLog:   log,
		Meta:        meta,
	})
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	return e
}

// openLeaseEngine opens a DDL engine wired to a shared schema log AND a shared
// DDL lease (§6 increment E): the ddl_command_start gate serializes cross-node
// DDL so multiple originators' appends never conflict.
func openLeaseEngine(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, log schemalog.Log, lease Lease) *Engine {
	t.Helper()
	createTestDB(t, ctx, db, schemaKV)
	e, err := Open(ctx, Config{
		Name:        db,
		Origin:      origin,
		Cluster:     cluster,
		Cache:       nodestate.New(origin),
		ConnURL:     dbURL(db),
		ReplConnURL: replURL(db),
		Publication: "syzy_pub",
		Slot:        "syzy_slot_" + db,
		OriginName:  "syzy_origin_" + db,
		Tables:      []string{"public.kv"},
		DDL:         true,
		SchemaLog:   log,
		Lease:       lease,
	})
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	return e
}

type intentRow struct {
	txid       int64
	ordinal    int
	commandTag string
	objectType string
	objsubid   int32
	isDrop     bool
}

// ddlIntentRows reads syzy_ddl_intent directly (ordered by seq), so a test can
// assert what the triggers wrote without driving capture.
func ddlIntentRows(t *testing.T, db string) []intentRow {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `SELECT txid, ordinal, command_tag, COALESCE(object_type,''), objsubid, is_drop
	                            FROM syzy_ddl_intent ORDER BY seq`)
	if err != nil {
		t.Fatalf("query intent: %v", err)
	}
	defer rows.Close()
	var out []intentRow
	for rows.Next() {
		var r intentRow
		if err := rows.Scan(&r.txid, &r.ordinal, &r.commandTag, &r.objectType, &r.objsubid, &r.isDrop); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// TestDDLIntentRowsWritten: the event triggers persist one structured intent row
// per top-level DDL command, and sql_drop records the directly-dropped object.
// NB: PostgreSQL reports a PRIMARY KEY as its own implicit CREATE INDEX
// command-end event, distinct from the CREATE TABLE — load-bearing for the
// CatalogOp build (increment C must fold the implicit PK index into the table).
func TestDDLIntentRowsWritten(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_rows", 60, crdt.ClusterID{0xd1})
	defer closeEngine(t, ctx, e)

	appExec(t, "syzy_ddl_rows", `CREATE TABLE public.t (id bigint PRIMARY KEY, a text)`)
	appExec(t, "syzy_ddl_rows", `ALTER TABLE public.t ADD COLUMN b int`)
	appExec(t, "syzy_ddl_rows", `CREATE INDEX ix_t_a ON public.t (a)`)
	appExec(t, "syzy_ddl_rows", `DROP TABLE public.t`)

	rows := ddlIntentRows(t, "syzy_ddl_rows")
	tags := map[string]int{}
	var sawTableDrop bool
	for _, r := range rows {
		if r.isDrop {
			if r.objectType == "table" {
				sawTableDrop = true
			}
			continue
		}
		tags[r.commandTag]++
		if r.objsubid != 0 {
			t.Errorf("command-end row has objsubid=%d, want 0 (per-command, not per-sub-object): %+v", r.objsubid, r)
		}
	}
	if tags["CREATE TABLE"] != 1 {
		t.Errorf("CREATE TABLE rows = %d, want 1: %+v", tags["CREATE TABLE"], rows)
	}
	if tags["ALTER TABLE"] != 1 {
		t.Errorf("ALTER TABLE rows = %d, want 1: %+v", tags["ALTER TABLE"], rows)
	}
	if tags["CREATE INDEX"] != 2 { // the implicit t_pkey + the explicit ix_t_a
		t.Errorf("CREATE INDEX rows = %d, want 2 (implicit PK index + explicit): %+v", tags["CREATE INDEX"], rows)
	}
	if !sawTableDrop {
		t.Errorf("no sql_drop row for DROP TABLE: %+v", rows)
	}
}

// TestDDLIntentMultiStatementTxn: a multi-statement migration accumulates one
// ordinal-tagged row per command, all sharing one txid (the grouping key).
func TestDDLIntentMultiStatementTxn(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_multi", 61, crdt.ClusterID{0xd2})
	defer closeEngine(t, ctx, e)

	// CREATE TABLE reports two command-end rows (the table and its implicit PK
	// index), then the ALTER: three commands, one transaction.
	appTxn(t, "syzy_ddl_multi",
		`CREATE TABLE public.m (id bigint PRIMARY KEY)`,
		`ALTER TABLE public.m ADD COLUMN x int`)

	rows := ddlIntentRows(t, "syzy_ddl_multi")
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3: %+v", len(rows), rows)
	}
	for i, r := range rows {
		if r.txid != rows[0].txid {
			t.Errorf("row %d txid %d, want %d (co-transactional)", i, r.txid, rows[0].txid)
		}
		if r.ordinal != i {
			t.Errorf("row %d ordinal = %d, want %d", i, r.ordinal, i)
		}
	}
}

// TestDDLIntentRollbackDiscards: the intent is co-transactional, so ROLLBACK
// leaves no phantom row.
func TestDDLIntentRollbackDiscards(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_rb", 62, crdt.ClusterID{0xd3})
	defer closeEngine(t, ctx, e)

	c, err := pgx.Connect(ctx, dbURL("syzy_ddl_rb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE public.r (id bigint PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rows := ddlIntentRows(t, "syzy_ddl_rb"); len(rows) != 0 {
		t.Fatalf("got %d intent rows after rollback, want 0: %+v", len(rows), rows)
	}
}

// TestDDLIntentCapturedAndPruned: capture decodes the intent rows into
// descriptors (round-trip: trigger-written → WAL-decoded → Go struct) and prunes
// the consumed rows.
func TestDDLIntentCapturedAndPruned(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_cap", 63, crdt.ClusterID{0xd4})
	defer closeEngine(t, ctx, e)

	var got []ddlIntent
	e.capt.onDDLIntents = func(_ context.Context, ds []ddlIntent) error {
		got = append(got, ds...)
		return nil
	}

	// CREATE TABLE reports the table and its implicit PK index; then the ALTER.
	appExec(t, "syzy_ddl_cap", `CREATE TABLE public.c (id bigint PRIMARY KEY, a text)`)
	appExec(t, "syzy_ddl_cap", `ALTER TABLE public.c ADD COLUMN b int`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if len(got) != 3 {
		t.Fatalf("captured %d DDL descriptors, want 3: %+v", len(got), got)
	}
	if got[0].commandTag != "CREATE TABLE" || got[0].objid == 0 || got[0].classid == 0 {
		t.Errorf("descriptor0 = %+v, want CREATE TABLE with non-zero classid/objid", got[0])
	}
	if got[2].commandTag != "ALTER TABLE" {
		t.Errorf("descriptor2 = %+v, want ALTER TABLE", got[2])
	}
	// Consumed rows are pruned.
	if rows := ddlIntentRows(t, "syzy_ddl_cap"); len(rows) != 0 {
		t.Fatalf("got %d intent rows after capture, want 0 (pruned): %+v", len(rows), rows)
	}
}

// TestBuildCatalogOpCreateDropTable: capture builds a typed CatalogOp from
// pg_catalog (increment C). CREATE TABLE … PRIMARY KEY allocates a fresh stable
// TableID/ColumnIDs and folds the implicit PK index into the one OpCreateTable
// (no stray CreateIndex); DROP TABLE resolves the same allocated TableID back
// through the OID⇄ID map.
func TestBuildCatalogOpCreateDropTable(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_build", 65, crdt.ClusterID{0xd6})
	defer closeEngine(t, ctx, e)

	conn, err := pgx.Connect(ctx, dbURL("syzy_ddl_build"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	var ops []crdt.CatalogOp
	e.capt.onDDLIntents = func(ctx context.Context, ds []ddlIntent) error {
		built, err := buildCatalogOps(ctx, conn, e.cat, ds)
		if err != nil {
			return err
		}
		ops = append(ops, built...)
		return nil
	}

	// Drain after each DDL txn, modelling the continuously-running orchestrator:
	// CREATE TABLE is introspected while the table exists; a batch capture would
	// instead see the post-DROP catalog and fail to build the create op (capture
	// is post-commit, so it always reads the *current* catalog — see the
	// build-from-live-catalog note in ddl_catalog.go / the increments plan).
	appExec(t, "syzy_ddl_build", `CREATE TABLE public.t (id bigint PRIMARY KEY, a text)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_build", `DROP TABLE public.t`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if len(ops) != 2 {
		t.Fatalf("built %d ops, want 2 (create + drop): %+v", len(ops), ops)
	}
	create, drop := ops[0], ops[1]

	if create.Kind != crdt.OpCreateTable {
		t.Fatalf("op0 kind = %v, want CreateTable", create.Kind)
	}
	if create.TableName != "t" {
		t.Errorf("table name = %q, want \"t\"", create.TableName)
	}
	if create.TableID == (crdt.TableID{}) {
		t.Errorf("CreateTable TableID is zero, want a fresh allocated id")
	}
	if len(create.Columns) != 2 {
		t.Fatalf("columns = %d, want 2: %+v", len(create.Columns), create.Columns)
	}
	id, a := create.Columns[0], create.Columns[1]
	if id.Name != "id" || id.Type != "bigint" || !id.IsPK || id.PKPos != 1 || !id.NotNull {
		t.Errorf("col id = %+v, want bigint PK NOT NULL pkpos=1", id)
	}
	if a.Name != "a" || a.Type != "text" || a.IsPK {
		t.Errorf("col a = %+v, want text non-PK", a)
	}
	if id.ID == (crdt.ColumnID{}) || a.ID == (crdt.ColumnID{}) {
		t.Errorf("column IDs not allocated: id=%v a=%v", id.ID, a.ID)
	}
	// One PK key, the implicit index folded in (no separate CreateIndex op).
	if len(create.Keys) != 1 || len(create.Keys[0].Members) != 1 {
		t.Fatalf("keys = %+v, want one PK key with one member", create.Keys)
	}
	if create.Keys[0].KeyID != (crdt.KeyID{}) {
		t.Errorf("PK KeyID = %v, want all-zero (PKKeyID)", create.Keys[0].KeyID)
	}
	if m := create.Keys[0].Members[0]; m.ColumnID != id.ID || m.Ordinal != 0 {
		t.Errorf("PK member = %+v, want id column at ordinal 0", m)
	}

	if drop.Kind != crdt.OpDropTable {
		t.Fatalf("op1 kind = %v, want DropTable", drop.Kind)
	}
	if drop.TableID != create.TableID {
		t.Errorf("drop TableID = %v, want the created %v (resolved via the OID⇄ID map)", drop.TableID, create.TableID)
	}
	// The map dropped it (both indexes trimmed).
	if e.cat.byID[create.TableID] != nil {
		t.Errorf("dropped table still in the catalog by id")
	}
}

// catalogOpCollector wires onDDLIntents to build CatalogOps from each captured
// DDL txn (increment C), appending them to *ops and recording the first build
// error in *firstErr (returning nil so capture does not halt — the test asserts
// the error instead).
func catalogOpCollector(t *testing.T, ctx context.Context, e *Engine, db string, ops *[]crdt.CatalogOp, firstErr *error) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	e.capt.onDDLIntents = func(ctx context.Context, ds []ddlIntent) error {
		built, err := buildCatalogOps(ctx, conn, e.cat, ds)
		if err != nil {
			if *firstErr == nil {
				*firstErr = err
			}
			return nil
		}
		*ops = append(*ops, built...)
		return nil
	}
}

// TestBuildCatalogOpAlterTable: the ALTER catalog-diff (§6 risk #2) reconstructs
// ADD/RENAME/DROP column and RENAME table from pg_attribute, keying on attnum so
// a rename preserves the stable ColumnID. Each ALTER is captured before the next
// runs (the diff reads the current catalog).
func TestBuildCatalogOpAlterTable(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_alter", 66, crdt.ClusterID{0xd7})
	defer closeEngine(t, ctx, e)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, "syzy_ddl_alter", &ops, &buildErr)

	appExec(t, "syzy_ddl_alter", `CREATE TABLE public.u (id bigint PRIMARY KEY, a text)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_alter", `ALTER TABLE public.u ADD COLUMN b int`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_alter", `ALTER TABLE public.u RENAME COLUMN a TO a2`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_alter", `ALTER TABLE public.u DROP COLUMN b`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_alter", `ALTER TABLE public.u RENAME TO u2`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if buildErr != nil {
		t.Fatalf("unexpected build error: %v", buildErr)
	}
	if len(ops) != 5 {
		t.Fatalf("built %d ops, want 5 (create,add,rename-col,drop,rename-table): %+v", len(ops), ops)
	}
	create, add, renCol, drop, renTab := ops[0], ops[1], ops[2], ops[3], ops[4]

	// id of column "a" from CREATE, and "b" from ADD, to verify rename/drop target
	// the right stable id.
	var aID crdt.ColumnID
	for _, c := range create.Columns {
		if c.Name == "a" {
			aID = c.ID
		}
	}
	if aID == (crdt.ColumnID{}) {
		t.Fatalf("column a not found in create op: %+v", create)
	}

	if add.Kind != crdt.OpAddColumn || len(add.Columns) != 1 {
		t.Fatalf("add op = %+v, want one AddColumn", add)
	}
	bCol := add.Columns[0]
	if bCol.Name != "b" || bCol.Type != "integer" || bCol.ID == (crdt.ColumnID{}) {
		t.Errorf("added column = %+v, want integer \"b\" with allocated id", bCol)
	}
	if add.TableID != create.TableID {
		t.Errorf("add op TableID = %v, want %v", add.TableID, create.TableID)
	}

	if renCol.Kind != crdt.OpRenameColumn || renCol.ColumnID != aID || renCol.ColumnName != "a2" {
		t.Errorf("rename-col op = %+v, want RenameColumn of a's id to a2", renCol)
	}

	if drop.Kind != crdt.OpDropColumn || drop.ColumnID != bCol.ID {
		t.Errorf("drop op = %+v, want DropColumn of b's id %v", drop, bCol.ID)
	}

	if renTab.Kind != crdt.OpRenameTable || renTab.TableID != create.TableID || renTab.TableName != "u2" {
		t.Errorf("rename-table op = %+v, want RenameTable to u2", renTab)
	}
}

// TestBuildCatalogOpAlterRenameOntoDroppedName: "DROP COLUMN a; RENAME COLUMN b
// TO a" in one txn. Because capture is post-commit the first intent's diff sees
// the final catalog (a gone, b now named "a"); the diff must drop a's stable id
// and rename b's stable id to "a" without corrupting the name index.
func TestBuildCatalogOpAlterRenameOntoDroppedName(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_collide", 68, crdt.ClusterID{0xd9})
	defer closeEngine(t, ctx, e)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, "syzy_ddl_collide", &ops, &buildErr)

	appExec(t, "syzy_ddl_collide", `CREATE TABLE public.x (id bigint PRIMARY KEY, a text, b text)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appTxn(t, "syzy_ddl_collide",
		`ALTER TABLE public.x DROP COLUMN a`,
		`ALTER TABLE public.x RENAME COLUMN b TO a`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if buildErr != nil {
		t.Fatalf("unexpected build error: %v", buildErr)
	}
	var aID, bID crdt.ColumnID
	for _, c := range ops[0].Columns {
		switch c.Name {
		case "a":
			aID = c.ID
		case "b":
			bID = c.ID
		}
	}
	// Among the post-create ops there must be a DropColumn of a's id and a
	// RenameColumn of b's id to "a".
	var sawDropA, sawRenameBtoA bool
	for _, op := range ops[1:] {
		if op.Kind == crdt.OpDropColumn && op.ColumnID == aID {
			sawDropA = true
		}
		if op.Kind == crdt.OpRenameColumn && op.ColumnID == bID && op.ColumnName == "a" {
			sawRenameBtoA = true
		}
	}
	if !sawDropA || !sawRenameBtoA {
		t.Fatalf("ops missing drop-a (%v) or rename-b-to-a (%v): %+v", sawDropA, sawRenameBtoA, ops)
	}
	// The name index now resolves "a" to b's stable id (no corruption), and a's
	// id is gone.
	ti := e.cat.byID[ops[0].TableID]
	if ti == nil {
		t.Fatal("table missing from catalog")
	}
	if ci := ti.byName["a"]; ci == nil || ci.cid != bID {
		t.Errorf("byName[\"a\"] = %+v, want b's id %v", ci, bID)
	}
	if _, ok := ti.byName["b"]; ok {
		t.Errorf("byName still has stale \"b\" entry")
	}
}

// TestBuildCatalogOpAlterRejectsUnsupported: the post-commit floor. With the
// pre-command snapshot disabled — as on a node whose DDL support was installed
// after the table already existed — admission has no prior shape to judge
// against and lets the narrowing ALTER commit. Capture must then reject it
// rather than silently drop it (which would diverge).
func TestBuildCatalogOpAlterRejectsUnsupported(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_reject", 67, crdt.ClusterID{0xd8})
	defer closeEngine(t, ctx, e)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, "syzy_ddl_reject", &ops, &buildErr)

	appExec(t, "syzy_ddl_reject", `CREATE TABLE public.w (id bigint PRIMARY KEY, a text)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_reject", `ALTER EVENT TRIGGER syzy_ddl_snapshot DISABLE`)
	appExec(t, "syzy_ddl_reject", `ALTER TABLE public.w ALTER COLUMN a TYPE varchar(20)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if buildErr == nil {
		t.Fatalf("expected a rejection for ALTER COLUMN TYPE, got none (ops=%+v)", ops)
	}
}

// TestBuildCatalogOpRejectsNonPKGeneratedAlwaysIdentity: a GENERATED ALWAYS AS
// IDENTITY column that is not the PK mints node-local values and cannot be
// UPDATEd, so concurrent same-PK inserts would diverge on it — buildCatalogOps
// rejects the shape rather than replicate a non-convergent column.
func TestBuildCatalogOpRejectsNonPKGeneratedAlwaysIdentity(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_genalways", 68, crdt.ClusterID{0xd9})
	defer closeEngine(t, ctx, e)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, "syzy_ddl_genalways", &ops, &buildErr)

	appExec(t, "syzy_ddl_genalways",
		`CREATE TABLE public.g (id text PRIMARY KEY, n bigint GENERATED ALWAYS AS IDENTITY)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if buildErr == nil {
		t.Fatalf("expected rejection of a non-PK GENERATED ALWAYS identity column, got none (ops=%+v)", ops)
	}
}

// TestBuildCatalogOpOwnedSequenceNotSerial: a column that OWNS a sequence
// (CREATE SEQUENCE … OWNED BY) but has no nextval default is NOT serial — the
// built op must keep the column plain and not hand a follower a nextval default
// the originator never had. Both run in one txn so the create op is built
// post-commit when the column already owns the sequence.
func TestBuildCatalogOpOwnedSequenceNotSerial(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_ownedseq", 69, crdt.ClusterID{0xdb})
	defer closeEngine(t, ctx, e)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, "syzy_ddl_ownedseq", &ops, &buildErr)

	appTxn(t, "syzy_ddl_ownedseq",
		`CREATE TABLE public.os (id bigint PRIMARY KEY)`,
		`CREATE SEQUENCE public.os_s OWNED BY public.os.id`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if buildErr != nil {
		t.Fatalf("unexpected build error: %v", buildErr)
	}
	if len(ops) != 1 || ops[0].Kind != crdt.OpCreateTable {
		t.Fatalf("want exactly 1 create op (the owned-sequence intent folded), got %+v", ops)
	}
	for _, c := range ops[0].Columns {
		if c.Name != "id" {
			continue
		}
		if c.Type != "bigint" {
			t.Errorf("id Type = %q, want bigint (owns a sequence but has no nextval default → not serial)", c.Type)
		}
		if c.Default != "" {
			t.Errorf("id Default = %q, want empty (the originator has no nextval default)", c.Default)
		}
	}
}

// TestSchemaLogReplication: a DDL transaction on the originator becomes a Bundle
// in the shared schema log, and the follower converges by applying log events
// (§6 increment D). Exercises the full append→read→apply round-trip.
func TestSchemaLogReplication(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	a := openDDLEngineLog(t, ctx, "syzy_sl_origin", 72, crdt.ClusterID{0xe0}, 0, log)
	defer closeEngine(t, ctx, a)
	b := openDDLEngineLog(t, ctx, "syzy_sl_follow", 73, crdt.ClusterID{0xe0}, 0, log)
	defer closeEngine(t, ctx, b)

	// Originator: each DDL txn is captured and appended as a Bundle.
	appExec(t, "syzy_sl_origin", `CREATE TABLE public.t (id bigint PRIMARY KEY, a text)`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)
	appExec(t, "syzy_sl_origin", `ALTER TABLE public.t ADD COLUMN b int`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)

	if got := a.schemaSeq.Load(); got != 2 {
		t.Fatalf("origin schemaSeq = %d, want 2 (create + alter)", got)
	}
	if head, _ := log.Head(ctx); head != 2 {
		t.Fatalf("log head = %d, want 2", head)
	}

	// Follower converges by catch-up.
	if err := b.catchUpSchema(ctx); err != nil {
		t.Fatalf("follower catch-up: %v", err)
	}
	if got := b.schemaSeq.Load(); got != 2 {
		t.Errorf("follower schemaSeq = %d, want 2", got)
	}
	if cols := pgColumnNames(t, "syzy_sl_follow", "t"); !equalStrings(cols, []string{"id", "a", "b"}) {
		t.Errorf("follower columns = %v, want [id a b]", cols)
	}
	// Both nodes bound the table to the SAME stable id (allocated once on the
	// originator, distributed via the log).
	var tid crdt.TableID
	for id := range a.cat.byID {
		if a.cat.byID[id].name == "t" {
			tid = id
		}
	}
	if tid == (crdt.TableID{}) || b.cat.byID[tid] == nil || b.cat.byID[tid].name != "t" {
		t.Errorf("follower missing table under originator's stable id %x", tid)
	}

	// A drop replicates the same way.
	appExec(t, "syzy_sl_origin", `DROP TABLE public.t`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)
	if err := b.catchUpSchema(ctx); err != nil {
		t.Fatalf("follower catch-up (drop): %v", err)
	}
	if cols := pgColumnNames(t, "syzy_sl_follow", "t"); len(cols) != 0 {
		t.Errorf("follower still has table t after drop: %v", cols)
	}
}

// TestDMLOnDDLCreatedTableCarriesSchemaDep: after a replicated CREATE TABLE,
// capture resolves DML on the new table through the OID⇄ID map (its id is
// allocated, not name-derived) and stamps the changeset's Deps[SchemaChain] with
// the schema event the table was created under — the dependency a follower's
// gate (a later increment) holds the DML on.
func TestDMLOnDDLCreatedTableCarriesSchemaDep(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	e := openDDLEngineLog(t, ctx, "syzy_dep", 74, crdt.ClusterID{0xe1}, 0, log)
	defer closeEngine(t, ctx, e)

	appExec(t, "syzy_dep", `CREATE TABLE public.evt (id bigint PRIMARY KEY, msg text)`)
	appExec(t, "syzy_dep", `INSERT INTO public.evt VALUES (1, 'hello')`)
	css := captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if e.schemaSeq.Load() != 1 {
		t.Fatalf("schemaSeq = %d, want 1 (one CREATE bundle)", e.schemaSeq.Load())
	}
	// evt's allocated stable id.
	var tid crdt.TableID
	for id, ti := range e.cat.byID {
		if ti.name == "evt" {
			tid = id
		}
	}
	if tid == (crdt.TableID{}) {
		t.Fatal("evt not in catalog")
	}

	// The INSERT is captured (DDL-created table resolved via the map) and its
	// changeset depends on schema event 1.
	var found *crdt.Changeset
	for _, cs := range css {
		for _, r := range cs.Records {
			if ins, ok := r.(crdt.Insert); ok && ins.Table == tid {
				found = cs
			}
		}
	}
	if found == nil {
		t.Fatalf("no captured Insert on evt (id %x) — DML on a DDL-created table was dropped", tid)
	}
	if got := uint64(found.Deps[crdt.SchemaChain]); got != 1 {
		t.Errorf("changeset Deps[SchemaChain] = %d, want 1", got)
	}
}

// TestLiveDDLConvergence: two live orchestrators sharing a schema log. A runs
// plain CREATE TABLE then INSERT; B converges — its catalog catches up from the
// log (creating the table) and the gated DML applies (§6 D4b: live append +
// apply-side schema gate, race-free now that capture defers catalog reads).
func TestLiveDDLConvergence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	cluster := crdt.ClusterID{0xe2}
	a := openDDLEngineLog(t, ctx, "syzy_lddl_a", 80, cluster, 0, log)
	defer closeEngine(t, ctx, a)
	b := openDDLEngineLog(t, ctx, "syzy_lddl_b", 81, cluster, 0, log)
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

	appExec(t, "syzy_lddl_a", `CREATE TABLE public.evt (id bigint PRIMARY KEY, msg text)`)
	appExec(t, "syzy_lddl_a", `INSERT INTO public.evt VALUES (1,'a'),(2,'b'),(3,'c')`)

	want := map[int64]string{1: "a", 2: "b", 3: "c"}
	deadline := time.Now().Add(20 * time.Second)
	var got map[int64]string
	for time.Now().Before(deadline) {
		if got = idMsgRows(t, "syzy_lddl_b", "public.evt"); mapsEqual(got, want) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if !mapsEqual(got, want) {
		t.Fatalf("B did not converge: got %v want %v", got, want)
	}
}

// TestLiveBigserialDDLConvergence: a bigserial PK table created via DDL. The
// originator A (ordinal 1) mints in the reserved [1, 2^47) low range (it never
// partitions its own sequence); the follower B (ordinal 2) applies the replicated
// CREATE, which re-creates B's OWN owned sequence (the bigserial pseudo-type, not
// the originator's nextval default) and partitions it to slice 2.
func TestLiveBigserialDDLConvergence(t *testing.T) {
	requirePG(t)
	ra, rb := twoNodeAutoID(t, "syzy_bs", 82,
		`CREATE TABLE public.t (id bigserial PRIMARY KEY, msg text)`)
	assertAutoIDConverged(t, ra, rb)
}

// TestLiveIdentityDDLConvergence: the same story for GENERATED ALWAYS AS IDENTITY
// — the follower re-creates the identity (with its own internal sequence,
// partitioned to slice 2) and apply feeds the replicated ids with OVERRIDING
// SYSTEM VALUE, which a GENERATED ALWAYS column otherwise rejects.
func TestLiveIdentityDDLConvergence(t *testing.T) {
	requirePG(t)
	ra, rb := twoNodeAutoID(t, "syzy_id", 84,
		`CREATE TABLE public.t (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, msg text)`)
	assertAutoIDConverged(t, ra, rb)
}

// twoNodeAutoID runs the §6 increment-B auto-increment convergence scenario for a
// public.t (id <auto> PRIMARY KEY, msg text) table created by createDDL: A
// (ordinal 1) runs the DDL and inserts 3 rows without ids; once B converges (so
// B applied the CREATE and partitioned its sequence) B inserts 2 more; A then
// re-converges. It returns both nodes' final id→msg maps.
func twoNodeAutoID(t *testing.T, prefix string, originBase crdt.Origin, createDDL string) (ra, rb map[int64]string) {
	t.Helper()
	ctx := context.Background()
	log := schemalog.NewLocal()
	cluster := crdt.ClusterID{byte(originBase)}
	dbA, dbB := prefix+"_a", prefix+"_b"
	a := openDDLEngineLog(t, ctx, dbA, originBase, cluster, 1, log)
	defer closeEngine(t, ctx, a)
	b := openDDLEngineLog(t, ctx, dbB, originBase+1, cluster, 2, log)
	defer closeEngine(t, ctx, b)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
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

	appExec(t, dbA, createDDL)
	appExec(t, dbA, `INSERT INTO public.t (msg) VALUES ('a'),('b'),('c')`)
	if !waitRows(t, dbB, "public.t", 3, 20*time.Second) {
		t.Fatalf("B did not converge on A's 3 rows: %v", idMsgRows(t, dbB, "public.t"))
	}
	appExec(t, dbB, `INSERT INTO public.t (msg) VALUES ('d'),('e')`)
	if !waitRows(t, dbA, "public.t", 5, 20*time.Second) {
		t.Fatalf("A did not converge on 5 rows: %v", idMsgRows(t, dbA, "public.t"))
	}
	return idMsgRows(t, dbA, "public.t"), idMsgRows(t, dbB, "public.t")
}

// assertAutoIDConverged checks both nodes hold the same 5 rows with A's 3 ids in
// the reserved low range and B's 2 in slice 2 (disjoint — no PK collision).
func assertAutoIDConverged(t *testing.T, ra, rb map[int64]string) {
	t.Helper()
	if !mapsEqual(ra, rb) {
		t.Fatalf("A and B diverged: A=%v B=%v", ra, rb)
	}
	if len(ra) != 5 {
		t.Fatalf("want 5 converged rows, got %d: %v", len(ra), ra)
	}
	lo2, _ := idSlice(2)
	var low, sliced int
	for id := range ra {
		if uint64(id) < lo2 {
			low++
		} else {
			sliced++
		}
	}
	if low != 3 || sliced != 2 {
		t.Fatalf("partition split = %d low-range + %d slice-2 (want 3+2): %v", low, sliced, ra)
	}
}

// TestDDLGateAdmitsAndReleases (diagnostic): one node, one gated CREATE. The
// gate must admit it (the appExec returns) and then RELEASE the lease once the
// DDL is appended (intent spool drains), so a peer could take over.
func TestDDLGateAdmitsAndReleases(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	lease := NewMemLease(5 * time.Second)
	a := openLeaseEngine(t, ctx, "syzy_gate1", 90, crdt.ClusterID{0xe6}, log, lease)
	defer closeEngine(t, ctx, a)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	wg.Add(1)
	go func() {
		defer wg.Done()
		bcast := func(ctx context.Context, cs *crdt.Changeset) error { return nil }
		if err := a.Run(runCtx, make(chan *crdt.Changeset), bcast); err != nil && runCtx.Err() == nil {
			t.Errorf("orch: %v", err)
		}
	}()

	appExec(t, "syzy_gate1", `CREATE TABLE public.g1 (id bigint PRIMARY KEY)`)
	holder := func() string { lease.mu.Lock(); defer lease.mu.Unlock(); return lease.holder }
	t.Logf("after gated CREATE: lease holder=%q", holder())
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && holder() != "" {
		time.Sleep(50 * time.Millisecond)
	}
	if holder() != "" {
		t.Fatalf("lease not released after gated DDL appended: holder=%q", holder())
	}
}

// TestLiveDDLLeaseSerializes: two nodes both originate DDL (A creates ta, B
// creates tb). The DDL lease serializes them — each node's ddl_command_start
// gate holds the lease and catches its schema log up to head before its CREATE
// commits, so the appends land at consecutive parents instead of conflicting.
// Without the gate B would append `tb` at parent 0 while head is 1 (A's `ta`) →
// ErrHeadMoved and a halted capture. Both nodes converge on both tables.
func TestLiveDDLLeaseSerializes(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	lease := NewMemLease(5 * time.Second)
	cluster := crdt.ClusterID{0xe5}
	a := openLeaseEngine(t, ctx, "syzy_lease_a", 86, cluster, log, lease)
	defer closeEngine(t, ctx, a)
	b := openLeaseEngine(t, ctx, "syzy_lease_b", 87, cluster, log, lease)
	defer closeEngine(t, ctx, b)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
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

	// A originates ta (acquires the lease, appends at seq 1, releases). B's CREATE
	// then blocks on its gate until it can take the lease, catches up to seq 1
	// (materializing ta on B), and appends tb at seq 2.
	appExec(t, "syzy_lease_a", `CREATE TABLE public.ta (id bigint PRIMARY KEY, msg text)`)
	appExec(t, "syzy_lease_a", `INSERT INTO public.ta VALUES (1,'a')`)
	// A must release the lease once ta is appended (intent spool drained), or B
	// could never take the lease for its own DDL.
	holder := func() string { lease.mu.Lock(); defer lease.mu.Unlock(); return lease.holder }
	rel := time.Now().Add(8 * time.Second)
	for time.Now().Before(rel) && holder() != "" {
		time.Sleep(50 * time.Millisecond)
	}
	if h := holder(); h != "" {
		head, _ := log.Head(ctx)
		t.Fatalf("A did not release the lease after its DDL: holder=%q, log head=%d", h, head)
	}
	appExec(t, "syzy_lease_b", `CREATE TABLE public.tb (id bigint PRIMARY KEY, msg text)`)
	appExec(t, "syzy_lease_b", `INSERT INTO public.tb VALUES (1,'b')`)

	converged := func() bool {
		return len(idMsgRows(t, "syzy_lease_a", "public.ta")) == 1 &&
			len(idMsgRows(t, "syzy_lease_a", "public.tb")) == 1 &&
			len(idMsgRows(t, "syzy_lease_b", "public.ta")) == 1 &&
			len(idMsgRows(t, "syzy_lease_b", "public.tb")) == 1
	}
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) && !converged() {
		time.Sleep(50 * time.Millisecond)
	}
	if !converged() {
		t.Fatalf("did not converge on both tables: A.ta=%v A.tb=%v B.ta=%v B.tb=%v",
			idMsgRows(t, "syzy_lease_a", "public.ta"), idMsgRows(t, "syzy_lease_a", "public.tb"),
			idMsgRows(t, "syzy_lease_b", "public.ta"), idMsgRows(t, "syzy_lease_b", "public.tb"))
	}
	if head, err := log.Head(ctx); err != nil || head != 2 {
		t.Fatalf("schema log head = %d (err %v), want 2 (ta@1, tb@2 — appends serialized, no conflict)", head, err)
	}
}

// idMsgRows reads (id, msg) from an id/msg table, returning an empty map if the
// table does not exist yet on this node (a follower mid-catch-up).
func idMsgRows(t *testing.T, db, table string) map[int64]string {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	out := map[int64]string{}
	rows, err := c.Query(ctx, `SELECT id, msg FROM `+table+` ORDER BY id`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var msg string
		if err := rows.Scan(&id, &msg); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = msg
	}
	return out
}

// waitRows polls until table on db holds exactly n rows or within elapses.
func waitRows(t *testing.T, db, table string, n int, within time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(idMsgRows(t, db, table)) == n {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// pgColumnNames returns a table's live column names in attnum order, read fresh.
func pgColumnNames(t *testing.T, db, table string) []string {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `
		SELECT a.attname FROM pg_attribute a
		JOIN pg_class cl ON cl.oid = a.attrelid
		WHERE cl.relname = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, table)
	if err != nil {
		t.Fatalf("columns query: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, n)
	}
	return out
}

// TestApplyCatalogOpRoundTrip: ops built on node A from real DDL apply on a
// fresh node B (increment D, single node). B's physical schema converges and its
// OID⇄ID map binds B's local OID/columns to A's allocated stable ids — the #4
// foundation's apply half.
func TestApplyCatalogOpRoundTrip(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	a := openDDLEngine(t, ctx, "syzy_ddl_origin", 70, crdt.ClusterID{0xda})
	defer closeEngine(t, ctx, a)
	b := openDDLEngine(t, ctx, "syzy_ddl_follow", 71, crdt.ClusterID{0xda})
	defer closeEngine(t, ctx, b)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, a, "syzy_ddl_origin", &ops, &buildErr)

	appExec(t, "syzy_ddl_origin", `CREATE TABLE public.t (id bigint PRIMARY KEY, a text)`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)
	appExec(t, "syzy_ddl_origin", `ALTER TABLE public.t ADD COLUMN b int`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)
	if buildErr != nil || len(ops) != 2 {
		t.Fatalf("origin built ops=%d err=%v: %+v", len(ops), buildErr, ops)
	}
	createOp, addOp := ops[0], ops[1]

	// Apply both on B.
	if err := applyCatalogOp(ctx, b.appl.conn, b.cat, createOp, b.cfg.NodeOrdinal); err != nil {
		t.Fatalf("apply create on B: %v", err)
	}
	if err := applyCatalogOp(ctx, b.appl.conn, b.cat, addOp, b.cfg.NodeOrdinal); err != nil {
		t.Fatalf("apply add on B: %v", err)
	}

	// B's physical schema converged.
	if got := pgColumnNames(t, "syzy_ddl_follow", "t"); !equalStrings(got, []string{"id", "a", "b"}) {
		t.Errorf("B columns = %v, want [id a b]", got)
	}

	// B's OID⇄ID map binds A's allocated ids to B's local OID/attnums.
	bti := b.cat.byID[createOp.TableID]
	if bti == nil {
		t.Fatalf("B catalog missing table id %x", createOp.TableID)
	}
	if bti.oid == 0 || b.cat.byOID[bti.oid] != bti {
		t.Errorf("B table not indexed by its local OID (oid=%d)", bti.oid)
	}
	// Every column id from the ops resolves in B to a colInfo with a real attnum.
	wantCols := map[string]crdt.ColumnID{}
	for _, c := range createOp.Columns {
		wantCols[c.Name] = c.ID
	}
	wantCols[addOp.Columns[0].Name] = addOp.Columns[0].ID
	for name, cid := range wantCols {
		ci := bti.byName[name]
		if ci == nil || ci.cid != cid || ci.attnum == 0 {
			t.Errorf("B column %q = %+v, want stable id %v with a real attnum", name, ci, cid)
		}
	}

	// DROP TABLE replicates and trims B's map.
	appExec(t, "syzy_ddl_origin", `DROP TABLE public.t`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)
	if len(ops) != 3 || ops[2].Kind != crdt.OpDropTable {
		t.Fatalf("expected a drop op, got %+v", ops)
	}
	if err := applyCatalogOp(ctx, b.appl.conn, b.cat, ops[2], b.cfg.NodeOrdinal); err != nil {
		t.Fatalf("apply drop on B: %v", err)
	}
	if b.cat.byID[createOp.TableID] != nil {
		t.Errorf("B catalog still has dropped table")
	}
	if cols := pgColumnNames(t, "syzy_ddl_follow", "t"); len(cols) != 0 {
		t.Errorf("B still has table t physically: %v", cols)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDDLInternalGuardSuppresses: syzy.internal='on' excludes a command from the
// intent spool — the guard the partitioner's internal ALTER and apply-side DDL
// rely on (§6).
func TestDDLInternalGuardSuppresses(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_guard", 64, crdt.ClusterID{0xd5})
	defer closeEngine(t, ctx, e)

	appTxn(t, "syzy_ddl_guard",
		`SET LOCAL syzy.internal = 'on'`,
		`CREATE TABLE public.g (id bigint PRIMARY KEY)`)

	if rows := ddlIntentRows(t, "syzy_ddl_guard"); len(rows) != 0 {
		t.Fatalf("guard did not suppress: got %d intent rows: %+v", len(rows), rows)
	}
}

// TestSchemaCatalogPersistedToMetadata: with a metadata store configured, an
// originator's DDL is recorded in the durable metadata catalog (syzy_table /
// syzy_column rows + schema_seq) via the shared (*Tx).ApplyCatalogOp — the
// foundation for rebuilding the OID⇄ID map on restart (§6 F).
func TestSchemaCatalogPersistedToMetadata(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	meta, err := metadata.Open(filepath.Join(t.TempDir(), "schemacat.meta"))
	if err != nil {
		t.Fatalf("open meta: %v", err)
	}
	defer meta.Close()
	log := schemalog.NewLocal()
	e := openDDLEngineLogMeta(t, ctx, "syzy_schemacat", 90, crdt.ClusterID{0xea}, 0, log, meta)
	defer closeEngine(t, ctx, e)

	appExec(t, "syzy_schemacat", `CREATE TABLE public.widget (id bigint PRIMARY KEY, label text)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_schemacat", `ALTER TABLE public.widget ADD COLUMN qty int`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	// schema_seq advanced durably (CREATE + ALTER = 2 events).
	if seq, ok, err := meta.GetSchemaSeq(); err != nil || !ok || seq != 2 {
		t.Fatalf("GetSchemaSeq = (%d, %v, %v), want (2, true, nil)", seq, ok, err)
	}

	// The catalog rows reflect widget with its three active columns.
	snap, err := meta.LoadCatalogSnapshot()
	if err != nil {
		t.Fatalf("LoadCatalogSnapshot: %v", err)
	}
	var widget *metadata.TableEntry
	for i := range snap.Tables {
		if snap.Tables[i].Name == "widget" && snap.Tables[i].State == metadata.StateActive {
			widget = &snap.Tables[i]
		}
	}
	if widget == nil {
		t.Fatalf("widget not in persisted catalog: %+v", snap.Tables)
	}
	cols := map[string]bool{}
	for _, c := range snap.Columns {
		if c.TableID == widget.ID && c.State == metadata.StateActive {
			cols[c.Name] = true
		}
	}
	for _, want := range []string{"id", "label", "qty"} {
		if !cols[want] {
			t.Errorf("persisted catalog missing column %q (have %v)", want, cols)
		}
	}
}

// TestSchemaCatalogRestoredOnRestart: a node that created tables via DDL
// rebuilds their OID⇄stable-ID map (and schema_seq) from the durable metadata
// catalog on restart — without it, introspectCatalog covers only the bootstrap
// Config.Tables and the DDL-created tables would be lost (§6 F, slice 3).
func TestSchemaCatalogRestoredOnRestart(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const (
		db     = "syzy_schemarestart"
		origin = crdt.Origin(91)
	)
	cluster := crdt.ClusterID{0xeb}
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	log := schemalog.NewLocal()

	reopen := func(meta *metadata.Store) *Engine {
		t.Helper()
		e, err := Open(ctx, Config{
			Name: db, Origin: origin, Cluster: cluster,
			Cache:       nodestate.New(origin),
			ConnURL:     dbURL(db),
			ReplConnURL: replURL(db),
			Publication: "syzy_pub",
			Slot:        "syzy_slot_" + db,
			OriginName:  "syzy_origin_" + db,
			Tables:      []string{"public.kv"},
			DDL:         true,
			SchemaLog:   log,
			Meta:        meta,
		})
		if err != nil {
			t.Fatalf("reopen %s: %v", db, err)
		}
		return e
	}

	// run 1: create a DDL table + alter it; appendDDLBundle persists the catalog.
	meta1, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta1: %v", err)
	}
	e1 := openDDLEngineLogMeta(t, ctx, db, origin, cluster, 0, log, meta1)
	appExec(t, db, `CREATE TABLE public.gadget (id bigint PRIMARY KEY, name text)`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)
	appExec(t, db, `ALTER TABLE public.gadget ADD COLUMN qty int`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)

	var tid crdt.TableID
	for id, ti := range e1.cat.byID {
		if ti.name == "gadget" {
			tid = id
		}
	}
	if tid == (crdt.TableID{}) {
		t.Fatal("gadget not in catalog after create")
	}
	_ = e1.Close()
	if err := meta1.Close(); err != nil {
		t.Fatalf("meta1 close: %v", err)
	}
	waitSlotInactive(t, ctx, dbURL(db), "syzy_slot_"+db)

	// run 2: reopen against the same db + metadata file, WITHOUT recreating the
	// database. A fresh Cache resets schemaSeq to 0; restore must rebuild it.
	meta2, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta2: %v", err)
	}
	defer meta2.Close()
	e2 := reopen(meta2)
	defer closeEngine(t, ctx, e2)

	if got := e2.schemaSeq.Load(); got != 2 {
		t.Fatalf("restored schemaSeq = %d, want 2 (create + alter)", got)
	}
	ti := e2.cat.byID[tid]
	if ti == nil {
		t.Fatal("gadget not restored to catalog after restart — DDL-created table was lost")
	}
	if ti.name != "gadget" || ti.oid == 0 {
		t.Fatalf("restored gadget = name %q oid %d, want bound oid", ti.name, ti.oid)
	}
	if e2.cat.byOID[ti.oid] != ti {
		t.Fatal("restored gadget not indexed by oid")
	}
	names := map[string]bool{}
	for _, c := range ti.cols {
		names[c.name] = true
	}
	for _, want := range []string{"id", "name", "qty"} {
		if !names[want] {
			t.Errorf("restored gadget missing column %q (have %v)", want, names)
		}
	}
}

// TestUniqueKeyRestoredOnRestart: a non-PK unique key (§5) recorded via
// ADD CONSTRAINT UNIQUE survives a restart — restore reassembles ti.uniqueKeys
// from the durable syzy_key rows so loser-null arbitration resumes against it.
func TestUniqueKeyRestoredOnRestart(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const (
		db     = "syzy_uniqrestart"
		origin = crdt.Origin(115)
	)
	cluster := crdt.ClusterID{0xfa}
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	log := schemalog.NewLocal()

	reopen := func(meta *metadata.Store) *Engine {
		t.Helper()
		e, err := Open(ctx, Config{
			Name: db, Origin: origin, Cluster: cluster,
			Cache:       nodestate.New(origin),
			ConnURL:     dbURL(db),
			ReplConnURL: replURL(db),
			Publication: "syzy_pub",
			Slot:        "syzy_slot_" + db,
			OriginName:  "syzy_origin_" + db,
			Tables:      []string{"public.kv"},
			DDL:         true,
			SchemaLog:   log,
			Meta:        meta,
		})
		if err != nil {
			t.Fatalf("reopen %s: %v", db, err)
		}
		return e
	}

	meta1, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta1: %v", err)
	}
	e1 := openDDLEngineLogMeta(t, ctx, db, origin, cluster, 0, log, meta1)
	appExec(t, db, `CREATE TABLE public.gizmo (id bigint PRIMARY KEY, sku text)`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)
	appExec(t, db, `ALTER TABLE public.gizmo ADD CONSTRAINT gizmo_sku_uq UNIQUE (sku)`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)

	var tid crdt.TableID
	for id, ti := range e1.cat.byID {
		if ti.name == "gizmo" {
			tid = id
		}
	}
	if tid == (crdt.TableID{}) {
		t.Fatal("gizmo not in catalog after create")
	}
	if ti := e1.cat.byID[tid]; len(ti.uniqueKeys) != 1 {
		t.Fatalf("pre-restart uniqueKeys = %+v, want one", ti.uniqueKeys)
	}
	wantKeyID := e1.cat.byID[tid].uniqueKeys[0].keyID
	_ = e1.Close()
	if err := meta1.Close(); err != nil {
		t.Fatalf("meta1 close: %v", err)
	}
	waitSlotInactive(t, ctx, dbURL(db), "syzy_slot_"+db)

	meta2, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta2: %v", err)
	}
	defer meta2.Close()
	e2 := reopen(meta2)
	defer closeEngine(t, ctx, e2)

	ti := e2.cat.byID[tid]
	if ti == nil {
		t.Fatal("gizmo not restored after restart")
	}
	if len(ti.uniqueKeys) != 1 {
		t.Fatalf("restored uniqueKeys = %+v, want one", ti.uniqueKeys)
	}
	uk := ti.uniqueKeys[0]
	if uk.keyID != wantKeyID {
		t.Errorf("restored key id = %x, want %x", uk.keyID, wantKeyID)
	}
	if len(uk.cols) != 1 || uk.cols[0].name != "sku" {
		t.Fatalf("restored key cols = %+v, want [sku]", uk.cols)
	}
}

// TestBuildCatalogOpSkipsAlreadyCataloged: re-delivering a CREATE TABLE intent
// for a table already bound in the catalog (a post-recovery re-delivery) builds
// no op — re-running buildCreateTableOp would AllocTableID a divergent id and
// re-append (§6 F originator recovery).
func TestBuildCatalogOpSkipsAlreadyCataloged(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	e := openDDLEngineLog(t, ctx, "syzy_recreate_skip", 95, crdt.ClusterID{0xec}, 0, log)
	defer closeEngine(t, ctx, e)

	appExec(t, "syzy_recreate_skip", `CREATE TABLE public.thing (id bigint PRIMARY KEY, v text)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	var oid uint32
	var tid crdt.TableID
	for id, ti := range e.cat.byID {
		if ti.name == "thing" {
			tid, oid = id, ti.oid
		}
	}
	if oid == 0 {
		t.Fatal("thing not cataloged after create")
	}

	conn, err := pgx.Connect(ctx, dbURL("syzy_recreate_skip"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	ops, err := buildCatalogOps(ctx, conn, e.cat, []ddlIntent{{
		commandTag: "CREATE TABLE", objectType: "table", objid: oid,
	}})
	if err != nil {
		t.Fatalf("buildCatalogOps: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("re-delivered CREATE built %d ops, want 0 (already cataloged): %+v", len(ops), ops)
	}
	if e.cat.byID[tid] == nil || e.cat.byOID[oid] == nil || e.cat.byOID[oid].tid != tid {
		t.Fatal("catalog binding changed after a skipped re-delivery")
	}
}

// TestSchemaCatchUpAtStartup: a node applies schema-log events above its
// restored schema_seq at Open (mirrors the SQLite producer's startup recovery),
// so a follower that was down while the cluster schema advanced is reconciled
// before capture runs — without any explicit catch-up call (§6 F).
func TestSchemaCatchUpAtStartup(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xed}
	log := schemalog.NewLocal()

	a := openDDLEngineLog(t, ctx, "syzy_startup_a", 96, cluster, 0, log)
	defer closeEngine(t, ctx, a)
	appExec(t, "syzy_startup_a", `CREATE TABLE public.t (id bigint PRIMARY KEY)`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)

	const (
		dbB     = "syzy_startup_b"
		originB = crdt.Origin(97)
	)
	metaPath := filepath.Join(t.TempDir(), "b.meta")
	reopenB := func(meta *metadata.Store) *Engine {
		t.Helper()
		e, err := Open(ctx, Config{
			Name: dbB, Origin: originB, Cluster: cluster,
			Cache:       nodestate.New(originB),
			ConnURL:     dbURL(dbB),
			ReplConnURL: replURL(dbB),
			Publication: "syzy_pub",
			Slot:        "syzy_slot_" + dbB,
			OriginName:  "syzy_origin_" + dbB,
			Tables:      []string{"public.kv"},
			DDL:         true,
			SchemaLog:   log,
			Meta:        meta,
		})
		if err != nil {
			t.Fatalf("reopen %s: %v", dbB, err)
		}
		return e
	}

	// B catches up event 1 (CREATE t), persists it, then goes down.
	metaB1, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("metaB1: %v", err)
	}
	b1 := openDDLEngineLogMeta(t, ctx, dbB, originB, cluster, 0, log, metaB1)
	if err := b1.catchUpSchema(ctx); err != nil {
		t.Fatalf("b1 catchUp: %v", err)
	}
	if got := b1.schemaSeq.Load(); got != 1 {
		t.Fatalf("b1 schemaSeq = %d, want 1", got)
	}
	_ = b1.Close()
	if err := metaB1.Close(); err != nil {
		t.Fatalf("metaB1 close: %v", err)
	}
	waitSlotInactive(t, ctx, dbURL(dbB), "syzy_slot_"+dbB)

	// While B is down, A appends event 2 (CREATE u).
	appExec(t, "syzy_startup_a", `CREATE TABLE public.u (id bigint PRIMARY KEY)`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)

	// Reopen B: the startup catch-up at Open must apply event 2 with no explicit
	// catchUpSchema call.
	metaB2, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("metaB2: %v", err)
	}
	defer metaB2.Close()
	b2 := reopenB(metaB2)
	defer closeEngine(t, ctx, b2)

	if got := b2.schemaSeq.Load(); got != 2 {
		t.Fatalf("b2 schemaSeq after startup catch-up = %d, want 2", got)
	}
	var haveT, haveU bool
	for _, ti := range b2.cat.byID {
		switch ti.name {
		case "t":
			haveT = true
		case "u":
			haveU = true
		}
	}
	if !haveT || !haveU {
		t.Fatalf("b2 catalog after startup catch-up: t=%v u=%v, want both", haveT, haveU)
	}
}

// TestBootstrapTableDropRecreateRestart: dropping then recreating a bootstrap
// (Config.Tables) table via DDL gives it an allocated id; on restart,
// introspectCatalog re-adds it under its name-derived id AND restore binds the
// allocated id to the same OID. The stale derived id must be evicted so the
// relation maps to a single id — else a delayed peer changeset under the old id
// would silently write to the new table (codex F-review finding 1).
func TestBootstrapTableDropRecreateRestart(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const (
		db     = "syzy_bootdrop"
		origin = crdt.Origin(98)
	)
	cluster := crdt.ClusterID{0xee}
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	log := schemalog.NewLocal()
	reopen := func(meta *metadata.Store) *Engine {
		t.Helper()
		e, err := Open(ctx, Config{
			Name: db, Origin: origin, Cluster: cluster,
			Cache:       nodestate.New(origin),
			ConnURL:     dbURL(db),
			ReplConnURL: replURL(db),
			Publication: "syzy_pub",
			Slot:        "syzy_slot_" + db,
			OriginName:  "syzy_origin_" + db,
			Tables:      []string{"public.kv"},
			DDL:         true,
			SchemaLog:   log,
			Meta:        meta,
		})
		if err != nil {
			t.Fatalf("reopen %s: %v", db, err)
		}
		return e
	}

	meta1, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta1: %v", err)
	}
	e1 := openDDLEngineLogMeta(t, ctx, db, origin, cluster, 0, log, meta1)
	appExec(t, db, `DROP TABLE public.kv`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)
	appExec(t, db, `CREATE TABLE public.kv (k bigint PRIMARY KEY, v text)`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)

	var tidDDL crdt.TableID
	var oid uint32
	for id, ti := range e1.cat.byID {
		if ti.name == "kv" {
			tidDDL, oid = id, ti.oid
		}
	}
	if tidDDL == (crdt.TableID{}) {
		t.Fatal("recreated kv not in catalog with an allocated id")
	}
	_ = e1.Close()
	if err := meta1.Close(); err != nil {
		t.Fatalf("meta1 close: %v", err)
	}
	waitSlotInactive(t, ctx, dbURL(db), "syzy_slot_"+db)

	meta2, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta2: %v", err)
	}
	defer meta2.Close()
	e2 := reopen(meta2)
	defer closeEngine(t, ctx, e2)

	if e2.cat.byID[tidDDL] == nil {
		t.Fatal("kv lost its allocated id after restart")
	}
	for id, ti := range e2.cat.byID {
		if ti.oid == oid && id != tidDDL {
			t.Fatalf("stale duplicate id %x bound to kv oid %d after restart (silent-divergence)", id, oid)
		}
	}
	if e2.cat.byOID[oid] == nil || e2.cat.byOID[oid].tid != tidDDL {
		t.Fatalf("kv oid %d not bound to its allocated id %x", oid, tidDDL)
	}
}

// TestSchemaRestoreToleratesCrashMidDDL: Postgres commits a DDL transaction
// BEFORE the sidecar appends/persists its schema event, so a crash in that window
// leaves the physical schema AHEAD of the recorded catalog (a RENAME's new name,
// a DROPped column/table). Two properties must hold (codex F-review finding 2):
//
//  1. Open must not fail — restore binds by oid (rename-invariant), so a
//     since-renamed/dropped relation never blocks it; AND it must reflect the
//     RECORDED (last-appended) state, not the live physical schema, so the
//     pending change is not silently swallowed.
//  2. Running capture must then PROPAGATE the pending change: the un-pruned
//     syzy_ddl_intent rows are re-delivered and re-derived by diffing the live
//     catalog against the recorded one, appending the RENAME/DROP to the chain.
//
// The crash window is simulated by executing the DDL directly while the engine is
// down (its triggers still spool intent rows, but the sidecar never consumes them).
func TestSchemaRestoreToleratesCrashMidDDL(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const (
		db     = "syzy_crashmidddl"
		origin = crdt.Origin(99)
	)
	cluster := crdt.ClusterID{0xef}
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	log := schemalog.NewLocal()
	reopen := func(meta *metadata.Store) *Engine {
		t.Helper()
		e, err := Open(ctx, Config{
			Name: db, Origin: origin, Cluster: cluster,
			Cache:       nodestate.New(origin),
			ConnURL:     dbURL(db),
			ReplConnURL: replURL(db),
			Publication: "syzy_pub",
			Slot:        "syzy_slot_" + db,
			OriginName:  "syzy_origin_" + db,
			Tables:      []string{"public.kv"},
			DDL:         true,
			SchemaLog:   log,
			Meta:        meta,
		})
		if err != nil {
			t.Fatalf("reopen %s: %v", db, err)
		}
		return e
	}

	// run 1: two DDL tables fully recorded. gadget(id,name)+qty will be renamed and
	// have a column dropped behind metadata's back; trinket will be dropped.
	meta1, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta1: %v", err)
	}
	e1 := openDDLEngineLogMeta(t, ctx, db, origin, cluster, 0, log, meta1)
	appExec(t, db, `CREATE TABLE public.gadget (id bigint PRIMARY KEY, name text)`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)
	appExec(t, db, `ALTER TABLE public.gadget ADD COLUMN qty int`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)
	appExec(t, db, `CREATE TABLE public.trinket (id bigint PRIMARY KEY, v text)`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)

	var gadgetTID, trinketTID crdt.TableID
	var gadgetOID uint32
	for id, ti := range e1.cat.byID {
		switch ti.name {
		case "gadget":
			gadgetTID, gadgetOID = id, ti.oid
		case "trinket":
			trinketTID = id
		}
	}
	if gadgetTID == (crdt.TableID{}) || trinketTID == (crdt.TableID{}) {
		t.Fatal("gadget/trinket not cataloged after create")
	}
	_ = e1.Close()
	if err := meta1.Close(); err != nil {
		t.Fatalf("meta1 close: %v", err)
	}
	waitSlotInactive(t, ctx, dbURL(db), "syzy_slot_"+db)

	// The crash window: physical schema advances past the recorded metadata.
	appExec(t, db, `ALTER TABLE public.gadget RENAME TO widget`)
	appExec(t, db, `ALTER TABLE public.widget DROP COLUMN qty`)
	appExec(t, db, `DROP TABLE public.trinket`)

	// run 2: restore must not fail Open despite the divergence.
	meta2, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta2: %v", err)
	}
	defer meta2.Close()
	e2 := reopen(meta2)
	defer closeEngine(t, ctx, e2)

	// (1) After Open, the catalog reflects the RECORDED state, not the live one —
	// gadget under its old name with qty still present, trinket still bound — so
	// the pending change is preserved for the diff-based recovery, not erased.
	colNames := func(ti *tableInfo) map[string]bool {
		m := map[string]bool{}
		for _, c := range ti.cols {
			m[c.name] = true
		}
		return m
	}
	ti := e2.cat.byID[gadgetTID]
	if ti == nil {
		t.Fatal("renamed table lost on restart — oid rebinding failed")
	}
	if ti.name != "gadget" || ti.oid != gadgetOID || e2.cat.byOID[gadgetOID] != ti {
		t.Fatalf("post-Open gadget = name %q oid %d, want recorded name %q oid %d", ti.name, ti.oid, "gadget", gadgetOID)
	}
	if cols := colNames(ti); !cols["id"] || !cols["name"] || !cols["qty"] {
		t.Fatalf("post-Open gadget cols = %v, want recorded id/name/qty (delta erased?)", cols)
	}
	if e2.cat.byID[trinketTID] == nil {
		t.Fatal("post-Open: dropped-in-crash trinket must stay bound so its DROP can replicate")
	}
	if got := e2.schemaSeq.Load(); got != 3 {
		t.Fatalf("post-Open schemaSeq = %d, want 3 (recorded head, before recovery)", got)
	}

	// (2) Running capture re-delivers the un-pruned intent rows; the RENAME, DROP
	// COLUMN, and DROP TABLE are re-derived against the recorded catalog and
	// appended, advancing the chain and converging the in-memory catalog to live.
	_ = captureAllWithin(t, ctx, e2, 800*time.Millisecond)

	if got := e2.schemaSeq.Load(); got <= 3 {
		t.Fatalf("post-recovery schemaSeq = %d, want > 3 (pending DDL must propagate)", got)
	}
	ti = e2.cat.byID[gadgetTID]
	if ti == nil || ti.name != "widget" {
		t.Fatalf("post-recovery gadget not renamed to widget (got %v)", ti)
	}
	if cols := colNames(ti); cols["qty"] || !cols["id"] || !cols["name"] {
		t.Fatalf("post-recovery widget cols = %v, want id/name (qty dropped)", cols)
	}
	if e2.cat.byID[trinketTID] != nil {
		t.Fatal("post-recovery: trinket DROP did not propagate")
	}
}

// TestSchemaUnhealthyOnUnreplicableDDL: when a node commits a DDL it cannot put
// on the schema chain (here, a CREATE TABLE with no PRIMARY KEY), the orchestrator
// must halt loudly with ErrSchemaUnhealthy and durably record it (§6 F) — never
// silently skip it (which would leave the local schema ahead of the cluster).
// A restart then refuses to resume until syzy_clone clears the marker.
func TestSchemaUnhealthyOnUnreplicableDDL(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const (
		db     = "syzy_schemaunhealthy"
		origin = crdt.Origin(101)
	)
	cluster := crdt.ClusterID{0xf1}
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	log := schemalog.NewLocal()
	reopen := func(meta *metadata.Store) (*Engine, error) {
		return Open(ctx, Config{
			Name: db, Origin: origin, Cluster: cluster,
			Cache:       nodestate.New(origin),
			ConnURL:     dbURL(db),
			ReplConnURL: replURL(db),
			Publication: "syzy_pub",
			Slot:        "syzy_slot_" + db,
			OriginName:  "syzy_origin_" + db,
			Tables:      []string{"public.kv"},
			DDL:         true,
			SchemaLog:   log,
			Meta:        meta,
		})
	}

	meta1, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta1: %v", err)
	}
	e1 := openDDLEngineLogMeta(t, ctx, db, origin, cluster, 0, log, meta1)

	// A partial UNIQUE index cannot converge — its predicate's truth varies per
	// replica, so captureUniqueKeys rejects it. Admission has no pre-commit rule
	// for it (the index is only judged once the sidecar reads the catalog), so
	// the DDL commits physically and the only safe response is to halt
	// schema-unhealthy.
	appExec(t, db, `CREATE TABLE public.part (id bigint PRIMARY KEY, a text)`)
	appExec(t, db, `CREATE UNIQUE INDEX part_a_uq ON public.part (a) WHERE a IS NOT NULL`)

	cctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	var out []*crdt.Changeset
	runErr := e1.capt.run(cctx, collectProcess(e1, &out), runOpts{})
	if !errors.Is(runErr, ErrSchemaUnhealthy) {
		t.Fatalf("capture of an unreplicable DDL = %v, want ErrSchemaUnhealthy", runErr)
	}
	if !e1.schemaUnhealthy.Load() {
		t.Fatal("in-memory schema-unhealthy flag not set")
	}
	if reason, unhealthy, err := loadSchemaHealth(meta1); err != nil || !unhealthy {
		t.Fatalf("durable marker not set (unhealthy=%v err=%v)", unhealthy, err)
	} else if reason == "" {
		t.Fatal("durable marker has no reason")
	}

	_ = e1.Close()
	if err := meta1.Close(); err != nil {
		t.Fatalf("meta1 close: %v", err)
	}
	waitSlotInactive(t, ctx, dbURL(db), "syzy_slot_"+db)

	// A restart refuses to resume: the divergence is durable until syzy_clone.
	meta2, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta2: %v", err)
	}
	defer meta2.Close()
	e2, err := reopen(meta2)
	if !errors.Is(err, ErrSchemaUnhealthy) {
		if e2 != nil {
			closeEngine(t, ctx, e2)
		}
		t.Fatalf("reopen of an unhealthy node = %v, want ErrSchemaUnhealthy", err)
	}
}

// TestAdmissionRejectsCreateTableAs (§6 G): a permanent CREATE TABLE AS
// materializes a node-local query result that cannot replicate, so the
// ddl_command_end trigger RAISEs pre-commit — the user txn rolls back cleanly
// (the table never exists) instead of committing and forcing a schema-unhealthy
// halt post-commit.
func TestAdmissionRejectsCreateTableAs(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_ddl_ctas"
	e := openDDLEngine(t, ctx, db, 102, crdt.ClusterID{0xf2})
	defer closeEngine(t, ctx, e)

	conn, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `CREATE TABLE public.derived AS SELECT 1 AS x`)
	if err == nil {
		t.Fatal("CREATE TABLE AS should be rejected pre-commit")
	}
	if !strings.Contains(err.Error(), "not replicable") {
		t.Fatalf("CREATE TABLE AS rejected for the wrong reason: %v", err)
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relname = 'derived'`).Scan(&n); err != nil {
		t.Fatalf("check table: %v", err)
	}
	if n != 0 {
		t.Fatalf("rejected CREATE TABLE AS left a table behind (count=%d) — txn did not roll back", n)
	}
}

// TestTempUnloggedTablesSkipped (§6 G): TEMP and UNLOGGED relations are out of
// the replicated set (session-local / not WAL-logged), so their DDL writes no
// intent row — capture neither catalogs them nor halts on them, and a temp
// CREATE TABLE AS is allowed (not rejected like a permanent one).
func TestTempUnloggedTablesSkipped(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_ddl_tempunlogged"
	e := openDDLEngine(t, ctx, db, 103, crdt.ClusterID{0xf3})
	defer closeEngine(t, ctx, e)

	conn, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	// All three must succeed (temp CTAS is NOT rejected; the permanent-only
	// admission rule skips it), and none must reach the replication path.
	for _, ddl := range []string{
		`CREATE TEMP TABLE tmp1 (id bigint PRIMARY KEY, v text)`,
		`CREATE TEMP TABLE tmp2 AS SELECT 1 AS x`,
		`CREATE UNLOGGED TABLE ulog (id bigint PRIMARY KEY, v text)`,
	} {
		if _, err := conn.Exec(ctx, ddl); err != nil {
			t.Fatalf("non-permanent DDL %q should be allowed: %v", ddl, err)
		}
	}

	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	for _, ti := range e.cat.byID {
		switch ti.name {
		case "tmp1", "tmp2", "ulog":
			t.Fatalf("non-permanent relation %q was cataloged (should be skipped)", ti.name)
		}
	}
}

// TestLiveCreateIndexConvergence: a secondary (non-unique) CREATE INDEX replicates
// as an opaque-SQL OpCreateIndex — a follower applies it via catch-up, so B both
// converges on the rows AND ends up with the index materialized.
func TestLiveCreateIndexConvergence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	cluster := crdt.ClusterID{0xf7}
	a := openDDLEngineLog(t, ctx, "syzy_lidx_a", 110, cluster, 0, log)
	defer closeEngine(t, ctx, a)
	b := openDDLEngineLog(t, ctx, "syzy_lidx_b", 111, cluster, 0, log)
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

	appExec(t, "syzy_lidx_a", `CREATE TABLE public.doc (id bigint PRIMARY KEY, msg text)`)
	appExec(t, "syzy_lidx_a", `CREATE INDEX doc_msg_idx ON public.doc (msg)`)
	appExec(t, "syzy_lidx_a", `INSERT INTO public.doc VALUES (1,'x'),(2,'y')`)

	hasIndex := func(db string) bool {
		c, err := pgx.Connect(ctx, dbURL(db))
		if err != nil {
			return false
		}
		defer c.Close(ctx)
		var n int
		_ = c.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relkind='i' AND relname='doc_msg_idx'`).Scan(&n)
		return n > 0
	}

	want := map[int64]string{1: "x", 2: "y"}
	deadline := time.Now().Add(20 * time.Second)
	var got map[int64]string
	var indexed bool
	for time.Now().Before(deadline) {
		got = idMsgRows(t, "syzy_lidx_b", "public.doc")
		indexed = hasIndex("syzy_lidx_b")
		if mapsEqual(got, want) && indexed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if !mapsEqual(got, want) {
		t.Fatalf("B rows did not converge: got %v want %v", got, want)
	}
	if !indexed {
		t.Fatal("B did not receive the replicated secondary index doc_msg_idx")
	}
}

// TestLiveUniqueKeyCapture: a CREATE UNIQUE INDEX on a nullable column is
// captured as a typed OpAddUniqueKey (§5), a follower binds it in-catalog only
// (no physical UNIQUE index — arbitration is the convergence mechanism), and a
// DROP INDEX maps back to an OpDropUniqueKey that removes the follower's key
// (rather than a no-op OpDropIndex that would leave it stale).
func TestLiveUniqueKeyCapture(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	a := openDDLEngine(t, ctx, "syzy_uniq_origin", 112, crdt.ClusterID{0xf8})
	defer closeEngine(t, ctx, a)
	b := openDDLEngine(t, ctx, "syzy_uniq_follow", 113, crdt.ClusterID{0xf8})
	defer closeEngine(t, ctx, b)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, a, "syzy_uniq_origin", &ops, &buildErr)

	appExec(t, "syzy_uniq_origin", `CREATE TABLE public.acct (id bigint PRIMARY KEY, email text)`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)
	appExec(t, "syzy_uniq_origin", `CREATE UNIQUE INDEX acct_email_idx ON public.acct (email)`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)

	if buildErr != nil {
		t.Fatalf("unique-key capture errored: %v", buildErr)
	}
	if len(ops) != 2 || ops[1].Kind != crdt.OpAddUniqueKey {
		t.Fatalf("expected [create_table, add_unique_key], got %+v", ops)
	}
	createOp, addOp := ops[0], ops[1]
	emailID := columnIDByName(createOp, "email")
	if addOp.KeyID == (crdt.KeyID{}) {
		t.Fatal("unique key got the reserved all-zero (PK) KeyID")
	}
	if len(addOp.Keys) != 1 || len(addOp.Keys[0].Members) != 1 || addOp.Keys[0].Members[0].ColumnID != emailID {
		t.Fatalf("add_unique_key members = %+v, want one member on email %x", addOp.Keys, emailID)
	}
	// Originator catalog reflects the key (with its backing index OID).
	ati := a.cat.byID[createOp.TableID]
	if len(ati.uniqueKeys) != 1 || ati.uniqueKeys[0].keyID != addOp.KeyID ||
		len(ati.uniqueKeys[0].cols) != 1 || ati.uniqueKeys[0].cols[0].name != "email" || ati.uniqueKeys[0].indexOID == 0 {
		t.Fatalf("originator uniqueKeys = %+v, want one email key with a backing index oid", ati.uniqueKeys)
	}

	// Apply on B: binds the key in-catalog, no physical unique index.
	if err := applyCatalogOp(ctx, b.appl.conn, b.cat, createOp, b.cfg.NodeOrdinal); err != nil {
		t.Fatalf("apply create on B: %v", err)
	}
	if err := applyCatalogOp(ctx, b.appl.conn, b.cat, addOp, b.cfg.NodeOrdinal); err != nil {
		t.Fatalf("apply add unique key on B: %v", err)
	}
	bti := b.cat.byID[createOp.TableID]
	if bti == nil || len(bti.uniqueKeys) != 1 || bti.uniqueKeys[0].keyID != addOp.KeyID ||
		len(bti.uniqueKeys[0].cols) != 1 || bti.uniqueKeys[0].cols[0].name != "email" {
		t.Fatalf("follower uniqueKeys = %+v, want one email key", bti.uniqueKeys)
	}
	if n := nonPKUniqueIndexCount(t, "syzy_uniq_follow", "acct"); n != 0 {
		t.Fatalf("follower built %d physical non-PK unique indexes, want 0 (arbitration-only)", n)
	}

	// DROP INDEX on the originator drops the key, replicated as OpDropUniqueKey.
	appExec(t, "syzy_uniq_origin", `DROP INDEX acct_email_idx`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)
	if len(ops) != 3 || ops[2].Kind != crdt.OpDropUniqueKey || ops[2].KeyID != addOp.KeyID {
		t.Fatalf("expected an OpDropUniqueKey for the dropped index, got %+v", ops)
	}
	if len(ati.uniqueKeys) != 0 {
		t.Fatalf("originator still has the dropped unique key: %+v", ati.uniqueKeys)
	}
	if err := applyCatalogOp(ctx, b.appl.conn, b.cat, ops[2], b.cfg.NodeOrdinal); err != nil {
		t.Fatalf("apply drop unique key on B: %v", err)
	}
	if len(bti.uniqueKeys) != 0 {
		t.Fatalf("follower still has the dropped unique key: %+v", bti.uniqueKeys)
	}
}

// TestBuildCatalogOpRejectsNotNullUnique: without coordinated uniqueness
// enabled (Config.CoordinatedUnique=false, this harness), a UNIQUE on a NOT
// NULL column is rejected at admission — a NOT NULL key column cannot be
// nulled to cede a collision, and there is no leaseholder gate to route it.
func TestBuildCatalogOpRejectsNotNullUnique(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_uniq_notnull"
	e := openDDLEngine(t, ctx, db, 114, crdt.ClusterID{0xf9})
	defer closeEngine(t, ctx, e)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, db, &ops, &buildErr)

	appExec(t, db, `CREATE TABLE public.acc2 (id bigint PRIMARY KEY, code text NOT NULL)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, db, `CREATE UNIQUE INDEX acc2_code_idx ON public.acc2 (code)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if buildErr == nil {
		t.Fatalf("expected NOT NULL UNIQUE to be rejected (§5 phase 2), got none (ops=%+v)", ops)
	}
	if !errors.Is(buildErr, errUnsupportedDDL) {
		t.Fatalf("NOT NULL UNIQUE rejected with non-admission error: %v", buildErr)
	}
}

// columnIDByName returns the ColumnID of a named column in a CatalogOp's column
// list (test helper).
func columnIDByName(op crdt.CatalogOp, name string) crdt.ColumnID {
	for _, c := range op.Columns {
		if c.Name == name {
			return c.ID
		}
	}
	return crdt.ColumnID{}
}

// nonPKUniqueIndexCount returns how many physical non-PK unique indexes exist on
// a table (test helper): followers must have zero — arbitration, not a physical
// constraint, is the §5 convergence mechanism.
func nonPKUniqueIndexCount(t *testing.T, db, table string) int {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	var n int
	if err := c.QueryRow(ctx, `
		SELECT count(*) FROM pg_index i
		JOIN pg_class cl ON cl.oid = i.indrelid
		WHERE cl.relname = $1 AND i.indisunique AND NOT i.indisprimary`, table).Scan(&n); err != nil {
		t.Fatalf("unique index count: %v", err)
	}
	return n
}

// TestLiveCreateViewConvergence: a CREATE VIEW replicates as an opaque-SQL
// OpCreateView (CREATE OR REPLACE VIEW on apply) — a view is a pure projection,
// so a follower applies it via catch-up and ends up with the view defined over
// its replicated tables.
func TestLiveCreateViewConvergence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	cluster := crdt.ClusterID{0xf9}
	a := openDDLEngineLog(t, ctx, "syzy_lview_a", 113, cluster, 0, log)
	defer closeEngine(t, ctx, a)
	b := openDDLEngineLog(t, ctx, "syzy_lview_b", 114, cluster, 0, log)
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

	appExec(t, "syzy_lview_a", `CREATE TABLE public.doc (id bigint PRIMARY KEY, msg text)`)
	appExec(t, "syzy_lview_a", `CREATE VIEW public.doc_v AS SELECT id, msg FROM public.doc WHERE id > 0`)
	appExec(t, "syzy_lview_a", `INSERT INTO public.doc VALUES (1,'x'),(2,'y')`)

	hasView := func(db string) bool {
		c, err := pgx.Connect(ctx, dbURL(db))
		if err != nil {
			return false
		}
		defer c.Close(ctx)
		var n int
		_ = c.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relkind='v' AND relname='doc_v'`).Scan(&n)
		return n > 0
	}

	want := map[int64]string{1: "x", 2: "y"}
	deadline := time.Now().Add(20 * time.Second)
	var got map[int64]string
	var viewed bool
	for time.Now().Before(deadline) {
		got = idMsgRows(t, "syzy_lview_b", "public.doc")
		viewed = hasView("syzy_lview_b")
		if mapsEqual(got, want) && viewed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if !mapsEqual(got, want) {
		t.Fatalf("B rows did not converge: got %v want %v", got, want)
	}
	if !viewed {
		t.Fatal("B did not receive the replicated view doc_v")
	}
}

// TestBuildCatalogOpRejectsFunction: a CREATE FUNCTION is not replicated — its
// determinism/side-effects aren't modeled and a replicated DEFAULT/GENERATED
// could reference it — so the op-builder rejects it as admission-class.
func TestLocalOnlyDDLIsSkipped(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_fnreject"
	e := openDDLEngine(t, ctx, db, 115, crdt.ClusterID{0xfa})
	defer closeEngine(t, ctx, e)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, db, &ops, &buildErr)

	for _, sql := range []string{
		`CREATE FUNCTION public.addone(x int) RETURNS int LANGUAGE sql AS 'SELECT x + 1'`,
		`CREATE SEQUENCE public.loose_seq`,
		`CREATE TYPE public.mood AS ENUM ('ok', 'bad')`,
		`CREATE SCHEMA staging`,
		`CREATE TABLE staging.notreplicated (a int)`,
		`CREATE EXTENSION IF NOT EXISTS pg_trgm`,
		`COMMENT ON TABLE public.kv IS 'local note'`,
		`GRANT SELECT ON public.kv TO postgres`,
		`CREATE TRIGGER kv_tg BEFORE UPDATE ON public.kv FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()`,
	} {
		appExec(t, db, sql)
	}
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if buildErr != nil {
		t.Fatalf("local-only DDL halted the node: %v", buildErr)
	}
	if len(ops) != 0 {
		t.Fatalf("local-only DDL produced %d ops, want none: %+v", len(ops), ops)
	}
	if rows := ddlIntentRows(t, db); len(rows) != 0 {
		t.Fatalf("local-only DDL spooled %d intent rows, want none: %+v", len(rows), rows)
	}
}

// TestAdmissionRejectsUnreplicableTable: shapes a replicated table can never
// have are refused before the CREATE commits.
func TestAdmissionRejectsUnreplicableTable(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_tblreject"
	e := openDDLEngine(t, ctx, db, 116, crdt.ClusterID{0xfb})
	defer closeEngine(t, ctx, e)

	appExec(t, db, `CREATE TYPE public.mood AS ENUM ('ok', 'bad')`)
	for _, tc := range []struct{ sql, want string }{
		{`CREATE TABLE public.nopk (a int, b text)`, "requires a PRIMARY KEY"},
		{`CREATE TABLE public.parted (id bigint, d date, PRIMARY KEY (id, d)) PARTITION BY RANGE (d)`, "partitioned tables"},
		{`CREATE TABLE public.enums (id bigint PRIMARY KEY, m public.mood)`, "user-defined type"},
		{`CREATE MATERIALIZED VIEW public.mv AS SELECT 1 AS x`, "not replicable"},
		{`CREATE TABLE public.ctas AS SELECT 1 AS x`, "not replicable"},
	} {
		err := appExecErr(t, db, tc.sql)
		if err == nil {
			t.Errorf("%s committed, want a pre-commit rejection", tc.sql)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %v, want it to mention %q", tc.sql, err, tc.want)
		}
	}
}

// stubSchemaLog is a test Log whose Read is scripted, to exercise catchUpSchema's
// failure paths (ErrBelowHorizon; an event that fails to apply) deterministically.
type stubSchemaLog struct {
	events  []schemalog.Event
	readErr error
}

func (s *stubSchemaLog) Append(context.Context, uint64, []byte, string) (uint64, error) {
	return 0, errors.New("stub: append unsupported")
}

func (s *stubSchemaLog) Read(_ context.Context, from uint64, _ int) ([]schemalog.Event, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	var out []schemalog.Event
	for _, e := range s.events {
		if e.SchemaSeq > from {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *stubSchemaLog) Head(context.Context) (uint64, error) {
	var h uint64
	for _, e := range s.events {
		if e.SchemaSeq > h {
			h = e.SchemaSeq
		}
	}
	return h, nil
}

// TestSchemaCatchUpBelowHorizonUnhealthy (§6 F): a node that has fallen behind the
// log's retention window cannot catch up locally — catchUpSchema marks it
// schema-unhealthy (durable) and returns ErrSchemaUnhealthy (repair = syzy_clone).
func TestSchemaCatchUpBelowHorizonUnhealthy(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	meta, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	defer meta.Close()
	e := openDDLEngineLogMeta(t, ctx, "syzy_belowhorizon", crdt.Origin(104), crdt.ClusterID{0xf4}, 0, schemalog.NewLocal(), meta)
	defer closeEngine(t, ctx, e)

	e.cfg.SchemaLog = &stubSchemaLog{readErr: schemalog.ErrBelowHorizon}
	if err := e.catchUpSchema(ctx); !errors.Is(err, ErrSchemaUnhealthy) {
		t.Fatalf("catchUpSchema below horizon = %v, want ErrSchemaUnhealthy", err)
	}
	if !e.schemaUnhealthy.Load() {
		t.Fatal("schemaUnhealthy flag not set")
	}
	if _, unhealthy, err := loadSchemaHealth(meta); err != nil || !unhealthy {
		t.Fatalf("durable marker not set after below-horizon (unhealthy=%v err=%v)", unhealthy, err)
	}
}

// TestSchemaCatchUpApplyFailureFailedLocal (§6 F): when a follower cannot apply a
// supported cluster DDL (a SQL-level error), it records the event failed_local and
// halts schema-unhealthy rather than silently skipping it (which would diverge).
func TestSchemaCatchUpApplyFailureFailedLocal(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	meta, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	defer meta.Close()
	e := openDDLEngineLogMeta(t, ctx, "syzy_failedlocal", crdt.Origin(105), crdt.ClusterID{0xf5}, 0, schemalog.NewLocal(), meta)
	defer closeEngine(t, ctx, e)

	// A CREATE TABLE whose column declares a non-existent type fails to apply with
	// a *pgconn.PgError — the terminal (not transient) classification.
	cid := crdt.ColumnID{1}
	badOp := crdt.CatalogOp{
		Kind: crdt.OpCreateTable, TableID: crdt.TableID{1}, TableName: "boomtbl",
		Columns: []crdt.CatalogColumn{{ID: cid, Name: "id", Type: "nonesuchtype", IsPK: true, PKPos: 1}},
		Keys:    []crdt.CatalogKey{{Members: []crdt.CatalogKeyMember{{ColumnID: cid, Ordinal: 0}}}},
	}
	encoded, err := crdt.EncodeCatalogOp(badOp)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	e.cfg.SchemaLog = &stubSchemaLog{events: []schemalog.Event{{
		SchemaSeq: 1, ParentSeq: 0, CatalogOp: encoded, RawSQL: "CREATE TABLE boomtbl (id nonesuchtype)",
	}}}

	if err := e.catchUpSchema(ctx); !errors.Is(err, ErrSchemaUnhealthy) {
		t.Fatalf("catchUpSchema apply failure = %v, want ErrSchemaUnhealthy", err)
	}
	if !e.schemaUnhealthy.Load() {
		t.Fatal("schemaUnhealthy flag not set")
	}
	failed, err := meta.ReadFailedLocalSchemaEvents()
	if err != nil {
		t.Fatalf("read failed_local: %v", err)
	}
	if len(failed) != 1 || failed[0].SchemaSeq != 1 {
		t.Fatalf("expected one failed_local event at seq 1, got %+v", failed)
	}
}

// TestAppendHeadMovedUnhealthy (§6 F): when this node's schema-log append loses
// the cross-node CAS (a peer advanced the head past our parent), our locally
// committed DDL cannot be appended and we do not rebase it — the node has
// diverged and must halt schema-unhealthy (syzy_clone). The parent-CAS, not a
// fencing epoch, is what serializes the cluster here.
func TestAppendHeadMovedUnhealthy(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_headmoved"
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	meta, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	defer meta.Close()
	log := schemalog.NewLocal()
	e := openDDLEngineLogMeta(t, ctx, db, crdt.Origin(106), crdt.ClusterID{0xf6}, 0, log, meta)
	defer closeEngine(t, ctx, e)

	// A peer advances the shared chain head past this node's parent (schema_seq 0)
	// AFTER Open, so startup catch-up did not absorb it and our parent stays 0.
	if _, err := log.Append(ctx, 0, []byte("peer-event"), ""); err != nil {
		t.Fatalf("seed peer event: %v", err)
	}

	// This node commits its own DDL; its append at parent 0 now loses the CAS.
	appExec(t, db, `CREATE TABLE public.late (id bigint PRIMARY KEY, v text)`)

	cctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	var out []*crdt.Changeset
	if err := e.capt.run(cctx, collectProcess(e, &out), runOpts{}); !errors.Is(err, ErrSchemaUnhealthy) {
		t.Fatalf("append at a moved head = %v, want ErrSchemaUnhealthy", err)
	}
	if !e.schemaUnhealthy.Load() {
		t.Fatal("schemaUnhealthy flag not set")
	}
	if _, unhealthy, err := loadSchemaHealth(meta); err != nil || !unhealthy {
		t.Fatalf("durable marker not set after head-moved (unhealthy=%v err=%v)", unhealthy, err)
	}
}

// --- §5 loser-null UNIQUE arbitration (apply path) -------------------------

// tableIDByName returns the stable TableID of a catalog table by current name.
func tableIDByName(e *Engine, name string) crdt.TableID {
	for id, ti := range e.cat.byID {
		if ti.name == name {
			return id
		}
	}
	return crdt.TableID{}
}

// mustBuild frames a changeset with a caller-chosen Dot/Stamp (so arbitration
// outcomes are deterministic), Deps[SchemaChain]=0 (the table is already bound in
// the catalog, so the apply gate is skipped at 0).
func mustBuild(t *testing.T, dot crdt.Dot, stamp crdt.Stamp, cluster crdt.ClusterID, recs ...crdt.Record) *crdt.Changeset {
	t.Helper()
	cs, err := crdt.Build(dot, stamp, crdt.Deps{crdt.SchemaChain: 0}, cluster, recs)
	if err != nil {
		t.Fatalf("build changeset: %v", err)
	}
	return cs
}

// cv builds a text-mode column image from (ColumnID, value) pairs.
func cv(pairs ...any) []crdt.ColValue {
	out := make([]crdt.ColValue, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, crdt.ColValue{
			Column: pairs[i].(crdt.ColumnID), TypeTag: crdt.ColText, Bytes: []byte(pairs[i+1].(string)),
		})
	}
	return out
}

// acctEmails reads public.acct as id→email ("<null>" for SQL NULL).
func acctEmails(t *testing.T, db string) map[int64]string {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `SELECT id, email FROM public.acct ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var email *string
		if err := rows.Scan(&id, &email); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if email == nil {
			out[id] = "<null>"
		} else {
			out[id] = *email
		}
	}
	return out
}

// setupUniqueTable creates public.acct (id bigint PK, email text[, note text])
// with a UNIQUE(email) index on e, capturing the ops into e.cat (ti.uniqueKeys
// bound) and returning them + the table id.
func setupUniqueTable(t *testing.T, ctx context.Context, e *Engine, db string, withNote bool) ([]crdt.CatalogOp, crdt.TableID) {
	t.Helper()
	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, db, &ops, &buildErr)
	cols := "id bigint PRIMARY KEY, email text"
	if withNote {
		cols += ", note text"
	}
	appExec(t, db, "CREATE TABLE public.acct ("+cols+")")
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, db, `CREATE UNIQUE INDEX acct_email_idx ON public.acct (email)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	if buildErr != nil {
		t.Fatalf("build unique table ops: %v", buildErr)
	}
	return ops, tableIDByName(e, "acct")
}

// TestUniqueArbitrationConvergence: two nodes concurrently insert different PKs
// carrying the same UNIQUE value; after each applies the other's changeset both
// converge to the SAME single owner (the dominating stamp), the loser's key
// column nulled — §5 loser-null, verified across the originator (physical UNIQUE
// index, must avoid 23505) and a follower (in-catalog key only).
func TestUniqueArbitrationConvergence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xfb}
	a := openDDLEngine(t, ctx, "syzy_uniqarb_a", 120, cluster)
	defer closeEngine(t, ctx, a)
	b := openDDLEngine(t, ctx, "syzy_uniqarb_b", 121, cluster)
	defer closeEngine(t, ctx, b)

	// A builds the schema; replay its ops on B so both share the allocated ids and
	// bind ti.uniqueKeys (B holds the key in-catalog, no physical index).
	ops, tid := setupUniqueTable(t, ctx, a, "syzy_uniqarb_a", false)
	for _, op := range ops {
		if err := applyCatalogOp(ctx, b.appl.conn, b.cat, op, b.cfg.NodeOrdinal); err != nil {
			t.Fatalf("replay %v on B: %v", op.Kind, err)
		}
	}
	if n := nonPKUniqueIndexCount(t, "syzy_uniqarb_b", "acct"); n != 0 {
		t.Fatalf("follower built %d physical unique indexes, want 0", n)
	}

	// Concurrent local inserts of the same email under different PKs.
	appExec(t, "syzy_uniqarb_a", `INSERT INTO public.acct VALUES (1, 'shared@x')`)
	csAs := captureAllWithin(t, ctx, a, 800*time.Millisecond)
	appExec(t, "syzy_uniqarb_b", `INSERT INTO public.acct VALUES (2, 'shared@x')`)
	csBs := captureAllWithin(t, ctx, b, 800*time.Millisecond)
	if len(csAs) != 1 || len(csBs) != 1 {
		t.Fatalf("expected one changeset each, got A=%d B=%d", len(csAs), len(csBs))
	}
	csA, csB := csAs[0], csBs[0]

	// Cross-apply (orchestrator order: each node already folded its own local write).
	if err := a.Applier().Apply(ctx, csB); err != nil {
		t.Fatalf("apply csB on A: %v", err)
	}
	if err := b.Applier().Apply(ctx, csA); err != nil {
		t.Fatalf("apply csA on B: %v", err)
	}

	stateA := acctEmails(t, "syzy_uniqarb_a")
	stateB := acctEmails(t, "syzy_uniqarb_b")
	if stateA[1] != stateB[1] || stateA[2] != stateB[2] {
		t.Fatalf("nodes diverged: A=%v B=%v", stateA, stateB)
	}
	winner, loser := int64(2), int64(1) // B's stamp wins by default
	if csA.Stamp.Dominates(csB.Stamp) {
		winner, loser = 1, 2
	}
	if stateA[winner] != "shared@x" {
		t.Fatalf("dominating writer pk=%d should own the value, got %v", winner, stateA)
	}
	if stateA[loser] != "<null>" {
		t.Fatalf("loser pk=%d should be nulled, got %q", loser, stateA[loser])
	}
	_ = tid
}

// TestUniqueArbitrationCellLWWProtectsStolenNull: after R steals a value from Q
// (nulling Q's key column at R.stamp), a causally-stale write to Q's key column
// must NOT resurrect it. The write passes PG's ROW gate (it beats Q's untouched
// row baseline) but must lose the per-key-column CELL gate (R.stamp) — the latent
// divergence a row-LWW-only port would miss. A non-key column in the same write
// still lands.
func TestUniqueArbitrationCellLWWProtectsStolenNull(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xfc}
	e := openDDLEngine(t, ctx, "syzy_uniqcell", 122, cluster)
	defer closeEngine(t, ctx, e)

	_, tid := setupUniqueTable(t, ctx, e, "syzy_uniqcell", true)
	ti := e.cat.byID[tid]
	idCol, emailCol, noteCol := ti.byName["id"].cid, ti.byName["email"].cid, ti.byName["note"].cid

	// Q owns 'x' at a low stamp.
	qPK := typedPK(t, e, "acct", "2")
	stampQ := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: 200}
	csQ := mustBuild(t, crdt.Dot{Origin: 200, Seq: 1}, stampQ, cluster,
		crdt.Insert{Table: tid, PK: qPK, CL: 1, Image: cv(idCol, "2", emailCol, "x")})
	if err := e.Applier().Apply(ctx, csQ); err != nil {
		t.Fatalf("apply Q: %v", err)
	}

	// R steals 'x' at a higher stamp.
	rPK := typedPK(t, e, "acct", "1")
	stampR := crdt.Stamp{Clock: crdt.Clock{WallTime: 300}, Origin: 201}
	csR := mustBuild(t, crdt.Dot{Origin: 201, Seq: 1}, stampR, cluster,
		crdt.Insert{Table: tid, PK: rPK, CL: 1, Image: cv(idCol, "1", emailCol, "x")})
	if err := e.Applier().Apply(ctx, csR); err != nil {
		t.Fatalf("apply R: %v", err)
	}

	if got := acctEmails(t, "syzy_uniqcell"); got[1] != "x" || got[2] != "<null>" {
		t.Fatalf("after steal want id1=x id2=null, got %v", got)
	}
	if got := e.cfg.Cache.RowState(tid, qPK).EffectiveStamp(emailCol, crdt.ByteRange{}); got != stampR {
		t.Fatalf("loser cell_clock = %v, want steal stamp %v", got, stampR)
	}

	// Causally-stale write to Q's email (stamp between Q baseline 100 and R 300):
	// passes the row gate, loses the cell gate. Updates carry the full image, so
	// include the PK; the note column must land.
	stampW := crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 200}
	csW := mustBuild(t, crdt.Dot{Origin: 200, Seq: 2}, stampW, cluster,
		crdt.Update{Table: tid, PK: qPK, CL: 1, Changed: cv(idCol, "2", emailCol, "y", noteCol, "kept")})
	if err := e.Applier().Apply(ctx, csW); err != nil {
		t.Fatalf("apply W: %v", err)
	}

	rows := acctRows3(t, "syzy_uniqcell")
	if rows[2].email != "<null>" {
		t.Fatalf("stale write resurrected the stolen email: id2.email=%q, want null", rows[2].email)
	}
	if rows[2].note != "kept" {
		t.Fatalf("non-key column should land: id2.note=%q, want 'kept'", rows[2].note)
	}
}

type acctRow struct{ email, note string }

func acctRows3(t *testing.T, db string) map[int64]acctRow {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `SELECT id, email, note FROM public.acct ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	out := map[int64]acctRow{}
	for rows.Next() {
		var id int64
		var email, note *string
		if err := rows.Scan(&id, &email, &note); err != nil {
			t.Fatalf("scan: %v", err)
		}
		r := acctRow{email: "<null>", note: "<null>"}
		if email != nil {
			r.email = *email
		}
		if note != nil {
			r.note = *note
		}
		out[id] = r
	}
	return out
}

// TestUniqueArbitrationWinnerRefreshesStaleCell (codex finding 2): after a steal
// leaves a cell_clock override on a loser's key column, a later WINNING write to
// that column must clear the stale override so the column's effective stamp
// follows the new value. Otherwise a third writer with a stamp between the steal
// and the winning write would mis-steal the value back. Regression guard.
func TestUniqueArbitrationWinnerRefreshesStaleCell(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xfe}
	e := openDDLEngine(t, ctx, "syzy_uniqrefresh", 124, cluster)
	defer closeEngine(t, ctx, e)
	_, tid := setupUniqueTable(t, ctx, e, "syzy_uniqrefresh", false)
	ti := e.cat.byID[tid]
	idCol, emailCol := ti.byName["id"].cid, ti.byName["email"].cid

	qPK := typedPK(t, e, "acct", "2")
	csQ := mustBuild(t, crdt.Dot{Origin: 200, Seq: 1}, crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: 200}, cluster,
		crdt.Insert{Table: tid, PK: qPK, CL: 1, Image: cv(idCol, "2", emailCol, "x")})
	if err := e.Applier().Apply(ctx, csQ); err != nil {
		t.Fatalf("apply Q: %v", err)
	}
	rPK := typedPK(t, e, "acct", "1")
	csR := mustBuild(t, crdt.Dot{Origin: 201, Seq: 1}, crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 201}, cluster,
		crdt.Insert{Table: tid, PK: rPK, CL: 1, Image: cv(idCol, "1", emailCol, "x")})
	if err := e.Applier().Apply(ctx, csR); err != nil {
		t.Fatalf("apply R (steal): %v", err)
	}
	// W sets Q.email='z' at t=300 — wins the cell-LWW pass and must clear the
	// stale steal cell (t=200) so Q.email's effective stamp becomes 300.
	csW := mustBuild(t, crdt.Dot{Origin: 200, Seq: 2}, crdt.Stamp{Clock: crdt.Clock{WallTime: 300}, Origin: 200}, cluster,
		crdt.Update{Table: tid, PK: qPK, CL: 1, Changed: cv(idCol, "2", emailCol, "z")})
	if err := e.Applier().Apply(ctx, csW); err != nil {
		t.Fatalf("apply W: %v", err)
	}
	if got := e.cfg.Cache.RowState(tid, qPK).EffectiveStamp(emailCol, crdt.ByteRange{}); got.WallTime != 300 {
		t.Fatalf("Q.email effective stamp = %v, want wall=300 (stale steal cell must be cleared)", got)
	}
	// R3 claims 'z' at t=250 (between steal 200 and W 300): must LOSE to Q's
	// current ownership (300), not steal it back via the stale 200 cell.
	r3PK := typedPK(t, e, "acct", "3")
	csR3 := mustBuild(t, crdt.Dot{Origin: 202, Seq: 1}, crdt.Stamp{Clock: crdt.Clock{WallTime: 250}, Origin: 202}, cluster,
		crdt.Insert{Table: tid, PK: r3PK, CL: 1, Image: cv(idCol, "3", emailCol, "z")})
	if err := e.Applier().Apply(ctx, csR3); err != nil {
		t.Fatalf("apply R3: %v", err)
	}
	got := acctEmails(t, "syzy_uniqrefresh")
	if got[2] != "z" {
		t.Fatalf("Q(id2) should keep 'z' (set at 300 > R3 250), got %q", got[2])
	}
	if got[3] != "<null>" {
		t.Fatalf("R3(id3) should cede (250 < 300), got %q", got[3])
	}
}

// TestBuildCatalogOpRejectsPartialUnique (codex finding 4): a partial UNIQUE
// index cannot replicate (its predicate truth varies across replicas), so the
// op-builder rejects it rather than silently skipping — which would leave the
// originator's physical constraint with no replicated counterpart.
func TestBuildCatalogOpRejectsPartialUnique(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const db = "syzy_uniq_partial"
	e := openDDLEngine(t, ctx, db, 125, crdt.ClusterID{0xfd})
	defer closeEngine(t, ctx, e)
	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, db, &ops, &buildErr)
	appExec(t, db, `CREATE TABLE public.acct (id bigint PRIMARY KEY, email text)`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, db, `CREATE UNIQUE INDEX acct_email_partial ON public.acct (email) WHERE email IS NOT NULL`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	if buildErr == nil || !errors.Is(buildErr, errUnsupportedDDL) {
		t.Fatalf("expected partial UNIQUE rejected as admission error, got %v (ops=%+v)", buildErr, ops)
	}
}

// TestApplyCatalogOpRejectsCoordinatedKey pins the engine boundary for key
// shapes this node cannot enforce: a partial (predicate) unique key always,
// and a coordinated key when this node runs WITHOUT coordination enabled —
// the catch-up path then halts schema-unhealthy instead of silently recording
// a key whose CP guarantee local commits would break.
func TestApplyCatalogOpRejectsCoordinatedKey(t *testing.T) {
	coord := crdt.CatalogOp{
		Kind:    crdt.OpAddUniqueKey,
		TableID: crdt.TableID{0x11},
		KeyID:   crdt.KeyID{0x22},
		Keys: []crdt.CatalogKey{{
			KeyID:       crdt.KeyID{0x22},
			Members:     []crdt.CatalogKeyMember{{ColumnID: crdt.ColumnID{0x33}}},
			Coordinated: true,
		}},
	}
	// The guard fires before any conn use; the catalog only supplies the
	// coordination flag (off here — a node without a bucket/lease).
	if err := applyCatalogOp(context.Background(), nil, &catalog{}, coord, 0); !errors.Is(err, errUnsupportedDDL) {
		t.Fatalf("coordinated key without coordination enabled: got %v; want errUnsupportedDDL", err)
	}
	partial := coord
	partial.Keys = []crdt.CatalogKey{{
		KeyID:     crdt.KeyID{0x22},
		Members:   []crdt.CatalogKeyMember{{ColumnID: crdt.ColumnID{0x33}}},
		Predicate: crdt.UniquePredicate{Root: &crdt.PredExpr{Op: crdt.PredIsNull, Col: crdt.ColumnID{0x33}}},
	}}
	if err := applyCatalogOp(context.Background(), nil, &catalog{coordUnique: true}, partial, 0); !errors.Is(err, errUnsupportedDDL) {
		t.Fatalf("partial key: got %v; want errUnsupportedDDL", err)
	}
}
