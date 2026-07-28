package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
)

// TestOpen_HealsSchemaSkewBehindAppDB is the end-to-end guard for the
// two-stream-restore durability bug: app.db's schema lags what the
// metadata catalog records as applied (schema_seq advanced, the ADD
// COLUMN syzy_schema_event = 'applied', the catalog carries the column),
// but the column is absent from app.db. Open must reconcile this before
// the node serves, re-applying the missing DDL from the catalog op
// (including its NOT NULL DEFAULT), so the node converges instead of
// silently dropping every write that touches the column.
func TestOpen_HealsSchemaSkewBehindAppDB(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	logPath := filepath.Join(dir, "schema.log")

	// 1. A consistent node: create the table, ADD a NOT NULL column with a
	// DEFAULT, and seed a row.
	sl1, err := schemalog.OpenFile(logPath)
	if err != nil {
		t.Fatalf("OpenFile schema log: %v", err)
	}
	n1, err := syzy.Open(ctx, syzy.Config{Path: dbPath, SchemaLog: sl1})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := n1.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := n1.Exec(`ALTER TABLE t ADD COLUMN c TEXT NOT NULL DEFAULT ''`); err != nil {
		t.Fatalf("ADD COLUMN: %v", err)
	}
	if err := n1.Exec(`INSERT INTO t (id, body, c) VALUES (1, 'x', 'live')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := n1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	_ = sl1.Close()

	// 2. Stage the skew: drop column c from app.db ONLY. The metadata DB
	// (catalog + schema_seq + applied syzy_schema_event) is untouched;
	// exactly the state a metadata-stream-ahead-of-data-stream restore
	// reconstructs.
	raw, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	if err := raw.Exec(`ALTER TABLE t DROP COLUMN c; PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("stage skew: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}
	if ok, _ := columnPresent(t, dbPath, "t", "c"); ok {
		t.Fatalf("staging failed: column c still present in app.db")
	}

	// 3. Reopen. The on-open reconciliation must heal app.db.
	sl2, err := schemalog.OpenFile(logPath)
	if err != nil {
		t.Fatalf("OpenFile schema log 2: %v", err)
	}
	n2, err := syzy.Open(ctx, syzy.Config{Path: dbPath, SchemaLog: sl2})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}

	// Healed at Open: the column is back and the pre-existing row was
	// backfilled with the NOT NULL DEFAULT.
	if ok, _ := columnPresent(t, dbPath, "t", "c"); !ok {
		t.Fatalf("Open did not heal: column c still absent from app.db")
	}
	if got := queryText(t, n2, `SELECT c FROM t WHERE id = 1`); got != "" {
		t.Fatalf("backfilled c = %q; want \"\" (NOT NULL DEFAULT)", got)
	}

	// A write referencing the healed column applies cleanly (pre-heal it
	// would fail "no such column: c").
	if err := n2.Exec(`INSERT INTO t (id, body, c) VALUES (2, 'y', 'new')`); err != nil {
		t.Fatalf("write referencing healed column: %v", err)
	}
	if got := queryText(t, n2, `SELECT c FROM t WHERE id = 2`); got != "new" {
		t.Fatalf("c for id=2 = %q; want \"new\"", got)
	}
	if err := n2.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_ = sl2.Close()

	// 4. Idempotent: reopening a now-healthy node heals nothing and opens
	// cleanly.
	sl3, err := schemalog.OpenFile(logPath)
	if err != nil {
		t.Fatalf("OpenFile schema log 3: %v", err)
	}
	n3, err := syzy.Open(ctx, syzy.Config{Path: dbPath, SchemaLog: sl3})
	if err != nil {
		t.Fatalf("third Open (idempotent): %v", err)
	}
	defer func() { _ = n3.Close(); _ = sl3.Close() }()
	if ok, _ := columnPresent(t, dbPath, "t", "c"); !ok {
		t.Fatalf("column c missing after idempotent reopen")
	}
}

func columnPresent(t *testing.T, dbPath, table, col string) (bool, error) {
	t.Helper()
	conn, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatalf("open app.db: %v", err)
	}
	defer conn.Close()
	return sqlitebridge.ColumnExists(conn, table, col)
}

func queryText(t *testing.T, n *syzy.Node, sql string) string {
	t.Helper()
	var v string
	if err := n.WriterDB().QueryRow(sql).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return v
}
