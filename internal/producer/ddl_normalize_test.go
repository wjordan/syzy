package producer

import (
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/sqlitebridge"
)

func normalizeConn(t *testing.T, ddl ...string) *sqlitebridge.Conn {
	t.Helper()
	c, err := sqlitebridge.Open(filepath.Join(t.TempDir(), "n.db"), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	for _, s := range ddl {
		if err := c.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
	return c
}

func scalar(t *testing.T, c *sqlitebridge.Conn, sql string) string {
	t.Helper()
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("prepare %q: %v", sql, err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("step %q: %v", sql, err)
	}
	if !hasRow {
		return ""
	}
	return stmt.ColumnText(0)
}

// TestNormalize_FailsClosedWhenStripDoesNotTake: the strip re-renders the
// stored CREATE TABLE through a parser that does not cover every shape
// SQLite accepts. A shape it renders back unchanged used to be reported
// as normalized, leaving the key active behind a live UNIQUE index — the
// one state this file exists to prevent. It must be an error instead.
func TestNormalize_FailsClosedWhenStripDoesNotTake(t *testing.T) {
	c := normalizeConn(t, `CREATE TABLE t (a TEXT NOT NULL, b TEXT, UNIQUE (a COLLATE NOCASE))`)
	_, err := NormalizeCoordinatedIndexes(c, "t", [][]string{{"a"}})
	if err == nil {
		t.Fatal("reported success with UNIQUE enforcement still present")
	}
	if got := scalar(t, c, `SELECT count(*) FROM pragma_index_list('t') WHERE "unique" = 1`); got != "1" {
		t.Errorf("unique indexes = %s, want the untouched 1 (the fixture's premise)", got)
	}
}

// TestNormalize_RestoresConnectionPragmas: the rebuild needs
// foreign_keys OFF and legacy_alter_table ON, both connection-level and
// not undone by a rollback. conn is the long-lived producer helper, so
// leaving either flipped would silently change every later statement on
// it.
func TestNormalize_RestoresConnectionPragmas(t *testing.T) {
	c := normalizeConn(t,
		`PRAGMA foreign_keys = ON`,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)`)
	changed, err := NormalizeCoordinatedIndexes(c, "t", [][]string{{"email"}})
	if err != nil || !changed {
		t.Fatalf("normalize: changed=%v err=%v", changed, err)
	}
	if got := scalar(t, c, `PRAGMA foreign_keys`); got != "1" {
		t.Errorf("foreign_keys = %s after rebuild, want 1", got)
	}
	if got := scalar(t, c, `PRAGMA legacy_alter_table`); got != "0" {
		t.Errorf("legacy_alter_table = %s after rebuild, want 0", got)
	}
}

// TestNormalize_RebuildPreservesTableShape: the rebuild copies rows and
// re-creates the table's other indexes and triggers, strips only the
// matching UNIQUE, and is idempotent on a second pass.
func TestNormalize_RebuildPreservesTableShape(t *testing.T) {
	c := normalizeConn(t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE, name TEXT DEFAULT 'anon' COLLATE NOCASE, CHECK (length(email) > 0))`,
		`CREATE INDEX ix_name ON t(name)`,
		`CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET name = 'x' WHERE id = new.id; END`,
		`CREATE VIEW v AS SELECT email FROM t`,
		`INSERT INTO t (id, email) VALUES (1, 'a@x'), (2, 'b@x')`)

	changed, err := NormalizeCoordinatedIndexes(c, "t", [][]string{{"email"}})
	if err != nil || !changed {
		t.Fatalf("normalize: changed=%v err=%v", changed, err)
	}
	for _, tc := range []struct{ what, sql, want string }{
		{"unique indexes", `SELECT count(*) FROM pragma_index_list('t') WHERE "unique" = 1 AND origin != 'pk'`, "0"},
		{"rows", `SELECT count(*) FROM t`, "2"},
		{"rowids", `SELECT group_concat(id) FROM t`, "1,2"},
		{"other index", `SELECT count(*) FROM sqlite_master WHERE type='index' AND name='ix_name'`, "1"},
		{"trigger", `SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name='tr'`, "1"},
		{"view", `SELECT count(*) FROM sqlite_master WHERE type='view' AND name='v'`, "1"},
		{"default", `SELECT dflt_value FROM pragma_table_info('t') WHERE name='name'`, "'anon'"},
		{"integrity", `PRAGMA integrity_check`, "ok"},
	} {
		if got := scalar(t, c, tc.sql); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.what, got, tc.want)
		}
	}
	// Duplicates are legal now: enforcement moved to the gate.
	if err := c.Exec(`INSERT INTO t (id, email) VALUES (3, 'a@x')`); err != nil {
		t.Errorf("duplicate rejected; the index was not fully stripped: %v", err)
	}
	// Idempotent: nothing left to normalize.
	if changed, err := NormalizeCoordinatedIndexes(c, "t", [][]string{{"email"}}); err != nil || changed {
		t.Errorf("second pass: changed=%v err=%v, want false/nil", changed, err)
	}
}

// TestNormalize_KeepsAnApplicationTableAtTheTmpName: the rebuild's
// scratch name is not reserved, so an application table can already
// occupy it. The rebuild used to DROP it, destroying user data; it must
// fail instead.
func TestNormalize_KeepsAnApplicationTableAtTheTmpName(t *testing.T) {
	c := normalizeConn(t,
		`CREATE TABLE `+normalizeTmpName+` (secret TEXT)`,
		`INSERT INTO `+normalizeTmpName+` VALUES ('keep')`,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)`)
	if _, err := NormalizeCoordinatedIndexes(c, "t", [][]string{{"email"}}); err == nil {
		t.Error("normalize succeeded over an occupied scratch name")
	}
	if got := scalar(t, c, `SELECT secret FROM `+normalizeTmpName); got != "keep" {
		t.Errorf("application data at the scratch name = %q, want \"keep\"", got)
	}
}
