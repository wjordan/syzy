package sqlitebridge

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestBusyTimeoutWaitsForWriter pins multi-connection writer behavior: with no busy_timeout
// a conn fails immediately with SQLITE_BUSY when another conn holds the write
// lock; with busy_timeout it waits for the lock to release and succeeds. Mirrors
// the FUSE conn contending with a sibling writer over a shared WAL DB.
func TestBusyTimeoutWaitsForWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.db")

	w, err := Open(path, 0)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	defer w.Close()
	if err := w.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("WAL: %v", err)
	}
	if err := w.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := w.Exec(`BEGIN IMMEDIATE`); err != nil { // hold the write lock
		t.Fatalf("begin immediate: %v", err)
	}

	// No busy_timeout: immediate SQLITE_BUSY.
	c1, err := Open(path, 0)
	if err != nil {
		t.Fatalf("Open c1: %v", err)
	}
	err1 := c1.Exec(`INSERT INTO t (id) VALUES (1)`)
	_ = c1.Close()
	if !IsCode(err1, ResultBusy) {
		t.Fatalf("no busy_timeout: want SQLITE_BUSY, got %v", err1)
	}

	// With busy_timeout: the INSERT blocks until the writer commits, then succeeds.
	c2, err := Open(path, 0)
	if err != nil {
		t.Fatalf("Open c2: %v", err)
	}
	defer c2.Close()
	if err := c2.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("set busy_timeout: %v", err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = w.Exec(`COMMIT`)
	}()
	if err := c2.Exec(`INSERT INTO t (id) VALUES (2)`); err != nil {
		t.Fatalf("with busy_timeout the INSERT should wait then succeed, got: %v", err)
	}
}

func memDB(t *testing.T) *Conn {
	t.Helper()
	c, err := Open(":memory:", 0)
	if err != nil {
		t.Fatalf("Open(:memory:): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestOpenInvalidPath(t *testing.T) {
	_, err := Open("/this/path/does/not/exist/syzy_test.db",
		OpenReadWrite|OpenCreate|OpenURI)
	if err == nil {
		t.Fatal("expected error for unwritable path; got nil")
	}
	var sqErr Error
	if !errors.As(err, &sqErr) {
		t.Fatalf("expected sqlitebridge.Error; got %T: %v", err, err)
	}
	if sqErr.Code == ResultOK {
		t.Errorf("error code should be non-OK; got %d", sqErr.Code)
	}
}

func TestOpenCloseFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	c, err := Open(path, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestExecDDLAndPragma(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	// Multi-statement Exec.
	if err := c.Exec(`INSERT INTO t (id, n) VALUES (1, 'a'); INSERT INTO t (id, n) VALUES (2, 'b');`); err != nil {
		t.Fatalf("multi-stmt INSERT: %v", err)
	}
	if got := c.Changes(); got != 1 {
		// Changes() reflects only the most recent statement; the second INSERT.
		t.Errorf("Changes() after 2 INSERTs = %d, want 1", got)
	}
	// PRAGMA returns rows but Exec discards them — should not fail.
	if err := c.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("PRAGMA: %v", err)
	}
}

func TestExecSyntaxError(t *testing.T) {
	c := memDB(t)
	err := c.Exec(`THIS IS NOT VALID SQL`)
	if err == nil {
		t.Fatal("expected syntax error; got nil")
	}
	var sqErr Error
	if !errors.As(err, &sqErr) {
		t.Fatalf("expected sqlitebridge.Error; got %T: %v", err, err)
	}
	if sqErr.Msg == "" {
		t.Error("expected non-empty error message")
	}
	if sqErr.Code == ResultOK {
		t.Errorf("error code should be non-OK; got %d", sqErr.Code)
	}
}

func TestPrepareBindStepRoundtrip(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT, v BLOB, f REAL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	ins, tail, err := c.Prepare(`INSERT INTO t (id, n, v, f) VALUES (?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("Prepare INSERT: %v", err)
	}
	if tail != "" {
		t.Errorf("Prepare tail = %q; want empty", tail)
	}
	defer ins.Finalize()

	if got := ins.BindParamCount(); got != 4 {
		t.Errorf("BindParamCount = %d; want 4", got)
	}

	cases := []struct {
		id    int64
		name  string
		blob  []byte
		flt   float64
		isNil bool // bind n as NULL instead of name
	}{
		{1, "alpha", []byte{0x01, 0x02, 0x03}, 1.5, false},
		{2, "", []byte{}, 0.0, false}, // empty text + empty blob
		{3, "gamma", nil, -2.25, true},
	}
	for _, tc := range cases {
		if err := ins.Reset(); err != nil {
			t.Fatalf("Reset: %v", err)
		}
		if err := ins.ClearBindings(); err != nil {
			t.Fatalf("ClearBindings: %v", err)
		}
		if err := ins.BindInt64(1, tc.id); err != nil {
			t.Fatalf("BindInt64: %v", err)
		}
		if tc.isNil {
			if err := ins.BindNull(2); err != nil {
				t.Fatalf("BindNull: %v", err)
			}
		} else {
			if err := ins.BindText(2, tc.name); err != nil {
				t.Fatalf("BindText: %v", err)
			}
		}
		if err := ins.BindBlob(3, tc.blob); err != nil {
			t.Fatalf("BindBlob: %v", err)
		}
		if err := ins.BindFloat64(4, tc.flt); err != nil {
			t.Fatalf("BindFloat64: %v", err)
		}
		hasRow, err := ins.Step()
		if err != nil {
			t.Fatalf("Step INSERT: %v", err)
		}
		if hasRow {
			t.Error("INSERT returned a row")
		}
	}

	// Read back.
	q, _, err := c.Prepare(`SELECT id, n, v, f, typeof(n), typeof(v) FROM t ORDER BY id`)
	if err != nil {
		t.Fatalf("Prepare SELECT: %v", err)
	}
	defer q.Finalize()
	if got := q.ColumnCount(); got != 6 {
		t.Errorf("ColumnCount = %d; want 6", got)
	}
	if got := q.ColumnName(0); got != "id" {
		t.Errorf("ColumnName(0) = %q; want id", got)
	}

	for i, tc := range cases {
		hasRow, err := q.Step()
		if err != nil {
			t.Fatalf("row %d Step: %v", i, err)
		}
		if !hasRow {
			t.Fatalf("row %d: missing", i)
		}
		if got := q.ColumnInt64(0); got != tc.id {
			t.Errorf("row %d id = %d; want %d", i, got, tc.id)
		}
		if tc.isNil {
			if got := q.ColumnType(1); got != ColumnNull {
				t.Errorf("row %d n type = %v; want ColumnNull", i, got)
			}
			if got := q.ColumnText(4); got != "null" {
				t.Errorf("row %d typeof(n) = %q; want null", i, got)
			}
		} else {
			if got := q.ColumnText(1); got != tc.name {
				t.Errorf("row %d n = %q; want %q", i, got, tc.name)
			}
			if got := q.ColumnText(4); got != "text" {
				t.Errorf("row %d typeof(n) = %q; want text", i, got)
			}
		}
		if got := q.ColumnBlob(2); !bytes.Equal(got, tc.blob) && !(len(got) == 0 && len(tc.blob) == 0) {
			t.Errorf("row %d v = %v; want %v", i, got, tc.blob)
		}
		if got := q.ColumnText(5); got != "blob" {
			t.Errorf("row %d typeof(v) = %q; want blob", i, got)
		}
		if got := q.ColumnFloat64(3); got != tc.flt {
			t.Errorf("row %d f = %v; want %v", i, got, tc.flt)
		}
	}
	hasRow, err := q.Step()
	if err != nil {
		t.Fatalf("trailing Step: %v", err)
	}
	if hasRow {
		t.Error("expected no more rows")
	}
}

func TestPrepareTail(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	sql := `INSERT INTO t (id) VALUES (1); SELECT id FROM t;`
	stmt, tail, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if stmt == nil {
		t.Fatal("expected first prepared statement")
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("Step first: %v", err)
	}
	if tail == "" {
		t.Fatal("expected non-empty tail after first Prepare")
	}

	stmt2, tail2, err := c.Prepare(tail)
	if err != nil {
		t.Fatalf("Prepare tail: %v", err)
	}
	if stmt2 == nil {
		t.Fatal("expected SELECT statement from tail")
	}
	defer stmt2.Finalize()
	hasRow, err := stmt2.Step()
	if err != nil {
		t.Fatalf("Step SELECT: %v", err)
	}
	if !hasRow {
		t.Fatal("expected a row")
	}
	if got := stmt2.ColumnInt64(0); got != 1 {
		t.Errorf("id = %d; want 1", got)
	}
	if tail2 != "" {
		s, _, err := c.Prepare(tail2)
		if err != nil {
			t.Fatalf("Prepare trailing whitespace: %v", err)
		}
		if s != nil {
			t.Errorf("expected nil stmt for trailing whitespace; got non-nil")
			s.Finalize()
		}
	}
}

func TestPrepareEmpty(t *testing.T) {
	c := memDB(t)
	stmt, tail, err := c.Prepare("")
	if err != nil {
		t.Fatalf("Prepare(\"\"): %v", err)
	}
	if stmt != nil {
		t.Error("expected nil stmt for empty SQL")
	}
	if tail != "" {
		t.Errorf("tail = %q; want empty", tail)
	}

	stmt, tail, err = c.Prepare("   -- just a comment\n")
	if err != nil {
		t.Fatalf("Prepare(comment): %v", err)
	}
	if stmt != nil {
		t.Error("expected nil stmt for comment-only SQL")
	}
	_ = tail
}

func TestPrepareSyntaxError(t *testing.T) {
	c := memDB(t)
	_, _, err := c.Prepare(`SELECT FROM nope`)
	if err == nil {
		t.Fatal("expected syntax error")
	}
	var sqErr Error
	if !errors.As(err, &sqErr) {
		t.Fatalf("expected sqlitebridge.Error; got %T", err)
	}
	if sqErr.Code == ResultOK {
		t.Errorf("error code should be non-OK; got %d", sqErr.Code)
	}
}

func TestColumnTypeNull(t *testing.T) {
	c := memDB(t)
	q, _, err := c.Prepare(`SELECT NULL`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer q.Finalize()
	if _, err := q.Step(); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if got := q.ColumnType(0); got != ColumnNull {
		t.Errorf("ColumnType(0) = %v; want ColumnNull", got)
	}
	if got := q.ColumnText(0); got != "" {
		t.Errorf("ColumnText(0) = %q; want empty", got)
	}
	if got := q.ColumnBlob(0); got != nil {
		t.Errorf("ColumnBlob(0) = %v; want nil", got)
	}
}

func TestConstraintErrorCode(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := c.Exec(`INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	err := c.Exec(`INSERT INTO t (id) VALUES (1)`)
	if err == nil {
		t.Fatal("expected PK-constraint error; got nil")
	}
	var sqErr Error
	if !errors.As(err, &sqErr) {
		t.Fatalf("expected sqlitebridge.Error; got %T", err)
	}
	// Primary code should be SQLITE_CONSTRAINT (19); extended codes share that
	// low byte, so mask before compare.
	if sqErr.Code&0xff != ResultConstraint {
		t.Errorf("expected primary code SQLITE_CONSTRAINT (%d); got %d (extended %d)",
			ResultConstraint, sqErr.Code&0xff, sqErr.Code)
	}
}

func TestChangesAndLastInsertRowID(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, n TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := c.Exec(`INSERT INTO t (n) VALUES ('a')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if got := c.LastInsertRowID(); got != 1 {
		t.Errorf("LastInsertRowID = %d; want 1", got)
	}
	if got := c.Changes(); got != 1 {
		t.Errorf("Changes after INSERT = %d; want 1", got)
	}
	if err := c.Exec(`UPDATE t SET n = 'aa' WHERE id = 1`); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if got := c.Changes(); got != 1 {
		t.Errorf("Changes after UPDATE = %d; want 1", got)
	}
}

func TestMultipleConnsIndependent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.db")
	a, err := Open(path, 0)
	if err != nil {
		t.Fatalf("Open a: %v", err)
	}
	defer a.Close()
	if err := a.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := a.Exec(`INSERT INTO t (id) VALUES (42)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	b, err := Open(path, 0)
	if err != nil {
		t.Fatalf("Open b: %v", err)
	}
	defer b.Close()
	q, _, err := b.Prepare(`SELECT id FROM t`)
	if err != nil {
		t.Fatalf("Prepare on b: %v", err)
	}
	defer q.Finalize()
	hasRow, err := q.Step()
	if err != nil {
		t.Fatalf("Step on b: %v", err)
	}
	if !hasRow {
		t.Fatal("expected a row on b")
	}
	if got := q.ColumnInt64(0); got != 42 {
		t.Errorf("b sees id = %d; want 42", got)
	}
}

// TestSQLPreprocessor exercises the Conn.SetSQLPreprocessor hook used
// by the producer to rewrite rowid-alias DDL before SQLite sees it.
// The preprocessor must fire for both Exec and Prepare and must let a
// returned error short-circuit the call.
func TestSQLPreprocessor(t *testing.T) {
	c := memDB(t)
	var seen []string
	c.SetSQLPreprocessor(func(sql string) (string, error) {
		seen = append(seen, sql)
		if sql == "CREATE TABLE rewrite_me (id INTEGER PRIMARY KEY)" {
			return "CREATE TABLE rewrite_me (id INT PRIMARY KEY NOT NULL)", nil
		}
		return sql, nil
	})

	// Exec routes through the preprocessor and runs the rewritten DDL.
	if err := c.Exec(`CREATE TABLE rewrite_me (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	// Confirm the rewritten shape landed in SQLite (declared type is the
	// rewritten "INT", not the original "INTEGER").
	stmt, _, err := c.Prepare(`SELECT type FROM pragma_table_info('rewrite_me') WHERE name='id'`)
	if err != nil {
		t.Fatalf("introspect prepare: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		t.Fatalf("introspect step: err=%v hasRow=%v", err, hasRow)
	}
	if got := stmt.ColumnText(0); got != "INT" {
		t.Errorf("declared type = %q; want INT (rewritten)", got)
	}
	stmt.Finalize()

	// Prepare also routes through the preprocessor.
	stmt2, _, err := c.Prepare(`SELECT 1`)
	if err != nil {
		t.Fatalf("Prepare passthrough: %v", err)
	}
	stmt2.Finalize()

	// A non-nil error from the preprocessor short-circuits the call.
	c.SetSQLPreprocessor(func(_ string) (string, error) {
		return "", errors.New("nope")
	})
	if err := c.Exec(`SELECT 1`); err == nil || err.Error() != "nope" {
		t.Fatalf("Exec preprocessor error: got %v; want \"nope\"", err)
	}
	if _, _, err := c.Prepare(`SELECT 1`); err == nil || err.Error() != "nope" {
		t.Fatalf("Prepare preprocessor error: got %v; want \"nope\"", err)
	}

	// We saw at least one DDL pass-through and the rewrite candidate.
	if len(seen) < 2 {
		t.Errorf("preprocessor invoked %d times; expected ≥2", len(seen))
	}

	// Clearing the preprocessor restores raw behavior.
	c.SetSQLPreprocessor(nil)
	if err := c.Exec(`SELECT 1`); err != nil {
		t.Fatalf("Exec after clear: %v", err)
	}
}
