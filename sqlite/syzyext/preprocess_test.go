package syzyext

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/syzy/sqlitebridge"
)

// testConn returns a real Conn with a preprocessor that mimics the
// producer's chain shape: rewrites a bare `INTEGER PRIMARY KEY` column
// to the gen_id form, turns `ALTER TABLE noop ...` into a SELECT 1
// line-comment no-op (the ADD COLUMN IF NOT EXISTS shape), and passes
// everything else through. The real chain is covered by the producer
// rewrite tests and the extension E2E test.
func testConn(t *testing.T) *sqlitebridge.Conn {
	t.Helper()
	conn, err := sqlitebridge.Open(filepath.Join(t.TempDir(), "t.db"), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetSQLPreprocessor(func(sql string) (string, error) {
		if strings.HasPrefix(sql, "ALTER TABLE noop") {
			return "SELECT 1 -- syzy: redundant DDL", nil
		}
		if strings.Contains(sql, "INTEGER PRIMARY KEY") {
			return strings.Replace(sql, "INTEGER PRIMARY KEY",
				"INT PRIMARY KEY NOT NULL DEFAULT (gen_id('t'))", 1), nil
		}
		return sql, nil
	})
	return conn
}

func TestPreprocessPrepareSingleStatement(t *testing.T) {
	conn := testConn(t)
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, ts INT)"
	out, consumed, changed := PreprocessPrepare(conn, sql)
	if !changed {
		t.Fatal("expected rewrite")
	}
	if consumed != len(sql) {
		t.Fatalf("consumed = %d, want %d", consumed, len(sql))
	}
	if !strings.Contains(out, "gen_id('t')") {
		t.Fatalf("rewritten = %q", out)
	}
}

func TestPreprocessPrepareUnchanged(t *testing.T) {
	conn := testConn(t)
	for _, sql := range []string{
		"SELECT * FROM t",
		"CREATE TABLE t (id INT PRIMARY KEY)",
		"",
	} {
		if _, _, changed := PreprocessPrepare(conn, sql); changed {
			t.Fatalf("unexpected rewrite of %q", sql)
		}
	}
}

func TestPreprocessPrepareMultiStatementTail(t *testing.T) {
	conn := testConn(t)
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t DEFAULT VALUES;"
	out, consumed, changed := PreprocessPrepare(conn, sql)
	if !changed {
		t.Fatal("expected rewrite")
	}
	if sql[consumed:] != " INSERT INTO t DEFAULT VALUES;" {
		t.Fatalf("tail = %q", sql[consumed:])
	}
	if !strings.HasSuffix(out, ";") || !strings.Contains(out, "gen_id") {
		t.Fatalf("rewritten = %q", out)
	}
}

func TestPreprocessPrepareErrorPassesThrough(t *testing.T) {
	conn := testConn(t)
	conn.SetSQLPreprocessor(func(string) (string, error) {
		return "", &sqlitebridge.Error{Code: 1, Msg: "boom"}
	})
	if _, _, changed := PreprocessPrepare(conn, "CREATE TABLE t (id INT)"); changed {
		t.Fatal("error must report changed=false")
	}
}

func TestPreprocessExecRewritesEachStatement(t *testing.T) {
	conn := testConn(t)
	sql := "CREATE TABLE a (id INTEGER PRIMARY KEY);\nCREATE TABLE b (id INTEGER PRIMARY KEY);"
	out, changed := PreprocessExec(conn, sql)
	if !changed {
		t.Fatal("expected rewrite")
	}
	if got := strings.Count(out, "gen_id('t')"); got != 2 {
		t.Fatalf("want 2 rewrites, got %d in %q", got, out)
	}
}

func TestPreprocessExecCommentRewriteKeepsNextStatement(t *testing.T) {
	conn := testConn(t)
	// The first segment rewrites to a line-comment-terminated no-op;
	// the second must survive on its own statement.
	sql := "ALTER TABLE noop ADD COLUMN IF NOT EXISTS x INT; CREATE TABLE c (id INT);"
	out, changed := PreprocessExec(conn, sql)
	if !changed {
		t.Fatal("expected rewrite")
	}
	conn2 := testConn(t)
	if err := conn2.Exec(out); err != nil {
		t.Fatalf("rewritten exec string does not run: %v\n%q", err, out)
	}
	exists, err := sqlitebridge.ObjectExists(conn2, "table", "c")
	if err != nil || !exists {
		t.Fatalf("table c missing after exec (err=%v):\n%q", err, out)
	}
}

func TestPreprocessExecUnchanged(t *testing.T) {
	conn := testConn(t)
	if _, changed := PreprocessExec(conn, "SELECT 1; SELECT 2;"); changed {
		t.Fatal("unexpected change")
	}
}
