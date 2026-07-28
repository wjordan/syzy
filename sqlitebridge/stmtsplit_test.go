package sqlitebridge

import "testing"

func TestFirstStatementSingle(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE t (id INTEGER PRIMARY KEY)",
		"CREATE TABLE t (id INTEGER PRIMARY KEY);",
		"CREATE TABLE t (id INTEGER PRIMARY KEY);  \n",
		"SELECT 1",
		"",
		"   ",
	} {
		stmt, consumed := FirstStatement(sql)
		// Single statement (with at most trailing whitespace after the
		// ';'): the statement must be a prefix and the remainder blank.
		if stmt != sql[:consumed] {
			t.Fatalf("FirstStatement(%q) stmt %q != prefix %q", sql, stmt, sql[:consumed])
		}
		for _, c := range sql[consumed:] {
			if c != ' ' && c != '\n' && c != '\t' {
				t.Fatalf("FirstStatement(%q) left non-blank remainder %q", sql, sql[consumed:])
			}
		}
	}
}

func TestFirstStatementMulti(t *testing.T) {
	sql := "CREATE TABLE a (id INT); INSERT INTO a VALUES (1);"
	stmt, consumed := FirstStatement(sql)
	if want := "CREATE TABLE a (id INT);"; stmt != want {
		t.Fatalf("stmt = %q, want %q", stmt, want)
	}
	if sql[consumed:] != " INSERT INTO a VALUES (1);" {
		t.Fatalf("remainder = %q", sql[consumed:])
	}
}

func TestFirstStatementSemicolonInString(t *testing.T) {
	sql := `INSERT INTO a VALUES ('x;y'); SELECT 1;`
	stmt, _ := FirstStatement(sql)
	if want := `INSERT INTO a VALUES ('x;y');`; stmt != want {
		t.Fatalf("stmt = %q, want %q", stmt, want)
	}
}

func TestFirstStatementEscapedQuote(t *testing.T) {
	sql := `INSERT INTO a VALUES ('it''s; fine'); SELECT 1;`
	stmt, _ := FirstStatement(sql)
	if want := `INSERT INTO a VALUES ('it''s; fine');`; stmt != want {
		t.Fatalf("stmt = %q, want %q", stmt, want)
	}
}

func TestFirstStatementComments(t *testing.T) {
	sql := "CREATE TABLE a (id INT) -- trailing; not a split\n; SELECT 1;"
	stmt, consumed := FirstStatement(sql)
	if sql[consumed:] != " SELECT 1;" {
		t.Fatalf("remainder = %q (stmt %q)", sql[consumed:], stmt)
	}
	sql = "CREATE TABLE a (id INT) /* block; comment */; SELECT 1;"
	_, consumed = FirstStatement(sql)
	if sql[consumed:] != " SELECT 1;" {
		t.Fatalf("block comment: remainder = %q", sql[consumed:])
	}
}

func TestFirstStatementTriggerBody(t *testing.T) {
	// The ';' terminating the trigger body's inner statement must not
	// split the CREATE TRIGGER — sqlite3_complete tracks BEGIN...END.
	sql := "CREATE TRIGGER tr AFTER INSERT ON a BEGIN UPDATE a SET x = 1; END; SELECT 1;"
	stmt, consumed := FirstStatement(sql)
	if want := "CREATE TRIGGER tr AFTER INSERT ON a BEGIN UPDATE a SET x = 1; END;"; stmt != want {
		t.Fatalf("stmt = %q, want %q", stmt, want)
	}
	if sql[consumed:] != " SELECT 1;" {
		t.Fatalf("remainder = %q", sql[consumed:])
	}
}

func TestCompleteBasics(t *testing.T) {
	if Complete("CREATE TABLE t (id INT)") {
		t.Fatal("statement without ';' reported complete")
	}
	if !Complete("CREATE TABLE t (id INT);") {
		t.Fatal("terminated statement reported incomplete")
	}
}
