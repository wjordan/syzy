package producer

import (
	"strings"
	"testing"
)

func TestShouldRewriteRowidAlias(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want bool
	}{
		{"bare INTEGER PK", `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`, true},
		{"AUTOINCREMENT", `CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`, true},
		{"case insensitive", `create table t (id integer primary key, v text)`, true},
		{"mixed case", `Create Table t (Id Integer Primary Key, v TEXT)`, true},
		{"extra whitespace", `CREATE TABLE t (id  INTEGER   PRIMARY  KEY, v TEXT)`, true},
		{"if not exists", `CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)`, true},
		{"single-column table", `CREATE TABLE t (id INTEGER PRIMARY KEY)`, true},

		// Already safe.
		{"WITHOUT ROWID", `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID`, false},
		{"INT not INTEGER", `CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT)`, false},
		{"BIGINT", `CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)`, false},
		{"composite PK", `CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))`, false},
		{"user-supplied default", `CREATE TABLE t (id INTEGER PRIMARY KEY DEFAULT (uuidv7()), v TEXT)`, false},
		{"user literal default", `CREATE TABLE t (id INTEGER PRIMARY KEY DEFAULT 42, v TEXT)`, false},

		// Non-CREATE-TABLE.
		{"ALTER", `ALTER TABLE t ADD COLUMN id INTEGER PRIMARY KEY`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := classifyDDL(c.sql)
			if err != nil {
				t.Fatalf("classifyDDL: %v", err)
			}
			if got := shouldRewriteRowidAlias(p); got != c.want {
				t.Errorf("shouldRewriteRowidAlias = %v; want %v\n  sql: %s", got, c.want, c.sql)
			}
		})
	}
}

// TestSynthesizeRewrittenSQL checks the splice preserves everything in
// the source except the PK column declaration. Type modifiers, CHECK
// clauses, COLLATE, comments — all should pass through verbatim, since
// the splice replaces only the rowid-alias column's source span.
func TestSynthesizeRewrittenSQL(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		wantPK    string // expected rewritten column declaration
		wantOther string // a substring that must survive verbatim
	}{
		{
			"basic",
			`CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)`,
			`"id" INT PRIMARY KEY NOT NULL DEFAULT (gen_id('posts'))`,
			`title TEXT`,
		},
		{
			"AUTOINCREMENT dropped",
			`CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)`,
			`"id" INT PRIMARY KEY NOT NULL DEFAULT (gen_id('posts'))`,
			`title TEXT`,
		},
		{
			"preserves VARCHAR(255) on a sibling column",
			`CREATE TABLE u (id INTEGER PRIMARY KEY, email VARCHAR(255) NOT NULL)`,
			`"id" INT PRIMARY KEY NOT NULL DEFAULT (gen_id('u'))`,
			`email VARCHAR(255) NOT NULL`,
		},
		{
			"preserves CHECK on a sibling column",
			`CREATE TABLE u (id INTEGER PRIMARY KEY, age INT CHECK (age >= 0))`,
			`"id" INT PRIMARY KEY NOT NULL DEFAULT (gen_id('u'))`,
			`age INT CHECK (age >= 0)`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := makeRowidAliasPreprocessor(false)(c.sql)
			if err != nil {
				t.Fatalf("preprocessor: %v", err)
			}
			if !strings.Contains(out, c.wantPK) {
				t.Errorf("rewritten SQL missing PK clause %q\n  got: %s", c.wantPK, out)
			}
			if !strings.Contains(out, c.wantOther) {
				t.Errorf("rewritten SQL lost sibling text %q\n  got: %s", c.wantOther, out)
			}
		})
	}
}

func TestPreprocessRowidAlias_Passthrough(t *testing.T) {
	cases := []string{
		`SELECT 1`,
		`INSERT INTO t (v) VALUES ('hi')`,
		`CREATE TABLE t (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('t')), v TEXT)`,
		`CREATE TABLE t (id INTEGER PRIMARY KEY DEFAULT (uuidv7()), v TEXT)`,
		`CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY (a, b))`,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID`,
		`CREATE TABLE _local (id INTEGER PRIMARY KEY)`, // local-only carve-out
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := makeRowidAliasPreprocessor(false)(sql)
			if err != nil {
				t.Fatalf("preprocessor: %v", err)
			}
			if out != sql {
				t.Errorf("expected passthrough\n  in:  %s\n  out: %s", sql, out)
			}
		})
	}
}

// TestPreprocessRowidAlias_ParseErrorFallsThrough verifies a SQL string
// the DDL parser can't classify (e.g., a typo) is returned untouched.
// The trace_v2 admission hook will re-classify with the same error and
// surface the proper reject. Preprocessor must not pre-empt that path.
func TestPreprocessRowidAlias_ParseErrorFallsThrough(t *testing.T) {
	sql := `CREATE TABLE AS SELECT * FROM other`
	out, err := makeRowidAliasPreprocessor(false)(sql)
	if err != nil {
		t.Fatalf("preprocessor returned error; want passthrough: %v", err)
	}
	if out != sql {
		t.Errorf("expected passthrough on parse error\n  in:  %s\n  out: %s", sql, out)
	}
}

// TestShouldRewriteRowidAlias_BailCases covers the carve-outs added in
// response to the codex review: INTEGER PRIMARY KEY DESC isn't a SQLite
// rowid alias, and column-level CHECK/COLLATE/REFERENCES/ON CONFLICT/
// named CONSTRAINT on the PK can't be losslessly preserved by the
// splice, so the preprocessor declines to rewrite them.
func TestShouldRewriteRowidAlias_BailCases(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"DESC PK", `CREATE TABLE t (id INTEGER PRIMARY KEY DESC, v TEXT)`},
		{"CHECK on PK", `CREATE TABLE t (id INTEGER PRIMARY KEY CHECK (id > 0), v TEXT)`},
		{"COLLATE on PK", `CREATE TABLE t (id INTEGER PRIMARY KEY COLLATE NOCASE, v TEXT)`},
		{"REFERENCES on PK", `CREATE TABLE t (id INTEGER PRIMARY KEY REFERENCES p(id), v TEXT)`},
		{"ON CONFLICT on PK", `CREATE TABLE t (id INTEGER PRIMARY KEY ON CONFLICT REPLACE, v TEXT)`},
		{"named CONSTRAINT on PK", `CREATE TABLE t (id INTEGER CONSTRAINT pk_id PRIMARY KEY, v TEXT)`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := classifyDDL(c.sql)
			if err != nil {
				t.Fatalf("classifyDDL: %v", err)
			}
			if shouldRewriteRowidAlias(p) {
				t.Errorf("expected false; PK column carries a clause the splice cannot preserve")
			}
		})
	}
}

// TestLeadingWhitespace_BOMAndFormFeed confirms leading UTF-8 BOM and
// form-feed do not divert DDL classification into ddlNone. SQLite treats
// both as whitespace; classifyDDL must too, or BOM-prefixed CREATE TABLE
// would slip past admission while SQLite still executed the DDL.
func TestLeadingWhitespace_BOMAndFormFeed(t *testing.T) {
	cases := []string{
		"\xEF\xBB\xBFCREATE TABLE t (id INTEGER PRIMARY KEY)",
		"\fCREATE TABLE t (id INTEGER PRIMARY KEY)",
		"\f\f\fCREATE TABLE t (id INTEGER PRIMARY KEY)",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			p, err := classifyDDL(sql)
			if err != nil {
				t.Fatalf("classifyDDL: %v", err)
			}
			if p.Kind != ddlCreateTable {
				t.Errorf("Kind = %v; want ddlCreateTable", p.Kind)
			}
		})
	}
}
