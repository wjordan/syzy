package syzyext

import (
	"strings"

	"github.com/wjordan/syzy/sqlitebridge"
)

// This file implements the SQL-rewrite entry points for the loadable
// extension's prepare/exec interposers (cmd/syzy-ext/autoload_shim.c).
// In extension mode the host app's sqlite3_prepare*/sqlite3_exec calls
// bypass sqlitebridge.Conn.Prepare/Exec, so the producer's SQL
// preprocessor (rowid-alias rewrite, ADD COLUMN IF NOT EXISTS) never
// sees app SQL. The LD_PRELOAD shim interposes the prepare family and
// routes DDL-looking statements through these functions before handing
// the text to the real SQLite.
//
// Both functions are pass-through-on-any-doubt: a parse failure, a
// preprocessor error, or an unchanged statement reports changed=false
// and the shim forwards the caller's original text untouched. The
// admission hook remains the backstop for anything not rewritten here.

// PreprocessPrepare rewrites the first statement of sql for a
// sqlite3_prepare* call on an attached connection. sql is exactly the
// byte range the app passed (the shim has already applied nByte).
//
// On a rewrite it returns the replacement text for the first statement
// and the byte offset in sql where the remainder begins; the shim
// prepares the rewritten text and points the caller's *pzTail at
// sql+consumed, preserving multi-statement tail-loop semantics against
// the caller's own buffer. changed=false means "use the original".
func PreprocessPrepare(conn *sqlitebridge.Conn, sql string) (rewritten string, consumed int, changed bool) {
	first, consumed := sqlitebridge.FirstStatement(sql)
	out, err := conn.PreprocessSQL(first)
	if err != nil || out == first {
		return "", 0, false
	}
	return out, consumed, true
}

// PreprocessExec rewrites every statement of a (possibly
// multi-statement) sqlite3_exec string. Unlike the prepare path, exec
// consumes the whole string in one call, so the shim cannot rely on
// per-statement interposition (libsqlite3's internal exec→prepare
// calls bypass the PLT on -Bsymbolic / -fno-semantic-interposition
// builds). changed=false means "run the original".
func PreprocessExec(conn *sqlitebridge.Conn, sql string) (string, bool) {
	var b strings.Builder
	changed := false
	rest := sql
	for len(rest) > 0 {
		seg, consumed := sqlitebridge.FirstStatement(rest)
		out, err := conn.PreprocessSQL(seg)
		if err != nil || out == seg {
			b.WriteString(seg)
		} else {
			changed = true
			b.WriteString(out)
			if consumed < len(rest) && !strings.HasSuffix(strings.TrimRight(out, " \t\n\r"), ";") {
				// A rewrite may drop the segment's ';' or end in a line
				// comment (e.g. "SELECT 1 -- syzy: ..."); re-terminate on
				// a fresh line so the next statement isn't swallowed.
				b.WriteString("\n;")
			}
		}
		rest = rest[consumed:]
	}
	if !changed {
		return "", false
	}
	return b.String(), true
}
