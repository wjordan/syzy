package producer

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// shouldRewriteRowidAlias reports whether p's CREATE TABLE matches the
// rowid-alias `INTEGER PRIMARY KEY [AUTOINCREMENT]` pattern that breaks
// under multi-writer replication. Only the literal type name "INTEGER"
// triggers SQLite's rowid alias (INT, BIGINT, etc. do not).
//
// Exempt cases:
//   - user-supplied DEFAULT (caller owns the multi-writer behavior),
//   - `PRIMARY KEY DESC` (SQLite does not alias the rowid for DESC PKs),
//   - column-level constraints the splice can't preserve losslessly
//     (CHECK, COLLATE, named CONSTRAINT, ON CONFLICT, REFERENCES) —
//     admission's validateNoRowidAlias backstop rejects these so users
//     refactor rather than getting a silently mangled DDL.
func shouldRewriteRowidAlias(p parsedDDL) bool {
	if p.Kind != ddlCreateTable || p.WithoutRowid || len(p.PKColumns) != 1 {
		return false
	}
	pkName := p.PKColumns[0]
	for _, c := range p.Columns {
		if c.Name != pkName {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(c.Type), "INTEGER") {
			return false
		}
		if c.Default != "" || c.PKDesc || c.HasUnsupportedRewrite || c.FK != nil {
			return false
		}
		return true
	}
	return false
}

// spliceRowidAliasRewrite returns the user's CREATE TABLE text with the
// rowid-alias PK column declaration replaced by the multi-writer-safe
// shape `"<col>" INT PRIMARY KEY NOT NULL DEFAULT (gen_id('<table>'))`.
// Sibling columns, table-level constraints, CHECK clauses, and comments
// pass through verbatim because the splice operates on raw source spans
// captured by the tokenizer (SrcStart/SrcEnd on the PK column).
//
// Behavior contract: the rewritten table stays on a rowid table (so
// blob_patch remains available); INT (not INTEGER) suppresses the
// rowid-alias rule so the DEFAULT actually runs; AUTOINCREMENT is
// dropped (gen_id partitioning replaces sqlite_sequence's monotonic
// guarantee). Caller must have first checked shouldRewriteRowidAlias.
func spliceRowidAliasRewrite(p parsedDDL) (string, error) {
	pkName := p.PKColumns[0]
	for _, c := range p.Columns {
		if c.Name != pkName {
			continue
		}
		if c.SrcStart == 0 || c.SrcEnd <= c.SrcStart {
			return "", errors.New("rowid-alias rewrite: PK column source span unavailable")
		}
		// SrcEnd points at the terminating ',' or ')'; that character
		// belongs to the surrounding column list, not the column we're
		// rewriting, so we keep it in the output.
		rewritten := fmt.Sprintf(`%s INT PRIMARY KEY NOT NULL DEFAULT (gen_id('%s'))`,
			sqlitebridge.QuoteIdent(c.Name), p.Name)
		return p.RawSQL[:c.SrcStart] + rewritten + p.RawSQL[c.SrcEnd:], nil
	}
	return "", errors.New("rowid-alias rewrite: PK column not found in p.Columns")
}

// makeRowidAliasPreprocessor returns the sqlitebridge.SQLPreprocessor
// the producer installs on its app conn. Non-DDL, non-matching DDL, and
// parser errors pass through unchanged (the trace_v2 admission hook
// will re-classify parser errors and surface the proper reject).
//
// Local-only tables are exempt: they don't replicate, so multi-writer
// collisions aren't a concern, and the rewrite would needlessly cost
// the user the rowid-alias behavior (last_insert_rowid + max(rowid)+1
// allocation) they asked for. replicateUnderscore mirrors the producer
// Config flag and selects whether underscore-prefixed tables are treated
// as local-only (default) or as ordinary replicated tables (opt-in).
// sqlite_* is always local-only. Matches the carve-out in
// ddl.go:handleStmt via isLocalOnlyName.
func makeRowidAliasPreprocessor(replicateUnderscore bool) sqlitebridge.SQLPreprocessor {
	return func(sql string) (string, error) {
		parsed, err := classifyDDL(sql)
		if err != nil {
			return sql, nil
		}
		if isLocalOnlyName(parsed, replicateUnderscore) || !shouldRewriteRowidAlias(parsed) {
			return sql, nil
		}
		return spliceRowidAliasRewrite(parsed)
	}
}

// bareGenIDRE matches the no-argument gen_id() call form inside a
// column DEFAULT.
var bareGenIDRE = regexp.MustCompile(`(?i)\bgen_id\s*\(\s*\)`)

// makeBareGenIDPreprocessor rewrites `DEFAULT (gen_id())` to the
// table-qualified `gen_id('<table>')` the runtime function requires
// (id allocation is per-table; the SQL function cannot learn the
// table it is defaulting for at call time). This lets schema
// templates and framework primary-key overrides declare one reusable
// default without repeating the table name. CREATE TABLE splices
// per-column source spans; ADD COLUMN has exactly one column (and no
// spans), so the whole-statement replace is unambiguous there.
func makeBareGenIDPreprocessor() sqlitebridge.SQLPreprocessor {
	return func(sql string) (string, error) {
		parsed, err := classifyDDL(sql)
		if err != nil {
			return sql, nil
		}
		hasBare := func(c parsedColumn) bool {
			return catalog.ClassifyPKDefault(c.Default).Kind == catalog.PKDefaultGenIDBare
		}
		qualified := fmt.Sprintf("gen_id('%s')", strings.ReplaceAll(parsed.Name, "'", "''"))
		switch parsed.Kind {
		case ddlCreateTable:
			out := parsed.RawSQL
			// Reverse source order so earlier spans stay valid.
			for i := len(parsed.Columns) - 1; i >= 0; i-- {
				c := parsed.Columns[i]
				if !hasBare(c) || c.SrcStart == 0 || c.SrcEnd <= c.SrcStart {
					continue
				}
				span := bareGenIDRE.ReplaceAllString(out[c.SrcStart:c.SrcEnd], qualified)
				out = out[:c.SrcStart] + span + out[c.SrcEnd:]
			}
			return out, nil
		case ddlAlterTableAddColumn:
			if len(parsed.Columns) == 1 && hasBare(parsed.Columns[0]) {
				return bareGenIDRE.ReplaceAllString(parsed.RawSQL, qualified), nil
			}
		}
		return sql, nil
	}
}

// addColumnIfNotExistsRE matches `ADD [COLUMN] IF NOT EXISTS` inside an
// ALTER TABLE statement. Captures the `ADD [COLUMN] ` prefix in group 1
// so a replacement preserves the user's original spacing/keywords.
// (?is) = case-insensitive + . matches newline, both common in
// hand-written migration SQL.
var addColumnIfNotExistsRE = regexp.MustCompile(`(?is)\bADD(\s+COLUMN)?\s+IF\s+NOT\s+EXISTS\s+`)

// ddlKeywordRE matches a leading CREATE/ALTER/DROP (after an optional BOM
// and whitespace) — the only statements the idempotent stage can no-op.
var ddlKeywordRE = regexp.MustCompile(`(?i)^[\s\x{FEFF}]*(?:create|alter|drop)\b`)

// makeAddColumnIfNotExistsPreprocessor accepts the non-standard
// `ALTER TABLE x ADD [COLUMN] IF NOT EXISTS y ...` syntax. SQLite
// itself doesn't support IF NOT EXISTS on ADD COLUMN; without this
// preprocessor a duplicate-column statement would error at PREPARE
// (before trace_v2 ever fires), bypassing admission. The preprocessor
// strips the "IF NOT EXISTS" tokens so SQLite accepts the statement,
// and — if the column is already present in the catalog — rewrites
// the whole statement to a `SELECT 1` no-op. This mirrors how
// `CREATE TABLE IF NOT EXISTS` short-circuits at admission.
//
// Pattern matching is text-level rather than parser-level because
// rqlite/sql doesn't recognize the IF NOT EXISTS form. The regex is
// anchored to `ADD [COLUMN]` to avoid false matches against the same
// phrase appearing in defaults, CHECKs, or comments. Multi-statement
// inputs are intentionally not handled; the producer's preprocessor
// contract is single-statement.
//
// lookup is admission's resolver, not the bare catalog: inside an
// explicit transaction it also sees the DDL that transaction has already
// admitted, so `ADD COLUMN c` followed by `ADD COLUMN IF NOT EXISTS c`
// in one transaction no-ops instead of reaching SQLite as a duplicate.
func makeAddColumnIfNotExistsPreprocessor(lookup func(string) (*catalog.Table, bool)) sqlitebridge.SQLPreprocessor {
	return func(sql string) (string, error) {
		if !addColumnIfNotExistsRE.MatchString(sql) {
			return sql, nil
		}
		// Strip "IF NOT EXISTS" so the parser and SQLite both accept
		// the statement. The captured group ($1) preserves whether the
		// user wrote "ADD COLUMN" or just "ADD".
		stripped := addColumnIfNotExistsRE.ReplaceAllString(sql, "ADD$1 ")
		// Parse the stripped form to learn the table + column. If the
		// rewrite couldn't be classified, fall through and let SQLite
		// produce its own diagnostic.
		parsed, err := classifyDDL(stripped)
		if err != nil || parsed.Kind != ddlAlterTableAddColumn || len(parsed.Columns) != 1 {
			return stripped, nil
		}
		tab, ok := lookup(parsed.Name)
		if !ok {
			// Table not in the catalog — let admission produce the
			// "table not in catalog" error path on the stripped form.
			return stripped, nil
		}
		if _, exists := tab.Column(parsed.Columns[0].Name); !exists {
			// Column absent. Apply the ALTER normally.
			return stripped, nil
		}
		// Column already present. Replace with a SELECT no-op so the
		// caller's Exec succeeds without re-running the ADD COLUMN.
		return fmt.Sprintf("SELECT 1 -- syzy: ADD COLUMN %s.%s already present",
			parsed.Name, parsed.Columns[0].Name), nil
	}
}

// makeSQLPreprocessor composes the rowid-alias, ADD COLUMN IF NOT
// EXISTS, and bare-gen_id preprocessors (ADD COLUMN first, so the
// later stages see parseable SQL; bare-gen_id last, on the final
// text). When idempotent is set, a redundant-DDL stage runs first: a
// statement whose effect is already present becomes SELECT 1.
func makeSQLPreprocessor(app *sqlitebridge.Conn, lookup func(string) (*catalog.Table, bool), replicateUnderscore, idempotent bool) sqlitebridge.SQLPreprocessor {
	addCol := makeAddColumnIfNotExistsPreprocessor(lookup)
	rowid := makeRowidAliasPreprocessor(replicateUnderscore)
	bareGenID := makeBareGenIDPreprocessor()
	redundant := makeIdempotentDDLPreprocessor(app, replicateUnderscore)
	return func(sql string) (string, error) {
		if idempotent {
			out, done, err := redundant(sql)
			if err != nil {
				return sql, err
			}
			if done {
				return out, nil
			}
		}
		out, err := addCol(sql)
		if err != nil {
			return out, err
		}
		out, err = rowid(out)
		if err != nil {
			return out, err
		}
		return bareGenID(out)
	}
}

// makeIdempotentDDLPreprocessor rewrites a replicated DDL to SELECT 1
// when its effect is already present — the writer-path twin of the
// receiver's opAlreadyAppliedInSQLite. Running before prepare is what
// lets a redundant DROP/ADD COLUMN/CREATE/DROP succeed uniformly (SQLite
// would otherwise reject them at prepare with errors indistinguishable
// from a real one). done=true means it owns the statement.
func makeIdempotentDDLPreprocessor(app *sqlitebridge.Conn, replicateUnderscore bool) func(string) (string, bool, error) {
	// Name only the identifier, never the (often multi-line) raw SQL: a
	// -- comment ends at the first newline.
	noop := func(p parsedDDL) string {
		return fmt.Sprintf("SELECT 1 -- syzy: redundant DDL on %q already satisfied", p.Name)
	}
	return func(sql string) (string, bool, error) {
		// Cheap bail-out before the parse: only CREATE/ALTER/DROP can be a
		// redundant DDL, so DML/SELECT (the bulk of writer traffic) skip the
		// regex + classifyDDL below. Comment-prefixed DDL won't match and
		// flows through normal admission instead of no-op'ing — fine, callers
		// submit bare statements.
		if !ddlKeywordRE.MatchString(sql) {
			return sql, false, nil
		}
		// Accept the non-standard ADD COLUMN IF NOT EXISTS form here too,
		// so the strip happens before classifyDDL (which can't read it).
		stripped := addColumnIfNotExistsRE.ReplaceAllString(sql, "ADD$1 ")
		parsed, err := classifyDDL(stripped)
		if err != nil || parsed.Kind == ddlNone || parsed.Kind == ddlBeginOrSavepoint {
			return sql, false, nil
		}
		if isLocalOnlyName(parsed, replicateUnderscore) {
			return sql, false, nil
		}
		present, err := ddlEffectPresent(parsed, app)
		if err != nil {
			return sql, false, err
		}
		if !present {
			return sql, false, nil
		}
		return noop(parsed), true, nil
	}
}

// ddlEffectPresent reports whether parsed's post-state is already present
// locally (name-level, mirroring opAlreadyAppliedInSQLite). Unhandled
// forms return false so they flow through normal admission.
func ddlEffectPresent(p parsedDDL, app *sqlitebridge.Conn) (bool, error) {
	switch p.Kind {
	case ddlCreateTable:
		return sqlitebridge.ObjectExists(app, "table", p.Name)
	case ddlDropTable:
		exists, err := sqlitebridge.ObjectExists(app, "table", p.Name)
		return !exists, err

	case ddlAlterTableAddColumn:
		if len(p.Columns) != 1 {
			return false, nil
		}
		return sqlitebridge.ColumnExists(app, p.Name, p.Columns[0].Name)
	case ddlAlterTableDropColumn:
		exists, err := sqlitebridge.ColumnExists(app, p.Name, p.DropColumn)
		return !exists, err

	case ddlAlterTableRenameTo:
		// Satisfied once the new name exists and the old one is gone.
		newExists, err := sqlitebridge.ObjectExists(app, "table", p.NewName)
		if err != nil || !newExists {
			return false, err
		}
		oldExists, err := sqlitebridge.ObjectExists(app, "table", p.Name)
		return !oldExists, err
	case ddlAlterTableRenameColumn:
		return sqlitebridge.ColumnExists(app, p.Name, p.NewColumn)

	case ddlCreateIndex, ddlCreateUniqueIndex:
		return sqlitebridge.ObjectExists(app, "index", p.Name)
	case ddlDropIndex:
		exists, err := sqlitebridge.ObjectExists(app, "index", p.Name)
		return !exists, err

	case ddlCreateView:
		return sqlitebridge.ObjectExists(app, "view", p.Name)
	case ddlDropView:
		exists, err := sqlitebridge.ObjectExists(app, "view", p.Name)
		return !exists, err

	case ddlCreateTrigger:
		return sqlitebridge.ObjectExists(app, "trigger", p.Name)
	case ddlDropTrigger:
		exists, err := sqlitebridge.ObjectExists(app, "trigger", p.Name)
		return !exists, err

	case ddlCreateVirtualTable, ddlDropVirtualTable:
		// Virtual tables register in sqlite_master as type 'table'.
		exists, err := sqlitebridge.ObjectExists(app, "table", p.Name)
		if p.Kind == ddlDropVirtualTable {
			return !exists, err
		}
		return exists, err
	}
	return false, nil
}

// synthTrigger is one rewritten cascade trigger: a deterministic name
// in the `_syzy_fkcascade_*` namespace plus the full CREATE TRIGGER SQL
// that emulates the cascade action against the child table.
type synthTrigger struct {
	Name string
	SQL  string
}

// synthesizeCascadeTriggers builds CREATE TRIGGER SQL for every
// cascade-style action declared on fks. Each FK with ON DELETE
// CASCADE/SET NULL/SET DEFAULT yields one BEFORE DELETE trigger; an FK
// with ON UPDATE CASCADE yields one BEFORE UPDATE trigger keyed on the
// referenced (parent) columns. Plain NO ACTION / RESTRICT FKs produce
// no trigger.
//
// childCols are the columns declared on the table being created; they
// supply the SQL DEFAULT for SET DEFAULT actions when the FK column
// has one.
func synthesizeCascadeTriggers(childTable string, childCols []parsedColumn, fks []parsedFK) []synthTrigger {
	var out []synthTrigger
	for i, fk := range fks {
		if t, ok := renderDeleteTrigger(childTable, i, fk, childCols); ok {
			out = append(out, t)
		}
		if t, ok := renderUpdateTrigger(childTable, i, fk); ok {
			out = append(out, t)
		}
	}
	return out
}

// CascadeTriggerName returns the deterministic name for the synth
// trigger emulating an FK's cascade action. suffix is "d" for the
// ON DELETE trigger and "u" for the ON UPDATE trigger.
func cascadeTriggerName(childTable string, idx int, suffix string) string {
	return fmt.Sprintf("_syzy_fkcascade_%s_%d_%s", childTable, idx, suffix)
}

func renderDeleteTrigger(childTab string, idx int, fk parsedFK, childCols []parsedColumn) (synthTrigger, bool) {
	body, ok := deleteActionBody(childTab, fk, childCols)
	if !ok {
		return synthTrigger{}, false
	}
	name := cascadeTriggerName(childTab, idx, "d")
	// IF NOT EXISTS makes the structural apply idempotent: the
	// originator's helper connection installs the trigger up front,
	// then the broker's catch-up loop re-applies the same op when it
	// catches up to this seq. Both runs land cleanly.
	sql := fmt.Sprintf(
		"CREATE TRIGGER IF NOT EXISTS %s BEFORE DELETE ON %s FOR EACH ROW BEGIN %s; END",
		sqlitebridge.QuoteIdent(name),
		sqlitebridge.QuoteIdent(fk.RefTable),
		body,
	)
	return synthTrigger{Name: name, SQL: sql}, true
}

func renderUpdateTrigger(childTab string, idx int, fk parsedFK) (synthTrigger, bool) {
	if fk.OnUpdate != fkCascade {
		// SET NULL / SET DEFAULT on UPDATE are uncommon; SQLite allows
		// them but the engine doesn't synthesize them yet. Fold in
		// later if a user reports a real need.
		return synthTrigger{}, false
	}
	if len(fk.RefCols) == 0 {
		return synthTrigger{}, false
	}
	var ofCols, set, where strings.Builder
	for i, c := range fk.RefCols {
		if i > 0 {
			ofCols.WriteString(", ")
			set.WriteString(", ")
			where.WriteString(" AND ")
		}
		ofCols.WriteString(sqlitebridge.QuoteIdent(c))
		fmt.Fprintf(&set, "%s = new.%s",
			sqlitebridge.QuoteIdent(fk.Cols[i]),
			sqlitebridge.QuoteIdent(c))
		fmt.Fprintf(&where, "%s = old.%s",
			sqlitebridge.QuoteIdent(fk.Cols[i]),
			sqlitebridge.QuoteIdent(c))
	}
	name := cascadeTriggerName(childTab, idx, "u")
	body := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		sqlitebridge.QuoteIdent(childTab),
		set.String(), where.String())
	sql := fmt.Sprintf(
		"CREATE TRIGGER IF NOT EXISTS %s BEFORE UPDATE OF %s ON %s FOR EACH ROW BEGIN %s; END",
		sqlitebridge.QuoteIdent(name),
		ofCols.String(),
		sqlitebridge.QuoteIdent(fk.RefTable),
		body,
	)
	return synthTrigger{Name: name, SQL: sql}, true
}

// deleteActionBody returns the SQL statement (without trailing
// semicolon) the BEFORE DELETE trigger should run; ok=false when the
// FK has no ON DELETE action that requires a trigger.
func deleteActionBody(childTab string, fk parsedFK, childCols []parsedColumn) (string, bool) {
	if !isCascadeAction(fk.OnDelete) {
		return "", false
	}
	if len(fk.RefCols) == 0 {
		// Cascade requires knowing the parent-side column names so the
		// WHERE clause can match against `old.<refcol>`. Fall back to
		// the FK's own column names assuming parent.col matches by
		// position; this matches SQLite's behavior when the parent's
		// PK is used implicitly.
		fk.RefCols = fk.Cols
	}
	where := matchClauseAgainstOld(fk.Cols, fk.RefCols)
	switch fk.OnDelete {
	case fkCascade:
		return fmt.Sprintf("DELETE FROM %s WHERE %s",
			sqlitebridge.QuoteIdent(childTab), where), true
	case fkSetNull:
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			sqlitebridge.QuoteIdent(childTab),
			nullAssign(fk.Cols), where), true
	case fkSetDefault:
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			sqlitebridge.QuoteIdent(childTab),
			defaultAssign(fk.Cols, childCols), where), true
	}
	return "", false
}

func matchClauseAgainstOld(childCols, refCols []string) string {
	var b strings.Builder
	for i, c := range childCols {
		if i > 0 {
			b.WriteString(" AND ")
		}
		fmt.Fprintf(&b, "%s = old.%s",
			sqlitebridge.QuoteIdent(c),
			sqlitebridge.QuoteIdent(refCols[i]))
	}
	return b.String()
}

func nullAssign(cols []string) string {
	var b strings.Builder
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s = NULL", sqlitebridge.QuoteIdent(c))
	}
	return b.String()
}

// defaultAssign builds `col = <default>` pairs for SET DEFAULT,
// looking up each FK column's declared default in childCols. Columns
// without a declared default fall through to NULL — matching SQLite's
// own SET DEFAULT semantics.
func defaultAssign(cols []string, childCols []parsedColumn) string {
	defaults := make(map[string]string, len(childCols))
	for _, c := range childCols {
		defaults[c.Name] = c.Default
	}
	var b strings.Builder
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		expr := defaults[c]
		if expr == "" {
			expr = "NULL"
		}
		fmt.Fprintf(&b, "%s = %s", sqlitebridge.QuoteIdent(c), expr)
	}
	return b.String()
}
