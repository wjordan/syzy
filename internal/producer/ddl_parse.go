// DDL classifier + AST → parsedDDL projection, backed by rqlite/sql.

package producer

import (
	"errors"
	"fmt"
	"strings"

	rsql "github.com/rqlite/sql"
)

// ErrUnsupportedDDL is returned by classifyDDL when the SQL matches a
// rejected DDL form (CREATE TABLE AS SELECT, etc.).
var ErrUnsupportedDDL = errors.New("producer: unsupported DDL")

type ddlKind int

const (
	ddlNone ddlKind = iota
	ddlCreateTable
	ddlAlterTableAddColumn
	ddlAlterTableRenameTo
	ddlAlterTableRenameColumn
	ddlAlterTableDropColumn
	ddlDropTable
	ddlCreateIndex
	ddlCreateUniqueIndex
	ddlDropIndex
	ddlCreateView
	ddlDropView
	ddlCreateVirtualTable
	ddlDropVirtualTable
	ddlCreateTrigger
	ddlDropTrigger
	ddlBeginOrSavepoint
)

type fkAction int

const (
	fkNone fkAction = iota
	fkNoAction
	fkRestrict
	fkCascade
	fkSetNull
	fkSetDefault
)

type parsedFK struct {
	Cols     []string
	RefTable string
	RefCols  []string
	OnDelete fkAction
	OnUpdate fkAction
}

func (f parsedFK) HasCascade() bool {
	return isCascadeAction(f.OnDelete) || isCascadeAction(f.OnUpdate)
}

func isCascadeAction(a fkAction) bool {
	return a == fkCascade || a == fkSetNull || a == fkSetDefault
}

// parsedDDL is the producer-internal projection of a SQLite DDL statement.
// Field meaning depends on Kind; see the case dispatch in buildCatalogOp.
type parsedDDL struct {
	Kind ddlKind

	// SavepointRollback marks a `ROLLBACK TO <savepoint>`.
	SavepointRollback bool

	// Unparsable marks a Kind=ddlNone the parser could not read at all
	// (as opposed to one it read and classified as non-replicated).
	// SQLite accepts syntax rqlite/sql does not, so such a statement may
	// still execute; admission checks that need to fail closed use this
	// to tell "verified harmless" from "unverifiable".
	Unparsable bool

	// Most kinds: target table/index/view/trigger name. ALTER TABLE
	// RENAME TO / RENAME COLUMN: pre-rename name.
	Name string

	// CreateTable, AlterTableAddColumn: column declarations.
	Columns []parsedColumn
	// CreateTable: PK column names in declared order.
	PKColumns []string
	// CreateTable: trailing WITHOUT ROWID.
	WithoutRowid bool
	// CreateTable: 1+ unique-key descriptors (column-level or table-level).
	UniqueKeys [][]string
	// CreateTable / AlterTableAddColumn: FK clauses (column-level and
	// table-level merged), used by the cascade-trigger synthesizer.
	FKs []parsedFK
	// AlterTableRenameTo: new table name.
	NewName string
	// AlterTableRenameColumn: old + new column names.
	OldColumn string
	NewColumn string
	// AlterTableDropColumn: column name.
	DropColumn string

	// IF EXISTS / IF NOT EXISTS were specified.
	IfExists    bool
	IfNotExists bool

	// BeginOrSavepoint: true when the statement is SAVEPOINT rather
	// than BEGIN. Admission tracks savepoint scopes because DDL inside
	// one has partial-rollback semantics the schema chain can't model.
	IsSavepoint bool

	// CreateIndex/CreateUniqueIndex: indexed table + column list.
	IndexTable     string
	IndexColumns   []string
	HasWhereClause bool
	// CreateUniqueIndex: the partial index WHERE expression (nil when
	// HasWhereClause is false). Compiled to a crdt.UniquePredicate in
	// buildCatalogOp, where the catalog resolves column IDs.
	WherePred rsql.Expr

	// Verbatim raw SQL of the statement — used for syzy_schema_event.raw_sql.
	RawSQL string
}

// parsedColumn is one column declaration inside a CREATE TABLE or
// ALTER TABLE ADD COLUMN.
type parsedColumn struct {
	Name          string
	Type          string // declared type (incl. parameters like VARCHAR(255)); "" for typeless
	NotNull       bool
	IsPK          bool   // column-level PRIMARY KEY constraint
	IsUnique      bool   // column-level UNIQUE constraint
	Default       string // SQL default expression text (with parens if user wrote them); "" for none
	Generated     bool   // GENERATED ALWAYS AS — receivers recompute
	AutoIncrement bool
	Collation     string // column-level COLLATE name (e.g. "NOCASE"); "" = BINARY
	FK            *parsedFK
	// PKDesc records column-level `PRIMARY KEY DESC`. SQLite does not
	// alias the rowid when DESC is present, so the rowid-alias rewrite
	// must skip such columns. rqlite/sql does not surface DESC on its
	// column-level PrimaryKeyConstraint node, so this is detected by a
	// short post-scan of the source text (see scanForDescAfterPK).
	PKDesc bool
	// HasUnsupportedRewrite is set when the column carries a clause the
	// rowid-alias splice cannot losslessly preserve — CHECK, COLLATE,
	// named CONSTRAINT, or REFERENCES. The preprocessor declines to
	// rewrite such columns; admission's backstop rejects them.
	HasUnsupportedRewrite bool
	// SrcStart / SrcEnd bracket the column declaration in the source
	// text. SrcStart = column name token offset; SrcEnd = offset of the
	// terminating ',' or ')'. Used by ddl_rewrite.go to splice the PK
	// column without re-rendering the rest of the table.
	SrcStart, SrcEnd int
}

// classifyDDL parses sql with rqlite/sql and projects the result into a
// parsedDDL. Returns Kind=ddlNone if the statement is DML (or anything
// not a recognized DDL form), Kind=ddlBeginOrSavepoint for BEGIN /
// SAVEPOINT (caller uses this to decide whether to admit nested DDL), or
// an error for explicitly rejected DDL shapes.
func classifyDDL(sql string) (parsedDDL, error) {
	// SQLite tolerates a leading UTF-8 BOM as whitespace; rqlite/sql's
	// scanner does not, so strip it before parsing.
	const utf8BOM = "\xEF\xBB\xBF"
	sql = strings.TrimPrefix(sql, utf8BOM)
	parser := rsql.NewParser(strings.NewReader(sql))
	stmt, err := parser.ParseStatement()
	if err != nil {
		// Parser failure. Pass through as non-DDL; SQLite will produce
		// its own diagnostic when it tries to compile the statement — or
		// accept syntax this parser does not cover, hence Unparsable.
		return parsedDDL{Kind: ddlNone, Unparsable: true, RawSQL: sql}, nil
	}

	switch s := stmt.(type) {
	case *rsql.BeginStatement:
		return parsedDDL{Kind: ddlBeginOrSavepoint, RawSQL: sql}, nil
	case *rsql.SavepointStatement:
		return parsedDDL{Kind: ddlBeginOrSavepoint, IsSavepoint: true, RawSQL: sql}, nil
	case *rsql.RollbackStatement:
		// ROLLBACK TO <savepoint> undoes row changes SQLite already
		// reported through the preupdate hook; the touch journal keeps
		// those records. Flagged, not admitted — see savepointRollback.
		return parsedDDL{Kind: ddlNone, SavepointRollback: s.SavepointName != nil, RawSQL: sql}, nil

	case *rsql.CreateTableStatement:
		return projectCreateTable(s, sql)

	case *rsql.AlterTableStatement:
		return projectAlterTable(s, sql)

	case *rsql.DropTableStatement:
		return parsedDDL{
			Kind:     ddlDropTable,
			Name:     identName(s.Name),
			IfExists: s.IfExists.IsValid(),
			RawSQL:   sql,
		}, nil

	case *rsql.CreateIndexStatement:
		return projectCreateIndex(s, sql)

	case *rsql.DropIndexStatement:
		return parsedDDL{
			Kind:     ddlDropIndex,
			Name:     identName(s.Name),
			IfExists: s.IfExists.IsValid(),
			RawSQL:   sql,
		}, nil

	case *rsql.CreateViewStatement:
		return parsedDDL{
			Kind:        ddlCreateView,
			Name:        identName(s.Name),
			IfNotExists: s.IfNotExists.IsValid(),
			RawSQL:      sql,
		}, nil

	case *rsql.DropViewStatement:
		return parsedDDL{
			Kind:     ddlDropView,
			Name:     identName(s.Name),
			IfExists: s.IfExists.IsValid(),
			RawSQL:   sql,
		}, nil

	case *rsql.CreateVirtualTableStatement:
		return parsedDDL{
			Kind:        ddlCreateVirtualTable,
			Name:        identName(s.Name),
			IfNotExists: s.IfNotExists.IsValid(),
			RawSQL:      sql,
		}, nil

	case *rsql.CreateTriggerStatement:
		return parsedDDL{
			Kind:        ddlCreateTrigger,
			Name:        identName(s.Name),
			IfNotExists: s.IfNotExists.IsValid(),
			RawSQL:      sql,
		}, nil

	case *rsql.DropTriggerStatement:
		return parsedDDL{
			Kind:     ddlDropTrigger,
			Name:     identName(s.Name),
			IfExists: s.IfExists.IsValid(),
			RawSQL:   sql,
		}, nil
	}

	return parsedDDL{Kind: ddlNone}, nil
}

func identName(id *rsql.Ident) string {
	if id == nil {
		return ""
	}
	return id.Name
}

// ----- CREATE TABLE projection -----

func projectCreateTable(s *rsql.CreateTableStatement, sql string) (parsedDDL, error) {
	if s.Select != nil {
		return parsedDDL{}, fmt.Errorf("%w: CREATE TABLE AS SELECT", ErrUnsupportedDDL)
	}
	d := parsedDDL{
		Kind:         ddlCreateTable,
		Name:         identName(s.Name),
		IfNotExists:  s.IfNotExists.IsValid(),
		WithoutRowid: s.Without.IsValid() && s.Rowid.IsValid(),
		RawSQL:       sql,
	}

	for _, col := range s.Columns {
		pc, err := projectColumn(col, sql)
		if err != nil {
			return parsedDDL{}, err
		}
		pc.SrcStart = col.Name.NamePos.Offset
		pc.SrcEnd = findColumnTerminator(sql, pc.SrcStart)
		d.Columns = append(d.Columns, pc)
		if pc.IsPK {
			d.PKColumns = append(d.PKColumns, pc.Name)
		}
		if pc.IsUnique {
			d.UniqueKeys = append(d.UniqueKeys, []string{pc.Name})
		}
		if pc.FK != nil {
			d.FKs = append(d.FKs, *pc.FK)
		}
	}

	// Table-level constraints
	for _, tc := range s.Constraints {
		switch c := tc.(type) {
		case *rsql.PrimaryKeyConstraint:
			for _, col := range c.Columns {
				d.PKColumns = append(d.PKColumns, identName(col))
			}
		case *rsql.UniqueConstraint:
			cols := indexedColumnNames(c.Columns)
			d.UniqueKeys = append(d.UniqueKeys, cols)
		case *rsql.ForeignKeyConstraint:
			fk := projectFKConstraint(c, identsToNames(c.Columns))
			d.FKs = append(d.FKs, fk)
		case *rsql.CheckConstraint:
			// SQLite enforces table CHECK constraints locally.
		}
	}
	return d, nil
}

func projectColumn(col *rsql.ColumnDefinition, sql string) (parsedColumn, error) {
	pc := parsedColumn{Name: identName(col.Name)}
	if col.Type != nil {
		pc.Type = typeText(sql, col.Type)
	}

	for _, c := range col.Constraints {
		// A named CONSTRAINT clause on the column blocks the rowid-alias
		// splice — the rewriter can't preserve it losslessly inside the
		// replacement template. (See ddl_rewrite.go:shouldRewriteRowidAlias.)
		if hasConstraintName(c) {
			pc.HasUnsupportedRewrite = true
		}

		switch x := c.(type) {
		case *rsql.PrimaryKeyConstraint:
			pc.IsPK = true
			if x.Autoincrement.IsValid() {
				pc.AutoIncrement = true
			}
			// rqlite/sql does not record DESC on a column-level PK;
			// detect by short scan of the source.
			if scanForDescAfterPK(sql, x.Key.Offset) {
				pc.PKDesc = true
			}
		case *rsql.NotNullConstraint:
			pc.NotNull = true
		case *rsql.UniqueConstraint:
			pc.IsUnique = true
		case *rsql.DefaultConstraint:
			pc.Default = defaultExprText(sql, x)
		case *rsql.GeneratedConstraint:
			pc.Generated = true
		case *rsql.CollateConstraint:
			pc.HasUnsupportedRewrite = true
			if x.Collation != nil {
				pc.Collation = identName(x.Collation)
			}
		case *rsql.CheckConstraint:
			pc.HasUnsupportedRewrite = true
		case *rsql.ForeignKeyConstraint:
			fk := projectFKConstraint(x, []string{pc.Name})
			pc.FK = &fk
			pc.HasUnsupportedRewrite = true
		}
	}
	return pc, nil
}

// hasConstraintName reports whether a column-level constraint declares
// an explicit `CONSTRAINT <name>` prefix.
func hasConstraintName(c rsql.Constraint) bool {
	switch x := c.(type) {
	case *rsql.PrimaryKeyConstraint:
		return x.Constraint.IsValid()
	case *rsql.NotNullConstraint:
		return x.Constraint.IsValid()
	case *rsql.UniqueConstraint:
		return x.Constraint.IsValid()
	case *rsql.DefaultConstraint:
		return x.Constraint.IsValid()
	case *rsql.GeneratedConstraint:
		return x.Constraint.IsValid()
	case *rsql.CollateConstraint:
		return x.Constraint.IsValid()
	case *rsql.CheckConstraint:
		return x.Constraint.IsValid()
	case *rsql.ForeignKeyConstraint:
		return x.Constraint.IsValid()
	}
	return false
}

// typeText returns the column type text as it appeared in the source,
// preserving the original whitespace inside parenthesized arg lists
// (e.g., "DECIMAL(10, 2)" not "DECIMAL(10,2)") and multi-word forms
// (e.g., "CHARACTER VARYING(20)").
func typeText(sql string, t *rsql.Type) string {
	if t == nil || t.Name == nil {
		return ""
	}
	start := t.Name.NamePos.Offset
	if t.Rparen.IsValid() {
		end := t.Rparen.Offset + 1
		if start >= 0 && end <= len(sql) && start < end {
			return sql[start:end]
		}
	}
	return t.Name.Name
}

// defaultExprText returns the textual form of a DEFAULT clause's
// expression as it appeared in the source. For `DEFAULT (uuidv7())` it
// returns "(uuidv7())" (parens preserved); for `DEFAULT 5` it returns
// "5". When the constraint is parenthesized we slice the raw SQL
// between Lparen and Rparen to keep exact whitespace; otherwise we
// fall back to the AST's String() rendering.
func defaultExprText(sql string, c *rsql.DefaultConstraint) string {
	if c.Lparen.IsValid() && c.Rparen.IsValid() {
		start := c.Lparen.Offset
		end := c.Rparen.Offset + 1
		if start >= 0 && end <= len(sql) && start < end {
			return sql[start:end]
		}
	}
	if c.Expr != nil {
		return c.Expr.String()
	}
	return ""
}

// projectFKConstraint maps an rsql.ForeignKeyConstraint into a parsedFK.
// childCols is the list of columns this FK is declared against — for a
// column-level FK that's a single-element slice with the column name;
// for a table-level FK, it's the columns inside `FOREIGN KEY (a, b)`.
func projectFKConstraint(c *rsql.ForeignKeyConstraint, childCols []string) parsedFK {
	fk := parsedFK{
		Cols:     childCols,
		RefTable: identName(c.ForeignTable),
		RefCols:  identsToNames(c.ForeignColumns),
	}
	for _, a := range c.Args {
		act := fkActionOf(a)
		switch {
		case a.OnDelete.IsValid():
			fk.OnDelete = act
		case a.OnUpdate.IsValid():
			fk.OnUpdate = act
		}
	}
	return fk
}

func fkActionOf(a *rsql.ForeignKeyArg) fkAction {
	switch {
	case a.Cascade.IsValid():
		return fkCascade
	case a.SetNull.IsValid():
		return fkSetNull
	case a.SetDefault.IsValid():
		return fkSetDefault
	case a.Restrict.IsValid():
		return fkRestrict
	case a.NoAction.IsValid():
		return fkNoAction
	}
	return fkNone
}

func identsToNames(ids []*rsql.Ident) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = identName(id)
	}
	return out
}

// indexedColumnNames extracts column names from a list of rsql.IndexedColumn.
// For UNIQUE/PRIMARY KEY columns the expression is normally a bare ident;
// expression-based indexes fall back to the rendered form (used only for
// IndexColumns, where exact name fidelity is not required).
func indexedColumnNames(cols []*rsql.IndexedColumn) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if id, ok := c.X.(*rsql.Ident); ok {
			out = append(out, id.Name)
			continue
		}
		if c.X != nil {
			out = append(out, c.X.String())
		}
	}
	return out
}

// ----- ALTER TABLE projection -----

func projectAlterTable(s *rsql.AlterTableStatement, sql string) (parsedDDL, error) {
	d := parsedDDL{Name: identName(s.Name), RawSQL: sql}
	switch {
	case s.NewName != nil:
		d.Kind = ddlAlterTableRenameTo
		d.NewName = identName(s.NewName)
		return d, nil

	case s.ColumnName != nil && s.NewColumnName != nil:
		d.Kind = ddlAlterTableRenameColumn
		d.OldColumn = identName(s.ColumnName)
		d.NewColumn = identName(s.NewColumnName)
		return d, nil

	case s.ColumnDef != nil:
		d.Kind = ddlAlterTableAddColumn
		pc, err := projectColumn(s.ColumnDef, sql)
		if err != nil {
			return parsedDDL{}, err
		}
		// ADD COLUMN doesn't need source spans — there's only one column.
		d.Columns = []parsedColumn{pc}
		if pc.FK != nil {
			d.FKs = append(d.FKs, *pc.FK)
		}
		return d, nil

	case s.DropColumnName != nil:
		d.Kind = ddlAlterTableDropColumn
		d.DropColumn = identName(s.DropColumnName)
		return d, nil
	}
	return parsedDDL{}, fmt.Errorf("%w: ALTER TABLE form not supported", ErrUnsupportedDDL)
}

// ----- CREATE INDEX projection -----

func projectCreateIndex(s *rsql.CreateIndexStatement, sql string) (parsedDDL, error) {
	d := parsedDDL{
		Name:           identName(s.Name),
		IfNotExists:    s.IfNotExists.IsValid(),
		IndexTable:     identName(s.Table),
		IndexColumns:   indexedColumnNames(s.Columns),
		HasWhereClause: s.Where.IsValid(),
		RawSQL:         sql,
	}
	if s.Unique.IsValid() {
		d.Kind = ddlCreateUniqueIndex
		// A partial unique index is admissible only in coordinated mode;
		// carry the predicate AST for buildCatalogOp to compile and
		// reject (eventual partial / unsupported grammar) there, where the
		// catalog is available to resolve column IDs and affinities.
		if d.HasWhereClause {
			d.WherePred = s.WhereExpr
		}
	} else {
		d.Kind = ddlCreateIndex
	}
	return d, nil
}

// ----- Source-text helpers -----

// findColumnTerminator returns the byte offset of the ',' or ')' that
// terminates the column declaration starting at colStart. Tracks paren
// depth and respects SQL string literals, identifier quoting (", [, `),
// and line / block comments. Returns -1 if no terminator is found
// (caller treats this as an unrecoverable malformed input).
func findColumnTerminator(sql string, colStart int) int {
	if colStart < 0 || colStart >= len(sql) {
		return -1
	}
	depth := 0
	i := colStart
	for i < len(sql) {
		c := sql[i]
		switch c {
		case '\'':
			i = skipSQLString(sql, i)
			continue
		case '"':
			i = skipDoubleQuoted(sql, i)
			continue
		case '`':
			i = skipBacktickQuoted(sql, i)
			continue
		case '[':
			i = skipBracketQuoted(sql, i)
			continue
		case '-':
			if i+1 < len(sql) && sql[i+1] == '-' {
				i = skipLineComment(sql, i)
				continue
			}
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				i = skipBlockComment(sql, i)
				continue
			}
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return i
			}
			depth--
		case ',':
			if depth == 0 {
				return i
			}
		}
		i++
	}
	return -1
}

// scanForDescAfterPK returns true if a `DESC` keyword appears as the
// next significant token after the `KEY` of a column-level PRIMARY KEY
// constraint. keyOffset is the byte offset of the KEY keyword.
func scanForDescAfterPK(sql string, keyOffset int) bool {
	// Skip past `KEY` itself (3 chars), then any whitespace / comments.
	i := keyOffset + 3
	for i < len(sql) {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			i = skipLineComment(sql, i)
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			i = skipBlockComment(sql, i)
		default:
			// Look for "DESC" as the next token. Match case-insensitive,
			// must be followed by a non-identifier character (or end).
			if i+4 <= len(sql) && strings.EqualFold(sql[i:i+4], "DESC") {
				if i+4 == len(sql) || !isIdentByte(sql[i+4]) {
					return true
				}
			}
			return false
		}
	}
	return false
}

func isIdentByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// --- Low-level lexical skippers used by both findColumnTerminator and
// scanForDescAfterPK. All take an offset pointing at the opening
// delimiter and return the offset one past the closing delimiter.

func skipSQLString(sql string, i int) int {
	i++ // past opening '
	for i < len(sql) {
		if sql[i] == '\'' {
			if i+1 < len(sql) && sql[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

func skipDoubleQuoted(sql string, i int) int {
	i++ // past opening "
	for i < len(sql) {
		if sql[i] == '"' {
			if i+1 < len(sql) && sql[i+1] == '"' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

func skipBacktickQuoted(sql string, i int) int {
	i++
	for i < len(sql) && sql[i] != '`' {
		i++
	}
	if i < len(sql) {
		i++
	}
	return i
}

func skipBracketQuoted(sql string, i int) int {
	i++
	for i < len(sql) && sql[i] != ']' {
		i++
	}
	if i < len(sql) {
		i++
	}
	return i
}

func skipLineComment(sql string, i int) int {
	i += 2 // past --
	for i < len(sql) && sql[i] != '\n' {
		i++
	}
	if i < len(sql) {
		i++ // consume newline
	}
	return i
}

func skipBlockComment(sql string, i int) int {
	i += 2 // past /*
	for i+1 < len(sql) {
		if sql[i] == '*' && sql[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return len(sql)
}
