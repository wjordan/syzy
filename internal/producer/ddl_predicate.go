// Compilation of a partial unique index's WHERE clause into a
// crdt.UniquePredicate at DDL admission. The compiled predicate is keyed
// by ColumnID (rename-stable) and restricted to a deterministic,
// collation-independent grammar so the writer's Go-side reserve gate and
// SQLite's own partial index agree on participation byte-for-byte.
//
// Grammar:
//   - <col> IS NULL / IS NOT NULL              (any column type)
//   - <col> <op> <literal>                     (op ∈ = <> < <= > >=)
//   - <col> [NOT] IN (<literal>…)
//   - AND / OR / NOT / parentheses of the above
//
// A comparison literal's class must match the column's affinity (numeric↔
// numeric, text↔text) so no affinity coercion happens. Text comparisons
// carry the column's collation (BINARY/NOCASE/RTRIM), captured in the
// catalog at admission and baked into the predicate, so the Go-side reserve
// gate, the rebuild enumerate (explicit COLLATE), and SQLite's own partial
// index agree. Blob literals are not supported; a custom-collation column
// is already rejected at table creation.

package producer

import (
	"fmt"
	"strconv"
	"strings"

	rsql "github.com/rqlite/sql"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// affinity is a column's SQLite type affinity, derived from its declared
// type per the rules in the SQLite docs ("Determination Of Column
// Affinity").
type affinity uint8

const (
	affBlob affinity = iota // BLOB (also the empty declared type)
	affText
	affNumeric
	affInteger
	affReal
)

func (a affinity) numeric() bool { return a == affNumeric || a == affInteger || a == affReal }

// affinityOf classifies a declared type string. Mirrors SQLite's substring
// rules, applied in priority order.
func affinityOf(declType string) affinity {
	t := strings.ToUpper(declType)
	switch {
	case strings.Contains(t, "INT"):
		return affInteger
	case strings.Contains(t, "CHAR"), strings.Contains(t, "CLOB"), strings.Contains(t, "TEXT"):
		return affText
	case t == "" || strings.Contains(t, "BLOB"):
		return affBlob
	case strings.Contains(t, "REAL"), strings.Contains(t, "FLOA"), strings.Contains(t, "DOUB"):
		return affReal
	default:
		return affNumeric
	}
}

type predColInfo struct {
	id   crdt.ColumnID
	aff  affinity
	coll crdt.Collation
}

// predCtx carries the resolved columns and error label for partial-unique
// predicate compilation.
type predCtx struct {
	cols  map[string]predColInfo
	label string
}

// compilePartialPredicate translates a CREATE UNIQUE INDEX … WHERE clause
// into a crdt.UniquePredicate, validating every referenced column is
// active, non-generated, and used within the supported grammar.
func compilePartialPredicate(where rsql.Expr, app *sqlitebridge.Conn, tab *catalog.Table) (crdt.UniquePredicate, error) {
	cols, err := predicateColumns(app, tab)
	if err != nil {
		return crdt.UniquePredicate{}, err
	}
	root, err := compilePredExpr(where, predCtx{cols: cols, label: "partial UNIQUE predicate"})
	if err != nil {
		return crdt.UniquePredicate{}, err
	}
	return crdt.UniquePredicate{Root: root}, nil
}

// predicateColumns resolves name → (ColumnID, affinity) for tab's active,
// non-generated columns. Generated and hidden columns are omitted, so a
// predicate referencing one fails resolution with a clear error.
func predicateColumns(app *sqlitebridge.Conn, tab *catalog.Table) (map[string]predColInfo, error) {
	type catCol struct {
		id   crdt.ColumnID
		coll crdt.Collation
	}
	byName := make(map[string]catCol, len(tab.Columns))
	for _, c := range tab.Columns {
		byName[c.Name] = catCol{id: c.ID, coll: c.Collation}
	}
	stmt, _, err := app.Prepare(`SELECT name, type, hidden FROM pragma_table_xinfo(?)`)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, tab.Name); err != nil {
		return nil, err
	}
	out := map[string]predColInfo{}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		name := stmt.ColumnText(0)
		hidden := stmt.ColumnInt64(2)
		cc, ok := byName[name]
		if !ok || hidden != 0 {
			// Not a replicated catalog column (generated/hidden, or
			// dropped): leave it out so a predicate referencing it errors.
			continue
		}
		out[name] = predColInfo{id: cc.id, aff: affinityOf(stmt.ColumnText(1)), coll: cc.coll}
	}
	return out, nil
}

func compilePredExpr(e rsql.Expr, ctx predCtx) (*crdt.PredExpr, error) {
	switch x := e.(type) {
	case *rsql.ParenExpr:
		return compilePredExpr(x.X, ctx)
	case *rsql.Null: // <col> IS NULL / <col> NOT NULL
		ci, err := resolveCol(x.X, ctx)
		if err != nil {
			return nil, err
		}
		op := crdt.PredIsNull
		if x.Op == rsql.NOTNULL {
			op = crdt.PredIsNotNull
		}
		return &crdt.PredExpr{Op: op, Col: ci.id}, nil
	case *rsql.UnaryExpr:
		if x.Op != rsql.NOT {
			return nil, fmt.Errorf("%s: unsupported unary operator %q", ctx.label, x.Op.String())
		}
		kid, err := compilePredExpr(x.X, ctx)
		if err != nil {
			return nil, err
		}
		return &crdt.PredExpr{Op: crdt.PredNot, Kids: []*crdt.PredExpr{kid}}, nil
	case *rsql.BinaryExpr:
		return compileBinary(x, ctx)
	default:
		return nil, fmt.Errorf("%s: unsupported expression %T (supported: IS NULL, numeric comparisons, IN, AND/OR/NOT)", ctx.label, e)
	}
}

func compileBinary(x *rsql.BinaryExpr, ctx predCtx) (*crdt.PredExpr, error) {
	switch x.Op {
	case rsql.AND, rsql.OR:
		l, err := compilePredExpr(x.X, ctx)
		if err != nil {
			return nil, err
		}
		r, err := compilePredExpr(x.Y, ctx)
		if err != nil {
			return nil, err
		}
		op := crdt.PredAnd
		if x.Op == rsql.OR {
			op = crdt.PredOr
		}
		return &crdt.PredExpr{Op: op, Kids: []*crdt.PredExpr{l, r}}, nil
	case rsql.EQ, rsql.NE, rsql.LT, rsql.LE, rsql.GT, rsql.GE:
		return compileCompare(x, ctx)
	case rsql.IN, rsql.NOTIN:
		return compileIn(x, ctx)
	default:
		return nil, fmt.Errorf("%s: unsupported operator %q", ctx.label, x.Op.String())
	}
}

// compileCompare handles <col> <op> <lit> and the mirrored <lit> <op>
// <col> (flipping the operator). The literal's class must match the
// column's affinity (numeric↔numeric, text↔text) so no affinity coercion
// occurs; a text comparison carries the column's collation.
func compileCompare(x *rsql.BinaryExpr, ctx predCtx) (*crdt.PredExpr, error) {
	op := cmpOp(x.Op)
	ci, lit, flipped, err := splitColLit(x.X, x.Y, ctx)
	if err != nil {
		return nil, err
	}
	if flipped {
		op = flipCmp(op)
	}
	val, coll, err := compileLiteral(lit, ci, ctx)
	if err != nil {
		return nil, err
	}
	return &crdt.PredExpr{Op: op, Col: ci.id, Lits: []crdt.ColValue{val}, Coll: coll}, nil
}

func compileIn(x *rsql.BinaryExpr, ctx predCtx) (*crdt.PredExpr, error) {
	ci, err := resolveCol(x.X, ctx)
	if err != nil {
		return nil, err
	}
	list, ok := x.Y.(*rsql.ExprList)
	if !ok {
		return nil, fmt.Errorf("%s: IN requires a parenthesized literal list", ctx.label)
	}
	lits := make([]crdt.ColValue, 0, len(list.Exprs))
	var coll crdt.Collation
	for _, el := range list.Exprs {
		val, c, err := compileLiteral(el, ci, ctx)
		if err != nil {
			return nil, err
		}
		coll = c
		lits = append(lits, val)
	}
	op := crdt.PredIn
	if x.Op == rsql.NOTIN {
		op = crdt.PredNotIn
	}
	return &crdt.PredExpr{Op: op, Col: ci.id, Lits: lits, Coll: coll}, nil
}

// compileLiteral converts a literal expression to a ColValue whose class
// matches the column's affinity, returning the collation to compare under
// (the column's, for text; BINARY otherwise). It rejects class mismatches
// (which SQLite would resolve by an affinity coercion this evaluator does
// not model) and unsupported literal forms.
func compileLiteral(e rsql.Expr, ci predColInfo, ctx predCtx) (crdt.ColValue, crdt.Collation, error) {
	if p, ok := e.(*rsql.ParenExpr); ok {
		return compileLiteral(p.X, ci, ctx)
	}
	if s, ok := e.(*rsql.StringLit); ok {
		if ci.aff != affText {
			return crdt.ColValue{}, crdt.CollBinary, fmt.Errorf("%s: text literal compared to a non-text column", ctx.label)
		}
		return crdt.ColValue{TypeTag: crdt.ColText, Bytes: []byte(s.Value)}, ci.coll, nil
	}
	// Numeric literal (incl. signed and TRUE/FALSE).
	if !ci.aff.numeric() {
		return crdt.ColValue{}, crdt.CollBinary, fmt.Errorf("%s: numeric literal compared to a non-numeric column (a text column needs a text literal)", ctx.label)
	}
	val, err := numericLiteral(e, ctx.label)
	return val, crdt.CollBinary, err
}

// splitColLit identifies which side of a comparison is the column and
// which is the literal. flipped is true when the literal was on the left.
func splitColLit(a, b rsql.Expr, ctx predCtx) (ci predColInfo, lit rsql.Expr, flipped bool, err error) {
	if ci, err = resolveCol(a, ctx); err == nil {
		return ci, b, false, nil
	}
	if ci, err = resolveCol(b, ctx); err == nil {
		return ci, a, true, nil
	}
	return predColInfo{}, nil, false, fmt.Errorf("%s: comparison must reference a table column", ctx.label)
}

func resolveCol(e rsql.Expr, ctx predCtx) (predColInfo, error) {
	if p, ok := e.(*rsql.ParenExpr); ok {
		return resolveCol(p.X, ctx)
	}
	id, ok := e.(*rsql.Ident)
	if !ok {
		return predColInfo{}, fmt.Errorf("%s: expected a column name, got %T", ctx.label, e)
	}
	ci, ok := ctx.cols[id.Name]
	if !ok {
		return predColInfo{}, fmt.Errorf("%s: column %q is not a replicated, non-generated column", ctx.label, id.Name)
	}
	return ci, nil
}

// numericLiteral converts a numeric literal expression (NumberLit, a unary
// ± NumberLit, or BoolLit) into a ColValue. Text/blob/NULL literals are
// rejected.
func numericLiteral(e rsql.Expr, label string) (crdt.ColValue, error) {
	switch lit := e.(type) {
	case *rsql.ParenExpr:
		return numericLiteral(lit.X, label)
	case *rsql.NumberLit:
		return parseNumber(lit.Value, false, label)
	case *rsql.UnaryExpr:
		if lit.Op == rsql.MINUS || lit.Op == rsql.PLUS {
			n, ok := lit.X.(*rsql.NumberLit)
			if !ok {
				return crdt.ColValue{}, fmt.Errorf("%s: unsupported literal %T", label, lit.X)
			}
			return parseNumber(n.Value, lit.Op == rsql.MINUS, label)
		}
		return crdt.ColValue{}, fmt.Errorf("%s: unsupported unary literal %q", label, lit.Op.String())
	case *rsql.BoolLit:
		v := int64(0)
		if lit.Value {
			v = 1
		}
		return crdt.IntColValue(v), nil
	case *rsql.NullLit:
		return crdt.ColValue{}, fmt.Errorf("%s: compare to NULL is not allowed; use IS NULL / IS NOT NULL", label)
	default:
		return crdt.ColValue{}, fmt.Errorf("%s: unsupported literal %T", label, e)
	}
}

func parseNumber(s string, negate bool, label string) (crdt.ColValue, error) {
	if i, err := strconv.ParseInt(s, 0, 64); err == nil {
		if negate {
			i = -i
		}
		return crdt.IntColValue(i), nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return crdt.ColValue{}, fmt.Errorf("%s: bad numeric literal %q", label, s)
	}
	if negate {
		f = -f
	}
	return crdt.RealColValue(f), nil
}

func cmpOp(t rsql.Token) crdt.PredOp {
	switch t {
	case rsql.EQ:
		return crdt.PredEq
	case rsql.NE:
		return crdt.PredNe
	case rsql.LT:
		return crdt.PredLt
	case rsql.LE:
		return crdt.PredLe
	case rsql.GT:
		return crdt.PredGt
	case rsql.GE:
		return crdt.PredGe
	}
	return 0
}

func flipCmp(op crdt.PredOp) crdt.PredOp {
	switch op {
	case crdt.PredLt:
		return crdt.PredGt
	case crdt.PredLe:
		return crdt.PredGe
	case crdt.PredGt:
		return crdt.PredLt
	case crdt.PredGe:
		return crdt.PredLe
	}
	return op // Eq / Ne are symmetric
}
