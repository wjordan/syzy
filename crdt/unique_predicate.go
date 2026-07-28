package crdt

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// UniquePredicate is the compiled WHERE clause of a partial unique index
// (`CREATE UNIQUE INDEX … WHERE <predicate>`). It is keyed by ColumnID,
// not column name, so it survives column renames, and supports a
// restricted, deterministic grammar (boolean combinations of NULL tests
// and column-vs-literal comparisons) that both:
//
//   - evaluates against a row image at the writer's reserve path
//     (UniquePredicate.Eval), deciding whether a row participates in the
//     coordinated reservation, and
//   - renders back to SQL against current column names at the
//     leaseholder's rebuild path (UniquePredicate.SQL), used as a WHERE
//     filter when reconstructing the taken-set.
//
// The grammar is restricted so the Go-side Eval and SQLite's own partial
// index agree on participation byte-for-byte: admission rejects anything
// outside it, anything collation-dependent, and any literal whose storage
// class would force an affinity coercion. A nil/zero UniquePredicate means
// "not partial" — every row participates.
type UniquePredicate struct {
	Root *PredExpr
}

// PredOp tags a node in a UniquePredicate tree. Encoded values are stable.
type PredOp uint8

const (
	PredIsNull    PredOp = 1 // <col> IS NULL
	PredIsNotNull PredOp = 2 // <col> IS NOT NULL
	PredEq        PredOp = 3 // <col> =  <lit>
	PredNe        PredOp = 4 // <col> <> <lit>
	PredLt        PredOp = 5 // <col> <  <lit>
	PredLe        PredOp = 6 // <col> <= <lit>
	PredGt        PredOp = 7 // <col> >  <lit>
	PredGe        PredOp = 8 // <col> >= <lit>
	PredIn        PredOp = 9 // <col> IN (<lit>, …)
	PredNotIn     PredOp = 10
	PredAnd       PredOp = 11 // all Kids
	PredOr        PredOp = 12 // any Kids
	PredNot       PredOp = 13 // single Kid
)

// PredExpr is one node of a UniquePredicate. The meaningful fields depend
// on Op: leaf NULL tests use Col; comparisons use Col+Lits[0]; IN/NOT IN
// use Col+Lits; AND/OR use Kids (n≥1); NOT uses Kids[0].
type PredExpr struct {
	Op   PredOp
	Col  ColumnID
	Lits []ColValue
	Kids []*PredExpr
	// Coll is the collating sequence for a text comparison/IN node
	// (CollBinary for numeric comparisons and the structural ops). It is
	// baked in at admission from the column's declared collation so Eval
	// is self-contained and the rebuild path can emit an explicit COLLATE.
	Coll Collation
}

const predMaxNodes = 256 // decode guard against malformed / hostile input

// tri is three-valued-logic: SQL's TRUE / FALSE / UNKNOWN (NULL).
type tri uint8

const (
	triFalse tri = iota
	triTrue
	triUnknown
)

// Eval reports whether a row participates in the partial index: the
// predicate evaluates to TRUE (UNKNOWN/NULL counts as not-participating,
// matching SQLite WHERE semantics). lookup returns the row image's value
// for a column; it must return a ColNull ColValue for an absent column.
func (p UniquePredicate) Eval(lookup func(ColumnID) ColValue) bool {
	if p.Root == nil {
		return true
	}
	return p.Root.eval(lookup) == triTrue
}

func (e *PredExpr) eval(lookup func(ColumnID) ColValue) tri {
	switch e.Op {
	case PredAnd:
		out := triTrue
		for _, k := range e.Kids {
			switch k.eval(lookup) {
			case triFalse:
				return triFalse
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case PredOr:
		out := triFalse
		for _, k := range e.Kids {
			switch k.eval(lookup) {
			case triTrue:
				return triTrue
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case PredNot:
		switch e.Kids[0].eval(lookup) {
		case triTrue:
			return triFalse
		case triFalse:
			return triTrue
		default:
			return triUnknown
		}
	case PredIsNull:
		return boolTri(lookup(e.Col).TypeTag == ColNull)
	case PredIsNotNull:
		return boolTri(lookup(e.Col).TypeTag != ColNull)
	case PredEq, PredNe, PredLt, PredLe, PredGt, PredGe:
		return evalCmp(e.Op, lookup(e.Col), e.Lits[0], e.Coll)
	case PredIn, PredNotIn:
		return evalIn(e.Op, lookup(e.Col), e.Lits, e.Coll)
	default:
		return triUnknown
	}
}

func boolTri(b bool) tri {
	if b {
		return triTrue
	}
	return triFalse
}

func evalCmp(op PredOp, col, lit ColValue, coll Collation) tri {
	if col.TypeTag == ColNull || lit.TypeTag == ColNull {
		return triUnknown
	}
	c := compareValues(col, lit, coll)
	switch op {
	case PredEq:
		return boolTri(c == 0)
	case PredNe:
		return boolTri(c != 0)
	case PredLt:
		return boolTri(c < 0)
	case PredLe:
		return boolTri(c <= 0)
	case PredGt:
		return boolTri(c > 0)
	case PredGe:
		return boolTri(c >= 0)
	}
	return triUnknown
}

// evalIn implements `col IN (lits)` / `col NOT IN (lits)` with SQL's
// NULL semantics: a NULL probe is UNKNOWN; otherwise a match is TRUE
// (IN) and a no-match in the presence of a NULL list element is UNKNOWN.
func evalIn(op PredOp, col ColValue, lits []ColValue, coll Collation) tri {
	if col.TypeTag == ColNull {
		return triUnknown
	}
	sawNull := false
	for i := range lits {
		if lits[i].TypeTag == ColNull {
			sawNull = true
			continue
		}
		if compareValues(col, lits[i], coll) == 0 {
			return boolTri(op == PredIn)
		}
	}
	if sawNull {
		return triUnknown
	}
	return boolTri(op == PredNotIn)
}

// compareValues orders two non-NULL values by SQLite's storage-class
// rule: INTEGER/REAL (numeric) < TEXT < BLOB, numeric compared by value,
// TEXT under the given collation, BLOB by memcmp (collation never applies
// to blobs). Admission forbids cross-class comparisons that would trigger
// an affinity coercion, so in practice col and lit share a class.
func compareValues(a, b ColValue, coll Collation) int {
	ra, rb := classRank(a.TypeTag), classRank(b.TypeTag)
	if ra < rb {
		return -1
	}
	if ra > rb {
		return 1
	}
	switch a.TypeTag {
	case ColInt, ColReal:
		return compareNumeric(a, b)
	case ColText:
		return coll.CompareText(a.Bytes, b.Bytes)
	default: // ColBlob
		return bytes.Compare(a.Bytes, b.Bytes)
	}
}

func classRank(t ColType) int {
	switch t {
	case ColInt, ColReal:
		return 0
	case ColText:
		return 1
	default: // ColBlob
		return 2
	}
}

func compareNumeric(a, b ColValue) int {
	if a.TypeTag == ColInt && b.TypeTag == ColInt {
		x, y := decodeInt(a.Bytes), decodeInt(b.Bytes)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default:
			return 0
		}
	}
	x, y := asFloat(a), asFloat(b)
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

func asFloat(v ColValue) float64 {
	if v.TypeTag == ColReal {
		return math.Float64frombits(uint64(decodeInt(v.Bytes)))
	}
	return float64(decodeInt(v.Bytes))
}

func decodeInt(b []byte) int64 {
	var v uint64
	for _, c := range b {
		v = v<<8 | uint64(c)
	}
	return int64(v)
}

// Columns returns the distinct ColumnIDs the predicate references, in
// first-seen order. Used by admission (dependency validation) and the
// rebuild path.
func (p UniquePredicate) Columns() []ColumnID {
	if p.Root == nil {
		return nil
	}
	var out []ColumnID
	seen := map[ColumnID]struct{}{}
	var walk func(e *PredExpr)
	walk = func(e *PredExpr) {
		switch e.Op {
		case PredAnd, PredOr, PredNot:
			for _, k := range e.Kids {
				walk(k)
			}
		default:
			if _, dup := seen[e.Col]; !dup {
				seen[e.Col] = struct{}{}
				out = append(out, e.Col)
			}
		}
	}
	walk(p.Root)
	return out
}

// SQL renders the predicate as a parenthesized SQL boolean expression,
// resolving each ColumnID to a quoted identifier via quoteName. It is the
// inverse of admission's compile step and is stable under column renames
// (the tree holds IDs, quoteName supplies the current name). quoteName
// must return an already-quoted identifier.
func (p UniquePredicate) SQL(quoteName func(ColumnID) string) (string, error) {
	if p.Root == nil {
		return "", nil
	}
	var sb strings.Builder
	if err := p.Root.sql(&sb, quoteName); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (e *PredExpr) sql(sb *strings.Builder, q func(ColumnID) string) error {
	switch e.Op {
	case PredAnd, PredOr:
		sep := " AND "
		if e.Op == PredOr {
			sep = " OR "
		}
		sb.WriteByte('(')
		for i, k := range e.Kids {
			if i > 0 {
				sb.WriteString(sep)
			}
			if err := k.sql(sb, q); err != nil {
				return err
			}
		}
		sb.WriteByte(')')
	case PredNot:
		sb.WriteString("(NOT ")
		if err := e.Kids[0].sql(sb, q); err != nil {
			return err
		}
		sb.WriteByte(')')
	case PredIsNull:
		fmt.Fprintf(sb, "(%s IS NULL)", q(e.Col))
	case PredIsNotNull:
		fmt.Fprintf(sb, "(%s IS NOT NULL)", q(e.Col))
	case PredEq, PredNe, PredLt, PredLe, PredGt, PredGe:
		lit, err := literalSQL(e.Lits[0])
		if err != nil {
			return err
		}
		fmt.Fprintf(sb, "(%s%s %s %s)", q(e.Col), collateSQL(e.Coll), cmpOpSQL(e.Op), lit)
	case PredIn, PredNotIn:
		kw := "IN"
		if e.Op == PredNotIn {
			kw = "NOT IN"
		}
		fmt.Fprintf(sb, "(%s%s %s (", q(e.Col), collateSQL(e.Coll), kw)
		for i := range e.Lits {
			if i > 0 {
				sb.WriteString(", ")
			}
			lit, err := literalSQL(e.Lits[i])
			if err != nil {
				return err
			}
			sb.WriteString(lit)
		}
		sb.WriteString("))")
	default:
		return fmt.Errorf("crdt: unknown predicate op %d", e.Op)
	}
	return nil
}

// collateSQL renders an explicit ` COLLATE <name>` suffix for a non-binary
// collation, empty for binary. Emitting it on the column operand makes the
// rebuilt enumerate honor the predicate's collation regardless of the
// local table's declared collation.
func collateSQL(c Collation) string {
	if c == CollBinary {
		return ""
	}
	return " COLLATE " + c.Name()
}

func cmpOpSQL(op PredOp) string {
	switch op {
	case PredEq:
		return "="
	case PredNe:
		return "<>"
	case PredLt:
		return "<"
	case PredLe:
		return "<="
	case PredGt:
		return ">"
	case PredGe:
		return ">="
	}
	return "?"
}

// literalSQL renders a literal ColValue as a SQL literal. Text is single-
// quoted with embedded quotes doubled; blobs use x'..' hex syntax.
func literalSQL(v ColValue) (string, error) {
	switch v.TypeTag {
	case ColNull:
		return "NULL", nil
	case ColInt:
		return fmt.Sprintf("%d", decodeInt(v.Bytes)), nil
	case ColReal:
		return strconv.FormatFloat(math.Float64frombits(uint64(decodeInt(v.Bytes))), 'g', -1, 64), nil
	case ColText:
		return "'" + strings.ReplaceAll(string(v.Bytes), "'", "''") + "'", nil
	case ColBlob:
		return "x'" + hex.EncodeToString(v.Bytes) + "'", nil
	}
	return "", fmt.Errorf("crdt: unknown literal type %d", v.TypeTag)
}

// IntColValue builds an integer ColValue (8-byte big-endian
// two's-complement) — the canonical capture encoding. Used to construct
// predicate literals.
func IntColValue(v int64) ColValue {
	b := make([]byte, 8)
	u := uint64(v)
	for i := 7; i >= 0; i-- {
		b[i] = byte(u)
		u >>= 8
	}
	return ColValue{TypeTag: ColInt, Bytes: b}
}

// RealColValue builds a real ColValue (8-byte big-endian IEEE 754
// binary64) — the canonical capture encoding.
func RealColValue(f float64) ColValue {
	b := make([]byte, 8)
	bits := math.Float64bits(f)
	for i := 7; i >= 0; i-- {
		b[i] = byte(bits)
		bits >>= 8
	}
	return ColValue{TypeTag: ColReal, Bytes: b}
}

// EncodeUniquePredicate returns the canonical bytes for p (a nil Root
// encodes as a single present=0 byte). Used to persist a partial index's
// predicate in metadata; the catalog-op wire path uses appendPredicate
// directly.
func EncodeUniquePredicate(p UniquePredicate) []byte { return appendPredicate(nil, p) }

// DecodeUniquePredicate parses bytes produced by EncodeUniquePredicate and
// rejects trailing garbage.
func DecodeUniquePredicate(b []byte) (UniquePredicate, error) {
	p, rest, err := readPredicate(b)
	if err != nil {
		return UniquePredicate{}, err
	}
	if len(rest) != 0 {
		return UniquePredicate{}, fmt.Errorf("crdt: %d trailing bytes after predicate", len(rest))
	}
	return p, nil
}

// appendPredicate appends p's tree to buf. A nil Root encodes as a single
// zero byte (present=0), so a non-partial key round-trips to an empty
// predicate.
func appendPredicate(buf []byte, p UniquePredicate) []byte {
	if p.Root == nil {
		return append(buf, 0)
	}
	buf = append(buf, 1)
	return p.Root.encode(buf)
}

func (e *PredExpr) encode(buf []byte) []byte {
	buf = append(buf, byte(e.Op))
	switch e.Op {
	case PredAnd, PredOr:
		buf = binary.AppendUvarint(buf, uint64(len(e.Kids)))
		for _, k := range e.Kids {
			buf = k.encode(buf)
		}
	case PredNot:
		buf = e.Kids[0].encode(buf)
	case PredIsNull, PredIsNotNull:
		buf = append(buf, e.Col[:]...)
	case PredEq, PredNe, PredLt, PredLe, PredGt, PredGe:
		buf = append(buf, e.Col[:]...)
		buf = appendLiteral(buf, e.Lits[0])
		buf = append(buf, byte(e.Coll))
	case PredIn, PredNotIn:
		buf = append(buf, e.Col[:]...)
		buf = binary.AppendUvarint(buf, uint64(len(e.Lits)))
		for i := range e.Lits {
			buf = appendLiteral(buf, e.Lits[i])
		}
		buf = append(buf, byte(e.Coll))
	}
	return buf
}

func appendLiteral(buf []byte, v ColValue) []byte {
	buf = append(buf, byte(v.TypeTag))
	switch v.TypeTag {
	case ColInt, ColReal, ColText, ColBlob:
		buf = binary.AppendUvarint(buf, uint64(len(v.Bytes)))
		buf = append(buf, v.Bytes...)
	}
	return buf
}

// readPredicate decodes a predicate written by appendPredicate.
func readPredicate(buf []byte) (UniquePredicate, []byte, error) {
	if len(buf) < 1 {
		return UniquePredicate{}, nil, ErrShortBuffer
	}
	present := buf[0]
	buf = buf[1:]
	if present == 0 {
		return UniquePredicate{}, buf, nil
	}
	n := 0
	root, rest, err := decodePredExpr(buf, &n)
	if err != nil {
		return UniquePredicate{}, nil, err
	}
	return UniquePredicate{Root: root}, rest, nil
}

func decodePredExpr(buf []byte, n *int) (*PredExpr, []byte, error) {
	if *n++; *n > predMaxNodes {
		return nil, nil, fmt.Errorf("crdt: predicate exceeds %d nodes", predMaxNodes)
	}
	if len(buf) < 1 {
		return nil, nil, ErrShortBuffer
	}
	e := &PredExpr{Op: PredOp(buf[0])}
	buf = buf[1:]
	var err error
	switch e.Op {
	case PredAnd, PredOr:
		cnt, sz := binary.Uvarint(buf)
		if sz <= 0 || cnt == 0 {
			return nil, nil, ErrShortBuffer
		}
		buf = buf[sz:]
		e.Kids = make([]*PredExpr, 0, cnt)
		for range cnt {
			var k *PredExpr
			if k, buf, err = decodePredExpr(buf, n); err != nil {
				return nil, nil, err
			}
			e.Kids = append(e.Kids, k)
		}
	case PredNot:
		var k *PredExpr
		if k, buf, err = decodePredExpr(buf, n); err != nil {
			return nil, nil, err
		}
		e.Kids = []*PredExpr{k}
	case PredIsNull, PredIsNotNull:
		if buf, err = readBytes16(buf, e.Col[:]); err != nil {
			return nil, nil, err
		}
	case PredEq, PredNe, PredLt, PredLe, PredGt, PredGe:
		if buf, err = readBytes16(buf, e.Col[:]); err != nil {
			return nil, nil, err
		}
		var lit ColValue
		if lit, buf, err = readLiteral(buf); err != nil {
			return nil, nil, err
		}
		e.Lits = []ColValue{lit}
		if e.Coll, buf, err = readColl(buf); err != nil {
			return nil, nil, err
		}
	case PredIn, PredNotIn:
		if buf, err = readBytes16(buf, e.Col[:]); err != nil {
			return nil, nil, err
		}
		cnt, sz := binary.Uvarint(buf)
		if sz <= 0 {
			return nil, nil, ErrShortBuffer
		}
		buf = buf[sz:]
		e.Lits = make([]ColValue, 0, cnt)
		for range cnt {
			var lit ColValue
			if lit, buf, err = readLiteral(buf); err != nil {
				return nil, nil, err
			}
			e.Lits = append(e.Lits, lit)
		}
		if e.Coll, buf, err = readColl(buf); err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, fmt.Errorf("crdt: unknown predicate op %d", e.Op)
	}
	return e, buf, nil
}

func readColl(buf []byte) (Collation, []byte, error) {
	if len(buf) < 1 {
		return CollBinary, nil, ErrShortBuffer
	}
	return Collation(buf[0]), buf[1:], nil
}

func readLiteral(buf []byte) (ColValue, []byte, error) {
	if len(buf) < 1 {
		return ColValue{}, nil, ErrShortBuffer
	}
	v := ColValue{TypeTag: ColType(buf[0])}
	buf = buf[1:]
	switch v.TypeTag {
	case ColNull:
		return v, buf, nil
	case ColInt, ColReal, ColText, ColBlob:
		byteLen, sz := binary.Uvarint(buf)
		if sz <= 0 {
			return ColValue{}, nil, ErrShortBuffer
		}
		buf = buf[sz:]
		if uint64(len(buf)) < byteLen {
			return ColValue{}, nil, ErrShortBuffer
		}
		v.Bytes = append([]byte(nil), buf[:byteLen]...)
		return v, buf[byteLen:], nil
	}
	return ColValue{}, nil, fmt.Errorf("%w: %d", ErrUnknownColType, v.TypeTag)
}
