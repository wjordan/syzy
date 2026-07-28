package crdt

import (
	"math"
	"reflect"
	"testing"
)

func cid(b byte) ColumnID { return ColumnID{15: b} }

func colInt(v int64) ColValue {
	b := make([]byte, 8)
	u := uint64(v)
	for i := 7; i >= 0; i-- {
		b[i] = byte(u)
		u >>= 8
	}
	return ColValue{TypeTag: ColInt, Bytes: b}
}

func colReal(v float64) ColValue { return colRealBits(math.Float64bits(v)) }
func colRealBits(bits uint64) ColValue {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(bits)
		bits >>= 8
	}
	return ColValue{TypeTag: ColReal, Bytes: b}
}
func colText(s string) ColValue { return ColValue{TypeTag: ColText, Bytes: []byte(s)} }
func colNull() ColValue         { return ColValue{TypeTag: ColNull} }

// img builds a column lookup from an id->value map, returning ColNull for
// any column the caller didn't set (matching the reserve path's contract).
func img(m map[byte]ColValue) func(ColumnID) ColValue {
	return func(id ColumnID) ColValue {
		if v, ok := m[id[15]]; ok {
			return v
		}
		return colNull()
	}
}

func leaf(op PredOp, col byte) *PredExpr { return &PredExpr{Op: op, Col: cid(col)} }
func cmp(op PredOp, col byte, lit ColValue) *PredExpr {
	return &PredExpr{Op: op, Col: cid(col), Lits: []ColValue{lit}}
}
func cmpColl(op PredOp, col byte, lit ColValue, coll Collation) *PredExpr {
	return &PredExpr{Op: op, Col: cid(col), Lits: []ColValue{lit}, Coll: coll}
}

func TestCollationCompareText(t *testing.T) {
	tests := []struct {
		coll Collation
		a, b string
		want int
	}{
		{CollBinary, "abc", "abc", 0},
		{CollBinary, "ABC", "abc", -1}, // 'A'(65) < 'a'(97)
		{CollNocase, "ABC", "abc", 0},
		{CollNocase, "Active", "active", 0},
		{CollNocase, "abc", "abd", -1},
		{CollRtrim, "ab  ", "ab", 0},
		{CollRtrim, "ab ", "ab", 0},
		{CollRtrim, "ab", "ac", -1},
	}
	for _, tc := range tests {
		if got := tc.coll.CompareText([]byte(tc.a), []byte(tc.b)); sign(got) != tc.want {
			t.Errorf("%s.CompareText(%q,%q) = %d, want sign %d", tc.coll.Name(), tc.a, tc.b, got, tc.want)
		}
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

func TestPredicateEvalCollation(t *testing.T) {
	status := byte(2)
	tests := []struct {
		name string
		root *PredExpr
		row  map[byte]ColValue
		want bool
	}{
		{"NOCASE eq folds case", cmpColl(PredEq, status, colText("active"), CollNocase), map[byte]ColValue{status: colText("ACTIVE")}, true},
		{"BINARY eq case-sensitive", cmp(PredEq, status, colText("active")), map[byte]ColValue{status: colText("ACTIVE")}, false},
		{"RTRIM eq ignores trailing space", cmpColl(PredEq, status, colText("ok"), CollRtrim), map[byte]ColValue{status: colText("ok   ")}, true},
		{
			"NOCASE IN",
			&PredExpr{Op: PredIn, Col: cid(status), Lits: []ColValue{colText("a"), colText("b")}, Coll: CollNocase},
			map[byte]ColValue{status: colText("B")},
			true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (UniquePredicate{Root: tc.root}).Eval(img(tc.row)); got != tc.want {
				t.Fatalf("Eval = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPredicateSQLCollate(t *testing.T) {
	q := func(id ColumnID) string { return `"status"` }
	got, err := (UniquePredicate{Root: cmpColl(PredEq, 2, colText("active"), CollNocase)}).SQL(q)
	if err != nil {
		t.Fatal(err)
	}
	want := `("status" COLLATE NOCASE = 'active')`
	if got != want {
		t.Fatalf("SQL = %q, want %q", got, want)
	}
}

func TestPredicateEval(t *testing.T) {
	delAt := byte(1)
	status := byte(2)
	qty := byte(3)

	tests := []struct {
		name string
		root *PredExpr
		row  map[byte]ColValue
		want bool
	}{
		{"nil predicate participates", nil, nil, true},
		{"IS NULL true", leaf(PredIsNull, delAt), map[byte]ColValue{delAt: colNull()}, true},
		{"IS NULL false", leaf(PredIsNull, delAt), map[byte]ColValue{delAt: colInt(123)}, false},
		{"IS NOT NULL true", leaf(PredIsNotNull, delAt), map[byte]ColValue{delAt: colInt(1)}, true},
		{"text eq", cmp(PredEq, status, colText("active")), map[byte]ColValue{status: colText("active")}, true},
		{"text eq miss", cmp(PredEq, status, colText("active")), map[byte]ColValue{status: colText("gone")}, false},
		{"int ge", cmp(PredGe, qty, colInt(10)), map[byte]ColValue{qty: colInt(10)}, true},
		{"int lt", cmp(PredLt, qty, colInt(10)), map[byte]ColValue{qty: colInt(10)}, false},
		{
			"AND both true",
			&PredExpr{Op: PredAnd, Kids: []*PredExpr{leaf(PredIsNull, delAt), cmp(PredEq, status, colText("active"))}},
			map[byte]ColValue{delAt: colNull(), status: colText("active")},
			true,
		},
		{
			"AND one false",
			&PredExpr{Op: PredAnd, Kids: []*PredExpr{leaf(PredIsNull, delAt), cmp(PredEq, status, colText("active"))}},
			map[byte]ColValue{delAt: colInt(5), status: colText("active")},
			false,
		},
		{
			"OR one true",
			&PredExpr{Op: PredOr, Kids: []*PredExpr{leaf(PredIsNull, delAt), cmp(PredEq, status, colText("active"))}},
			map[byte]ColValue{delAt: colInt(5), status: colText("active")},
			true,
		},
		{"NOT inverts", &PredExpr{Op: PredNot, Kids: []*PredExpr{leaf(PredIsNull, delAt)}}, map[byte]ColValue{delAt: colInt(5)}, true},
		// 3VL: comparison against a NULL column cell is UNKNOWN, which is
		// not TRUE → row does not participate (matches SQLite WHERE).
		{"cmp null col is unknown→false", cmp(PredEq, qty, colInt(5)), map[byte]ColValue{qty: colNull()}, false},
		{
			"NOT unknown stays unknown→false",
			&PredExpr{Op: PredNot, Kids: []*PredExpr{cmp(PredEq, qty, colInt(5))}},
			map[byte]ColValue{qty: colNull()},
			false,
		},
		{
			"IN match",
			&PredExpr{Op: PredIn, Col: cid(status), Lits: []ColValue{colText("a"), colText("b")}},
			map[byte]ColValue{status: colText("b")},
			true,
		},
		{
			"NOT IN miss→true",
			&PredExpr{Op: PredNotIn, Col: cid(status), Lits: []ColValue{colText("a"), colText("b")}},
			map[byte]ColValue{status: colText("c")},
			true,
		},
		{"real compare", cmp(PredGt, qty, colReal(1.5)), map[byte]ColValue{qty: colReal(2.0)}, true},
		{"int vs real promote", cmp(PredGt, qty, colReal(1.5)), map[byte]ColValue{qty: colInt(2)}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := UniquePredicate{Root: tc.root}
			if got := p.Eval(img(tc.row)); got != tc.want {
				t.Fatalf("Eval = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPredicateSQL(t *testing.T) {
	q := func(id ColumnID) string {
		switch id[15] {
		case 1:
			return `"deleted_at"`
		case 2:
			return `"status"`
		}
		return `"?"`
	}
	root := &PredExpr{Op: PredAnd, Kids: []*PredExpr{
		leaf(PredIsNull, 1),
		cmp(PredEq, 2, colText("act've")),
	}}
	got, err := UniquePredicate{Root: root}.SQL(q)
	if err != nil {
		t.Fatal(err)
	}
	want := `(("deleted_at" IS NULL) AND ("status" = 'act''ve'))`
	if got != want {
		t.Fatalf("SQL = %q, want %q", got, want)
	}
}

func TestPredicateCodecRoundTrip(t *testing.T) {
	roots := []*PredExpr{
		nil,
		leaf(PredIsNull, 1),
		cmp(PredEq, 2, colText("active")),
		cmp(PredGe, 3, colInt(-42)),
		cmp(PredLt, 3, colReal(3.14)),
		cmpColl(PredEq, 2, colText("Active"), CollNocase),
		{Op: PredIn, Col: cid(2), Lits: []ColValue{colText("a"), colText("b"), colNull()}, Coll: CollRtrim},
		{Op: PredAnd, Kids: []*PredExpr{
			leaf(PredIsNotNull, 1),
			{Op: PredOr, Kids: []*PredExpr{cmp(PredEq, 2, colText("x")), {Op: PredNot, Kids: []*PredExpr{leaf(PredIsNull, 3)}}}},
		}},
	}
	for i, root := range roots {
		in := UniquePredicate{Root: root}
		buf := appendPredicate(nil, in)
		out, rest, err := readPredicate(buf)
		if err != nil {
			t.Fatalf("case %d: decode: %v", i, err)
		}
		if len(rest) != 0 {
			t.Fatalf("case %d: %d trailing bytes", i, len(rest))
		}
		if !reflect.DeepEqual(in, out) {
			t.Fatalf("case %d: round-trip mismatch\n in=%+v\nout=%+v", i, in, out)
		}
	}
}

func TestPredicateColumns(t *testing.T) {
	root := &PredExpr{Op: PredAnd, Kids: []*PredExpr{
		leaf(PredIsNull, 1),
		cmp(PredEq, 2, colText("x")),
		leaf(PredIsNotNull, 1), // dup
	}}
	got := UniquePredicate{Root: root}.Columns()
	want := []ColumnID{cid(1), cid(2)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns = %v, want %v", got, want)
	}
}
