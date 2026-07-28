package catalog

import "testing"

func TestClassifyPKDefault(t *testing.T) {
	cases := []struct {
		in   string
		want PKDefault
	}{
		// uuidv7
		{"uuidv7()", PKDefault{Kind: PKDefaultUUIDv7}},
		{"(uuidv7())", PKDefault{Kind: PKDefaultUUIDv7}},
		{"  uuidv7 ( ) ", PKDefault{Kind: PKDefaultUUIDv7}},
		{"UUIDV7()", PKDefault{Kind: PKDefaultUUIDv7}},
		{"uuidv7('x')", PKDefault{}}, // unexpected arg

		// gen_id strict
		{"gen_id('t')", PKDefault{Kind: PKDefaultGenID, Arg: "t"}},
		{"(gen_id('t'))", PKDefault{Kind: PKDefaultGenID, Arg: "t"}},
		{"  GEN_ID ( 'doc' ) ", PKDefault{Kind: PKDefaultGenID, Arg: "doc"}},
		{"gen_id('table_with_underscore')", PKDefault{Kind: PKDefaultGenID, Arg: "table_with_underscore"}},

		// gen_id bare (future-only, classified but rejected at admission)
		{"gen_id()", PKDefault{Kind: PKDefaultGenIDBare}},
		{"  gen_id (  )  ", PKDefault{Kind: PKDefaultGenIDBare}},

		// Malformed / opaque
		{"gen_id(\"t\")", PKDefault{}}, // double-quoted arg not accepted
		{"gen_id('t', 'x')", PKDefault{}},
		{"gen_id('t\\'s')", PKDefault{}}, // backslash escape
		{"gen_id('it''s')", PKDefault{}}, // embedded single quote (doubled)
		{"random()", PKDefault{}},
		{"42", PKDefault{}},
		{"", PKDefault{}},
		{"  ", PKDefault{}},
		{"((uuidv7()))", PKDefault{Kind: PKDefaultUUIDv7}},
		{"uuidv7", PKDefault{}}, // missing parens
	}
	for _, c := range cases {
		got := ClassifyPKDefault(c.in)
		if got != c.want {
			t.Errorf("ClassifyPKDefault(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
}
