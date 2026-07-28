package catalog

import "strings"

// PKDefaultKind tags how a column's DEFAULT expression is recognized by
// syzy. Only the strict forms `uuidv7()` and `gen_id('<table>')` are
// classified; anything else (including `gen_id()` with no argument)
// stays opaque user-supplied text.
type PKDefaultKind int

const (
	// PKDefaultNone covers the no-DEFAULT case and any DEFAULT we don't
	// recognize. Callers should treat the default as opaque app SQL.
	PKDefaultNone PKDefaultKind = iota
	// PKDefaultUUIDv7 is the strict form `uuidv7()`.
	PKDefaultUUIDv7
	// PKDefaultGenID is the strict form `gen_id('<table>')` whose literal
	// argument is captured in PKDefault.Arg.
	PKDefaultGenID
	// PKDefaultGenIDBare is `gen_id()` with no arg. Not callable today
	// (the SQL function rejects it) but recognized so future DDL rewrite
	// can fill in the table name without changing the parser surface.
	PKDefaultGenIDBare
)

// PKDefault is the parsed form of a column DEFAULT expression. Arg is
// non-empty only for PKDefaultGenID.
type PKDefault struct {
	Kind PKDefaultKind
	Arg  string
}

// ClassifyPKDefault recognizes the strict default forms `uuidv7()` and
// `gen_id('<table>')` (and the future-only `gen_id()`). Anything else
// returns PKDefaultNone — including malformed gen_id calls, double-quoted
// args, and args with embedded escapes. Whitespace and case in the
// function name are tolerated.
//
// Inputs come from two paths: the producer's DDL parser (`pc.Default`,
// which retains the surrounding parens written by the user) and SQLite's
// `pragma_table_xinfo.dflt_value` (which strips the outer parens). We
// accept both.
func ClassifyPKDefault(text string) PKDefault {
	s := strings.TrimSpace(text)
	for strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	if s == "" {
		return PKDefault{}
	}

	// Find the function name; everything up to the first '(' lowercased.
	open := strings.Index(s, "(")
	if open < 0 {
		return PKDefault{}
	}
	name := strings.ToLower(strings.TrimSpace(s[:open]))
	args := strings.TrimSpace(s[open+1:])
	if !strings.HasSuffix(args, ")") {
		return PKDefault{}
	}
	args = strings.TrimSpace(args[:len(args)-1])

	switch name {
	case "uuidv7":
		if args != "" {
			return PKDefault{}
		}
		return PKDefault{Kind: PKDefaultUUIDv7}
	case "gen_id":
		if args == "" {
			return PKDefault{Kind: PKDefaultGenIDBare}
		}
		// Strict literal: single-quoted, no embedded quotes or escapes.
		if len(args) < 2 || args[0] != '\'' || args[len(args)-1] != '\'' {
			return PKDefault{}
		}
		inner := args[1 : len(args)-1]
		if strings.ContainsAny(inner, "'\\") {
			return PKDefault{}
		}
		return PKDefault{Kind: PKDefaultGenID, Arg: inner}
	}
	return PKDefault{}
}
