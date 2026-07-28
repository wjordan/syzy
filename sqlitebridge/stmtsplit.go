package sqlitebridge

/*
#include <stdlib.h>
#include "syzy_sqlite.h"
*/
import "C"

import "unsafe"

// Complete reports whether sql ends with a complete SQL statement, per
// sqlite3_complete's lexical rules (a statement is complete at a
// semicolon that is not inside a string, comment, or trigger
// BEGIN...END body).
func Complete(sql string) bool {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	return C.sx_complete(csql) != 0
}

// FirstStatement returns the first complete SQL statement in sql
// (including its terminating ';') and the byte offset where the
// remainder begins. When sql holds no top-level ';' — the common
// single-statement case — the whole input is returned with
// consumed == len(sql).
//
// Candidate ';' positions come from a quote/comment-aware scan; each
// candidate prefix is verified with sqlite3_complete, which is what
// keeps a ';' inside a trigger body from splitting the statement. Each
// Complete call copies its prefix, so a statement with many interior
// ';' candidates (a trigger body) costs O(len²) in that statement's
// length — fine for the DDL-sized inputs this serves; don't point it
// at megabyte trigger bodies.
//
// The lexical skippers below deliberately parallel the ones in
// internal/producer/ddl_parse.go (which scans inside a single
// statement for column spans); they answer different questions over
// the same token grammar, and producer's are private to its parser.
func FirstStatement(sql string) (stmt string, consumed int) {
	for i := 0; i < len(sql); {
		switch sql[i] {
		case '\'', '"', '`', '[':
			i = skipQuoted(sql, i)
		case '-':
			if i+1 < len(sql) && sql[i+1] == '-' {
				i = skipToLineEnd(sql, i+2)
				continue
			}
			i++
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				i = skipPastBlockComment(sql, i+2)
				continue
			}
			i++
		case ';':
			if Complete(sql[:i+1]) {
				return sql[:i+1], i + 1
			}
			i++
		default:
			i++
		}
	}
	return sql, len(sql)
}

// skipQuoted advances past a quoted region opened at i. Single and
// double quotes escape by doubling; backtick and bracket quoting do
// not. Returns len(sql) on an unterminated quote.
func skipQuoted(sql string, i int) int {
	open := sql[i]
	close := open
	doubling := open == '\'' || open == '"'
	if open == '[' {
		close = ']'
		doubling = false
	}
	i++
	for i < len(sql) {
		if sql[i] != close {
			i++
			continue
		}
		if doubling && i+1 < len(sql) && sql[i+1] == close {
			i += 2
			continue
		}
		return i + 1
	}
	return i
}

func skipToLineEnd(sql string, i int) int {
	for i < len(sql) && sql[i] != '\n' {
		i++
	}
	return i
}

func skipPastBlockComment(sql string, i int) int {
	for i+1 < len(sql) {
		if sql[i] == '*' && sql[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return len(sql)
}
