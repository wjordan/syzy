package sqlitebridge

/*
#include <stdlib.h>
#include "syzy_sqlite.h"

// SQLITE_TRANSIENT is defined as ((sqlite3_destructor_type)-1) — a cast that
// the cgo type checker won't surface as a usable Go value. Wrap in a function
// so each cgo translation unit gets its own static copy with internal linkage.
static sqlite3_destructor_type syzy_bind_transient(void) {
	return SQLITE_TRANSIENT;
}

// A non-NULL zero-length C string for binding empty TEXT/BLOB values. SQLite
// treats a NULL pointer as SQL NULL regardless of the length argument, so the
// empty case needs a real (but zero-length) buffer.
static const char *syzy_empty_text(void) {
	static const char z[1] = {0};
	return z;
}
*/
import "C"

import (
	"unsafe"
)

// sqliteTransient caches the SQLITE_TRANSIENT sentinel so Bind calls don't pay
// a cgo crossing per parameter just to fetch a constant.
var sqliteTransient = C.syzy_bind_transient()

// ColumnType matches SQLITE_INTEGER/FLOAT/TEXT/BLOB/NULL.
type ColumnType int

const (
	ColumnInt  ColumnType = C.SQLITE_INTEGER
	ColumnReal ColumnType = C.SQLITE_FLOAT
	ColumnText ColumnType = C.SQLITE_TEXT
	ColumnBlob ColumnType = C.SQLITE_BLOB
	ColumnNull ColumnType = C.SQLITE_NULL
)

// Stmt wraps a sqlite3_stmt*. Like Conn, Stmt is not safe for concurrent use.
type Stmt struct {
	stmt *C.sqlite3_stmt
	conn *Conn
}

// Prepare compiles a single SQL statement. Returns the compiled statement and
// the leftover tail (text past the first terminating semicolon). For
// multi-statement SQL, call Prepare in a loop, feeding each tail back in.
//
// If sql contains only whitespace or comments, both stmt and err are nil and
// tail is the empty string.
func (c *Conn) Prepare(sql string) (stmt *Stmt, tail string, err error) {
	if sql == "" {
		return nil, "", nil
	}
	sql, err = c.applyPreprocess(sql)
	if err != nil {
		return nil, "", err
	}
	// Keep prepare input in C memory. sqlite3_prepare_v3 returns ctail as a
	// pointer into this buffer, and keeping Go string storage in C pointer
	// locals has triggered GC "pointer to free object" failures under stress.
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var raw *C.sqlite3_stmt
	var ctail *C.char
	rc := C.sx_prepare_v3(c.db, csql, C.int(len(sql)+1), 0, &raw, &ctail)
	if rc != C.SQLITE_OK {
		return nil, "", newErrorFromDB(rc, c.db)
	}

	if ctail != nil {
		off := uintptr(unsafe.Pointer(ctail)) - uintptr(unsafe.Pointer(csql))
		if off > 0 {
			tail = sql[off:]
		}
	}

	if raw == nil {
		return nil, tail, nil
	}
	return &Stmt{stmt: raw, conn: c}, tail, nil
}

// Finalize releases the statement. Safe to call multiple times.
func (s *Stmt) Finalize() error {
	if s.stmt == nil {
		return nil
	}
	rc := C.sx_finalize(s.stmt)
	s.stmt = nil
	if rc != C.SQLITE_OK {
		return newErrorFromCode(rc)
	}
	return nil
}

// Reset returns the statement to its initial pre-Step state. Bindings are
// preserved; call ClearBindings to also reset them.
func (s *Stmt) Reset() error {
	rc := C.sx_reset(s.stmt)
	if rc != C.SQLITE_OK {
		return newErrorFromDB(rc, s.conn.db)
	}
	return nil
}

// ClearBindings resets all bound parameters to NULL.
func (s *Stmt) ClearBindings() error {
	rc := C.sx_clear_bindings(s.stmt)
	if rc != C.SQLITE_OK {
		return newErrorFromDB(rc, s.conn.db)
	}
	return nil
}

// Step advances the statement.
//   - hasRow=true,  err=nil  → SQLITE_ROW; read columns via Column*.
//   - hasRow=false, err=nil  → SQLITE_DONE; statement finished cleanly.
//   - err non-nil            → SQLite error or SQLITE_INTERRUPT.
func (s *Stmt) Step() (hasRow bool, err error) {
	rc := C.sx_step(s.stmt)
	switch rc {
	case C.SQLITE_ROW:
		return true, nil
	case C.SQLITE_DONE:
		return false, nil
	default:
		return false, s.conn.refineCommitHookError(newErrorFromDB(rc, s.conn.db))
	}
}

// BindParamCount returns the number of parameter placeholders (?, ?n, :n, $n)
// in the prepared statement.
func (s *Stmt) BindParamCount() int {
	return int(C.sx_bind_parameter_count(s.stmt))
}

// BindNull binds NULL to the i-th parameter (1-based).
func (s *Stmt) BindNull(i int) error {
	rc := C.sx_bind_null(s.stmt, C.int(i))
	if rc != C.SQLITE_OK {
		return newErrorFromDB(rc, s.conn.db)
	}
	return nil
}

// BindInt64 binds a 64-bit integer.
func (s *Stmt) BindInt64(i int, v int64) error {
	rc := C.sx_bind_int64(s.stmt, C.int(i), C.sqlite3_int64(v))
	if rc != C.SQLITE_OK {
		return newErrorFromDB(rc, s.conn.db)
	}
	return nil
}

// BindFloat64 binds an IEEE-754 double.
func (s *Stmt) BindFloat64(i int, v float64) error {
	rc := C.sx_bind_double(s.stmt, C.int(i), C.double(v))
	if rc != C.SQLITE_OK {
		return newErrorFromDB(rc, s.conn.db)
	}
	return nil
}

// BindText binds a UTF-8 string. The empty string binds as TEXT ” (not NULL);
// use BindNull to bind a SQL NULL.
func (s *Stmt) BindText(i int, v string) error {
	n := C.int(len(v))
	var p *C.char
	if len(v) == 0 {
		p = C.syzy_empty_text()
	} else {
		p = (*C.char)(unsafe.Pointer(unsafe.StringData(v)))
	}
	rc := C.sx_bind_text(s.stmt, C.int(i), p, n, sqliteTransient)
	if rc != C.SQLITE_OK {
		return newErrorFromDB(rc, s.conn.db)
	}
	return nil
}

// BindBlob binds raw bytes. A nil/empty slice binds as a zero-length BLOB
// (x”), not NULL — sqlite3_bind_blob with a NULL pointer binds SQL NULL
// regardless of length, so the empty case routes through a non-NULL sentinel.
// Use BindNull to bind a SQL NULL.
func (s *Stmt) BindBlob(i int, v []byte) error {
	n := C.int(len(v))
	var p unsafe.Pointer
	if len(v) == 0 {
		p = unsafe.Pointer(C.syzy_empty_text())
	} else {
		p = unsafe.Pointer(&v[0])
	}
	rc := C.sx_bind_blob(s.stmt, C.int(i), p, n, sqliteTransient)
	if rc != C.SQLITE_OK {
		return newErrorFromDB(rc, s.conn.db)
	}
	return nil
}

// ColumnCount returns the number of result columns produced by the statement.
func (s *Stmt) ColumnCount() int {
	return int(C.sx_column_count(s.stmt))
}

// ColumnName returns the name of the i-th result column (0-based).
func (s *Stmt) ColumnName(i int) string {
	return C.GoString(C.sx_column_name(s.stmt, C.int(i)))
}

// ColumnType returns the dynamic type of the i-th result column in the
// current row (0-based).
func (s *Stmt) ColumnType(i int) ColumnType {
	return ColumnType(C.sx_column_type(s.stmt, C.int(i)))
}

// ColumnIsNull reports whether the i-th column of the current row is SQL
// NULL. Equivalent to ColumnType(i) == ColumnNull.
func (s *Stmt) ColumnIsNull(i int) bool {
	return ColumnType(C.sx_column_type(s.stmt, C.int(i))) == ColumnNull
}

// ColumnInt64 returns the i-th column of the current row as int64. Type
// coercions follow SQLite's standard rules.
func (s *Stmt) ColumnInt64(i int) int64 {
	return int64(C.sx_column_int64(s.stmt, C.int(i)))
}

// ColumnFloat64 returns the i-th column as a double.
func (s *Stmt) ColumnFloat64(i int) float64 {
	return float64(C.sx_column_double(s.stmt, C.int(i)))
}

// ColumnText returns the i-th column as a UTF-8 string.
func (s *Stmt) ColumnText(i int) string {
	n := C.sx_column_bytes(s.stmt, C.int(i))
	if n == 0 {
		return ""
	}
	p := C.sx_column_text(s.stmt, C.int(i))
	return C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
}

// ColumnDecltype returns the declared type of the i-th column from
// the table that produced it (e.g. "DATETIME", "INTEGER"). For
// expression columns or sub-selects without a corresponding table
// column, returns the empty string. Wraps sqlite3_column_decltype.
func (s *Stmt) ColumnDecltype(i int) string {
	p := C.sx_column_decltype(s.stmt, C.int(i))
	if p == nil {
		return ""
	}
	return C.GoString(p)
}

// ColumnBlob returns a copy of the i-th column's raw bytes. The result is
// owned by the caller and survives Reset/Step/Finalize.
func (s *Stmt) ColumnBlob(i int) []byte {
	n := C.sx_column_bytes(s.stmt, C.int(i))
	if n == 0 {
		return nil
	}
	p := C.sx_column_blob(s.stmt, C.int(i))
	return C.GoBytes(p, n)
}
