package sqlitebridge

/*
#include <stdlib.h>
#include "syzy_sqlite.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// OpenFlag selects bits in the SQLITE_OPEN_* family. Combine with bitwise OR.
type OpenFlag int

const (
	OpenReadOnly     OpenFlag = C.SQLITE_OPEN_READONLY
	OpenReadWrite    OpenFlag = C.SQLITE_OPEN_READWRITE
	OpenCreate       OpenFlag = C.SQLITE_OPEN_CREATE
	OpenURI          OpenFlag = C.SQLITE_OPEN_URI
	OpenMemory       OpenFlag = C.SQLITE_OPEN_MEMORY
	OpenNoMutex      OpenFlag = C.SQLITE_OPEN_NOMUTEX
	OpenFullMutex    OpenFlag = C.SQLITE_OPEN_FULLMUTEX
	OpenSharedCache  OpenFlag = C.SQLITE_OPEN_SHAREDCACHE
	OpenPrivateCache OpenFlag = C.SQLITE_OPEN_PRIVATECACHE
)

// DefaultOpenFlags is the recommended flag set for syzy connections:
// read/write, create-if-missing, URI-style filenames enabled, no
// per-connection mutex (callers serialize per Conn — see the Conn doc).
const DefaultOpenFlags = OpenReadWrite | OpenCreate | OpenURI | OpenNoMutex

// SQLPreprocessor is an optional per-Conn hook that rewrites SQL text
// before Prepare / Exec hand it to SQLite. A non-nil error short-
// circuits Prepare / Exec with that error. The producer uses this hook
// to rewrite rowid-alias DDL into a multi-writer-safe shape; see
// internal/producer/ddl_rewrite.go.
type SQLPreprocessor func(sql string) (string, error)

// Conn wraps a sqlite3* handle.
//
// Conn is not safe for concurrent use. Callers must serialize per-connection
// access. We compile SQLite with SQLITE_THREADSAFE=2 (multi-threaded mode)
// and open with SQLITE_OPEN_NOMUTEX, so this isolation is the caller's
// responsibility.
type Conn struct {
	db         *C.sqlite3
	state      *connState
	preprocess SQLPreprocessor
}

// SetSQLPreprocessor installs (or with nil, clears) a preprocessor that
// transforms SQL text before Prepare / Exec submit it to SQLite. The
// rewritten string is what SQLite compiles and what flows back through
// the trace hook on first Step, so any downstream classifier sees the
// rewritten form. Invocations are serialized by the per-Conn single-
// writer contract.
func (c *Conn) SetSQLPreprocessor(fn SQLPreprocessor) {
	c.preprocess = fn
}

// applyPreprocess runs the installed preprocessor if any, otherwise
// returns sql unchanged.
func (c *Conn) applyPreprocess(sql string) (string, error) {
	if c.preprocess == nil {
		return sql, nil
	}
	return c.preprocess(sql)
}

// PreprocessSQL runs the installed preprocessor on sql and returns the
// result. Used by the loadable-extension prepare interposer, where the
// host app's sqlite3_prepare* calls bypass this package's Prepare/Exec
// and the rewrite has to be applied from the interposition shim
// instead. Returns sql unchanged when no preprocessor is installed.
func (c *Conn) PreprocessSQL(sql string) (string, error) {
	return c.applyPreprocess(sql)
}

// Open returns a new Conn against path (which may be a SQLite URI when
// OpenURI is set). flags=0 selects DefaultOpenFlags.
func Open(path string, flags OpenFlag) (*Conn, error) {
	if flags == 0 {
		flags = DefaultOpenFlags
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var db *C.sqlite3
	rc := C.sx_open_v2(cpath, &db, C.int(flags), nil)
	if rc != C.SQLITE_OK {
		// sqlite3_open_v2 may allocate the db handle even on failure; close it
		// before surfacing the error.
		err := newErrorFromDB(rc, db)
		if db != nil {
			C.sx_close_v2(db)
		}
		return nil, err
	}
	c := &Conn{db: db}
	if err := registerFuncs(c); err != nil {
		C.sx_close_v2(db)
		return nil, err
	}
	return c, nil
}

// Close releases the connection's resources. Safe to call multiple times.
func (c *Conn) Close() error {
	if c.db == nil {
		return nil
	}
	c.clearState()
	clearGenIDState(c.db)
	clearChangesState(c.db)
	rc := C.sx_close_v2(c.db)
	c.db = nil
	if rc != C.SQLITE_OK {
		return newErrorFromCode(rc)
	}
	return nil
}

// WrapHandle adopts an existing *sqlite3 handle (passed as an
// unsafe.Pointer to keep this header free of cgo types). The returned
// Conn does NOT own the handle's lifecycle — Release tears down our
// per-conn state without calling sqlite3_close. Used by the
// loadable-extension shim where the host SQLite owns the handle and
// the extension only attaches hooks to it.
func WrapHandle(p unsafe.Pointer) (*Conn, error) {
	if p == nil {
		return nil, Error{Code: int(C.SQLITE_MISUSE), Msg: "WrapHandle: nil handle"}
	}
	c := &Conn{db: (*C.sqlite3)(p)}
	if err := registerFuncs(c); err != nil {
		return nil, err
	}
	return c, nil
}

// Release tears down per-conn syzy state (touch journal, hooks,
// gen_id state) WITHOUT calling sqlite3_close_v2 on the handle. Pair
// with WrapHandle. Idempotent.
func (c *Conn) Release() error {
	if c.db == nil {
		return nil
	}
	c.clearState()
	clearGenIDState(c.db)
	clearChangesState(c.db)
	c.db = nil
	return nil
}

// Exec runs one or more SQL statements with no parameter binding and no
// result rows. It is appropriate for DDL, pragmas, and other administrative
// SQL. Use Prepare/Step for queries that bind parameters or read columns.
func (c *Conn) Exec(sql string) error {
	sql, err := c.applyPreprocess(sql)
	if err != nil {
		return err
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	var errmsg *C.char
	rc := C.sx_exec(c.db, csql, nil, nil, &errmsg)
	if rc != C.SQLITE_OK {
		e := newErrorFromDB(rc, c.db)
		if errmsg != nil {
			e.Msg = C.GoString(errmsg) // sx_exec's own message is per-statement; prefer it
			C.sx_free(unsafe.Pointer(errmsg))
		}
		return c.refineCommitHookError(e)
	}
	return nil
}

// QueryInt64Row executes sql and returns the integer columns of its first
// row. For statements like PRAGMA wal_checkpoint or PRAGMA data_version
// whose result is one row of integers.
func (c *Conn) QueryInt64Row(sql string) ([]int64, error) {
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, fmt.Errorf("sqlitebridge: %q returned no rows", sql)
	}
	out := make([]int64, stmt.ColumnCount())
	for i := range out {
		out[i] = stmt.ColumnInt64(i)
	}
	return out, nil
}

// Changes returns the number of rows modified by the most recent INSERT,
// UPDATE, or DELETE on this connection (sqlite3_changes64).
func (c *Conn) Changes() int64 {
	return int64(C.sx_changes64(c.db))
}

// LastInsertRowID returns the rowid of the most recent successful INSERT on
// this connection (sqlite3_last_insert_rowid).
func (c *Conn) LastInsertRowID() int64 {
	return int64(C.sx_last_insert_rowid(c.db))
}

// Interrupt aborts any in-progress operation on the connection. Returns when
// SQLite acknowledges the interrupt request; the affected statement returns
// SQLITE_INTERRUPT to its caller. No-op when the connection is already
// closed; matches the nil-safety of Close / Release.
func (c *Conn) Interrupt() {
	if c.db == nil {
		return
	}
	C.sx_interrupt(c.db)
}

// InAutocommit reports whether the connection is in autocommit mode (no
// open transaction). Used by trace_v2 to reject DDL inside explicit
// BEGIN / SAVEPOINT.
func (c *Conn) InAutocommit() bool {
	return C.sx_get_autocommit(c.db) != 0
}

func (c *Conn) handle() *C.sqlite3 {
	return c.db
}
