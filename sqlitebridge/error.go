package sqlitebridge

/*
#include "syzy_sqlite.h"
*/
import "C"

import (
	"errors"
	"fmt"
)

// Error wraps a SQLite result code and message. Code holds the primary or
// extended SQLite result code as documented at <https://sqlite.org/rescode.html>.
// Extended holds the extended result code when the connection reported one
// refining Code (else it equals Code) — e.g. SQLITE_CONSTRAINT_COMMITHOOK
// vs SQLITE_CONSTRAINT_UNIQUE, which Code alone merges as SQLITE_CONSTRAINT.
type Error struct {
	Code     int
	Extended int
	Msg      string
}

func (e Error) Error() string {
	if e.Msg == "" {
		return fmt.Sprintf("sqlite: %s (code %d)", errStrFromCode(C.int(e.Code)), e.Code)
	}
	return fmt.Sprintf("sqlite: %s (code %d)", e.Msg, e.Code)
}

// IsCode reports whether err wraps a sqlitebridge.Error with the given primary
// or extended SQLite result code.
func IsCode(err error, code int) bool {
	var e Error
	return errors.As(err, &e) && (e.Code == code || e.Extended == code)
}

func (c *Conn) refineCommitHookError(err error) error {
	if c.state == nil || c.state.commitCause == nil {
		return err
	}
	cause := c.state.commitCause
	c.state.commitCause = nil
	if !IsCode(err, ResultConstraintCommitHook) {
		return err
	}
	return fmt.Errorf("%w: %w", cause, err)
}

// ResultConstraintCommitHook is the extended code for a commit rejected by
// the registered commit hook — for syzy, a coordinated-UNIQUE reservation
// that conflicted or whose backend was unavailable. Higher layers can attach
// a distinct Go cause with SetCommitHookCause while retaining this SQLite
// result code.
const ResultConstraintCommitHook = int(C.SQLITE_CONSTRAINT_COMMITHOOK)

const (
	ResultOK         = int(C.SQLITE_OK)
	ResultError      = int(C.SQLITE_ERROR)
	ResultBusy       = int(C.SQLITE_BUSY)
	ResultLocked     = int(C.SQLITE_LOCKED)
	ResultFull       = int(C.SQLITE_FULL)
	ResultMisuse     = int(C.SQLITE_MISUSE)
	ResultConstraint = int(C.SQLITE_CONSTRAINT)
	ResultRow        = int(C.SQLITE_ROW)
	ResultDone       = int(C.SQLITE_DONE)
	ResultInterrupt  = int(C.SQLITE_INTERRUPT)
)

func newErrorFromDB(rc C.int, db *C.sqlite3) Error {
	msg := ""
	ext := int(rc)
	if db != nil {
		msg = C.GoString(C.sx_errmsg(db))
		// The extended code distinguishes failure flavors the primary code
		// merges (e.g. SQLITE_CONSTRAINT_COMMITHOOK vs _UNIQUE) — but only
		// when it refines THIS rc; a stale errcode from an earlier statement
		// on the connection must not be attributed to this failure.
		if e := int(C.sx_extended_errcode(db)); e&0xff == int(rc) {
			ext = e
		}
	}
	if msg == "" {
		msg = errStrFromCode(rc)
	}
	return Error{Code: int(rc), Extended: ext, Msg: msg}
}

func newErrorFromCode(rc C.int) Error {
	return Error{Code: int(rc), Msg: errStrFromCode(rc)}
}

func errStrFromCode(rc C.int) string {
	return C.GoString(C.sx_errstr(rc))
}
