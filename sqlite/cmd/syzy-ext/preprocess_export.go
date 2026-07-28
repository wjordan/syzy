//go:build syzy_extension

package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../../third_party/sqlite
#cgo CFLAGS: -I${SRCDIR}/../../../sqlitebridge
#include "syzy_sqlite.h"
*/
import "C"

import (
	"unsafe"

	"github.com/wjordan/syzy/internal/syzylog"
	"github.com/wjordan/syzy/sqlite/syzyext"
	"github.com/wjordan/syzy/sqlitebridge"
)

// cgo exports called by the prepare/exec interposers in
// autoload_shim.c. Both return a malloc'd rewritten SQL string (the C
// side frees it with free()) or NULL for "use the caller's original
// text". They must never fail a prepare: any internal error — including
// a Go panic, which would otherwise abort the host app process —
// degrades to NULL/passthrough, leaving DDL admission as the backstop.

// cMain is the constant "main" schema name handed to sx_db_filename;
// allocated once instead of per lookup.
var cMain = C.CString("main")

// lookupWriter resolves a host sqlite3* to its attached writer Conn,
// or nil when the handle has no syzy attachment (the common case for
// unrelated DBs in the same process). extMap entries outlive their
// conns (no close hook; see the extMap TODO), so a recycled handle
// address from a later open of a different database can collide with a
// stale entry — verify the handle's live "main" filename still matches
// the path the entry attached to, and drop the entry when it doesn't.
func lookupWriter(db *C.sqlite3) *sqlitebridge.Conn {
	extMu.Lock()
	defer extMu.Unlock()
	key := uintptr(unsafe.Pointer(db))
	st := extMap[key]
	if st == nil {
		return nil
	}
	if fn := C.sx_db_filename(db, cMain); fn == nil || C.GoString(fn) != st.dbPath {
		// Stale entry: the original conn closed and the address was
		// reused by an unrelated open. Acting on it would rewrite SQL
		// for (or install hooks on) the wrong database.
		delete(extMap, key)
		return nil
	}
	return st.writer
}

// recoverPassthrough is the shared panic backstop for the cgo exports:
// a Go panic would otherwise unwind into the host app's C stack and
// abort the process. The interposers treat nil as "use the original
// text", so degrading to nil keeps the app running with admission as
// the backstop.
func recoverPassthrough(what string, ret **C.char) {
	if r := recover(); r != nil {
		syzylog.Printf("syzy: %s panic (passing through): %v", what, r)
		*ret = nil
	}
}

//export sx_syzy_preprocess
func sx_syzy_preprocess(db *C.sqlite3, zSql *C.char, n C.int, pzConsumed *C.int) (ret *C.char) {
	defer recoverPassthrough("prepare preprocess", &ret)
	w := lookupWriter(db)
	if w == nil || zSql == nil || n < 0 {
		return nil
	}
	out, consumed, changed := syzyext.PreprocessPrepare(w, C.GoStringN(zSql, n))
	if !changed {
		return nil
	}
	*pzConsumed = C.int(consumed)
	return C.CString(out)
}

//export sx_syzy_reassert_wal_hook
func sx_syzy_reassert_wal_hook(db *C.sqlite3) {
	// openDatabase installs SQLite's autocheckpoint wal_hook AFTER
	// auto-extension load, clobbering the producer wal_hook the attach
	// just installed (no journaling, no DDL intent resolution). The
	// open interposers call this after the real open returns to put
	// the producer hook back. No-op for unattached handles. A panic
	// here would abort the host app, so recover like the other exports
	// (skipping the re-assert degrades to the pre-fix behavior, not a
	// crash).
	defer func() {
		if r := recover(); r != nil {
			syzylog.Printf("syzy: reassert wal hook panic (skipping): %v", r)
		}
	}()
	if w := lookupWriter(db); w != nil {
		w.ReassertWALHook()
	}
}

//export sx_syzy_preprocess_exec
func sx_syzy_preprocess_exec(db *C.sqlite3, zSql *C.char) (ret *C.char) {
	defer recoverPassthrough("exec preprocess", &ret)
	w := lookupWriter(db)
	if w == nil || zSql == nil {
		return nil
	}
	out, changed := syzyext.PreprocessExec(w, C.GoString(zSql))
	if !changed {
		return nil
	}
	return C.CString(out)
}
