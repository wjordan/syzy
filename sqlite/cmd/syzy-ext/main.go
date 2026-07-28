//go:build syzy_extension

// Command syzy-ext is the SQLite loadable-extension shim. Build with:
//
//	CGO_ENABLED=1 go build -tags syzy_extension \
//	    -buildmode=c-shared -o ext/syzy.so ./cmd/syzy-ext
//
// Then load from any SQLite client:
//
//	sqlite3 app.db
//	sqlite> .load ./ext/syzy
//	sqlite> INSERT INTO ...   -- writes are journalled under app.db-syzy/origins/<hex>/
//
// A separate `syzy daemon --db app.db` process (typically auto-spawned
// the first time an extension-loaded host opens app.db) drains those
// journals through the broadcast pipeline.
//
// Host SQLite must be built with SQLITE_ENABLE_PREUPDATE_HOOK. Most
// distro packages (Debian/Ubuntu/Homebrew) include it. .load fails
// loud when it's absent (sx_resolve_preupdate reports the missing
// symbol).
//
// The actual producer-setup logic lives in syzyext. This file is the
// cgo shim that runs when SQLite dlopen's syzy.so: it wires the host's
// sqlite3_api routines into our cgo TUs, dlsym's the preupdate
// symbols, and hands control to syzyext.Attach.
package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../../third_party/sqlite
#cgo CFLAGS: -I${SRCDIR}/../../../sqlitebridge
#cgo CFLAGS: -DSYZY_EXTENSION=1
#cgo CFLAGS: -fPIC
// See cgo_extension.go for why FORTIFY is off — keeps the .so musl-loadable.
#cgo CFLAGS: -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0
#cgo CFLAGS: -Wno-unused-parameter
#cgo LDFLAGS: -ldl

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>

#include "syzy_sqlite.h"

// sqlite3_syzy_init is the SQLite extension entry point exported by
// Go below. cgo declares the pApi parameter as void* (matching the
// Go unsafe.Pointer signature); the forward decl matches that so the
// autoload hook (defined in autoload_shim.c) can delegate without a
// type clash against the cgo-generated header.
extern int sqlite3_syzy_init(sqlite3 *db, char **pzErrMsg, void *pApi);

// Path-aware auto-extension hook + sqlite3_initialize interposer live
// in autoload_shim.c. cgo compiles every .c file in the package
// exactly once, which side-steps the multiple-definition problem
// you'd hit putting non-static code in the cgo preamble (cgo emits
// the preamble into several generated .o files).
*/
import "C"

import (
	"fmt"
	"os"
	"sync"
	"unsafe"

	"github.com/wjordan/syzy/sqlite/syzyext"
	"github.com/wjordan/syzy/sqlitebridge"
)

// liveExt holds one running attachment per host SQLite conn. Keyed by
// the host's *sqlite3 handle. The map prevents Go GC from reclaiming
// the producer + cache + metadata before the conn closes.
//
// TODO: entries are never cleaned up. A process that repeatedly opens
// a SQLite conn, loads syzy.so, and closes the conn leaks the
// attachment (origin flock, metadata handle, ctrl socket, producer
// state) until the process exits. The typical usage pattern is one
// long-lived conn per process so this is bounded in practice, but a
// long-running daemon-style client that opens/closes connections
// would accumulate leaked attachments. The fix is to register a
// sqlite3_create_function_v2 xDestroy callback on a sentinel function
// (the only extension-callable lifecycle hook tied to conn close)
// that calls Attached.Close + writer.Release and deletes the extMap
// entry.
var (
	extMu  sync.Mutex
	extMap = map[uintptr]*extState{}
)

type extState struct {
	writer   *sqlitebridge.Conn
	attached *syzyext.Attached
	// handle is the host sqlite3* this attachment rides. Park re-wraps
	// it (WrapHandle) for the fresh attach at unpark.
	handle unsafe.Pointer
	// dbPath is the canonical "main" filename the attach was made
	// against. extMap entries are never removed on conn close (see the
	// TODO above), so a recycled sqlite3* address from a later open of
	// a DIFFERENT database can collide with a stale entry; callers that
	// act on a handle (rewrite its SQL, reinstall hooks) must verify
	// the handle's current filename still matches before trusting the
	// entry. A reopen of the SAME path re-attaches and overwrites the
	// entry first, so a match means the entry is live.
	dbPath string
}

// main is required by go's c-shared build but never called.
func main() {}

//export sqlite3_syzy_init
func sqlite3_syzy_init(db *C.sqlite3, pzErrMsg **C.char, pApi unsafe.Pointer) C.int {
	// Wire the host's api routines into our cgo TUs.
	sqlitebridge.SyzyExtensionInit(pApi)
	// Resolve the preupdate symbols via dlsym. Fails loud if the host
	// libsqlite3 wasn't built with SQLITE_ENABLE_PREUPDATE_HOOK.
	if msg := C.sx_resolve_preupdate(); msg != nil {
		setErr(pzErrMsg, "syzy: "+C.GoString(msg))
		return failLoud(db)
	}
	if err := initExtension(db); err != nil {
		setErr(pzErrMsg, "syzy: "+err.Error())
		return failLoud(db)
	}
	return C.int(0) // SQLITE_OK
}

// failLoud puts the connection into query_only mode before reporting an
// init failure.
//
// A client that loads the extension and then writes anyway is the worst
// outcome: SQLite's `.load` failure is a printed message, not a fatal
// one — `sqlite3 -cmd '.load syzy' app.db "INSERT ..."` reports the
// error and then runs the INSERT, landing a row that no peer will ever
// see and that no later repair can distinguish from a legitimate one.
// query_only turns that into an ordinary write failure on the very next
// statement.
//
// It is deliberately pure SQLite connection state, not a hook: SQLite
// dlclose's an extension whose init returned an error, so any callback
// left pointing into this library would dangle. A user who really wants
// to continue unreplicated can say so with `PRAGMA query_only=OFF`.
func failLoud(db *C.sqlite3) C.int {
	C.sx_exec(db, cstr("PRAGMA query_only=ON"), nil, nil, nil)
	return C.int(1) // SQLITE_ERROR
}

func initExtension(db *C.sqlite3) error {
	cpath := C.sx_db_filename(db, cstr("main"))
	if cpath == nil {
		return fmt.Errorf("sqlite3_db_filename returned NULL; extension can only attach to a file-backed connection")
	}
	dbPath := C.GoString(cpath)
	if dbPath == "" {
		return fmt.Errorf("attached database is in-memory; syzy requires a file-backed connection")
	}

	// Wrap the host's db handle. WrapHandle adopts the existing
	// sqlite3 without opening a fresh one; hooks installed on this
	// Conn target the host's writer.
	writer, err := sqlitebridge.WrapHandle(unsafe.Pointer(db))
	if err != nil {
		return fmt.Errorf("wrap host conn: %w", err)
	}

	// SYZY_AUTOSPAWN=0 skips the auto-spawn; the extension still
	// works, but nothing drains the journal until `syzy daemon` starts
	// manually.
	autoSpawn := os.Getenv("SYZY_AUTOSPAWN") != "0"
	attached, err := syzyext.AttachWithRetry(writer, syzyext.Config{
		DBPath:    dbPath,
		AutoSpawn: autoSpawn,
	})
	if err != nil {
		_ = writer.Release()
		return err
	}

	extMu.Lock()
	extMap[uintptr(unsafe.Pointer(db))] = &extState{
		writer:   writer,
		attached: attached,
		handle:   unsafe.Pointer(db),
		dbPath:   dbPath,
	}
	extMu.Unlock()
	startParkControl()
	return nil
}

// setErr copies msg into a sqlite3-allocated buffer and stores it at
// *pzErrMsg. The caller (SQLite) frees with sqlite3_free.
func setErr(pzErrMsg **C.char, msg string) {
	if pzErrMsg == nil {
		return
	}
	cmsg := C.CString(msg)
	defer C.free(unsafe.Pointer(cmsg))
	n := len(msg) + 1
	dst := (*C.char)(unsafe.Pointer(C.sx_malloc(C.int(n))))
	if dst == nil {
		return
	}
	C.memcpy(unsafe.Pointer(dst), unsafe.Pointer(cmsg), C.size_t(n))
	*pzErrMsg = dst
}

func cstr(s string) *C.char {
	return C.CString(s)
}
