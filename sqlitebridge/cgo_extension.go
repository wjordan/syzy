//go:build syzy_extension

package sqlitebridge

// Extension build: compile against sqlite3ext.h and route every
// sqlite3_xxx call through the host SQLite's sqlite3_api_routines
// pointer. The host process's libsqlite3 owns the actual symbols at
// load time. The c-shared shim in sqlite/cmd/syzy-ext calls
// SyzyExtensionInit() with the api pointer to wire this up.
//
// -DSYZY_EXTENSION switches the .c files in this package to their
// extension paths (see bridge.c, hooks.c, funcs.c — each guards on
// SYZY_EXTENSION). bridge.c's amalgamation #include is suppressed;
// the extension entry definitions live in bridge_ext.c.
//
// The PIC flag is required by -buildmode=c-shared. We don't link
// against -lm/-lpthread here because the host SQLite already pulled
// them in.

/*
#cgo CFLAGS: -I${SRCDIR}/../third_party/sqlite
#cgo CFLAGS: -DSYZY_EXTENSION=1
#cgo CFLAGS: -DSQLITE_DQS=0
#cgo CFLAGS: -DSQLITE_ENABLE_PREUPDATE_HOOK=1
#cgo CFLAGS: -DSQLITE_THREADSAFE=2
#cgo CFLAGS: -DHAVE_USLEEP=1
#cgo CFLAGS: -fPIC
// Drop glibc's _FORTIFY_SOURCE printf wrappers so the resulting .so
// loads on musl too (Alpine etc.) — musl ships only the plain
// vfprintf/fprintf symbols, no __vfprintf_chk/__fprintf_chk. The
// FORTIFY checks are a marginal hardening win for the sqlite3
// amalgamation; LD_PRELOAD'ing into Alpine apps matters more.
#cgo CFLAGS: -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0
#cgo CFLAGS: -Wno-unused-parameter -Wno-unused-function

#cgo LDFLAGS: -ldl

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

// SyzyExtensionInit wires the host's sqlite3_api_routines into our
// macro-rerouted sqlite3_xxx calls. Must be called before any other
// sqlite3_* call from this TU.
static int syzyExtensionInit(const sqlite3_api_routines *api) {
    SQLITE_EXTENSION_INIT2(api);
    return 0;
}
*/
import "C"

import "unsafe"

// SyzyExtensionInit wires the host SQLite's sqlite3_api_routines so
// every subsequent sqlite3_xxx call from this package resolves
// through the host's table. Must be called before any Conn / Stmt
// construction. Pass the api pointer the host gave to the extension's
// sqlite3_syzy_init entry point. apiPtr is opaque to Go (treated as
// unsafe.Pointer); the sqlite/cmd/syzy-ext shim handles the conversion.
func SyzyExtensionInit(apiPtr unsafe.Pointer) {
	C.syzyExtensionInit((*C.sqlite3_api_routines)(apiPtr))
}
