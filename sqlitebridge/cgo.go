//go:build !syzy_extension

package sqlitebridge

// Linked build: compile + link the vendored SQLite amalgamation. Used
// by the Go library and the daemon binary. The extension build (tag
// syzy_extension) takes the api-routed path in cgo_extension.go
// instead.
//
// SQLITE_DQS=0 disables double-quoted string literals — a long-standing SQL
// footgun (it lets unquoted column names silently become string constants).
// SQLITE_THREADSAFE=2 + SQLITE_OPEN_NOMUTEX shifts per-connection serialization
// to the caller, who already does it.

/*
#cgo CFLAGS: -I${SRCDIR}/../third_party/sqlite
#cgo CFLAGS: -DSQLITE_ENABLE_PREUPDATE_HOOK=1
#cgo CFLAGS: -DSQLITE_THREADSAFE=2
#cgo CFLAGS: -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1
#cgo CFLAGS: -DSQLITE_OMIT_DEPRECATED=1
#cgo CFLAGS: -DSQLITE_DQS=0
#cgo CFLAGS: -DSQLITE_ENABLE_FTS5=1
#cgo CFLAGS: -DHAVE_USLEEP=1
#cgo CFLAGS: -Wno-unused-parameter -Wno-unused-function

#cgo LDFLAGS: -lm -lpthread
*/
import "C"
