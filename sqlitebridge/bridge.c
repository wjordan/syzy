// Compile the vendored SQLite amalgamation as part of this cgo TU per
// https://sqlite.org/howtocompile.html#cli_export. Suppressed in the
// extension build (SYZY_EXTENSION) — the host process already loaded
// libsqlite3 and we route through sqlite3_api_routines instead.

#ifndef SYZY_EXTENSION
#include "sqlite3.c"
#endif
