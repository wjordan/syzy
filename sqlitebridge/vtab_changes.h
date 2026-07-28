#ifndef SYZY_VTAB_CHANGES_H
#define SYZY_VTAB_CHANGES_H

#include "syzy_sqlite.h"

// syzy_register_changes_vtab installs the eponymous "syzy_changes"
// virtual table on db. feed_path is the absolute path of the
// notify-feed mmap file (typically layout.NotifyFeed(appPath)). The
// path is duplicated into module-private storage and freed when the
// module is unregistered. Returns SQLITE_OK or the failing
// sqlite3_create_module_v2 result code.
//
// Schema (declare_vtab):
//
//   CREATE TABLE x(
//     origin          INTEGER,
//     seq             INTEGER,
//     table_name      TEXT,
//     op              TEXT,    -- 'insert'|'update'|'delete'|'blob_patch'|'lossy'
//     pk              BLOB,
//     pk_truncated    INTEGER,
//     table_truncated INTEGER,
//     timeout_ms      INTEGER HIDDEN
//   )
//
// xBestIndex picks up these WHERE constraints and pushes them into
// xFilter via idxNum:
//
//   bit 0: table_name = ?  (single-table filter)
//   bit 1: timeout_ms = ?  (xFilter blocking deadline; <0 indefinite,
//                           =0 peek-only, >0 ms cap)
//
// All other WHERE clauses are filtered client-side by SQLite.
int syzy_register_changes_vtab(sqlite3 *db, const char *feed_path);

// syzy_register_changes_scalars installs the companion scalar
// functions on db: syzy_my_origin() (the connection's claimed origin)
// and syzy_pk_decode(table TEXT, pk BLOB) → JSON TEXT. Both delegate
// to Go via the per-connection changesProvider registered alongside
// the vtab.
int syzy_register_changes_scalars(sqlite3 *db);

#endif
