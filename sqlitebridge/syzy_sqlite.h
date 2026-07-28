#ifndef SYZY_SQLITE_H
#define SYZY_SQLITE_H

// Header shim included by every cgo preamble in this package.
//
// Linked build (default): #include "sqlite3.h" — direct symbol
// resolution against the amalgamation linked into this binary.
//
// Extension build (SYZY_EXTENSION): #include "sqlite3ext.h" with
// SQLITE_EXTENSION_INIT3 so each TU gets the extern declaration of
// the shared sqlite3_api pointer (defined once in cgo_extension.go's
// TU via SQLITE_EXTENSION_INIT1).
//
// In both modes, every Go `C.sx_xxx(...)` call resolves through the
// `sx_xxx` wrappers below. In linked mode the wrappers are static
// inline aliases for the real symbols; in extension mode they call
// sqlite3_api->xxx. Cgo can introspect either form as a function.
//
// Why the rename: cgo can't dereference the macro form
// `sqlite3_xxx → sqlite3_api->xxx` — it sees a struct field, not a
// callable. We can't shadow the original sqlite3_xxx prototypes from
// sqlite3.h either, so we use a distinct name (`sx_xxx`) for the
// cgo-visible entry points. Adding a new cgo crossing means adding
// a wrapper here and calling it as `C.sx_xxx(...)` from Go.

#ifdef SYZY_EXTENSION
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3
#else
#include "sqlite3.h"
#endif

#ifdef SYZY_EXTENSION
#define SX_CALL(name) sqlite3_api->name
#else
#define SX_CALL(name) sqlite3_##name
#endif

// Special-case: sqlite3_interrupt's api struct field is `interruptx`
// (renamed in v3.41 to make room for an unblock variant). The linked
// path keeps the original name. SX_INT() picks the right one.
#ifdef SYZY_EXTENSION
#define SX_INT() sqlite3_api->interruptx
#else
#define SX_INT() sqlite3_interrupt
#endif

static inline int sx_bind_blob(sqlite3_stmt *s, int i, const void *p, int n, void(*d)(void*)) {
    return SX_CALL(bind_blob)(s, i, p, n, d);
}
static inline int sx_bind_double(sqlite3_stmt *s, int i, double v) {
    return SX_CALL(bind_double)(s, i, v);
}
static inline int sx_bind_int64(sqlite3_stmt *s, int i, sqlite3_int64 v) {
    return SX_CALL(bind_int64)(s, i, v);
}
static inline int sx_bind_null(sqlite3_stmt *s, int i) {
    return SX_CALL(bind_null)(s, i);
}
static inline int sx_bind_parameter_count(sqlite3_stmt *s) {
    return SX_CALL(bind_parameter_count)(s);
}
static inline int sx_bind_text(sqlite3_stmt *s, int i, const char *t, int n, void(*d)(void*)) {
    return SX_CALL(bind_text)(s, i, t, n, d);
}
static inline sqlite3_int64 sx_changes64(sqlite3 *db) {
    return SX_CALL(changes64)(db);
}
static inline int sx_clear_bindings(sqlite3_stmt *s) {
    return SX_CALL(clear_bindings)(s);
}
static inline int sx_close_v2(sqlite3 *db) {
    return SX_CALL(close_v2)(db);
}
static inline const void *sx_column_blob(sqlite3_stmt *s, int i) {
    return SX_CALL(column_blob)(s, i);
}
static inline int sx_column_bytes(sqlite3_stmt *s, int i) {
    return SX_CALL(column_bytes)(s, i);
}
static inline int sx_column_count(sqlite3_stmt *s) {
    return SX_CALL(column_count)(s);
}
static inline const char *sx_column_decltype(sqlite3_stmt *s, int i) {
    return SX_CALL(column_decltype)(s, i);
}
static inline double sx_column_double(sqlite3_stmt *s, int i) {
    return SX_CALL(column_double)(s, i);
}
static inline sqlite3_int64 sx_column_int64(sqlite3_stmt *s, int i) {
    return SX_CALL(column_int64)(s, i);
}
static inline const char *sx_column_name(sqlite3_stmt *s, int i) {
    return SX_CALL(column_name)(s, i);
}
static inline const unsigned char *sx_column_text(sqlite3_stmt *s, int i) {
    return SX_CALL(column_text)(s, i);
}
static inline int sx_column_type(sqlite3_stmt *s, int i) {
    return SX_CALL(column_type)(s, i);
}
static inline int sx_create_function_v2(
    sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp,
    void (*xFunc)(sqlite3_context*, int, sqlite3_value**),
    void (*xStep)(sqlite3_context*, int, sqlite3_value**),
    void (*xFinal)(sqlite3_context*),
    void (*xDestroy)(void*)) {
    return SX_CALL(create_function_v2)(db, zFunctionName, nArg, eTextRep, pApp, xFunc, xStep, xFinal, xDestroy);
}
static inline const char *sx_errmsg(sqlite3 *db) {
    return SX_CALL(errmsg)(db);
}
static inline int sx_errcode(sqlite3 *db) {
    return SX_CALL(errcode)(db);
}
static inline int sx_extended_errcode(sqlite3 *db) {
    return SX_CALL(extended_errcode)(db);
}
static inline const char *sx_errstr(int rc) {
    return SX_CALL(errstr)(rc);
}
static inline int sx_exec(sqlite3 *db, const char *sql,
    int (*cb)(void*,int,char**,char**), void *arg, char **err) {
    return SX_CALL(exec)(db, sql, cb, arg, err);
}
static inline int sx_finalize(sqlite3_stmt *s) {
    return SX_CALL(finalize)(s);
}
static inline void sx_free(void *p) {
    SX_CALL(free)(p);
}
static inline int sx_get_autocommit(sqlite3 *db) {
    return SX_CALL(get_autocommit)(db);
}
static inline void sx_interrupt(sqlite3 *db) {
    SX_INT()(db);
}
// sx_is_interrupted polls whether sqlite3_interrupt has been called on
// db without scheduling a new interrupt. Long-running vtab xFilter
// loops poll this to surface Ctrl-C / sqlite3_interrupt as
// SQLITE_INTERRUPT instead of hanging in futex_wait.
static inline int sx_is_interrupted(sqlite3 *db) {
    return SX_CALL(is_interrupted)(db);
}
static inline sqlite3_int64 sx_last_insert_rowid(sqlite3 *db) {
    return SX_CALL(last_insert_rowid)(db);
}
static inline const char *sx_libversion(void) {
    return SX_CALL(libversion)();
}
static inline int sx_libversion_number(void) {
    return SX_CALL(libversion_number)();
}
static inline int sx_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs) {
    return SX_CALL(open_v2)(filename, ppDb, flags, zVfs);
}
static inline int sx_prepare_v3(sqlite3 *db, const char *zSql, int nByte,
    unsigned int prepFlags, sqlite3_stmt **ppStmt, const char **pzTail) {
    return SX_CALL(prepare_v3)(db, zSql, nByte, prepFlags, ppStmt, pzTail);
}
// preupdate is intentionally absent from sqlite3_api_routines (the
// SQLite project keeps SQLITE_ENABLE_PREUPDATE_HOOK compile-time).
// In linked mode the wrappers alias the real symbols; in extension
// mode they're declared here and defined in syzy_preupdate_ext.c,
// where they call function pointers populated via dlsym at init
// time. Init-time failure to resolve any preupdate symbol means the
// host SQLite was built without PREUPDATE_HOOK — sqlite3_syzy_init
// reports the error to the caller and returns SQLITE_ERROR.
//
// hooks.c uses the same sx_preupdate_* names so both build modes
// share one code path.
#ifdef SYZY_EXTENSION
extern int sx_preupdate_count(sqlite3 *db);
extern int sx_preupdate_depth(sqlite3 *db);
extern int sx_preupdate_blobwrite(sqlite3 *db);
extern int sx_preupdate_old(sqlite3 *db, int i, sqlite3_value **v);
extern int sx_preupdate_new(sqlite3 *db, int i, sqlite3_value **v);
extern void *sx_preupdate_hook(sqlite3 *db,
    void (*cb)(void*, sqlite3*, int, char const*, char const*, sqlite3_int64, sqlite3_int64),
    void *p);
// sx_resolve_preupdate dlsym's the host's preupdate symbols. Returns
// NULL on success, or a static error string when any symbol is
// missing (host wasn't compiled with SQLITE_ENABLE_PREUPDATE_HOOK).
extern const char *sx_resolve_preupdate(void);
#else
static inline int sx_preupdate_count(sqlite3 *db) {
    return sqlite3_preupdate_count(db);
}
static inline int sx_preupdate_depth(sqlite3 *db) {
    return sqlite3_preupdate_depth(db);
}
static inline int sx_preupdate_blobwrite(sqlite3 *db) {
    return sqlite3_preupdate_blobwrite(db);
}
static inline int sx_preupdate_old(sqlite3 *db, int i, sqlite3_value **v) {
    return sqlite3_preupdate_old(db, i, v);
}
static inline int sx_preupdate_new(sqlite3 *db, int i, sqlite3_value **v) {
    return sqlite3_preupdate_new(db, i, v);
}
static inline void *sx_preupdate_hook(sqlite3 *db,
    void (*cb)(void*, sqlite3*, int, char const*, char const*, sqlite3_int64, sqlite3_int64),
    void *p) {
    return sqlite3_preupdate_hook(db, cb, p);
}
#endif
static inline int sx_reset(sqlite3_stmt *s) {
    return SX_CALL(reset)(s);
}
static inline int sx_step(sqlite3_stmt *s) {
    return SX_CALL(step)(s);
}
static inline const char *sx_db_filename(sqlite3 *db, const char *zName) {
    return SX_CALL(db_filename)(db, zName);
}
// sx_complete reports whether zSql ends with a complete SQL statement.
// Pure lexical scan (no db handle, no allocation); used by the
// statement splitter to verify candidate ';' boundaries, which is what
// keeps splitting correct across trigger BEGIN...END bodies.
static inline int sx_complete(const char *zSql) {
    return SX_CALL(complete)(zSql);
}
// sx_malloc allocates via sqlite3_malloc so the buffer can be returned
// to SQLite (e.g. as *pzErrMsg from sqlite3_syzy_init) and freed with
// sqlite3_free. Caller must free.
static inline void *sx_malloc(int n) {
    return SX_CALL(malloc)(n);
}

// Backup API: page-level streaming copy from one open db to another.
// Used by clone to snapshot app.db / cluster.db without blocking
// concurrent writers on the source. See sqlite3_backup_*.
static inline sqlite3_backup *sx_backup_init(sqlite3 *dst, const char *zDstName,
    sqlite3 *src, const char *zSrcName) {
    return SX_CALL(backup_init)(dst, zDstName, src, zSrcName);
}
static inline int sx_backup_step(sqlite3_backup *b, int nPage) {
    return SX_CALL(backup_step)(b, nPage);
}
static inline int sx_backup_finish(sqlite3_backup *b) {
    return SX_CALL(backup_finish)(b);
}
static inline int sx_backup_pagecount(sqlite3_backup *b) {
    return SX_CALL(backup_pagecount)(b);
}
static inline int sx_backup_remaining(sqlite3_backup *b) {
    return SX_CALL(backup_remaining)(b);
}

// sqlite3_value_* wrappers, used by the syzy_changes vtab to read
// xFilter argv values from Go without crossing the macro layer.
static inline int sx_value_type(sqlite3_value *v) {
    return SX_CALL(value_type)(v);
}
static inline sqlite3_int64 sx_value_int64(sqlite3_value *v) {
    return SX_CALL(value_int64)(v);
}
static inline const unsigned char *sx_value_text(sqlite3_value *v) {
    return SX_CALL(value_text)(v);
}
static inline int sx_value_bytes(sqlite3_value *v) {
    return SX_CALL(value_bytes)(v);
}
static inline const void *sx_value_blob(sqlite3_value *v) {
    return SX_CALL(value_blob)(v);
}

// sqlite3_result_* wrappers used by Go-side xColumn callbacks. The
// _copy variants pass SQLITE_TRANSIENT internally so SQLite copies
// the bytes out of Go-managed memory before returning.
static inline void sx_result_null(sqlite3_context *ctx) {
    SX_CALL(result_null)(ctx);
}
static inline void sx_result_int64(sqlite3_context *ctx, sqlite3_int64 v) {
    SX_CALL(result_int64)(ctx, v);
}
static inline void sx_result_text_copy(sqlite3_context *ctx,
        const char *t, int n) {
    SX_CALL(result_text)(ctx, t, n, SQLITE_TRANSIENT);
}
static inline void sx_result_blob_copy(sqlite3_context *ctx,
        const void *p, int n) {
    SX_CALL(result_blob)(ctx, p, n, SQLITE_TRANSIENT);
}
static inline void sx_result_error_copy(sqlite3_context *ctx,
        const char *msg, int n) {
    SX_CALL(result_error)(ctx, msg, n);
}

// Incremental BLOB I/O (sqlite3_blob_*). Used by blob_patch capture
// (read post-commit NEW bytes) and apply (sqlite3_blob_write into
// the receiver's row). flags: 0 = read-only, 1 = read-write.
static inline int sx_blob_open(sqlite3 *db, const char *zDb, const char *zTable,
        const char *zColumn, sqlite3_int64 iRow, int flags, sqlite3_blob **ppBlob) {
    return SX_CALL(blob_open)(db, zDb, zTable, zColumn, iRow, flags, ppBlob);
}
static inline int sx_blob_read(sqlite3_blob *b, void *p, int n, int off) {
    return SX_CALL(blob_read)(b, p, n, off);
}
static inline int sx_blob_write(sqlite3_blob *b, const void *p, int n, int off) {
    return SX_CALL(blob_write)(b, p, n, off);
}
static inline int sx_blob_bytes(sqlite3_blob *b) {
    return SX_CALL(blob_bytes)(b);
}
static inline int sx_blob_close(sqlite3_blob *b) {
    return SX_CALL(blob_close)(b);
}
static inline int sx_blob_reopen(sqlite3_blob *b, sqlite3_int64 iRow) {
    return SX_CALL(blob_reopen)(b, iRow);
}

#endif // SYZY_SQLITE_H
