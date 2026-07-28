#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "vtab_changes.h"

// Go-side callbacks. Each takes the vtab/cursor pointer as a uintptr_t
// (used as a sync.Map key on the Go side) and writes results back via
// out parameters. Errors come back as a malloc'd C string the caller
// frees, so we don't need to expose Go's error type to C.
extern int syzyVTabConnect(sqlite3 *db, const char *feed_path,
    uintptr_t vtab_id, char **err_out);
extern void syzyVTabDisconnect(uintptr_t vtab_id);
extern void syzyVTabCursorOpen(uintptr_t vtab_id, uintptr_t cursor_id);
extern void syzyVTabCursorClose(uintptr_t vtab_id, uintptr_t cursor_id);
extern int syzyVTabFilter(uintptr_t vtab_id, uintptr_t cursor_id,
    int idxNum, sqlite3_value *table_arg, sqlite3_value *timeout_arg,
    char **err_out);
extern void syzyVTabNext(uintptr_t vtab_id, uintptr_t cursor_id);
extern int syzyVTabEof(uintptr_t vtab_id, uintptr_t cursor_id);
extern void syzyVTabColumn(uintptr_t vtab_id, uintptr_t cursor_id,
    sqlite3_context *ctx, int col);
extern sqlite3_int64 syzyVTabRowid(uintptr_t vtab_id, uintptr_t cursor_id);

// Column indices. Order must match the schema string in xConnect and
// the column dispatch in syzyVTabColumn (Go side).
enum {
    SYZY_COL_ORIGIN          = 0,
    SYZY_COL_SEQ             = 1,
    SYZY_COL_TABLE_NAME      = 2,
    SYZY_COL_OP              = 3,
    SYZY_COL_PK              = 4,
    SYZY_COL_PK_TRUNCATED    = 5,
    SYZY_COL_TABLE_TRUNCATED = 6,
    SYZY_COL_TIMEOUT_MS      = 7,
};

// idxNum bit assignments shared with the Go side.
#define SYZY_IDX_TABLE_EQ   0x1
#define SYZY_IDX_TIMEOUT_EQ 0x2

typedef struct syzy_vtab {
    sqlite3_vtab base;
    sqlite3 *db; // for sx_is_interrupted from Go
} syzy_vtab;

typedef struct syzy_cursor {
    sqlite3_vtab_cursor base;
} syzy_cursor;

#define SYZY_CHANGES_SCHEMA \
    "CREATE TABLE x(" \
    "origin INTEGER," \
    "seq INTEGER," \
    "table_name TEXT," \
    "op TEXT," \
    "pk BLOB," \
    "pk_truncated INTEGER," \
    "table_truncated INTEGER," \
    "timeout_ms INTEGER HIDDEN" \
    ")"

static int syzyXConnect(sqlite3 *db, void *aux, int argc,
        const char *const *argv, sqlite3_vtab **ppVtab, char **pzErr) {
    (void)argc; (void)argv;
    const char *feed_path = (const char *)aux;

    int rc = sqlite3_declare_vtab(db, SYZY_CHANGES_SCHEMA);
    if (rc != SQLITE_OK) {
        return rc;
    }

    syzy_vtab *v = sqlite3_malloc(sizeof(*v));
    if (v == NULL) {
        return SQLITE_NOMEM;
    }
    memset(v, 0, sizeof(*v));
    v->db = db;

    char *err = NULL;
    rc = syzyVTabConnect(db, feed_path, (uintptr_t)v, &err);
    if (rc != SQLITE_OK) {
        if (err != NULL) {
            *pzErr = sqlite3_mprintf("syzy_changes: %s", err);
            free(err);
        }
        sqlite3_free(v);
        return rc;
    }
    *ppVtab = &v->base;
    return SQLITE_OK;
}
static int syzyXDisconnect(sqlite3_vtab *pVtab) {
    syzy_vtab *v = (syzy_vtab *)pVtab;
    syzyVTabDisconnect((uintptr_t)v);
    sqlite3_free(v);
    return SQLITE_OK;
}

// xBestIndex accepts at most one `table_name = ?` and one
// `timeout_ms = ?` constraint. Other WHERE clauses fall through to
// SQLite's client-side filter. We pack their presence into idxNum and
// hand the values to xFilter via argv in the order [table?, timeout?].
static int syzyXBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *info) {
    (void)pVtab;
    int idxNum = 0;
    int next_argv = 1; // SQLite argvIndex is 1-based.
    int table_ci = -1, timeout_ci = -1;

    for (int i = 0; i < info->nConstraint; i++) {
        const struct sqlite3_index_constraint *c = &info->aConstraint[i];
        if (!c->usable) continue;
        if (c->op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
        if (c->iColumn == SYZY_COL_TABLE_NAME && table_ci < 0) {
            table_ci = i;
        } else if (c->iColumn == SYZY_COL_TIMEOUT_MS && timeout_ci < 0) {
            timeout_ci = i;
        }
    }
    if (table_ci >= 0) {
        idxNum |= SYZY_IDX_TABLE_EQ;
        info->aConstraintUsage[table_ci].argvIndex = next_argv++;
        info->aConstraintUsage[table_ci].omit = 1;
    }
    if (timeout_ci >= 0) {
        idxNum |= SYZY_IDX_TIMEOUT_EQ;
        info->aConstraintUsage[timeout_ci].argvIndex = next_argv++;
        info->aConstraintUsage[timeout_ci].omit = 1;
    }
    info->idxNum = idxNum;
    info->estimatedCost = 1.0;
    info->estimatedRows = 1;
    return SQLITE_OK;
}

static int syzyXOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
    syzy_cursor *c = sqlite3_malloc(sizeof(*c));
    if (c == NULL) {
        return SQLITE_NOMEM;
    }
    memset(c, 0, sizeof(*c));
    syzy_vtab *v = (syzy_vtab *)pVtab;
    syzyVTabCursorOpen((uintptr_t)v, (uintptr_t)c);
    *ppCursor = &c->base;
    return SQLITE_OK;
}

static int syzyXClose(sqlite3_vtab_cursor *cur) {
    syzy_vtab *v = (syzy_vtab *)cur->pVtab;
    syzy_cursor *c = (syzy_cursor *)cur;
    syzyVTabCursorClose((uintptr_t)v, (uintptr_t)c);
    sqlite3_free(c);
    return SQLITE_OK;
}

static int syzyXFilter(sqlite3_vtab_cursor *cur, int idxNum,
        const char *idxStr, int argc, sqlite3_value **argv) {
    (void)idxStr;
    syzy_vtab *v = (syzy_vtab *)cur->pVtab;
    syzy_cursor *c = (syzy_cursor *)cur;

    sqlite3_value *table_arg = NULL;
    sqlite3_value *timeout_arg = NULL;
    int ai = 0;
    if (idxNum & SYZY_IDX_TABLE_EQ) {
        if (ai < argc) table_arg = argv[ai++];
    }
    if (idxNum & SYZY_IDX_TIMEOUT_EQ) {
        if (ai < argc) timeout_arg = argv[ai++];
    }

    char *err = NULL;
    int rc = syzyVTabFilter((uintptr_t)v, (uintptr_t)c, idxNum,
        table_arg, timeout_arg, &err);
    if (rc != SQLITE_OK && err != NULL) {
        sqlite3_free(v->base.zErrMsg);
        v->base.zErrMsg = sqlite3_mprintf("syzy_changes: %s", err);
        free(err);
    }
    return rc;
}

static int syzyXNext(sqlite3_vtab_cursor *cur) {
    syzy_vtab *v = (syzy_vtab *)cur->pVtab;
    syzy_cursor *c = (syzy_cursor *)cur;
    syzyVTabNext((uintptr_t)v, (uintptr_t)c);
    return SQLITE_OK;
}

static int syzyXEof(sqlite3_vtab_cursor *cur) {
    syzy_vtab *v = (syzy_vtab *)cur->pVtab;
    syzy_cursor *c = (syzy_cursor *)cur;
    return syzyVTabEof((uintptr_t)v, (uintptr_t)c);
}

static int syzyXColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col) {
    syzy_vtab *v = (syzy_vtab *)cur->pVtab;
    syzy_cursor *c = (syzy_cursor *)cur;
    syzyVTabColumn((uintptr_t)v, (uintptr_t)c, ctx, col);
    return SQLITE_OK;
}

static int syzyXRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *pRowid) {
    syzy_vtab *v = (syzy_vtab *)cur->pVtab;
    syzy_cursor *c = (syzy_cursor *)cur;
    *pRowid = syzyVTabRowid((uintptr_t)v, (uintptr_t)c);
    return SQLITE_OK;
}

static const sqlite3_module syzy_changes_module = {
    /* iVersion    */ 0,
    /* xCreate     */ NULL, // eponymous
    /* xConnect    */ syzyXConnect,
    /* xBestIndex  */ syzyXBestIndex,
    /* xDisconnect */ syzyXDisconnect,
    /* xDestroy    */ NULL,
    /* xOpen       */ syzyXOpen,
    /* xClose      */ syzyXClose,
    /* xFilter     */ syzyXFilter,
    /* xNext       */ syzyXNext,
    /* xEof        */ syzyXEof,
    /* xColumn     */ syzyXColumn,
    /* xRowid      */ syzyXRowid,
    /* xUpdate     */ NULL,
    /* xBegin      */ NULL,
    /* xSync       */ NULL,
    /* xCommit     */ NULL,
    /* xRollback   */ NULL,
    /* xFindFunction */ NULL,
    /* xRename     */ NULL,
    /* xSavepoint  */ NULL,
    /* xRelease    */ NULL,
    /* xRollbackTo */ NULL,
    /* xShadowName */ NULL,
};

int syzy_register_changes_vtab(sqlite3 *db, const char *feed_path) {
    char *cp = sqlite3_mprintf("%s", feed_path != NULL ? feed_path : "");
    if (cp == NULL) {
        return SQLITE_NOMEM;
    }
    return sqlite3_create_module_v2(db, "syzy_changes",
        &syzy_changes_module, cp, sqlite3_free);
}

// Companion scalars: syzy_my_origin() and syzy_pk_decode(table, pk).
// Both delegate to Go via exports keyed by the connection handle so
// they can pull from the same per-connection provider the vtab uses.

extern void syzyGoMyOrigin(sqlite3 *db, sqlite3_int64 *out, char **err_out);
extern void syzyGoPKDecode(sqlite3 *db, const char *table, int table_len,
    const void *pk, int pk_len, char **out_text, int *out_len,
    int *out_null, char **err_out);

static void syzy_fn_my_origin(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc; (void)argv;
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    sqlite3_int64 origin = 0;
    char *err = NULL;
    syzyGoMyOrigin(db, &origin, &err);
    if (err != NULL) {
        sqlite3_result_error(ctx, err, -1);
        free(err);
        return;
    }
    sqlite3_result_int64(ctx, origin);
}

static void syzy_fn_pk_decode(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc != 2 || sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
        sqlite3_result_error(ctx,
            "syzy_pk_decode: expected (table TEXT, pk BLOB)", -1);
        return;
    }
    int table_len = sqlite3_value_bytes(argv[0]);
    const char *table = (const char *)sqlite3_value_text(argv[0]);

    int pk_type = sqlite3_value_type(argv[1]);
    const void *pk = NULL;
    int pk_len = 0;
    if (pk_type == SQLITE_BLOB) {
        pk = sqlite3_value_blob(argv[1]);
        pk_len = sqlite3_value_bytes(argv[1]);
    } else if (pk_type == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    } else {
        sqlite3_result_error(ctx,
            "syzy_pk_decode: pk must be a BLOB", -1);
        return;
    }

    sqlite3 *db = sqlite3_context_db_handle(ctx);
    char *out_text = NULL;
    int out_len = 0;
    int out_null = 0;
    char *err = NULL;
    syzyGoPKDecode(db, table, table_len, pk, pk_len,
        &out_text, &out_len, &out_null, &err);
    if (err != NULL) {
        sqlite3_result_error(ctx, err, -1);
        free(err);
        return;
    }
    if (out_null != 0) {
        sqlite3_result_null(ctx);
        return;
    }
    // out_text is malloc'd by Go; copy into SQLite's buffer with
    // SQLITE_TRANSIENT so we can free immediately.
    sqlite3_result_text(ctx, out_text, out_len, SQLITE_TRANSIENT);
    free(out_text);
}

int syzy_register_changes_scalars(sqlite3 *db) {
    int rc = sqlite3_create_function_v2(db, "syzy_my_origin", 0,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
        syzy_fn_my_origin, NULL, NULL, NULL);
    if (rc != SQLITE_OK) return rc;
    return sqlite3_create_function_v2(db, "syzy_pk_decode", 2,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
        syzy_fn_pk_decode, NULL, NULL, NULL);
}
