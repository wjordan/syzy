// Extension-only: dlsym-resolved wrappers for the sqlite3_preupdate_*
// family. The SQLite project intentionally keeps preupdate out of
// sqlite3_api_routines (SQLITE_ENABLE_PREUPDATE_HOOK is compile-time
// only), but the host binary's libsqlite3 still has the actual
// symbols when built with -DSQLITE_ENABLE_PREUPDATE_HOOK. We grab
// them via dlsym(RTLD_DEFAULT) at sqlite3_syzy_init time; failure
// surfaces a loud error and the .load fails.
//
// Linked builds get the inline aliases in syzy_sqlite.h instead and
// don't compile this file (whole TU is no-op without SYZY_EXTENSION).

#ifdef SYZY_EXTENSION

#include <stddef.h>
#include <dlfcn.h>

#include "syzy_sqlite.h"

static int (*p_preupdate_count)(sqlite3 *);
static int (*p_preupdate_depth)(sqlite3 *);
static int (*p_preupdate_blobwrite)(sqlite3 *);
static int (*p_preupdate_old)(sqlite3 *, int, sqlite3_value **);
static int (*p_preupdate_new)(sqlite3 *, int, sqlite3_value **);
static void *(*p_preupdate_hook)(sqlite3 *,
    void (*)(void *, sqlite3 *, int, char const *, char const *, sqlite3_int64, sqlite3_int64),
    void *);

const char *sx_resolve_preupdate(void) {
    p_preupdate_count = dlsym(RTLD_DEFAULT, "sqlite3_preupdate_count");
    if (!p_preupdate_count) {
        return "sqlite3_preupdate_count missing — host sqlite3 was not built with SQLITE_ENABLE_PREUPDATE_HOOK";
    }
    p_preupdate_depth = dlsym(RTLD_DEFAULT, "sqlite3_preupdate_depth");
    if (!p_preupdate_depth) {
        return "sqlite3_preupdate_depth missing";
    }
    // blobwrite was added in 3.36 and may legitimately be absent on
    // older hosts. Treat as optional — wrapper returns 0 (no blob
    // write) when unresolved.
    p_preupdate_blobwrite = dlsym(RTLD_DEFAULT, "sqlite3_preupdate_blobwrite");
    p_preupdate_old = dlsym(RTLD_DEFAULT, "sqlite3_preupdate_old");
    if (!p_preupdate_old) {
        return "sqlite3_preupdate_old missing";
    }
    p_preupdate_new = dlsym(RTLD_DEFAULT, "sqlite3_preupdate_new");
    if (!p_preupdate_new) {
        return "sqlite3_preupdate_new missing";
    }
    p_preupdate_hook = dlsym(RTLD_DEFAULT, "sqlite3_preupdate_hook");
    if (!p_preupdate_hook) {
        return "sqlite3_preupdate_hook missing";
    }
    return NULL;
}

int sx_preupdate_count(sqlite3 *db) { return p_preupdate_count(db); }
int sx_preupdate_depth(sqlite3 *db) { return p_preupdate_depth(db); }
int sx_preupdate_blobwrite(sqlite3 *db) {
    return p_preupdate_blobwrite ? p_preupdate_blobwrite(db) : 0;
}
int sx_preupdate_old(sqlite3 *db, int i, sqlite3_value **v) {
    return p_preupdate_old(db, i, v);
}
int sx_preupdate_new(sqlite3 *db, int i, sqlite3_value **v) {
    return p_preupdate_new(db, i, v);
}
void *sx_preupdate_hook(sqlite3 *db,
    void (*cb)(void *, sqlite3 *, int, char const *, char const *, sqlite3_int64, sqlite3_int64),
    void *p) {
    return p_preupdate_hook(db, cb, p);
}

#endif // SYZY_EXTENSION
