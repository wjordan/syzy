// autoload_shim.c: SYZY_AUTOLOAD registration + path-aware
// auto-extension hook.
//
// Why this file exists, and why these symbols live outside main.go's
// cgo preamble: cgo splits its preamble into several generated .o
// files, so any non-static function in the preamble is emitted
// multiple times and the linker rejects them. A sibling .c file is
// compiled exactly once.
//
// Why we wrap sqlite3_initialize: glibc and musl differ on when
// LD_PRELOAD'd .so constructors fire relative to the main executable's
// DT_NEEDED libs.
//   - glibc: preloads' constructors run AFTER deps, so a constructor
//     doing dlsym(RTLD_DEFAULT, "sqlite3_auto_extension") finds it.
//   - musl: preloads' constructors run BEFORE deps, so the same dlsym
//     returns NULL on Alpine and autoload silently doesn't happen.
// Interposing sqlite3_initialize works on both libcs because by the
// time our wrapper is entered, libsqlite3 is fully present in the
// process (the wrapper is being called *from* libsqlite3's open path).
//
// Two build modes share this file:
//   - monolith (ext/syzy.so): compiled into the Go c-shared object,
//     the sx_syzy_* crossings resolve at link time. The Go runtime
//     starts at .so load in every preloaded process.
//   - lazy shim (ext/syzy-shim.so, -DSYZY_LAZY_SHIM): pure C, no Go
//     linked. The engine (the monolith .so) is dlopen'd only after an
//     open's canonical path matches $SYZY_DB, so preloaded processes
//     that never touch the target DB never start the Go runtime
//     (whose extra threads break musl's setxid broadcast in
//     priv-dropping entrypoints like setpriv/su-exec).

#define _GNU_SOURCE
#include <ctype.h>
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "syzy_app_vfs.h"
#include "syzy_sqlite.h"

static int sx_dbg(void) {
    static int v = -1;
    if (v == -1) v = getenv("SYZY_DEBUG") ? 1 : 0;
    return v;
}

#ifndef SYZY_LAZY_SHIM
// sqlite3_syzy_init is the Go-exported extension entry. cgo emits it
// as a void* pApi (matching unsafe.Pointer); we accept the same so
// the call survives ABI checking.
extern int sqlite3_syzy_init(sqlite3 *db, char **pzErrMsg, void *pApi);
#else
// ---- lazy engine loading (SYZY_LAZY_SHIM) ----

static int sx_engine_loaded = 0; // atomic; set only after all symbols resolve
static pthread_mutex_t sx_engine_mu = PTHREAD_MUTEX_INITIALIZER;
static int (*sx_engine_init)(sqlite3 *, char **, void *);
static char *(*sx_engine_preprocess)(sqlite3 *, char *, int, int *);
static char *(*sx_engine_preprocess_exec)(sqlite3 *, char *);
static void (*sx_engine_reassert)(sqlite3 *);
static char sx_engine_errbuf[512];

static int sx_engine_ready(void) {
    return __atomic_load_n(&sx_engine_loaded, __ATOMIC_ACQUIRE);
}

// sx_engine_load dlopen's the engine (RTLD_LOCAL keeps its embedded
// copies of these interposers out of the global lookup scope) and
// resolves the four crossings. Returns NULL once loaded, else an
// error message; retried on the next matching open. The release store
// publishes the resolved pointers to sx_engine_ready's acquire load.
static const char *sx_engine_load(void) {
    if (sx_engine_ready()) return NULL;
    pthread_mutex_lock(&sx_engine_mu);
    if (sx_engine_ready()) {
        pthread_mutex_unlock(&sx_engine_mu);
        return NULL;
    }
    const char *path = getenv("SYZY_ENGINE");
    if (path == NULL || path[0] == '\0') path = "/usr/local/lib/syzy-engine.so";
    const char *err = NULL;
    // A failed dlsym leaks the handle deliberately: dlclose of a Go
    // c-shared object is unsafe (the runtime cannot be unloaded).
    void *h = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (h == NULL) {
        snprintf(sx_engine_errbuf, sizeof(sx_engine_errbuf),
                "syzy-shim: dlopen %s: %s", path, dlerror());
        err = sx_engine_errbuf;
    } else {
        struct { const char *name; void **slot; } syms[] = {
            {"sqlite3_syzy_init", (void **)&sx_engine_init},
            {"sx_syzy_preprocess", (void **)&sx_engine_preprocess},
            {"sx_syzy_preprocess_exec", (void **)&sx_engine_preprocess_exec},
            {"sx_syzy_reassert_wal_hook", (void **)&sx_engine_reassert},
        };
        for (size_t k = 0; k < sizeof(syms) / sizeof(syms[0]); k++) {
            *syms[k].slot = dlsym(h, syms[k].name);
            if (*syms[k].slot == NULL) {
                snprintf(sx_engine_errbuf, sizeof(sx_engine_errbuf),
                        "syzy-shim: %s: missing symbol %s", path, syms[k].name);
                err = sx_engine_errbuf;
                break;
            }
        }
    }
    if (err == NULL) {
        __atomic_store_n(&sx_engine_loaded, 1, __ATOMIC_RELEASE);
        if (sx_dbg()) fprintf(stderr, "syzy-shim: engine loaded: %s\n", path);
    }
    pthread_mutex_unlock(&sx_engine_mu);
    return err;
}

// Same-name stand-ins for the Go crossings the interposers call.
// Until the engine loads, no attach can exist, so the Go side would
// return NULL / no-op anyway — skip the crossing entirely.
static char *sx_syzy_preprocess(sqlite3 *db, char *zSql, int nByte, int *pzConsumed) {
    if (!sx_engine_ready()) return NULL;
    return sx_engine_preprocess(db, zSql, nByte, pzConsumed);
}
static char *sx_syzy_preprocess_exec(sqlite3 *db, char *zSql) {
    if (!sx_engine_ready()) return NULL;
    return sx_engine_preprocess_exec(db, zSql);
}
static void sx_syzy_reassert_wal_hook(sqlite3 *db) {
    if (sx_engine_ready()) sx_engine_reassert(db);
}
#endif // SYZY_LAZY_SHIM

// syzy_autoload_hook is the path-aware wrapper registered with
// sqlite3_auto_extension. SQLite calls it on every fresh sqlite3* in
// the process. We only attach when the open's canonical path matches
// $SYZY_DB; any other open is a one-realpath-and-compare no-op so we
// never accidentally engage syzy on unrelated DBs.
int syzy_autoload_hook(sqlite3 *db, char **pzErrMsg,
        const sqlite3_api_routines *pApi) {
    if (pApi == NULL || pApi->db_filename == NULL) return SQLITE_OK;
    const char *fn = pApi->db_filename(db, "main");
    const char *target = getenv("SYZY_DB");
    if (fn == NULL || target == NULL) return SQLITE_OK;
    char *target_real = realpath(target, NULL);
    if (target_real == NULL) return SQLITE_OK;
    int match = strcmp(fn, target_real) == 0;
    free(target_real);
    if (!match) return SQLITE_OK;
#ifdef SYZY_LAZY_SHIM
    // Engine load failure must fail the open LOUD: a silent
    // fallthrough would mean unreplicated writes to the shared DB.
    const char *err = sx_engine_load();
    if (err != NULL) {
        fprintf(stderr, "%s\n", err);
        if (pzErrMsg != NULL && pApi->mprintf != NULL) {
            *pzErrMsg = pApi->mprintf("%s", err);
        }
        return SQLITE_ERROR;
    }
    return sx_engine_init(db, pzErrMsg, (void *)pApi);
#else
    return sqlite3_syzy_init(db, pzErrMsg, (void *)pApi);
#endif
}

// The interposers below must be referenced by their bare sqlite3_xxx
// names so the dynamic linker can resolve client-app calls to them.
// syzy_sqlite.h's macros would expand them to sqlite3_api->open*
// dispatches, so strip just those mappings here.
#undef sqlite3_initialize
#undef sqlite3_open
#undef sqlite3_open_v2
#undef sqlite3_open16
#undef sqlite3_prepare
#undef sqlite3_prepare_v2
#undef sqlite3_prepare_v3
#undef sqlite3_exec
#undef sqlite3_step
#undef sqlite3_finalize
#undef sqlite3_wal_autocheckpoint

// sx_autoload_registered uses atomic CAS so concurrent first opens
// from different threads can't both call sqlite3_auto_extension
// (which appends to a list, so a double-register would re-fire the
// hook on every connection).
static int sx_autoload_registered = 0;

static void sx_try_register(void) {
    int expected = 0;
    if (!__atomic_compare_exchange_n(&sx_autoload_registered, &expected, 1,
            0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        return;
    }
    const char *enable = getenv("SYZY_AUTOLOAD");
    if (enable == NULL || strcmp(enable, "1") != 0) return;
    typedef int (*xAutoExt)(int(*)(sqlite3*, char**, const sqlite3_api_routines*));
    xAutoExt fn = (xAutoExt)dlsym(RTLD_DEFAULT, "sqlite3_auto_extension");
    if (fn == NULL) {
        // Rewind the flag: the registration didn't happen, a later
        // interposer pass should retry once libsqlite3 is mapped.
        __atomic_store_n(&sx_autoload_registered, 0, __ATOMIC_RELEASE);
        if (sx_dbg()) fprintf(stderr, "syzy-shim: sqlite3_auto_extension not yet visible\n");
        return;
    }
    fn(syzy_autoload_hook);
    if (sx_dbg()) fprintf(stderr, "syzy-shim: registered auto-extension\n");
    // Register the parkable wrapper VFS ("syzy-app", app_vfs.c) so the
    // open interposers can steer $SYZY_DB opens onto it. Non-default:
    // opens of any other database are untouched. SYZY_APP_PARK=0 is
    // the escape hatch back to plain unix-VFS opens.
    const char *park = getenv("SYZY_APP_PARK");
    if (getenv("SYZY_DB") != NULL && (park == NULL || strcmp(park, "0") != 0)) {
        if (sx_app_vfs_register() && sx_dbg()) {
            fprintf(stderr, "syzy-shim: registered syzy-app vfs\n");
        }
    }
}

// Per-thread re-entry guard for the interposers. libsqlite3's own
// sqlite3_initialize calls into other libsqlite3 symbols
// (sqlite3_os_init, sqlite3_pcache_initialize, ...) whose internal
// calls go back through the PLT and reach our LD_PRELOAD'd interposer
// again. Without the guard we infinitely recurse on the same thread.
// The same risk applies in principle to sqlite3_open / sqlite3_open_v2,
// so all three share the guard.
static __thread int sx_in_shim = 0;

int sqlite3_initialize(void) {
    typedef int (*xInit)(void);
    static xInit real_init = NULL;
    // Resolve real_init BEFORE consulting sx_in_shim. The guard
    // exists to skip re-running sx_try_register on recursive entry,
    // not to skip calling real_init itself. real_init is idempotent
    // (libsqlite3's sqlite3_initialize early-returns SQLITE_OK once
    // its isInit flag is set), so calling it on recursive entry is
    // safe and necessary — when sx_in_shim was set by a different
    // outer shim (e.g. sqlite3_open_v2 → sx_try_register →
    // sqlite3_auto_extension → sqlite3_initialize), real_init may
    // never have been resolved, and the old early-return-zero path
    // told libsqlite3 init succeeded without actually running it.
    // Libsqlite3 would then walk a NULL global config and crash with
    // a NULL function pointer (IP=0).
    if (real_init == NULL) {
        real_init = (xInit)dlsym(RTLD_NEXT, "sqlite3_initialize");
    }
    if (real_init == NULL) return 0;
    if (sx_in_shim) return real_init();
    sx_in_shim = 1;
    int rc = real_init();
    if (rc == 0) sx_try_register();
    sx_in_shim = 0;
    return rc;
}

// sx_post_open runs after a successful interposed open. openDatabase
// calls sqlite3_wal_autocheckpoint AFTER auto-extension load, which
// silently replaces the producer wal_hook the attach just installed;
// re-assert it so journaling and DDL intent resolution actually run.
// (Go-side no-op for handles without a syzy attachment.)
#ifndef SYZY_LAZY_SHIM
extern void sx_syzy_reassert_wal_hook(sqlite3 *db);
#endif

static void sx_post_open(int rc, sqlite3 **ppDb) {
    if (rc == 0 && ppDb != NULL && *ppDb != NULL) {
        sx_syzy_reassert_wal_hook(*ppDb);
    }
}

// sqlite3_open_v2 / sqlite3_open are interposed as a fallback for the
// case where sqlite3_initialize is reached via a libsqlite3-internal
// call (which does not go through the PLT and so bypasses our shim).
// Every user-facing client must call one of these; they each invoke
// sqlite3_initialize internally before opening, so by the time control
// returns to user code, our shim has had a chance to register.
int sqlite3_open_v2(const char *filename, sqlite3 **ppDb,
        int flags, const char *zVfs) {
    typedef int (*xOpenV2)(const char *, sqlite3 **, int, const char *);
    static xOpenV2 real_open_v2 = NULL;
    if (real_open_v2 == NULL) {
        real_open_v2 = (xOpenV2)dlsym(RTLD_NEXT, "sqlite3_open_v2");
        if (real_open_v2 == NULL) return 1; // SQLITE_ERROR
    }
    if (!sx_in_shim) {
        sx_in_shim = 1;
        sx_try_register();
        // Steer the target DB onto the parkable wrapper VFS. Only when
        // the caller didn't pick a VFS itself; an explicit choice wins
        // (the open then just isn't parkable).
        if (zVfs == NULL && sx_app_vfs_steer(filename)) {
            zVfs = "syzy-app";
            if (sx_dbg()) fprintf(stderr, "syzy-shim: steering open_v2 onto syzy-app\n");
        }
        sx_in_shim = 0;
    }
    int rc = real_open_v2(filename, ppDb, flags, zVfs);
    sx_post_open(rc, ppDb);
    return rc;
}

int sqlite3_open(const char *filename, sqlite3 **ppDb) {
    typedef int (*xOpen)(const char *, sqlite3 **);
    typedef int (*xOpenV2)(const char *, sqlite3 **, int, const char *);
    static xOpen real_open = NULL;
    static xOpenV2 real_open_v2 = NULL;
    if (real_open == NULL) {
        real_open = (xOpen)dlsym(RTLD_NEXT, "sqlite3_open");
        if (real_open == NULL) return 1;
    }
    int steer = 0;
    if (!sx_in_shim) {
        sx_in_shim = 1;
        sx_try_register();
        steer = sx_app_vfs_steer(filename);
        sx_in_shim = 0;
    }
    if (steer) {
        // sqlite3_open has no VFS parameter; reroute through open_v2
        // with sqlite3_open's documented flags to pick the wrapper.
        if (sx_dbg()) fprintf(stderr, "syzy-shim: steering open onto syzy-app\n");
        if (real_open_v2 == NULL) {
            real_open_v2 = (xOpenV2)dlsym(RTLD_NEXT, "sqlite3_open_v2");
        }
        if (real_open_v2 != NULL) {
            int rc = real_open_v2(filename, ppDb,
                    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "syzy-app");
            sx_post_open(rc, ppDb);
            return rc;
        }
    }
    int rc = real_open(filename, ppDb);
    sx_post_open(rc, ppDb);
    return rc;
}

int sqlite3_open16(const void *filename, sqlite3 **ppDb) {
    typedef int (*xOpen16)(const void *, sqlite3 **);
    static xOpen16 real_open16 = NULL;
    if (real_open16 == NULL) {
        real_open16 = (xOpen16)dlsym(RTLD_NEXT, "sqlite3_open16");
        if (real_open16 == NULL) return 1;
    }
    if (!sx_in_shim) {
        sx_in_shim = 1;
        sx_try_register();
        sx_in_shim = 0;
    }
    int rc = real_open16(filename, ppDb);
    sx_post_open(rc, ppDb);
    return rc;
}

// ---- SQL-rewrite interposers (prepare family, exec, step) ----
//
// The producer's SQL preprocessor (rowid-alias rewrite, ADD COLUMN IF
// NOT EXISTS) hooks sqlitebridge.Conn.Prepare/Exec — paths the host
// app's own sqlite3_prepare*/sqlite3_exec calls never touch. These
// interposers route app SQL on attached connections through the Go
// preprocessor (sx_syzy_preprocess*, exported from
// preprocess_export.go) before the real SQLite compiles it.
//
// Scope and guards:
//   - Only statements whose first token is CREATE/ALTER/DROP cross
//     into Go (sx_leading_ddl_keyword); DML/SELECT pay one short
//     C-side scan.
//   - sx_step_depth skips any prepare/exec issued while a sqlite3_step
//     is executing on the same thread. App code can't run prepare
//     mid-step (except inside UDF/progress callbacks, where skipping
//     is the safe choice); a mid-step prepare is SQLite-internal —
//     e.g. fts5 creating its 'x_content(id INTEGER PRIMARY KEY, ...)'
//     shadow tables, which must NOT be rewritten.
//   - Go returning NULL means "use the original text"; the interposer
//     can change what SQLite compiles, never whether it compiles.
//   - sqlite3_prepare16* are NOT interposed: the rewrite is UTF-8-only
//     and not defining them leaves UTF-16 callers on the real symbols.
//
// pzTail: the rewritten text covers only the FIRST statement of the
// input; Go also returns the byte offset where the remainder begins in
// the ORIGINAL text, and *pzTail is pointed there — so drivers that
// loop on the tail (e.g. multi-statement Exec) keep working against
// their own buffer, with each statement rewritten on its own
// iteration.

#ifndef SYZY_LAZY_SHIM
extern char *sx_syzy_preprocess(sqlite3 *db, char *zSql, int nByte, int *pzConsumed);
extern char *sx_syzy_preprocess_exec(sqlite3 *db, char *zSql);
#endif

static __thread int sx_step_depth = 0;

// sx_leading_ddl_keyword reports whether the first token of z is
// CREATE, ALTER, or DROP, after whitespace and an optional UTF-8 BOM.
// limit < 0 means NUL-terminated; the scan never reads past a NUL or
// past limit bytes, and never scans more than the leading token — so
// the non-DDL hot path (every SELECT/INSERT prepare in the process)
// pays a few bytes of scanning and no strlen. Comment-prefixed DDL is
// intentionally not matched — mirroring the Go side's ddlKeywordRE
// (ddl_rewrite.go) — and flows through unrewritten to the admission
// backstop.
static int sx_leading_ddl_keyword(const char *z, int limit) {
    size_t max = (limit < 0) ? (size_t)-1 : (size_t)limit;
    size_t i = 0;
    if (max >= 3 && (unsigned char)z[0] == 0xEF &&
        (unsigned char)z[1] == 0xBB && (unsigned char)z[2] == 0xBF) {
        i = 3;
    }
    while (i < max && (z[i] == ' ' || z[i] == '\t' || z[i] == '\n' ||
                       z[i] == '\r' || z[i] == '\f' || z[i] == '\v')) {
        i++;
    }
    static const struct { const char *kw; size_t n; } kws[] = {
        {"create", 6}, {"alter", 5}, {"drop", 4},
    };
    for (size_t k = 0; k < sizeof(kws) / sizeof(kws[0]); k++) {
        size_t n = kws[k].n;
        if (max - i < n || strncasecmp(z + i, kws[k].kw, n) != 0) continue;
        // strncasecmp stops at a NUL inside z, so a match guarantees n
        // real bytes. Reject "createx"-style identifiers.
        if (max - i == n || z[i + n] == '\0') return 1;
        char c = z[i + n];
        int ident = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') || c == '_';
        if (!ident) return 1;
    }
    return 0;
}

// ---- wal_autocheckpoint re-clobber guard ----
//
// sqlite3_wal_autocheckpoint installs SQLite's internal checkpoint
// wal_hook, silently replacing the producer's — the same clobber
// sx_post_open repairs for openDatabase, but reachable at any time
// via the C API or `PRAGMA wal_autocheckpoint` (which many ORMs and
// backup tools issue). Three routes, each re-asserting afterwards:
//   - the sqlite3_wal_autocheckpoint interposer (direct API calls,
//     plus the pragma on builds whose internal calls cross the PLT),
//   - sqlite3_exec strings mentioning the pragma,
//   - prepare+step: the pragma takes effect at step, so prepare
//     records the stmt in a small watch set and the first step
//     re-asserts (a reset-and-re-stepped pragma stmt is no longer
//     watched — acceptable: the first step already re-clobbered and
//     was repaired; re-running it installs the same internal hook
//     with the same nFrame, which the first repair already replaced).
// An app installing its OWN sqlite3_wal_hook is a real conflict we
// deliberately do not paper over.

#define SX_WATCH_MAX 8
static pthread_mutex_t sx_watch_mu = PTHREAD_MUTEX_INITIALIZER;
static sqlite3_stmt *sx_watch[SX_WATCH_MAX];
static int sx_watch_count = 0; // read unlocked as a fast-path hint

static void sx_watch_add(sqlite3_stmt *st) {
    pthread_mutex_lock(&sx_watch_mu);
    for (int k = 0; k < SX_WATCH_MAX; k++) {
        if (sx_watch[k] == NULL) {
            sx_watch[k] = st;
            __atomic_fetch_add(&sx_watch_count, 1, __ATOMIC_RELEASE);
            break;
        }
    }
    // Overflow: silently unwatched. 8 concurrently prepared,
    // not-yet-stepped wal_autocheckpoint pragmas would be absurd.
    pthread_mutex_unlock(&sx_watch_mu);
}

static int sx_watch_take(sqlite3_stmt *st) {
    pthread_mutex_lock(&sx_watch_mu);
    int hit = 0;
    for (int k = 0; k < SX_WATCH_MAX; k++) {
        if (sx_watch[k] == st) {
            sx_watch[k] = NULL;
            __atomic_fetch_sub(&sx_watch_count, 1, __ATOMIC_RELEASE);
            hit = 1;
            break;
        }
    }
    pthread_mutex_unlock(&sx_watch_mu);
    return hit;
}

// Stable CREATE rewrites carry an unguessable marker that is authorized for
// exactly one sqlite3_step. Keep prepared statements marked until finalize so
// reset cannot resurrect a dropped table with the same catalog identity.
typedef struct sx_once_stmt {
    sqlite3_stmt *stmt;
    int stepped;
    struct sx_once_stmt *next;
} sx_once_stmt;

static pthread_mutex_t sx_once_mu = PTHREAD_MUTEX_INITIALIZER;
static sx_once_stmt *sx_once_head = NULL;
static int sx_once_count = 0; // read unlocked as a non-CREATE fast-path hint

static int sx_once_add(sqlite3_stmt *stmt) {
    sx_once_stmt *entry = malloc(sizeof(*entry));
    if (entry == NULL) return 0;
    entry->stmt = stmt;
    entry->stepped = 0;
    pthread_mutex_lock(&sx_once_mu);
    entry->next = sx_once_head;
    sx_once_head = entry;
    __atomic_fetch_add(&sx_once_count, 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&sx_once_mu);
    return 1;
}

static int sx_once_replayed(sqlite3_stmt *stmt) {
    int replayed = 0;
    pthread_mutex_lock(&sx_once_mu);
    for (sx_once_stmt *entry = sx_once_head; entry != NULL; entry = entry->next) {
        if (entry->stmt != stmt) continue;
        replayed = entry->stepped;
        entry->stepped = 1;
        break;
    }
    pthread_mutex_unlock(&sx_once_mu);
    return replayed;
}

static void sx_once_remove(sqlite3_stmt *stmt) {
    pthread_mutex_lock(&sx_once_mu);
    sx_once_stmt **link = &sx_once_head;
    while (*link != NULL) {
        if ((*link)->stmt == stmt) {
            sx_once_stmt *entry = *link;
            *link = entry->next;
            free(entry);
            __atomic_fetch_sub(&sx_once_count, 1, __ATOMIC_RELEASE);
            break;
        }
        link = &(*link)->next;
    }
    pthread_mutex_unlock(&sx_once_mu);
}

static int sx_real_finalize(sqlite3_stmt *stmt) {
    typedef int (*xFinalize)(sqlite3_stmt *);
    static xFinalize real = NULL;
    if (real == NULL) real = (xFinalize)dlsym(RTLD_NEXT, "sqlite3_finalize");
    if (real == NULL) return SQLITE_MISUSE;
    return real(stmt);
}

// Bounded case-insensitive substring scan. strcasestr needs a
// NUL-terminated haystack; prepare's nByte semantics don't supply one.
static int sx_contains_ci(const char *z, size_t max, const char *needle) {
    size_t nlen = strlen(needle);
    for (size_t i = 0; i < max && z[i] != '\0'; i++) {
        size_t j = 0;
        while (j < nlen && i + j < max && z[i + j] != '\0' &&
               tolower((unsigned char)z[i + j]) == (unsigned char)needle[j]) {
            j++;
        }
        if (j == nlen) return 1;
    }
    return 0;
}

// sx_pragma_wal_autocheckpoint: leading token is PRAGMA and the
// statement mentions wal_autocheckpoint. Pragma statements are short,
// so the substring scan after the cheap leading-token check is fine.
static int sx_pragma_wal_autocheckpoint(const char *z, int limit) {
    size_t max = (limit < 0) ? (size_t)-1 : (size_t)limit;
    size_t i = 0;
    if (max >= 3 && (unsigned char)z[0] == 0xEF &&
        (unsigned char)z[1] == 0xBB && (unsigned char)z[2] == 0xBF) {
        i = 3;
    }
    while (i < max && (z[i] == ' ' || z[i] == '\t' || z[i] == '\n' ||
                       z[i] == '\r' || z[i] == '\f' || z[i] == '\v')) {
        i++;
    }
    if (max - i < 6 || strncasecmp(z + i, "pragma", 6) != 0) return 0;
    return sx_contains_ci(z + i + 6, max - i - 6, "wal_autocheckpoint");
}

static void sx_reassert_hook(sqlite3 *db) {
    if (db == NULL || sx_in_shim) return;
    sx_in_shim = 1;
    sx_syzy_reassert_wal_hook(db);
    sx_in_shim = 0;
}

// PRAGMA wal_autocheckpoint funnels into this API inside libsqlite3;
// on builds whose internal calls cross the PLT we catch the pragma
// here too, but the exec/step routes don't rely on that.
int sqlite3_wal_autocheckpoint(sqlite3 *db, int nFrame) {
    typedef int (*xWalAC)(sqlite3 *, int);
    static xWalAC real = NULL;
    if (real == NULL) {
        real = (xWalAC)dlsym(RTLD_NEXT, "sqlite3_wal_autocheckpoint");
        if (real == NULL) return 1; // SQLITE_ERROR
    }
    int rc = real(db, nFrame);
    sx_reassert_hook(db);
    return rc;
}

// sx_prep names the resolved prepare implementation for
// sx_prepare_common: the function pointer plus whether it takes the
// prepare_v3 prepFlags parameter.
typedef struct {
    void *fn;
    int v3;
    unsigned int flags;
} sx_prep;

static int sx_prep_call(const sx_prep *p, sqlite3 *db, const char *zSql,
        int nByte, sqlite3_stmt **ppStmt, const char **pzTail) {
    if (p->v3) {
        typedef int (*fn)(sqlite3 *, const char *, int, unsigned int,
                sqlite3_stmt **, const char **);
        return ((fn)p->fn)(db, zSql, nByte, p->flags, ppStmt, pzTail);
    }
    typedef int (*fn)(sqlite3 *, const char *, int,
            sqlite3_stmt **, const char **);
    return ((fn)p->fn)(db, zSql, nByte, ppStmt, pzTail);
}

// sx_prepare_common is the shared interpose body for the prepare
// family: prefilter, Go rewrite crossing, tail fix-up.
static int sx_prepare_common(const sx_prep *p, sqlite3 *db, const char *zSql,
        int nByte, sqlite3_stmt **ppStmt, const char **pzTail) {
    if (sx_in_shim || sx_step_depth > 0 || db == NULL || zSql == NULL) {
        return sx_prep_call(p, db, zSql, nByte, ppStmt, pzTail);
    }
    if (!sx_leading_ddl_keyword(zSql, nByte)) {
        if (sx_pragma_wal_autocheckpoint(zSql, nByte)) {
            // The pragma re-installs SQLite's checkpoint wal_hook when
            // it EXECUTES; watch the stmt so step can re-assert.
            int rc = sx_prep_call(p, db, zSql, nByte, ppStmt, pzTail);
            if (rc == 0 && ppStmt != NULL && *ppStmt != NULL) {
                sx_watch_add(*ppStmt);
            }
            return rc;
        }
        return sx_prep_call(p, db, zSql, nByte, ppStmt, pzTail);
    }
    // DDL keyword present: now pay for the exact length (nByte
    // semantics per sqlite3_prepare docs: a NUL terminates the input
    // even when nByte is positive).
    size_t len = (nByte < 0) ? strlen(zSql) : strnlen(zSql, (size_t)nByte);
    if (len == 0 || len > 0x7fffffff) {
        return sx_prep_call(p, db, zSql, nByte, ppStmt, pzTail);
    }
    int consumed = 0;
    sx_in_shim = 1;
    char *rw = sx_syzy_preprocess(db, (char *)zSql, (int)len, &consumed);
    sx_in_shim = 0;
    if (rw == NULL) {
        return sx_prep_call(p, db, zSql, nByte, ppStmt, pzTail);
    }
    if (sx_dbg()) fprintf(stderr, "syzy-shim: rewrote DDL for prepare: %s\n", rw);
    int stable_create = sx_contains_ci(rw, strlen(rw), "/*syzy-stable-create:");
    int rc = sx_prep_call(p, db, rw, -1, ppStmt, NULL);
    if (rc == SQLITE_OK && stable_create && ppStmt != NULL && *ppStmt != NULL &&
            !sx_once_add(*ppStmt)) {
        sx_real_finalize(*ppStmt);
        *ppStmt = NULL;
        rc = SQLITE_NOMEM;
    }
    // Point the caller's tail into ITS buffer at the start of the
    // remainder (consumed <= len <= nByte keeps this in bounds), so
    // statement-at-a-time drivers loop on to the next statement.
    if (pzTail != NULL) *pzTail = zSql + consumed;
    free(rw);
    return rc;
}

int sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte,
        sqlite3_stmt **ppStmt, const char **pzTail) {
    static void *real = NULL;
    if (real == NULL) {
        real = dlsym(RTLD_NEXT, "sqlite3_prepare_v2");
        if (real == NULL) return 1; // SQLITE_ERROR
    }
    sx_prep p = {real, 0, 0};
    return sx_prepare_common(&p, db, zSql, nByte, ppStmt, pzTail);
}

int sqlite3_prepare(sqlite3 *db, const char *zSql, int nByte,
        sqlite3_stmt **ppStmt, const char **pzTail) {
    static void *real = NULL;
    if (real == NULL) {
        real = dlsym(RTLD_NEXT, "sqlite3_prepare");
        if (real == NULL) return 1;
    }
    sx_prep p = {real, 0, 0};
    return sx_prepare_common(&p, db, zSql, nByte, ppStmt, pzTail);
}

// If the underlying libsqlite3 predates prepare_v3 (< 3.20), fall back
// to prepare_v2 dropping prepFlags — they are performance hints
// (PERSISTENT) or hardening (NO_VTAB), never semantics. An app calling
// v3 against such a lib could only have resolved OUR symbol anyway.
int sqlite3_prepare_v3(sqlite3 *db, const char *zSql, int nByte,
        unsigned int prepFlags, sqlite3_stmt **ppStmt, const char **pzTail) {
    static void *real_v3 = NULL;
    static void *real_v2 = NULL;
    if (real_v3 == NULL && real_v2 == NULL) {
        real_v3 = dlsym(RTLD_NEXT, "sqlite3_prepare_v3");
        if (real_v3 == NULL) {
            real_v2 = dlsym(RTLD_NEXT, "sqlite3_prepare_v2");
            if (real_v2 == NULL) return 1;
        }
    }
    sx_prep p = {real_v3 != NULL ? real_v3 : real_v2,
                 real_v3 != NULL, prepFlags};
    return sx_prepare_common(&p, db, zSql, nByte, ppStmt, pzTail);
}

// sqlite3_exec consumes a whole (possibly multi-statement) string in
// one call. Its internal per-statement prepare calls reach our
// prepare_v2 interposer only on builds where libsqlite3-internal calls
// go through the PLT (no -Bsymbolic-functions /
// -fno-semantic-interposition), so the rewrite cannot rely on that:
// the whole string is rewritten up front, and sx_in_shim is held
// across the real exec so PLT-visible internal prepares don't rewrite
// a second time.
int sqlite3_exec(sqlite3 *db, const char *zSql,
        int (*cb)(void *, int, char **, char **), void *arg, char **errmsg) {
    typedef int (*xExec)(sqlite3 *, const char *,
            int (*)(void *, int, char **, char **), void *, char **);
    static xExec real = NULL;
    if (real == NULL) {
        real = (xExec)dlsym(RTLD_NEXT, "sqlite3_exec");
        if (real == NULL) return 1;
    }
    if (sx_in_shim || sx_step_depth > 0 || db == NULL || zSql == NULL) {
        return real(db, zSql, cb, arg, errmsg);
    }
    // The pragma replaces the producer wal_hook as it executes;
    // re-assert after the real exec. It can share the string with DDL,
    // so this composes with the rewrite below rather than bypassing
    // it. Known limitation: a replicated write that commits AFTER the
    // pragma WITHIN the same exec string runs with the hook clobbered
    // (the re-assert lands post-exec) — per-statement repair would
    // mean reimplementing exec's loop. Real apps send setup pragmas
    // as their own exec call, which this handles.
    int reassert = strcasestr(zSql, "wal_autocheckpoint") != NULL;
    int rc;
    // Cheap prefilter: a DDL keyword anywhere in the string (statements
    // after the first make a leading-token check insufficient). False
    // positives just cost the Go crossing, which returns NULL.
    if (strcasestr(zSql, "create") == NULL &&
        strcasestr(zSql, "alter") == NULL &&
        strcasestr(zSql, "drop") == NULL) {
        rc = real(db, zSql, cb, arg, errmsg);
        if (reassert) sx_reassert_hook(db);
        return rc;
    }
    sx_in_shim = 1;
    char *rw = sx_syzy_preprocess_exec(db, (char *)zSql);
    sx_in_shim = 0;
    if (rw == NULL) {
        rc = real(db, zSql, cb, arg, errmsg);
    } else {
        if (sx_dbg()) fprintf(stderr, "syzy-shim: rewrote DDL for exec: %s\n", rw);
        sx_in_shim = 1;
        rc = real(db, rw, cb, arg, errmsg);
        sx_in_shim = 0;
        free(rw);
    }
    if (reassert) sx_reassert_hook(db);
    return rc;
}

// sqlite3_step maintains sx_step_depth (see the guard rationale above) and
// enforces one-use stable CREATE statements. The depth must track the OUTER,
// app-issued step; SQLite-internal nested steps may or may not reach this
// interposer depending on build flags, and either way depth stays > 0.
int sqlite3_step(sqlite3_stmt *pStmt) {
    typedef int (*xStep)(sqlite3_stmt *);
    static xStep real = NULL;
    if (real == NULL) {
        real = (xStep)dlsym(RTLD_NEXT, "sqlite3_step");
        if (real == NULL) return 21; // SQLITE_MISUSE
    }
    if (__atomic_load_n(&sx_once_count, __ATOMIC_ACQUIRE) > 0 &&
            pStmt != NULL && sx_once_replayed(pStmt)) return SQLITE_AUTH;
    sx_step_depth++;
    int rc = real(pStmt);
    sx_step_depth--;
    // A watched wal_autocheckpoint pragma just executed: re-assert the
    // producer wal_hook it replaced. The unlocked count read keeps the
    // universal step hot path at one predictable branch.
    if (__atomic_load_n(&sx_watch_count, __ATOMIC_ACQUIRE) > 0 &&
        pStmt != NULL && sx_watch_take(pStmt)) {
        typedef sqlite3 *(*xDbHandle)(sqlite3_stmt *);
        static xDbHandle dbh = NULL;
        if (dbh == NULL) dbh = (xDbHandle)dlsym(RTLD_NEXT, "sqlite3_db_handle");
        if (dbh != NULL) sx_reassert_hook(dbh(pStmt));
    }
    return rc;
}

int sqlite3_finalize(sqlite3_stmt *pStmt) {
    if (pStmt != NULL) {
        if (__atomic_load_n(&sx_once_count, __ATOMIC_ACQUIRE) > 0) {
            sx_once_remove(pStmt);
        }
        if (__atomic_load_n(&sx_watch_count, __ATOMIC_ACQUIRE) > 0) {
            sx_watch_take(pStmt);
        }
    }
    return sx_real_finalize(pStmt);
}
