// ext-rewrite-client: minimal dynamically-linked SQLite client used by
// ext-rewrite-test.sh to exercise the LD_PRELOAD prepare/exec
// interposers exactly as a real app would.
//
//   ext-rewrite-client <db> exec  <sql>...  sqlite3_exec per <sql> arg, one
//                                           connection (multi-statement OK)
//   ext-rewrite-client <db> loop  <sql>   sqlite3_prepare_v2 + pzTail loop
//   ext-rewrite-client <db> query <sql>   prepare one statement, print col 0 rows
//   ext-rewrite-client <db> lazycheck <engine> unloaded|loaded
//       open <db>, run DDL through exec + prepare, then assert the lazy
//       shim's Go-runtime laziness: unloaded = engine unmapped, exactly
//       one thread, setgid/setuid to current ids succeed; loaded =
//       engine mapped, Go runtime threads present.
//
// Build: cc -o ext-rewrite-client ext-rewrite-client.c -lsqlite3
#include <dirent.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int maps_contains(const char *path) {
    char real[PATH_MAX];
    if (realpath(path, real) == NULL) {
        snprintf(real, sizeof(real), "%s", path);
    }
    FILE *f = fopen("/proc/self/maps", "r");
    if (f == NULL) return -1;
    char line[PATH_MAX + 128];
    int found = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strstr(line, real)) { found = 1; break; }
    }
    fclose(f);
    return found;
}

static int task_count(void) {
    DIR *d = opendir("/proc/self/task");
    if (d == NULL) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (e->d_name[0] != '.') n++;
    }
    closedir(d);
    return n;
}

static int lazycheck(sqlite3 *db, const char *engine, const char *expect) {
    // Exercise both interposed statement paths before measuring.
    char *err = NULL;
    if (sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS lazyprobe (id INTEGER PRIMARY KEY)",
            NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "client: lazycheck exec: %s\n", err ? err : "?");
        sqlite3_free(err);
        return 1;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM lazyprobe", -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "client: lazycheck prepare: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    int tasks = task_count();
    int mapped = maps_contains(engine);
    if (tasks < 0 || mapped < 0) {
        fprintf(stderr, "client: lazycheck: /proc unavailable\n");
        return 1;
    }
    if (strcmp(expect, "unloaded") == 0) {
        const char *preload = getenv("LD_PRELOAD");
        if (preload == NULL || maps_contains(preload) != 1) {
            fprintf(stderr, "client: lazycheck: preload shim not mapped\n");
            return 1;
        }
        if (mapped) {
            fprintf(stderr, "client: lazycheck: engine mapped for unrelated db\n");
            return 1;
        }
        if (tasks != 1) {
            fprintf(stderr, "client: lazycheck: %d threads, want 1 (Go runtime leaked in)\n", tasks);
            return 1;
        }
        // setxid proxy: with no runtime threads, the libc setxid
        // broadcast has nothing to trip over.
        if (setgid(getgid()) != 0 || setuid(getuid()) != 0) {
            perror("client: lazycheck: setxid");
            return 1;
        }
    } else {
        if (!mapped) {
            fprintf(stderr, "client: lazycheck: engine NOT mapped for matching db\n");
            return 1;
        }
        if (tasks <= 1) {
            fprintf(stderr, "client: lazycheck: %d threads, want >1 (engine loaded)\n", tasks);
            return 1;
        }
    }
    printf("lazycheck %s ok: tasks=%d engine_mapped=%d\n", expect, tasks, mapped);
    return 0;
}

static int die(sqlite3 *db, const char *what) {
    fprintf(stderr, "client: %s: %s\n", what, db ? sqlite3_errmsg(db) : "?");
    if (db) sqlite3_close(db);
    return 1;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "usage: %s <db> exec|loop|query <sql>...\n", argv[0]);
        return 2;
    }
    const char *mode = argv[2], *sql = argv[3];
    sqlite3 *db = NULL;
    if (sqlite3_open(argv[1], &db) != SQLITE_OK) return die(db, "open");

    if (strcmp(mode, "exec") == 0) {
        for (int a = 3; a < argc; a++) {
            char *err = NULL;
            if (sqlite3_exec(db, argv[a], NULL, NULL, &err) != SQLITE_OK) {
                fprintf(stderr, "client: exec: %s\n", err ? err : "?");
                sqlite3_free(err);
                sqlite3_close(db);
                return 1;
            }
        }
    } else if (strcmp(mode, "loop") == 0) {
        // Statement-at-a-time loop over pzTail — the shape used by Go's
        // database/sql drivers. Verifies the interposer's tail points
        // into the caller's original buffer at the right offset.
        const char *rest = sql;
        while (rest && *rest) {
            sqlite3_stmt *stmt = NULL;
            const char *tail = NULL;
            if (sqlite3_prepare_v2(db, rest, -1, &stmt, &tail) != SQLITE_OK) {
                return die(db, "prepare");
            }
            if (stmt) {
                int rc = sqlite3_step(stmt);
                if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
                    sqlite3_finalize(stmt);
                    return die(db, "step");
                }
                sqlite3_finalize(stmt);
            }
            // The tail must lie within the buffer we passed.
            if (tail != NULL && (tail < rest || tail > rest + strlen(rest))) {
                fprintf(stderr, "client: tail outside caller buffer\n");
                sqlite3_close(db);
                return 1;
            }
            rest = tail;
        }
    } else if (strcmp(mode, "lazycheck") == 0) {
        if (argc < 5) {
            fprintf(stderr, "usage: %s <db> lazycheck <engine> unloaded|loaded\n", argv[0]);
            sqlite3_close(db);
            return 2;
        }
        int rc = lazycheck(db, argv[3], argv[4]);
        sqlite3_close(db);
        return rc;
    } else if (strcmp(mode, "query") == 0) {
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
            return die(db, "prepare");
        }
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            printf("%s\n", sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);
    } else {
        fprintf(stderr, "client: unknown mode %s\n", mode);
        sqlite3_close(db);
        return 2;
    }
    sqlite3_close(db);
    return 0;
}
