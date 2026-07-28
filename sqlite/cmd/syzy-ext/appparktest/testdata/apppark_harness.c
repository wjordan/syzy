// apppark_harness.c: exercises the "syzy-app" wrapper VFS (app_vfs.c)
// against the host libsqlite3, driven by apppark_test.go. Lives in
// testdata/ so cgo does not compile it into the extension package.
//
//   apppark_harness <mode> <dir>
//
// modes:
//   roundtrip  open two pooled connections (WAL), write, park, verify
//              zero /proc/self/{fd,maps} refs under dir, delete the
//              -shm file (simulating a fresh backing mount), unpark,
//              write + read back through both connections
//   nack       park_begin must refuse while a read transaction is open
//              and must leave the connection usable
//   blocked    a reader that arrives while parked blocks at the gate,
//              survives park/unpark, and completes
//
// Exits 0 printing "OK" on success; nonzero with a message otherwise.

#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "sqlite3.h"
#include "syzy_app_vfs.h"

static const char *g_dir;

static void die(const char *what, const char *detail) {
    fprintf(stderr, "FAIL %s: %s\n", what, detail ? detail : "");
    exit(1);
}

static sqlite3 *open_db(void) {
    sqlite3 *db = NULL;
    char path[512];
    snprintf(path, sizeof(path), "%s/app.db", g_dir);
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
            "syzy-app") != SQLITE_OK) {
        die("open", db ? sqlite3_errmsg(db) : "no handle");
    }
    return db;
}

static void mustexec(sqlite3 *db, const char *sql) {
    char *msg = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &msg) != SQLITE_OK) {
        die(sql, msg);
    }
}

static int count_rows(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    int n = -1;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &st, NULL) != SQLITE_OK) {
        die("prepare count", sqlite3_errmsg(db));
    }
    if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return n;
}

// scan_refs counts fds and mappings under dir. Mirrors the supervisor's
// holder scan: any nonzero count would keep the backing mount busy.
static int scan_refs(void) {
    char prefix[512], buf[1024], line[1024];
    int n = 0;
    snprintf(prefix, sizeof(prefix), "%s/", g_dir);
    DIR *d = opendir("/proc/self/fd");
    if (d != NULL) {
        struct dirent *e;
        while ((e = readdir(d)) != NULL) {
            char p[64];
            ssize_t len;
            snprintf(p, sizeof(p), "/proc/self/fd/%s", e->d_name);
            len = readlink(p, buf, sizeof(buf) - 1);
            if (len <= 0) continue;
            buf[len] = '\0';
            if (strncmp(buf, prefix, strlen(prefix)) == 0) {
                fprintf(stderr, "ref fd: %s\n", buf);
                n++;
            }
        }
        closedir(d);
    }
    FILE *m = fopen("/proc/self/maps", "r");
    if (m != NULL) {
        while (fgets(line, sizeof(line), m) != NULL) {
            if (strstr(line, prefix) != NULL) {
                fprintf(stderr, "ref map: %s", line);
                n++;
            }
        }
        fclose(m);
    }
    return n;
}

static void must_step(const char *what, int rc, char *err) {
    if (rc != 0) die(what, err);
}

static void park(void) {
    char err[256] = "";
    must_step("park_begin", sx_app_park_begin(err, sizeof(err)), err);
    must_step("park_commit", sx_app_park_commit(err, sizeof(err)), err);
}

static void unpark(void) {
    char err[256] = "";
    must_step("unpark_files", sx_app_unpark_files(err, sizeof(err)), err);
    must_step("unpark_open", sx_app_unpark_open(err, sizeof(err)), err);
}

static int mode_roundtrip(void) {
    sqlite3 *a = open_db();
    sqlite3 *b = open_db();
    char shm[512];
    mustexec(a, "PRAGMA journal_mode=WAL");
    mustexec(a, "CREATE TABLE t(x)");
    mustexec(a, "INSERT INTO t VALUES(1)");
    mustexec(b, "INSERT INTO t VALUES(2)");

    park();
    if (scan_refs() != 0) die("scan", "refs remain under dir after park");

    // A remounted share presents fresh backing state; deleting the
    // -shm file forces the fullest version of that (wal-index rebuilt
    // from the WAL via recovery after unpark).
    snprintf(shm, sizeof(shm), "%s/app.db-shm", g_dir);
    if (unlink(shm) != 0) die("unlink shm", strerror(errno));

    unpark();
    mustexec(a, "INSERT INTO t VALUES(3)");
    if (count_rows(b) != 3) die("count", "expected 3 rows");
    sqlite3_close(a);
    sqlite3_close(b);
    return 0;
}

static int mode_nack(void) {
    sqlite3 *db = open_db();
    char err[256] = "";
    mustexec(db, "PRAGMA journal_mode=WAL");
    mustexec(db, "CREATE TABLE t(x)");
    mustexec(db, "INSERT INTO t VALUES(1)");
    mustexec(db, "BEGIN");
    if (count_rows(db) != 1) die("count in txn", "expected 1 row");
    if (sx_app_park_begin(err, sizeof(err)) == 0) {
        die("park_begin", "succeeded with an open read transaction");
    }
    fprintf(stderr, "nack reason: %s\n", err);
    mustexec(db, "COMMIT");
    mustexec(db, "INSERT INTO t VALUES(2)"); // gate must be open again
    if (count_rows(db) != 2) die("count after nack", "expected 2 rows");
    sqlite3_close(db);
    return 0;
}

static sqlite3 *g_blocked_db;
static int g_blocked_result = -1;

static void *blocked_reader(void *arg) {
    (void)arg;
    g_blocked_result = count_rows(g_blocked_db);
    return NULL;
}

static int mode_blocked(void) {
    pthread_t tid;
    sqlite3 *db = open_db();
    mustexec(db, "PRAGMA journal_mode=WAL");
    mustexec(db, "CREATE TABLE t(x)");
    mustexec(db, "INSERT INTO t VALUES(1)");

    park();
    g_blocked_db = db;
    if (pthread_create(&tid, NULL, blocked_reader, NULL) != 0) {
        die("pthread_create", strerror(errno));
    }
    usleep(50 * 1000);
    if (g_blocked_result != -1) die("gate", "reader completed while parked");
    unpark();
    pthread_join(tid, NULL);
    if (g_blocked_result != 1) die("blocked count", "expected 1 row");
    sqlite3_close(db);
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 3) die("usage", "apppark_harness <mode> <dir>");
    g_dir = argv[2];
    if (!sx_app_vfs_register()) die("register", "sx_app_vfs_register failed");
    int rc;
    if (strcmp(argv[1], "roundtrip") == 0) rc = mode_roundtrip();
    else if (strcmp(argv[1], "nack") == 0) rc = mode_nack();
    else if (strcmp(argv[1], "blocked") == 0) rc = mode_blocked();
    else { die("mode", argv[1]); return 1; }
    printf("OK\n");
    return rc;
}
