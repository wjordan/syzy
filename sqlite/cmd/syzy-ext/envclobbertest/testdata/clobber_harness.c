// Harness for the env-block theft an extension must survive.
//
// Runtimes that implement setproctitle the BSD way (Ruby, libuv,
// Python's setproctitle) copy environ onto the heap, repoint environ
// at the copy, and then reuse the original argv+environ stack block as
// scratch space for the title. getenv keeps working; the block a Go
// runtime reads when it starts does not.
//
// Usage: clobber_harness [clobber]
// Opens $SYZY_DB through ordinary dynamic linking (so the LD_PRELOAD'd
// shim's interposers see it) and writes one row. Prints the number of
// SYZY_* entries still visible in /proc/self/environ, which is the same
// stack block Go reads, so the test can tell a real theft from a no-op.
#define _GNU_SOURCE
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern char **environ;

static void steal_env_block(char **argv) {
    int n = 0;
    while (environ[n]) n++;
    char **copy = malloc((size_t)(n + 1) * sizeof(char *));
    if (copy == NULL) exit(2);
    for (int i = 0; i < n; i++) copy[i] = strdup(environ[i]);
    copy[n] = NULL;
    char *start = argv[0];
    char *end = environ[n - 1] + strlen(environ[n - 1]) + 1;
    environ = copy;
    memset(start, 0, (size_t)(end - start));
    strcpy(start, "puma: cluster worker 0: 42 [myapp]");
}

// stack_syzy_entries counts SYZY_* variables left in the process's
// original environment block.
static int stack_syzy_entries(void) {
    FILE *f = fopen("/proc/self/environ", "rb");
    if (f == NULL) return -1;
    char buf[65536];
    size_t n = fread(buf, 1, sizeof(buf), f);
    fclose(f);
    int count = 0;
    for (size_t i = 0; i < n;) {
        size_t len = strnlen(buf + i, n - i);
        if (strncmp(buf + i, "SYZY_", 5) == 0) count++;
        i += len + 1;
    }
    return count;
}

int main(int argc, char **argv) {
    if (argc > 1 && strcmp(argv[1], "clobber") == 0) steal_env_block(argv);
    printf("stack_syzy_entries=%d\n", stack_syzy_entries());

    const char *path = getenv("SYZY_DB");
    if (path == NULL) {
        printf("error=SYZY_DB unset\n");
        return 1;
    }
    sqlite3 *db = NULL;
    int rc = sqlite3_open(path, &db);
    if (rc != 0) {
        printf("error=open: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    char *err = NULL;
    rc = sqlite3_exec(db, "INSERT INTO t(v) VALUES('x')", NULL, NULL, &err);
    if (rc != 0) {
        printf("error=write: %s\n", err == NULL ? "(none)" : err);
        return 1;
    }
    printf("ok\n");
    return 0;
}
