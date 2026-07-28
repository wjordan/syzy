// syzy_app_vfs.h: the "syzy-app" parkable wrapper VFS (app_vfs.c).
// Shared between autoload_shim.c (registration + open steering) and
// the Go engine's park control socket (via dlsym, so the preloaded
// copy is always the one addressed).
#ifndef SYZY_APP_VFS_H
#define SYZY_APP_VFS_H

// sx_app_vfs_register wraps the process default VFS as "syzy-app"
// (registered non-default). Returns 1 when active (idempotent), 0 when
// registration is impossible (no libsqlite3 symbols yet; retryable).
int sx_app_vfs_register(void);

// sx_app_vfs_steer reports whether an interposed open of filename
// (plain path or file: URI) resolves to $SYZY_DB and should be routed
// onto the "syzy-app" VFS.
int sx_app_vfs_steer(const char *filename);

// Park/unpark. All return 0 on success; on failure they write a reason
// into err and leave the gate in a safe state (begin failure reopens
// the gate; unpark failure keeps it latched). Sequence:
//   park_begin    latch new VFS activity, drain in-flight calls, nack
//                 if any connection holds a lock (open transaction)
//   park_commit   close file handles, replace shm mmaps with anonymous
//                 pages at the same addresses, close shm fds
//   unpark_files  reopen files, restore lock state, remap shm regions
//                 at their original addresses
//   unpark_open   reopen the gate
int sx_app_park_begin(char *err, int errlen);
int sx_app_park_commit(char *err, int errlen);
int sx_app_unpark_files(char *err, int errlen);
int sx_app_unpark_open(char *err, int errlen);

// sx_app_gate_bypass exempts the calling thread from the park latch
// (on=1) so engine teardown/re-attach between begin/commit and
// unpark_files/unpark_open can use the still-open connections.
void sx_app_gate_bypass(int on);

// sx_park_ops is published through the registered VFS's pAppData so
// the engine always drives the ACTIVE wrapper instance. Two copies of
// this code coexist in lazy-shim mode (preloaded shim + dlopen'd
// engine), and the engine is linked -Bsymbolic, so a dlsym from it
// binds to its own dormant copy; sqlite3_vfs_find("syzy-app") is the
// only authoritative route to the copy that owns the tracked state.
typedef struct sx_park_ops {
    int (*park_begin)(char *err, int errlen);
    int (*park_commit)(char *err, int errlen);
    int (*unpark_files)(char *err, int errlen);
    int (*unpark_open)(char *err, int errlen);
    void (*gate_bypass)(int on);
} sx_park_ops;

#endif
