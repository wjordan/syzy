// app_vfs.c: "syzy-app" — a wrapper VFS that makes $SYZY_DB SQLite
// connections *parkable*: on demand the process drops every fd, mmap
// and posix lock those connections hold, survives the backing
// directory going away and coming back (share unmount/remount, or a
// VM snapshot with clones restored elsewhere), and resumes the same
// connections against the fresh backing files. The application sees
// nothing but a paused call.
//
// Structure:
//   - File I/O delegates to the wrapped (default, "unix") VFS: each
//     wrapper file embeds a real file object it can close at park and
//     reopen at unpark (the open interposers guarantee the zName
//     pointer stays valid until xClose).
//   - Shared memory is implemented HERE, not delegated: SQLite caches
//     xShmMap pointers and reads the wal-index header without holding
//     any lock, so park must atomically replace the mappings in place
//     (MAP_FIXED anonymous pages — never a hole to fault on) and
//     unpark must remap the file at the SAME addresses. That requires
//     owning the shm fd and region addresses outright. The
//     implementation is a port of the amalgamation's unixShm* (same
//     -shm file, same fcntl byte offsets, same DMS protocol, same
//     in-process lock arbitration), minus SETLK_TIMEOUT and the
//     heap-backed unix-excl mode.
//   - A gate latches every wrapper entry point that creates or uses
//     backing state while parked. Latched threads block (and, frozen
//     blocked, are safe to snapshot); they resume after unpark. A
//     designated bypass thread (the engine's control thread) is
//     exempt so engine teardown/re-attach can run inside the window.
//
// park_begin refuses (nack) while any connection holds a shm lock or
// a >SHARED file lock — i.e. an open transaction. No forcing: the
// caller falls back to its slower-but-safe path.

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "sqlite3.h"
#include "syzy_app_vfs.h"

#define SX_SHM_NLOCK 8
#define SX_SHM_BASE 120 /* (22+SQLITE_SHM_NLOCK)*4: first lock byte */
#define SX_SHM_DMS (SX_SHM_BASE + SX_SHM_NLOCK) /* deadman switch */

// ---- gate ----

static pthread_mutex_t g_gate_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_gate_cv = PTHREAD_COND_INITIALIZER;
static int g_parked = 0;
static int g_inflight = 0;
static pthread_t g_bypass;
static int g_bypass_set = 0;

static int sx_is_bypass(void) {
    return g_bypass_set && pthread_equal(g_bypass, pthread_self());
}

static void sx_gate_enter(void) {
    pthread_mutex_lock(&g_gate_mu);
    while (g_parked && !sx_is_bypass()) {
        pthread_cond_wait(&g_gate_cv, &g_gate_mu);
    }
    g_inflight++;
    pthread_mutex_unlock(&g_gate_mu);
}

static void sx_gate_exit(void) {
    pthread_mutex_lock(&g_gate_mu);
    g_inflight--;
    pthread_cond_broadcast(&g_gate_cv);
    pthread_mutex_unlock(&g_gate_mu);
}

void sx_app_gate_bypass(int on) {
    pthread_mutex_lock(&g_gate_mu);
    if (on) {
        g_bypass = pthread_self();
        g_bypass_set = 1;
    } else {
        g_bypass_set = 0;
    }
    pthread_cond_broadcast(&g_gate_cv);
    pthread_mutex_unlock(&g_gate_mu);
}

// ---- state registries (files + shm nodes), under g_reg_mu ----
// Lock order: gate (g_gate_mu, released before the op) then g_reg_mu.

static pthread_mutex_t g_reg_mu = PTHREAD_MUTEX_INITIALIZER;

typedef struct sx_shmnode sx_shmnode;

typedef struct sx_file {
    sqlite3_file base;   /* must be first */
    sqlite3_file *real;  /* trailing buffer, wrapped VFS's file */
    const char *zName;   /* SQLite-owned; valid until xClose; NULL for temp */
    int openFlags;
    int eLock;           /* tracked via xLock/xUnlock */
    int parked;          /* real handle closed by park_commit */
    sx_shmnode *shm;     /* attached shm node (main db only) */
    unsigned short sharedMask, exclMask; /* this connection's shm locks */
    struct sx_file *next;
} sx_file;

struct sx_shmnode {
    char *zShmPath;
    int fd;              /* -1 while parked */
    int szRegion;
    int nRegion;
    char **apRegion;     /* region base addresses; stable across park */
    int aLock[SX_SHM_NLOCK]; /* -1 excl, else shared holder count */
    int nRef;
    int isReadonly;
    int isUnlocked;      /* readonly conn that couldn't take DMS yet */
    sx_shmnode *next;
};

static sx_file *g_files;
static sx_shmnode *g_shmnodes;

static sqlite3_vfs *g_real_vfs;
static sqlite3_vfs g_vfs;
static sqlite3_io_methods g_io_methods;

#define SX_FILE_HDR ((int)((sizeof(sx_file) + 15) & ~(size_t)15))

// ---- shm implementation (port of unixShm*) ----

static int sx_shm_syslock(sx_shmnode *node, int lockType, int ofst, int n) {
    struct flock f;
    if (node->fd < 0) return SQLITE_OK;
    memset(&f, 0, sizeof(f));
    f.l_type = lockType;
    f.l_whence = SEEK_SET;
    f.l_start = ofst;
    f.l_len = n;
    if (fcntl(node->fd, F_SETLK, &f) == -1) return SQLITE_BUSY;
    return SQLITE_OK;
}

// sx_shm_lock_dms runs the deadman-switch dance on a freshly opened
// shm fd: first attacher (nobody holds DMS) takes it exclusive and
// truncates the file (forcing wal-index recovery), then everyone
// downgrades to shared. Held-exclusive means another process is mid-
// truncate: SQLITE_BUSY, retryable.
static int sx_shm_lock_dms(sx_shmnode *node) {
    struct flock lock;
    int rc = SQLITE_OK;
    memset(&lock, 0, sizeof(lock));
    lock.l_whence = SEEK_SET;
    lock.l_start = SX_SHM_DMS;
    lock.l_len = 1;
    lock.l_type = F_WRLCK;
    if (fcntl(node->fd, F_GETLK, &lock) != 0) {
        rc = SQLITE_IOERR_LOCK;
    } else if (lock.l_type == F_UNLCK) {
        if (node->isReadonly) {
            node->isUnlocked = 1;
            rc = SQLITE_READONLY_CANTINIT;
        } else {
            rc = sx_shm_syslock(node, F_WRLCK, SX_SHM_DMS, 1);
            if (rc == SQLITE_OK && ftruncate(node->fd, 3) != 0) {
                rc = SQLITE_IOERR_SHMOPEN;
            }
        }
    } else if (lock.l_type == F_WRLCK) {
        rc = SQLITE_BUSY;
    }
    if (rc == SQLITE_OK) {
        rc = sx_shm_syslock(node, F_RDLCK, SX_SHM_DMS, 1);
    }
    return rc;
}

// sx_shm_extend grows the shm file to nByte by writing the last byte
// of each new OS page (allocates eagerly, mirroring the wrapped VFS,
// to avoid SIGBUS on later access to the mapping).
static int sx_shm_extend(sx_shmnode *node, off_t nByte) {
    static const int pgsz = 4096;
    struct stat st;
    off_t iPg;
    if (fstat(node->fd, &st) != 0) return SQLITE_IOERR_SHMSIZE;
    if (st.st_size >= nByte) return SQLITE_OK;
    for (iPg = st.st_size / pgsz; iPg < nByte / pgsz; iPg++) {
        if (pwrite(node->fd, "", 1, iPg * pgsz + pgsz - 1) != 1) {
            return SQLITE_IOERR_SHMSIZE;
        }
    }
    return SQLITE_OK;
}

// sx_shm_open_fd opens (creating if permitted) the -shm file for node,
// matching the database file's permissions, and runs the DMS dance.
static int sx_shm_open_fd(sx_shmnode *node, const char *zDbName) {
    struct stat st;
    mode_t mode = 0644;
    if (zDbName != NULL && stat(zDbName, &st) == 0) mode = st.st_mode & 0777;
    node->fd = open(node->zShmPath, O_RDWR | O_CREAT | O_NOFOLLOW, mode);
    if (node->fd < 0) {
        node->fd = open(node->zShmPath, O_RDONLY | O_NOFOLLOW, mode);
        if (node->fd < 0) return SQLITE_CANTOPEN;
        node->isReadonly = 1;
    }
    return sx_shm_lock_dms(node);
}

static sx_shmnode *sx_shm_find(const char *zShmPath) {
    sx_shmnode *n;
    for (n = g_shmnodes; n != NULL; n = n->next) {
        if (strcmp(n->zShmPath, zShmPath) == 0) return n;
    }
    return NULL;
}

// sx_shm_attach binds file to its shm node, creating the node (and
// opening the -shm file) on first use. g_reg_mu held.
static int sx_shm_attach(sx_file *file) {
    char zShm[PATH_MAX];
    sx_shmnode *node;
    int rc;
    if (file->zName == NULL ||
        snprintf(zShm, sizeof(zShm), "%s-shm", file->zName) >= (int)sizeof(zShm)) {
        return SQLITE_CANTOPEN;
    }
    node = sx_shm_find(zShm);
    if (node == NULL) {
        node = calloc(1, sizeof(*node));
        if (node == NULL) return SQLITE_NOMEM;
        node->zShmPath = strdup(zShm);
        if (node->zShmPath == NULL) {
            free(node);
            return SQLITE_NOMEM;
        }
        node->fd = -1;
        rc = sx_shm_open_fd(node, file->zName);
        if (rc != SQLITE_OK && rc != SQLITE_READONLY_CANTINIT) {
            if (node->fd >= 0) close(node->fd);
            free(node->zShmPath);
            free(node);
            return rc;
        }
        node->next = g_shmnodes;
        g_shmnodes = node;
    }
    node->nRef++;
    file->shm = node;
    return SQLITE_OK;
}

static int sx_shm_map(sx_file *file, int iRegion, int szRegion, int bExtend,
        void volatile **pp) {
    sx_shmnode *node;
    int rc = SQLITE_OK;
    *pp = NULL;
    pthread_mutex_lock(&g_reg_mu);
    if (file->shm == NULL) {
        rc = sx_shm_attach(file);
        if (rc != SQLITE_OK) goto out;
    }
    node = file->shm;
    if (node->isUnlocked) {
        rc = sx_shm_lock_dms(node);
        if (rc != SQLITE_OK) goto out;
        node->isUnlocked = 0;
    }
    if (node->nRegion > 0 && node->szRegion != szRegion) {
        rc = SQLITE_IOERR_SHMMAP;
        goto out;
    }
    if (node->nRegion <= iRegion) {
        off_t nByte = (off_t)(iRegion + 1) * szRegion;
        struct stat st;
        char **apNew;
        node->szRegion = szRegion;
        if (fstat(node->fd, &st) != 0) {
            rc = SQLITE_IOERR_SHMSIZE;
            goto out;
        }
        if (st.st_size < nByte) {
            if (!bExtend) goto out;
            rc = sx_shm_extend(node, nByte);
            if (rc != SQLITE_OK) goto out;
        }
        apNew = realloc(node->apRegion, (size_t)(iRegion + 1) * sizeof(char *));
        if (apNew == NULL) {
            rc = SQLITE_NOMEM;
            goto out;
        }
        node->apRegion = apNew;
        while (node->nRegion <= iRegion) {
            void *mem = mmap(NULL, szRegion,
                    node->isReadonly ? PROT_READ : PROT_READ | PROT_WRITE,
                    MAP_SHARED, node->fd, (off_t)node->nRegion * szRegion);
            if (mem == MAP_FAILED) {
                rc = SQLITE_IOERR_SHMMAP;
                goto out;
            }
            node->apRegion[node->nRegion++] = mem;
        }
    }
out:
    if (file->shm != NULL && file->shm->nRegion > iRegion) {
        *pp = file->shm->apRegion[iRegion];
    }
    if (rc == SQLITE_OK && file->shm != NULL && file->shm->isReadonly) {
        rc = SQLITE_READONLY;
    }
    pthread_mutex_unlock(&g_reg_mu);
    return rc;
}

// sx_shm_lock ports unixShmLock: per-connection shared/excl bitmasks,
// node-wide aLock[] arbitration between in-process connections, fcntl
// range locks (non-blocking) against other processes.
static int sx_shm_lock(sx_file *p, int ofst, int n, int flags) {
    sx_shmnode *node = p->shm;
    int rc = SQLITE_OK;
    unsigned short mask = (unsigned short)((1 << (ofst + n)) - (1 << ofst));
    int *aLock;
    if (node == NULL) return SQLITE_IOERR_SHMLOCK;
    aLock = node->aLock;
    if (((flags & SQLITE_SHM_UNLOCK) && ((p->exclMask | p->sharedMask) & mask)) ||
        (flags == (SQLITE_SHM_SHARED | SQLITE_SHM_LOCK) && 0 == (p->sharedMask & mask)) ||
        (flags == (SQLITE_SHM_EXCLUSIVE | SQLITE_SHM_LOCK))) {
        pthread_mutex_lock(&g_reg_mu);
        if (flags & SQLITE_SHM_UNLOCK) {
            int bUnlock = 1;
            if (flags & SQLITE_SHM_SHARED) {
                if (aLock[ofst] > 1) {
                    bUnlock = 0;
                    aLock[ofst]--;
                    p->sharedMask &= ~mask;
                }
            }
            if (bUnlock) {
                rc = sx_shm_syslock(node, F_UNLCK, ofst + SX_SHM_BASE, n);
                if (rc == SQLITE_OK) {
                    memset(&aLock[ofst], 0, sizeof(int) * n);
                    p->sharedMask &= ~mask;
                    p->exclMask &= ~mask;
                }
            }
        } else if (flags & SQLITE_SHM_SHARED) {
            if (aLock[ofst] < 0) {
                rc = SQLITE_BUSY;
            } else if (aLock[ofst] == 0) {
                rc = sx_shm_syslock(node, F_RDLCK, ofst + SX_SHM_BASE, n);
            }
            if (rc == SQLITE_OK) {
                p->sharedMask |= mask;
                aLock[ofst]++;
            }
        } else {
            int ii;
            for (ii = ofst; ii < ofst + n; ii++) {
                if (aLock[ii]) {
                    rc = SQLITE_BUSY;
                    break;
                }
            }
            if (rc == SQLITE_OK) {
                rc = sx_shm_syslock(node, F_WRLCK, ofst + SX_SHM_BASE, n);
                if (rc == SQLITE_OK) {
                    p->exclMask |= mask;
                    for (ii = ofst; ii < ofst + n; ii++) aLock[ii] = -1;
                }
            }
        }
        pthread_mutex_unlock(&g_reg_mu);
    }
    return rc;
}

// sx_shm_detach releases file's node reference; the last reference
// unmaps the regions and closes (optionally deleting) the -shm file.
// Mirrors unixShmUnmap: SQLite releases all shm locks before this.
static void sx_shm_detach(sx_file *file, int deleteFlag) {
    sx_shmnode *node = file->shm;
    if (node == NULL) return;
    pthread_mutex_lock(&g_reg_mu);
    file->shm = NULL;
    file->sharedMask = 0;
    file->exclMask = 0;
    node->nRef--;
    if (node->nRef == 0) {
        sx_shmnode **pp;
        int i;
        for (i = 0; i < node->nRegion; i++) {
            munmap(node->apRegion[i], node->szRegion);
        }
        if (node->fd >= 0) {
            if (deleteFlag) unlink(node->zShmPath);
            close(node->fd);
        }
        for (pp = &g_shmnodes; *pp != node; pp = &(*pp)->next) {}
        *pp = node->next;
        free(node->apRegion);
        free(node->zShmPath);
        free(node);
    }
    pthread_mutex_unlock(&g_reg_mu);
}

// ---- io methods (gate + delegate) ----

#define SX_FILE(f) ((sx_file *)(f))
#define SX_REAL_M(p) ((p)->real->pMethods)

// SX_GATED_BODY is the uniform gated-delegate method body: enter the
// gate (blocks while parked), run the delegate against the real file —
// or return the parked sentinel if park_commit closed it underneath a
// mis-sequenced caller — and exit the gate. Methods with extra work
// (close, lock/unlock's eLock tracking, shm, fetch) stay written out.
#define SX_GATED_BODY(f, parkedVal, call) \
    sx_file *p = SX_FILE(f); \
    int rc; \
    sx_gate_enter(); \
    rc = p->parked ? (parkedVal) : (call); \
    sx_gate_exit(); \
    return rc

static int sx_io_close(sqlite3_file *f) {
    sx_file *p = SX_FILE(f);
    int rc = SQLITE_OK;
    sx_gate_enter();
    sx_shm_detach(p, 0);
    if (!p->parked && SX_REAL_M(p) != NULL) {
        rc = SX_REAL_M(p)->xClose(p->real);
    }
    pthread_mutex_lock(&g_reg_mu);
    {
        sx_file **pp;
        for (pp = &g_files; *pp != NULL; pp = &(*pp)->next) {
            if (*pp == p) {
                *pp = p->next;
                break;
            }
        }
    }
    pthread_mutex_unlock(&g_reg_mu);
    sx_gate_exit();
    return rc;
}

static int sx_io_read(sqlite3_file *f, void *buf, int amt, sqlite3_int64 ofst) {
    SX_GATED_BODY(f, SQLITE_IOERR_READ, SX_REAL_M(p)->xRead(p->real, buf, amt, ofst));
}

static int sx_io_write(sqlite3_file *f, const void *buf, int amt, sqlite3_int64 ofst) {
    SX_GATED_BODY(f, SQLITE_IOERR_WRITE, SX_REAL_M(p)->xWrite(p->real, buf, amt, ofst));
}

static int sx_io_truncate(sqlite3_file *f, sqlite3_int64 size) {
    SX_GATED_BODY(f, SQLITE_IOERR_TRUNCATE, SX_REAL_M(p)->xTruncate(p->real, size));
}

static int sx_io_sync(sqlite3_file *f, int flags) {
    SX_GATED_BODY(f, SQLITE_IOERR_FSYNC, SX_REAL_M(p)->xSync(p->real, flags));
}

static int sx_io_filesize(sqlite3_file *f, sqlite3_int64 *pSize) {
    SX_GATED_BODY(f, SQLITE_IOERR_FSTAT, SX_REAL_M(p)->xFileSize(p->real, pSize));
}

static int sx_io_lock(sqlite3_file *f, int level) {
    sx_file *p = SX_FILE(f);
    int rc;
    sx_gate_enter();
    rc = p->parked ? SQLITE_IOERR_LOCK : SX_REAL_M(p)->xLock(p->real, level);
    if (rc == SQLITE_OK) p->eLock = level;
    sx_gate_exit();
    return rc;
}

static int sx_io_unlock(sqlite3_file *f, int level) {
    sx_file *p = SX_FILE(f);
    int rc;
    sx_gate_enter();
    rc = p->parked ? SQLITE_IOERR_UNLOCK : SX_REAL_M(p)->xUnlock(p->real, level);
    if (rc == SQLITE_OK) p->eLock = level;
    sx_gate_exit();
    return rc;
}

static int sx_io_checkreservedlock(sqlite3_file *f, int *pResOut) {
    SX_GATED_BODY(f, SQLITE_IOERR_CHECKRESERVEDLOCK,
            SX_REAL_M(p)->xCheckReservedLock(p->real, pResOut));
}

static int sx_io_filecontrol(sqlite3_file *f, int op, void *pArg) {
    SX_GATED_BODY(f, SQLITE_NOTFOUND, SX_REAL_M(p)->xFileControl(p->real, op, pArg));
}

static int sx_io_sectorsize(sqlite3_file *f) {
    SX_GATED_BODY(f, 4096, SX_REAL_M(p)->xSectorSize(p->real));
}

static int sx_io_devicecharacteristics(sqlite3_file *f) {
    SX_GATED_BODY(f, 0, SX_REAL_M(p)->xDeviceCharacteristics(p->real));
}

static int sx_io_shmmap(sqlite3_file *f, int iRegion, int szRegion, int bExtend,
        void volatile **pp) {
    int rc;
    sx_gate_enter();
    rc = sx_shm_map(SX_FILE(f), iRegion, szRegion, bExtend, pp);
    sx_gate_exit();
    return rc;
}

static int sx_io_shmlock(sqlite3_file *f, int ofst, int n, int flags) {
    int rc;
    sx_gate_enter();
    rc = sx_shm_lock(SX_FILE(f), ofst, n, flags);
    sx_gate_exit();
    return rc;
}

static void sx_io_shmbarrier(sqlite3_file *f) {
    (void)f;
    __sync_synchronize();
    pthread_mutex_lock(&g_reg_mu);
    pthread_mutex_unlock(&g_reg_mu);
}

static int sx_io_shmunmap(sqlite3_file *f, int deleteFlag) {
    sx_gate_enter();
    sx_shm_detach(SX_FILE(f), deleteFlag);
    sx_gate_exit();
    return SQLITE_OK;
}

static int sx_io_fetch(sqlite3_file *f, sqlite3_int64 ofst, int amt, void **pp) {
    sx_file *p = SX_FILE(f);
    int rc = SQLITE_OK;
    *pp = NULL;
    sx_gate_enter();
    if (!p->parked && SX_REAL_M(p)->iVersion >= 3 && SX_REAL_M(p)->xFetch != NULL) {
        rc = SX_REAL_M(p)->xFetch(p->real, ofst, amt, pp);
    }
    sx_gate_exit();
    return rc;
}

static int sx_io_unfetch(sqlite3_file *f, sqlite3_int64 ofst, void *pPage) {
    sx_file *p = SX_FILE(f);
    int rc = SQLITE_OK;
    sx_gate_enter();
    if (!p->parked && SX_REAL_M(p)->iVersion >= 3 && SX_REAL_M(p)->xUnfetch != NULL) {
        rc = SX_REAL_M(p)->xUnfetch(p->real, ofst, pPage);
    }
    sx_gate_exit();
    return rc;
}

// ---- vfs methods ----

static int sx_vfs_open(sqlite3_vfs *vfs, const char *zName, sqlite3_file *f,
        int flags, int *pOutFlags) {
    sx_file *p = SX_FILE(f);
    int rc;
    (void)vfs;
    sx_gate_enter();
    memset(p, 0, SX_FILE_HDR);
    p->real = (sqlite3_file *)((unsigned char *)p + SX_FILE_HDR);
    rc = g_real_vfs->xOpen(g_real_vfs, zName, p->real, flags, pOutFlags);
    if (rc == SQLITE_OK) {
        p->zName = zName;
        p->openFlags = flags;
        p->base.pMethods = &g_io_methods;
        pthread_mutex_lock(&g_reg_mu);
        p->next = g_files;
        g_files = p;
        pthread_mutex_unlock(&g_reg_mu);
        if (getenv("SYZY_DEBUG") != NULL) {
            fprintf(stderr, "syzy-app: xOpen %s (state %p)\n",
                    zName ? zName : "(temp)", (void *)&g_files);
        }
    }
    sx_gate_exit();
    return rc;
}

static int sx_vfs_delete(sqlite3_vfs *vfs, const char *zName, int syncDir) {
    int rc;
    (void)vfs;
    sx_gate_enter();
    rc = g_real_vfs->xDelete(g_real_vfs, zName, syncDir);
    sx_gate_exit();
    return rc;
}

static int sx_vfs_access(sqlite3_vfs *vfs, const char *zName, int flags, int *pResOut) {
    (void)vfs;
    return g_real_vfs->xAccess(g_real_vfs, zName, flags, pResOut);
}

static int sx_vfs_fullpathname(sqlite3_vfs *vfs, const char *zName, int nOut, char *zOut) {
    (void)vfs;
    return g_real_vfs->xFullPathname(g_real_vfs, zName, nOut, zOut);
}

static void *sx_vfs_dlopen(sqlite3_vfs *vfs, const char *zName) {
    (void)vfs;
    return g_real_vfs->xDlOpen(g_real_vfs, zName);
}

static void sx_vfs_dlerror(sqlite3_vfs *vfs, int nByte, char *zErrMsg) {
    (void)vfs;
    g_real_vfs->xDlError(g_real_vfs, nByte, zErrMsg);
}

static void (*sx_vfs_dlsym(sqlite3_vfs *vfs, void *pH, const char *zSym))(void) {
    (void)vfs;
    return g_real_vfs->xDlSym(g_real_vfs, pH, zSym);
}

static void sx_vfs_dlclose(sqlite3_vfs *vfs, void *pHandle) {
    (void)vfs;
    g_real_vfs->xDlClose(g_real_vfs, pHandle);
}

static int sx_vfs_randomness(sqlite3_vfs *vfs, int nByte, char *zOut) {
    (void)vfs;
    return g_real_vfs->xRandomness(g_real_vfs, nByte, zOut);
}

static int sx_vfs_sleep(sqlite3_vfs *vfs, int microseconds) {
    (void)vfs;
    return g_real_vfs->xSleep(g_real_vfs, microseconds);
}

static int sx_vfs_currenttime(sqlite3_vfs *vfs, double *pTime) {
    (void)vfs;
    return g_real_vfs->xCurrentTime(g_real_vfs, pTime);
}

static int sx_vfs_getlasterror(sqlite3_vfs *vfs, int nByte, char *zOut) {
    (void)vfs;
    if (g_real_vfs->xGetLastError == NULL) return 0;
    return g_real_vfs->xGetLastError(g_real_vfs, nByte, zOut);
}

static int sx_vfs_currenttimeint64(sqlite3_vfs *vfs, sqlite3_int64 *pTime) {
    (void)vfs;
    return g_real_vfs->xCurrentTimeInt64(g_real_vfs, pTime);
}

// ---- registration + open steering ----

static const sx_park_ops g_park_ops = {
    sx_app_park_begin,
    sx_app_park_commit,
    sx_app_unpark_files,
    sx_app_unpark_open,
    sx_app_gate_bypass,
};

int sx_app_vfs_register(void) {
    typedef sqlite3_vfs *(*xVfsFind)(const char *);
    typedef int (*xVfsRegister)(sqlite3_vfs *, int);
    static pthread_mutex_t reg_once_mu = PTHREAD_MUTEX_INITIALIZER;
    xVfsFind vfs_find;
    xVfsRegister vfs_register;
    sqlite3_vfs *real;
    int ok = 0;

    if (g_real_vfs != NULL) return 1;
    pthread_mutex_lock(&reg_once_mu);
    if (g_real_vfs != NULL) {
        pthread_mutex_unlock(&reg_once_mu);
        return 1;
    }
    vfs_find = (xVfsFind)dlsym(RTLD_DEFAULT, "sqlite3_vfs_find");
    vfs_register = (xVfsRegister)dlsym(RTLD_DEFAULT, "sqlite3_vfs_register");
    if (vfs_find == NULL || vfs_register == NULL) goto out;
    real = vfs_find(NULL);
    if (real == NULL) goto out;

    memset(&g_io_methods, 0, sizeof(g_io_methods));
    g_io_methods.iVersion = 3;
    g_io_methods.xClose = sx_io_close;
    g_io_methods.xRead = sx_io_read;
    g_io_methods.xWrite = sx_io_write;
    g_io_methods.xTruncate = sx_io_truncate;
    g_io_methods.xSync = sx_io_sync;
    g_io_methods.xFileSize = sx_io_filesize;
    g_io_methods.xLock = sx_io_lock;
    g_io_methods.xUnlock = sx_io_unlock;
    g_io_methods.xCheckReservedLock = sx_io_checkreservedlock;
    g_io_methods.xFileControl = sx_io_filecontrol;
    g_io_methods.xSectorSize = sx_io_sectorsize;
    g_io_methods.xDeviceCharacteristics = sx_io_devicecharacteristics;
    g_io_methods.xShmMap = sx_io_shmmap;
    g_io_methods.xShmLock = sx_io_shmlock;
    g_io_methods.xShmBarrier = sx_io_shmbarrier;
    g_io_methods.xShmUnmap = sx_io_shmunmap;
    g_io_methods.xFetch = sx_io_fetch;
    g_io_methods.xUnfetch = sx_io_unfetch;

    memset(&g_vfs, 0, sizeof(g_vfs));
    g_vfs.iVersion = real->iVersion < 3 ? real->iVersion : 3;
    g_vfs.szOsFile = SX_FILE_HDR + real->szOsFile;
    g_vfs.mxPathname = real->mxPathname;
    g_vfs.zName = "syzy-app";
    g_vfs.pAppData = (void *)&g_park_ops; // engine's route to THIS copy
    g_vfs.xOpen = sx_vfs_open;
    g_vfs.xDelete = sx_vfs_delete;
    g_vfs.xAccess = sx_vfs_access;
    g_vfs.xFullPathname = sx_vfs_fullpathname;
    g_vfs.xDlOpen = real->xDlOpen ? sx_vfs_dlopen : NULL;
    g_vfs.xDlError = real->xDlError ? sx_vfs_dlerror : NULL;
    g_vfs.xDlSym = real->xDlSym ? sx_vfs_dlsym : NULL;
    g_vfs.xDlClose = real->xDlClose ? sx_vfs_dlclose : NULL;
    g_vfs.xRandomness = sx_vfs_randomness;
    g_vfs.xSleep = sx_vfs_sleep;
    g_vfs.xCurrentTime = sx_vfs_currenttime;
    g_vfs.xGetLastError = sx_vfs_getlasterror;
    if (g_vfs.iVersion >= 2 && real->xCurrentTimeInt64 != NULL) {
        g_vfs.xCurrentTimeInt64 = sx_vfs_currenttimeint64;
    }
    // v3 syscall overrides are a test facility; do not expose them.

    if (vfs_register(&g_vfs, 0) != SQLITE_OK) goto out;
    g_real_vfs = real;
    ok = 1;
out:
    pthread_mutex_unlock(&reg_once_mu);
    return ok;
}

// sx_canon canonicalizes path into out: realpath when it exists, else
// realpath(dirname) + basename so a database that is about to be
// CREATED still matches. Returns 0 on failure.
static int sx_canon(const char *path, char *out, size_t outlen) {
    char dir[PATH_MAX], real_dir[PATH_MAX];
    const char *slash;
    size_t dirlen;
    if (realpath(path, out) != NULL) return 1;
    slash = strrchr(path, '/');
    if (slash == NULL) {
        strcpy(dir, ".");
    } else {
        dirlen = (size_t)(slash - path);
        if (dirlen == 0) dirlen = 1; /* "/x" -> dir "/" */
        if (dirlen >= sizeof(dir)) return 0;
        memcpy(dir, path, dirlen);
        dir[dirlen] = '\0';
    }
    if (realpath(dir, real_dir) == NULL) return 0;
    if (snprintf(out, outlen, "%s/%s", strcmp(real_dir, "/") == 0 ? "" : real_dir,
            slash == NULL ? path : slash + 1) >= (int)outlen) {
        return 0;
    }
    return 1;
}

int sx_app_vfs_steer(const char *filename) {
    char pathbuf[PATH_MAX];
    char canonf[PATH_MAX], canont[PATH_MAX];
    const char *p = filename;
    const char *env;
    if (g_real_vfs == NULL || filename == NULL || filename[0] == '\0') return 0;
    env = getenv("SYZY_DB");
    if (env == NULL || env[0] == '\0') return 0;
    if (strncmp(p, "file:", 5) == 0) {
        // Minimal URI parse: scheme, optional authority, path up to
        // query/fragment. Percent-encoded paths won't match — such an
        // open just isn't steered, which is the safe direction.
        size_t n;
        p += 5;
        if (p[0] == '/' && p[1] == '/') {
            const char *slash = strchr(p + 2, '/');
            if (slash == NULL) return 0;
            p = slash;
        }
        n = strcspn(p, "?#");
        if (n == 0 || n >= sizeof(pathbuf)) return 0;
        memcpy(pathbuf, p, n);
        pathbuf[n] = '\0';
        p = pathbuf;
    }
    // No caching: before the database (or the symlink chain to it)
    // exists, canonicalization is dirname-based and would poison a
    // cache once creation lets realpath resolve further.
    if (!sx_canon(p, canonf, sizeof(canonf))) return 0;
    if (!sx_canon(env, canont, sizeof(canont))) return 0;
    return strcmp(canonf, canont) == 0;
}

// ---- park / unpark ----

static int sx_err(char *err, int errlen, const char *fmt, const char *arg) {
    if (err != NULL && errlen > 0) snprintf(err, errlen, fmt, arg);
    return 1;
}

int sx_app_park_begin(char *err, int errlen) {
    struct timespec deadline;
    const char *reason = NULL;
    sx_file *p;
    sx_shmnode *node;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 1;

    pthread_mutex_lock(&g_gate_mu);
    if (g_parked) {
        pthread_mutex_unlock(&g_gate_mu);
        return sx_err(err, errlen, "%s", "already parked");
    }
    g_parked = 1;
    while (g_inflight > 0) {
        if (pthread_cond_timedwait(&g_gate_cv, &g_gate_mu, &deadline) == ETIMEDOUT) {
            g_parked = 0;
            pthread_cond_broadcast(&g_gate_cv);
            pthread_mutex_unlock(&g_gate_mu);
            return sx_err(err, errlen, "%s", "in-flight file operations did not drain");
        }
    }
    pthread_mutex_unlock(&g_gate_mu);

    pthread_mutex_lock(&g_reg_mu);
    for (p = g_files; p != NULL && reason == NULL; p = p->next) {
        if (p->sharedMask | p->exclMask) reason = "connection holds a wal-index lock";
        else if (p->eLock > SQLITE_LOCK_SHARED) reason = "connection holds a write lock";
    }
    // A readonly -shm cannot be reopened read-write at unpark, so a park
    // would be a one-way door. Refuse instead; the caller falls back.
    for (node = g_shmnodes; node != NULL && reason == NULL; node = node->next) {
        if (node->isReadonly || node->isUnlocked) reason = "shm file is read-only";
    }
    pthread_mutex_unlock(&g_reg_mu);
    if (reason != NULL) {
        pthread_mutex_lock(&g_gate_mu);
        g_parked = 0;
        pthread_cond_broadcast(&g_gate_cv);
        pthread_mutex_unlock(&g_gate_mu);
        return sx_err(err, errlen, "%s", reason);
    }
    return 0;
}

int sx_app_park_commit(char *err, int errlen) {
    sx_file *p;
    sx_shmnode *node;
    int rc = 0;
    pthread_mutex_lock(&g_reg_mu);
    if (getenv("SYZY_DEBUG") != NULL) {
        int nf = 0, ns = 0;
        for (p = g_files; p != NULL; p = p->next) nf++;
        for (node = g_shmnodes; node != NULL; node = node->next) ns++;
        fprintf(stderr, "syzy-app: park_commit files=%d shmnodes=%d (state %p)\n",
                nf, ns, (void *)&g_files);
    }
    for (p = g_files; p != NULL; p = p->next) {
        // Unnamed (temp) files live outside the parked directory and
        // have no path to reopen; leave them alone.
        if (p->parked || p->zName == NULL) continue;
        if (SX_REAL_M(p) != NULL) SX_REAL_M(p)->xClose(p->real);
        p->parked = 1;
    }
    for (node = g_shmnodes; node != NULL; node = node->next) {
        int i;
        if (node->fd < 0) continue;
        for (i = 0; i < node->nRegion; i++) {
            // Atomic in-place replace: SQLite caches these addresses
            // and reads the wal-index header through them without any
            // lock held, so there must never be an unmapped hole. An
            // anonymous zero page fails the header checksum, routing
            // readers into recovery — which blocks at the latched
            // xShmLock until unpark.
            if (mmap(node->apRegion[i], node->szRegion, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0) == MAP_FAILED) {
                rc = sx_err(err, errlen, "shm anonymous replace: %s", strerror(errno));
                goto out;
            }
        }
        close(node->fd);
        node->fd = -1;
    }
out:
    pthread_mutex_unlock(&g_reg_mu);
    return rc;
}

// sx_retry_busy runs fn until it stops returning SQLITE_BUSY, ~1s cap.
static int sx_retry_busy(int (*fn)(void *), void *arg) {
    int i, rc = SQLITE_BUSY;
    for (i = 0; i < 100 && rc == SQLITE_BUSY; i++) {
        rc = fn(arg);
        if (rc == SQLITE_BUSY) usleep(10 * 1000);
    }
    return rc;
}

static int sx_relock_shared_cb(void *arg) {
    sx_file *p = arg;
    return SX_REAL_M(p)->xLock(p->real, SQLITE_LOCK_SHARED);
}

static int sx_dms_cb(void *arg) {
    return sx_shm_lock_dms(arg);
}

int sx_app_unpark_files(char *err, int errlen) {
    sx_file *p;
    sx_shmnode *node;
    int rc = 0;
    pthread_mutex_lock(&g_reg_mu);
    for (p = g_files; p != NULL; p = p->next) {
        int outFlags;
        if (!p->parked) continue;
        if (g_real_vfs->xOpen(g_real_vfs, p->zName, p->real, p->openFlags,
                &outFlags) != SQLITE_OK) {
            rc = sx_err(err, errlen, "reopen %s failed", p->zName);
            goto out;
        }
        if (p->eLock >= SQLITE_LOCK_SHARED &&
                sx_retry_busy(sx_relock_shared_cb, p) != SQLITE_OK) {
            rc = sx_err(err, errlen, "relock %s failed", p->zName);
            goto out;
        }
        p->parked = 0;
    }
    for (node = g_shmnodes; node != NULL; node = node->next) {
        int i;
        if (node->fd >= 0) continue;
        node->fd = open(node->zShmPath, O_RDWR | O_CREAT | O_NOFOLLOW, 0644);
        if (node->fd < 0) {
            rc = sx_err(err, errlen, "reopen %s failed", node->zShmPath);
            goto out;
        }
        if (sx_retry_busy(sx_dms_cb, node) != SQLITE_OK) {
            rc = sx_err(err, errlen, "shm deadman lock on %s failed", node->zShmPath);
            goto out;
        }
        if (sx_shm_extend(node, (off_t)node->nRegion * node->szRegion) != SQLITE_OK) {
            rc = sx_err(err, errlen, "shm extend %s failed", node->zShmPath);
            goto out;
        }
        for (i = 0; i < node->nRegion; i++) {
            if (mmap(node->apRegion[i], node->szRegion, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_FIXED, node->fd,
                    (off_t)i * node->szRegion) == MAP_FAILED) {
                rc = sx_err(err, errlen, "shm remap: %s", strerror(errno));
                goto out;
            }
        }
    }
out:
    pthread_mutex_unlock(&g_reg_mu);
    return rc;
}

int sx_app_unpark_open(char *err, int errlen) {
    (void)err;
    (void)errlen;
    pthread_mutex_lock(&g_gate_mu);
    g_parked = 0;
    pthread_cond_broadcast(&g_gate_cv);
    pthread_mutex_unlock(&g_gate_mu);
    return 0;
}
