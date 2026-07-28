# Syzy SQLite Extension

`ext/syzy.so` is a SQLite loadable extension that turns any SQLite
client (sqlite3 CLI, Python `sqlite3`, Node `better-sqlite3`, Go's
`mattn/go-sqlite3`, Rust `rusqlite`, etc.) into a syzy producer for
its app database. Replication, broadcast, and inbound apply happen
in a separate `syzy daemon` process that the extension auto-spawns
the first time it's loaded against a fresh database.

The extension and the `syzy` CLI ship together; both must be on the
host. The extension auto-spawns `syzy daemon` via `SYZY_BIN` (default:
`syzy` on PATH).

## Building

From the repository root:

```bash
make build   # syzy CLI → bin/syzy
make ext     # extension → ext/syzy.so, preload shim → ext/syzy-shim.so
export PATH="$PWD/bin:$PATH"
```

Requirements: Go 1.25+ and a C compiler. `make ext` is a cgo
`c-shared` build against the SQLite amalgamation vendored in
`third_party/sqlite/`, so no system SQLite development headers are
needed. The `PATH` export (or `SYZY_BIN`) is what lets the extension
auto-spawn the daemon.

The SQLite *client* that loads the extension must be built with
`SQLITE_ENABLE_PREUPDATE_HOOK`. Most distro packages
(Debian/Ubuntu/Homebrew) include it; `.load` fails loud when it's
absent.

## Loading

```bash
sqlite3 app.db
sqlite> .load ./ext/syzy
sqlite> CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, value TEXT);
sqlite> INSERT INTO t VALUES ('a', 'hello');
```

That's it. The extension installs hooks on the connection, claims
its own per-process origin slot under `app.db-syzy/origins/<hex>/`,
and journals every replicated DML write. The auto-spawned daemon
drains that journal and broadcasts to peers.

DDL replicates from the extension by default — the schema log lives
at `<app.db>-syzy/schema.db` (a sibling of the auto-defaulted object
backend). Override with `SYZY_CLUSTER=<url>` (file:// or s3://) for
multi-peer setups, or with the granular `SYZY_SCHEMA_LOG{,_DIAL,_S3}`
env vars when you want the schema log on a different store than the
objects.

Direct `.load` clients must declare generated integer keys explicitly as `INT
PRIMARY KEY NOT NULL DEFAULT (gen_id('table_name'))`; the linked Go API and
`LD_PRELOAD` path can rewrite a bare `INTEGER PRIMARY KEY` automatically. See
[SQL preprocessing](../sqlite/docs/DDL.md#sql-preprocessing) for the
integration-path details.

Incremental `sqlite3_blob_write()` does not yet replicate via the
extension; full-row BLOB `INSERT` / `UPDATE` values replicate as
ordinary DML.

Exit the SQLite client when done — the daemon keeps running
detached, so the next client load on the same database reuses it.

## LD_PRELOAD auto-attach

For apps whose SQLite client can't issue `.load` (or shouldn't need
to), preload the standalone shim and point it at the target database:

```bash
LD_PRELOAD=/usr/local/lib/syzy-shim.so \
SYZY_ENGINE=/usr/local/lib/syzy-engine.so \
SYZY_DB=/data/app.db SYZY_AUTOLOAD=1 myapp
```

`syzy-shim.so` is pure C (no Go linked; built by `make -C ext shim`
from the same interposer source as the monolith). It registers a
path-aware `sqlite3_auto_extension` hook and loads the Syzy extension —
the monolithic `syzy.so`, at `SYZY_ENGINE` (default
`/usr/local/lib/syzy-engine.so`) — only when an open's canonical path
matches `SYZY_DB`. Preloaded processes that never open the target DB
therefore never start the Go runtime: no extra threads (musl's setxid
broadcast in priv-dropping entrypoints like `setpriv`/`su-exec` stays
intact), no signal handlers, no memory cost. If the path matches but
the extension can't be loaded, the open fails loud (`SQLITE_ERROR`)
rather than silently writing unreplicated.

| Var             | Default                          | Effect |
|-----------------|----------------------------------|--------|
| `SYZY_AUTOLOAD` | unset                            | `1` enables the preload auto-attach hook |
| `SYZY_DB`       | unset                            | Canonical path of the database to attach to |
| `SYZY_ENGINE`   | `/usr/local/lib/syzy-engine.so`  | Extension `.so` the shim loads on first match |

## Host SQLite requirements

The host's `libsqlite3` **must** be built with
`SQLITE_ENABLE_PREUPDATE_HOOK`. Standard distro packages include it:

- Debian / Ubuntu (`libsqlite3-0`): yes
- macOS Homebrew (`sqlite`): yes
- Python 3 stdlib `sqlite3` on most platforms: yes
- Alpine `sqlite-libs`: **no** (compile your own with the flag)

When missing, `.load syzy` fails loud with:

```
syzy: sqlite3_preupdate_count missing — host sqlite3 was not
built with SQLITE_ENABLE_PREUPDATE_HOOK
```

The producer's DML capture relies on preupdate. SQLite intentionally
keeps this API out of `sqlite3_api_routines`; the extension uses
`dlsym(RTLD_DEFAULT, …)` to grab the symbols at init time. They
exist in any libsqlite3 compiled with the flag.

## Environment variables

Most users need none. The defaults derive everything from
`<app.db>-syzy/`. Set `SYZY_CLUSTER=s3://bkt/myapp` for production.

| Var                    | Default       | Effect |
|------------------------|---------------|--------|
| `SYZY_BIN`             | `syzy` (PATH) | Path to the syzy CLI used for auto-spawn |
| `SYZY_CLUSTER`         | unset         | Shared cluster root URL (file:// or s3://). Persisted to metadata on first init; subsequent reopens read from the metadata. Mismatched env vs. persisted root fails loud. |
| `SYZY_LISTEN`          | unset         | `--listen` forwarded. Default behavior: unix socket for file:// clusters, `:7000` for s3://. |
| `SYZY_BUNDLE_LISTEN`   | unset         | `--bundle-listen` forwarded. Default behavior parallel to SYZY_LISTEN. |
| `SYZY_SEEDS`           | unset         | `--seeds` forwarded |
| `SYZY_OBJECT_BACKEND`  | unset         | Literal object-backend URL; use only when objects must live separately from the schema log. |
| `SYZY_SCHEMA_LOG`      | unset         | Override: file-backed schema log. |
| `SYZY_SCHEMA_LOG_DIAL` | unset         | Override: follow a remote schema log over TCP, `unix:`, or `vsock:`. |
| `SYZY_SCHEMA_LOG_S3`   | unset         | Override: explicit S3 bucket URL for the schema log. |
| `SYZY_AUTOSPAWN`       | `1`           | Set `0` to skip auto-spawn (start daemon manually) |

The auto-spawned daemon is a normal `syzy daemon` process — same
binary, same flags, idle-exits after 5 minutes with no attached
extension clients. For systemd-managed setups, run `syzy daemon
--idle-timeout 0 --cluster <url> --db <path>` and the extension's
`.load syzy` will attach to it via the per-DB control socket
(`$XDG_RUNTIME_DIR/syzy/<db-hash>.sock`) instead of spawning.

## Host-process integration caveats

**No pre-fork loading.** Don't `.load syzy` before forking workers
(uWSGI master, Gunicorn preload, etc.). The extension claims a
per-process origin via flock; forked children would inherit the fd
and step on each other. Each worker should `.load syzy` after fork.

**Signal handlers.** SQLite extensions inherit the host's signal
handlers. The Go runtime inside `syzy.so` installs handlers for
SIGINT, SIGTERM, SIGSEGV, SIGFPE, and a few others when initialized.
On hosts that already have application-level handlers for those,
behavior is host-specific (Go usually chains, but some hosts
override). Test under load before relying.

**Fork safety after load.** Once the extension is loaded, the host
process holds: an open writer connection (with hooks), a flock on
the per-origin directory, an mmap of the journal segment. Forking
(`fork()` without `exec`) duplicates all of this in the child, but
the journal mmap is shared and the flock is **not** preserved across
fork — the child's writes wouldn't be properly serialized. Don't
fork after `.load syzy`. Use `fork()+exec()` (which restarts the
process and drops loaded extensions) or load the extension after
fork in each child.

**dlclose limitations.** Unloading `syzy.so` mid-process is unsafe.
The Go runtime can't be cleanly torn down, the daemon-role flock
state may survive, and any background goroutines would be left
dangling. SQLite's extension model normally doesn't unload extensions
within a process lifetime; don't try.

**Privilege-dropping after extension load (`setpriv` breakage).**
Once the extension is loaded the process is multithreaded, and
`setpriv`-style privilege drops fail: on musl the broadcast
`setresgid` returns EPERM, on glibc libc aborts the process. This is
not a setxid-broadcast failure and not Go-specific: one idle C
pthread reproduces it. libc emulates POSIX process-wide setuid/setgid
by re-running the syscall on every thread (musl: SIGSYNCCALL over its
internal thread list; glibc: SIGSETXID), and Syzy's threads
cooperate correctly. What breaks is per-thread capability state:
`setpriv` drops uid first under `PR_SET_KEEPCAPS`, then re-raises
`CAP_SETGID` with `capset(2)`, and both affect only the calling
thread. Every other thread loses all capabilities at the uid
broadcast, so its `setresgid` in the next broadcast fails. Classic
root-first drops (`setgroups` then `setgid` then `setuid`, e.g.
`su-exec` or a plain `setuid()`) work with the extension loaded, on both
libcs. The lazy shim keeps entrypoints safe by construction:
`setpriv`/`su-exec` are fresh-exec'd binaries that never open
`$SYZY_DB`, so they carry only the single-threaded C shim. The one
unsafe pattern is a process that opens the shared DB and then performs
a KEEPCAPS-style uid-before-gid drop in the same process image; the
out-of-process daemon mode eliminates even that.

**Process exit cleanup.** When the host SQLite client exits, the OS
reclaims its origin flock, journal mmap, and writer connection. The
daemon's secondary drainer notices the journal segment didn't
grow further and stops draining; on the next extension load against
that database, the producer recycles the same origin slot
(`layout.Recycle`) and resumes journaling.

**Multiple writer processes per box.** Concurrent extension-loaded
clients on the same database get distinct origin slots automatically.
No coordination needed — `layout.Acquire` mints a fresh origin per
process. The daemon's secondary scan attaches a drainer for each
origin's journal directory it discovers under `origins/`.

## Comparison with the in-process Go library

| Aspect | Go library | Loadable extension |
|--------|-----------|--------------------|
| Setup | `sqlite.Open(...)` | `.load syzy` |
| Hooks installed on | Owned writer | Host's writer |
| Sink/drainer/broker | In-process | In daemon process |
| Snapshot/gossip | In-process | In daemon process |
| Cross-language support | Go only | Any SQLite client |
| Daemon required | No (single-node) / inline | Yes (auto-spawned) |
| Replicated DDL | Direct via `sqlite.Config.SchemaLog` | Via `SYZY_SCHEMA_LOG{,_DIAL,_S3}` env vars |
| HLC monotonicity | Single Cache | Per-process Cache, daemon coordinates via shared sender_seq |

Use the Go library when your application is Go and you want
everything in one process. Use the extension to bring syzy into
Python/Node/Ruby/Rust/Erlang/etc. apps without writing a Go shim,
or when you want the daemon's broadcast pipeline isolated from the
host process for stability.

## Diagnostics

Auto-spawned daemon logs to `<app.db>-syzy/daemon.log`. Useful when
the extension reports an init error — the daemon may have failed to
take its lock.

```bash
syzy status --db app.db   # cluster_id, frontier, schema_seq
syzy check --db app.db    # journal integrity scan
```

If the extension hangs on `.load`, check `<app.db>-syzy/daemon.log`
for daemon-side startup failures (typically a port binding conflict
on `SYZY_LISTEN`).
