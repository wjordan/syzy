# Updating the vendored SQLite amalgamation

The amalgamation is committed in-tree (`sqlite3.c`, `sqlite3.h`, `sqlite3ext.h`)
so `go install`/`go test` work without an external fetch step. To bump the
version:

1. Find the desired release on <https://sqlite.org/download.html>. Note the
   release year (used in the URL path) and the published SHA3-256 hash.

2. Download and verify:

   ```sh
   curl -fsSL -o /tmp/sqlite.zip \
     https://sqlite.org/<YEAR>/sqlite-amalgamation-<VER>.zip
   openssl dgst -sha3-256 /tmp/sqlite.zip
   # must match the SHA3-256 published on sqlite.org/download.html
   ```

3. Replace the three source files in this directory with the contents of the
   extracted `sqlite-amalgamation-<VER>/` directory:

   ```sh
   unzip -j /tmp/sqlite.zip 'sqlite-amalgamation-*/sqlite3.c' \
     'sqlite-amalgamation-*/sqlite3.h' 'sqlite-amalgamation-*/sqlite3ext.h' \
     -d third_party/sqlite/
   ```

4. Update `VERSION` in this directory to the new dotted version
   (e.g. `3.53.1`).

5. Run `go test ./...` and the bench suite. Note any preupdate-API or
   trace_v2-API changes in the release notes — Syzy depends on
   `sqlite3_preupdate_*` and `sqlite3_trace_v2`.

The repo's `.gitattributes` marks `sqlite3.c` and `sqlite3.h` as
`linguist-vendored` so they don't dominate GitHub's language stats.

## Pinned compile flags

Build flags live in the cgo directives of `sqlitebridge` (added in
phase 2). The relevant defines for Syzy:

- `SQLITE_ENABLE_PREUPDATE_HOOK` — required for replication capture.
- `SQLITE_THREADSAFE=2` — multi-threaded, callers serialize per connection.
- `SQLITE_DEFAULT_WAL_SYNCHRONOUS=1` — WAL `synchronous=NORMAL` default
  matches the engine's documented durability stance.
- `SQLITE_OMIT_DEPRECATED` — drop legacy APIs Syzy never calls.

These are set in `sqlitebridge/cgo.go` and are not exposed as build
tags; we want one canonical SQLite build for the engine.
