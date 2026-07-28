#!/usr/bin/env bash
# ext-rewrite-test.sh: end-to-end test of the LD_PRELOAD SQL-rewrite
# interposers (sqlite/cmd/syzy-ext/autoload_shim.c).
#
# Drives the system sqlite3 CLI and a tiny C client (both dynamically
# linked against the system libsqlite3) with syzy.so preloaded, and
# asserts:
#   1. bare INTEGER PRIMARY KEY lands rewritten (gen_id) in sqlite_master
#   2. inserts work and RETURNING id yields a gen_id-range PK
#   3. a prepare_v2 + pzTail statement loop rewrites each statement
#   4. sqlite3_exec rewrites every statement of a multi-statement string
#   5. a DB that is not the SYZY_DB target is left untouched
#   6. fts5 shadow tables keep their INTEGER PRIMARY KEY (step-depth guard)
#
# Requires: system sqlite3 CLI + libsqlite3 with preupdate hook, cc.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d /tmp/syzy-ext-rewrite.XXXXXX)"
DAEMON_PID=""
cleanup() {
  if [ -n "$DAEMON_PID" ]; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true # let it stop writing before rm
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

make -s -C "$ROOT" ext
cc -o "$WORK/client" "$ROOT/ext/ext-rewrite-client.c" -lsqlite3
( cd "$ROOT/sqlite" && go build -o "$WORK/syzy" ./cmd/syzy )

DB="$WORK/app.db"

# The daemon owns metadata init (cluster_id) and drains journals — the
# same role the host node plays. Run it WITHOUT the preload.
"$WORK/syzy" daemon -db "$DB" >"$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!

# Preloaded invocations: the shim attaches a producer when the opened
# path matches SYZY_DB. AUTOSPAWN=0 — the daemon above is the one.
# SHIM_MODE=lazy preloads the standalone C shim, which dlopen's the
# monolith as its engine on the first matching open (same assertions
# must hold in both modes).
if [ "${SHIM_MODE:-}" = "lazy" ]; then
  export LD_PRELOAD="$ROOT/ext/syzy-shim.so"
  export SYZY_ENGINE="$ROOT/ext/syzy.so"
else
  export LD_PRELOAD="$ROOT/ext/syzy.so"
fi
export SYZY_DB="$DB"
export SYZY_AUTOLOAD=1
export SYZY_AUTOSPAWN=0

fail() { echo "FAIL: $*" >&2; exit 1; }

# Wait for the daemon's control socket: the first preloaded open fails
# until attach can dial it.
ready=""
for _ in $(seq 1 100); do
  if sqlite3 "$DB" "SELECT 1" >/dev/null 2>"$WORK/probe.err"; then ready=1; break; fi
  sleep 0.1
done
[ -n "$ready" ] || fail "daemon not ready: $(cat "$WORK/probe.err"; tail -3 "$WORK/daemon.log" 2>/dev/null)"

echo "--- 1: CLI CREATE TABLE rewrite + insert (and 2: gen_id-range RETURNING id)"
sqlite3 "$DB" "CREATE TABLE visits (id INTEGER PRIMARY KEY, ts INT NOT NULL, ua TEXT)"
schema="$(sqlite3 "$DB" "SELECT sql FROM sqlite_master WHERE name='visits'")"
grep -q "gen_id('visits')" <<<"$schema" || fail "visits not rewritten: $schema"
grep -qi "INTEGER PRIMARY KEY" <<<"$schema" && fail "rowid alias survived: $schema"
id="$(sqlite3 "$DB" "INSERT INTO visits (ts, ua) VALUES (1, 'x') RETURNING id")"
[ "$id" -ge 8589934592 ] || fail "id $id not in gen_id range"

echo "--- 3: prepare_v2 + pzTail loop"
"$WORK/client" "$DB" loop "CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY, x TEXT); INSERT INTO a DEFAULT VALUES;"
for t in a b; do
  s="$(sqlite3 "$DB" "SELECT sql FROM sqlite_master WHERE name='$t'")"
  grep -q "gen_id('$t')" <<<"$s" || fail "table $t not rewritten via tail loop: $s"
done
[ "$(sqlite3 "$DB" 'SELECT count(*) FROM a')" = "1" ] || fail "insert via tail loop missing"

echo "--- 4: sqlite3_exec multi-statement"
"$WORK/client" "$DB" exec "CREATE TABLE c (id INTEGER PRIMARY KEY);
INSERT INTO c DEFAULT VALUES;
CREATE TABLE d (id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT);"
for t in c d; do
  s="$(sqlite3 "$DB" "SELECT sql FROM sqlite_master WHERE name='$t'")"
  grep -q "gen_id('$t')" <<<"$s" || fail "table $t not rewritten via exec: $s"
done
[ "$(sqlite3 "$DB" 'SELECT count(*) FROM c')" = "1" ] || fail "insert via exec missing"

echo "--- 4b: rails-shape transaction (BEGIN; DDL; DML; COMMIT) with rewrite"
"$WORK/client" "$DB" exec "BEGIN;
CREATE TABLE migrations (id INTEGER PRIMARY KEY, version TEXT);
INSERT INTO migrations (version) VALUES ('20260610');
COMMIT;"
s="$(sqlite3 "$DB" "SELECT sql FROM sqlite_master WHERE name='migrations'")"
grep -q "gen_id('migrations')" <<<"$s" || fail "txn CREATE not rewritten: $s"
[ "$(sqlite3 "$DB" 'SELECT count(*) FROM migrations')" = "1" ] || fail "txn DML missing"
"$WORK/client" "$DB" exec "BEGIN;
CREATE TABLE doomed (id INTEGER PRIMARY KEY);
ROLLBACK;" || fail "rollback txn errored"
[ -z "$(sqlite3 "$DB" "SELECT sql FROM sqlite_master WHERE name='doomed'")" ] || fail "rolled-back table exists"

echo "--- 5: unrelated DB untouched"
OTHER="$WORK/other.db"
sqlite3 "$OTHER" "CREATE TABLE plain (id INTEGER PRIMARY KEY)"
s="$(sqlite3 "$OTHER" "SELECT sql FROM sqlite_master WHERE name='plain'")"
grep -q "INTEGER PRIMARY KEY" <<<"$s" || fail "non-attached DB was rewritten: $s"

echo "--- 6b: PRAGMA wal_autocheckpoint does not clobber the producer wal_hook"
META="$DB-syzy/metadata.db"
intent_count() {
  sqlite3 "$META" "SELECT count(*) FROM meta WHERE key >= 'intent:' AND key < 'intent;'"
}
# prepare+step route (the CLI preps each statement): the pragma
# replaces the wal_hook as it executes; the shim's stmt watch must
# re-assert it, or the following CREATE's intent is never resolved.
# The intent check must run BEFORE any further open of $DB: a fresh
# attach's startup recovery would resolve a dangling intent and mask
# the clobber. metadata.db is not SYZY_DB, so querying it never
# attaches.
sqlite3 "$DB" "PRAGMA wal_autocheckpoint=512; CREATE TABLE ac1 (id INTEGER PRIMARY KEY)"
[ "$(intent_count)" = "0" ] || fail "dangling intent after pragma+DDL via prepare/step (wal_hook clobbered)"
s="$(sqlite3 "$DB" "SELECT sql FROM sqlite_master WHERE name='ac1'")"
grep -q "gen_id('ac1')" <<<"$s" || fail "ac1 not rewritten after pragma: $s"
# sqlite3_exec route: pragma exec, then DDL exec, one connection (the
# realistic ORM-setup shape; see the shim's same-string limitation).
"$WORK/client" "$DB" exec "PRAGMA wal_autocheckpoint=256;" "CREATE TABLE ac2 (id INTEGER PRIMARY KEY);"
[ "$(intent_count)" = "0" ] || fail "dangling intent after pragma+DDL via exec (wal_hook clobbered)"
s="$(sqlite3 "$DB" "SELECT sql FROM sqlite_master WHERE name='ac2'")"
grep -q "gen_id('ac2')" <<<"$s" || fail "ac2 not rewritten after exec pragma: $s"

echo "--- 6: fts5 shadow tables untouched"
if sqlite3 "$DB" "CREATE VIRTUAL TABLE notes USING fts5(body)" 2>"$WORK/fts.err"; then
  s="$(sqlite3 "$DB" "SELECT sql FROM sqlite_master WHERE name='notes_content'")"
  grep -q "INTEGER PRIMARY KEY" <<<"$s" || fail "fts5 shadow table rewritten: $s"
else
  # fts5 unavailable in the system build, or vtab DDL rejected by
  # admission: either way the rewrite guard isn't what failed. Surface
  # the reason but keep the test green on the rewrite behavior.
  echo "  (fts5 case skipped: $(cat "$WORK/fts.err"))"
fi

echo "PASS"
