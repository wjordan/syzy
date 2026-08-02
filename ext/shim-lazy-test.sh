#!/usr/bin/env bash
# shim-lazy-test.sh: end-to-end test of the standalone lazy preload
# (ext/syzy-shim.so). Two parts:
#   1. behavior parity: the full ext-rewrite-test.sh suite re-run with
#      the lazy shim preloaded and the monolith as SYZY_ENGINE
#   2. laziness itself:
#      - unrelated DB: engine unmapped, exactly one thread (no Go
#        runtime), setgid/setuid to current ids succeed
#      - matching DB: engine mapped, Go runtime threads present
#      - matching DB with a missing engine: the open fails LOUD
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== 1: rewrite suite under lazy shim"
SHIM_MODE=lazy "$ROOT/ext/ext-rewrite-test.sh"

echo "=== 2: lazy-specific assertions"
WORK="$(mktemp -d /tmp/syzy-shim-lazy.XXXXXX)"
DAEMON_PID=""
cleanup() {
  if [ -n "$DAEMON_PID" ]; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true # let it stop writing before rm
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

cc -o "$WORK/client" "$ROOT/ext/ext-rewrite-client.c" -lsqlite3
( cd "$ROOT/sqlite" && go build -o "$WORK/syzy" ./cmd/syzy )

DB="$WORK/app.db"
ENGINE="$ROOT/ext/syzy.so"
"$WORK/syzy" daemon -db "$DB" >"$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!

export LD_PRELOAD="$ROOT/ext/syzy-shim.so"
export SYZY_ENGINE="$ENGINE"
export SYZY_DB="$DB"
export SYZY_AUTOLOAD=1
export SYZY_AUTOSPAWN=0

echo "--- unrelated DB: no engine, one thread, setxid works"
"$WORK/client" "$WORK/other.db" lazycheck "$ENGINE" unloaded

echo "--- matching DB: engine loads, attach works"
ready=""
for _ in $(seq 1 100); do
  if "$WORK/client" "$DB" query "SELECT 1" >/dev/null 2>"$WORK/probe.err"; then ready=1; break; fi
  sleep 0.1
done
[ -n "$ready" ] || fail "daemon not ready: $(cat "$WORK/probe.err"; tail -3 "$WORK/daemon.log" 2>/dev/null)"
"$WORK/client" "$DB" lazycheck "$ENGINE" loaded
# Attach side effect: the shim-created probe table was rewritten.
s="$(sqlite3 "$DB" "SELECT sql FROM sqlite_master WHERE name='lazyprobe'")"
grep -q "gen_id('lazyprobe')" <<<"$s" || fail "lazyprobe not rewritten (engine attach missing): $s"

echo "--- runtime whose sqlite binding is dlopen'd RTLD_LOCAL"
# CPython imports its sqlite3 module with RTLD_NOW and no RTLD_GLOBAL, so
# libsqlite3 lands in a local scope that dlsym(RTLD_NEXT) cannot reach. The
# binding's calls still arrive here (global scope is searched first), so an
# interposer that only consults RTLD_NEXT gets the call and has nothing to
# forward to. That used to segfault on the first connect().
if command -v python3 >/dev/null 2>&1 && python3 -c "import sqlite3" 2>/dev/null; then
  py() { python3 -c "import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute('SELECT 42').fetchone()[0])" "$1"; }
  [ "$(py ':memory:')" = 42 ] || fail "python3 :memory: under the shim"
  [ "$(py "$WORK/py.db")" = 42 ] || fail "python3 unrelated file DB under the shim"
  [ "$(py "$DB")" = 42 ] || fail "python3 on the attached DB under the shim"
else
  echo "    (skipped: no python3 with sqlite3)"
fi

echo "--- matching DB with missing engine: open fails loud"
if SYZY_ENGINE="$WORK/nonexistent.so" "$WORK/client" "$DB" exec "SELECT 1" 2>"$WORK/loud.err"; then
  fail "open succeeded with a missing engine (silent fallthrough)"
fi
grep -q "syzy-shim" "$WORK/loud.err" || fail "no loud engine error: $(cat "$WORK/loud.err")"

echo "PASS"
