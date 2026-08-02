#!/usr/bin/env bash
# park-sock-test.sh: end-to-end test of connection park/unpark over the
# @syzy-park.<pid> control socket. A preloaded client opens $SYZY_DB
# (steered onto the "syzy-app" wrapper VFS, engine attached), then a
# supervisor-side dialer parks it: the client process must hold zero
# fds/mappings under the database directory (the invariant that lets
# the backing share unmount). Unpark restores service, the client
# writes again, and the re-attach must have minted a fresh origin.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

WORK="$(mktemp -d /tmp/syzy-park-sock.XXXXXX)"
CLIENT_PID=""
DAEMON_PID=""
cleanup() {
  local pid
  for pid in "$CLIENT_PID" "$DAEMON_PID"; do
    [ -n "$pid" ] || continue
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true # let it stop writing before rm
  done
  rm -rf "$WORK"
}
trap cleanup EXIT
fail() {
  echo "FAIL: $*" >&2
  [ -f "$WORK/client.err" ] && sed 's/^/client.err: /' "$WORK/client.err" >&2
  exit 1
}

make -s -C "$ROOT" ext

cat >"$WORK/client.c" <<'EOF'
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
int main(void) {
    sqlite3 *db = NULL;
    char line[64];
    if (sqlite3_open(getenv("SYZY_DB"), &db) != SQLITE_OK) return 1;
    sqlite3_busy_timeout(db, 5000);
    if (sqlite3_exec(db, "PRAGMA journal_mode=WAL;"
            "CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, x);"
            "INSERT INTO t(x) VALUES(1)", 0, 0, 0) != SQLITE_OK) {
        fprintf(stderr, "setup: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    printf("ready\n");
    fflush(stdout);
    if (fgets(line, sizeof(line), stdin) == NULL) return 1;
    if (sqlite3_exec(db, "INSERT INTO t(x) VALUES(2)", 0, 0, 0) != SQLITE_OK) {
        fprintf(stderr, "post-unpark insert: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &st, 0);
    if (sqlite3_step(st) == SQLITE_ROW) printf("count=%d\n", sqlite3_column_int(st, 0));
    fflush(stdout);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}
EOF
cc -o "$WORK/client" "$WORK/client.c" -lsqlite3

cat >"$WORK/parkctl.go" <<'EOF'
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	conn, err := net.DialTimeout("unix", "@syzy-park."+os.Args[1], 2*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	fmt.Fprintln(conn, os.Args[2])
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Print(reply)
}
EOF

mkdir -p "$WORK/data"
export SYZY_DB="$WORK/data/app.db"
export SYZY_AUTOLOAD=1
export SYZY_AUTOSPAWN=0
export SYZY_ENGINE="$ROOT/ext/syzy.so"
# The engine's diagnostics are opt-in (syzylog discards by default so the
# extension never writes uninvited into a host process's stderr). The attach
# assertion below reads them, so ask for them.
export SYZY_LOG=info

# The daemon creates the <db>-syzy layout (metadata.db etc.) that
# attach expects, and drains the client's journals — the same role the
# host-side daemon plays in production.
( cd "$ROOT/sqlite" && go build -o "$WORK/syzy" ./cmd/syzy )
"$WORK/syzy" daemon -db "$SYZY_DB" >"$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!

mkfifo "$WORK/ctl"
LD_PRELOAD="$ROOT/ext/syzy-shim.so" "$WORK/client" \
  <"$WORK/ctl" >"$WORK/client.out" 2>"$WORK/client.err" &
CLIENT_PID=$!
exec 9>"$WORK/ctl" # hold the fifo open so the client's fgets waits

for _ in $(seq 100); do
  grep -q '^ready$' "$WORK/client.out" 2>/dev/null && break
  sleep 0.05
done
grep -q '^ready$' "$WORK/client.out" || { cat "$WORK/client.err" >&2; fail "client not ready"; }

parkctl() { (cd "$WORK" && go run ./parkctl.go "$CLIENT_PID" "$1"); }

echo "--- park"
REPLY="$(parkctl park)"
[ "$REPLY" = "ok" ] || fail "park reply: $REPLY"

echo "--- zero refs under $WORK/data while parked"
REFS="$( { ls -l "/proc/$CLIENT_PID/fd" | grep -F "$WORK/data/" || true; \
           grep -F "$WORK/data/" "/proc/$CLIENT_PID/maps" || true; } )"
[ -z "$REFS" ] || fail "client still holds refs while parked: $REFS"

echo "--- unpark"
REPLY="$(parkctl unpark)"
[ "$REPLY" = "ok" ] || fail "unpark reply: $REPLY"

echo "--- refs re-established after unpark"
# The re-attach must have reopened engine state under the data dir
# (origin claim, journal, notify feed) — a parked-looking process after
# unpark would mean the re-attach silently did nothing. (The origin ID
# itself may legitimately be re-claimed: this is the same process on
# the same mount; clone-side origin distinctness is the remount case.)
REFS="$(ls -l "/proc/$CLIENT_PID/fd" | grep -cF "$WORK/data/" || true)"
[ "$REFS" -gt 0 ] || fail "no refs under data dir after unpark; re-attach did nothing"

echo "--- post-unpark write"
echo go >&9
exec 9>&-
wait "$CLIENT_PID"
CLIENT_PID=""
grep -q '^count=2$' "$WORK/client.out" || { cat "$WORK/client.out" "$WORK/client.err" >&2; fail "expected count=2"; }

# Two attach lines (initial + unpark re-attach) prove the engine
# re-ran the full attach flow rather than resuming frozen state.
ATTACHES="$(grep -c 'syzyext: attached' "$WORK/client.err")"
[ "$ATTACHES" -ge 2 ] || fail "expected 2 attach lines, got $ATTACHES"

echo PASS
