#!/usr/bin/env bash
# Live-Postgres container for the PG engine test suite, used by CI and
# local runs so the recipe lives in one place.
#
# Contract: tests honor SYZY_PG_TEST_URL. Set → tests MUST connect and
# fail (not skip) if they can't; unset → PG tests skip. CI always sets
# it on Linux, so a broken container fails the build instead of
# silently skipping the suite.
#
# fsync=off is a test-only speed setting. synchronous_commit=off must
# NOT be added: it breaks the keepalive-LSN coherence the capture path
# relies on.
set -euo pipefail

NAME=syzy-pg-test
PORT=${SYZY_PG_TEST_PORT:-5433}
IMAGE=postgres:17
# Coordinated uniqueness needs Postgres to dial BACK into the sidecar, over
# a unix socket. In a container that means a bind mount both sides can see;
# tests find it via SYZY_PG_TEST_SOCKDIR. Mounted at the same path inside so
# one conninfo works from either side.
SOCKDIR=${SYZY_PG_TEST_SOCKDIR:-/tmp/syzy-pg-test-sock}

case "${1:-start}" in
start)
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  # World-writable: the sidecar (the test process) creates sockets here and
  # the server's postgres user connects to them, and the two are different
  # uids — more so under rootless containers, which remap them again.
  mkdir -p "$SOCKDIR"
  chmod 777 "$SOCKDIR"
  docker run -d --name "$NAME" -p "$PORT:5432" \
    -e POSTGRES_PASSWORD=syzy \
    -v "$SOCKDIR:$SOCKDIR:z" \
    "$IMAGE" \
    -c wal_level=logical \
    -c max_replication_slots=64 \
    -c max_wal_senders=64 \
    -c max_connections=500 \
    -c fsync=off >/dev/null
  # -h 127.0.0.1 forces TCP: the image's initdb phase runs a temp
  # socket-only server that pg_isready would otherwise report ready.
  for _ in $(seq 1 120); do
    if docker exec "$NAME" pg_isready -q -h 127.0.0.1 -U postgres 2>/dev/null; then
      wal=$(docker exec "$NAME" psql -q -h 127.0.0.1 -U postgres -tAc "show wal_level")
      if [ "$wal" != "logical" ]; then
        echo "wal_level=$wal, want logical" >&2
        exit 1
      fi
      echo "postgres://postgres:syzy@localhost:$PORT/"
      exit 0
    fi
    sleep 0.5
  done
  echo "postgres container failed to become ready" >&2
  docker logs "$NAME" >&2 || true
  exit 1
  ;;
stop)
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  ;;
url)
  echo "postgres://postgres:syzy@localhost:$PORT/"
  ;;
*)
  echo "usage: $0 [start|stop|url]" >&2
  exit 2
  ;;
esac
