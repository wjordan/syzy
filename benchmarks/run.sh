#!/usr/bin/env bash
# benchmarks/run.sh — convenience wrapper for the bench harness.
#
# Default: full report at the canonical -benchtime/-count. Pass -h to
# the underlying Go program for flags (e.g., -filter, -benchtime).

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# Pin to 2 cores with GOMAXPROCS=2 to remove scheduler/cache-line noise
# from the round-trip benches (publisher + subscriber goroutines bouncing
# across 24 logical CPUs adds ~3us of variance). Falls back to plain
# `go run` if taskset isn't available (non-Linux).
if command -v taskset >/dev/null 2>&1; then
	exec taskset -c 0,1 env GOMAXPROCS=2 go run ./benchmarks "$@"
fi
exec env GOMAXPROCS=2 go run ./benchmarks "$@"
