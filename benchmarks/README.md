# syzy benchmarks

Regenerate with `./benchmarks/run.sh`.

AMD Ryzen 9 7900X 12-Core Processor · go1.26.4 · linux/amd64 · kernel 6.17.0-22-generic · `a16d19b` · `-benchtime=50000x -count=5` · 2026-07-11 08:10:03 UTC

## Local

Local commit-thread cost — no replication.

| Scenario | ns/op | ops/sec | Δ vs baseline | allocs/op | B/op |
|---|---:|---:|---:|---:|---:|
| sqlite (single) | 8,626 | 115,929 | — | 1 | 8 |
| syzy (single) | 10,335 | 96,759 | +1,709 ns (19.8%) | 18 | 1774 |
| sqlite (batch=8) | 2,530 | 395,257 | — | 1 | 8 |
| syzy (batch=8) | 3,407 | 293,513 | +877 ns (34.7%) | 9 | 1439 |
| sqlite (batch=64) | 1,565 | 638,978 | — | 1 | 8 |
| syzy (batch=64) | 2,252 | 444,050 | +687 ns (43.9%) | 8 | 1749 |
| sqlite (batch=512) | 1,046 | 956,023 | — | 1 | 8 |
| syzy (batch=512) | 1,639 | 610,128 | +593 ns (56.7%) | 8 | 1986 |

## Replication

Both syzy modes are paired against stock SQLite at the same batch size. **round-trip** waits per row for B to apply (the latency-floor measurement: A's commit serialized with B's apply). **pipelined** issues all INSERTs back-to-back with one WaitApplied at the end (the steady-state throughput measurement: A's commit, drainer encode, and B's apply run concurrently). In-process transport, so this is the protocol-overhead floor — production network latency adds to round-trip and may also bound pipelined when the slow side becomes the network rather than B's apply.

| Scenario | ns/op | ops/sec | Δ vs baseline | allocs/op | B/op |
|---|---:|---:|---:|---:|---:|
| sqlite (single) | 8,626 | 115,929 | — | 1 | 8 |
| syzy round-trip (single) | 34,255 | 29,193 | +25,629 ns (297.1%) | 50 | 4871 |
| syzy pipelined (single) | 19,314 | 51,776 | +10,688 ns (123.9%) | 43 | 4662 |
| sqlite (batch=8) | 2,530 | 395,257 | — | 1 | 8 |
| syzy round-trip (batch=8) | 9,922 | 100,786 | +7,392 ns (292.2%) | 22 | 3580 |
| syzy pipelined (batch=8) | 6,246 | 160,102 | +3,716 ns (146.9%) | 21 | 3593 |
| sqlite (batch=64) | 1,565 | 638,978 | — | 1 | 8 |
| syzy round-trip (batch=64) | 6,165 | 162,206 | +4,600 ns (293.9%) | 18 | 3744 |
| syzy pipelined (batch=64) | 3,891 | 257,003 | +2,326 ns (148.6%) | 17 | 3938 |
| sqlite (batch=512) | 1,046 | 956,023 | — | 1 | 8 |
| syzy round-trip (batch=512) | 4,700 | 212,766 | +3,654 ns (349.3%) | 17 | 3939 |
| syzy pipelined (batch=512) | 3,295 | 303,490 | +2,249 ns (215.0%) | 17 | 4374 |

## Producer commit-thread latency

Per-iteration time covers SQLite's own statement execution + WAL fsync, plus syzy's commit-thread work.

| Scenario | ns/op | ops/sec | Δ vs baseline | allocs/op | B/op |
|---|---:|---:|---:|---:|---:|
| Single-row INSERT (full pipeline) | 10,349 | 96,628 | +1,360 ns (15.1%) | 18 | 1775 |
| Single-row UPDATE (full pipeline) | 7,525 | 132,890 | +737 ns (10.9%) | 20 | 1270 |
| Fixture floor: INSERT (no-op walHook) | 8,989 | 111,247 | — | 2 | 15 |
| Fixture floor: UPDATE (no-op walHook) | 6,788 | 147,319 | — | 2 | 23 |

## Components

| Scenario | ns/op | ops/sec | Δ vs baseline | allocs/op | B/op |
|---|---:|---:|---:|---:|---:|
| Journal append (small payload) | 91 | 11,036,309 | — | 0 | 0 |
| CRDT changeset build (INSERT) | 246 | 4,061,738 | — | 6 | 448 |
| CRDT changeset decode (INSERT) | 232 | 4,317,789 | — | 5 | 448 |
| Broker apply: INSERT | 10,260 | 97,466 | — | 8 | 900 |


Numbers are medians across runs (first dropped as warmup). Microbenchmarks; real-world latency depends on disk speed, fsync mode, and host load. `Δ vs baseline` is the scenario's overhead vs its declared comparison row (the matching stock-SQLite row at the same batch size).
