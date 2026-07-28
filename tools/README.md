# tools

Developer diagnostics — not part of the production surface.

- `jprobe` — diagnose a WaitForDrain that never converges: decode
  journal state, CAS counters, drain positions.
- `jinspect` — dump a journal segment's records.
- `syzy-dumpcs` — decode and print a changeset payload.

Production binaries live in `cmd/`.
