# Golden wire-v1 fixtures

`wirev1_full.bin` / `wirev1_empty.bin` are Changesets encoded by the
**pre-bump wire-v1 encoder** (parent of d558c8d, the WireVersion 1→2
bump). They pin the legacy decode path (`decodeColumnsV1`): production
fleets carry v1 payloads in mirror journals and epoch history across
the rolling upgrade.

Generated once by running, in a worktree at `d558c8d^`, a small
program that calls that commit's `crdt.Build` with the inputs asserted
in `wirev1_golden_test.go` (all four record ops; ColValue tags
Int/Real/Text/Blob/Null) and writes `Changeset.Encoded()` verbatim.
Do not regenerate with a current encoder — the committed bytes ARE the
compatibility contract.
