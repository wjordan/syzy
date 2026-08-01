# Syzy CRDT Model — Specification

This is the math layer for Syzy's logical replication: the consistency
guarantees, the named invariants, the layer specification in Burckhardt
(vis, ar, F) form, and the prior-art the design rests on.

The arbitration rules, wire semantics, invariants, and consistency
claims stated here are **spec-authoritative**: the code must match
them, and disagreement is a bug in the code.
Go implementation shapes — type signatures, CL transition
helpers, codec internals — are **code-authoritative**: each section
links to the canonical Go file, which this doc sketches rather than
restates.

For surrounding docs see [ARCHITECTURE.md](ARCHITECTURE.md),
[PROTOCOL.md](PROTOCOL.md), [SCHEMA.md](SCHEMA.md),
[BLOB_PATCH.md](BLOB_PATCH.md), and [PRUNING.md](PRUNING.md).

## Consistency Model

Syzy provides **Transactional Causal Consistency with CRDT convergence
(TCC+)** per Akkoorath et al. *Cure* (ICDCS 2016) / AntidoteDB. In the
HAT taxonomy of Bailis et al. (VLDB 2014): **Monotonic Atomic View under
sticky-causal availability**.

Three guarantees follow:

- **Causal+:** if Changeset c1 is in the transitive `Deps` of c2, every
  replica that observes c2 has also observed c1.
- **Read Atomic** (Bailis et al., SIGMOD 2014): a Changeset's Records are
  visible together at every replica or none of them are. (On
  cell-group tables — `F_cell` — arbitration is per column, so a
  Record's columns apply independently; atomic visibility holds per
  cell, not per record.)
- **Strong Eventual Consistency:** replicas that have received the same
  set of Changesets have equivalent logical state under the admitted
  conflict layers. This does not claim byte-identical native database
  files. It reduces to op-commutativity for concurrent operations,
  causal delivery, and exactly-once apply, per Gomes et al. (OOPSLA
  2017).

SEC follows from invariants (3), (6), and (9) plus exactly-once delivery
(provided by the transport's `Subscribe + Fetch` plus the (4) frontier
check). This is the Kleppmann OOPSLA'17 framework instantiated at our
parameters; no fresh proof required.

Liveness is **bounded staleness** in Mahajan et al. *Depot* (OSDI 2010)
form: an offline replica past
`T_offline_deadline = T_announce + T_propagate + Δ` must be evicted
before its absence stops blocking stability. Eviction is
operator-driven (`syzy_evict`); see [PRUNING.md](PRUNING.md) for the
membership / horizon split (Bauwens & Gonzalez Boix MPLR 2020) — a
design-stage contract, not yet code.

## Invariants

CRDT.md is the authoritative index of these claims. Each is enforced by
exactly one layer; each is tested in the referenced test file — the
pure-model ones as `TestInvariant_*` in
[`invariants_test.go`](../crdt/invariants_test.go) — or noted as
enforced in another package.

1. **Dot uniqueness.** For any `(Origin, Seq)`, at most one Changeset
   exists in the cluster. *Producer-enforced* via `sender_seq.next_seq`
   monotonicity + origin rotation on unclean restart. Tested in
   `internal/producer`.
2. **Per-origin Clock monotonicity.** If `c1.Origin == c2.Origin` and
   `c1.Seq < c2.Seq`, then `c1.Clock < c2.Clock`. *Producer-enforced*
   via HLC `Update`. Tested in `internal/producer`.
3. **Causal closure of Deps.** For any Changeset `c`, every Dot in the
   transitive closure of `c.Deps` is causally < `c.Dot`. *Encoded at
   commit time.* Tested in
   [`deps_test.go`](../crdt/deps_test.go).
4. **Frontier monotonicity.** The per-origin applied frontier
   (`nodestate.Cache`) only grows. *Cache-enforced via `MarkApplied`.*
   Tested in `internal/nodestate`.
5. **Stamp arbitration totality.** For any two Stamps `s1`, `s2`,
   exactly one of `s1 < s2`, `s2 < s1`, `s1 == s2`. *By construction.*
   Tested in
   [`identity_test.go`](../crdt/identity_test.go) +
   [`invariants_test.go`](../crdt/invariants_test.go).
6. **Apply commutativity for concurrent ops.** If `c1` and `c2` are
   concurrent (neither in the other's transitive Deps),
   `Apply(c1) ∘ Apply(c2) ≡ Apply(c2) ∘ Apply(c1)`. *By LWW
   max-selection on `(CL, Stamp)`; for counter columns, by
   commutativity of addition within a CL generation.* Tested in
   [`invariants_test.go`](../crdt/invariants_test.go).
7. **Per-pk CL monotonicity.** `RowState.CL` only grows. *Metadata-
   enforced via UPSERT.* Tested in
   [`state_test.go`](../crdt/state_test.go).
8. **Schema chain totality.** Distinct SchemaEvents have distinct
   `schema_seq`. *Schema-log-enforced via CAS.* Tested in
   `schemalog`.
9. **Idempotent apply.** `Apply(c) ∘ Apply(c) ≡ Apply(c)`. *From
   the applied-frontier short-circuit (`Cache.IsAppliedRemote`) + LWW
   skip on equal
   Stamp. Counter contributions are not SQL-idempotent; for them the
   short-circuit is load-bearing and is made crash-durable by the
   applied marker written atomically with the DML (see `F_counter`
   under Layers).* Tested in
   [`invariants_test.go`](../crdt/invariants_test.go).
10. **Unique-key exclusivity.** For each active unique key `K` and each
    non-NULL value `v`, at most one live row `r` *that `K` admits*
    satisfies `canonical(r, K) = v` — every live row for a total key;
    for a partial key (`… WHERE <predicate>`), only rows the predicate
    admits, so a non-participating row (e.g. a soft-deleted one) may
    share `v` with the live owner. *Coordinated (`NOT NULL`) keys: by
    construction* — a linearizable reservation admits exactly one
    claimant cluster-wide before commit, so the invariant is never
    transiently violated. *Eventual (nullable) keys: apply-enforced* via
    cell-LWW arbitration with loser-null. See
    [SCHEMA.md](SCHEMA.md#unique-keys).

## Causal Length (CL)

Every pk carries a **causal length** — cr-sqlite's `cl` — a per-pk
generation counter whose parity encodes liveness:

- `CL == 0` — the pk has never existed;
- `CL` odd — row alive at generation `(CL+1)/2`;
- `CL` even — row tombstoned at generation `CL/2`.

Producer transitions: INSERT on a never-existed or locally-tombstoned
pk takes the next odd value (`max(CL observed) + 1`); UPDATE on a live
row leaves CL unchanged; DELETE on a live row takes `CL + 1` (the next
even value).

**Arbitration between two writes on the same pk is strict
lexicographic comparison of `(CL, Stamp)`, CL first**: the higher CL
wins outright; only equal CLs fall through to Stamp domination (the
`ar` order under Layers below). CL replaces the "tombstones win
unconditionally" carve-out of earlier designs. Per-cell and
per-byte-range overrides are scoped to the row's current CL; bumping
CL on resurrection implicitly tombstones prior-generation overrides
without explicit GC.

The Go shape is [`state.go`](../crdt/state.go) (`RowState`,
`DominatedBy`, `NextLiveCL`, `NextTombCL`).

## Layers — Burckhardt POPL'14 Specification

Each layer is a function `F : OperationContext → Value` parameterized by
`(vis, ar)`:

- `vis` — the set of operations the apply has observed
  (= the per-origin applied frontier plus out-of-order exception
  [`SeqSet`](../crdt/causal.go), held in `nodestate.Cache` —
  a causal context in the Almeida et al. §7.4 compressed form).
- `ar` — total order over visible operations
  (= [`Stamp`](../crdt/identity.go) lex order). Row/cell writes compare
  the causal length before `ar` — see
  [Causal Length (CL)](#causal-length-cl).

Four layers, one specification skeleton, where `Sintreg` denotes
sequential register semantics (apply in `ar` order, last write wins):

```text
F_row(o, vis, ar)     = Sintreg( (vis, ar) restricted to row(o) )
F_cell(o, vis, ar)    = Sintreg( (vis, ar) restricted to cell(o) )
F_range(o, vis, ar)   = per-byte LWW over (vis, ar) restricted to range(o)
F_counter(o, vis, _)  = Σ contributions in vis restricted to cell(o),
                        scoped to the row's current generation (CL)
```

`F_counter` governs declared **counter columns** (see
[SQLite DDL](../sqlite/docs/DDL.md#counter-columns) and the
[Postgres engine](postgres.md#8-merge-semantics-clock-groups-and-counters)). It
is the one layer that
ignores `ar` entirely: within a row generation, every operation on a
counter cell is a signed **contribution** — an UPDATE ships the delta
`NEW − OLD`, and a concurrent same-generation INSERT image contributes
its initial value — and the cell's value is the sum of all
contributions in `vis`. Addition is commutative and associative, so no
arbitration order is needed and no contribution is ever lost to a
concurrent writer. The record that *establishes* a generation on a
replica (the CL-bumping Insert, or an out-of-order Update racing ahead
of its resurrecting Insert) applies its contribution absolutely,
resetting the cell for the new generation — unless the physical row
already carries a same-generation contribution the row clock hasn't
caught up with (a locally-committed write still queued behind the
drain), in which case the establishing insert merges additively, since
every other replica sums both. Row liveness stays row-level: Delete bumps CL and
dominates all same-generation contributions, exactly as for the
register layers.

Because addition is not idempotent, `F_counter` leans on exactly-once
`vis` membership harder than the register layers do: a register
re-apply is a harmless UPSERT, a re-added delta is corruption. The
frontier (invariant 9) provides exactly-once in steady state; the
crash window between native DML commit and frontier persistence — which
registers cover by idempotent re-apply — must be closed for counter-bearing
changesets by a **durable applied marker** written inside the same native
transaction as the DML. The engine realizations are
[`apply.go`](../internal/broker/apply.go) and
[`pg/internal/postgres/apply.go`](../pg/internal/postgres/apply.go). On
redelivery of a marked changeset, counter
contributions are stripped and the remaining idempotent effects re-apply.

Interplay with `F_cell` on the same row: counter cells never carry
Stamps and are never gated on them; register cells of the same record
arbitrate per column exactly as before. A record carrying counter
contributions is therefore gated on CL only (its Stamp still
arbitrates its register columns).

A fourth specification, `F_unique`, governs unique keys. It has two
modes, selected per key by member-column nullability.

**Eventual (nullable) keys** — a *constraint over* `F_cell`, not a
parallel layer:

```text
F_unique(K, v, vis, ar) = argmax_{(pk, s) ∈ writes(K)=v in vis} s
                          losers' K-columns ⇒ NULL at the winner's stamp
```

For each unique key `K` (a tuple of columns) and each non-NULL value
`v`, exactly one row owns `v`: the row whose effective `F_cell` stamp
across `K`'s columns is highest. The loser-null effect is realized
through ordinary `F_cell` writes at the winner's stamp, so no new state
is introduced — `F_unique` is a derived view over `F_cell` plus the
per-row tuple lookup.

**Coordinated (`NOT NULL`) keys** sit *outside* the `(vis, ar)` algebra.
Uniqueness is not a merge function applied after the fact; it is a
**linearizable precondition on admission**: the writer reserves `v` against
the cluster's reservation leaseholder before its commit may proceed. The
reservation gate is the *only* enforcement point — no replica, the DDL
originator included, holds a native unique index for a coordinated
key, so apply never arbitrates and never rejects. A competing operation is
rejected before commit or cannot reach an admitting leaseholder, so there is
no loser to null and no convergence step. This is the system's one CP operation; it is
unavailable under partition (a retryable error) by CAP necessity. Because the
admission gate precedes commit and the apply path does not coordinate, SEC and
the layer composition above are unaffected — coordinated keys only shrink the
admitted operation set.

For a **partial** coordinated key (`… WHERE <predicate>`) the predicate
selects which rows participate: a row the predicate excludes holds no
reservation and is free to share `v` with the live owner. The writer
evaluates the predicate over the operation's full pre-/post-image rows,
so participation is decided at the same single site as the reservation
itself — never on a converging replica. Partial is admissible here, but
not for eventual keys, whose arbitration runs on every receiver.

A value is **reclaimed** (returned to the free pool after the owning
row is deleted or changes value) by a *different* row only after a
**release hold** — a conservative bound ≥ the cluster's
bounded-staleness deadline ([PRUNING.md](PRUNING.md)), by which a lagging
node has observed the release or been evicted. Under an *enforced*
staleness bound, any later claimant of `v` follows the release at every
replica, and no replica observes two live rows claiming `v`, even
transiently. That bound is a deployment contract: eviction is
operator-driven and its mechanization is design-stage (see
[PRUNING.md](PRUNING.md)), so a replica lagging beyond the hold window may
transiently observe the departing duplicate until the releasing change
arrives. (The owning row may reclaim its own value immediately.)

See [SCHEMA.md](SCHEMA.md#unique-keys) for the key model. Engine-specific apply
and reservation flows are specified in the
[SQLite DDL specification](../sqlite/docs/DDL.md#unique-keys) and
[Postgres engine](postgres.md#7-unique-keys).

### Layer composition

Same `(vis, ar)` across all four layers ⇒ they compose trivially. The
layer dispatch is structural, not coordinative: reads resolve the
effective Stamp by falling through byte-range override → cell
override → row base — see [`state.go`](../crdt/state.go)
`RowState.EffectiveStamp` for the fall-through.

## Anti-Entropy Precondition

[`antientropy.Plan`](../internal/antientropy/plan.go) is a pure
function: given the local frontier + gap set and a peer tip report,
return the `(origin, seq)` ranges to fetch. It is the pull-side
realization of the **causal delta-merging condition** of Almeida et
al. (2016) Def. 6:

> A delta-interval `Δ_j^{a,b}` may be shipped to peer `i` only if
> `X_i ⊒ X_j^a`.

In Syzy terms: a Changeset is applied only when its `Deps` are already
satisfied at the receiver. The one cross-chain dependency is the schema
chain — the receiver gates each inbound apply on
`Deps[SchemaChain] ≤ local schema_seq` and holds the payload for
catch-up otherwise (see [SCHEMA.md](SCHEMA.md)). Within a chain, per-origin
seq ordering plus the gap `SeqSet` provides the prefix condition.

## Materialization

Changeset materialization is a δ-state **delta-mutator** in the sense of Almeida
et al.: `Δ = Materialize(...)` such that `X ⊔ Δ = NewState`. Rows that
materialize no effect are omitted, corresponding to irredundant deltas
(Almeida §4.1).

SQLite materializes captured hook evidence in `internal/syncer/materialize.go`;
Postgres folds logical-decoding records in `pg/internal/postgres/capture.go`.
Both produce the canonical changeset in [PROTOCOL.md](PROTOCOL.md).

## Cross-Document Map

| Doc | What it imports from CRDT.md |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | End-to-end transaction, distribution, apply, and recovery lifecycle. |
| [PROTOCOL.md](PROTOCOL.md) | Canonical encoding of identities, dependencies, values, and DML records. |
| [SCHEMA.md](SCHEMA.md) | SchemaEvent ordering, `SchemaChain` dependency gating, and invariant (8). |
| [BLOB_PATCH.md](BLOB_PATCH.md) | `F_range` — the byte-range layer's `(vis, ar)` specialization — and its composition with the row/cell layers. |
| [PRUNING.md](PRUNING.md) | The stability-horizon contract (design), offline-deadline eviction, CL-driven tombstone collapse. |

## References

- Akkoorath et al., *Cure: Strong semantics meets high availability and
  low latency,* ICDCS 2016. (TCC consistency model.)
- Almeida, Shoker, Baquero, *Delta State Replicated Data Types,* 2016.
  (Lex-pair lattice §7.1; causal-context compression §7.4; causal
  delta-merging condition Def. 6; irredundant deltas §4.1.)
- Bailis et al., *Highly Available Transactions,* VLDB 2014.
  (HAT taxonomy; MAV under sticky-causal.)
- Bailis et al., *Scalable Atomic Visibility with RAMP Transactions,*
  SIGMOD 2014. (Read Atomic isolation; fractured reads.)
- Bauwens & Gonzalez Boix, *From Causality to Stability,* MPLR 2020.
  (Causal-stability detection; membership/stability separation.)
- Baquero, Almeida, Shoker, *Pure Operation-Based Replicated Data
  Types,* 2017. (PO-Log redundancy framework.)
- Burckhardt, Gotsman, Yang, Zawirski, *Replicated Data Types:
  Specification, Verification, Optimality,* POPL 2014.
  (`(vis, ar, F)` specification language.)
- Gomes, Kleppmann, Mulligan, Beresford, *Verifying Strong Eventual
  Consistency in Distributed Systems,* OOPSLA 2017. (Isabelle SEC
  framework.)
- Lloyd et al., *Don't Settle for Eventual,* SOSP 2011 (COPS); *Stronger
  Semantics for Low-Latency Geo-Replicated Storage,* NSDI 2013 (Eiger).
  (Explicit dependency stamps; one-hop reduction.)
- Mahajan et al., *Depot: Cloud Storage with Minimal Trust,* OSDI 2010.
  (Bounded-staleness contract form.)
- Preguiça, Baquero, Almeida, *Dotted Version Vectors,* 2010 / 2012.
  (Dot as identity primitive.)
- cr-sqlite (vlcn.io). (Causal length `cl`; per-column LWW pattern.)
- Corrosion (Fly.io). (`SyncState` wire-format conventions.)
- CockroachDB `pkg/util/hlc`. (HLC implementation pattern.)
- Yjs INTERNALS. (Item run-coalescing invariant.)
