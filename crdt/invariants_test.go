package crdt

import (
	"math/rand/v2"
	"reflect"
	"testing"
)

// This file holds tests of the CRDT invariants documented in
// CRDT.md#invariants. Each invariant is either:
//
//   (a) testable in isolation here (numbers below), or
//   (b) producer/broker-enforced and tested against the SQLite engine
//       in internal/producer or internal/broker.
//
// Mapping:
//   (1) Dot uniqueness                  -> producer-enforced; tested in
//                                          internal/producer
//   (2) Per-origin Clock monotonicity   -> producer-enforced; tested in
//                                          internal/producer
//   (3) Causal closure of Deps          -> deps_test.go (TestDeps_Satisfied,
//                                          TestDeps_SetMonotonic)
//   (4) Frontier monotonicity           -> Cache.MarkApplied-enforced; tested
//                                          in internal/nodestate
//   (5) Stamp arbitration totality      -> identity_test.go
//                                          (TestStamp_Dominates_TotalOrder) +
//                                          below: TestInvariant_DominatesIsStrictTotalOrder
//   (6) Apply commutativity (concurrent) -> below: TestInvariant_ApplyCommutes
//   (7) Per-pk CL monotonicity          -> state_test.go
//                                          (TestRowState_CL_MonotonicTransitions)
//   (8) Schema chain totality           -> schemalog-enforced via CAS; tested
//                                          in schemalog
//   (9) Idempotent apply                -> below: TestInvariant_IdempotentApply
//   (10) Unique-key exclusivity         -> coordinated keys: reservation-
//                                          enforced (unique package); eventual
//                                          keys: apply-enforced; tested in
//                                          internal/broker

// modelApply is a minimal in-memory apply model used by the property
// tests. It is NOT the production apply path — it exercises the CRDT
// model alone, abstracting away SQLite. The production apply lives in
// internal/broker.
type modelKey struct {
	Table TableID
	PK    string // PKBlob as string for map key
}

type modelState map[modelKey]RowState

// modelApplyChangeset folds one Changeset into modelState under
// (CL, Stamp) LWW. Records carry the writer's CL; receivers compare
// (record.CL, c.Stamp) lex against (RowState.CL, RowState.Base) and
// apply iff incoming dominates.
func modelApplyChangeset(state modelState, c *Changeset) {
	for _, r := range c.Records {
		h := r.Header()
		k := modelKey{Table: h.Table, PK: string(h.PK)}
		cur := state[k]
		// LWW on lex (CL, Stamp). Apply iff incoming strictly dominates.
		if h.CL < cur.CL {
			continue
		}
		if h.CL == cur.CL && !c.Stamp.Dominates(cur.Base) {
			continue
		}
		cur.CL = h.CL
		cur.Base = c.Stamp
		state[k] = cur
	}
}

func TestInvariant_ApplyCommutes(t *testing.T) {
	// Invariant (6): for concurrent (no Deps relationship) Changesets,
	// apply order does not affect final state.

	// Build a corpus of changesets touching a small key space.
	corpus := make([]*Changeset, 0, 30)
	tables := []TableID{{0xAA}, {0xBB}}
	pks := [][]byte{{0x01}, {0x02}, {0x03}}
	rng := rand.New(rand.NewPCG(7, 13))

	// Each "writer" tracks its locally-observed CL per (table, pk) so
	// records carry a plausible writer-side CL. (In production this is
	// the producer's RowState read at commit time.)
	type writerState map[modelKey]uint64
	writers := make(map[Origin]writerState)
	for o := Origin(1); o <= 3; o++ {
		writers[o] = make(writerState)
	}

	for i := range 30 {
		origin := Origin(rng.Uint64N(3) + 1)
		seq := Seq(i + 1)
		stamp := Stamp{
			Clock:  Clock{WallTime: rng.Int64N(1000), Logical: rng.Int32N(10)},
			Origin: origin,
		}
		tbl := tables[rng.IntN(len(tables))]
		pk := pks[rng.IntN(len(pks))]
		key := modelKey{Table: tbl, PK: string(pk)}
		ws := writers[origin]
		curCL := ws[key]
		// Choose a record kind respecting the writer's local CL view.
		var rec Record
		live := curCL > 0 && curCL%2 == 1
		switch rng.IntN(3) {
		case 0: // Insert (only valid when not live; otherwise treat as Update)
			if live {
				rec = Update{Table: tbl, PK: PKBlob(pk), CL: curCL}
			} else {
				newCL := curCL + 1 // become odd
				rec = Insert{Table: tbl, PK: PKBlob(pk), CL: newCL}
				ws[key] = newCL
			}
		case 1: // Update (only valid when live; else fall back to Insert)
			if live {
				rec = Update{Table: tbl, PK: PKBlob(pk), CL: curCL}
			} else {
				newCL := curCL + 1
				rec = Insert{Table: tbl, PK: PKBlob(pk), CL: newCL}
				ws[key] = newCL
			}
		case 2: // Delete (only valid when live; else skip)
			if !live {
				continue
			}
			newCL := curCL + 1 // become even
			rec = Delete{Table: tbl, PK: PKBlob(pk), CL: newCL}
			ws[key] = newCL
		}
		cs, err := Build(
			Dot{Origin: origin, Seq: seq},
			stamp, nil, ClusterID{},
			[]Record{rec},
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		corpus = append(corpus, cs)
	}

	// Canonical: apply in original order.
	canonical := make(modelState)
	for _, cs := range corpus {
		modelApplyChangeset(canonical, cs)
	}

	// Random orderings should converge.
	for trial := range 50 {
		shuffled := make([]*Changeset, len(corpus))
		copy(shuffled, corpus)
		r := rand.New(rand.NewPCG(uint64(trial), 0))
		r.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		got := make(modelState)
		for _, cs := range shuffled {
			modelApplyChangeset(got, cs)
		}
		if !reflect.DeepEqual(got, canonical) {
			t.Fatalf("trial %d: divergent state\ngot %v\nwant %v", trial, got, canonical)
		}
	}
}

func TestInvariant_IdempotentApply(t *testing.T) {
	// Invariant (9): re-applying the same Changeset is a no-op on
	// modelState (after the initial apply).
	cs, err := Build(
		Dot{Origin: 1, Seq: 1},
		makeStamp(100, 0, 1), nil, ClusterID{},
		[]Record{Insert{Table: TableID{0xAA}, PK: PKBlob{0x01}, CL: 1}},
	)
	if err != nil {
		t.Fatal(err)
	}
	state := make(modelState)
	modelApplyChangeset(state, cs)
	snapshot := make(modelState)
	for k, v := range state {
		snapshot[k] = v
	}
	for range 5 {
		modelApplyChangeset(state, cs)
	}
	if !reflect.DeepEqual(state, snapshot) {
		t.Errorf("re-apply changed state:\ngot %v\nwant %v", state, snapshot)
	}
}

func TestInvariant_DominatesIsStrictTotalOrder(t *testing.T) {
	// Invariant (5) restated as antisymmetry + transitivity: if a > b
	// and b > c, then a > c; never both a > b and b > a.
	rng := rand.New(rand.NewPCG(11, 19))
	stamps := make([]Stamp, 30)
	for i := range stamps {
		stamps[i] = Stamp{
			Clock:  Clock{WallTime: rng.Int64N(20), Logical: rng.Int32N(3)},
			Origin: Origin(rng.Uint64N(5)),
		}
	}
	for _, a := range stamps {
		for _, b := range stamps {
			if a.Dominates(b) && b.Dominates(a) {
				t.Errorf("antisymmetry violated: %v <> %v", a, b)
			}
			for _, c := range stamps {
				if a.Dominates(b) && b.Dominates(c) && !a.Dominates(c) {
					t.Errorf("transitivity violated: %v > %v > %v but !(%v > %v)", a, b, c, a, c)
				}
			}
		}
	}
}
