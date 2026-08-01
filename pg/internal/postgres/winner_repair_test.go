package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
)

// TestWinnerRepairApplyStashesInsert verifies the apply-side half of
// winner repair (§9): a winning Insert is stashed in the
// engine's winnerStash with its post-arbitration image, so a later local fold
// can detect a loss and self-correct against it.
func TestWinnerRepairApplyStashesInsert(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xa1, 0x9d}

	a := newTestEngine(t, ctx, "syzy_wra1", 51, cluster)
	defer closeEngine(t, ctx, a)
	b := newTestEngine(t, ctx, "syzy_wrb1", 52, cluster)
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_wra1", `INSERT INTO public.kv VALUES (100,'peer')`)
	csA := captureAll(t, ctx, a)
	if len(csA) != 1 {
		t.Fatalf("A: want 1 changeset, got %d", len(csA))
	}
	if err := b.appl.Apply(ctx, csA[0]); err != nil {
		t.Fatalf("B apply: %v", err)
	}
	h := csA[0].Records[0].Header()
	w, ok := b.winners.winner(h.Table, h.PK)
	if !ok {
		t.Fatal("expected Cache.Winner stashed after apply of an Insert")
	}
	if w.CL != h.CL {
		t.Errorf("Winner.CL=%d, want %d", w.CL, h.CL)
	}
	if w.Stamp != csA[0].Stamp {
		t.Errorf("Winner.Stamp=%+v, want %+v", w.Stamp, csA[0].Stamp)
	}
	if w.Dot != csA[0].Dot {
		t.Errorf("Winner.Dot=%+v, want %+v", w.Dot, csA[0].Dot)
	}
	if len(w.Image) == 0 {
		t.Error("Winner.Image is empty; expected post-arb columns of the Insert")
	}
}

// TestWinnerRepairApplyStashesUpdate verifies the slice-2 extension: a
// winning peer Update is stashed with the FULL post-merge row image (read in
// the apply tx via readRowImage), not just the changeset's Changed columns.
// Without this, a later self-correct UPSERT would leave un-Changed columns at
// whatever the local loser put there.
func TestWinnerRepairApplyStashesUpdate(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xa2, 0x9d}

	a := newTestEngine(t, ctx, "syzy_wra3", 53, cluster)
	defer closeEngine(t, ctx, a)
	b := newTestEngine(t, ctx, "syzy_wrb3", 54, cluster)
	defer closeEngine(t, ctx, b)

	// A: Insert then Update. Capture two changesets; apply both to B in order.
	appExec(t, "syzy_wra3", `INSERT INTO public.kv VALUES (200,'init')`)
	appExec(t, "syzy_wra3", `UPDATE public.kv SET val='updated' WHERE id=200`)
	css := captureAll(t, ctx, a)
	if len(css) != 2 {
		t.Fatalf("A: want 2 changesets, got %d", len(css))
	}
	for _, cs := range css {
		if err := b.appl.Apply(ctx, cs); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	// The Update applied second wins — stash should reflect the post-UPDATE
	// FULL row (val='updated'), not just the changeset's Changed columns.
	h := css[1].Records[0].Header()
	w, ok := b.winners.winner(h.Table, h.PK)
	if !ok {
		t.Fatal("expected Cache.Winner stashed after Update apply")
	}
	if w.Stamp != css[1].Stamp {
		t.Errorf("Winner.Stamp=%+v, want Update's stamp %+v", w.Stamp, css[1].Stamp)
	}
	// The full image must carry BOTH the id (unchanged) and val='updated', not
	// just the Changed col(s) the Update record carried. Values are canonical
	// typed (value.go): id is an 8-byte BE ColInt, val a ColText.
	var sawID, sawUpdated bool
	for _, cv := range w.Image {
		text, err := colValueText(cv)
		if err != nil {
			t.Fatalf("stash value: %v", err)
		}
		switch text {
		case "200":
			sawID = true
		case "updated":
			sawUpdated = true
		}
	}
	if !sawID || !sawUpdated {
		t.Errorf("Winner.Image missing full row; got %+v (sawID=%v sawUpdated=%v)", w.Image, sawID, sawUpdated)
	}
}

// TestWinnerRepairFoldSelfCorrects verifies the fold-side half of
// winner repair (§9): when a local fold's (CL, Stamp) loses
// to a stashed winner, the fold drops the loser from the outbound changeset
// and the orchestrator UPSERTs the winner's image back to the local table.
//
// Direct cross-machine clock skew can't be reproduced single-machine because
// PG apply's MarkApplied advances Cache.hlcLast past the peer's stamp (the
// CRDT-correct HLC observation), so a normal Apply-then-fold path would have
// the local fold dominate. We instead seed the Cache+stash directly (skipping
// Apply's HLC bump), simulating "B's clock is behind the cluster" — the
// scenario winner-repair exists to repair.
func TestWinnerRepairFoldSelfCorrects(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xa1, 0x9e}

	b := newTestEngine(t, ctx, "syzy_wrb2", 52, cluster)
	defer closeEngine(t, ctx, b)

	// Seed B's table with the 'peer' value (the cluster's known winner) and
	// drain that local commit so capture's progress is past it.
	appExec(t, "syzy_wrb2", `INSERT INTO public.kv VALUES (100,'peer')`)
	_ = captureAll(t, ctx, b)

	ti := b.cat.table(deriveTableID("public", "kv"))
	if ti == nil {
		t.Fatal("kv table missing from PG catalog")
	}
	idCol := ti.byName["id"].cid
	valCol := ti.byName["val"].cid
	idVal := crdt.ColValue{Column: idCol, TypeTag: crdt.ColText, Bytes: []byte("100")}
	winnerVal := crdt.ColValue{Column: valCol, TypeTag: crdt.ColText, Bytes: []byte("peer")}
	pk := typedPK(t, b, "kv", "100")
	// Synthetic "peer" winner stamp far in the future — overrides B's own
	// just-folded stamp on this key, simulating cross-node clock skew where
	// the peer's wall clock is ahead of B's beyond HLC tolerance.
	high := crdt.Stamp{
		Clock:  crdt.Clock{WallTime: int64(1) << 46, Logical: 0},
		Origin: 51,
	}
	b.cfg.Cache.PutRowState(ti.tid, pk, crdt.RowState{CL: 1, Base: high})
	b.winners.stash(ti.tid, pk, winnerEntry{
		Dot:   crdt.Dot{Origin: 51, Seq: 1},
		CL:    1,
		Stamp: high,
		Image: []crdt.ColValue{idVal, winnerVal},
	})

	// B's app UPSERTs 'local' over the row — since it exists, PG runs UPDATE
	// physically. B's table now holds 'local'; the next fold builds an
	// Update record at CL=1 with B's own (low) wall stamp.
	appExec(t, "syzy_wrb2", `INSERT INTO public.kv VALUES (100,'local') ON CONFLICT (id) DO UPDATE SET val=excluded.val`)
	if got := dumpKV(t, "syzy_wrb2")[100]; got != "local" {
		t.Fatalf("pre-fold: B[100]=%q, want local", got)
	}

	enqueueDrain(t, ctx, b)
	var draft *txnAccum
	select {
	case draft = <-b.orch.drafts:
	case <-time.After(time.Second):
		t.Fatal("expected B's local commit enqueued")
	}
	var broadcasted []*crdt.Changeset
	sink := func(_ context.Context, cs *crdt.Changeset) error {
		broadcasted = append(broadcasted, cs)
		return nil
	}
	if err := b.orch.fold(ctx, draft, sink); err != nil {
		t.Fatalf("B fold: %v", err)
	}
	if len(broadcasted) != 0 {
		t.Errorf("expected the loser to be dropped; got %d broadcast(s)", len(broadcasted))
	}
	if got := dumpKV(t, "syzy_wrb2")[100]; got != "peer" {
		t.Fatalf("after self-correct: B[100]=%q, want peer (repaired)", got)
	}
	if _, ok := b.winners.winner(ti.tid, pk); !ok {
		t.Errorf("expected Cache.Winner kept on self-correct path (only cleared on dominance)")
	}
}
