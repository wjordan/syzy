package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
)

// enqueueDrain runs capture with the orchestrator's live enqueue process
// (decode → o.drafts, no fold) within an idle window, so the node's pending
// local commit lands in o.drafts unfolded — the state the live loop is in
// between a draft arriving and the actor folding it.
func enqueueDrain(t *testing.T, ctx context.Context, e *Engine) {
	t.Helper()
	cctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	if err := e.capt.run(cctx, e.orch.enqueue, runOpts{}); err != nil {
		t.Fatalf("enqueue-drain %s: %v", e.cfg.Name, err)
	}
}

// TestGateInterleavingFoldsBeforeApply forces the drainToWALTarget interleaving
// TestLiveConvergence can't make deterministic (the codex finding-7 gap): a peer
// changeset for PK k arrives while this node's OWN write to k is decoded and
// enqueued as a draft but not yet folded. The gate must fold the pending local
// draft into the Cache before arbitrating the remote, so both nodes converge on
// one LWW winner for k. Driven step by step (no live loop, no sleeps): enqueue
// B's draft via o.enqueue, pin prog to the WAL head, then call applyRemote with
// A's changeset directly.
func TestGateInterleavingFoldsBeforeApply(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x9a, 0x7e}

	a := newTestEngine(t, ctx, "syzy_gia", 41, cluster)
	defer closeEngine(t, ctx, a)
	b := newTestEngine(t, ctx, "syzy_gib", 42, cluster)
	defer closeEngine(t, ctx, b)

	// A writes PK=100 and folds it — this is the "remote" write that will arrive
	// at B mid-gate (captureAll folds inline, so A's Cache + table hold 'fromA').
	appExec(t, "syzy_gia", `INSERT INTO public.kv VALUES (100,'fromA')`)
	csA := captureAll(t, ctx, a)
	if len(csA) != 1 {
		t.Fatalf("A: want 1 folded changeset, got %d", len(csA))
	}

	// B writes PK=100; decode it into B's drafts WITHOUT folding (the enqueued-
	// but-unfolded state). The app commit already put 'fromB' in B's table.
	appExec(t, "syzy_gib", `INSERT INTO public.kv VALUES (100,'fromB')`)
	enqueueDrain(t, ctx, b)
	if len(b.orch.drafts) != 1 {
		t.Fatalf("B: want 1 pending unfolded draft, got %d", len(b.orch.drafts))
	}

	// Pin prog to the current WAL head so the gate's prog<target wait is satisfied
	// deterministically: capture is stopped here, so prog won't advance on its own
	// (in the live loop capture keeps advancing it). This isolates the queued-draft
	// drain — the finding-7 path — from capture-progress timing. advance is
	// monotonic-max, so it never lowers prog.
	target, err := b.appl.currentWALLSN(ctx)
	if err != nil {
		t.Fatalf("currentWALLSN: %v", err)
	}
	b.prog.advance(target)

	// B's folded local draft ships here (selfLog is nil, so fold broadcasts inline).
	var bOut []*crdt.Changeset
	bBroadcast := func(_ context.Context, cs *crdt.Changeset) error {
		bOut = append(bOut, cs)
		return nil
	}

	// The interleaving: a remote PK=100 write arrives while B's own PK=100 draft
	// is enqueued-but-unfolded. applyRemote must drain+fold the draft first.
	if err := b.orch.applyRemote(ctx, csA[0], bBroadcast); err != nil {
		t.Fatalf("B applyRemote: %v", err)
	}
	if len(bOut) != 1 {
		t.Fatalf("B: expected its local draft folded + broadcast (1 cs) before the remote apply, got %d", len(bOut))
	}

	// Deliver B's folded changeset back to A and let A arbitrate it.
	if err := a.appl.Apply(ctx, bOut[0]); err != nil {
		t.Fatalf("A apply csB: %v", err)
	}

	// Convergence: both nodes agree on PK=100, and the row is one of the two
	// concurrent writes (not lost, not corrupted). This is the invariant the gate
	// exists to hold under the interleaving — without folding B's draft first, B's
	// own write would be stranded and the nodes would diverge.
	da, dbv := dumpKV(t, "syzy_gia")[100], dumpKV(t, "syzy_gib")[100]
	if da != dbv {
		t.Fatalf("did not converge on PK=100: A=%q B=%q", da, dbv)
	}
	if da != "fromA" && da != "fromB" {
		t.Fatalf("PK=100 = %q; want one of the two concurrent writes", da)
	}
}
