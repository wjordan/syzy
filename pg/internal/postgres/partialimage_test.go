package postgres

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// The two shapes the partial-image renderer has to serve on a counter table,
// beyond the plain out-of-order case TestCellUpdateOutrunningItsInsertKeepsItsValue
// covers: a contribution that lands on an absent row, and a contribution the
// applied marker has already certified (§8).

// TestCounterContributionOnAbsentRowSurvivesRetry: the update outruns its Insert
// while carrying a counter contribution, where a phantom success is
// unrecoverable. The applied marker commits in the apply transaction, so it
// would certify a contribution the zero-row UPDATE never added — and the
// redelivery that could re-add it strips it instead, leaving the total short by
// that contribution on this node forever.
func TestCounterContributionOnAbsentRowSurvivesRetry(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xac}
	// label is NOT NULL with no default, so the contribution's image cannot
	// build a whole row and takes the partial-image path.
	schema := schemaKV + `;
CREATE TABLE public.hits (id bigint PRIMARY KEY, label text NOT NULL, n bigint NOT NULL);
ALTER TABLE public.hits REPLICA IDENTITY FULL`
	const dbA, dbB = "syzy_cabs_a", "syzy_cabs_b"
	a := openCounterEngine(t, ctx, dbA, 73, cluster, schema)
	defer closeEngine(t, ctx, a)
	b := openCounterEngine(t, ctx, dbB, 74, cluster, schema)
	defer closeEngine(t, ctx, b)

	appExec(t, dbA, `INSERT INTO public.hits VALUES (1,'x',5)`)
	ins := captureAll(t, ctx, a)
	appExec(t, dbA, `UPDATE public.hits SET n = n + 3 WHERE id = 1`)
	upd := captureAll(t, ctx, a)
	if len(ins) != 1 || len(upd) != 1 {
		t.Fatalf("captured %d insert / %d update changesets, want 1 each", len(ins), len(upd))
	}

	if err := b.appl.Apply(ctx, upd[0]); err == nil {
		t.Fatal("the contribution reported success against an absent row")
	} else if !isDeterministicApplyErr(err) {
		t.Fatalf("apply error is not routed to quarantine: %v", err)
	}
	if err := b.appl.apply(ctx, ins[0], true); err != nil {
		t.Fatalf("B apply insert: %v", err)
	}
	if err := b.appl.apply(ctx, upd[0], true); err != nil {
		t.Fatalf("B retry contribution: %v", err)
	}
	if n := counterValue(t, dbB, 1); n != 8 {
		t.Errorf("B hits.n = %d, want 8 — the retried contribution is still summed in", n)
	}
}

// TestCertifiedInsertDoesNotRecountAtSameCL: an Insert redelivered against a row
// that is physically live at the same causal length — a peer's concurrent insert
// established the generation — is normalized by crdt.AsCellUpdate, which re-tags
// its counter columns as contributions. That normalization runs before the
// row-level certified renderer is ever consulted, so the marker only makes the
// redelivery idempotent if the cell path renders certified counters
// insert-if-absent too.
func TestCertifiedInsertDoesNotRecountAtSameCL(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xad}
	schema := schemaKV + `;
CREATE TABLE public.hits (id bigint PRIMARY KEY, label text, n bigint NOT NULL);
ALTER TABLE public.hits REPLICA IDENTITY FULL`
	const dbA, dbC, dbB = "syzy_recnt_a", "syzy_recnt_c", "syzy_recnt_b"
	a := openCounterEngine(t, ctx, dbA, 75, cluster, schema)
	defer closeEngine(t, ctx, a)
	c := openCounterEngine(t, ctx, dbC, 76, cluster, schema)
	defer closeEngine(t, ctx, c)
	b := openCounterEngine(t, ctx, dbB, 77, cluster, schema)
	defer closeEngine(t, ctx, b)

	appExec(t, dbA, `INSERT INTO public.hits VALUES (1,'a',5)`)
	csA := captureAll(t, ctx, a)
	appExec(t, dbC, `INSERT INTO public.hits VALUES (1,'c',3)`)
	csC := captureAll(t, ctx, c)
	if len(csA) != 1 || len(csC) != 1 {
		t.Fatalf("captured %d/%d changesets, want 1 each", len(csA), len(csC))
	}
	if err := b.appl.Apply(ctx, csA[0]); err != nil {
		t.Fatalf("B apply A: %v", err)
	}
	if err := b.appl.Apply(ctx, csC[0]); err != nil {
		t.Fatalf("B apply C: %v", err)
	}
	if n := counterValue(t, dbB, 1); n != 8 {
		t.Fatalf("B hits.n = %d after both inserts, want 8 (5 + 3)", n)
	}
	if err := b.appl.apply(ctx, csC[0], true); err != nil {
		t.Fatalf("B redeliver C: %v", err)
	}
	if n := counterValue(t, dbB, 1); n != 8 {
		t.Errorf("B hits.n = %d after the certified redelivery, want 8 — the marker already counted it", n)
	}
}
