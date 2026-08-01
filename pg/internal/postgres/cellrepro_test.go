package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// TestOpenRejectsBootstrapCounterShape: a bootstrap table is created before
// this node has DDL event triggers, so introspection is the only gate its
// counter columns ever pass. A counter outside the cell clock group would ship
// absolute values and silently overwrite concurrent increments.
func TestOpenRejectsBootstrapCounterShape(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	for _, tc := range []struct{ name, cols, extra, want string }{
		{"row group", "id bigint PRIMARY KEY, n public.syzy_counter NOT NULL DEFAULT 0", "", "REPLICA IDENTITY FULL"},
		{"nullable", "id bigint PRIMARY KEY, n public.syzy_counter", "ALTER TABLE public.hits REPLICA IDENTITY FULL", "must be NOT NULL"},
		{"in pk", "id public.syzy_counter NOT NULL PRIMARY KEY", "ALTER TABLE public.hits REPLICA IDENTITY FULL", "PRIMARY KEY"},
		{"unique", "id bigint PRIMARY KEY, n public.syzy_counter NOT NULL UNIQUE", "ALTER TABLE public.hits REPLICA IDENTITY FULL", "UNIQUE key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := "syzy_cnt_boot_" + strings.ReplaceAll(tc.name, " ", "")
			createTestDB(t, ctx, db, schemaKV)
			appExec(t, db, `CREATE DOMAIN public.syzy_counter AS bigint`)
			appExec(t, db, `CREATE TABLE public.hits (`+tc.cols+`)`)
			if tc.extra != "" {
				appExec(t, db, tc.extra)
			}
			cfg := baseTestConfig(db, 180, crdt.ClusterID{0xd3})
			cfg.Tables = []string{"public.kv", "public.hits"}
			e, err := Open(ctx, cfg)
			if err == nil {
				closeEngine(t, ctx, e)
				t.Fatalf("Open accepted %s counter shape", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Open error = %v, want mention of %q", err, tc.want)
			}
		})
	}
}

// TestRowGroupUpdateRejectsContribution: upsertSQL renders a FormatDelta value
// as SQL addition, so an update carrying one for a non-counter column has to
// fail deterministically (into quarantine) instead of silently adding to it.
func TestRowGroupUpdateRejectsContribution(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := newTestEngine(t, ctx, "syzy_cnt_rowdelta", 185, crdt.ClusterID{0xd6})
	defer closeEngine(t, ctx, e)

	ti := e.cat.table(deriveTableID("public", "kv"))
	if ti.cellGroup() {
		t.Fatal("kv must be a row-group table for this case")
	}
	bad := intVal(ti.byName["val"].cid, 5)
	bad.Format = crdt.FormatDelta
	cs, err := crdt.Build(crdt.Dot{Origin: 186, Seq: 1}, crdt.Stamp{Clock: crdt.Clock{WallTime: 9}, Origin: 186},
		nil, e.cfg.Cluster, []crdt.Record{
			crdt.Update{Table: ti.tid, PK: typedPK(t, e, "kv", "1"), CL: 1, Changed: []crdt.ColValue{bad}},
		})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := e.appl.apply(ctx, cs, false); !errors.Is(err, errCounterApply) {
		t.Fatalf("apply(row-group update carrying a contribution) = %v, want errCounterApply", err)
	}
}

// TestStrippedUpdateStillAdvancesGeneration: a redelivered counter-only update
// whose sole contribution the applied marker stripped still has to publish the
// generation advance its committed transaction made. Without it the row clock
// stays a generation behind and the resurrecting Insert arrives as a row-level
// write that overwrites the contribution already in the table.
func TestStrippedUpdateStillAdvancesGeneration(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	schema := schemaKV + `;
CREATE TABLE public.hits (id bigint PRIMARY KEY, label text, n bigint NOT NULL DEFAULT 0);
ALTER TABLE public.hits REPLICA IDENTITY FULL`
	e := openCounterEngine(t, ctx, "syzy_cnt_strip", 183, crdt.ClusterID{0xd5}, schema)
	defer closeEngine(t, ctx, e)

	ti := e.cat.table(deriveTableID("public", "hits"))
	pk := typedPK(t, e, "hits", "1")
	tx, err := e.appl.conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 99}, Origin: 184}
	out, err := e.appl.applyCellUpdate(ctx, tx, e.cfg.Cache, ti,
		crdt.Update{Table: ti.tid, PK: pk, CL: 3}, crdt.RowState{CL: 1}, stamp, false, false)
	if err != nil {
		t.Fatalf("applyCellUpdate: %v", err)
	}
	if !out.applied || out.rowUpdate == nil {
		t.Fatalf("stripped new-generation update was dropped (applied=%v, rowUpdate=%v)", out.applied, out.rowUpdate)
	}
	if out.rowUpdate.state.CL != 3 || !out.rowUpdate.state.Base.IsZero() || !out.rowUpdate.clearCells {
		t.Fatalf("row clock = %+v, want CL 3 with a zero Base and cleared cells", *out.rowUpdate)
	}
	if len(out.winnerCols) != 0 {
		t.Fatalf("no DML ran, yet the record claims to have won %d columns", len(out.winnerCols))
	}
}

// TestCounterInsertOntoUndrainedRow: a peer Insert that establishes the row's
// generation lands on a physical row this node already committed but has not
// yet folded (the undrained local-commit window). The physical content is a
// same-generation contribution every peer sums, so an absolute image here would
// erase it locally while the peers add both.
func TestCounterInsertOntoUndrainedRow(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xd1}
	schema := schemaKV + `;
CREATE TABLE public.hits (id bigint PRIMARY KEY, label text, n bigint NOT NULL DEFAULT 0);
ALTER TABLE public.hits REPLICA IDENTITY FULL`
	a := openCounterEngine(t, ctx, "syzy_cnt_undrain_a", 174, cluster, schema)
	defer closeEngine(t, ctx, a)
	b := openCounterEngine(t, ctx, "syzy_cnt_undrain_b", 175, cluster, schema)
	defer closeEngine(t, ctx, b)

	// B inserts first (earlier stamp), A second — so A's fold dominates and its
	// record ships. Neither node has folded when A applies B's insert.
	appExec(t, "syzy_cnt_undrain_b", `INSERT INTO public.hits VALUES (1,'x',7)`)
	csB := captureAll(t, ctx, b)
	appExec(t, "syzy_cnt_undrain_a", `INSERT INTO public.hits VALUES (1,'x',5)`)

	if err := a.appl.Apply(ctx, csB[0]); err != nil {
		t.Fatalf("A apply B's insert: %v", err)
	}
	csA := captureAll(t, ctx, a)
	if len(csA) != 1 {
		t.Fatalf("A folded %d changesets, want 1", len(csA))
	}
	if err := b.appl.Apply(ctx, csA[0]); err != nil {
		t.Fatalf("B apply A's insert: %v", err)
	}
	nA := counterValue(t, "syzy_cnt_undrain_a", 1)
	nB := counterValue(t, "syzy_cnt_undrain_b", 1)
	if nA != nB {
		t.Fatalf("diverged: A.n = %d, B.n = %d", nA, nB)
	}
	if nA != 12 {
		t.Fatalf("n = %d on both, want 12 (5+7 both summed)", nA)
	}
}

// TestWinnerRepairKeepsCounterContribution: when a stashed peer winner
// dominates a local counter update (clock skew — what winner-repair exists
// for), the register columns lose but the contribution must still ship.
// Contributions do not arbitrate by stamp; dropping one loses it everywhere.
func TestWinnerRepairKeepsCounterContribution(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xd2}
	schema := schemaKV + `;
CREATE TABLE public.hits (id bigint PRIMARY KEY, label text, n bigint NOT NULL DEFAULT 0);
ALTER TABLE public.hits REPLICA IDENTITY FULL`
	a := openCounterEngine(t, ctx, "syzy_cnt_skew_a", 176, cluster, schema)
	defer closeEngine(t, ctx, a)

	appExec(t, "syzy_cnt_skew_a", `INSERT INTO public.hits VALUES (1,'x',0)`)
	_ = captureAll(t, ctx, a)

	ti := a.cat.table(deriveTableID("public", "hits"))
	pk := typedPK(t, a, "hits", "1")
	// A peer winner whose wall clock is far ahead of this node's — the skew
	// winner-repair exists to repair. It arbitrated both the register and the
	// counter cell.
	high := crdt.Stamp{Clock: crdt.Clock{WallTime: int64(1) << 46}, Origin: 177}
	a.cfg.Cache.PutRowState(ti.tid, pk, crdt.RowState{CL: 1, Base: high})
	a.winners.stash(ti.tid, pk, winnerEntry{
		Dot: crdt.Dot{Origin: 177, Seq: 1}, CL: 1, Stamp: high,
		Image: []crdt.ColValue{
			intVal(ti.byName["id"].cid, 1),
			textVal(ti.byName["label"].cid, "peer"),
			intVal(ti.byName["n"].cid, 0),
		},
		Cols: map[crdt.ColumnID]struct{}{
			ti.byName["label"].cid: {},
			ti.byName["n"].cid:     {},
		},
	})

	appExec(t, "syzy_cnt_skew_a", `UPDATE public.hits SET label = 'local', n = n + 3 WHERE id = 1`)
	got := captureAll(t, ctx, a)
	var deltas int
	for _, cs := range got {
		for _, r := range cs.Records {
			upd, ok := r.(crdt.Update)
			if !ok {
				continue
			}
			for _, v := range upd.Changed {
				if v.Format == crdt.FormatDelta {
					deltas++
				}
			}
		}
	}
	if deltas != 1 {
		t.Fatalf("fold shipped %d counter contributions across %d changesets, want 1 — a losing local fold must never drop one", deltas, len(got))
	}
}
