package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
)

// Conflict observability (§9): every arbitration that discards a committed
// write lands in public.syzy_conflicts, on the node where the value was lost.

type conflictRow struct {
	tbl, pk, kind, side, op string
	cols                    []string
	lost                    map[string]*string
	winnerOrigin            int64
	loserOrigin             int64
}

func readConflicts(t *testing.T, db string) []conflictRow {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `SELECT tbl, pk, kind, loser_side, op, cols, lost_values,
	    winner_origin, loser_origin FROM `+conflictsTable+` ORDER BY seq`)
	if err != nil {
		t.Fatalf("read conflicts: %v", err)
	}
	defer rows.Close()
	var out []conflictRow
	for rows.Next() {
		var r conflictRow
		if err := rows.Scan(&r.tbl, &r.pk, &r.kind, &r.side, &r.op, &r.cols, &r.lost,
			&r.winnerOrigin, &r.loserOrigin); err != nil {
			t.Fatalf("scan conflict: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("conflict rows: %v", err)
	}
	return out
}

// TestConflictLogRecordsBothSides: one row written concurrently on two nodes.
// Each node keeps the winner, and the node that discarded a value records what
// it lost — the loser's side records its own committed value as lost, the
// winner's side records the peer's rejected write.
func TestConflictLogRecordsBothSides(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xd1}
	a := openEngine(t, ctx, "syzy_confa", 81, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, a)
	b := openEngine(t, ctx, "syzy_confb", 82, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_confa", `INSERT INTO public.kv VALUES (1,'seed')`)
	seed := captureAll(t, ctx, a)
	if err := b.appl.Apply(ctx, seed[0]); err != nil {
		t.Fatalf("B apply seed: %v", err)
	}
	// Concurrent same-column writes: both nodes commit, then exchange.
	appExec(t, "syzy_confa", `UPDATE public.kv SET val = 'from-A' WHERE id = 1`)
	appExec(t, "syzy_confb", `UPDATE public.kv SET val = 'from-B' WHERE id = 1`)
	csA := captureAll(t, ctx, a)
	csB := captureAll(t, ctx, b)
	if err := b.appl.Apply(ctx, csA[0]); err != nil {
		t.Fatalf("B apply A: %v", err)
	}
	if err := a.appl.Apply(ctx, csB[0]); err != nil {
		t.Fatalf("A apply B: %v", err)
	}

	// Exactly one of the two nodes overrode a local value; the other rejected the
	// inbound write. Both are recorded, on the node where the loss happened.
	got := map[string][]conflictRow{
		"syzy_confa": readConflicts(t, "syzy_confa"),
		"syzy_confb": readConflicts(t, "syzy_confb"),
	}
	var local, inbound int
	for db, rows := range got {
		for _, r := range rows {
			if r.tbl != "public.kv" || r.pk != "id=1" {
				t.Errorf("%s: conflict on %s (%s), want public.kv id=1", db, r.tbl, r.pk)
			}
			if r.kind != "concurrent" {
				t.Errorf("%s: kind = %q, want concurrent (same generation, different origins)", db, r.kind)
			}
			if len(r.cols) != 1 || r.cols[0] != "val" {
				t.Errorf("%s: cols = %v, want [val]", db, r.cols)
			}
			if r.winnerOrigin == r.loserOrigin {
				t.Errorf("%s: winner and loser are the same origin %d", db, r.winnerOrigin)
			}
			switch r.side {
			case "local":
				local++
				if v := r.lost["val"]; v == nil || (*v != "from-A" && *v != "from-B") {
					t.Errorf("%s: lost local value = %v, want the overridden val", db, r.lost)
				}
			case "inbound":
				inbound++
			default:
				t.Errorf("%s: loser_side = %q", db, r.side)
			}
		}
	}
	if local != 1 || inbound != 1 {
		t.Fatalf("recorded %d local and %d inbound losses, want 1 of each: %+v", local, inbound, got)
	}
}

// TestConflictLogIgnoresUncontendedApply: an apply that overwrites nothing
// another origin wrote records nothing — the log is a conflict log, not a
// replication log.
func TestConflictLogIgnoresUncontendedApply(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xd2}
	a := openEngine(t, ctx, "syzy_confc", 83, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, a)
	b := openEngine(t, ctx, "syzy_confd", 84, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_confc", `INSERT INTO public.kv VALUES (1,'one')`)
	appExec(t, "syzy_confc", `UPDATE public.kv SET val = 'two' WHERE id = 1`)
	for _, cs := range captureAll(t, ctx, a) {
		if err := b.appl.Apply(ctx, cs); err != nil {
			t.Fatalf("B apply: %v", err)
		}
	}
	if rows := readConflicts(t, "syzy_confd"); len(rows) != 0 {
		t.Fatalf("uncontended applies recorded %d conflicts: %+v", len(rows), rows)
	}
}

// TestConflictLogSplitsCellLossesByWriter: on a cell-group table an inbound
// write can lose one column while winning another, and the losses are
// attributed to the origin that actually holds each column.
func TestConflictLogSplitsCellLossesByWriter(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xd3}
	a := openEngine(t, ctx, "syzy_confe", 85, cluster, schemaCell, []string{"public.kv", "public.doc"})
	defer closeEngine(t, ctx, a)
	b := openEngine(t, ctx, "syzy_conff", 86, cluster, schemaCell, []string{"public.kv", "public.doc"})
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_confe", `INSERT INTO public.doc VALUES (1,'t','b')`)
	seed := captureAll(t, ctx, a)
	if err := b.appl.Apply(ctx, seed[0]); err != nil {
		t.Fatalf("B apply seed: %v", err)
	}
	// A writes title only; B writes title AND body. Applying A's update on B
	// merges nothing new for body and contends only on title.
	appExec(t, "syzy_confe", `UPDATE public.doc SET title = 'A' WHERE id = 1`)
	appExec(t, "syzy_conff", `UPDATE public.doc SET title = 'B', body = 'B' WHERE id = 1`)
	csA := captureAll(t, ctx, a)
	_ = captureAll(t, ctx, b)
	if err := b.appl.Apply(ctx, csA[0]); err != nil {
		t.Fatalf("B apply A: %v", err)
	}

	rows := readConflicts(t, "syzy_conff")
	if len(rows) != 1 {
		t.Fatalf("recorded %d conflicts, want 1 (title only): %+v", len(rows), rows)
	}
	r := rows[0]
	if len(r.cols) != 1 || r.cols[0] != "title" {
		t.Errorf("cols = %v, want [title] — body was never contended", r.cols)
	}
	// Whichever side lost, only the title column is involved and the two writes
	// are recorded as concurrent.
	if r.kind != "concurrent" {
		t.Errorf("kind = %q, want concurrent", r.kind)
	}
	if r.op != "update" {
		t.Errorf("op = %q, want update", r.op)
	}
}
