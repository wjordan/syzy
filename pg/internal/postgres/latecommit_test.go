package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
)

// TestFoldReassertsRowOverwrittenBeforeFold covers the window that
// drainToWALTarget cannot close: a local commit that lands AFTER the
// orchestrator reads pg_current_wal_lsn is already visible in the table but is
// not yet folded, so it has no stamp and no row clock. An inbound changeset
// arbitrating against the row's OLD state then wins and overwrites the row.
//
// The local commit is folded afterwards, and its stamp — allocated after
// MarkApplied absorbed the peer's clock — dominates. So it publishes, every
// peer takes its value, and the node that produced it is the only one still
// holding the peer's: the fold assumed the local table still contained what the
// commit wrote, which stopped being true when the apply overwrote it.
//
// The fix is for a winning fold to re-assert its own image whenever the row's
// last known state came from another origin. Without it the divergence is
// permanent — the node's row clock already names it the winner, so no later
// message disagrees with it and nothing ever repairs the value.
func TestFoldReassertsRowOverwrittenBeforeFold(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x7c}
	a := openEngine(t, ctx, "syzy_latea", 81, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, a)
	b := openEngine(t, ctx, "syzy_lateb", 82, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, b)

	// Both nodes hold the same seed row, drained on each side.
	appExec(t, "syzy_latea", `INSERT INTO public.kv VALUES (1,'seed')`)
	seed := captureAll(t, ctx, a)
	if len(seed) != 1 {
		t.Fatalf("seed produced %d changesets, want 1", len(seed))
	}
	if err := b.appl.Apply(ctx, seed[0]); err != nil {
		t.Fatalf("B apply seed: %v", err)
	}

	// A writes and publishes. B writes too — and B's commit is NOT captured
	// before A's changeset is applied, which is exactly the drain window.
	appExec(t, "syzy_latea", `UPDATE public.kv SET val = 'from-a' WHERE id = 1`)
	csA := captureAll(t, ctx, a)
	if len(csA) != 1 {
		t.Fatalf("A produced %d changesets, want 1", len(csA))
	}
	appExec(t, "syzy_lateb", `UPDATE public.kv SET val = 'from-b' WHERE id = 1`)
	if err := b.appl.Apply(ctx, csA[0]); err != nil {
		t.Fatalf("B apply A: %v", err)
	}
	if got := dumpKV(t, "syzy_lateb")[1]; got != "from-a" {
		t.Fatalf("precondition: B's row = %q after applying A, want from-a "+
			"(the apply must have overwritten the un-folded local commit)", got)
	}

	// Now fold B's local commit. Whatever B publishes, B's own table must hold —
	// that is the invariant a publish asserts.
	csB := captureAll(t, ctx, b)
	local := dumpKV(t, "syzy_lateb")[1]
	if len(csB) == 0 {
		// B's fold lost and was dropped: B must be left holding A's value.
		if local != "from-a" {
			t.Errorf("B published nothing but holds %q, want from-a", local)
		}
		return
	}
	published := publishedKVValue(t, b, csB)
	if published != local {
		t.Errorf("B published %q but its own table holds %q — every peer will take "+
			"%q and B alone keeps %q, permanently", published, local, published, local)
	}
	if err := a.appl.Apply(ctx, csB[0]); err != nil {
		t.Fatalf("A apply B: %v", err)
	}
	if got := dumpKV(t, "syzy_latea")[1]; got != local {
		t.Errorf("A converged on %q, B holds %q", got, local)
	}
}

// publishedKVValue extracts the val column that the changesets assert for kv
// row 1 — the value every peer will hold after applying them.
func publishedKVValue(t *testing.T, e *Engine, sets []*crdt.Changeset) string {
	t.Helper()
	ti := e.cat.table(deriveTableID("public", "kv"))
	if ti == nil {
		t.Fatal("kv missing from catalog")
	}
	valCol := ti.byName["val"].cid
	got := ""
	for _, cs := range sets {
		for _, r := range cs.Records {
			var vals []crdt.ColValue
			switch rec := r.(type) {
			case crdt.Insert:
				vals = rec.Image
			case crdt.Update:
				vals = rec.Changed
			default:
				continue
			}
			for _, v := range vals {
				if v.Column != valCol {
					continue
				}
				text, err := colValueText(v)
				if err != nil {
					t.Fatalf("render published val: %v", err)
				}
				got = text
			}
		}
	}
	if got == "" {
		t.Fatalf("no val column published across %d changesets", len(sets))
	}
	return got
}

// schemaResurrect is a cell-group table (REPLICA IDENTITY FULL) with a NOT NULL
// column the resurrecting write does not carry.
const schemaResurrect = schemaKV + `;
CREATE TABLE public.note (id bigint PRIMARY KEY, title text NOT NULL, body text);
ALTER TABLE public.note REPLICA IDENTITY FULL`

// TestFoldAgainstPeerDeleteNeverWritesPartialRow: a cell-group UPDATE carries
// only the columns it changed, so when a peer's Delete removes the row before
// that commit is folded there is nothing the fold can write back — a partial
// image cannot define a row (its untouched NOT NULL columns have no value).
// Either the row stays deleted or it comes back whole; a half-row is not an
// option, and neither is aborting capture on a constraint violation.
func TestFoldAgainstPeerDeleteNeverWritesPartialRow(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x7d}
	tables := []string{"public.kv", "public.note"}
	a := openEngine(t, ctx, "syzy_resa", 91, cluster, schemaResurrect, tables)
	defer closeEngine(t, ctx, a)
	b := openEngine(t, ctx, "syzy_resb", 92, cluster, schemaResurrect, tables)
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_resa", `INSERT INTO public.note VALUES (1,'t','b')`)
	seed := captureAll(t, ctx, a)
	if len(seed) != 1 {
		t.Fatalf("seed produced %d changesets, want 1", len(seed))
	}
	if err := b.appl.Apply(ctx, seed[0]); err != nil {
		t.Fatalf("B apply seed: %v", err)
	}

	// A deletes and publishes; B updates one column without folding it first.
	appExec(t, "syzy_resa", `DELETE FROM public.note WHERE id = 1`)
	csA := captureAll(t, ctx, a)
	if len(csA) != 1 {
		t.Fatalf("A produced %d changesets, want 1", len(csA))
	}
	appExec(t, "syzy_resb", `UPDATE public.note SET body = 'b2' WHERE id = 1`)
	if err := b.appl.Apply(ctx, csA[0]); err != nil {
		t.Fatalf("B apply A's delete: %v", err)
	}
	if n := noteCount(t, "syzy_resb"); n != 0 {
		t.Fatalf("precondition: B still holds %d note rows after applying the delete", n)
	}

	csB := captureAll(t, ctx, b)
	if len(csB) == 0 {
		if n := noteCount(t, "syzy_resb"); n != 0 {
			t.Errorf("B published nothing but holds %d rows", n)
		}
		return
	}
	// B's write won, so the row is back — whole, with every NOT NULL column.
	title, body := noteRow(t, "syzy_resb", 1)
	if title != "t" || body != "b2" {
		t.Errorf("B's row = (%q,%q), want (t,b2)", title, body)
	}
	for _, cs := range csB {
		if err := a.appl.Apply(ctx, cs); err != nil {
			t.Fatalf("A apply B: %v", err)
		}
	}
	aTitle, aBody := noteRow(t, "syzy_resa", 1)
	if aTitle != title || aBody != body {
		t.Errorf("A converged on (%q,%q), B holds (%q,%q)", aTitle, aBody, title, body)
	}
}

func noteCount(t *testing.T, db string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect %s: %v", db, err)
	}
	defer c.Close(ctx)
	var n int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM public.note`).Scan(&n); err != nil {
		t.Fatalf("count note: %v", err)
	}
	return n
}

func noteRow(t *testing.T, db string, id int64) (title, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect %s: %v", db, err)
	}
	defer c.Close(ctx)
	var b *string
	if err := c.QueryRow(ctx, `SELECT title, body FROM public.note WHERE id=$1`, id).Scan(&title, &b); err != nil {
		t.Fatalf("read note %s: %v", db, err)
	}
	if b != nil {
		body = *b
	}
	return title, body
}
