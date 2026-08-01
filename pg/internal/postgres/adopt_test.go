package postgres

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// Adoption (§10): a database that already had rows when the slot was created.
// Replication starts at the slot's LSN, so without adoption those rows would
// never reach a peer.

// TestAdoptPublishesPreexistingRows: the rows are published once, a peer that
// applies them ends up with the same table, and a second Open with -adopt still
// set republishes nothing.
func TestAdoptPublishesPreexistingRows(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xa7}
	const db = "syzy_adopt_src"

	// Rows committed before the sidecar ever ran.
	createTestDB(t, ctx, db, schemaKV)
	appExec(t, db, `INSERT INTO public.kv VALUES (1,'one'), (2,'two'), (3,'three')`)

	meta, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	defer meta.Close()
	cfg := baseTestConfig(db, 91, cluster)
	cfg.Tables = []string{"public.kv"}
	cfg.Adopt = true
	cfg.Meta = meta
	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open with adopt: %v", err)
	}
	defer closeEngine(t, ctx, a)
	if a.adoptedRows != 3 {
		t.Fatalf("adopted %d rows, want 3", a.adoptedRows)
	}
	if len(a.pendingAdopt) != 1 {
		t.Fatalf("held %d adoption changesets, want 1", len(a.pendingAdopt))
	}

	// A peer applies them and holds the same table.
	b := openEngine(t, ctx, "syzy_adopt_dst", 92, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, b)
	for _, cs := range a.pendingAdopt {
		if err := b.appl.Apply(ctx, cs); err != nil {
			t.Fatalf("peer apply adopted rows: %v", err)
		}
	}
	got := dumpKV(t, "syzy_adopt_dst")
	if len(got) != 3 || got[1] != "one" || got[3] != "three" {
		t.Fatalf("peer kv after adoption = %v, want the three adopted rows", got)
	}

	// A later local write still replicates normally — adoption did not consume
	// the row's identity or leave the clock ahead of the writer.
	appExec(t, db, `UPDATE public.kv SET val = 'ONE' WHERE id = 1`)
	css := captureAll(t, ctx, a)
	if len(css) != 1 {
		t.Fatalf("post-adoption write produced %d changesets, want 1", len(css))
	}
	if err := b.appl.Apply(ctx, css[0]); err != nil {
		t.Fatalf("peer apply post-adoption write: %v", err)
	}
	if got := dumpKV(t, "syzy_adopt_dst"); got[1] != "ONE" {
		t.Errorf("peer row 1 = %q, want ONE", got[1])
	}
}

// TestAdoptIsIdempotent: the marker is durable, so a sidecar left with -adopt in
// its unit file does not republish the database on every restart.
func TestAdoptIsIdempotent(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xa8}
	const db = "syzy_adopt_twice"
	createTestDB(t, ctx, db, schemaKV)
	appExec(t, db, `INSERT INTO public.kv VALUES (1,'one')`)

	metaPath := filepath.Join(t.TempDir(), "meta.db")
	open := func() *Engine {
		t.Helper()
		meta, err := metadata.Open(metaPath)
		if err != nil {
			t.Fatalf("meta: %v", err)
		}
		t.Cleanup(func() { meta.Close() })
		cfg := baseTestConfig(db, 93, cluster)
		cfg.Tables = []string{"public.kv"}
		cfg.Adopt = true
		cfg.Meta = meta
		e, err := Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return e
	}
	first := open()
	if first.adoptedRows != 1 {
		t.Fatalf("first run adopted %d rows, want 1", first.adoptedRows)
	}
	_ = first.Close()
	waitSlotInactive(t, ctx, dbURL(db), "syzy_slot_"+db)

	second := open()
	defer closeEngine(t, ctx, second)
	if second.adoptedRows != 0 || len(second.pendingAdopt) != 0 {
		t.Fatalf("second run re-adopted %d rows (%d changesets); the marker must make it a no-op",
			second.adoptedRows, len(second.pendingAdopt))
	}
}
