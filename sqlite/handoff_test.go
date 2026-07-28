package sqlite_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	syzy "github.com/wjordan/syzy/sqlite"
)

// countOrigins counts claimed origin directories under <db>-syzy/origins/.
// 1 == the node kept its single origin; >1 == a rotation left a retired one.
func countOrigins(t *testing.T, dbPath string) int {
	t.Helper()
	ents, err := os.ReadDir(dbPath + "-syzy/origins")
	if err != nil {
		t.Fatalf("read origins: %v", err)
	}
	n := 0
	for _, e := range ents {
		if e.IsDir() {
			n++
		}
	}
	return n
}

// TestHandoff_DetachAttach is the in-process analog of a hot-restart daemon-role
// handoff: a node Detaches (keeping its lock held), a successor Attaches by
// adopting that lock, and the role transfers with no lock window, no origin
// rotation, and full data continuity. Mirrors the cross-process kernel property
// proven by a dedicated lockflip spike harness.
func TestHandoff_DetachAttach(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "app.db")
	cfg := syzy.Config{Path: dbPath, InProcessOnly: true}
	ctx := context.Background()

	// Predecessor: open and seed control-plane state.
	a, err := syzy.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	da := syzy.NewDB(a)
	if _, err := da.Exec(`CREATE TABLE vms (id TEXT PRIMARY KEY, status TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := da.Exec(`INSERT INTO vms VALUES ('vm-1','running'),('vm-2','paused')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	originA := a.OriginHex()

	// Detach: hand off the role WITHOUT releasing the lock.
	h, err := a.Detach()
	if err != nil {
		t.Fatalf("Detach: %v", err)
	}

	// During the handoff window the lock is still held: a fresh Open (a third
	// party, or a confused restart) must be rejected, and no origin rotated.
	if _, err := syzy.Open(ctx, cfg); err == nil {
		t.Fatal("fresh Open succeeded during handoff — lock window leaked!")
	} else {
		t.Logf("fresh Open correctly rejected mid-handoff: %v", err)
	}
	if got := countOrigins(t, dbPath); got != 1 {
		t.Fatalf("origins on disk mid-handoff = %d, want 1 (no rotation)", got)
	}

	// Successor: Attach by adopting the handed-off lock.
	b, err := syzy.Attach(ctx, cfg, h)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if b.OriginHex() != originA {
		t.Fatalf("origin rotated across handoff: A=%s B=%s", originA, b.OriginHex())
	}
	// Predecessor commits: drops its FD refs; the successor keeps the lock.
	if err := h.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	db := syzy.NewDB(b)
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM vms`).Scan(&n); err != nil {
		t.Fatalf("query after handoff: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows after handoff = %d, want 2 (data continuity)", n)
	}
	// Successor writes on the same origin.
	if _, err := db.Exec(`INSERT INTO vms VALUES ('vm-3','running')`); err != nil {
		t.Fatalf("insert on successor: %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM vms`).Scan(&n); err != nil {
		t.Fatalf("re-query: %v", err)
	}
	if n != 3 {
		t.Fatalf("rows after successor write = %d, want 3", n)
	}
	if got := countOrigins(t, dbPath); got != 1 {
		t.Fatalf("origins after handoff = %d, want 1 (no rotation)", got)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close B: %v", err)
	}

	// A clean successor Close recycles the origin on the next normal Open
	// (no rotation), proving the handoff left durable state consistent.
	c, err := syzy.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen after handoff: %v", err)
	}
	if c.OriginHex() != originA {
		t.Fatalf("reopen rotated origin: want %s got %s", originA, c.OriginHex())
	}
	if got := countOrigins(t, dbPath); got != 1 {
		t.Fatalf("origins after reopen = %d, want 1", got)
	}
	_ = c.Close()
}

// TestHandoff_Rollback proves the safe-rollback property: if the successor never
// materializes (a child crash before adopt), the predecessor can Resume by
// Attaching its own still-held handoff. No lock was ever released; no re-Open
// from scratch.
func TestHandoff_Rollback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "app.db")
	cfg := syzy.Config{Path: dbPath, InProcessOnly: true}
	ctx := context.Background()

	a, err := syzy.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := syzy.NewDB(a).Exec(`CREATE TABLE t (k TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := syzy.NewDB(a).Exec(`INSERT INTO t VALUES ('x'),('y')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	originA := a.OriginHex()

	h, err := a.Detach()
	if err != nil {
		t.Fatalf("Detach: %v", err)
	}

	// Successor "crashed": never Attached. Resume in this process by adopting
	// our own held handoff — the lock never left this process.
	a2, err := syzy.Attach(ctx, cfg, h)
	if err != nil {
		t.Fatalf("Resume via Attach: %v", err)
	}
	if a2.OriginHex() != originA {
		t.Fatalf("resume rotated origin: want %s got %s", originA, a2.OriginHex())
	}
	var n int
	if err := syzy.NewDB(a2).QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("query after resume: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows after resume = %d, want 2", n)
	}
	if got := countOrigins(t, dbPath); got != 1 {
		t.Fatalf("origins after resume = %d, want 1", got)
	}
	// a2 owns the FDs now (Attach consumed the handoff); Close releases them.
	if err := a2.Close(); err != nil {
		t.Fatalf("close after resume: %v", err)
	}
}
