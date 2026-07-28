package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestHandoff_BestEffortDrain is the regression for a production hot-restart
// fragility: a node whose producer drain has NOT converged at Detach time must
// still hand off cleanly. The predecessor's drain is best-effort — the successor
// adopts the same origin + on-disk journal and re-drains from the last persisted
// offset — so a stalled drain must NOT fail Detach (which would force the caller
// into a full cold restart). Observed on a spot node: a transient drain stall
// during `systemctl reload` turned a zero-downtime handoff into an exit-for-
// systemd-restart outage.
func TestHandoff_BestEffortDrain(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "app.db")
	cfg := Config{Path: dbPath, InProcessOnly: true}
	ctx := context.Background()

	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	da := NewDB(a)
	if _, err := da.Exec(`CREATE TABLE vms (id INTEGER PRIMARY KEY, status TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Baseline rows, fully drained — so what we assert recovered later is the
	// UN-drained tail, not just the steady state.
	for i := 0; i < 3; i++ {
		if _, err := da.Exec(`INSERT INTO vms (status) VALUES ('running')`); err != nil {
			t.Fatalf("baseline insert: %v", err)
		}
	}
	if err := a.producer.WaitForDrain(ctx); err != nil {
		t.Fatalf("baseline drain: %v", err)
	}
	originA := a.OriginHex()

	// Wedge the drainer, then write the tail. These rows are durable in the
	// journal (head advances) but never reach the sink (drained frozen) — the
	// exact state a stalled drain leaves at Detach time.
	a.producer.StopDrainer()
	const tail = 5
	for i := 0; i < tail; i++ {
		if _, err := da.Exec(`INSERT INTO vms (status) VALUES ('paused')`); err != nil {
			t.Fatalf("tail insert %d: %v", i, err)
		}
	}
	if drained, head := a.producer.DrainProgress(); drained >= head {
		t.Fatalf("test setup: drainer did not lag (drained=%d head=%d); cannot exercise a stalled drain", drained, head)
	}

	// Keep the deliberately-stalled handoff drain sub-second; the successor's
	// own startup drain (the full default budget) catches the tail up.
	a.handoffDrainTimeout = 50 * time.Millisecond

	// The fix under test: Detach must SUCCEED despite the un-converged drain.
	h, err := a.Detach()
	if err != nil {
		t.Fatalf("Detach with a stalled drain must succeed (best-effort), got: %v", err)
	}

	// Successor adopts the origin (no rotation) and re-drains the journal tail
	// from the last persisted offset as part of its own Open. Data is continuous
	// and it can keep writing on the adopted origin.
	b, err := Attach(ctx, cfg, h)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if b.OriginHex() != originA {
		t.Fatalf("origin rotated across handoff: A=%s B=%s", originA, b.OriginHex())
	}
	if err := h.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	db := NewDB(b)
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM vms`).Scan(&n); err != nil {
		t.Fatalf("query after handoff: %v", err)
	}
	if want := 3 + tail; n != want {
		t.Fatalf("rows after handoff = %d, want %d", n, want)
	}
	if _, err := db.Exec(`INSERT INTO vms (status) VALUES ('running')`); err != nil {
		t.Fatalf("successor write on adopted origin: %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM vms`).Scan(&n); err != nil {
		t.Fatalf("re-query: %v", err)
	}
	if want := 3 + tail + 1; n != want {
		t.Fatalf("rows after successor write = %d, want %d", n, want)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close B: %v", err)
	}
}
