package testcluster

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport/memtransport"
)

// TestClusterRecoverMirrorReplay simulates an unclean shutdown of node B
// after applying records from A but BEFORE a snapshot persisted those
// applies. On restart, B's cache should be brought forward by the
// mirror-journal replay so its frontier matches A's contiguous head.
//
// Sequence:
//  1. Start A and B in a shared dir.
//  2. A inserts 3 rows. WaitApplied each on B.
//  3. Force a snapshot on B — captures frontier=3, rowClock for those rows.
//  4. A inserts 2 more rows. WaitApplied each on B (B's app.db reflects
//     them but cache state is past the last snapshot).
//  5. Tear down B (no final snapshot — the post-snapshot frontier
//     advance is only in memory + in the mirror journal).
//  6. Re-open B at the same dir. LoadFromMeta gets frontier=3;
//     RecoverMirror replays the 2 mirror records and brings frontier to 5.
//  7. Verify B.Cache.FrontierFor(A) = 5.
func TestClusterRecoverMirrorReplay(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A is plain — its identity is what matters; we don't restart A.
	a := NewWithCache(t, hub, 1, eventSchema, 0)
	a.Start(t, ctx)

	// B is the recovery target; capture its dir so we can re-open it.
	bDir := t.TempDir()
	bCtx, bCancel := context.WithCancel(ctx)
	b := newWithCacheAt(t, hub, 2, eventSchema, bDir, 0, "NORMAL")
	b.Start(t, bCtx)

	stmt, _, err := a.AppWrite.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	insert := func(i int) {
		t.Helper()
		stmt.Reset()
		var id [8]byte
		id[0] = byte(i)
		if err := stmt.BindBlob(1, id[:]); err != nil {
			t.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			t.Fatalf("Step: %v", err)
		}
	}

	// Phase 1: 3 rows, then snapshot B.
	for i := 1; i <= 3; i++ {
		insert(i)
		b.WaitApplied(t, a.Origin, crdt.Seq(i), 5*time.Second)
	}
	if err := b.Snapshotter.SnapshotOnce(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if got, _, err := b.Meta.FrontierFor(a.Origin); err != nil || got.LastSeq != 3 {
		t.Fatalf("post-snapshot metadata frontier = %v err=%v; want LastSeq=3", got, err)
	}

	// Phase 2: 2 more rows applied but NOT snapshotted. B.Cache moves
	// to frontier=5; metadata still at 3.
	for i := 4; i <= 5; i++ {
		insert(i)
		b.WaitApplied(t, a.Origin, crdt.Seq(i), 5*time.Second)
	}
	if got, ok := b.Cache.FrontierFor(a.Origin); !ok || got.LastSeq != 5 {
		t.Fatalf("pre-restart cache frontier = %v ok=%v; want LastSeq=5", got, ok)
	}
	if got, _, err := b.Meta.FrontierFor(a.Origin); err != nil || got.LastSeq != 3 {
		t.Fatalf("pre-restart metadata frontier = %v err=%v; want LastSeq=3 (no second snapshot)", got, err)
	}

	// Phase 3: stop B's broker + snapshotter without a final snapshot.
	bCancel()
	b.WaitShutdown()
	if err := b.Broker.Close(); err != nil {
		t.Fatalf("Broker.Close: %v", err)
	}
	if err := b.Producer.Close(); err != nil {
		t.Fatalf("Producer.Close: %v", err)
	}
	if err := b.MirrorJournals.Close(); err != nil {
		t.Fatalf("MirrorJournals.Close: %v", err)
	}
	if err := b.Meta.Close(); err != nil {
		t.Fatalf("Meta.Close: %v", err)
	}
	if err := b.AppWrite.Close(); err != nil {
		t.Fatalf("AppWrite.Close: %v", err)
	}
	if err := b.AppApply.Close(); err != nil {
		t.Fatalf("AppApply.Close: %v", err)
	}

	// Re-open B at the same dir.
	t.Logf("re-opening B at %s", bDir)
	b2 := newWithCacheAt(t, hub, 2, eventSchema, bDir, 0, "NORMAL")
	// Don't Start broker — just verify cache state after recovery. Start
	// would race with A's continuing broadcasts.
	if got, ok := b2.Cache.FrontierFor(a.Origin); !ok || got.LastSeq != 5 {
		t.Errorf("post-recovery cache frontier = %v ok=%v; want LastSeq=5 (metadata=3 + mirror replay of 4,5)", got, ok)
	}

	// Sanity: B's app.db should still hold 5 rows from the original run.
	row, _, err := b2.Read.Prepare(`SELECT count(*) FROM event`)
	if err != nil {
		t.Fatalf("Prepare count: %v", err)
	}
	defer row.Finalize()
	if _, err := row.Step(); err != nil {
		t.Fatalf("count step: %v", err)
	}
	if got := row.ColumnInt64(0); got != 5 {
		t.Errorf("post-recovery app.db has %d rows; want 5", got)
	}

	_ = filepath.Join // silence
}

// TestClusterRecoverSelfReplay simulates an unclean shutdown of node A
// (the writer) after committing records past the last snapshot. On
// restart, the producer's drainer replays self-journal records past
// markers[self], advancing senderNextSeq + rowClock + hlcLast to
// match what was written pre-crash. No re-broadcast (OnEncoded is
// only registered after WaitForDrain catches up).
func TestClusterRecoverSelfReplay(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aDir := t.TempDir()
	aCtx, aCancel := context.WithCancel(ctx)
	a := newWithCacheAt(t, hub, 1, eventSchema, aDir, 0, "NORMAL")
	a.Start(t, aCtx)

	stmt, _, err := a.AppWrite.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	insertA := func(i int) {
		t.Helper()
		stmt.Reset()
		var id [8]byte
		id[0] = byte(i)
		if err := stmt.BindBlob(1, id[:]); err != nil {
			t.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			t.Fatalf("Step: %v", err)
		}
	}

	for i := 1; i <= 3; i++ {
		insertA(i)
	}
	if err := a.Producer.WaitForDrain(ctx); err != nil {
		t.Fatalf("WaitForDrain: %v", err)
	}
	if err := a.Snapshotter.SnapshotOnce(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if got := a.Cache.SenderNextSeq(a.Origin); got != 4 {
		t.Fatalf("post-snapshot senderNextSeq = %d; want 4", got)
	}

	// Two more rows past the last snapshot.
	for i := 4; i <= 5; i++ {
		insertA(i)
	}
	if err := a.Producer.WaitForDrain(ctx); err != nil {
		t.Fatalf("WaitForDrain: %v", err)
	}
	if got := a.Cache.SenderNextSeq(a.Origin); got != 6 {
		t.Fatalf("post-2-more cache senderNextSeq = %d; want 6", got)
	}
	if seqs, err := a.Meta.SenderSeqs(); err != nil || seqs[a.Origin] != 4 {
		t.Fatalf("post-2-more metadata senderNextSeq[self] = %d err=%v; want 4 (no second snapshot)", seqs[a.Origin], err)
	}

	// Stop A without a final snapshot.
	stmt.Finalize()
	aCancel()
	if err := a.Broker.Close(); err != nil {
		t.Fatalf("Broker.Close: %v", err)
	}
	if err := a.Producer.Close(); err != nil {
		t.Fatalf("Producer.Close: %v", err)
	}
	if err := a.MirrorJournals.Close(); err != nil {
		t.Fatalf("MirrorJournals.Close: %v", err)
	}
	if err := a.Meta.Close(); err != nil {
		t.Fatalf("Meta.Close: %v", err)
	}
	if err := a.AppWrite.Close(); err != nil {
		t.Fatalf("AppWrite.Close: %v", err)
	}
	if err := a.AppApply.Close(); err != nil {
		t.Fatalf("AppApply.Close: %v", err)
	}

	// Re-open. Recovery walks self journal past the marker; cache
	// returns to senderNextSeq=6.
	a2 := newWithCacheAt(t, hub, 1, eventSchema, aDir, 0, "NORMAL")
	if got := a2.Cache.SenderNextSeq(a2.Origin); got != 6 {
		t.Errorf("post-recovery senderNextSeq = %d; want 6", got)
	}
}
