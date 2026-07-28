package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport/memtransport"
)

// BenchmarkRoundTripInsert measures the full round-trip latency for
// an INSERT on node A reaching the apply path on node B. Both nodes
// run through the Cache + LWW path; the receiver applies to app.db
// inside one BEGIN IMMEDIATE / COMMIT and advances the cache.
func BenchmarkRoundTripInsert(b *testing.B) {
	runRoundTripInsert(b, false)
}

// BenchmarkRoundTripInsert_JournalSyncOn is the same end-to-end
// round-trip but with the writer node's self journal in SyncOn mode.
// Pair with BenchmarkRoundTripInsert for the operator-visible cost
// of closing the host-crash window on commit. Real-disk b.TempDir()
// required; tmpfs hides msync cost.
func BenchmarkRoundTripInsert_JournalSyncOn(b *testing.B) {
	runRoundTripInsert(b, true)
}

func runRoundTripInsert(b *testing.B, journalSync bool) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mkNode := NewWithCache
	if journalSync {
		mkNode = NewWithCacheJournalSync
	}
	a := mkNode(b, hub, 1, eventSchema, 0)
	other := mkNode(b, hub, 2, eventSchema, 0)
	a.Start(b, ctx)
	other.Start(b, ctx)

	stmt, _, err := a.AppWrite.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Reset()
		var id [8]byte
		for j := 0; j < 8; j++ {
			id[j] = byte(i >> (8 * j))
		}
		if err := stmt.BindBlob(1, id[:]); err != nil {
			b.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatalf("Step: %v", err)
		}
		other.WaitApplied(b, a.Origin, crdt.Seq(i+1), 5*time.Second)
	}
}

// BenchmarkRoundTripInsertBatched amortizes the per-INSERT round-trip
// cost across batched commits. b.N total INSERTs grouped into
// transactions of `batch` rows each; reports per-INSERT ns/op (b.N
// normalized). One commit on A produces one Dot.Seq → one changeset
// → one applied batch on B.
func BenchmarkRoundTripInsertBatched(b *testing.B) {
	for _, batch := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			runRoundTripBatched(b, batch)
		})
	}
}

func runRoundTripBatched(b *testing.B, batch int) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(b, hub, 1, eventSchema, 0)
	other := NewWithCache(b, hub, 2, eventSchema, 0)
	a.Start(b, ctx)
	other.Start(b, ctx)

	insStmt, _, err := a.AppWrite.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare INSERT: %v", err)
	}
	defer insStmt.Finalize()
	begStmt, _, err := a.AppWrite.Prepare(`BEGIN IMMEDIATE`)
	if err != nil {
		b.Fatalf("Prepare BEGIN: %v", err)
	}
	defer begStmt.Finalize()
	cmtStmt, _, err := a.AppWrite.Prepare(`COMMIT`)
	if err != nil {
		b.Fatalf("Prepare COMMIT: %v", err)
	}
	defer cmtStmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()

	rowsLeft := b.N
	txCount := 0
	for rowsLeft > 0 {
		thisBatch := batch
		if thisBatch > rowsLeft {
			thisBatch = rowsLeft
		}
		if err := begStmt.Reset(); err != nil {
			b.Fatalf("BEGIN reset: %v", err)
		}
		if _, err := begStmt.Step(); err != nil {
			b.Fatalf("BEGIN step: %v", err)
		}
		for k := 0; k < thisBatch; k++ {
			i := b.N - rowsLeft + k
			var id [8]byte
			for j := 0; j < 8; j++ {
				id[j] = byte(i >> (8 * j))
			}
			if err := insStmt.Reset(); err != nil {
				b.Fatalf("INSERT reset: %v", err)
			}
			if err := insStmt.BindBlob(1, id[:]); err != nil {
				b.Fatalf("Bind: %v", err)
			}
			if _, err := insStmt.Step(); err != nil {
				b.Fatalf("INSERT step: %v", err)
			}
		}
		if err := cmtStmt.Reset(); err != nil {
			b.Fatalf("COMMIT reset: %v", err)
		}
		if _, err := cmtStmt.Step(); err != nil {
			b.Fatalf("COMMIT step: %v", err)
		}
		rowsLeft -= thisBatch
		txCount++
		other.WaitApplied(b, a.Origin, crdt.Seq(txCount), 5*time.Second)
	}
}

// BenchmarkPipelinedInsert measures steady-state throughput rather
// than round-trip latency: issue all b.N INSERTs back-to-back and
// WaitApplied once at the end. A's commits, the drainer encode,
// memtransport delivery, and B's apply all run concurrently — the
// per-row cost falls to ~max(A_commit_cost, B_apply_cost) instead of
// their sum.
func BenchmarkPipelinedInsert(b *testing.B) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(b, hub, 1, eventSchema, 0)
	other := NewWithCache(b, hub, 2, eventSchema, 0)
	a.Start(b, ctx)
	other.Start(b, ctx)

	stmt, _, err := a.AppWrite.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Reset()
		var id [8]byte
		for j := 0; j < 8; j++ {
			id[j] = byte(i >> (8 * j))
		}
		if err := stmt.BindBlob(1, id[:]); err != nil {
			b.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatalf("Step: %v", err)
		}
	}
	other.WaitApplied(b, a.Origin, crdt.Seq(b.N), 60*time.Second)
}

// BenchmarkPipelinedInsertBatched is the batched companion: each tx
// commits `batch` rows, all txs issued back-to-back, single
// WaitApplied at the end on the highest seq.
func BenchmarkPipelinedInsertBatched(b *testing.B) {
	for _, batch := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			runPipelinedBatched(b, batch)
		})
	}
}

func runPipelinedBatched(b *testing.B, batch int) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(b, hub, 1, eventSchema, 0)
	other := NewWithCache(b, hub, 2, eventSchema, 0)
	a.Start(b, ctx)
	other.Start(b, ctx)

	insStmt, _, err := a.AppWrite.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare INSERT: %v", err)
	}
	defer insStmt.Finalize()
	begStmt, _, err := a.AppWrite.Prepare(`BEGIN IMMEDIATE`)
	if err != nil {
		b.Fatalf("Prepare BEGIN: %v", err)
	}
	defer begStmt.Finalize()
	cmtStmt, _, err := a.AppWrite.Prepare(`COMMIT`)
	if err != nil {
		b.Fatalf("Prepare COMMIT: %v", err)
	}
	defer cmtStmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()

	rowsLeft := b.N
	txCount := 0
	for rowsLeft > 0 {
		thisBatch := batch
		if thisBatch > rowsLeft {
			thisBatch = rowsLeft
		}
		if err := begStmt.Reset(); err != nil {
			b.Fatalf("BEGIN reset: %v", err)
		}
		if _, err := begStmt.Step(); err != nil {
			b.Fatalf("BEGIN step: %v", err)
		}
		for k := 0; k < thisBatch; k++ {
			i := b.N - rowsLeft + k
			var id [8]byte
			for j := 0; j < 8; j++ {
				id[j] = byte(i >> (8 * j))
			}
			if err := insStmt.Reset(); err != nil {
				b.Fatalf("INSERT reset: %v", err)
			}
			if err := insStmt.BindBlob(1, id[:]); err != nil {
				b.Fatalf("Bind: %v", err)
			}
			if _, err := insStmt.Step(); err != nil {
				b.Fatalf("INSERT step: %v", err)
			}
		}
		if err := cmtStmt.Reset(); err != nil {
			b.Fatalf("COMMIT reset: %v", err)
		}
		if _, err := cmtStmt.Step(); err != nil {
			b.Fatalf("COMMIT step: %v", err)
		}
		rowsLeft -= thisBatch
		txCount++
	}
	other.WaitApplied(b, a.Origin, crdt.Seq(txCount), 60*time.Second)
}

// TestClusterCacheEndToEnd exercises the apply path end to end
// (broker.Cache + producer.Cache + Snapshotter). Inserts on A
// replicate to B, B's app.db reflects the row, and both Caches end
// up in equivalent state. Triggers a manual snapshot and verifies
// B's metadata contains the frontier + row_clock entries we'd
// recover from.
func TestClusterCacheEndToEnd(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(t, hub, 1, eventSchema, 0)
	b := NewWithCache(t, hub, 2, eventSchema, 0)
	a.Start(t, ctx)
	b.Start(t, ctx)

	stmt, _, err := a.AppWrite.Prepare(`INSERT INTO event (id, n) VALUES (?, ?)`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	const n = 5
	for i := 0; i < n; i++ {
		stmt.Reset()
		var id [8]byte
		id[0] = byte(i + 1)
		if err := stmt.BindBlob(1, id[:]); err != nil {
			t.Fatalf("BindBlob: %v", err)
		}
		if err := stmt.BindText(2, "v"); err != nil {
			t.Fatalf("BindText: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			t.Fatalf("Step: %v", err)
		}
		b.WaitApplied(t, a.Origin, crdt.Seq(i+1), 5*time.Second)
	}

	// Verify B's app.db has the rows. (Sanity — the apply path writes
	// to it directly.)
	row, _, err := b.Read.Prepare(`SELECT count(*) FROM event`)
	if err != nil {
		t.Fatalf("Prepare count: %v", err)
	}
	defer row.Finalize()
	if _, err := row.Step(); err != nil {
		t.Fatalf("count step: %v", err)
	}
	if got := row.ColumnInt64(0); got != n {
		t.Errorf("B has %d rows; want %d", got, n)
	}

	// Both caches should agree about A's frontier.
	bf, ok := b.Cache.FrontierFor(a.Origin)
	if !ok || bf.LastSeq != crdt.Seq(n) {
		t.Errorf("B.Cache frontier(a) = %v ok=%v; want LastSeq=%d", bf, ok, n)
	}
	// The sink publishes each changeset before it advances senderNextSeq,
	// so B applying seq n does not imply A's counter already reads n+1;
	// poll briefly for the accounting to land.
	deadline := time.Now().Add(5 * time.Second)
	for a.Cache.SenderNextSeq(a.Origin) != crdt.Seq(n+1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := a.Cache.SenderNextSeq(a.Origin); got != crdt.Seq(n+1) {
		t.Errorf("A.Cache senderNextSeq = %d; want %d", got, n+1)
	}

	// Trigger a snapshot on each side and verify the metadata reflects
	// the frontier + row_clock state.
	if err := a.Snapshotter.SnapshotOnce(); err != nil {
		t.Fatalf("A snapshot: %v", err)
	}
	if err := b.Snapshotter.SnapshotOnce(); err != nil {
		t.Fatalf("B snapshot: %v", err)
	}
	if seqs, err := a.Meta.SenderSeqs(); err != nil || seqs[a.Origin] != crdt.Seq(n+1) {
		t.Errorf("A metadata sender_seq[self] = %d (err=%v); want %d", seqs[a.Origin], err, n+1)
	}
	bMetaFront, ok, err := b.Meta.FrontierFor(a.Origin)
	if err != nil || !ok || bMetaFront.LastSeq != crdt.Seq(n) {
		t.Errorf("B metadata frontier(a) = %v ok=%v err=%v; want LastSeq=%d", bMetaFront, ok, err, n)
	}
}

// TestClusterCacheLWW checks that a remote-origin write that's
// dominated by an existing rowClock entry is correctly skipped. We
// inject a fake "older" stamp into B's cache, send a write from A,
// and verify B applies it (newer stamp wins). Then we put a "newer"
// stamp into B's cache and verify the next A write does NOT overwrite
// app.db (LWW dominates the new arrival).
func TestClusterCacheLWW(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(t, hub, 1, eventSchema, 0)
	b := NewWithCache(t, hub, 2, eventSchema, 0)
	a.Start(t, ctx)
	b.Start(t, ctx)

	insStmt, _, err := a.AppWrite.Prepare(`INSERT INTO event (id, n) VALUES (?, ?)`)
	if err != nil {
		t.Fatalf("Prepare INSERT: %v", err)
	}
	defer insStmt.Finalize()
	updStmt, _, err := a.AppWrite.Prepare(`UPDATE event SET n = ? WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare UPDATE: %v", err)
	}
	defer updStmt.Finalize()

	id := [8]byte{0x42}

	// First write — INSERT, no prior state.
	insStmt.Reset()
	if err := insStmt.BindBlob(1, id[:]); err != nil {
		t.Fatalf("Bind id: %v", err)
	}
	if err := insStmt.BindText(2, "first"); err != nil {
		t.Fatalf("Bind n: %v", err)
	}
	if _, err := insStmt.Step(); err != nil {
		t.Fatalf("INSERT step: %v", err)
	}
	b.WaitApplied(t, a.Origin, 1, 5*time.Second)

	// Pre-stuff B's cache with a "newer than the next A write" stamp so
	// the next write loses LWW. WallTime is bounded by 47 bits (≈ year
	// 6429); pick a far-future ms value within that range. CL=5 (live)
	// also strictly dominates A's next CL on UPDATE (which will be 3).
	const futureWall = int64(1) << 46
	bFuture := crdt.RowState{
		CL:   5,
		Base: crdt.Stamp{Clock: crdt.Clock{WallTime: futureWall}, Origin: 99},
	}
	tab, ok := b.Catalog.Table("event")
	if !ok {
		t.Fatal("event table missing in B catalog")
	}
	// PK bytes in cache use catalog.EncodePK form (column-id + type tag
	// + varint len + value), not the raw row PK. Build the canonical
	// blob so we hit the same map key the apply path will look up.
	idCol := tab.PK[0]
	pkBlob, err := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{
		idCol.ID: {Column: idCol.ID, TypeTag: crdt.ColBlob, Bytes: id[:]},
	})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	b.Cache.PutRowState(tab.ID, pkBlob, bFuture)

	// Second write from A — UPDATE (so the producer captures it as an
	// UPDATE record, generating a fresh record with NextLiveCL=3).
	updStmt.Reset()
	if err := updStmt.BindText(1, "second"); err != nil {
		t.Fatalf("Bind n: %v", err)
	}
	if err := updStmt.BindBlob(2, id[:]); err != nil {
		t.Fatalf("Bind id: %v", err)
	}
	if _, err := updStmt.Step(); err != nil {
		t.Fatalf("UPDATE step: %v", err)
	}
	// Wait for B to mark seq=2 applied (the write flowed through, even
	// if no DML landed).
	b.WaitApplied(t, a.Origin, 2, 5*time.Second)

	// Verify B's app.db still says "first" — the LWW skip kept it.
	row, _, err := b.Read.Prepare(`SELECT n FROM event WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare select: %v", err)
	}
	defer row.Finalize()
	row.Reset()
	if err := row.BindBlob(1, id[:]); err != nil {
		t.Fatalf("Bind select: %v", err)
	}
	hasRow, err := row.Step()
	if err != nil || !hasRow {
		t.Fatalf("select: hasRow=%v err=%v", hasRow, err)
	}
	if got := row.ColumnText(0); got != "first" {
		t.Errorf("B app.db row n = %q; want %q (LWW should reject A's overwrite)", got, "first")
	}
}
