package postgres

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/nodestate"
)

// These are pure unit tests (no Postgres) of the self-log recovery crux: the
// Cache is rebuilt from the EXACT shipped changeset bytes, never re-derived.

const selfTestOrigin = crdt.Origin(7)

func appendSelf(t *testing.T, j *journal.Journal, lsn uint64, cs *crdt.Changeset) {
	t.Helper()
	if _, _, err := j.Append(journal.KindLocalDML, cs.Stamp.Clock.Pack(), uint64(cs.Dot.Origin),
		encodeSelfLogPayload(pglogrepl.LSN(lsn), cs.Encoded())); err != nil {
		t.Fatalf("append self-log: %v", err)
	}
}

func selfCS(t *testing.T, seq uint64, wall int64, pk string, cl uint64) *crdt.Changeset {
	t.Helper()
	cs, err := crdt.Build(
		crdt.Dot{Origin: selfTestOrigin, Seq: crdt.Seq(seq)},
		crdt.Stamp{Clock: crdt.Clock{WallTime: wall}, Origin: selfTestOrigin},
		crdt.Deps{crdt.SchemaChain: 0}, crdt.ClusterID{0xaa},
		[]crdt.Record{crdt.Insert{Table: crdt.TableID{0x01}, PK: crdt.PKBlob(pk), CL: cl}},
	)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return cs
}

// TestRecoverSelfRestoresExactState: replaying the self-log restores row state,
// the seq counter, the HLC, and the head LSN from the logged bytes — not by
// re-deriving anything.
func TestRecoverSelfRestoresExactState(t *testing.T) {
	j, err := journal.Open(filepath.Join(t.TempDir(), "self"), 1<<20, journal.SyncOff)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer j.Close()

	tid := crdt.TableID{0x01}
	appendSelf(t, j, 1000, selfCS(t, 1, 100, "a", 1))
	appendSelf(t, j, 2000, selfCS(t, 2, 200, "b", 1))
	appendSelf(t, j, 3000, selfCS(t, 3, 300, "a", 3)) // supersedes a@cl1

	cache := nodestate.New(selfTestOrigin)
	head, err := recoverSelf(cache, j)
	if err != nil {
		t.Fatalf("recoverSelf: %v", err)
	}
	if head != pglogrepl.LSN(3000) {
		t.Fatalf("head=%s want 3000", head)
	}
	if got := cache.SenderNextSeq(selfTestOrigin); got != 4 {
		t.Fatalf("senderNextSeq=%d want 4 (no seq reuse)", got)
	}
	if rs := cache.RowState(tid, crdt.PKBlob("a")); rs.CL != 3 {
		t.Fatalf("row a CL=%d want 3 (latest generation)", rs.CL)
	}
	if rs := cache.RowState(tid, crdt.PKBlob("b")); rs.CL != 1 {
		t.Fatalf("row b CL=%d want 1", rs.CL)
	}
	if cache.HLCLast().WallTime < 300 {
		t.Fatalf("hlcLast=%v want >= wall 300", cache.HLCLast())
	}

	// Idempotent: a second replay changes nothing.
	if _, err := recoverSelf(cache, j); err != nil {
		t.Fatalf("recoverSelf (2nd): %v", err)
	}
	if got := cache.SenderNextSeq(selfTestOrigin); got != 4 {
		t.Fatalf("senderNextSeq after re-replay=%d want 4", got)
	}
}

// countSelfLog returns the number of KindLocalDML records appended to j.
func countSelfLog(t *testing.T, j *journal.Journal) int {
	t.Helper()
	n := 0
	head := j.Head()
	it := j.Iterate(0)
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) || it.Offset() > head {
			break
		}
		if err != nil {
			t.Fatalf("count self-log: %v", err)
		}
		if rec.Kind == journal.KindLocalDML && !rec.Aborted() {
			n++
		}
	}
	return n
}

// TestFoldSkipsAlreadyShipped: a re-delivered commit at or below the self-log
// head (skipThrough) — which the slot replays when a standby ack lagged the
// append before a crash — is dropped, not re-folded. Re-folding would build a
// duplicate Dot, re-derive its stamp, and append a redundant self-log entry. A
// commit past the head folds and appends normally. fold itself never
// broadcasts with a self-log set (the publisher ships from the log), so this
// asserts on the log, and the sink must not be called.
func TestFoldSkipsAlreadyShipped(t *testing.T) {
	j, err := journal.Open(filepath.Join(t.TempDir(), "self"), 1<<20, journal.SyncOff)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer j.Close()

	cache := nodestate.New(selfTestOrigin)
	o := &orchestrator{
		capt:        &capturer{cfg: Config{Cache: cache, Origin: selfTestOrigin, Cluster: crdt.ClusterID{0xaa}}},
		selfLog:     j,
		skipThrough: pglogrepl.LSN(5000),
	}
	tid := crdt.TableID{0x01}
	draft := func(endLSN uint64, pk string) *txnAccum {
		k := rowKey{tid, pk}
		return &txnAccum{
			commitMs: 100,
			endLSN:   pglogrepl.LSN(endLSN),
			order:    []rowKey{k},
			rows:     map[rowKey]*rowAccum{k: {tid: tid, pk: crdt.PKBlob(pk), firstOp: 'i', lastOp: 'i'}},
		}
	}
	noBcast := func(context.Context, *crdt.Changeset) error {
		t.Fatal("fold broadcast inline with a self-log set; the publisher must ship")
		return nil
	}

	// endLSN <= skipThrough: dropped (no fold, no self-log append).
	if err := o.fold(context.Background(), draft(3000, "a"), noBcast); err != nil {
		t.Fatalf("fold(skipped): %v", err)
	}
	if n := countSelfLog(t, j); n != 0 {
		t.Fatalf("appended %d self-log entries for an already-shipped commit, want 0", n)
	}
	if rs := cache.RowState(tid, crdt.PKBlob("a")); rs.CL != 0 {
		t.Fatalf("re-folded a skipped commit: row CL=%d want 0", rs.CL)
	}
	if o.shipped.Load() != 3000 {
		t.Fatalf("shipped LSN=%d want 3000 (advanced past the skipped commit)", o.shipped.Load())
	}

	// endLSN > skipThrough: folded and appended to the self-log.
	if err := o.fold(context.Background(), draft(6000, "b"), noBcast); err != nil {
		t.Fatalf("fold(fresh): %v", err)
	}
	if n := countSelfLog(t, j); n != 1 {
		t.Fatalf("appended %d self-log entries for a fresh commit, want 1", n)
	}
	if rs := cache.RowState(tid, crdt.PKBlob("b")); rs.CL == 0 {
		t.Fatalf("fresh commit not folded: row b CL=0")
	}
	if o.shipped.Load() != 6000 {
		t.Fatalf("shipped LSN=%d want 6000", o.shipped.Load())
	}
}

// TestRecoverSelfYieldsToDominatingRemote: a self changeset that LOST LWW to a
// remote apply (already in the loaded snapshot) must not be re-asserted by
// replay — the DominatedBy gate keeps the remote winner, so recovery converges
// regardless of self-log vs mirror replay order.
func TestRecoverSelfYieldsToDominatingRemote(t *testing.T) {
	j, err := journal.Open(filepath.Join(t.TempDir(), "self"), 1<<20, journal.SyncOff)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer j.Close()

	tid := crdt.TableID{0x01}
	appendSelf(t, j, 1000, selfCS(t, 1, 100, "a", 1)) // self a@(cl1, wall100)

	cache := nodestate.New(selfTestOrigin)
	// A remote apply won "a" at a higher stamp before recovery (snapshot state).
	remote := crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: 9}
	cache.PutRowState(tid, crdt.PKBlob("a"), crdt.RowState{CL: 1, Base: remote})

	if _, err := recoverSelf(cache, j); err != nil {
		t.Fatalf("recoverSelf: %v", err)
	}
	if rs := cache.RowState(tid, crdt.PKBlob("a")); rs.Base != remote {
		t.Fatalf("replay clobbered the dominating remote: got %+v want %+v", rs.Base, remote)
	}
	// Seq counter still advances (the self changeset's Dot was consumed).
	if got := cache.SenderNextSeq(selfTestOrigin); got != 2 {
		t.Fatalf("senderNextSeq=%d want 2", got)
	}
}

// waitUntil polls cond until true or the deadline, failing the test on timeout.
func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPublisherShipsUnbroadcastOnRestart covers the durability crux the self-log
// exists for: entries that are in the log but were
// never broadcast — a crash between fold's append+fsync and delivery — must be
// shipped by the publisher when it (re)starts. Here the entries are appended
// directly (simulating a prior run's folds) and a fresh publisher delivers them
// all, advancing delivered. Before the async publisher these would have been
// dropped as already-shipped (skipThrough) while no peer ever received them.
func TestPublisherShipsUnbroadcastOnRestart(t *testing.T) {
	j, err := journal.Open(filepath.Join(t.TempDir(), "self"), 1<<20, journal.SyncOff)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer j.Close()
	appendSelf(t, j, 1000, selfCS(t, 1, 100, "a", 1))
	appendSelf(t, j, 2000, selfCS(t, 2, 200, "b", 1))
	appendSelf(t, j, 3000, selfCS(t, 3, 300, "c", 1))

	o := &orchestrator{selfLog: j}
	var mu sync.Mutex
	var got []crdt.Seq
	bcast := func(_ context.Context, cs *crdt.Changeset) error {
		mu.Lock()
		got = append(got, cs.Dot.Seq)
		mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() { _ = o.publish(ctx, bcast); close(doneCh) }()

	waitUntil(t, time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) >= 3 })
	cancel()
	<-doneCh

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("publisher shipped %v, want [1 2 3] in order", got)
	}
	if o.delivered.Load() != uint64(j.Head()) {
		t.Fatalf("delivered=%d want Head=%d (publisher caught up)", o.delivered.Load(), j.Head())
	}
}

// TestPublisherRetriesTransportError: a transient broadcast error must not tear
// the publisher down — it retries the same entry and the actor's path (the
// self-log) is unaffected. delivered advances only after the transport finally
// accepts the entry.
func TestPublisherRetriesTransportError(t *testing.T) {
	defer func(b time.Duration) { broadcastBackoff = b }(broadcastBackoff)
	broadcastBackoff = time.Millisecond

	j, err := journal.Open(filepath.Join(t.TempDir(), "self"), 1<<20, journal.SyncOff)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer j.Close()
	appendSelf(t, j, 1000, selfCS(t, 1, 100, "a", 1))

	o := &orchestrator{selfLog: j}
	var attempts atomic.Int32
	bcast := func(_ context.Context, _ *crdt.Changeset) error {
		if attempts.Add(1) <= 3 {
			return errors.New("transport down")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() { _ = o.publish(ctx, bcast); close(doneCh) }()

	waitUntil(t, time.Second, func() bool { return o.delivered.Load() == uint64(j.Head()) })
	cancel()
	<-doneCh

	if n := attempts.Load(); n < 4 {
		t.Fatalf("broadcast attempts=%d want >=4 (3 failures then success)", n)
	}
}
