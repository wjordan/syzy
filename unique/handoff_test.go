package unique

import (
	"context"
	"testing"

	"github.com/wjordan/objectstore"
)

func newTestLeaseholderH(store *LeaseStore, handoff *HandoffStore, owner string, clk *fakeClock, enum func(context.Context) (Snapshot, error)) *Leaseholder {
	return NewLeaseholder(LeaseholderConfig{
		Store: store, Handoff: handoff, Owner: owner,
		Enumerate:    enum,
		TTLUS:        1_000_000,
		DrainUS:      100,
		QuarantineUS: 1000,
		GraceUS:      1000,
		NowUS:        clk.now,
	})
}

// The snapshot captures the live taken-set and quarantine; after it the table
// refuses grants with the (false,nil) not-serving sentinel; a fresh table that
// loads it resumes the exact state, quarantine windows included.
func TestReservationTable_SnapshotStopAndLoad(t *testing.T) {
	clk := &fakeClock{us: 1000}
	rt := newReservationTable(clk.now, 1000)
	serveKeys(rt, kref(1, 1))
	if ok, _ := rt.reserve([]Claim{claim("a", 1, 1, "v1")}); !ok {
		t.Fatal("reserve v1")
	}
	if ok, _ := rt.reserve([]Claim{claim("b", 1, 1, "v2")}); !ok {
		t.Fatal("reserve v2")
	}
	// The rows come to show only v1 (v2's owner deleted it): v2 enters the
	// hold at that observation.
	rt.ingest(Snapshot{Keys: k11, Claims: []Claim{claim("a", 1, 1, "v1")}}, 0, nil)

	snap := rt.snapshotAndStop()
	if ok, conflict := rt.reserve([]Claim{claim("c", 1, 1, "v3")}); ok || conflict != nil {
		t.Fatalf("stopped table must refuse with (false,nil); got ok=%v conflict=%v", ok, conflict)
	}

	rt2 := newReservationTable(clk.now, 1000)
	rt2.load(snap)
	serveKeys(rt2, kref(1, 1)) // the successor's first ingest marks keys servable
	if ok, _ := rt2.reserve([]Claim{claim("z", 1, 1, "v1")}); ok {
		t.Fatal("v1 must stay taken after load")
	}
	if ok, _ := rt2.reserve([]Claim{claim("z", 1, 1, "v2")}); ok {
		t.Fatal("v2 must stay quarantined for a different owner after load")
	}
	if ok, _ := rt2.reserve([]Claim{claim("b", 1, 1, "v2")}); !ok {
		t.Fatal("original owner b must reclaim its quarantined v2 after load")
	}
}

func TestHandoffStore_WriteRead(t *testing.T) {
	b, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	h := OpenHandoff(b, "unique/handoff")
	ctx := context.Background()
	if _, _, ok, err := h.Read(ctx); ok || err != nil {
		t.Fatalf("empty read: ok=%v err=%v", ok, err)
	}
	want := tableSnapshot{Taken: []takenSnap{{Key: []byte{0xde, 0xad}, Owner: []byte{0xbe, 0xef}, ReservedUS: 7}}}
	if err := h.Write(ctx, 41, want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	snap, gen, ok, err := h.Read(ctx)
	if err != nil || !ok || gen != 41 {
		t.Fatalf("Read: gen=%d ok=%v err=%v", gen, ok, err)
	}
	if len(snap.Taken) != 1 || string(snap.Taken[0].Owner) != "\xbe\xef" || string(snap.Taken[0].Key) != "\xde\xad" {
		t.Fatalf("roundtrip mangled binary owner/key: %+v", snap.Taken)
	}
}

// Graceful shutdown: the successor adopts the published taken-set and serves
// immediately — no drain — and still knows the reservation even though enum
// (the replica rebuild source) is EMPTY, proving the state came from the
// handoff, not a rebuild.
func TestLeaseholder_GracefulHandoffSkipsDrain(t *testing.T) {
	bk, _ := objectstore.OpenFS(t.TempDir())
	leaseStore := OpenLease(bk, "unique/leader")
	handoff := OpenHandoff(bk, "unique/handoff")
	clk := &fakeClock{us: 1_000_000}
	ctx := context.Background()
	emptyEnum := snapEnum(nil, kref(1, 1))

	a := newTestLeaseholderH(leaseStore, handoff, "a", clk, emptyEnum)
	if err := a.Start(); err != nil {
		t.Fatalf("a Start: %v", err)
	}
	settle(a, clk)

	client := NewLeaseClient(leaseStore)
	client.nowUS = clk.now
	defer client.Close()
	if ok, _, err := client.Reserve(ctx, []Claim{claim("p1", 1, 1, "v")}); err != nil || !ok {
		t.Fatalf("reserve via A: ok=%v err=%v", ok, err)
	}

	a.Close() // graceful: publishes the handoff (gen 1) and releases the lease

	b := newTestLeaseholderH(leaseStore, handoff, "b", clk, emptyEnum)
	if err := b.Start(); err != nil {
		t.Fatalf("b Start: %v", err)
	}
	defer b.Close()
	b.tick(ctx) // acquire gen 2 + adopt handoff — NO drain advance

	if !b.canServe(b.lease.Generation) {
		t.Fatal("B must serve immediately after adopting a graceful handoff (no drain)")
	}
	ok, conflict, err := client.Reserve(ctx, []Claim{claim("p2", 1, 1, "v")})
	if err != nil {
		t.Fatalf("reserve via B: %v", err)
	}
	if ok || conflict == nil {
		t.Fatalf("graceful handoff lost the reservation (enum is empty): ok=%v", ok)
	}
}

// Ungraceful failover (no handoff published): the successor must NOT serve
// until it has crossed the drain and rebuilt from the replica.
func TestLeaseholder_UngracefulFailoverDrains(t *testing.T) {
	bk, _ := objectstore.OpenFS(t.TempDir())
	leaseStore := OpenLease(bk, "unique/leader")
	handoff := OpenHandoff(bk, "unique/handoff")
	clk := &fakeClock{us: 1_000_000}
	ctx := context.Background()
	rows := []Claim{claim("p1", 1, 1, "v")}
	enum := snapEnum(rows, kref(1, 1))

	a := newTestLeaseholderH(leaseStore, handoff, "a", clk, enum)
	if err := a.Start(); err != nil {
		t.Fatalf("a Start: %v", err)
	}
	settle(a, clk)
	_ = a.tr.Close()          // simulate crash: RPC gone, NO releaseLease/handoff
	clk.us += a.cfg.TTLUS + 1 // A's lease lapses

	b := newTestLeaseholderH(leaseStore, handoff, "b", clk, enum)
	if err := b.Start(); err != nil {
		t.Fatalf("b Start: %v", err)
	}
	defer b.Close()
	b.tick(ctx) // acquire gen 2; no handoff → needRebuild + drain
	if b.canServe(b.lease.Generation) {
		t.Fatal("B served without the drain on an ungraceful failover")
	}
	clk.us += b.cfg.DrainUS + 1
	b.tick(ctx) // rebuild from rows
	b.tick(ctx) // serve
	if !b.canServe(b.lease.Generation) {
		t.Fatal("B not serving after drain+rebuild")
	}

	client := NewLeaseClient(leaseStore)
	client.nowUS = clk.now
	defer client.Close()
	ok, conflict, err := client.Reserve(ctx, []Claim{claim("p2", 1, 1, "v")})
	if err != nil {
		t.Fatalf("reserve via B: %v", err)
	}
	if ok || conflict == nil {
		t.Fatalf("rebuild lost the reservation: ok=%v", ok)
	}
}

// Only a direct baton pass (tag == acquiring gen - 1) is adopted; a stale tag
// falls through to rebuild+drain.
func TestLeaseholder_HandoffGenTagGuard(t *testing.T) {
	bk, _ := objectstore.OpenFS(t.TempDir())
	handoff := OpenHandoff(bk, "unique/handoff")
	clk := &fakeClock{us: 1000}
	ctx := context.Background()
	lh := newTestLeaseholderH(OpenLease(bk, "unique/leader"), handoff, "a", clk, snapEnum(nil, kref(1, 1)))

	snap := tableSnapshot{Taken: []takenSnap{{Key: []byte("x"), Owner: []byte("o"), ReservedUS: 1}}}
	if err := handoff.Write(ctx, 99, snap); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if lh.tryAdoptHandoff(ctx, 2) {
		t.Fatal("adopted a stale-generation handoff (tag 99, acquiring gen 2)")
	}
	if err := handoff.Write(ctx, 1, snap); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !lh.tryAdoptHandoff(ctx, 2) {
		t.Fatal("did not adopt the immediately-prior handoff (tag 1, acquiring gen 2)")
	}
}
