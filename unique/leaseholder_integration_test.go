package unique

import (
	"context"
	"errors"
	"testing"

	"github.com/wjordan/objectstore"
)

func sharedStore(t *testing.T) *LeaseStore {
	t.Helper()
	b, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	return OpenLease(b, "unique/leader")
}

// settle drives the leaseholder to a serving state on the shared fake
// clock: acquire, advance past the drain window, then rebuild + serve.
func settle(l *Leaseholder, clk *fakeClock) {
	l.tick(context.Background()) // acquire (sets serveAfter = now + drain)
	clk.us += l.cfg.DrainUS + 1  // cross the failover drain
	l.tick(context.Background()) // renew + rebuild from rows
	l.tick(context.Background()) // serve
}

// snapEnum returns an Enumerate producing a snapshot with the given
// (fixed) row-backed claims under the given key identities.
func snapEnum(rows []Claim, keys ...KeyRef) func(context.Context) (Snapshot, error) {
	return func(context.Context) (Snapshot, error) {
		return Snapshot{Keys: keys, Claims: rows}, nil
	}
}

// newTestLeaseholder builds a leaseholder on the shared clock with short
// windows suited to deterministic tick-driven tests.
func newTestLeaseholder(store *LeaseStore, owner string, clk *fakeClock, enum func(context.Context) (Snapshot, error)) *Leaseholder {
	return NewLeaseholder(LeaseholderConfig{
		Store: store, Owner: owner,
		Enumerate:    enum,
		TTLUS:        1_000_000,
		DrainUS:      100,
		QuarantineUS: 1000,
		GraceUS:      1000,
		NowUS:        clk.now,
	})
}

func TestLeaseholder_ClientReserveThroughLeader(t *testing.T) {
	store := sharedStore(t)
	clk := &fakeClock{us: 1_000_000}
	lh := newTestLeaseholder(store, "a", clk, snapEnum(nil, kref(1, 1)))
	if err := lh.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer lh.Close()
	settle(lh, clk)

	client := NewLeaseClient(store)
	client.nowUS = clk.now
	defer client.Close()
	ctx := context.Background()

	ok, _, err := client.Reserve(ctx, []Claim{claim("p1", 1, 1, "a@x.com")})
	if err != nil || !ok {
		t.Fatalf("reserve via leader: ok=%v err=%v", ok, err)
	}
	ok, conflict, err := client.Reserve(ctx, []Claim{claim("p2", 1, 1, "a@x.com")})
	if err != nil {
		t.Fatalf("conflicting reserve err: %v", err)
	}
	if ok || conflict == nil {
		t.Fatalf("want conflict, got ok=%v", ok)
	}
	if err := client.Release(ctx, []Claim{claim("p1", 1, 1, "a@x.com")}); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestLeaseClient_UnavailableWithoutLeader(t *testing.T) {
	store := sharedStore(t)
	client := NewLeaseClient(store)
	defer client.Close()
	_, _, err := client.Reserve(context.Background(), []Claim{claim("p1", 1, 1, "v")})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v; want ErrUnavailable", err)
	}
}

func TestLeaseholder_FollowerDoesNotSteal(t *testing.T) {
	store := sharedStore(t)
	clk := &fakeClock{us: 1_000_000}
	enum := snapEnum(nil, kref(1, 1))
	a := newTestLeaseholder(store, "a", clk, enum)
	b := newTestLeaseholder(store, "b", clk, enum)
	if err := a.Start(); err != nil {
		t.Fatalf("a Start: %v", err)
	}
	defer a.Close()
	if err := b.Start(); err != nil {
		t.Fatalf("b Start: %v", err)
	}
	defer b.Close()

	settle(a, clk)
	b.tick(context.Background())
	if b.canServe(b.lease.Generation) {
		t.Fatal("b became leader while a holds the lease")
	}
	if !a.canServe(a.lease.Generation) {
		t.Fatal("a is not serving as expected")
	}
}

// A reserve naming a key outside the last enumeration snapshot (a
// just-created key) answers NotLeader and kicks the maintenance loop, so
// the key activates on the client's first retry instead of waiting out
// the scheduled tick.
func TestLeaseholder_UnknownKeyKicksMaintenance(t *testing.T) {
	store := sharedStore(t)
	clk := &fakeClock{us: 1_000_000}
	keys := []KeyRef{kref(1, 1)}
	enum := func(context.Context) (Snapshot, error) {
		return Snapshot{Keys: keys}, nil
	}
	lh := newTestLeaseholder(store, "a", clk, enum)
	if err := lh.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer lh.Close()
	settle(lh, clk)

	gen := lh.lease.Generation
	if _, _, notLeader := lh.ReserveLocal(gen, []Claim{claim("p", 2, 2, "v")}); !notLeader {
		t.Fatal("reserve on a not-yet-enumerated key must answer NotLeader")
	}
	select {
	case <-lh.kick:
	default:
		t.Fatal("unknown-key refusal did not kick maintenance")
	}
	// The key appears in the next enumeration (the kicked tick): served.
	keys = append(keys, kref(2, 2))
	lh.tick(context.Background())
	if ok, _, notLeader := lh.ReserveLocal(gen, []Claim{claim("p", 2, 2, "v")}); !ok || notLeader {
		t.Fatalf("key not served after kicked tick: ok=%v notLeader=%v", ok, notLeader)
	}
}

// TestLeaseholder_FailoverPreservesReservations: A leads with a value
// reserved; A relinquishes; B takes over, rebuilds the taken-set from the
// (replicated) rows, and a different owner is still rejected through B.
func TestLeaseholder_FailoverPreservesReservations(t *testing.T) {
	store := sharedStore(t)
	clk := &fakeClock{us: 1_000_000}
	ctx := context.Background()

	rows := []Claim{claim("p1", 1, 1, "a@x.com")}
	enum := snapEnum(rows, kref(1, 1))

	a := newTestLeaseholder(store, "a", clk, enum)
	if err := a.Start(); err != nil {
		t.Fatalf("a Start: %v", err)
	}
	settle(a, clk)

	client := NewLeaseClient(store)
	client.nowUS = clk.now
	defer client.Close()
	if ok, _, _ := client.Reserve(ctx, []Claim{claim("p2", 1, 1, "a@x.com")}); ok {
		t.Fatal("conflicting reserve via A unexpectedly succeeded")
	}

	a.Close() // failover: A relinquishes the lease

	b := newTestLeaseholder(store, "b", clk, enum)
	if err := b.Start(); err != nil {
		t.Fatalf("b Start: %v", err)
	}
	defer b.Close()
	settle(b, clk) // B acquires (gen 2) and rebuilds from rows

	ok, conflict, err := client.Reserve(ctx, []Claim{claim("p2", 1, 1, "a@x.com")})
	if err != nil {
		t.Fatalf("reserve via B after failover: %v", err)
	}
	if ok || conflict == nil {
		t.Fatalf("failover lost the reservation: ok=%v", ok)
	}
}
