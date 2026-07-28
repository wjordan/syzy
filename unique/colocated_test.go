package unique

import (
	"context"
	"errors"
	"testing"
)

// TestLeaseClient_ColocatedServesInProcess: when the client is co-located
// with the leaseholder (UseLocalLeaseholder) and the live lease names it as
// Owner, Reserve/Release run in-process and never dial. failDial proves the
// dial path is not taken — the point of the fix, since the published address
// is advertised for remote peers and need not be self-reachable under NAT.
func TestLeaseClient_ColocatedServesInProcess(t *testing.T) {
	store := sharedStore(t)
	clk := &fakeClock{us: 1_000_000}
	lh := newTestLeaseholder(store, "a", clk, snapEnum(nil, kref(1, 1)))
	if err := lh.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer lh.Close()
	settle(lh, clk)

	client := NewLeaseClientTransport(store, failDial{}).UseLocalLeaseholder(lh)
	client.nowUS = clk.now
	defer client.Close()
	ctx := context.Background()

	ok, _, err := client.Reserve(ctx, []Claim{claim("p1", 1, 1, "a@x.com")})
	if err != nil || !ok {
		t.Fatalf("co-located reserve: ok=%v err=%v (must not dial)", ok, err)
	}
	// A second owner colliding on the same value is still rejected in-process.
	ok, conflict, err := client.Reserve(ctx, []Claim{claim("p2", 1, 1, "a@x.com")})
	if err != nil {
		t.Fatalf("co-located conflicting reserve err: %v", err)
	}
	if ok || conflict == nil {
		t.Fatalf("want in-process conflict, got ok=%v conflict=%v", ok, conflict)
	}
	if err := client.Release(ctx, []Claim{claim("p1", 1, 1, "a@x.com")}); err != nil {
		t.Fatalf("co-located release: %v (must not dial)", err)
	}
}

// TestLeaseClient_ColocatedGatesOnOwner: the in-process path is taken only
// when the lease Owner matches the local leaseholder. A client whose local
// leaseholder is not the current owner must fall through to the dial path (it
// must reach the real, remote leader), proving the short-circuit is gated on
// ownership and can't serve a lease it doesn't hold.
func TestLeaseClient_ColocatedGatesOnOwner(t *testing.T) {
	store := sharedStore(t)
	clk := &fakeClock{us: 1_000_000}
	// "b" holds the lease.
	b := newTestLeaseholder(store, "b", clk, snapEnum(nil, kref(1, 1)))
	if err := b.Start(); err != nil {
		t.Fatalf("Start b: %v", err)
	}
	defer b.Close()
	settle(b, clk)

	// The client is co-located with "a", which does NOT own the lease, so it
	// must dial. failDial makes that observable as ErrUnavailable rather than a
	// silent (wrong) in-process serve.
	notOwner := newTestLeaseholder(store, "a", clk, snapEnum(nil, kref(1, 1)))
	client := NewLeaseClientTransport(store, failDial{}).UseLocalLeaseholder(notOwner)
	client.nowUS = clk.now
	defer client.Close()

	_, _, err := client.Reserve(context.Background(), []Claim{claim("p1", 1, 1, "a@x.com")})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Reserve err = %v; want ErrUnavailable (must dial the real owner, not serve locally)", err)
	}
}
