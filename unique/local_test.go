package unique

import (
	"context"
	"testing"
)

// claim builds a Claim owned by owner with single-byte table/key tags.
func claim(owner string, table, key byte, value string) Claim {
	c := Claim{Value: []byte(value), Owner: []byte(owner)}
	c.Table[0] = table
	c.Key[0] = key
	return c
}

func mustReserve(t *testing.T, r Registry, claims ...Claim) {
	t.Helper()
	ok, conflict, err := r.Reserve(context.Background(), claims)
	if err != nil {
		t.Fatalf("Reserve: unexpected err %v", err)
	}
	if !ok {
		t.Fatalf("Reserve: want ok, got conflict %+v", conflict)
	}
}

func TestLocal_FreshReserveSucceeds(t *testing.T) {
	r := NewLocal()
	mustReserve(t, r, claim("alice", 1, 1, "alice@example.com"))

	o, held := r.Owner(claim("alice", 1, 1, "alice@example.com"))
	if !held || string(o) != "alice" {
		t.Fatalf("Owner = %q, held=%v; want alice/true", o, held)
	}
}

func TestLocal_ConflictNamesClaimAndGrantsNothing(t *testing.T) {
	r := NewLocal()
	mustReserve(t, r, claim("alice", 1, 1, "e@x.com"))

	// A different owner claiming the same value loses.
	ok, conflict, err := r.Reserve(context.Background(),
		[]Claim{claim("bob", 1, 1, "e@x.com")})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok {
		t.Fatal("Reserve: want conflict, got ok")
	}
	if conflict == nil || string(conflict.Value) != "e@x.com" {
		t.Fatalf("conflict = %+v; want value e@x.com", conflict)
	}
	// Value still owned by alice, untouched.
	o, _ := r.Owner(claim("alice", 1, 1, "e@x.com"))
	if string(o) != "alice" {
		t.Fatalf("Owner = %q; want alice", o)
	}
}

func TestLocal_IdempotentReReserveBySameOwner(t *testing.T) {
	r := NewLocal()
	c := claim("alice", 1, 1, "e@x.com")
	mustReserve(t, r, c)
	mustReserve(t, r, c) // replay / re-assert is a success
}

func TestLocal_BatchAtomicity(t *testing.T) {
	r := NewLocal()
	mustReserve(t, r, claim("carol", 1, 1, "taken"))

	// bob's batch: free, taken-by-carol, free. The conflict must abort the
	// whole batch — neither free value gets reserved.
	ok, conflict, err := r.Reserve(context.Background(), []Claim{
		claim("bob", 1, 1, "free-a"),
		claim("bob", 1, 1, "taken"),
		claim("bob", 1, 1, "free-b"),
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok {
		t.Fatal("Reserve: want conflict")
	}
	if conflict == nil || string(conflict.Value) != "taken" {
		t.Fatalf("conflict = %+v; want value taken", conflict)
	}
	if _, held := r.Owner(claim("bob", 1, 1, "free-a")); held {
		t.Fatal("free-a was reserved despite batch conflict")
	}
	if _, held := r.Owner(claim("bob", 1, 1, "free-b")); held {
		t.Fatal("free-b was reserved despite batch conflict")
	}
}

func TestLocal_MultiOwnerBatch(t *testing.T) {
	// One Reserve call can carry claims with distinct owners (a txn that
	// inserts several rows). All succeed or none do.
	r := NewLocal()
	mustReserve(t, r,
		claim("p1", 1, 1, "a@x.com"),
		claim("p2", 1, 1, "b@x.com"),
	)
	if o, _ := r.Owner(claim("p1", 1, 1, "a@x.com")); string(o) != "p1" {
		t.Fatalf("a owner = %q; want p1", o)
	}
	if o, _ := r.Owner(claim("p2", 1, 1, "b@x.com")); string(o) != "p2" {
		t.Fatalf("b owner = %q; want p2", o)
	}
}

func TestLocal_ReleaseFreesForReclaim(t *testing.T) {
	r := NewLocal()
	c := claim("alice", 1, 1, "e@x.com")
	mustReserve(t, r, c)

	if err := r.Release(context.Background(), []Claim{c}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, held := r.Owner(c); held {
		t.Fatal("claim still held after Release")
	}
	// Reclaim by a new owner now succeeds.
	bob := claim("bob", 1, 1, "e@x.com")
	mustReserve(t, r, bob)
	if o, _ := r.Owner(bob); string(o) != "bob" {
		t.Fatalf("Owner = %q after reclaim; want bob", o)
	}
}

func TestLocal_ReleaseByNonOwnerIsNoop(t *testing.T) {
	r := NewLocal()
	mustReserve(t, r, claim("alice", 1, 1, "e@x.com"))

	// bob releasing alice's value is a no-op.
	if err := r.Release(context.Background(), []Claim{claim("bob", 1, 1, "e@x.com")}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if o, held := r.Owner(claim("alice", 1, 1, "e@x.com")); !held || string(o) != "alice" {
		t.Fatalf("Owner = %q held=%v; alice's claim must survive a foreign Release", o, held)
	}
}

func TestLocal_DistinctKeysAndTablesDoNotCollide(t *testing.T) {
	r := NewLocal()
	// Same value bytes, different (table, key) namespaces are independent.
	mustReserve(t, r, claim("alice", 1, 1, "v"))
	mustReserve(t, r, claim("bob", 1, 2, "v"))   // different key
	mustReserve(t, r, claim("carol", 2, 1, "v")) // different table
}

func TestLocal_EmptyBatchSucceeds(t *testing.T) {
	r := NewLocal()
	mustReserve(t, r) // no claims
}

func TestLocal_ContextCancellation(t *testing.T) {
	r := NewLocal()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := r.Reserve(ctx, []Claim{claim("alice", 1, 1, "v")}); err == nil {
		t.Fatal("Reserve: want context error")
	}
	if err := r.Release(ctx, []Claim{claim("alice", 1, 1, "v")}); err == nil {
		t.Fatal("Release: want context error")
	}
}
