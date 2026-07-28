package unique

import "testing"

// fakeClock is a manually-advanced µs clock for quarantine tests.
type fakeClock struct{ us int64 }

func (c *fakeClock) now() int64 { return c.us }

func kref(table, key byte) KeyRef {
	var k KeyRef
	k.Table[0], k.Key[0] = table, key
	return k
}

// farGrace keeps every unbacked grant alive across an ingest, so tests can
// exercise one behavior at a time.
const farGrace = int64(1) << 60

// serveKeys makes keys servable without touching the taken-set (an ingest
// of an empty replica with an infinite grace).
func serveKeys(rt *reservationTable, keys ...KeyRef) {
	rt.ingest(Snapshot{Keys: keys}, farGrace, nil)
}

// observeRows ingests a snapshot whose rows are exactly claims, with zero
// grace: every unbacked value not in claims exits through the release hold
// at the current clock — the "leaseholder observed the release" event.
func observeRows(rt *reservationTable, keys []KeyRef, claims ...Claim) {
	rt.ingest(Snapshot{Keys: keys, Claims: claims}, 0, nil)
}

var k11 = []KeyRef{kref(1, 1)}

func TestReservationTable_ReserveAndConflict(t *testing.T) {
	rt := newReservationTable((&fakeClock{}).now, 1000)
	serveKeys(rt, kref(1, 1))
	if ok, _ := rt.reserve([]Claim{claim("a", 1, 1, "v")}); !ok {
		t.Fatal("fresh reserve failed")
	}
	ok, conflict := rt.reserve([]Claim{claim("b", 1, 1, "v")})
	if ok || conflict == nil {
		t.Fatalf("want conflict, got ok=%v", ok)
	}
	// Same owner re-reserve is idempotent.
	if ok, _ := rt.reserve([]Claim{claim("a", 1, 1, "v")}); !ok {
		t.Fatal("idempotent re-reserve failed")
	}
}

// A key absent from the last ingested snapshot is refused with the
// (false, nil) not-serving sentinel: the taken-set for it has not been
// derived, so granting would race its existing rows. An ingested key —
// even one with zero participating rows — serves normally (empty-key
// activation).
func TestReservationTable_UnservableKeyRefused(t *testing.T) {
	rt := newReservationTable((&fakeClock{}).now, 1000)
	if ok, conflict := rt.reserve([]Claim{claim("a", 1, 1, "v")}); ok || conflict != nil {
		t.Fatalf("reserve before any ingest: ok=%v conflict=%v; want (false, nil)", ok, conflict)
	}
	serveKeys(rt, kref(1, 1))
	if ok, _ := rt.reserve([]Claim{claim("a", 1, 1, "v")}); !ok {
		t.Fatal("reserve on ingested empty key failed")
	}
	if ok, conflict := rt.reserve([]Claim{claim("a", 1, 2, "v")}); ok || conflict != nil {
		t.Fatalf("reserve on unknown key: ok=%v conflict=%v; want (false, nil)", ok, conflict)
	}
}

// Key activation over existing rows: the first ingest that carries a new
// key also seeds its rows' claims, so a stable leaseholder rejects a
// peer's claim to an existing value immediately — no failover needed.
func TestReservationTable_IngestSeedsExistingRows(t *testing.T) {
	rt := newReservationTable((&fakeClock{}).now, 1000)
	observeRows(rt, k11, claim("rowA", 1, 1, "taken@x.com"))
	if ok, conflict := rt.reserve([]Claim{claim("rowB", 1, 1, "taken@x.com")}); ok || conflict == nil {
		t.Fatalf("existing row's value granted to another owner: ok=%v", ok)
	}
	if ok, _ := rt.reserve([]Claim{claim("rowB", 1, 1, "free@x.com")}); !ok {
		t.Fatal("unrelated value refused")
	}
}

func TestReservationTable_ObservedReleaseQuarantinesThenFrees(t *testing.T) {
	clk := &fakeClock{us: 100}
	rt := newReservationTable(clk.now, 1000)
	serveKeys(rt, kref(1, 1))
	rt.reserve([]Claim{claim("a", 1, 1, "v")})
	// The rows never show v (owner deleted it / crashed pre-commit); the
	// hold starts at this observation, not at any release signal.
	observeRows(rt, k11)

	// Within the window, another owner cannot reclaim.
	clk.us = 100 + 500
	if ok, _ := rt.reserve([]Claim{claim("b", 1, 1, "v")}); ok {
		t.Fatal("reclaim within quarantine window succeeded; want conflict")
	}
	// After the window, it is free.
	clk.us = 100 + 1001
	if ok, _ := rt.reserve([]Claim{claim("b", 1, 1, "v")}); !ok {
		t.Fatal("reclaim after quarantine window failed")
	}
}

func TestReservationTable_OwnerReclaimsOwnImmediately(t *testing.T) {
	clk := &fakeClock{us: 100}
	rt := newReservationTable(clk.now, 1000)
	serveKeys(rt, kref(1, 1))
	rt.reserve([]Claim{claim("a", 1, 1, "v")})
	observeRows(rt, k11)
	// The same owner may reclaim its just-released value without waiting.
	clk.us = 100 + 1
	if ok, _ := rt.reserve([]Claim{claim("a", 1, 1, "v")}); !ok {
		t.Fatal("owner could not reclaim its own quarantined value")
	}
}

func TestReservationTable_WithinBatchDuplicateConflicts(t *testing.T) {
	// Two different owners claiming one value in a single batch must fail
	// (all-or-nothing), even though neither is in `taken` yet.
	rt := newReservationTable((&fakeClock{}).now, 1000)
	serveKeys(rt, kref(1, 1))
	ok, conflict := rt.reserve([]Claim{
		claim("p1", 1, 1, "dup"),
		claim("p2", 1, 1, "dup"),
	})
	if ok || conflict == nil {
		t.Fatalf("within-batch duplicate granted: ok=%v", ok)
	}
	if _, held := rt.ownerOf(claim("p1", 1, 1, "dup")); held {
		t.Fatal("nothing should be granted on a within-batch conflict")
	}
}

func TestReservationTable_PrevOwnerTransfer(t *testing.T) {
	rt := newReservationTable((&fakeClock{}).now, 1000)
	serveKeys(rt, kref(1, 1))
	rt.reserve([]Claim{claim("oldpk", 1, 1, "v")})

	// A PK-changing update keeps the value but moves it from oldpk to newpk.
	transfer := claim("newpk", 1, 1, "v")
	transfer.Prev = []byte("oldpk")
	if ok, _ := rt.reserve([]Claim{transfer}); !ok {
		t.Fatal("transfer from Prev owner rejected")
	}
	if o, _ := rt.ownerOf(claim("newpk", 1, 1, "v")); o != "newpk" {
		t.Fatalf("owner after transfer = %q; want newpk", o)
	}

	// A transfer naming the wrong Prev still loses to the real owner.
	bad := claim("interloper", 1, 1, "v")
	bad.Prev = []byte("someone-else")
	if ok, _ := rt.reserve([]Claim{bad}); ok {
		t.Fatal("transfer with wrong Prev unexpectedly granted")
	}
}

// An in-flight transfer survives ingests that still show the old owner's
// row: the young grant's owner wins the derived entry until the
// claimant's committed row replicates in and backs it.
func TestReservationTable_IngestKeepsInFlightTransfer(t *testing.T) {
	clk := &fakeClock{us: 100}
	rt := newReservationTable(clk.now, 1000)
	observeRows(rt, k11, claim("oldpk", 1, 1, "v"))
	transfer := claim("newpk", 1, 1, "v")
	transfer.Prev = []byte("oldpk")
	if ok, _ := rt.reserve([]Claim{transfer}); !ok {
		t.Fatal("transfer rejected")
	}
	// Rows still show oldpk (the transferring txn hasn't replicated here).
	rt.ingest(Snapshot{Keys: k11, Claims: []Claim{claim("oldpk", 1, 1, "v")}}, 1000, nil)
	if o, _ := rt.ownerOf(claim("newpk", 1, 1, "v")); o != "newpk" {
		t.Fatalf("owner after ingest = %q; want the in-flight grantee newpk", o)
	}
	// The claimant's row lands: the entry is row-backed for newpk.
	clk.us += 2000
	rt.ingest(Snapshot{Keys: k11, Claims: []Claim{claim("newpk", 1, 1, "v")}}, 1000, nil)
	if o, _ := rt.ownerOf(claim("newpk", 1, 1, "v")); o != "newpk" {
		t.Fatalf("owner after backing ingest = %q; want newpk", o)
	}
}

func TestReservationTable_BatchAllOrNothing(t *testing.T) {
	rt := newReservationTable((&fakeClock{}).now, 1000)
	serveKeys(rt, kref(1, 1))
	rt.reserve([]Claim{claim("a", 1, 1, "taken")})
	ok, _ := rt.reserve([]Claim{
		claim("b", 1, 1, "free-a"),
		claim("b", 1, 1, "taken"),
	})
	if ok {
		t.Fatal("want conflict for the batch")
	}
	if _, held := rt.ownerOf(claim("b", 1, 1, "free-a")); held {
		t.Fatal("free-a granted despite batch conflict")
	}
}

func TestReservationTable_ClearThenIngestReplacesState(t *testing.T) {
	clk := &fakeClock{us: 100}
	rt := newReservationTable(clk.now, 1000)
	serveKeys(rt, kref(1, 1))
	rt.reserve([]Claim{claim("old", 1, 1, "stale")})
	observeRows(rt, k11) // "stale" enters the hold

	// Takeover: clear + first ingest derive fresh state from the rows; the
	// drain guaranteed pre-takeover releases are stable, so the quarantine
	// restarts empty.
	rt.clear()
	observeRows(rt, k11, claim("a", 1, 1, "v"))
	if o, held := rt.ownerOf(claim("a", 1, 1, "v")); !held || o != "a" {
		t.Fatalf("derived owner = %q held=%v; want a", o, held)
	}
	clk.us = 101
	if ok, _ := rt.reserve([]Claim{claim("b", 1, 1, "stale")}); !ok {
		t.Fatal("post-takeover reclaim of cleared quarantine failed")
	}
}

// Finding-1 regression: a grant whose row never appears (reserver crashed
// between reserve and commit, or its release was lost) must exit through
// the release hold when it ages past grace — never straight to the free
// pool, where a lagging replica could still show the old row.
func TestReservationTable_AgedLeakExitsThroughHold(t *testing.T) {
	clk := &fakeClock{us: 100}
	rt := newReservationTable(clk.now, 1000)
	serveKeys(rt, kref(1, 1))
	rt.reserve([]Claim{claim("a", 1, 1, "backed")})
	rt.reserve([]Claim{claim("b", 1, 1, "leaked")})

	rowBacked := []Claim{claim("a", 1, 1, "backed")}

	// Within grace, the leak is preserved (might be in-flight).
	clk.us = 100 + 500
	rt.ingest(Snapshot{Keys: k11, Claims: rowBacked}, 1000, nil)
	if _, held := rt.ownerOf(claim("b", 1, 1, "leaked")); !held {
		t.Fatal("in-flight grant dropped within grace")
	}
	// Past grace, the unbacked grant leaves the taken-set — into the hold.
	clk.us = 100 + 1001
	rt.ingest(Snapshot{Keys: k11, Claims: rowBacked}, 1000, nil)
	if _, held := rt.ownerOf(claim("a", 1, 1, "backed")); !held {
		t.Fatal("backed reservation wrongly dropped")
	}
	if _, held := rt.ownerOf(claim("b", 1, 1, "leaked")); held {
		t.Fatal("leaked reservation still taken past grace")
	}
	// Not freed: a different owner is blocked for the full hold window
	// from the observation.
	if ok, _ := rt.reserve([]Claim{claim("c", 1, 1, "leaked")}); ok {
		t.Fatal("aged leak skipped the release hold")
	}
	clk.us += 1001
	if ok, _ := rt.reserve([]Claim{claim("c", 1, 1, "leaked")}); !ok {
		t.Fatal("value not freed after the hold elapsed")
	}
}

func TestReservationTable_SweepDropsElapsed(t *testing.T) {
	clk := &fakeClock{us: 100}
	rt := newReservationTable(clk.now, 1000)
	serveKeys(rt, kref(1, 1))
	rt.reserve([]Claim{claim("a", 1, 1, "v")})
	observeRows(rt, k11)
	clk.us = 100 + 1001
	rt.sweep()
	rt.mu.Lock()
	n := len(rt.quarantined)
	rt.mu.Unlock()
	if n != 0 {
		t.Fatalf("quarantined len = %d after sweep; want 0", n)
	}
}

// A coordinated value held by two live rows is invisible everywhere else:
// no node has a physical index for the key and apply skips arbitration.
// duplicateClaims over the enumeration snapshot is the only place it
// surfaces; ingest fences the value until the rows are repaired.
func TestDuplicateClaims(t *testing.T) {
	for _, tc := range []struct {
		name   string
		claims []Claim
		want   int
	}{
		{"clean", []Claim{claim("a", 1, 1, "v"), claim("b", 1, 1, "w")}, 0},
		{"same owner repeated", []Claim{claim("a", 1, 1, "v"), claim("a", 1, 1, "v")}, 0},
		{"two owners, one value", []Claim{claim("a", 1, 1, "v"), claim("b", 1, 1, "v")}, 1},
		{"three owners, one value", []Claim{
			claim("a", 1, 1, "v"), claim("b", 1, 1, "v"), claim("c", 1, 1, "v"),
		}, 2},
		// Same value under a different key or table is not a duplicate.
		{"other key", []Claim{claim("a", 1, 1, "v"), claim("b", 1, 2, "v")}, 0},
		{"other table", []Claim{claim("a", 1, 1, "v"), claim("b", 2, 1, "v")}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := duplicateClaims(tc.claims); len(got) != tc.want {
				t.Fatalf("duplicateClaims = %d; want %d", len(got), tc.want)
			}
		})
	}
}

// Two live rows holding one value fence that value: every grant on it is
// refused (any winner the map picked would be arbitrary), other values
// serve normally, and a repaired enumeration lifts the fence.
func TestReservationTable_DuplicateValueFencedUntilRepaired(t *testing.T) {
	clk := &fakeClock{us: 100}
	rt := newReservationTable(clk.now, 1000)
	dupRows := []Claim{claim("a", 1, 1, "v"), claim("b", 1, 1, "v"), claim("c", 1, 1, "w")}
	rt.ingest(Snapshot{Keys: k11, Claims: dupRows}, 1000, duplicateClaims(dupRows))

	for _, owner := range []string{"a", "b", "z"} {
		if ok, conflict := rt.reserve([]Claim{claim(owner, 1, 1, "v")}); ok || conflict == nil {
			t.Fatalf("fenced value granted to %q: ok=%v", owner, ok)
		}
	}
	if ok, _ := rt.reserve([]Claim{claim("c", 1, 1, "w")}); !ok {
		t.Fatal("unaffected value refused while another value is fenced")
	}

	// Operator deletes the extra row; the next enumeration lifts the fence.
	repaired := []Claim{claim("a", 1, 1, "v"), claim("c", 1, 1, "w")}
	rt.ingest(Snapshot{Keys: k11, Claims: repaired}, 1000, duplicateClaims(repaired))
	if ok, _ := rt.reserve([]Claim{claim("a", 1, 1, "v")}); !ok {
		t.Fatal("fence not lifted after a clean enumeration")
	}
}
