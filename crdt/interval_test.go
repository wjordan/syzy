package crdt

import (
	"math/rand/v2"
	"testing"
)

// stamp builds a Stamp at a given (wall, origin) for terse test setup.
func stamp(wall int64, origin Origin) Stamp {
	return Stamp{Clock: Clock{WallTime: wall}, Origin: origin}
}

func TestIntervalMap_Empty(t *testing.T) {
	m := NewIntervalMap()
	if !m.IsEmpty() {
		t.Error("new map should be empty")
	}
	if !m.At(0).IsZero() {
		t.Error("At on empty should return zero Stamp")
	}
	if got := m.Apply(5, 10, stamp(0, 0), stamp(0, 0)); len(got) != 0 {
		t.Errorf("Apply with non-dominating stamp on empty = %v, want none won", got)
	}
	if !m.IsEmpty() {
		t.Error("non-dominating Apply should not add entries")
	}
}

func TestIntervalMap_FirstWriteFillsRange(t *testing.T) {
	m := NewIntervalMap()
	c := stamp(100, 1)
	baseline := stamp(0, 0)
	won := m.Apply(5, 10, c, baseline)
	if len(won) != 1 || won[0] != (ByteRange{Start: 5, End: 10}) {
		t.Errorf("won = %v, want [{5,10}]", won)
	}
	if got := m.At(7); !got.Equal(c) {
		t.Errorf("At(7) = %v, want %v", got, c)
	}
	if got := m.At(4); !got.IsZero() {
		t.Errorf("At(4) outside range should be zero, got %v", got)
	}
	if got := m.At(10); !got.IsZero() {
		t.Errorf("At(10) at boundary should be zero (half-open), got %v", got)
	}
}

func TestIntervalMap_DominatingOverwrite(t *testing.T) {
	m := NewIntervalMap()
	low := stamp(50, 1)
	high := stamp(100, 1)
	baseline := stamp(0, 0)

	m.Apply(0, 10, low, baseline)
	won := m.Apply(3, 7, high, baseline)
	if len(won) != 1 || won[0] != (ByteRange{Start: 3, End: 7}) {
		t.Errorf("dominating overwrite won = %v, want [{3,7}]", won)
	}
	// Pre-region [0,3) at low, mid [3,7) at high, post [7,10) at low.
	for off := uint64(0); off < 3; off++ {
		if got := m.At(off); !got.Equal(low) {
			t.Errorf("At(%d) = %v, want low %v", off, got, low)
		}
	}
	for off := uint64(3); off < 7; off++ {
		if got := m.At(off); !got.Equal(high) {
			t.Errorf("At(%d) = %v, want high %v", off, got, high)
		}
	}
	for off := uint64(7); off < 10; off++ {
		if got := m.At(off); !got.Equal(low) {
			t.Errorf("At(%d) = %v, want low %v", off, got, low)
		}
	}
}

func TestIntervalMap_NonDominatingPreserved(t *testing.T) {
	m := NewIntervalMap()
	high := stamp(100, 1)
	low := stamp(50, 1)
	baseline := stamp(0, 0)

	m.Apply(0, 10, high, baseline)
	won := m.Apply(3, 7, low, baseline)
	if len(won) != 0 {
		t.Errorf("non-dominating Apply should return no won; got %v", won)
	}
	for off := uint64(0); off < 10; off++ {
		if got := m.At(off); !got.Equal(high) {
			t.Errorf("At(%d) = %v, want high %v", off, got, high)
		}
	}
}

func TestIntervalMap_BaselineGate(t *testing.T) {
	// A Stamp dominating an existing entry but NOT the baseline should
	// still overwrite the existing entry, but not fill gaps. This case
	// is rare (baseline > existing entry would violate the invariant);
	// the more interesting case: baseline dominates c → no fill.
	m := NewIntervalMap()
	baseline := stamp(200, 1)
	c := stamp(100, 1) // c does NOT dominate baseline
	won := m.Apply(0, 10, c, baseline)
	if len(won) != 0 {
		t.Errorf("Apply with c < baseline should fill nothing; got %v", won)
	}
	if !m.IsEmpty() {
		t.Errorf("Apply with c < baseline should not add entries")
	}
}

func TestIntervalMap_RunCoalesce(t *testing.T) {
	m := NewIntervalMap()
	c := stamp(100, 1)
	baseline := stamp(0, 0)
	// Two adjacent writes with the same Stamp should coalesce.
	m.Apply(0, 5, c, baseline)
	m.Apply(5, 10, c, baseline)
	es := m.Entries()
	if len(es) != 1 {
		t.Fatalf("expected coalesced into 1 entry, got %d: %v", len(es), es)
	}
	if es[0].Range != (ByteRange{Start: 0, End: 10}) {
		t.Errorf("coalesced range = %v, want [0,10)", es[0].Range)
	}
}

func TestIntervalMap_Prune(t *testing.T) {
	m := NewIntervalMap()
	baseline := stamp(0, 0)
	low := stamp(50, 1)
	mid := stamp(100, 1)
	high := stamp(200, 1)
	m.Apply(0, 5, low, baseline)
	m.Apply(5, 10, mid, baseline)
	m.Apply(10, 15, high, baseline)

	m.Prune(mid)
	es := m.Entries()
	// Drops entries with Stamp <= mid: low (50) and mid (100). Keeps high.
	if len(es) != 1 {
		t.Fatalf("after Prune(mid) entries = %d, want 1: %v", len(es), es)
	}
	if !es[0].Stamp.Equal(high) {
		t.Errorf("surviving entry stamp = %v, want high %v", es[0].Stamp, high)
	}
}

func TestIntervalMap_Clip(t *testing.T) {
	m := NewIntervalMap()
	baseline := stamp(0, 0)
	c := stamp(100, 1)
	m.Apply(0, 20, c, baseline)
	m.Clip(15)
	es := m.Entries()
	if len(es) != 1 || es[0].Range != (ByteRange{Start: 0, End: 15}) {
		t.Errorf("after Clip(15) = %v, want [{0,15}]", es)
	}
	m.Clip(0)
	if !m.IsEmpty() {
		t.Errorf("Clip(0) should empty the map, got %v", m.Entries())
	}
}

func TestIntervalMap_ConvergesUnderRandomOrders(t *testing.T) {
	// Property: applying the same set of patches in different orders
	// converges to the same final byte → Stamp mapping (per-byte LWW).
	type patch struct {
		start, end uint64
		stamp      Stamp
	}
	patches := []patch{
		{0, 100, stamp(10, 1)},
		{20, 50, stamp(20, 2)},
		{40, 60, stamp(15, 3)},
		{0, 30, stamp(25, 1)},
		{70, 90, stamp(5, 2)},
		{80, 100, stamp(30, 3)},
	}
	baseline := stamp(0, 0)

	// Compute canonical answer by applying in stamp-ascending order
	// (each strictly dominates baseline; order doesn't matter for LWW).
	canonical := make([]Stamp, 100)
	for off := range canonical {
		var winner Stamp
		for _, p := range patches {
			if uint64(off) >= p.start && uint64(off) < p.end {
				if p.stamp.Dominates(winner) {
					winner = p.stamp
				}
			}
		}
		canonical[off] = winner
	}

	for trial := 0; trial < 50; trial++ {
		rng := rand.New(rand.NewPCG(uint64(trial), 0))
		order := make([]patch, len(patches))
		copy(order, patches)
		rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })

		m := NewIntervalMap()
		for _, p := range order {
			m.Apply(p.start, p.end, p.stamp, baseline)
		}
		for off, want := range canonical {
			if got := m.At(uint64(off)); !got.Equal(want) {
				t.Fatalf("trial %d off=%d got %v want %v", trial, off, got, want)
			}
		}
	}
}

func TestIntervalMap_IdempotentApply(t *testing.T) {
	// Invariant (9): re-applying the same patch is a no-op.
	m := NewIntervalMap()
	c := stamp(100, 1)
	baseline := stamp(0, 0)
	m.Apply(5, 15, c, baseline)
	es1 := append([]IntervalEntry(nil), m.Entries()...)

	won := m.Apply(5, 15, c, baseline)
	if len(won) != 0 {
		t.Errorf("re-apply should yield no won, got %v", won)
	}
	es2 := m.Entries()
	if len(es1) != len(es2) {
		t.Fatalf("re-apply changed entry count: %d vs %d", len(es1), len(es2))
	}
	for i := range es1 {
		if es1[i] != es2[i] {
			t.Errorf("entry[%d] changed: %v -> %v", i, es1[i], es2[i])
		}
	}
}

func TestIntervalMap_AdjacentDifferentStampsNoCoalesce(t *testing.T) {
	m := NewIntervalMap()
	c1 := stamp(100, 1)
	c2 := stamp(200, 2)
	baseline := stamp(0, 0)
	m.Apply(0, 5, c1, baseline)
	m.Apply(5, 10, c2, baseline)
	es := m.Entries()
	if len(es) != 2 {
		t.Fatalf("different-stamp adjacent should not coalesce: %v", es)
	}
}
