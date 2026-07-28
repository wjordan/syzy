package crdt

import (
	"math/rand/v2"
	"testing"
)

func TestClock_LessAndEqual(t *testing.T) {
	a := Clock{WallTime: 100, Logical: 0}
	b := Clock{WallTime: 100, Logical: 1}
	c := Clock{WallTime: 101, Logical: 0}

	if !a.Less(b) {
		t.Errorf("expected a < b on Logical")
	}
	if !b.Less(c) {
		t.Errorf("expected b < c on WallTime")
	}
	if a.Less(a) {
		t.Errorf("expected !(a < a)")
	}
	if !a.Equal(Clock{WallTime: 100, Logical: 0}) {
		t.Errorf("expected a == itself")
	}
}

func TestClock_Forward(t *testing.T) {
	a := Clock{WallTime: 100, Logical: 5}
	b := Clock{WallTime: 100, Logical: 3}
	if got := a.Forward(b); !got.Equal(a) {
		t.Errorf("Forward(a, b) = %+v, want %+v", got, a)
	}
	if got := b.Forward(a); !got.Equal(a) {
		t.Errorf("Forward(b, a) = %+v, want %+v", got, a)
	}
}

func TestStamp_Dominates_TotalOrder(t *testing.T) {
	// Invariant (5): for any two distinct Stamps, exactly one dominates.
	rng := rand.New(rand.NewPCG(1, 2))
	stamps := make([]Stamp, 200)
	for i := range stamps {
		stamps[i] = Stamp{
			Clock:  Clock{WallTime: rng.Int64N(50), Logical: rng.Int32N(5)},
			Origin: Origin(rng.Uint64N(7)),
		}
	}
	for i, a := range stamps {
		for _, b := range stamps[i+1:] {
			ad := a.Dominates(b)
			bd := b.Dominates(a)
			eq := a.Equal(b)
			switch {
			case eq && (ad || bd):
				t.Fatalf("equal stamps should not dominate: %v vs %v", a, b)
			case !eq && ad == bd:
				t.Fatalf("trichotomy violated: %v vs %v (a.Dom=%v b.Dom=%v)", a, b, ad, bd)
			}
		}
	}
}

func TestStamp_Dominates_TiebreakOrder(t *testing.T) {
	base := Clock{WallTime: 100, Logical: 5}
	a := Stamp{Clock: base, Origin: 1}
	b := Stamp{Clock: base, Origin: 2}
	if !b.Dominates(a) {
		t.Errorf("expected origin tiebreak: 2 dominates 1 at equal Clock")
	}
	if a.Dominates(b) {
		t.Errorf("expected !a.Dominates(b)")
	}
}

func TestDot_String(t *testing.T) {
	d := Dot{Origin: 7, Seq: 42}
	if got, want := d.String(), "7/42"; got != want {
		t.Errorf("Dot.String() = %q, want %q", got, want)
	}
}

func TestStamp_IsZero(t *testing.T) {
	if !(Stamp{}).IsZero() {
		t.Error("zero Stamp should be IsZero")
	}
	if (Stamp{Origin: 1}).IsZero() {
		t.Error("non-zero Origin should not be IsZero")
	}
	if (Stamp{Clock: Clock{Logical: 1}}).IsZero() {
		t.Error("non-zero Logical should not be IsZero")
	}
}
