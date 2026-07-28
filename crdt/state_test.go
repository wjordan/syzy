package crdt

import "testing"

func TestRowState_LivenessHelpers(t *testing.T) {
	cases := []struct {
		cl                 uint64
		never, live, tomb  bool
		nextLive, nextTomb uint64
	}{
		{cl: 0, never: true, live: false, tomb: false, nextLive: 1, nextTomb: 0},
		{cl: 1, never: false, live: true, tomb: false, nextLive: 1, nextTomb: 2},
		{cl: 2, never: false, live: false, tomb: true, nextLive: 3, nextTomb: 2},
		{cl: 3, never: false, live: true, tomb: false, nextLive: 3, nextTomb: 4},
		{cl: 4, never: false, live: false, tomb: true, nextLive: 5, nextTomb: 4},
	}
	for _, tc := range cases {
		r := RowState{CL: tc.cl}
		if got := r.IsNeverExisted(); got != tc.never {
			t.Errorf("CL=%d IsNeverExisted=%v want %v", tc.cl, got, tc.never)
		}
		if got := r.IsLive(); got != tc.live {
			t.Errorf("CL=%d IsLive=%v want %v", tc.cl, got, tc.live)
		}
		if got := r.IsTombstoned(); got != tc.tomb {
			t.Errorf("CL=%d IsTombstoned=%v want %v", tc.cl, got, tc.tomb)
		}
		if got := r.NextLiveCL(); got != tc.nextLive {
			t.Errorf("CL=%d NextLiveCL=%d want %d", tc.cl, got, tc.nextLive)
		}
		if got := r.NextTombCL(); got != tc.nextTomb {
			t.Errorf("CL=%d NextTombCL=%d want %d", tc.cl, got, tc.nextTomb)
		}
	}
}

func TestRowState_CL_MonotonicTransitions(t *testing.T) {
	// Invariant (7): per-pk CL only grows under the producer's transition
	// rules.
	r := RowState{CL: 0}
	// INSERT on never-existed
	r.CL = r.NextLiveCL()
	if r.CL != 1 {
		t.Fatalf("after first INSERT CL = %d, want 1", r.CL)
	}
	// UPDATE doesn't bump CL on the producer; the producer simply does
	// not call NextLiveCL/NextTombCL for UPDATE. Skip a no-op transition.
	// DELETE on live
	r.CL = r.NextTombCL()
	if r.CL != 2 {
		t.Fatalf("after DELETE CL = %d, want 2", r.CL)
	}
	// Resurrecting INSERT on tombstone
	r.CL = r.NextLiveCL()
	if r.CL != 3 {
		t.Fatalf("after resurrect CL = %d, want 3", r.CL)
	}
}

func TestRowState_EffectiveStamp_Fallthrough(t *testing.T) {
	col := ColumnID{0xAA}
	base := Stamp{Clock: Clock{WallTime: 100}, Origin: 1}
	cell := Stamp{Clock: Clock{WallTime: 200}, Origin: 2}

	r := RowState{
		CL:    1,
		Base:  base,
		Cells: map[ColumnID]Stamp{col: cell},
	}

	// No range, no override on a different column → Base.
	other := ColumnID{0xBB}
	if got := r.EffectiveStamp(other, ByteRange{}); !got.Equal(base) {
		t.Errorf("non-overridden column = %v, want Base %v", got, base)
	}
	// No range, override on this column → Cells[col].
	if got := r.EffectiveStamp(col, ByteRange{}); !got.Equal(cell) {
		t.Errorf("overridden column with no range = %v, want Cell %v", got, cell)
	}
}

func TestRowState_EffectiveStamp_NeverExisted(t *testing.T) {
	r := RowState{}
	col := ColumnID{0xCC}
	if got := r.EffectiveStamp(col, ByteRange{}); !got.IsZero() {
		t.Errorf("never-existed row should yield zero Stamp, got %v", got)
	}
}

func TestByteRange_Helpers(t *testing.T) {
	if !(ByteRange{}).Empty() {
		t.Error("zero ByteRange should be Empty")
	}
	if (ByteRange{Start: 5, End: 10}).Empty() {
		t.Error("non-empty range should not be Empty")
	}
	if got := (ByteRange{Start: 5, End: 10}).Length(); got != 5 {
		t.Errorf("Length = %d, want 5", got)
	}
	if !(ByteRange{Start: 1, End: 5}).Overlaps(ByteRange{Start: 3, End: 8}) {
		t.Error("expected overlap")
	}
	if (ByteRange{Start: 1, End: 3}).Overlaps(ByteRange{Start: 3, End: 5}) {
		t.Error("touching but non-overlapping (half-open)")
	}
}
