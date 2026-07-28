package metadata

import (
	"testing"

	"github.com/wjordan/syzy/crdt"
)

func TestFrontierEmpty(t *testing.T) {
	sc, _ := openTemp(t)
	got, err := sc.Frontier()
	if err != nil {
		t.Fatalf("Frontier: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty Frontier = %v; want empty", got)
	}
}

func TestFrontierUpsertAndQuery(t *testing.T) {
	sc, _ := openTemp(t)
	c1 := crdt.Clock{WallTime: 1, Logical: 0}
	c2 := crdt.Clock{WallTime: 2, Logical: 0}
	if err := sc.AdvanceFrontier(7, 5, c1); err != nil {
		t.Fatalf("AdvanceFrontier: %v", err)
	}
	if err := sc.AdvanceFrontier(8, 10, c2); err != nil {
		t.Fatalf("AdvanceFrontier 8: %v", err)
	}
	if err := sc.AdvanceFrontier(7, 6, c2); err != nil {
		t.Fatalf("AdvanceFrontier upsert: %v", err)
	}

	got, err := sc.Frontier()
	if err != nil {
		t.Fatalf("Frontier: %v", err)
	}
	if got[7].LastSeq != 6 || !got[7].LastHLC.Equal(c2) {
		t.Errorf("frontier[7] = %v; want {6, c2}", got[7])
	}
	if got[8].LastSeq != 10 || !got[8].LastHLC.Equal(c2) {
		t.Errorf("frontier[8] = %v; want {10, c2}", got[8])
	}

	one, ok, err := sc.FrontierFor(7)
	if err != nil || !ok || one.LastSeq != 6 {
		t.Errorf("FrontierFor(7) = (%v,%v,%v); want ({6,c2},true,nil)", one, ok, err)
	}

	_, ok, err = sc.FrontierFor(99)
	if err != nil || ok {
		t.Errorf("FrontierFor(99) = (_, %v, %v); want (_, false, nil)", ok, err)
	}
}

func TestRowClockUpsert(t *testing.T) {
	sc, _ := openTemp(t)
	tab := crdt.TableID{1, 2, 3}
	pk := crdt.PKBlob{0xa, 0xb}

	if _, ok, err := sc.GetRowClock(tab, pk); err != nil || ok {
		t.Errorf("absent row_clock = (_, %v, %v); want (_, false, nil)", ok, err)
	}

	want := RowClockEntry{
		CL:   3,
		Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 100, Logical: 1}, Origin: 42},
	}
	if err := sc.PutRowClock(tab, pk, want); err != nil {
		t.Fatalf("PutRowClock: %v", err)
	}
	got, ok, err := sc.GetRowClock(tab, pk)
	if err != nil || !ok {
		t.Fatalf("GetRowClock: ok=%v err=%v", ok, err)
	}
	if got.CL != want.CL {
		t.Errorf("CL = %d; want %d", got.CL, want.CL)
	}
	if !got.Base.Equal(want.Base) {
		t.Errorf("Base = %v; want %v", got.Base, want.Base)
	}

	// Upsert moves CL forward.
	want.CL = 5
	want.Base.WallTime = 200
	if err := sc.PutRowClock(tab, pk, want); err != nil {
		t.Fatalf("PutRowClock upsert: %v", err)
	}
	got, _, _ = sc.GetRowClock(tab, pk)
	if got.CL != 5 || got.Base.WallTime != 200 {
		t.Errorf("after upsert: CL=%d wall=%d; want 5, 200", got.CL, got.Base.WallTime)
	}
}
