package metadata

import (
	"testing"

	"github.com/wjordan/syzy/crdt"
)

func TestCellClockUpsertAndDelete(t *testing.T) {
	sc, _ := openTemp(t)
	tab := crdt.TableID{0xa1}
	pk := crdt.PKBlob{0x01}
	colA := crdt.ColumnID{0xC1}
	colB := crdt.ColumnID{0xC2}
	stampA := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: 7}
	stampB := crdt.Stamp{Clock: crdt.Clock{WallTime: 200, Logical: 3}, Origin: 11}

	if got, err := sc.GetCellClocks(tab, pk); err != nil || got != nil {
		t.Fatalf("empty row: got=%v err=%v", got, err)
	}

	if err := sc.WithTx(func(tx *Tx) error {
		if err := tx.PutCellClock(tab, pk, colA, stampA); err != nil {
			return err
		}
		return tx.PutCellClock(tab, pk, colB, stampB)
	}); err != nil {
		t.Fatalf("PutCellClock: %v", err)
	}

	got, err := sc.GetCellClocks(tab, pk)
	if err != nil {
		t.Fatalf("GetCellClocks: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries; want 2", len(got))
	}
	byCol := map[crdt.ColumnID]crdt.Stamp{}
	for _, e := range got {
		byCol[e.Column] = e.Stamp
	}
	if byCol[colA] != stampA {
		t.Errorf("colA stamp = %v; want %v", byCol[colA], stampA)
	}
	if byCol[colB] != stampB {
		t.Errorf("colB stamp = %v; want %v", byCol[colB], stampB)
	}

	// Update one column to a fresh stamp; the other should remain.
	stampA2 := crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: 13}
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.PutCellClock(tab, pk, colA, stampA2)
	}); err != nil {
		t.Fatalf("update colA: %v", err)
	}
	got, err = sc.GetCellClocks(tab, pk)
	if err != nil {
		t.Fatalf("GetCellClocks after update: %v", err)
	}
	byCol = map[crdt.ColumnID]crdt.Stamp{}
	for _, e := range got {
		byCol[e.Column] = e.Stamp
	}
	if byCol[colA] != stampA2 {
		t.Errorf("colA after update = %v; want %v", byCol[colA], stampA2)
	}
	if byCol[colB] != stampB {
		t.Errorf("colB after update = %v; want %v", byCol[colB], stampB)
	}

	// Delete one column.
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.DeleteCellClock(tab, pk, colA)
	}); err != nil {
		t.Fatalf("DeleteCellClock: %v", err)
	}
	got, err = sc.GetCellClocks(tab, pk)
	if err != nil {
		t.Fatalf("GetCellClocks after one-col delete: %v", err)
	}
	if len(got) != 1 || got[0].Column != colB {
		t.Errorf("after delete colA: got=%v", got)
	}

	// Delete-all-for-row.
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.DeleteCellClocksForRow(tab, pk)
	}); err != nil {
		t.Fatalf("DeleteCellClocksForRow: %v", err)
	}
	got, err = sc.GetCellClocks(tab, pk)
	if err != nil || got != nil {
		t.Errorf("after row delete: got=%v err=%v", got, err)
	}
}

func TestCellClockAllScan(t *testing.T) {
	sc, _ := openTemp(t)
	tabA := crdt.TableID{0xa1}
	tabB := crdt.TableID{0xa2}
	pk1 := crdt.PKBlob{0x01}
	pk2 := crdt.PKBlob{0x02}
	col := crdt.ColumnID{0xC1}
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 99}, Origin: 5}

	if err := sc.WithTx(func(tx *Tx) error {
		if err := tx.PutCellClock(tabA, pk1, col, stamp); err != nil {
			return err
		}
		if err := tx.PutCellClock(tabA, pk2, col, stamp); err != nil {
			return err
		}
		return tx.PutCellClock(tabB, pk1, col, stamp)
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	all, err := sc.AllCellClocks()
	if err != nil {
		t.Fatalf("AllCellClocks: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("got %d entries; want 3", len(all))
	}
}
