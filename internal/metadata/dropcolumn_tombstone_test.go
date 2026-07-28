package metadata

import (
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// TestDropColumnPreservesTombstoneFields: the drop must tombstone in
// place — name, ordinal, and create_seq survive so historical layout
// reconstruction and structural reconciliation can still resolve the
// column after it is gone.
func TestDropColumnPreservesTombstoneFields(t *testing.T) {
	sc, err := Open(filepath.Join(t.TempDir(), "syzy.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer sc.Close()

	tid := crdt.TableID{1}
	cid := crdt.ColumnID{2}
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.UpsertColumn(ColumnEntry{
			TableID: tid, ColumnID: cid, Name: "dead", Ordinal: 9,
			State: StateActive, ClockGroup: ClockGroupRow, CreateSeq: 3,
		})
	}); err != nil {
		t.Fatalf("seed column: %v", err)
	}

	if err := sc.WithTx(func(tx *Tx) error {
		return tx.ApplyCatalogOp(crdt.CatalogOp{
			Kind: crdt.OpDropColumn, TableID: tid, ColumnID: cid,
		}, 7)
	}); err != nil {
		t.Fatalf("drop: %v", err)
	}

	snap, err := sc.LoadCatalogSnapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(snap.Columns))
	}
	got := snap.Columns[0]
	want := ColumnEntry{
		TableID: tid, ColumnID: cid, Name: "dead", Ordinal: 9,
		State: StateDropped, ClockGroup: ClockGroupRow, CreateSeq: 3, DropSeq: 7,
	}
	if got != want {
		t.Errorf("tombstone = %+v\nwant        %+v", got, want)
	}

	// Replayed drop on a node that never saw the add: upsert fallback
	// records the degenerate tombstone rather than failing.
	tid2, cid2 := crdt.TableID{9}, crdt.ColumnID{8}
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.ApplyCatalogOp(crdt.CatalogOp{
			Kind: crdt.OpDropColumn, TableID: tid2, ColumnID: cid2,
		}, 8)
	}); err != nil {
		t.Fatalf("drop without add: %v", err)
	}
	snap, err = sc.LoadCatalogSnapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(snap.Columns))
	}
}
