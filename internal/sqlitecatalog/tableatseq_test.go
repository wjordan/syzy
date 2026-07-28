package catalog

import (
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// applyOp runs one catalog op at schemaSeq and advances meta.schema_seq,
// mirroring what a DDL resolve/catch-up apply does to the metadata.
func applyOp(t *testing.T, sc *metadata.Store, op crdt.CatalogOp, schemaSeq uint64) {
	t.Helper()
	if err := sc.WithTx(func(tx *metadata.Tx) error {
		return tx.ApplyCatalogOp(op, schemaSeq)
	}); err != nil {
		t.Fatalf("ApplyCatalogOp(seq=%d): %v", schemaSeq, err)
	}
	if err := sc.SetSchemaSeq(schemaSeq); err != nil {
		t.Fatalf("SetSchemaSeq(%d): %v", schemaSeq, err)
	}
}

func colNames(cols []Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

func sameNames(a []string, b ...string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestTableAtSeq_DropThenAddReusesOrdinal pins the layout-reconstruction
// contract for the exact shape that aliases positionally-captured
// values: DROP the trailing column, then ADD a new one that inherits
// the freed ordinal. A record stamped before the drop must decode
// under (id, keep, dead); one stamped after must see (id, keep, fresh).
func TestTableAtSeq_DropThenAddReusesOrdinal(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id INT PRIMARY KEY NOT NULL, keep TEXT, dead INT)`)
	cat, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	tab, _ := cat.Table("t")
	dead, ok := tab.Column("dead")
	if !ok {
		t.Fatal("column dead missing")
	}

	// seq 1: DROP COLUMN dead (trailing, ordinal 2).
	applyOp(t, sc, crdt.CatalogOp{Kind: crdt.OpDropColumn, TableID: tab.ID, ColumnID: dead.ID}, 1)
	// seq 2: ADD COLUMN fresh — reuses the freed ordinal 2.
	freshID := AllocColumnID()
	applyOp(t, sc, crdt.CatalogOp{
		Kind: crdt.OpAddColumn, TableID: tab.ID,
		Columns: []crdt.CatalogColumn{{ID: freshID, Name: "fresh", Ordinal: 2}},
	}, 2)
	if err := cat.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if got := cat.SchemaSeq(); got != 2 {
		t.Fatalf("SchemaSeq = %d, want 2", got)
	}

	tab, _ = cat.Table("t")
	if !sameNames(colNames(tab.Columns), "id", "keep", "fresh") {
		t.Fatalf("current layout = %v", colNames(tab.Columns))
	}

	// Capture-time layout at seq 0: the pre-drop shape, with dead's ID.
	at0, ok := cat.TableAtSeq(tab, 0)
	if !ok {
		t.Fatal("TableAtSeq(0) unreconstructable")
	}
	if !sameNames(colNames(at0.Columns), "id", "keep", "dead") {
		t.Fatalf("layout@0 = %v", colNames(at0.Columns))
	}
	if at0.Columns[2].ID != dead.ID {
		t.Error("layout@0 ordinal 2 must carry dead's ColumnID")
	}
	if len(at0.PK) != 1 || at0.PK[0].Name != "id" || at0.PK[0].PKPos != 1 {
		t.Errorf("layout@0 PK = %+v", at0.PK)
	}

	// seq 1: between drop and add — just (id, keep).
	at1, ok := cat.TableAtSeq(tab, 1)
	if !ok {
		t.Fatal("TableAtSeq(1) unreconstructable")
	}
	if !sameNames(colNames(at1.Columns), "id", "keep") {
		t.Fatalf("layout@1 = %v", colNames(at1.Columns))
	}

	// seq 2 (current) and future stamps resolve to the live table.
	if at2, ok := cat.TableAtSeq(tab, 2); !ok || at2 != tab {
		t.Error("TableAtSeq(current) must return the live table")
	}
	if at9, ok := cat.TableAtSeq(tab, 9); !ok || at9 != tab {
		t.Error("TableAtSeq(future) must return the live table")
	}
}

// TestTableAtSeq_LegacyTombstoneUnreliable: a drop recorded by the old
// upsert (name/ordinal/create_seq clobbered) makes pre-drop layouts
// unreconstructable — TableAtSeq must refuse rather than guess.
func TestTableAtSeq_LegacyTombstoneUnreliable(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id INT PRIMARY KEY NOT NULL, keep TEXT, dead INT)`)
	cat, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	tab, _ := cat.Table("t")
	dead, _ := tab.Column("dead")

	// Simulate the legacy drop shape: full upsert with cleared fields.
	if err := sc.WithTx(func(tx *metadata.Tx) error {
		return tx.UpsertColumn(metadata.ColumnEntry{
			TableID: tab.ID, ColumnID: dead.ID, Name: "",
			State: metadata.StateDropped, ClockGroup: metadata.ClockGroupRow,
			CreateSeq: 0, DropSeq: 1,
		})
	}); err != nil {
		t.Fatalf("legacy tombstone: %v", err)
	}
	if err := sc.SetSchemaSeq(1); err != nil {
		t.Fatalf("SetSchemaSeq: %v", err)
	}
	if err := cat.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	tab, _ = cat.Table("t")
	if _, ok := cat.TableAtSeq(tab, 0); ok {
		t.Error("TableAtSeq must refuse a table with a legacy (nameless) tombstone")
	}
}
