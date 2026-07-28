package catalog

import (
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// createOp builds a minimal OpCreateTable for a table with a BLOB PK
// named id plus the given extra text columns.
func createOp(name string, extra ...string) (crdt.CatalogOp, crdt.TableID, crdt.ColumnID) {
	tabID := crdt.TableID{1}
	copy(tabID[:], []byte(name+"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
	pk := crdt.ColumnID{0xAA}
	op := crdt.CatalogOp{
		Kind: crdt.OpCreateTable, TableID: tabID, TableName: name,
		Columns: []crdt.CatalogColumn{{
			ID: pk, Name: "id", Ordinal: 0, Type: "BLOB",
			NotNull: true, IsPK: true, PKPos: 1, ClockGroup: metadata.ClockGroupRow,
		}},
		Keys: []crdt.CatalogKey{{
			KeyID: metadata.PKKeyID, Members: []crdt.CatalogKeyMember{{ColumnID: pk}},
		}},
	}
	for i, c := range extra {
		op.Columns = append(op.Columns, crdt.CatalogColumn{
			ID: crdt.ColumnID{byte(0xB0 + i)}, Name: c, Ordinal: i + 1,
			Type: "TEXT", ClockGroup: metadata.ClockGroupRow,
		})
	}
	return op, tabID, pk
}

func TestOverlay_EmptyDelegatesToBase(t *testing.T) {
	o := NewOverlay(&Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}})
	if _, ok := o.Table("nope"); ok {
		t.Error("resolved a table that does not exist")
	}
}

func TestOverlay_CreateThenResolve(t *testing.T) {
	base := &Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}}
	o := NewOverlay(base)
	op, tabID, pk := createOp("t", "v")
	if err := o.Apply(op); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	tab, ok := o.Table("t")
	if !ok {
		t.Fatal("created table not resolvable by name")
	}
	if tab.ID != tabID {
		t.Errorf("table id = %x, want %x", tab.ID[:4], tabID[:4])
	}
	if len(tab.Columns) != 2 {
		t.Fatalf("table has %d columns, want 2", len(tab.Columns))
	}
	if len(tab.PK) != 1 || tab.PK[0].ID != pk {
		t.Errorf("PK not derived from the create op: %+v", tab.PK)
	}
	if _, ok := o.TableByID(tabID); !ok {
		t.Error("created table not resolvable by id")
	}
	// The base catalog must be untouched.
	if _, ok := base.Table("t"); ok {
		t.Error("overlay leaked its table into the base catalog")
	}
}

func TestOverlay_AddAndDropColumn(t *testing.T) {
	o := NewOverlay(&Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}})
	op, tabID, _ := createOp("t", "v")
	if err := o.Apply(op); err != nil {
		t.Fatal(err)
	}
	newCol := crdt.ColumnID{0xC1}
	if err := o.Apply(crdt.CatalogOp{
		Kind: crdt.OpAddColumn, TableID: tabID,
		Columns: []crdt.CatalogColumn{{ID: newCol, Name: "extra", Ordinal: 2, Type: "TEXT"}},
	}); err != nil {
		t.Fatal(err)
	}
	tab, _ := o.Table("t")
	if _, ok := tab.Column("extra"); !ok {
		t.Fatal("added column not visible")
	}
	if err := o.Apply(crdt.CatalogOp{
		Kind: crdt.OpDropColumn, TableID: tabID, ColumnID: newCol,
	}); err != nil {
		t.Fatal(err)
	}
	tab, _ = o.Table("t")
	if _, ok := tab.Column("extra"); ok {
		t.Error("dropped column still visible")
	}
}

// A rename must expose the new name and stop resolving the old one, even
// when the old name still exists in the base catalog.
func TestOverlay_RenameShadowsOldName(t *testing.T) {
	base := &Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}}
	op, tabID, _ := createOp("t", "v")
	seed := tableFromCreateOp(op)
	base.byName["t"] = seed
	base.byID[tabID] = seed

	o := NewOverlay(base)
	if err := o.Apply(crdt.CatalogOp{
		Kind: crdt.OpRenameTable, TableID: tabID, TableName: "t2",
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := o.Table("t2"); !ok {
		t.Error("new name not resolvable after rename")
	}
	if _, ok := o.Table("t"); ok {
		t.Error("old name still resolves after rename")
	}
	// Base is untouched — a rolled-back transaction leaves no trace.
	if tab, ok := base.Table("t"); !ok || tab.Name != "t" {
		t.Error("rename mutated the base catalog")
	}
}

func TestOverlay_DropShadowsBaseTable(t *testing.T) {
	base := &Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}}
	op, tabID, _ := createOp("t")
	seed := tableFromCreateOp(op)
	base.byName["t"] = seed
	base.byID[tabID] = seed

	o := NewOverlay(base)
	if err := o.Apply(crdt.CatalogOp{Kind: crdt.OpDropTable, TableID: tabID}); err != nil {
		t.Fatal(err)
	}
	if _, ok := o.Table("t"); ok {
		t.Error("dropped table still resolves by name")
	}
	if _, ok := o.TableByID(tabID); !ok {
		t.Error("dropped table must stay resolvable by id")
	}
	if _, ok := base.Table("t"); !ok {
		t.Error("drop mutated the base catalog")
	}
}

// A create followed by a drop of the same table inside one op sequence.
func TestOverlay_CreateThenDrop(t *testing.T) {
	o := NewOverlay(&Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}})
	op, tabID, _ := createOp("tmp")
	if err := o.Apply(op); err != nil {
		t.Fatal(err)
	}
	if err := o.Apply(crdt.CatalogOp{Kind: crdt.OpDropTable, TableID: tabID}); err != nil {
		t.Fatal(err)
	}
	if _, ok := o.Table("tmp"); ok {
		t.Error("table dropped in the same sequence still resolves")
	}
}

func TestOverlay_UniqueKeys(t *testing.T) {
	o := NewOverlay(&Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}})
	op, tabID, _ := createOp("t", "v")
	if err := o.Apply(op); err != nil {
		t.Fatal(err)
	}
	tab, _ := o.Table("t")
	vCol, _ := tab.Column("v")
	keyID := crdt.KeyID{0xD1}
	if err := o.Apply(crdt.CatalogOp{
		Kind: crdt.OpAddUniqueKey, TableID: tabID, KeyID: keyID,
		Keys: []crdt.CatalogKey{{
			KeyID: keyID, Coordinated: true,
			Members: []crdt.CatalogKeyMember{{ColumnID: vCol.ID}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	tab, _ = o.Table("t")
	if len(tab.UniqueKeys) != 1 {
		t.Fatalf("table has %d unique keys, want 1", len(tab.UniqueKeys))
	}
	if !tab.UniqueKeys[0].Coordinated {
		t.Error("Coordinated flag lost")
	}
	if err := o.Apply(crdt.CatalogOp{
		Kind: crdt.OpDropUniqueKey, TableID: tabID, KeyID: keyID,
	}); err != nil {
		t.Fatal(err)
	}
	tab, _ = o.Table("t")
	if len(tab.UniqueKeys) != 0 {
		t.Errorf("table has %d unique keys after drop, want 0", len(tab.UniqueKeys))
	}
}

// A counter column makes the table cell-grouped, mirroring what the
// metadata apply path derives.
func TestOverlay_CounterColumnSetsCellGroup(t *testing.T) {
	o := NewOverlay(&Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}})
	op, _, _ := createOp("inv")
	op.Columns = append(op.Columns, crdt.CatalogColumn{
		ID: crdt.ColumnID{0xE1}, Name: "qty", Ordinal: 1,
		Type: "INTEGER", ClockGroup: metadata.ClockGroupCounter,
	})
	if err := o.Apply(op); err != nil {
		t.Fatal(err)
	}
	tab, _ := o.Table("inv")
	if !tab.CellGroup() {
		t.Error("counter column did not make the table cell-grouped")
	}
	if !tab.HasCounters() {
		t.Error("HasCounters not set")
	}
}

func TestOverlay_BundleAndOpaqueOps(t *testing.T) {
	o := NewOverlay(&Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}})
	op, _, _ := createOp("t", "v")
	if err := o.Apply(crdt.CatalogOp{Kind: crdt.OpBundle, SubOps: []crdt.CatalogOp{
		op,
		{Kind: crdt.OpCreateIndex, ObjectName: "i", RawSQL: "CREATE INDEX i ON t(v)"},
	}}); err != nil {
		t.Fatalf("Apply bundle: %v", err)
	}
	if _, ok := o.Table("t"); !ok {
		t.Error("bundle's create did not land")
	}
}

func TestOverlay_UnknownTableIsAnError(t *testing.T) {
	o := NewOverlay(&Catalog{byName: map[string]*Table{}, byID: map[crdt.TableID]*Table{}})
	err := o.Apply(crdt.CatalogOp{Kind: crdt.OpDropTable, TableID: crdt.TableID{9}})
	if err == nil {
		t.Error("dropping an unknown table id silently succeeded")
	}
}
