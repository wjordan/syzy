package catalog

import (
	"sync"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// A published *Table must stay immutable for its whole life: readers get
// the pointer from Table()/TableByID() and then read its Columns without
// holding c.mu, so any in-place edit races every one of them. This is
// the regression guard for that — it fails under -race if
// refreshPKDefaultsFromBuilt goes back to patching live tables.
func TestRefreshPKDefaults_DoesNotMutatePublishedTables(t *testing.T) {
	tableID := crdt.TableID{7}
	cols := []Column{
		{Name: "id", ID: crdt.ColumnID{1}, PKPos: 1},
		{Name: "body", ID: crdt.ColumnID{2}},
	}
	live := &Table{Name: "notes", ID: tableID, Columns: cols, PK: cols[:1]}
	c := &Catalog{
		byName: map[string]*Table{"notes": live},
		byID:   map[crdt.TableID]*Table{tableID: live},
	}

	// What the app-side build would produce: same columns, now annotated.
	built := map[string]*Table{"notes": {
		Name: "notes", ID: tableID,
		Columns: []Column{
			{Name: "id", ID: crdt.ColumnID{1}, PKPos: 1, PKDefault: PKDefault{Kind: PKDefaultUUIDv7}},
			{Name: "body", ID: crdt.ColumnID{2}},
		},
	}}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Reader: exactly the shape of the DDL-admission path — take the
	// pointer under the lock, then read columns off it without one.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			tbl, ok := c.Table("notes")
			if !ok {
				continue
			}
			if col, ok := tbl.Column("id"); ok && col.Name != "id" {
				panic("column name and payload torn apart")
			}
			tbl.ColumnByID(crdt.ColumnID{2})
		}
	}()

	for i := 0; i < 200; i++ {
		c.refreshPKDefaultsFromBuilt(built)
	}
	close(stop)
	wg.Wait()

	// The refresh must still take effect, via a replacement table.
	got, ok := c.Table("notes")
	if !ok {
		t.Fatal("notes vanished from the catalog")
	}
	if col, _ := got.Column("id"); col.PKDefault.Kind != PKDefaultUUIDv7 {
		t.Errorf("PKDefault not refreshed: got %v", col.PKDefault.Kind)
	}
	// byName and byID must not have forked onto different tables.
	byID, ok := c.TableByID(tableID)
	if !ok {
		t.Fatal("notes vanished from byID")
	}
	if byID != got {
		t.Error("byName and byID hold different *Table values for one table")
	}
	// The originally-published table is untouched.
	if col, _ := live.Column("id"); col.PKDefault.Kind != PKDefaultNone {
		t.Error("the published table was mutated in place")
	}
}
