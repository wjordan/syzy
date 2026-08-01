package catalog

import "github.com/wjordan/syzy/crdt"

// crdt.CellTable implementation: the shape hooks cell-group record
// normalization (crdt.AsCellUpdate / crdt.CoversAllNonPK) consults.

// ColumnRole classifies an active column as PK member and/or declared
// counter. Unknown or dropped columns report both false.
func (t *Table) ColumnRole(id crdt.ColumnID) (pk, counter bool) {
	c, ok := t.ColumnByID(id)
	if !ok {
		return false, false
	}
	return c.PKPos > 0, c.Counter()
}

// NonPKColumns lists the table's active non-PK column IDs.
func (t *Table) NonPKColumns() []crdt.ColumnID {
	out := make([]crdt.ColumnID, 0, len(t.Columns))
	for _, c := range t.Columns {
		if c.PKPos > 0 {
			continue
		}
		out = append(out, c.ID)
	}
	return out
}

// CellGroupTable resolves a table for per-column cell-clock replay:
// known, not dropped, and cell clock group. Satisfies
// nodestate.CellTables.
func (c *Catalog) CellGroupTable(id crdt.TableID) (crdt.CellTable, bool) {
	tab, ok := c.TableByID(id)
	if !ok || tab.Dropped() || !tab.CellGroup() {
		return nil, false
	}
	return tab, true
}
