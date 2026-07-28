package catalog

import (
	"fmt"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// TableResolver looks up catalog tables by id. Both *Catalog and
// *Overlay satisfy it, which lets the schema-apply paths resolve
// against a set of ops that have not been committed to the catalog yet.
type TableResolver interface {
	TableByID(id crdt.TableID) (*Table, bool)
}

// Overlay answers table lookups as if a sequence of CatalogOps had
// already been applied on top of a Catalog, without touching the
// Catalog itself.
//
// DDL admission needs this inside an explicit transaction: the ops for
// a multi-statement migration are built at each statement's trace_v2,
// but the catalog only learns about them when the transaction commits
// and the intent resolves. Without an overlay the second statement of
// `CREATE TABLE t; CREATE INDEX ON t(v)` cannot resolve `t`.
//
// The overlay is copy-on-write: base tables are cloned before being
// mutated, so an abandoned (rolled back) transaction leaves no trace.
// Not safe for concurrent use; admission is serialized by the writer
// connection's single-writer contract.
type Overlay struct {
	base *Catalog
	// pending holds every table the op sequence created or modified,
	// keyed by id. Dropped tables stay in the map so later ops can still
	// resolve them by id.
	pending map[crdt.TableID]*Table
	// shadowed marks names the op sequence removed — dropped, or renamed
	// away — so a lookup does not fall through to a stale base entry.
	shadowed map[string]bool
}

// NewOverlay returns an empty overlay over base. An empty overlay
// resolves exactly like base.
func NewOverlay(base *Catalog) *Overlay {
	return &Overlay{
		base:     base,
		pending:  map[crdt.TableID]*Table{},
		shadowed: map[string]bool{},
	}
}

// Table resolves an active table by name, preferring the overlay.
func (o *Overlay) Table(name string) (*Table, bool) {
	for _, t := range o.pending {
		if t.Name == name && !t.dropped {
			return t, true
		}
	}
	if o.shadowed[name] {
		return nil, false
	}
	return o.base.Table(name)
}

// TableByID resolves a table by id — active or dropped — preferring the
// overlay.
func (o *Overlay) TableByID(id crdt.TableID) (*Table, bool) {
	if t, ok := o.pending[id]; ok {
		return t, true
	}
	return o.base.TableByID(id)
}

// mutable returns the overlay's copy of the table op targets, cloning it
// out of the base catalog on first touch.
func (o *Overlay) mutable(id crdt.TableID) (*Table, error) {
	if t, ok := o.pending[id]; ok {
		return t, nil
	}
	base, ok := o.base.TableByID(id)
	if !ok {
		return nil, fmt.Errorf("catalog overlay: unknown table id %x", id[:])
	}
	t := base.clone()
	o.pending[id] = t
	return t, nil
}

// Apply folds one CatalogOp into the overlay. Ops with no typed catalog
// effect (indexes, views, virtual tables, triggers) are accepted and
// ignored, mirroring metadata.Tx.ApplyCatalogOp.
func (o *Overlay) Apply(op crdt.CatalogOp) error {
	switch op.Kind {
	case crdt.OpBundle:
		for _, sub := range op.SubOps {
			if err := o.Apply(sub); err != nil {
				return err
			}
		}
		return nil

	case crdt.OpCreateTable:
		t := tableFromCreateOp(op)
		o.pending[op.TableID] = t
		delete(o.shadowed, t.Name)
		return nil

	case crdt.OpDropTable:
		t, err := o.mutable(op.TableID)
		if err != nil {
			return err
		}
		t.dropped = true
		o.shadowed[t.Name] = true
		return nil

	case crdt.OpRenameTable:
		t, err := o.mutable(op.TableID)
		if err != nil {
			return err
		}
		o.shadowed[t.Name] = true
		t.Name = op.TableName
		delete(o.shadowed, t.Name)
		return nil

	case crdt.OpAddColumn:
		t, err := o.mutable(op.TableID)
		if err != nil {
			return err
		}
		for _, c := range op.Columns {
			col := columnFromOp(c)
			t.Columns = append(t.Columns, col)
			t.allColumns = append(t.allColumns, col)
			if col.Counter() {
				t.hasCounters = true
			}
		}
		sortColumnsByOrdinal(t.Columns)
		sortColumnsByOrdinalThenSeq(t.allColumns)
		t.PK = pkColumns(t.Columns)
		return nil

	case crdt.OpDropColumn:
		t, err := o.mutable(op.TableID)
		if err != nil {
			return err
		}
		for i, c := range t.Columns {
			if c.ID == op.ColumnID {
				t.Columns = append(t.Columns[:i], t.Columns[i+1:]...)
				break
			}
		}
		t.hasCounters = hasCounterColumns(t.Columns)
		t.PK = pkColumns(t.Columns)
		return nil

	case crdt.OpRenameColumn:
		t, err := o.mutable(op.TableID)
		if err != nil {
			return err
		}
		renameColumnByID(t.Columns, op.ColumnID, op.ColumnName)
		renameColumnByID(t.allColumns, op.ColumnID, op.ColumnName)
		t.PK = pkColumns(t.Columns)
		return nil

	case crdt.OpAddUniqueKey:
		t, err := o.mutable(op.TableID)
		if err != nil {
			return err
		}
		for _, k := range op.Keys {
			uk := UniqueKey{KeyID: k.KeyID, Coordinated: k.Coordinated, Predicate: k.Predicate}
			for _, m := range k.Members {
				if col, ok := t.ColumnByID(m.ColumnID); ok {
					uk.Columns = append(uk.Columns, col)
				}
			}
			t.UniqueKeys = append(t.UniqueKeys, uk)
		}
		return nil

	case crdt.OpDropUniqueKey:
		t, err := o.mutable(op.TableID)
		if err != nil {
			return err
		}
		for i, k := range t.UniqueKeys {
			if k.KeyID == op.KeyID {
				t.UniqueKeys = append(t.UniqueKeys[:i], t.UniqueKeys[i+1:]...)
				break
			}
		}
		return nil

	case crdt.OpSetClockGroup:
		t, err := o.mutable(op.TableID)
		if err != nil {
			return err
		}
		t.clockGroup = op.ClockGroup
		return nil

	case crdt.OpCreateIndex, crdt.OpDropIndex,
		crdt.OpCreateView, crdt.OpDropView,
		crdt.OpCreateVirtualTable, crdt.OpDropVirtualTable,
		crdt.OpCreateTrigger, crdt.OpDropTrigger:
		// Opaque-SQL forms carry no typed catalog rows.
		return nil
	}
	return fmt.Errorf("catalog overlay: unsupported op kind %v", op.Kind)
}

// tableFromCreateOp builds the in-memory table an OpCreateTable
// produces, mirroring metadata's catApplyCreateTable so the overlay and
// the committed catalog agree.
func tableFromCreateOp(op crdt.CatalogOp) *Table {
	t := &Table{
		Name:            op.TableName,
		ID:              op.TableID,
		clockGroup:      metadata.ClockGroupRow,
		historyReliable: true,
	}
	for _, c := range op.Columns {
		col := columnFromOp(c)
		if col.Counter() {
			// A counter column's contributions are per-column payloads, so
			// the table must arbitrate per cell.
			t.clockGroup = metadata.ClockGroupCell
			t.hasCounters = true
		}
		t.Columns = append(t.Columns, col)
		t.allColumns = append(t.allColumns, col)
	}
	for _, k := range op.Keys {
		if k.KeyID == metadata.PKKeyID {
			for i, m := range k.Members {
				ord := m.Ordinal
				if ord == 0 && i > 0 {
					ord = i
				}
				for j := range t.Columns {
					if t.Columns[j].ID == m.ColumnID {
						t.Columns[j].PKPos = ord + 1
					}
				}
			}
			continue
		}
		uk := UniqueKey{KeyID: k.KeyID, Coordinated: k.Coordinated, Predicate: k.Predicate}
		for _, m := range k.Members {
			for _, col := range t.Columns {
				if col.ID == m.ColumnID {
					uk.Columns = append(uk.Columns, col)
				}
			}
		}
		t.UniqueKeys = append(t.UniqueKeys, uk)
	}
	sortColumnsByOrdinal(t.Columns)
	sortColumnsByOrdinalThenSeq(t.allColumns)
	t.PK = pkColumns(t.Columns)
	return t
}

func columnFromOp(c crdt.CatalogColumn) Column {
	group := c.ClockGroup
	if group == "" {
		group = metadata.ClockGroupRow
	}
	return Column{
		Name:       c.Name,
		ID:         c.ID,
		Ordinal:    c.Ordinal,
		Collation:  c.Collation,
		ClockGroup: group,
	}
}

func renameColumnByID(cols []Column, id crdt.ColumnID, name string) {
	for i := range cols {
		if cols[i].ID == id {
			cols[i].Name = name
		}
	}
}

// clone deep-copies the slices an overlay mutates so the base catalog's
// table is never modified in place.
func (t *Table) clone() *Table {
	cp := *t
	cp.Columns = append([]Column(nil), t.Columns...)
	cp.PK = append([]Column(nil), t.PK...)
	cp.allColumns = append([]Column(nil), t.allColumns...)
	cp.UniqueKeys = append([]UniqueKey(nil), t.UniqueKeys...)
	return &cp
}
