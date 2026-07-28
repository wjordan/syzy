package catalog

import "github.com/wjordan/syzy/crdt"

// Cell-group record normalization shared by the live apply path
// (internal/broker) and mirror-journal recovery replay
// (internal/nodestate) — one implementation so the two sides cannot
// drift on which records arbitrate per column.

// AsCellUpdate normalizes a cell-group record into the Update shape
// the per-column arbitration paths consume. An Insert landing on a
// live row at the same CL is semantically an update (SQLite UPSERT on
// the producer): its image arbitrates per column so it can't absorb
// columns it loses. A counter column's image value becomes a
// FormatDelta contribution — within a generation the cell is the sum
// of all contributions, so a concurrent same-PK insert adds its
// opening value instead of stomping increments already applied
// (CRDT.md F_counter). Returns ok=false for records that stay on the
// row-level path (Delete; Insert that bumps CL).
func (t *Table) AsCellUpdate(rec crdt.Record, rs crdt.RowState) (crdt.Update, bool) {
	switch r := rec.(type) {
	case crdt.Update:
		return r, true
	case crdt.Insert:
		if !rs.IsLive() || r.CL != rs.CL {
			return crdt.Update{}, false
		}
		changed := make([]crdt.ColValue, 0, len(r.Image))
		for _, v := range r.Image {
			col, ok := t.ColumnByID(v.Column)
			if ok && col.PKPos > 0 {
				continue
			}
			if ok && col.Counter() && v.TypeTag == crdt.ColInt && len(v.Bytes) == 8 {
				v.Format = crdt.FormatDelta
			}
			changed = append(changed, v)
		}
		return crdt.Update{Table: r.Table, PK: r.PK, CL: r.CL, Changed: changed}, true
	}
	return crdt.Update{}, false
}

// CoversAllNonPK reports whether writes covers every active non-PK
// column of t. A cell-group update covering every column absorbs the
// row back into its baseline (opportunistic collapse).
func (t *Table) CoversAllNonPK(writes []crdt.ColValue) bool {
	written := make(map[crdt.ColumnID]struct{}, len(writes))
	for _, v := range writes {
		written[v.Column] = struct{}{}
	}
	for _, c := range t.Columns {
		if c.PKPos > 0 {
			continue
		}
		if _, ok := written[c.ID]; !ok {
			return false
		}
	}
	return true
}
