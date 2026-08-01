package crdt

// Cell-group record normalization shared by every engine's live apply
// path and by mirror-journal recovery replay (internal/nodestate) —
// one implementation so no two consumers can drift on which records
// arbitrate per column.

// CellTable is the table shape cell-group normalization consults:
// per-column roles and the active non-PK column set. Each engine's
// catalog implements it.
type CellTable interface {
	// ColumnRole classifies an active column as PK member and/or
	// declared counter. Unknown or dropped columns report both false.
	ColumnRole(ColumnID) (pk, counter bool)
	// NonPKColumns lists the table's active non-PK column IDs.
	NonPKColumns() []ColumnID
}

// AsCellUpdate normalizes a cell-group record into the Update shape
// the per-column arbitration paths consume. An Insert landing on a
// live row at the same CL is semantically an update (an UPSERT on the
// producer): its image arbitrates per column so it can't absorb
// columns it loses. A counter column's image value becomes a
// FormatDelta contribution — within a generation the cell is the sum
// of all contributions, so a concurrent same-PK insert adds its
// opening value instead of stomping increments already applied
// (CRDT.md F_counter). Returns ok=false for records that stay on the
// row-level path (Delete; Insert that bumps CL).
func AsCellUpdate(t CellTable, rec Record, rs RowState) (Update, bool) {
	switch r := rec.(type) {
	case Update:
		return r, true
	case Insert:
		if !rs.IsLive() || r.CL != rs.CL {
			return Update{}, false
		}
		changed := make([]ColValue, 0, len(r.Image))
		for _, v := range r.Image {
			pk, counter := t.ColumnRole(v.Column)
			if pk {
				continue
			}
			if counter && v.TypeTag == ColInt && len(v.Bytes) == 8 {
				v.Format = FormatDelta
			}
			changed = append(changed, v)
		}
		return Update{Table: r.Table, PK: r.PK, CL: r.CL, Changed: changed}, true
	}
	return Update{}, false
}

// CoversAllNonPK reports whether writes covers every active non-PK
// column of t. A cell-group update covering every column absorbs the
// row back into its baseline (opportunistic collapse).
func CoversAllNonPK(t CellTable, writes []ColValue) bool {
	written := make(map[ColumnID]struct{}, len(writes))
	for _, v := range writes {
		written[v.Column] = struct{}{}
	}
	for _, id := range t.NonPKColumns() {
		if _, ok := written[id]; !ok {
			return false
		}
	}
	return true
}
