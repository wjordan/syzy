package crdt

import (
	"encoding/binary"
	"errors"
	"fmt"
)

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

// ErrCounterValue marks a value that cannot participate in counter
// summation: a counter cell that is not an 8-byte integer, or a
// difference that overflows int64. Producers surface it as a hard stop
// (a wrong delta would apply everywhere); receivers route it to
// quarantine.
var ErrCounterValue = errors.New("crdt: counter value")

// ColValueEqual reports whether two values carry the same logical
// content — the diff predicate cell-group producers use to decide
// which columns a transaction actually changed.
func ColValueEqual(a, b ColValue) bool {
	if a.TypeTag != b.TypeTag || len(a.Bytes) != len(b.Bytes) {
		return false
	}
	for i := range a.Bytes {
		if a.Bytes[i] != b.Bytes[i] {
			return false
		}
	}
	return true
}

// CounterDelta returns the FormatDelta contribution NEW − OLD for one
// counter cell: receivers sum it (CRDT.md F_counter), so concurrent
// increments merge instead of stomping each other. Both sides must be
// 8-byte ColInt values, and the subtraction is checked — a wrapped
// delta would apply as arithmetically wrong on every node, so it fails
// loudly (ErrCounterValue) rather than silently. Callers wrap the error
// with their table and column names.
func CounterDelta(old, new ColValue) (ColValue, error) {
	oldV, ok := counterInt(old)
	if !ok {
		return ColValue{}, fmt.Errorf("%w: old value is not an 8-byte integer (tag %d, %d bytes)", ErrCounterValue, old.TypeTag, len(old.Bytes))
	}
	newV, ok := counterInt(new)
	if !ok {
		return ColValue{}, fmt.Errorf("%w: new value is not an 8-byte integer (tag %d, %d bytes)", ErrCounterValue, new.TypeTag, len(new.Bytes))
	}
	delta := newV - oldV
	if (oldV > 0 && delta > newV) || (oldV < 0 && delta < newV) {
		return ColValue{}, fmt.Errorf("%w: delta overflows int64 (old %d, new %d)", ErrCounterValue, oldV, newV)
	}
	return ColValue{Column: new.Column, TypeTag: ColInt, Format: FormatDelta, Bytes: encodeInt64(delta)}, nil
}

func counterInt(v ColValue) (int64, bool) {
	if v.TypeTag != ColInt || len(v.Bytes) != 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(v.Bytes)), true
}

func encodeInt64(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
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
