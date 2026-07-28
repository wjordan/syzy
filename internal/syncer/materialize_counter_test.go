package syncer

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
)

func colInt(id crdt.ColumnID, v int64) crdt.ColValue {
	cv := intVal(v)
	cv.Column = id
	return cv
}

func counterTestTable() (*catalog.Table, crdt.ColumnID, crdt.ColumnID) {
	idID := crdt.ColumnID{1}
	qtyID := crdt.ColumnID{2}
	noteID := crdt.ColumnID{3}
	tab := &catalog.Table{
		Name: "inv",
		ID:   crdt.TableID{9},
		Columns: []catalog.Column{
			{Name: "id", ID: idID, Ordinal: 0, PKPos: 1},
			{Name: "qty", ID: qtyID, Ordinal: 1, ClockGroup: metadata.ClockGroupCounter},
			{Name: "note", ID: noteID, Ordinal: 2},
		},
	}
	return tab, qtyID, noteID
}

// TestCounterDeltasInPlace: a counter column's changed entry becomes a
// FormatDelta of NEW − OLD (sign-preserving); register columns are left
// alone.
func TestCounterDeltasInPlace(t *testing.T) {
	t.Parallel()
	tab, qtyID, noteID := counterTestTable()
	old := []crdt.ColValue{colInt(crdt.ColumnID{1}, 7), colInt(qtyID, 100), {Column: noteID, TypeTag: crdt.ColText, Bytes: []byte("a")}}
	new := []crdt.ColValue{colInt(crdt.ColumnID{1}, 7), colInt(qtyID, 58), {Column: noteID, TypeTag: crdt.ColText, Bytes: []byte("b")}}
	changed := []crdt.ColValue{
		{Column: qtyID, TypeTag: crdt.ColInt, Bytes: new[1].Bytes},
		{Column: noteID, TypeTag: crdt.ColText, Bytes: []byte("b")},
	}
	if err := counterDeltasInPlace(tab, old, new, changed); err != nil {
		t.Fatalf("counterDeltasInPlace: %v", err)
	}
	if changed[0].Format != crdt.FormatDelta {
		t.Fatalf("qty Format = %d; want FormatDelta", changed[0].Format)
	}
	if got := int64(binary.BigEndian.Uint64(changed[0].Bytes)); got != -42 {
		t.Fatalf("qty delta = %d; want -42 (58 - 100)", got)
	}
	if changed[1].Format != crdt.FormatText || string(changed[1].Bytes) != "b" {
		t.Fatalf("note value mutated: %+v", changed[1])
	}
}

// TestCounterDeltasInPlace_NonIntegerErrors: a non-INTEGER value in a
// counter column is a hard materialize error, not a silent fallback.
func TestCounterDeltasInPlace_NonIntegerErrors(t *testing.T) {
	t.Parallel()
	tab, qtyID, _ := counterTestTable()
	old := []crdt.ColValue{colInt(crdt.ColumnID{1}, 7), {Column: qtyID, TypeTag: crdt.ColText, Bytes: []byte("oops")}, {}}
	new := []crdt.ColValue{colInt(crdt.ColumnID{1}, 7), colInt(qtyID, 58), {}}
	changed := []crdt.ColValue{{Column: qtyID, TypeTag: crdt.ColInt, Bytes: new[1].Bytes}}
	err := counterDeltasInPlace(tab, old, new, changed)
	if err == nil || !strings.Contains(err.Error(), "counter column") {
		t.Fatalf("err = %v; want counter-column type error", err)
	}
}

// TestCounterDeltasInPlace_SubtractionOverflowErrors: a wrapped NEW−OLD
// would ship an arithmetically wrong contribution to every peer.
func TestCounterDeltasInPlace_SubtractionOverflowErrors(t *testing.T) {
	t.Parallel()
	tab, qtyID, _ := counterTestTable()
	old := []crdt.ColValue{colInt(crdt.ColumnID{1}, 7), colInt(qtyID, -9223372036854775808), {}}
	new := []crdt.ColValue{colInt(crdt.ColumnID{1}, 7), colInt(qtyID, 9223372036854775807), {}}
	changed := []crdt.ColValue{{Column: qtyID, TypeTag: crdt.ColInt, Bytes: new[1].Bytes}}
	err := counterDeltasInPlace(tab, old, new, changed)
	if err == nil || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("err = %v; want delta-overflow error", err)
	}
}
