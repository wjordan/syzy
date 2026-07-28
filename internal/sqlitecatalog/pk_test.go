package catalog

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

func TestEncodeDecodePKSingleColumn(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, n TEXT)`)
	cat, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	tab, _ := cat.Table("t")

	idCol := tab.PK[0].ID
	want := map[crdt.ColumnID]crdt.ColValue{
		idCol: {Column: idCol, TypeTag: crdt.ColBlob, Bytes: []byte{0xab, 0xcd}},
	}
	blob, err := tab.EncodePK(want)
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	got, err := tab.DecodePK(blob)
	if err != nil {
		t.Fatalf("DecodePK: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip = %v; want %v", got, want)
	}
}

func TestEncodePKMultiColumn(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE pair (a INT NOT NULL, b INT NOT NULL, c TEXT, PRIMARY KEY (b, a))`)
	cat, _ := SeedFromSchema(app, sc)
	pair, _ := cat.Table("pair")
	aCol, _ := pair.Column("a")
	bCol, _ := pair.Column("b")

	vals := map[crdt.ColumnID]crdt.ColValue{
		aCol.ID: {Column: aCol.ID, TypeTag: crdt.ColInt, Bytes: []byte{0, 0, 0, 0, 0, 0, 0, 7}},
		bCol.ID: {Column: bCol.ID, TypeTag: crdt.ColInt, Bytes: []byte{0, 0, 0, 0, 0, 0, 0, 3}},
	}
	blob, err := pair.EncodePK(vals)
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	// PK order is (b, a) per DDL — verify b's column_id appears first.
	bID := bCol.ID
	if !bytes.Equal(blob[:16], bID[:]) {
		t.Errorf("first 16 bytes (%x) != b.ID (%x)", blob[:16], bID[:])
	}
	got, _ := pair.DecodePK(blob)
	if got[aCol.ID].Bytes[7] != 7 || got[bCol.ID].Bytes[7] != 3 {
		t.Errorf("decoded values = a=%v b=%v; want a=7 b=3", got[aCol.ID].Bytes, got[bCol.ID].Bytes)
	}
}

func TestEncodePKRejectsNullAndMissing(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`)
	cat, _ := SeedFromSchema(app, sc)
	tab, _ := cat.Table("t")
	idCol := tab.PK[0].ID

	if _, err := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{}); err == nil {
		t.Error("expected error for missing PK; got nil")
	}
	if _, err := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: {Column: idCol, TypeTag: crdt.ColNull}}); err == nil {
		t.Error("expected error for NULL PK; got nil")
	}
}

// TestEncodePKFromSliceShortRecord pins schema-evolution tolerance: a value
// slice written before an ADD COLUMN (covering only the original columns) must
// still PK-encode, since the PK columns precede the added one. Without this, a
// pre-migration journal record kills the secondary drainer at materialize time.
func TestEncodePKFromSliceShortRecord(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id INT PRIMARY KEY NOT NULL, a TEXT, b TEXT, c INT NOT NULL DEFAULT 0)`)
	cat, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	tab, _ := cat.Table("t")
	idCol, _ := tab.Column("id")

	// 3-value slice (id, a, b) — simulates a record from before column c
	// was added. Position-keyed, in t.Columns order.
	short := []crdt.ColValue{
		{Column: idCol.ID, TypeTag: crdt.ColInt, Bytes: []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		{TypeTag: crdt.ColText, Bytes: []byte("x")},
		{TypeTag: crdt.ColText, Bytes: []byte("y")},
	}
	pk, err := tab.EncodePKFromSlice(nil, short)
	if err != nil {
		t.Fatalf("short slice with PK present must encode: %v", err)
	}
	// Must match the encoding of the full 4-column slice — the PK (id) is
	// unaffected by the trailing added column.
	full := append(append([]crdt.ColValue{}, short...),
		crdt.ColValue{TypeTag: crdt.ColInt, Bytes: []byte{0, 0, 0, 0, 0, 0, 0, 0}})
	pkFull, err := tab.EncodePKFromSlice(nil, full)
	if err != nil {
		t.Fatalf("full slice: %v", err)
	}
	if !bytes.Equal(pk, pkFull) {
		t.Fatalf("short-slice PK %x != full-slice PK %x", pk, pkFull)
	}
}

// TestEncodePKFromSlicePKBeyondSlice guards the remaining error: a PK column
// that genuinely falls outside the slice is still rejected (the slice is too
// short to contain a value SQLite never defaults — a PK column).
func TestEncodePKFromSlicePKBeyondSlice(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (a TEXT, id INT NOT NULL, PRIMARY KEY (id))`)
	cat, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	tab, _ := cat.Table("t")

	// 1-value slice (only column a) — id is at ordinal 1, beyond the slice.
	short := []crdt.ColValue{{TypeTag: crdt.ColText, Bytes: []byte("x")}}
	if _, err := tab.EncodePKFromSlice(nil, short); err == nil {
		t.Fatal("expected error: PK column beyond the value slice")
	}
}

func TestDecodePKRejectsTruncated(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`)
	cat, _ := SeedFromSchema(app, sc)
	tab, _ := cat.Table("t")
	if _, err := tab.DecodePK([]byte{1, 2, 3}); err == nil {
		t.Error("expected error for truncated blob; got nil")
	}
}
