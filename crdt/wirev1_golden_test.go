package crdt

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// The wirev1_*.bin fixtures were encoded by the pre-bump (wire-v1)
// encoder — see testdata/README.md. These tests pin the legacy decode
// path: v1 payloads persisted by the production fleet (mirror
// journals, epoch history) must decode into the current structs.

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return b
}

func realBE(v float64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
	return b[:]
}

func TestDecode_WireV1_Golden_Full(t *testing.T) {
	raw := readFixture(t, "wirev1_full.bin")
	if raw[0] != wireVersionV1 {
		t.Fatalf("fixture version = %d, want %d", raw[0], wireVersionV1)
	}

	cs, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode(v1): %v", err)
	}

	if want := (Dot{Origin: 7, Seq: 42}); cs.Dot != want {
		t.Errorf("Dot = %+v, want %+v", cs.Dot, want)
	}
	wantStamp := Stamp{Clock: Clock{WallTime: 1748400000000, Logical: 5}, Origin: 7}
	if !cs.Stamp.Equal(wantStamp) {
		t.Errorf("Stamp = %+v, want %+v", cs.Stamp, wantStamp)
	}
	wantDeps := Deps{SchemaChain: 9, ChainID(3): 17}
	if !cs.Deps.Equal(wantDeps) {
		t.Errorf("Deps = %+v, want %+v", cs.Deps, wantDeps)
	}
	if want := (ClusterID{0xCC, 0xDD}); cs.ClusterID != want {
		t.Errorf("ClusterID = %x, want %x", cs.ClusterID, want)
	}
	if len(cs.Records) != 4 {
		t.Fatalf("records = %d, want 4", len(cs.Records))
	}

	tbl := TableID{0xAA, 0x01}

	ins, ok := cs.Records[0].(Insert)
	if !ok {
		t.Fatalf("record 0 = %T, want Insert", cs.Records[0])
	}
	if ins.Table != tbl || !bytes.Equal(ins.PK, PKBlob{0x10, 0x11}) || ins.CL != 1 {
		t.Errorf("Insert header = %+v", ins.Header())
	}
	wantImage := []ColValue{
		{Column: ColumnID{0x01}, TypeTag: ColInt, Bytes: int64BE(-42)},
		{Column: ColumnID{0x02}, TypeTag: ColReal, Bytes: realBE(3.5)},
		{Column: ColumnID{0x03}, TypeTag: ColText, Bytes: []byte("héllo v1")},
		{Column: ColumnID{0x04}, TypeTag: ColBlob, Bytes: []byte{0x00, 0xFF, 0x7F}},
		{Column: ColumnID{0x05}, TypeTag: ColNull}, // v1 tag 5 → TypeTag 0
	}
	assertCols(t, "Insert.Image", ins.Image, wantImage)

	upd, ok := cs.Records[1].(Update)
	if !ok {
		t.Fatalf("record 1 = %T, want Update", cs.Records[1])
	}
	if !bytes.Equal(upd.PK, PKBlob{0x20}) || upd.CL != 3 {
		t.Errorf("Update header = %+v", upd.Header())
	}
	assertCols(t, "Update.Changed", upd.Changed, []ColValue{
		{Column: ColumnID{0x03}, TypeTag: ColText, Bytes: []byte("updated")},
		{Column: ColumnID{0x01}, TypeTag: ColNull},
	})

	del, ok := cs.Records[2].(Delete)
	if !ok {
		t.Fatalf("record 2 = %T, want Delete", cs.Records[2])
	}
	if !bytes.Equal(del.PK, PKBlob{0x30}) || del.CL != 4 {
		t.Errorf("Delete header = %+v", del.Header())
	}

	bp, ok := cs.Records[3].(BlobPatch)
	if !ok {
		t.Fatalf("record 3 = %T, want BlobPatch", cs.Records[3])
	}
	if bp.Col != (ColumnID{0x04}) || bp.CL != 5 || len(bp.Ranges) != 2 {
		t.Fatalf("BlobPatch = %+v", bp)
	}
	if bp.Ranges[0].Offset != 0 || !bytes.Equal(bp.Ranges[0].Bytes, []byte{1, 2, 3}) ||
		bp.Ranges[1].Offset != 4096 || !bytes.Equal(bp.Ranges[1].Bytes, []byte{0xEE}) {
		t.Errorf("BlobPatch.Ranges = %+v", bp.Ranges)
	}
}

func TestDecode_WireV1_Golden_Empty(t *testing.T) {
	raw := readFixture(t, "wirev1_empty.bin")
	cs, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode(v1 empty): %v", err)
	}
	if want := (Dot{Origin: 1, Seq: 1}); cs.Dot != want {
		t.Errorf("Dot = %+v, want %+v", cs.Dot, want)
	}
	if len(cs.Records) != 0 || len(cs.Deps) != 0 {
		t.Errorf("want empty records/deps, got %d/%d", len(cs.Records), len(cs.Deps))
	}
}

// TestEncode_AlwaysWireV2 pins emission: decode accepts v1, but
// producers only ever emit the current version.
func TestEncode_AlwaysWireV2(t *testing.T) {
	cs, err := Build(Dot{Origin: 1, Seq: 1}, makeStamp(1, 0, 1), nil, ClusterID{}, []Record{
		Insert{Table: TableID{1}, PK: PKBlob{1}, CL: 1,
			Image: []ColValue{{Column: ColumnID{1}, TypeTag: ColInt, Bytes: int64BE(7)}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cs.Encoded()[0] != WireVersion || WireVersion != 2 {
		t.Fatalf("encoded version = %d, want 2", cs.Encoded()[0])
	}
	// And v2 round-trips.
	got, err := Decode(cs.Encoded())
	if err != nil {
		t.Fatalf("Decode(v2): %v", err)
	}
	assertCols(t, "v2 round-trip", got.Records[0].(Insert).Image,
		[]ColValue{{Column: ColumnID{1}, TypeTag: ColInt, Bytes: int64BE(7)}})
}

func assertCols(t *testing.T, what string, got, want []ColValue) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: len = %d, want %d", what, len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Column != w.Column || g.TypeTag != w.TypeTag || g.Format != w.Format ||
			!bytes.Equal(g.Bytes, w.Bytes) {
			t.Errorf("%s[%d] = {%x tag=%d fmt=%d %q}, want {%x tag=%d fmt=%d %q}",
				what, i, g.Column, g.TypeTag, g.Format, g.Bytes,
				w.Column, w.TypeTag, w.Format, w.Bytes)
		}
	}
}
