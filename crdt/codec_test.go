package crdt

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func makeStamp(wall int64, log int32, origin Origin) Stamp {
	return Stamp{Clock: Clock{WallTime: wall, Logical: log}, Origin: origin}
}

func TestChangeset_RoundTrip_AllRecordTypes(t *testing.T) {
	tbl := TableID{0xAA}
	col1 := ColumnID{0x01}
	col2 := ColumnID{0x02}
	colBlob := ColumnID{0x03}
	cluster := ClusterID{0xCC}

	records := []Record{
		Insert{
			Table: tbl,
			PK:    PKBlob{0x10, 0x11},
			Image: []ColValue{
				{Column: col1, TypeTag: ColInt, Bytes: int64BE(42)},
				{Column: col2, TypeTag: ColText, Bytes: []byte("hello")},
			},
		},
		Update{
			Table:   tbl,
			PK:      PKBlob{0x20},
			Changed: []ColValue{{Column: col1, TypeTag: ColNull}},
		},
		Delete{
			Table: tbl,
			PK:    PKBlob{0x30},
		},
		BlobPatch{
			Table: tbl,
			PK:    PKBlob{0x40},
			Col:   colBlob,
			Ranges: []BlobPatchRange{
				{Offset: 0, Bytes: []byte{1, 2, 3}},
				{Offset: 100, Bytes: []byte{0xff}},
			},
		},
	}

	dot := Dot{Origin: 7, Seq: 42}
	stamp := makeStamp(1234567, 5, 7)
	deps := Deps{SchemaChain: 9}

	cs, err := Build(dot, stamp, deps, cluster, records)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if cs.Encoded() == nil {
		t.Fatal("Encoded should be non-nil after Build")
	}
	if cs.CRC() == 0 {
		t.Errorf("CRC should be non-zero for non-empty changeset")
	}

	got, err := Decode(cs.Encoded())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Dot != dot {
		t.Errorf("Dot = %+v, want %+v", got.Dot, dot)
	}
	if !got.Stamp.Equal(stamp) {
		t.Errorf("Stamp = %+v, want %+v", got.Stamp, stamp)
	}
	if !got.Deps.Equal(deps) {
		t.Errorf("Deps = %+v, want %+v", got.Deps, deps)
	}
	if got.ClusterID != cluster {
		t.Errorf("ClusterID = %x, want %x", got.ClusterID, cluster)
	}
	if len(got.Records) != len(records) {
		t.Fatalf("records len = %d, want %d", len(got.Records), len(records))
	}
	// Check round-trip for each record (deep compare via re-encoding).
	encoded2, err := encodeRecords(nil, got.Records)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	encoded1, err := encodeRecords(nil, records)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !bytes.Equal(encoded1, encoded2) {
		t.Errorf("record payloads differ after round-trip")
	}
}

func TestChangeset_EncodedIsCanonical(t *testing.T) {
	// Build → bytes A. Decode A → cs. Re-encode cs → bytes B. A == B.
	dot := Dot{Origin: 1, Seq: 1}
	stamp := makeStamp(100, 0, 1)
	cs1, err := Build(dot, stamp, Deps{SchemaChain: 0}, ClusterID{}, []Record{
		Delete{Table: TableID{0x01}, PK: PKBlob{0x99}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cs2, err := Decode(cs1.Encoded())
	if err != nil {
		t.Fatal(err)
	}
	cs3, err := Build(cs2.Dot, cs2.Stamp, cs2.Deps, cs2.ClusterID, cs2.Records)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cs1.Encoded(), cs3.Encoded()) {
		t.Errorf("re-encoded bytes differ from original")
	}
}

func TestChangeset_CRC_Detects_Corruption(t *testing.T) {
	cs, err := Build(Dot{Origin: 1, Seq: 1}, makeStamp(50, 0, 1), nil, ClusterID{}, []Record{
		Delete{Table: TableID{0x01}, PK: PKBlob{0x42}},
	})
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), cs.Encoded()...)
	// Flip a bit deep in the payload.
	corrupt[len(corrupt)-1] ^= 0x80
	if _, err := Decode(corrupt); err != ErrCRCMismatch {
		t.Errorf("Decode(corrupt) = %v, want ErrCRCMismatch", err)
	}
}

func TestChangeset_OriginMismatch(t *testing.T) {
	if _, err := Build(
		Dot{Origin: 1, Seq: 1},
		makeStamp(0, 0, 2), // origin != Dot.Origin
		nil, ClusterID{}, nil,
	); err != ErrOriginMismatch {
		t.Errorf("Build with origin mismatch = %v, want ErrOriginMismatch", err)
	}
}

func TestChangeset_UnknownVersion(t *testing.T) {
	cs, err := Build(Dot{Origin: 1, Seq: 1}, makeStamp(0, 0, 1), nil, ClusterID{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), cs.Encoded()...)
	bad[0] = 0xFF
	if _, err := Decode(bad); err != ErrUnknownVersion {
		t.Errorf("Decode(bad version) = %v, want ErrUnknownVersion", err)
	}
}

func TestChangeset_EmptyRecords(t *testing.T) {
	cs, err := Build(Dot{Origin: 1, Seq: 1}, makeStamp(0, 0, 1), nil, ClusterID{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(cs.Encoded())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 0 {
		t.Errorf("expected zero records, got %d", len(got.Records))
	}
}

// int64BE is a small helper for tests: encodes an int64 in BE form.
func int64BE(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}
