package syncer

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestParseJournalBlobIntent verifies the SYZY_OP_BLOB_INTENT decoder.
// Hand-crafts the C-side wire format and round-trips through parseJournal.
func TestParseJournalBlobIntent(t *testing.T) {
	const dbName = "main"
	const tblName = "blobrow"
	const colName = "body"
	const rowid int64 = 0x0102030405060708
	const offset uint64 = 0xcafebabe
	const length uint32 = 12345

	var buf bytes.Buffer
	buf.WriteByte(syzyBlobIntent)
	var i64 [8]byte
	binary.BigEndian.PutUint64(i64[:], uint64(rowid))
	buf.Write(i64[:])
	writeShortBytes(&buf, []byte(dbName))
	writeShortBytes(&buf, []byte(tblName))
	writeShortBytes(&buf, []byte(colName))
	binary.BigEndian.PutUint64(i64[:], offset)
	buf.Write(i64[:])
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], length)
	buf.Write(u32[:])

	recs, err := parseJournal(buf.Bytes(), nil)
	if err != nil {
		t.Fatalf("parseJournal: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("len(recs)=%d want 1", len(recs))
	}
	r := recs[0]
	if r.Op != syzyBlobIntent {
		t.Errorf("Op=%d want %d", r.Op, syzyBlobIntent)
	}
	if r.OldRowID != rowid {
		t.Errorf("OldRowID=%x want %x", r.OldRowID, rowid)
	}
	if string(r.DBName) != dbName {
		t.Errorf("DBName=%q want %q", r.DBName, dbName)
	}
	if string(r.Table) != tblName {
		t.Errorf("Table=%q want %q", r.Table, tblName)
	}
	if string(r.BlobColName) != colName {
		t.Errorf("BlobColName=%q want %q", r.BlobColName, colName)
	}
	if r.BlobOffset != offset {
		t.Errorf("BlobOffset=%x want %x", r.BlobOffset, offset)
	}
	if r.BlobLen != length {
		t.Errorf("BlobLen=%d want %d", r.BlobLen, length)
	}
	if len(r.Values) != 0 || len(r.NewValues) != 0 {
		t.Errorf("intent records carry no values: got %d/%d", len(r.Values), len(r.NewValues))
	}
}

// TestCoalesceIntentRanges checks that overlapping/adjacent ranges are
// merged and disjoint ranges are kept distinct, sorted by offset.
func TestCoalesceIntentRanges(t *testing.T) {
	cases := []struct {
		name string
		in   []intentRange
		want []intentRange
	}{
		{"empty", nil, nil},
		{"single", []intentRange{{10, 20}}, []intentRange{{10, 20}}},
		{
			"overlap",
			[]intentRange{{10, 20}, {15, 25}},
			[]intentRange{{10, 25}},
		},
		{
			"adjacent",
			[]intentRange{{10, 20}, {20, 30}},
			[]intentRange{{10, 30}},
		},
		{
			"disjoint",
			[]intentRange{{30, 40}, {10, 20}},
			[]intentRange{{10, 20}, {30, 40}},
		},
		{
			"chain",
			[]intentRange{{0, 10}, {5, 15}, {15, 20}, {30, 40}},
			[]intentRange{{0, 20}, {30, 40}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := coalesceIntentRanges(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("len=%d want %d (got=%v)", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("range[%d]=%v want %v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func writeShortBytes(buf *bytes.Buffer, b []byte) {
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(b)))
	buf.Write(n[:])
	buf.Write(b)
}
