package syncer

import (
	"bytes"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

func TestPayloadRoundTrip(t *testing.T) {
	tab1 := crdt.TableID{0xa1, 0xa2, 0xa3}
	tab2 := crdt.TableID{0xb1, 0xb2}
	col1 := crdt.ColumnID{0xc1, 0xc2}
	col2 := crdt.ColumnID{0xc3, 0xc4}

	hdr := payloadHeader{
		hlc:        0x1122334455667788,
		dotOrigin:  77,
		schemaSeq:  3,
		recordsLen: 4,
	}
	in := []recordEvidence{
		{
			op:      evOpInsert,
			tableID: tab1,
			newPK:   crdt.PKBlob{0x01, 0x02, 0x03},
			image: []crdt.ColValue{
				{Column: col1, TypeTag: crdt.ColInt, Bytes: []byte{0, 0, 0, 0, 0, 0, 0, 9}},
				{Column: col2, TypeTag: crdt.ColText, Bytes: []byte("hello")},
			},
		},
		{
			op:      evOpUpdate,
			tableID: tab2,
			newPK:   crdt.PKBlob{0xff},
			changed: []crdt.ColValue{
				{Column: col2, TypeTag: crdt.ColBlob, Bytes: []byte{0xde, 0xad, 0xbe, 0xef}},
			},
		},
		{
			op:      evOpDelete,
			tableID: tab1,
			oldPK:   crdt.PKBlob{0x10},
		},
		{
			op:      evOpUpdatePKChange,
			tableID: tab1,
			oldPK:   crdt.PKBlob{0x20},
			newPK:   crdt.PKBlob{0x21},
			image: []crdt.ColValue{
				{Column: col1, TypeTag: crdt.ColNull, Bytes: nil},
				{Column: col2, TypeTag: crdt.ColReal, Bytes: []byte{0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18}},
			},
		},
	}

	buf, err := encodePayload(nil, hdr, in)
	if err != nil {
		t.Fatalf("encodePayload: %v", err)
	}

	gotHdr, gotRecs, err := decodePayload(buf)
	if err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	if gotHdr.hlc != hdr.hlc {
		t.Errorf("hlc=%x want %x", gotHdr.hlc, hdr.hlc)
	}
	if gotHdr.dotOrigin != hdr.dotOrigin {
		t.Errorf("dotOrigin=%d want %d", gotHdr.dotOrigin, hdr.dotOrigin)
	}
	if gotHdr.schemaSeq != hdr.schemaSeq {
		t.Errorf("schemaSeq=%d want %d", gotHdr.schemaSeq, hdr.schemaSeq)
	}
	if len(gotRecs) != len(in) {
		t.Fatalf("records=%d want %d", len(gotRecs), len(in))
	}
	for i, want := range in {
		got := gotRecs[i]
		if got.op != want.op || got.tableID != want.tableID {
			t.Errorf("rec[%d] op/table mismatch: got (%d,%x) want (%d,%x)",
				i, got.op, got.tableID, want.op, want.tableID)
		}
		if !bytes.Equal(got.oldPK, want.oldPK) {
			t.Errorf("rec[%d] oldPK got %x want %x", i, got.oldPK, want.oldPK)
		}
		if !bytes.Equal(got.newPK, want.newPK) {
			t.Errorf("rec[%d] newPK got %x want %x", i, got.newPK, want.newPK)
		}
		if !colsEq(got.image, want.image) {
			t.Errorf("rec[%d] image mismatch", i)
		}
		if !colsEq(got.changed, want.changed) {
			t.Errorf("rec[%d] changed mismatch", i)
		}
	}
}

func TestPayloadBlobPatchRoundTrip(t *testing.T) {
	tab := crdt.TableID{0xa1, 0xa2}
	col := crdt.ColumnID{0xc1, 0xc2}
	hdr := payloadHeader{recordsLen: 1}
	in := []recordEvidence{
		{
			op:      evOpBlobPatch,
			tableID: tab,
			newPK:   crdt.PKBlob{0x07, 0x08},
			blobCol: col,
			blobRanges: []crdt.BlobPatchRange{
				{Offset: 0, Bytes: []byte{0x11, 0x22, 0x33}},
				{Offset: 100, Bytes: []byte{0xfe, 0xff}},
			},
		},
	}

	buf, err := encodePayload(nil, hdr, in)
	if err != nil {
		t.Fatalf("encodePayload: %v", err)
	}
	_, gotRecs, err := decodePayload(buf)
	if err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	if len(gotRecs) != 1 {
		t.Fatalf("records=%d want 1", len(gotRecs))
	}
	got := gotRecs[0]
	if got.op != evOpBlobPatch || got.tableID != tab || got.blobCol != col {
		t.Fatalf("header mismatch: op=%d tab=%x col=%x", got.op, got.tableID, got.blobCol)
	}
	if !bytes.Equal(got.newPK, in[0].newPK) {
		t.Fatalf("pk mismatch: got %x want %x", got.newPK, in[0].newPK)
	}
	if len(got.blobRanges) != len(in[0].blobRanges) {
		t.Fatalf("ranges=%d want %d", len(got.blobRanges), len(in[0].blobRanges))
	}
	for i, want := range in[0].blobRanges {
		gr := got.blobRanges[i]
		if gr.Offset != want.Offset || !bytes.Equal(gr.Bytes, want.Bytes) {
			t.Errorf("range[%d] = (%d, %x); want (%d, %x)",
				i, gr.Offset, gr.Bytes, want.Offset, want.Bytes)
		}
	}
}

func TestPayloadRejectsBadVersion(t *testing.T) {
	buf := make([]byte, payloadHeaderLen)
	buf[0] = 0xff // version
	if _, _, err := decodePayload(buf); err == nil {
		t.Error("decodePayload accepted bad version")
	}
}

func TestPayloadRejectsTrailingBytes(t *testing.T) {
	hdr := payloadHeader{recordsLen: 0}
	buf, err := encodePayload(nil, hdr, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	buf = append(buf, 0x00)
	if _, _, err := decodePayload(buf); err == nil {
		t.Error("decodePayload accepted trailing bytes")
	}
}

func colsEq(a, b []crdt.ColValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Column != b[i].Column {
			return false
		}
		if a[i].TypeTag != b[i].TypeTag {
			return false
		}
		if !bytes.Equal(a[i].Bytes, b[i].Bytes) {
			return false
		}
	}
	return true
}
