package syncer

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

// Journal payload wire format for KindLocalDML records (deferred drain).
//
// All multi-byte ints are little-endian. The format is process-internal:
// every producer that reads it ran the same build at append time.
//
//	[ 4 bytes ] format_version (currently 1)
//	[ 4 bytes ] flags (reserved, 0)
//	[ 8 bytes ] hlc (packed crdt.Clock)
//	[ 8 bytes ] dot_origin
//	[ 4 bytes ] schema_seq (informational; sink's catalog must agree)
//	[ 4 bytes ] record_count
//	[ records  ] one per touched (table, rowid) — see encodeEvidence
//
// Each record:
//
//	[ 1 byte  ] op (1=INSERT, 2=UPDATE, 3=DELETE, 4=UPDATE_PK_CHANGED)
//	[ 16 bytes] table_id
//	[ varies  ] PK section(s) + column-data section(s) per op:
//	            INSERT            : pk(newPK) || cols(image)
//	            UPDATE            : pk(newPK) || cols(changed)
//	            DELETE            : pk(oldPK)
//	            UPDATE_PK_CHANGED : pk(oldPK) || pk(newPK) || cols(image)
//
// PK section:
//	[ 4 bytes ] pk_len
//	[ pk_len  ] pk_bytes
//
// Column-data section:
//	[ 4 bytes ] col_count
//	[ per col ]
//	  [ 16 bytes ] column_id
//	  [ 1 byte   ] type (0=null, 1=int, 2=real, 3=text, 4=blob)
//	  [ 4 bytes  ] value_len (0 for null)
//	  [ value_len bytes ] value

const (
	payloadFormatV1 uint32 = 1

	evOpInsert         uint8 = 1
	evOpUpdate         uint8 = 2
	evOpDelete         uint8 = 3
	evOpUpdatePKChange uint8 = 4
	evOpBlobPatch      uint8 = 5

	payloadHeaderLen = 28
)

// recordEvidence is the per-touched-row evidence captured at commit_hook
// time. Sink decodes the journal payload back into recordEvidence and
// turns each entry into one or two crdt.Record values (UPDATE_PK_CHANGED
// produces both a Delete and an Insert).
type recordEvidence struct {
	op      uint8
	tableID crdt.TableID
	oldPK   crdt.PKBlob     // for DELETE, UPDATE_PK_CHANGED
	newPK   crdt.PKBlob     // for INSERT, UPDATE, UPDATE_PK_CHANGED, BlobPatch
	image   []crdt.ColValue // for INSERT, UPDATE_PK_CHANGED — full row
	changed []crdt.ColValue // for UPDATE — changed cols only
	// blob_patch fields
	blobCol    crdt.ColumnID         // for BlobPatch
	blobRanges []crdt.BlobPatchRange // for BlobPatch — pre-diffed ranges
}

// payloadHeader is the parsed prefix; record bytes follow.
type payloadHeader struct {
	hlc        uint64
	dotOrigin  uint64
	schemaSeq  uint32
	recordsLen int
}

// encodePayload serializes header + record evidence into buf, growing
// if needed. Pass nil (or an empty slice) to allocate fresh; pass a
// scratch buffer to reuse capacity across calls. Returns the populated
// slice (length == total payload size).
func encodePayload(buf []byte, hdr payloadHeader, records []recordEvidence) ([]byte, error) {
	if int64(hdr.recordsLen) != int64(len(records)) {
		return nil, fmt.Errorf("payload: header recordsLen=%d but %d records", hdr.recordsLen, len(records))
	}
	size := payloadHeaderLen
	for _, r := range records {
		size += evidenceEncodedLen(r)
	}
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	binary.LittleEndian.PutUint32(buf[0:], payloadFormatV1)
	binary.LittleEndian.PutUint32(buf[4:], 0) // flags reserved
	binary.LittleEndian.PutUint64(buf[8:], hdr.hlc)
	binary.LittleEndian.PutUint64(buf[16:], hdr.dotOrigin)
	binary.LittleEndian.PutUint32(buf[24:], hdr.schemaSeq)
	// recordCount is implicit: len(records). Encoded as a redundant trailer
	// would aid corruption detection but the journal CRC already covers it.
	off := payloadHeaderLen
	for _, r := range records {
		n, err := encodeEvidence(buf[off:], r)
		if err != nil {
			return nil, err
		}
		off += n
	}
	if off != size {
		return nil, fmt.Errorf("payload: encoded %d bytes but reserved %d", off, size)
	}
	return buf, nil
}

func evidenceEncodedLen(r recordEvidence) int {
	n := 1 + 16 // op + tableID
	switch r.op {
	case evOpInsert, evOpUpdate:
		n += pkEncodedLen(r.newPK)
		if r.op == evOpInsert {
			n += colsEncodedLen(r.image)
		} else {
			n += colsEncodedLen(r.changed)
		}
	case evOpDelete:
		n += pkEncodedLen(r.oldPK)
	case evOpUpdatePKChange:
		n += pkEncodedLen(r.oldPK)
		n += pkEncodedLen(r.newPK)
		n += colsEncodedLen(r.image)
	case evOpBlobPatch:
		n += pkEncodedLen(r.newPK)
		n += 16 // blob col id
		n += 4  // range count
		for _, rr := range r.blobRanges {
			n += 8 + 4 + len(rr.Bytes)
		}
	}
	return n
}

func pkEncodedLen(pk crdt.PKBlob) int { return 4 + len(pk) }

func colsEncodedLen(cols []crdt.ColValue) int {
	n := 4 // count
	for _, c := range cols {
		n += 16 + 1 + 4 + len(c.Bytes) // colID + type + len + bytes
	}
	return n
}

func encodeEvidence(buf []byte, r recordEvidence) (int, error) {
	off := 0
	buf[off] = r.op
	off++
	copy(buf[off:off+16], r.tableID[:])
	off += 16
	switch r.op {
	case evOpInsert:
		off += encodePKField(buf[off:], r.newPK)
		off += encodeCols(buf[off:], r.image)
	case evOpUpdate:
		off += encodePKField(buf[off:], r.newPK)
		off += encodeCols(buf[off:], r.changed)
	case evOpDelete:
		off += encodePKField(buf[off:], r.oldPK)
	case evOpUpdatePKChange:
		off += encodePKField(buf[off:], r.oldPK)
		off += encodePKField(buf[off:], r.newPK)
		off += encodeCols(buf[off:], r.image)
	case evOpBlobPatch:
		off += encodePKField(buf[off:], r.newPK)
		copy(buf[off:off+16], r.blobCol[:])
		off += 16
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(r.blobRanges)))
		off += 4
		for _, rr := range r.blobRanges {
			binary.LittleEndian.PutUint64(buf[off:], rr.Offset)
			off += 8
			binary.LittleEndian.PutUint32(buf[off:], uint32(len(rr.Bytes)))
			off += 4
			copy(buf[off:off+len(rr.Bytes)], rr.Bytes)
			off += len(rr.Bytes)
		}
	default:
		return 0, fmt.Errorf("payload: unknown op %d", r.op)
	}
	return off, nil
}

func encodePKField(buf []byte, pk crdt.PKBlob) int {
	binary.LittleEndian.PutUint32(buf[0:], uint32(len(pk)))
	copy(buf[4:4+len(pk)], pk)
	return 4 + len(pk)
}

func encodeCols(buf []byte, cols []crdt.ColValue) int {
	binary.LittleEndian.PutUint32(buf[0:], uint32(len(cols)))
	off := 4
	for _, c := range cols {
		copy(buf[off:off+16], c.Column[:])
		off += 16
		buf[off] = uint8(c.TypeTag)
		off++
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(c.Bytes)))
		off += 4
		copy(buf[off:off+len(c.Bytes)], c.Bytes)
		off += len(c.Bytes)
	}
	return off
}

func wireToColType(t uint8) (crdt.ColType, error) {
	switch crdt.ColType(t) {
	case crdt.ColInt, crdt.ColReal, crdt.ColText, crdt.ColBlob, crdt.ColNull:
		return crdt.ColType(t), nil
	}
	return 0, fmt.Errorf("payload: unknown column type %d", t)
}

// decodePayload parses the header and returns evidence + header. The
// returned evidence aliases the input buf (PK bytes, column bytes); the
// caller must copy if buf is borrowed from a journal mmap.
//
// Note: the drainer copies payloads before calling Apply, so under the
// drainer-sink call path buf is already an owned copy.
func decodePayload(buf []byte) (payloadHeader, []recordEvidence, error) {
	if len(buf) < payloadHeaderLen {
		return payloadHeader{}, nil, errors.New("payload: short header")
	}
	ver := binary.LittleEndian.Uint32(buf[0:])
	if ver != payloadFormatV1 {
		return payloadHeader{}, nil, fmt.Errorf("payload: unsupported format version %d", ver)
	}
	hdr := payloadHeader{
		hlc:       binary.LittleEndian.Uint64(buf[8:]),
		dotOrigin: binary.LittleEndian.Uint64(buf[16:]),
		schemaSeq: binary.LittleEndian.Uint32(buf[24:]),
	}
	out := []recordEvidence{}
	off := payloadHeaderLen
	for off < len(buf) {
		r, n, err := decodeEvidence(buf[off:])
		if err != nil {
			return payloadHeader{}, nil, fmt.Errorf("payload: record %d: %w", len(out), err)
		}
		out = append(out, r)
		off += n
	}
	if off != len(buf) {
		return payloadHeader{}, nil, fmt.Errorf("payload: trailing %d bytes", len(buf)-off)
	}
	hdr.recordsLen = len(out)
	return hdr, out, nil
}

func decodeEvidence(buf []byte) (recordEvidence, int, error) {
	if len(buf) < 1+16 {
		return recordEvidence{}, 0, errors.New("payload: short evidence header")
	}
	r := recordEvidence{op: buf[0]}
	copy(r.tableID[:], buf[1:17])
	off := 17
	switch r.op {
	case evOpInsert:
		pk, n, err := decodePKField(buf[off:])
		if err != nil {
			return r, 0, err
		}
		r.newPK = pk
		off += n
		cols, n, err := decodeCols(buf[off:])
		if err != nil {
			return r, 0, err
		}
		r.image = cols
		off += n
	case evOpUpdate:
		pk, n, err := decodePKField(buf[off:])
		if err != nil {
			return r, 0, err
		}
		r.newPK = pk
		off += n
		cols, n, err := decodeCols(buf[off:])
		if err != nil {
			return r, 0, err
		}
		r.changed = cols
		off += n
	case evOpDelete:
		pk, n, err := decodePKField(buf[off:])
		if err != nil {
			return r, 0, err
		}
		r.oldPK = pk
		off += n
	case evOpUpdatePKChange:
		old, n, err := decodePKField(buf[off:])
		if err != nil {
			return r, 0, err
		}
		r.oldPK = old
		off += n
		nw, n2, err := decodePKField(buf[off:])
		if err != nil {
			return r, 0, err
		}
		r.newPK = nw
		off += n2
		cols, n3, err := decodeCols(buf[off:])
		if err != nil {
			return r, 0, err
		}
		r.image = cols
		off += n3
	case evOpBlobPatch:
		pk, n, err := decodePKField(buf[off:])
		if err != nil {
			return r, 0, err
		}
		r.newPK = pk
		off += n
		if off+16+4 > len(buf) {
			return r, 0, errors.New("payload: short blob_patch header")
		}
		copy(r.blobCol[:], buf[off:off+16])
		off += 16
		rc := int(binary.LittleEndian.Uint32(buf[off:]))
		off += 4
		ranges := make([]crdt.BlobPatchRange, rc)
		for i := 0; i < rc; i++ {
			if off+8+4 > len(buf) {
				return r, 0, fmt.Errorf("payload: short blob range %d", i)
			}
			rng := crdt.BlobPatchRange{
				Offset: binary.LittleEndian.Uint64(buf[off:]),
			}
			off += 8
			ln := int(binary.LittleEndian.Uint32(buf[off:]))
			off += 4
			if off+ln > len(buf) {
				return r, 0, fmt.Errorf("payload: short blob range bytes %d", i)
			}
			rng.Bytes = buf[off : off+ln]
			off += ln
			ranges[i] = rng
		}
		r.blobRanges = ranges
	default:
		return r, 0, fmt.Errorf("unknown op %d", r.op)
	}
	return r, off, nil
}

func decodePKField(buf []byte) (crdt.PKBlob, int, error) {
	if len(buf) < 4 {
		return nil, 0, errors.New("payload: short pk length")
	}
	n := int(binary.LittleEndian.Uint32(buf[0:]))
	if 4+n > len(buf) {
		return nil, 0, errors.New("payload: short pk bytes")
	}
	return crdt.PKBlob(buf[4 : 4+n]), 4 + n, nil
}

func decodeCols(buf []byte) ([]crdt.ColValue, int, error) {
	if len(buf) < 4 {
		return nil, 0, errors.New("payload: short col count")
	}
	n := int(binary.LittleEndian.Uint32(buf[0:]))
	off := 4
	out := make([]crdt.ColValue, 0, n)
	for i := 0; i < n; i++ {
		if off+16+1+4 > len(buf) {
			return nil, 0, fmt.Errorf("payload: short col header at %d", i)
		}
		var col crdt.ColumnID
		copy(col[:], buf[off:off+16])
		off += 16
		t, err := wireToColType(buf[off])
		if err != nil {
			return nil, 0, err
		}
		off++
		vlen := int(binary.LittleEndian.Uint32(buf[off:]))
		off += 4
		if off+vlen > len(buf) {
			return nil, 0, fmt.Errorf("payload: short col value at %d", i)
		}
		out = append(out, crdt.ColValue{Column: col, TypeTag: t, Bytes: buf[off : off+vlen]})
		off += vlen
	}
	return out, off, nil
}
