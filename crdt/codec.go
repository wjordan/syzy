package crdt

// This file is the canonical reference for the Changeset wire format.
// Wire format == storage format: identical bytes appear in log.payload
// (metadata), on the transport, and in Changeset.encoded.
//
// Changeset frame:
//
//	1  byte    version
//	8  bytes   origin     (Dot.Origin, big-endian uint64; bit 63 reserved 0)
//	8  bytes   seq        (Dot.Seq, big-endian uint64)
//	8  bytes   hlc        (Stamp.Clock packed: 47-bit wall ms ‖ 16-bit logical)
//	16 bytes   cluster_id
//	1  byte    deps_count (≤ 255)
//	deps_count × (2 bytes chain_id, 8 bytes seq, big-endian)
//	8  bytes   crc64-ecma (over the frame with this slot zeroed)
//	varint     payload_length
//	N  bytes   payload    (record stream)
//
// Record frame (one per Record in payload):
//
//	1  byte    op   (1=Insert, 2=Update, 3=Delete, 4=BlobPatch)
//	16 bytes   table_id
//	varint     pk_blob_len, then pk_blob
//	varint     cl                    (writer's view of post-op CL)
//	op-specific payload:
//	  Insert/Update:
//	    varint column_count
//	    column_count × {16 byte column_id, 4 byte type_tag (big-endian),
//	                    1 byte format,
//	                    if type_tag != 0: varint value_len, value bytes}
//	  Delete: varint column_count == 0
//	  BlobPatch:
//	    16 bytes blob_column_id
//	    varint range_count
//	    range_count × {varint offset, varint byte_len, byte_len bytes}
//
// Shipping engines use the canonical storage-class type tags. The codec itself
// interprets only type_tag == 0, which is NULL and carries no format-dependent
// value bytes (§4 / wire-v2).
//
// Legacy wire-v1 (decode-only): identical frame, but each column is
// {16 byte column_id, 1 byte type_tag, if type_tag != 5 (NULL): varint
// value_len, value bytes} — no format byte. See decodeColumnsV1.
//
// Spec: docs/PROTOCOL.md#changeset-envelope.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"maps"
	"slices"
)

// crcTable is the polynomial Syzy uses for Changeset integrity. ECMA-182
// is also the polynomial Go's hash/crc64 package recommends for general
// integrity work.
var crcTable = crc64.MakeTable(crc64.ECMA)

// Errors surfaced by Decode.
var (
	ErrShortBuffer    = errors.New("crdt: short buffer")
	ErrUnknownVersion = errors.New("crdt: unknown wire version")
	ErrCRCMismatch    = errors.New("crdt: CRC mismatch")
	ErrUnknownOp      = errors.New("crdt: unknown record op")
	// ErrUnknownColType is surfaced by the predicate literal codec, whose
	// tag space is the four SQLite storage classes (the changeset codec
	// treats TypeTag as opaque and never validates it).
	ErrUnknownColType = errors.New("crdt: unknown column type tag")
	ErrOriginMismatch = errors.New("crdt: Stamp.Origin must equal Dot.Origin")
)

// Build encodes a Changeset from typed inputs and returns the
// immutable result. The producer calls Build at commit time.
func Build(dot Dot, stamp Stamp, deps Deps, cluster ClusterID, records []Record) (*Changeset, error) {
	if stamp.Origin != dot.Origin {
		return nil, ErrOriginMismatch
	}
	cs := &Changeset{
		Dot:       dot,
		Stamp:     stamp,
		Deps:      deps,
		ClusterID: cluster,
		Records:   records,
	}
	if err := encodeChangeset(cs); err != nil {
		return nil, err
	}
	return cs, nil
}

// Decode parses canonical bytes into a Changeset. The broker calls
// Decode on inbound delivery; the recovery path uses it when
// replaying mirror journals. The returned Changeset's Encoded field
// points into a copy of buf, owned by the Changeset.
func Decode(buf []byte) (*Changeset, error) {
	return decodeChangeset(buf)
}

// changesetHeaderLen returns the byte offset of the CRC field for a
// Changeset with depsCount entries. Layout up to (but not including)
// the CRC: version(1) + origin(8) + seq(8) + hlc(8) + cluster_id(16)
// + deps_count(1) + deps_count*(chain_id(2)+seq(8)).
func changesetHeaderLen(depsCount int) int {
	return 1 + 8 + 8 + 8 + 16 + 1 + depsCount*10
}

// encodeChangeset writes the canonical wire form into cs.encoded and
// stores the integrity CRC in cs.crc. Pure on cs.{Dot,Stamp,Deps,
// ClusterID,Records}.
//
// Wire layout:
//
//	header (changesetHeaderLen bytes)
//	8 bytes  CRC-64 (over header || varint(payloadLen) || payload, with
//	         this 8-byte slot itself excluded)
//	varint   payload_length
//	N bytes  payload
func encodeChangeset(cs *Changeset) error {
	if len(cs.Deps) > 0xff {
		return fmt.Errorf("crdt: too many Deps entries: %d (max 255)", len(cs.Deps))
	}
	payload, err := encodeRecords(nil, cs.Records)
	if err != nil {
		return err
	}

	hdrLen := changesetHeaderLen(len(cs.Deps))
	body := make([]byte, 0, hdrLen+8+binary.MaxVarintLen64+len(payload))

	body = append(body, WireVersion)
	body = binary.BigEndian.AppendUint64(body, uint64(cs.Dot.Origin))
	body = binary.BigEndian.AppendUint64(body, uint64(cs.Dot.Seq))
	body = binary.BigEndian.AppendUint64(body, cs.Stamp.Clock.Pack())
	body = append(body, cs.ClusterID[:]...)
	body = append(body, uint8(len(cs.Deps)))

	if len(cs.Deps) > 0 {
		chains := slices.Sorted(maps.Keys(cs.Deps))
		for _, chain := range chains {
			body = binary.BigEndian.AppendUint16(body, uint16(chain))
			body = binary.BigEndian.AppendUint64(body, uint64(cs.Deps[chain]))
		}
	}

	// Reserve 8 bytes for the CRC, then append payload-length and payload.
	body = append(body, 0, 0, 0, 0, 0, 0, 0, 0)
	body = binary.AppendUvarint(body, uint64(len(payload)))
	body = append(body, payload...)

	// Compute CRC over body with the CRC slot excluded; patch it in.
	cs.crc = crc64ChangesetIntegrity(body, hdrLen)
	binary.BigEndian.PutUint64(body[hdrLen:hdrLen+8], cs.crc)
	cs.encoded = body
	return nil
}

// crc64ChangesetIntegrity computes a CRC over body with the 8-byte CRC
// slot at crcOffset excluded. Receivers do the same and compare.
func crc64ChangesetIntegrity(body []byte, crcOffset int) uint64 {
	h := crc64.New(crcTable)
	h.Write(body[:crcOffset])
	h.Write(body[crcOffset+8:])
	return h.Sum64()
}

// decodeChangeset parses canonical bytes; CRC-checked. The returned
// Changeset owns its encoded form; PKBlob and ColValue.Bytes alias into
// it (no per-record copies).
func decodeChangeset(input []byte) (*Changeset, error) {
	if len(input) < 1+8+8+8+16+1+8 {
		return nil, ErrShortBuffer
	}
	version := input[0]
	if version != WireVersion && version != wireVersionV1 {
		return nil, ErrUnknownVersion
	}
	// Defensive copy upfront: subsequent sub-slices into buf belong to the
	// Changeset, even if the caller mutates input afterwards.
	buf := slices.Clone(input)
	cs := &Changeset{encoded: buf}
	off := 1
	cs.Dot.Origin = Origin(binary.BigEndian.Uint64(buf[off:]))
	off += 8
	cs.Dot.Seq = Seq(binary.BigEndian.Uint64(buf[off:]))
	off += 8
	cs.Stamp.Clock = UnpackClock(binary.BigEndian.Uint64(buf[off:]))
	cs.Stamp.Origin = cs.Dot.Origin
	off += 8
	copy(cs.ClusterID[:], buf[off:off+16])
	off += 16

	depsCount := int(buf[off])
	off++
	if len(buf) < off+depsCount*10+8 {
		return nil, ErrShortBuffer
	}
	if depsCount > 0 {
		cs.Deps = make(Deps, depsCount)
		for range depsCount {
			chain := ChainID(binary.BigEndian.Uint16(buf[off:]))
			off += 2
			seq := Seq(binary.BigEndian.Uint64(buf[off:]))
			off += 8
			cs.Deps[chain] = seq
		}
	}
	crcOffset := off
	expectedCRC := binary.BigEndian.Uint64(buf[off:])
	off += 8

	payloadLen, n := binary.Uvarint(buf[off:])
	if n <= 0 {
		return nil, ErrShortBuffer
	}
	off += n
	if uint64(len(buf)-off) < payloadLen {
		return nil, ErrShortBuffer
	}
	payload := buf[off : off+int(payloadLen)]
	off += int(payloadLen)
	if off != len(buf) {
		return nil, fmt.Errorf("crdt: trailing bytes (%d) after Changeset", len(buf)-off)
	}

	if crc64ChangesetIntegrity(buf, crcOffset) != expectedCRC {
		return nil, ErrCRCMismatch
	}

	records, err := decodeRecords(payload, version)
	if err != nil {
		return nil, err
	}
	cs.Records = records
	cs.crc = expectedCRC
	return cs, nil
}

// encodeRecords appends the payload portion of a Changeset to buf and
// returns the extended slice. Each record's wire layout:
//
//	1 byte   op
//	16 bytes table_id
//	varint   pk_blob_len, then pk_blob bytes
//	varint   cl
//	op-specific payload (column list for Insert/Update;
//	         empty for Delete; blob_column_id + ranges for BlobPatch).
func encodeRecords(buf []byte, recs []Record) ([]byte, error) {
	for _, r := range recs {
		h := r.Header()
		buf = append(buf, byte(h.Op))
		buf = append(buf, h.Table[:]...)
		buf = binary.AppendUvarint(buf, uint64(len(h.PK)))
		buf = append(buf, h.PK...)
		buf = binary.AppendUvarint(buf, h.CL)

		switch v := r.(type) {
		case Insert:
			buf = encodeColumns(buf, v.Image)
		case Update:
			buf = encodeColumns(buf, v.Changed)
		case Delete:
			buf = binary.AppendUvarint(buf, 0)
		case BlobPatch:
			buf = append(buf, v.Col[:]...)
			buf = binary.AppendUvarint(buf, uint64(len(v.Ranges)))
			for _, rr := range v.Ranges {
				buf = binary.AppendUvarint(buf, rr.Offset)
				buf = binary.AppendUvarint(buf, uint64(len(rr.Bytes)))
				buf = append(buf, rr.Bytes...)
			}
		default:
			return nil, fmt.Errorf("crdt: unknown record type %T", r)
		}
	}
	return buf, nil
}

// encodeColumns appends the (column_count, [column_id, type_tag, ...])
// section shared by Insert and Update.
func encodeColumns(buf []byte, cols []ColValue) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(cols)))
	for _, c := range cols {
		buf = append(buf, c.Column[:]...)
		buf = binary.BigEndian.AppendUint32(buf, c.TypeTag)
		buf = append(buf, c.Format)
		if c.TypeTag == ColNull {
			continue
		}
		buf = binary.AppendUvarint(buf, uint64(len(c.Bytes)))
		buf = append(buf, c.Bytes...)
	}
	return buf
}

// decodeRecords parses the payload, selecting the column layout by version.
func decodeRecords(buf []byte, version uint8) ([]Record, error) {
	decodeCols := decodeColumns
	if version == wireVersionV1 {
		decodeCols = decodeColumnsV1
	}
	var recs []Record
	for len(buf) > 0 {
		if len(buf) < 1+16 {
			return nil, ErrShortBuffer
		}
		op := recordOp(buf[0])
		buf = buf[1:]
		var tbl TableID
		copy(tbl[:], buf[:16])
		buf = buf[16:]

		pkLen, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, ErrShortBuffer
		}
		buf = buf[n:]
		if uint64(len(buf)) < pkLen {
			return nil, ErrShortBuffer
		}
		// Sub-slice into the Changeset's owned buffer; no copy.
		pk := PKBlob(buf[:pkLen])
		buf = buf[pkLen:]

		cl, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, ErrShortBuffer
		}
		buf = buf[n:]

		switch op {
		case opInsert:
			cols, rest, err := decodeCols(buf)
			if err != nil {
				return nil, err
			}
			recs = append(recs, Insert{Table: tbl, PK: pk, CL: cl, Image: cols})
			buf = rest
		case opUpdate:
			cols, rest, err := decodeCols(buf)
			if err != nil {
				return nil, err
			}
			recs = append(recs, Update{Table: tbl, PK: pk, CL: cl, Changed: cols})
			buf = rest
		case opDelete:
			cnt, n := binary.Uvarint(buf)
			if n <= 0 {
				return nil, ErrShortBuffer
			}
			if cnt != 0 {
				return nil, fmt.Errorf("crdt: Delete record carries %d columns; want 0", cnt)
			}
			buf = buf[n:]
			recs = append(recs, Delete{Table: tbl, PK: pk, CL: cl})
		case opBlobPatch:
			if len(buf) < 16 {
				return nil, ErrShortBuffer
			}
			var col ColumnID
			copy(col[:], buf[:16])
			buf = buf[16:]
			rangeCount, n := binary.Uvarint(buf)
			if n <= 0 {
				return nil, ErrShortBuffer
			}
			buf = buf[n:]
			ranges := make([]BlobPatchRange, rangeCount)
			for i := range ranges {
				offset, n := binary.Uvarint(buf)
				if n <= 0 {
					return nil, ErrShortBuffer
				}
				buf = buf[n:]
				byteLen, n := binary.Uvarint(buf)
				if n <= 0 {
					return nil, ErrShortBuffer
				}
				buf = buf[n:]
				if uint64(len(buf)) < byteLen {
					return nil, ErrShortBuffer
				}
				ranges[i] = BlobPatchRange{Offset: offset, Bytes: buf[:byteLen]}
				buf = buf[byteLen:]
			}
			recs = append(recs, BlobPatch{Table: tbl, PK: pk, CL: cl, Col: col, Ranges: ranges})
		default:
			return nil, fmt.Errorf("%w: %d", ErrUnknownOp, op)
		}
	}
	return recs, nil
}

func decodeColumns(buf []byte) ([]ColValue, []byte, error) {
	cnt, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil, nil, ErrShortBuffer
	}
	buf = buf[n:]
	cols := make([]ColValue, cnt)
	for i := range cols {
		if len(buf) < 16+4+1 {
			return nil, nil, ErrShortBuffer
		}
		copy(cols[i].Column[:], buf[:16])
		buf = buf[16:]
		cols[i].TypeTag = binary.BigEndian.Uint32(buf[:4])
		buf = buf[4:]
		cols[i].Format = buf[0]
		buf = buf[1:]
		if cols[i].TypeTag == ColNull {
			continue // NULL: no value bytes
		}
		byteLen, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, nil, ErrShortBuffer
		}
		buf = buf[n:]
		if uint64(len(buf)) < byteLen {
			return nil, nil, ErrShortBuffer
		}
		cols[i].Bytes = buf[:byteLen]
		buf = buf[byteLen:]
	}
	return cols, buf, nil
}

// decodeColumnsV1 parses the legacy layout: a one-byte ColType and no Format.
func decodeColumnsV1(buf []byte) ([]ColValue, []byte, error) {
	const v1ColNull = 5
	cnt, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil, nil, ErrShortBuffer
	}
	buf = buf[n:]
	cols := make([]ColValue, cnt)
	for i := range cols {
		if len(buf) < 17 {
			return nil, nil, ErrShortBuffer
		}
		copy(cols[i].Column[:], buf[:16])
		buf = buf[16:]
		tag := buf[0]
		buf = buf[1:]
		switch uint32(tag) {
		case ColInt, ColReal, ColText, ColBlob:
			cols[i].TypeTag = uint32(tag)
		case v1ColNull:
			continue
		default:
			return nil, nil, fmt.Errorf("%w: %d", ErrUnknownColType, tag)
		}
		byteLen, n := binary.Uvarint(buf)
		if n <= 0 {
			return nil, nil, ErrShortBuffer
		}
		buf = buf[n:]
		if uint64(len(buf)) < byteLen {
			return nil, nil, ErrShortBuffer
		}
		cols[i].Bytes = buf[:byteLen]
		buf = buf[byteLen:]
	}
	return cols, buf, nil
}
