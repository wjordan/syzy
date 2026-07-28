package journal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// File layout:
//
//   [0,  fileHeaderSize)        immutable file header
//   [fileHeaderSize, tail)      published records, written by Append in order
//   [tail, segmentSize)         zeroed reserve, available for future appends
//
// Records are one-shot cells. The first 4-byte word is zero while the
// writer fills the header, payload, and CRC; the writer publishes last
// with an atomic store of the nonzero kind word. Readers load that word
// with acquire semantics. Zero means the iterator is caught up at this
// offset; nonzero means the rest of the record is complete and visible.
//
// Records are padded so each starts at an 8-byte-aligned offset,
// keeping the in-record flags field (at record_start + flagsHeaderOffset)
// properly aligned for atomic uint32 ops — we do not assume the host
// supports unaligned atomics. The CRC trailer covers (header without
// flags || payload) so MarkAborted can flip a flag bit without
// invalidating verification.
//
// All multi-byte integers are little-endian. The format is per-process
// and never shipped, so cross-host endianness compatibility is a
// non-goal.

const (
	magic            uint64 = 0x4e524a595a59533a // "SYZYJRN:" little-endian
	formatVersion    uint32 = 2
	fileHeaderSize          = 64
	recordHeaderLen         = 40
	crcLen                  = 4
	recordTrailerLen        = 8 // crcLen + 4-byte reserved/padding
	recordAlign             = 8

	flagsHeaderOffset = 4 // byte offset of Flags within recordHeader
)

type fileHeader struct {
	Magic       uint64
	Version     uint32
	SegmentSize uint32
	CreatedUs   uint64
	Salt0       uint64
	Salt1       uint64
	HeaderCRC   uint32
}

func encodeFileHeader(h fileHeader) [fileHeaderSize]byte {
	var b [fileHeaderSize]byte
	binary.LittleEndian.PutUint64(b[0:], h.Magic)
	binary.LittleEndian.PutUint32(b[8:], h.Version)
	binary.LittleEndian.PutUint32(b[12:], h.SegmentSize)
	binary.LittleEndian.PutUint64(b[16:], h.CreatedUs)
	binary.LittleEndian.PutUint64(b[24:], h.Salt0)
	binary.LittleEndian.PutUint64(b[32:], h.Salt1)
	h.HeaderCRC = crc32.ChecksumIEEE(b[:60])
	binary.LittleEndian.PutUint32(b[60:], h.HeaderCRC)
	return b
}

func decodeFileHeader(b []byte) (fileHeader, error) {
	if len(b) < fileHeaderSize {
		return fileHeader{}, errShortHeader
	}
	h := fileHeader{
		Magic:       binary.LittleEndian.Uint64(b[0:]),
		Version:     binary.LittleEndian.Uint32(b[8:]),
		SegmentSize: binary.LittleEndian.Uint32(b[12:]),
		CreatedUs:   binary.LittleEndian.Uint64(b[16:]),
		Salt0:       binary.LittleEndian.Uint64(b[24:]),
		Salt1:       binary.LittleEndian.Uint64(b[32:]),
		HeaderCRC:   binary.LittleEndian.Uint32(b[60:]),
	}
	if h.Magic != magic {
		return fileHeader{}, errBadMagic
	}
	if h.Version != formatVersion {
		return fileHeader{}, errUnsupportedVersion
	}
	want := crc32.ChecksumIEEE(b[:60])
	if want != h.HeaderCRC {
		return fileHeader{}, errBadHeaderCRC
	}
	return h, nil
}

// recordHeader is the 40-byte prefix of every record. Layout:
//
//	bytes  [0,  4)  publish/kind word (0 while pending; low byte is Kind)
//	bytes  [4,  8)  Flags (uint32; low 16 bits used)
//	bytes  [8, 12)  PayloadLen
//	bytes [12, 16)  SchemaSeq (0 = none/pre-stamp record)
//	bytes [16, 24)  Seq
//	bytes [24, 32)  HLC
//	bytes [32, 40)  Origin
//
// SchemaSeq is the writer's schema-chain position when the payload was
// captured; consumers whose payload decode depends on column layout
// (touch records) use it to select the capture-time layout. Records
// written before the field existed carry 0, which readers treat as
// "decode under the current layout" (the pre-stamp behavior).
//
// The Flags field is mutable post-append; readers load it via atomic
// uint32 ops at record_start + flagsHeaderOffset.
type recordHeader struct {
	Kind       uint8
	PayloadLen uint32
	SchemaSeq  uint32
	Seq        uint64
	HLC        uint64
	Origin     uint64
}

func decodeRecordHeader(b []byte) recordHeader {
	kindWord := binary.LittleEndian.Uint32(b[0:])
	return recordHeader{
		Kind:       uint8(kindWord),
		PayloadLen: binary.LittleEndian.Uint32(b[8:]),
		SchemaSeq:  binary.LittleEndian.Uint32(b[12:]),
		Seq:        binary.LittleEndian.Uint64(b[16:]),
		HLC:        binary.LittleEndian.Uint64(b[24:]),
		Origin:     binary.LittleEndian.Uint64(b[32:]),
	}
}

// recordCRC computes the trailer CRC over (header without publish word
// or flags || payload). Skipping the publish word lets writers publish
// the record with one final atomic store; skipping flags lets
// MarkAborted flip a bit without invalidating the trailer.
func recordCRC(headerBytes []byte, payload []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(headerBytes[flagsHeaderOffset+4:])
	h.Write(payload)
	return h.Sum32()
}

// recordTotalLen returns the total on-disk size of a record with the
// given payload length, padded up to recordAlign.
func recordTotalLen(payloadLen uint32) int {
	raw := recordHeaderLen + int(payloadLen) + recordTrailerLen
	return (raw + recordAlign - 1) &^ (recordAlign - 1)
}

var (
	errShortHeader        = errors.New("journal: file shorter than header")
	errBadMagic           = errors.New("journal: bad magic")
	errUnsupportedVersion = errors.New("journal: unsupported format version")
	errBadHeaderCRC       = errors.New("journal: file header CRC mismatch")
)
