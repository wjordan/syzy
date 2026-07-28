package crdt

// WireVersion is the current Changeset wire-format version. Producers
// always emit this constant. Receivers accept it plus wireVersionV1;
// any other version is rejected.
const WireVersion uint8 = 2

// wireVersionV1 is the pre-TypeTag wire format: per-column 1-byte
// ColType (NULL encoded as tag 5) and no Format byte. Decode-only:
// production journals and epoch history can retain v1 payloads.
const wireVersionV1 uint8 = 1

// ClusterID is a 16-byte cluster-wide UUID. Receivers reject Changesets
// whose ClusterID does not match their configured cluster (mis-route
// defense — see ARCHITECTURE.md).
type ClusterID [16]byte

// ColType names the canonical storage-class TypeTag values shared by shipping
// engines. It is an alias for uint32 (the ColValue.TypeTag width). The core
// never interprets a nonzero TypeTag; arbitration is over Stamps.
type ColType = uint32

// Well-known TypeTag values. ColNull (0) is reserved cluster-wide as the NULL
// tag, so zero unambiguously means "SQL NULL, no bytes".
const (
	ColNull ColType = 0
	ColInt  ColType = 1
	ColReal ColType = 2
	ColText ColType = 3
	ColBlob ColType = 4
)

// ColValue.Format values. The codec round-trips Format untouched; the
// apply path interprets it.
const (
	FormatText   uint8 = 0
	FormatBinary uint8 = 1
	// FormatDelta marks a counter-column contribution: Bytes is a signed
	// int64 adjustment (TypeTag ColInt layout) summed into the current
	// cell value instead of overwriting it. CRDT.md F_counter.
	FormatDelta uint8 = 2
)

// ColValue is one column-id → typed-value pair carried inside an Insert.Image or
// Update.Changed slice (§4 / wire-v2).
type ColValue struct {
	Column ColumnID
	// TypeTag is an opaque, engine-defined type discriminator the core never
	// interprets. Shipping engines use the canonical storage-class tags
	// (ColInt/ColReal/ColText/ColBlob). TypeTag == 0 (ColNull) means the
	// value is SQL NULL and carries no Bytes.
	TypeTag uint32
	// Format is the byte encoding of Bytes: FormatText (0, the canonical
	// OID-free external form — the default), FormatBinary (1, a future
	// cluster-wide mode), or FormatDelta (2, a signed additive adjustment
	// to a counter column — same 8-byte int64 layout as an absolute
	// ColInt, applied as `col = col + ?`; see sqlite/docs/DDL.md#counter-columns).
	// It rides with every value so the decoder is unambiguous.
	Format uint8
	// Bytes carries the raw encoded value (empty when TypeTag == 0). For the
	// canonical tags: ColInt = 8-byte big-endian int64, ColReal = 8-byte big-endian
	// IEEE 754 binary64, ColText = UTF-8, and ColBlob = raw bytes.
	Bytes []byte
}

// Record is the sealed-sum DML record carried inside a Changeset.
// Implementations: Insert, Update, Delete, BlobPatch.
//
// Every record carries the CL (causal length) it applies under — the
// writer's view of the row's generation at write time:
//
//   - Insert: the post-INSERT CL (writer's NextLiveCL — always odd).
//   - Update: the row's current CL on the writer (must be odd; UPDATE
//     does not bump CL).
//   - Delete: the post-DELETE CL (writer's NextTombCL — always even).
//   - BlobPatch: the row's current CL on the writer (must be odd).
//
// Receivers apply a record iff its (CL, Stamp) lex-dominates the
// receiver's current (RowState.CL, RowState.Base) — see
// CRDT.md#causal-length-cl.
type Record interface {
	Header() RecordHeader
}

// RecordHeader is the per-record prefix shared by every Record:
// the wire op tag, the (Table, PK) key, and the writer's view of the
// post-op CL.
type RecordHeader struct {
	Op    recordOp
	Table TableID
	PK    PKBlob
	CL    uint64
}

// recordOp is the wire op-byte tagging Record implementations.
type recordOp uint8

const (
	opInsert    recordOp = 1
	opUpdate    recordOp = 2
	opDelete    recordOp = 3
	opBlobPatch recordOp = 4
)

// Insert carries a full row image of active non-generated columns plus
// the post-INSERT CL.
type Insert struct {
	Table TableID
	PK    PKBlob
	CL    uint64
	Image []ColValue
}

func (r Insert) Header() RecordHeader {
	return RecordHeader{Op: opInsert, Table: r.Table, PK: r.PK, CL: r.CL}
}

// Update carries only columns whose final value differs from
// first-touch evidence, plus the row's current CL on the writer.
type Update struct {
	Table   TableID
	PK      PKBlob
	CL      uint64
	Changed []ColValue
}

func (r Update) Header() RecordHeader {
	return RecordHeader{Op: opUpdate, Table: r.Table, PK: r.PK, CL: r.CL}
}

// Delete tombstones a row keyed by (Table, PK) at the post-DELETE CL.
// No column payload.
type Delete struct {
	Table TableID
	PK    PKBlob
	CL    uint64
}

func (r Delete) Header() RecordHeader {
	return RecordHeader{Op: opDelete, Table: r.Table, PK: r.PK, CL: r.CL}
}

// BlobPatchRange is one (offset, bytes) sub-range inside a BlobPatch.
type BlobPatchRange struct {
	Offset uint64
	Bytes  []byte
}

// End returns Offset + len(Bytes).
func (r BlobPatchRange) End() uint64 { return r.Offset + uint64(len(r.Bytes)) }

// BlobPatch carries non-overlapping per-byte updates to a single blob
// column on a single row, scoped to the row's current CL on the writer.
// See BLOB_PATCH.md for the full algorithm.
type BlobPatch struct {
	Table  TableID
	PK     PKBlob
	CL     uint64
	Col    ColumnID
	Ranges []BlobPatchRange
}

func (r BlobPatch) Header() RecordHeader {
	return RecordHeader{Op: opBlobPatch, Table: r.Table, PK: r.PK, CL: r.CL}
}

// Changeset is the replicated unit: one committed local transaction's
// DML records, framed with identity (Dot), arbitration (Stamp),
// dependencies (Deps), and integrity (CRC). Immutable after construction.
//
// Build encodes a Changeset from typed records (used by the producer at
// commit time). Decode parses wire/storage bytes (used by the broker on
// inbound apply and by recovery's mirror-journal replay). The Encoded
// field caches the canonical bytes; a Changeset's Encoded is identical
// to its journal-record payload and to its on-the-wire bytes.
type Changeset struct {
	Dot       Dot
	Stamp     Stamp
	Deps      Deps
	ClusterID ClusterID
	Records   []Record

	encoded []byte
	crc     uint64
}

// Encoded returns the canonical wire/storage bytes. Caller must not
// mutate the returned slice.
func (c *Changeset) Encoded() []byte { return c.encoded }

// CRC returns the 64-bit integrity checksum carried in the header.
func (c *Changeset) CRC() uint64 { return c.crc }
