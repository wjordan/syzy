package crdt

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// CatalogOpKind tags one shape of CatalogOp on the wire and in
// syzy_schema_event.catalog_op. Stable across versions; do not
// reorder existing values.
type CatalogOpKind uint8

const (
	OpUnknown            CatalogOpKind = 0
	OpCreateTable        CatalogOpKind = 1
	OpAddColumn          CatalogOpKind = 2
	OpRenameTable        CatalogOpKind = 3
	OpRenameColumn       CatalogOpKind = 4
	OpDropColumn         CatalogOpKind = 5
	OpDropTable          CatalogOpKind = 6
	OpAddUniqueKey       CatalogOpKind = 7
	OpDropUniqueKey      CatalogOpKind = 8
	OpCreateIndex        CatalogOpKind = 9
	OpDropIndex          CatalogOpKind = 10
	OpCreateView         CatalogOpKind = 11
	OpDropView           CatalogOpKind = 12
	OpCreateVirtualTable CatalogOpKind = 13
	OpDropVirtualTable   CatalogOpKind = 14
	OpCreateTrigger      CatalogOpKind = 15
	OpDropTrigger        CatalogOpKind = 16
	OpBundle             CatalogOpKind = 17
	OpSetClockGroup      CatalogOpKind = 18
)

// Catalog key layouts are durable schema-log formats. The high bits of the
// kind byte extend the original layout with coordinated-key and partial-key
// fields while leaving operations that need neither byte-identical.
const (
	catalogOpV2Flag = 0x80
	catalogOpV3Flag = 0x40
)

// String returns a stable human-readable name for the op kind. Used by
// logging, status output, and catalog_op debug dumps.
func (k CatalogOpKind) String() string {
	switch k {
	case OpCreateTable:
		return "create_table"
	case OpAddColumn:
		return "add_column"
	case OpRenameTable:
		return "rename_table"
	case OpRenameColumn:
		return "rename_column"
	case OpDropColumn:
		return "drop_column"
	case OpDropTable:
		return "drop_table"
	case OpAddUniqueKey:
		return "add_unique_key"
	case OpDropUniqueKey:
		return "drop_unique_key"
	case OpCreateIndex:
		return "create_index"
	case OpDropIndex:
		return "drop_index"
	case OpCreateView:
		return "create_view"
	case OpDropView:
		return "drop_view"
	case OpCreateVirtualTable:
		return "create_vtab"
	case OpDropVirtualTable:
		return "drop_vtab"
	case OpCreateTrigger:
		return "create_trigger"
	case OpDropTrigger:
		return "drop_trigger"
	case OpBundle:
		return "bundle"
	case OpSetClockGroup:
		return "set_clock_group"
	}
	return "unknown"
}

// CatalogColumn describes one column inside CreateTable / AddColumn.
// Default carries the textual default expression as it appears in the
// declaration (or the empty string for none); receivers re-emit it
// verbatim.
type CatalogColumn struct {
	ID         ColumnID
	Name       string
	Ordinal    int
	Type       string // declared SQLite type, e.g. "INTEGER", "TEXT", "BLOB", "" for typeless.
	NotNull    bool
	Default    string // SQL default expression, "" for none.
	IsPK       bool   // true if column is part of the PK
	PKPos      int    // 1-indexed PK position; meaningful only when IsPK.
	ClockGroup string // "row" (default) or "cell".
	Generated  bool   // STORED or VIRTUAL generated column; receivers recompute.
	// Collation is the column's text collating sequence. The zero value
	// (CollBinary) is SQLite's default.
	Collation Collation
}

// CatalogKeyMember names one member column of a unique key.
type CatalogKeyMember struct {
	ColumnID ColumnID
	Ordinal  int
}

// CatalogOp is the typed catalog mutation written to the schema
// log and replayed by every node's apply path. The Kind tag
// selects which fields are meaningful; encoding is dense.
//
// All op variants share two header fields so the decoder can dispatch
// without looking inside each variant's body. Fields that don't apply
// to a given Kind are ignored on encode and zero on decode.
type CatalogOp struct {
	Kind CatalogOpKind

	// CreateTable, DropTable, RenameTable, AddColumn, DropColumn,
	// RenameColumn, AddUniqueKey, DropUniqueKey: target table.
	TableID   TableID
	TableName string // post-rename name for RenameTable; declared name for CreateTable; ignored otherwise.

	// AddColumn, DropColumn, RenameColumn: target column.
	ColumnID   ColumnID
	ColumnName string // post-rename name for RenameColumn; new column name for AddColumn.

	// CreateTable: full column list (declared order). AddColumn: a
	// single-element column descriptor.
	Columns []CatalogColumn
	// CreateTable: render the receiver table with WITHOUT ROWID.
	WithoutRowid bool

	// CreateTable, AddUniqueKey: key membership. CreateTable always
	// includes the PK at KeyID = PKKeyID (all-zero). Unique keys appear
	// at distinct KeyIDs.
	Keys []CatalogKey

	// AddUniqueKey / DropUniqueKey: key id.
	KeyID KeyID

	// CreateIndex, DropIndex, CreateView, DropView,
	// CreateVirtualTable, DropVirtualTable: opaque SQL replayed
	// verbatim on receivers. Replicated views/vtables are not typed.
	RawSQL string

	// CreateView/DropView/CreateVirtualTable/DropVirtualTable/
	// CreateTrigger/DropTrigger: the object's name. Used by receivers
	// to re-prepare structural state checks (e.g. DROP VIEW IF EXISTS).
	//
	// On CreateTrigger/DropTrigger, TableID doubles as a marker: zero
	// means a user-written trigger; non-zero means a cascade-
	// synthesized trigger owned by that child table (apply path
	// registers/unregisters syzy_synth_trigger accordingly).
	ObjectName string

	// Bundle: ordered list of sub-ops applied atomically on receivers
	// in a single metadata txn. Used for compound DDL (e.g. CREATE
	// TABLE plus its synthesized cascade triggers). Bundles do not
	// nest; SubOps must not contain another OpBundle.
	SubOps []CatalogOp

	// SetClockGroup: the table's new default_clock_group ('row' or
	// 'cell'). Target table in TableID.
	ClockGroup string
}

// CatalogKey describes one key (PK or unique) inside CatalogOp.Keys.
type CatalogKey struct {
	KeyID   KeyID
	Members []CatalogKeyMember
	// Coordinated marks a CP unique key (NOT NULL UNIQUE) whose global
	// uniqueness is enforced by reservation before commit. Always false
	// for the PK and for eventual (loser-null) unique keys. See
	// docs/SCHEMA.md#unique-keys.
	Coordinated bool
	// Predicate is the compiled WHERE clause of a partial unique index
	// (zero/nil Root for a total key). A partial key is always
	// Coordinated. See
	// docs/SCHEMA.md#unique-keys.
	Predicate UniquePredicate
}

// EncodeCatalogOp returns the canonical wire bytes for op. Errors only
// for unsupported kinds or oversized strings (uvarint length tags are
// generous; the limits exist to fail loudly on malformed input rather
// than silently truncate).
func EncodeCatalogOp(op CatalogOp) ([]byte, error) {
	if op.Kind == OpUnknown {
		return nil, errors.New("crdt: cannot encode CatalogOp with Kind=Unknown")
	}
	var buf []byte
	partial := opCarriesPartialKey(op)
	versioned := opCarriesCoordinatedKey(op) || partial
	kindByte := byte(op.Kind)
	if versioned {
		kindByte |= catalogOpV2Flag
	}
	if partial {
		kindByte |= catalogOpV3Flag
	}
	buf = append(buf, kindByte)
	switch op.Kind {
	case OpCreateTable:
		buf = appendBytes16(buf, op.TableID[:])
		buf = appendString(buf, op.TableName)
		buf = appendColumns(buf, op.Columns)
		buf = appendKeys(buf, op.Keys, versioned, partial)
		if op.WithoutRowid {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	case OpAddColumn:
		buf = appendBytes16(buf, op.TableID[:])
		buf = appendColumns(buf, op.Columns)
	case OpRenameTable:
		buf = appendBytes16(buf, op.TableID[:])
		buf = appendString(buf, op.TableName)
	case OpRenameColumn:
		buf = appendBytes16(buf, op.TableID[:])
		buf = appendBytes16(buf, op.ColumnID[:])
		buf = appendString(buf, op.ColumnName)
	case OpDropColumn:
		buf = appendBytes16(buf, op.TableID[:])
		buf = appendBytes16(buf, op.ColumnID[:])
	case OpDropTable:
		buf = appendBytes16(buf, op.TableID[:])
	case OpSetClockGroup:
		buf = appendBytes16(buf, op.TableID[:])
		buf = appendString(buf, op.ClockGroup)
	case OpAddUniqueKey:
		buf = appendBytes16(buf, op.TableID[:])
		buf = appendBytes16(buf, op.KeyID[:])
		buf = appendKeyMembers(buf, op.Keys, versioned, partial)
	case OpDropUniqueKey:
		buf = appendBytes16(buf, op.TableID[:])
		buf = appendBytes16(buf, op.KeyID[:])
	case OpCreateIndex, OpDropIndex,
		OpCreateView, OpDropView,
		OpCreateVirtualTable, OpDropVirtualTable:
		buf = appendString(buf, op.ObjectName)
		buf = appendString(buf, op.RawSQL)
	case OpCreateTrigger, OpDropTrigger:
		// Triggers carry an optional child-table id distinguishing
		// cascade-synthesized triggers (non-zero) from user-written
		// triggers (zero). The apply path registers / removes
		// syzy_synth_trigger rows on the non-zero branch.
		buf = appendBytes16(buf, op.TableID[:])
		buf = appendString(buf, op.ObjectName)
		buf = appendString(buf, op.RawSQL)
	case OpBundle:
		buf = binary.AppendUvarint(buf, uint64(len(op.SubOps)))
		for _, sub := range op.SubOps {
			if sub.Kind == OpBundle {
				return nil, errors.New("crdt: nested OpBundle is not allowed")
			}
			subBuf, err := EncodeCatalogOp(sub)
			if err != nil {
				return nil, fmt.Errorf("crdt: encode bundle sub-op: %w", err)
			}
			buf = binary.AppendUvarint(buf, uint64(len(subBuf)))
			buf = append(buf, subBuf...)
		}
	default:
		return nil, fmt.Errorf("crdt: unsupported CatalogOpKind %d", op.Kind)
	}
	return buf, nil
}

// DecodeCatalogOp parses the bytes produced by EncodeCatalogOp.
func DecodeCatalogOp(buf []byte) (CatalogOp, error) {
	if len(buf) < 1 {
		return CatalogOp{}, ErrShortBuffer
	}
	versioned := buf[0]&catalogOpV2Flag != 0
	partial := buf[0]&catalogOpV3Flag != 0
	kind := CatalogOpKind(buf[0] &^ (catalogOpV2Flag | catalogOpV3Flag))
	rest := buf[1:]
	op := CatalogOp{Kind: kind}
	var err error
	switch kind {
	case OpCreateTable:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
		if op.TableName, rest, err = readString(rest); err != nil {
			return CatalogOp{}, err
		}
		if op.Columns, rest, err = readColumns(rest); err != nil {
			return CatalogOp{}, err
		}
		if op.Keys, rest, err = readKeys(rest, versioned, partial); err != nil {
			return CatalogOp{}, err
		}
		if len(rest) < 1 {
			return CatalogOp{}, ErrShortBuffer
		}
		op.WithoutRowid = rest[0] != 0
		rest = rest[1:]
	case OpAddColumn:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
		if op.Columns, rest, err = readColumns(rest); err != nil {
			return CatalogOp{}, err
		}
	case OpRenameTable:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
		if op.TableName, rest, err = readString(rest); err != nil {
			return CatalogOp{}, err
		}
	case OpRenameColumn:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
		if rest, err = readBytes16(rest, op.ColumnID[:]); err != nil {
			return CatalogOp{}, err
		}
		if op.ColumnName, rest, err = readString(rest); err != nil {
			return CatalogOp{}, err
		}
	case OpDropColumn:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
		if rest, err = readBytes16(rest, op.ColumnID[:]); err != nil {
			return CatalogOp{}, err
		}
	case OpDropTable:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
	case OpSetClockGroup:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
		if op.ClockGroup, rest, err = readString(rest); err != nil {
			return CatalogOp{}, err
		}
	case OpAddUniqueKey:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
		if rest, err = readBytes16(rest, op.KeyID[:]); err != nil {
			return CatalogOp{}, err
		}
		if op.Keys, rest, err = readKeyMembers(rest, op.KeyID, versioned, partial); err != nil {
			return CatalogOp{}, err
		}
	case OpDropUniqueKey:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
		if rest, err = readBytes16(rest, op.KeyID[:]); err != nil {
			return CatalogOp{}, err
		}
	case OpCreateIndex, OpDropIndex,
		OpCreateView, OpDropView,
		OpCreateVirtualTable, OpDropVirtualTable:
		if op.ObjectName, rest, err = readString(rest); err != nil {
			return CatalogOp{}, err
		}
		if op.RawSQL, rest, err = readString(rest); err != nil {
			return CatalogOp{}, err
		}
	case OpCreateTrigger, OpDropTrigger:
		if rest, err = readBytes16(rest, op.TableID[:]); err != nil {
			return CatalogOp{}, err
		}
		if op.ObjectName, rest, err = readString(rest); err != nil {
			return CatalogOp{}, err
		}
		if op.RawSQL, rest, err = readString(rest); err != nil {
			return CatalogOp{}, err
		}
	case OpBundle:
		n, sz := binary.Uvarint(rest)
		if sz <= 0 {
			return CatalogOp{}, ErrShortBuffer
		}
		rest = rest[sz:]
		op.SubOps = make([]CatalogOp, 0, n)
		for range n {
			subLen, sz := binary.Uvarint(rest)
			if sz <= 0 {
				return CatalogOp{}, ErrShortBuffer
			}
			rest = rest[sz:]
			if uint64(len(rest)) < subLen {
				return CatalogOp{}, ErrShortBuffer
			}
			sub, err := DecodeCatalogOp(rest[:subLen])
			if err != nil {
				return CatalogOp{}, fmt.Errorf("crdt: decode bundle sub-op: %w", err)
			}
			if sub.Kind == OpBundle {
				return CatalogOp{}, errors.New("crdt: nested OpBundle is not allowed")
			}
			op.SubOps = append(op.SubOps, sub)
			rest = rest[subLen:]
		}
	default:
		return CatalogOp{}, fmt.Errorf("crdt: unknown CatalogOpKind %d", kind)
	}
	if len(rest) != 0 {
		return CatalogOp{}, fmt.Errorf("crdt: %d trailing bytes after CatalogOp", len(rest))
	}
	return op, nil
}

// appendBytes16 appends a fixed 16-byte ID without any length prefix.
func appendBytes16(buf, b []byte) []byte {
	if len(b) != 16 {
		panic(fmt.Sprintf("crdt: appendBytes16: got len=%d", len(b)))
	}
	return append(buf, b...)
}

func readBytes16(buf, dst []byte) ([]byte, error) {
	if len(buf) < 16 {
		return nil, ErrShortBuffer
	}
	copy(dst, buf[:16])
	return buf[16:], nil
}

// appendString writes a length-prefixed UTF-8 string. Length is varint-
// encoded so short strings stay 1 byte of overhead.
func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func readString(buf []byte) (string, []byte, error) {
	n, sz := binary.Uvarint(buf)
	if sz <= 0 {
		return "", nil, ErrShortBuffer
	}
	buf = buf[sz:]
	if uint64(len(buf)) < n {
		return "", nil, ErrShortBuffer
	}
	return string(buf[:n]), buf[n:], nil
}

func appendColumns(buf []byte, cols []CatalogColumn) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(cols)))
	for _, c := range cols {
		buf = appendBytes16(buf, c.ID[:])
		buf = appendString(buf, c.Name)
		buf = binary.AppendUvarint(buf, uint64(c.Ordinal))
		buf = appendString(buf, c.Type)
		var flags byte
		if c.NotNull {
			flags |= 1 << 0
		}
		if c.IsPK {
			flags |= 1 << 1
		}
		if c.Generated {
			flags |= 1 << 2
		}
		buf = append(buf, flags)
		buf = binary.AppendUvarint(buf, uint64(c.PKPos))
		buf = appendString(buf, c.Default)
		buf = appendString(buf, c.ClockGroup)
		buf = append(buf, byte(c.Collation))
	}
	return buf
}

func readColumns(buf []byte) ([]CatalogColumn, []byte, error) {
	n, sz := binary.Uvarint(buf)
	if sz <= 0 {
		return nil, nil, ErrShortBuffer
	}
	buf = buf[sz:]
	out := make([]CatalogColumn, 0, n)
	for range n {
		var c CatalogColumn
		var err error
		if buf, err = readBytes16(buf, c.ID[:]); err != nil {
			return nil, nil, err
		}
		if c.Name, buf, err = readString(buf); err != nil {
			return nil, nil, err
		}
		ord, sz := binary.Uvarint(buf)
		if sz <= 0 {
			return nil, nil, ErrShortBuffer
		}
		buf = buf[sz:]
		c.Ordinal = int(ord)
		if c.Type, buf, err = readString(buf); err != nil {
			return nil, nil, err
		}
		if len(buf) < 1 {
			return nil, nil, ErrShortBuffer
		}
		flags := buf[0]
		buf = buf[1:]
		if flags&^byte(0x07) != 0 {
			return nil, nil, fmt.Errorf("crdt: column %q carries unsupported flags 0x%02x", c.Name, flags)
		}
		c.NotNull = flags&1 != 0
		c.IsPK = flags&(1<<1) != 0
		c.Generated = flags&(1<<2) != 0
		pos, sz := binary.Uvarint(buf)
		if sz <= 0 {
			return nil, nil, ErrShortBuffer
		}
		buf = buf[sz:]
		c.PKPos = int(pos)
		if c.Default, buf, err = readString(buf); err != nil {
			return nil, nil, err
		}
		if c.ClockGroup, buf, err = readString(buf); err != nil {
			return nil, nil, err
		}
		if len(buf) < 1 {
			return nil, nil, ErrShortBuffer
		}
		c.Collation = Collation(buf[0])
		buf = buf[1:]
		out = append(out, c)
	}
	return out, buf, nil
}

func opCarriesCoordinatedKey(op CatalogOp) bool {
	switch op.Kind {
	case OpCreateTable, OpAddUniqueKey:
		for _, k := range op.Keys {
			if k.Coordinated {
				return true
			}
		}
	}
	return false
}

func opCarriesPartialKey(op CatalogOp) bool {
	switch op.Kind {
	case OpCreateTable, OpAddUniqueKey:
		for _, k := range op.Keys {
			if k.Predicate.Root != nil {
				return true
			}
		}
	}
	return false
}

func appendKeys(buf []byte, keys []CatalogKey, versioned, partial bool) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendBytes16(buf, k.KeyID[:])
		if versioned {
			buf = appendBool(buf, k.Coordinated)
		}
		if partial {
			buf = appendPredicate(buf, k.Predicate)
		}
		buf = binary.AppendUvarint(buf, uint64(len(k.Members)))
		for _, m := range k.Members {
			buf = appendBytes16(buf, m.ColumnID[:])
			buf = binary.AppendUvarint(buf, uint64(m.Ordinal))
		}
	}
	return buf
}

func readKeys(buf []byte, versioned, partial bool) ([]CatalogKey, []byte, error) {
	n, sz := binary.Uvarint(buf)
	if sz <= 0 {
		return nil, nil, ErrShortBuffer
	}
	buf = buf[sz:]
	out := make([]CatalogKey, 0, n)
	for range n {
		var k CatalogKey
		var err error
		if buf, err = readBytes16(buf, k.KeyID[:]); err != nil {
			return nil, nil, err
		}
		if versioned {
			if k.Coordinated, buf, err = readBool(buf); err != nil {
				return nil, nil, err
			}
		}
		if partial {
			if k.Predicate, buf, err = readPredicate(buf); err != nil {
				return nil, nil, err
			}
		}
		mc, sz := binary.Uvarint(buf)
		if sz <= 0 {
			return nil, nil, ErrShortBuffer
		}
		buf = buf[sz:]
		k.Members = make([]CatalogKeyMember, 0, mc)
		for range mc {
			var m CatalogKeyMember
			if buf, err = readBytes16(buf, m.ColumnID[:]); err != nil {
				return nil, nil, err
			}
			ord, sz := binary.Uvarint(buf)
			if sz <= 0 {
				return nil, nil, ErrShortBuffer
			}
			buf = buf[sz:]
			m.Ordinal = int(ord)
			k.Members = append(k.Members, m)
		}
		out = append(out, k)
	}
	return out, buf, nil
}

// appendKeyMembers writes one key's member list (no surrounding count).
// AddUniqueKey carries one key per op, so the surrounding "count of
// keys" is implicit (=1).
func appendKeyMembers(buf []byte, keys []CatalogKey, versioned, partial bool) []byte {
	if len(keys) != 1 {
		panic(fmt.Sprintf("crdt: appendKeyMembers expects 1 key, got %d", len(keys)))
	}
	k := keys[0]
	if versioned {
		buf = appendBool(buf, k.Coordinated)
	}
	if partial {
		buf = appendPredicate(buf, k.Predicate)
	}
	buf = binary.AppendUvarint(buf, uint64(len(k.Members)))
	for _, m := range k.Members {
		buf = appendBytes16(buf, m.ColumnID[:])
		buf = binary.AppendUvarint(buf, uint64(m.Ordinal))
	}
	return buf
}

func readKeyMembers(buf []byte, keyID KeyID, versioned, partial bool) ([]CatalogKey, []byte, error) {
	var coordinated bool
	var predicate UniquePredicate
	var err error
	if versioned {
		if coordinated, buf, err = readBool(buf); err != nil {
			return nil, nil, err
		}
	}
	if partial {
		if predicate, buf, err = readPredicate(buf); err != nil {
			return nil, nil, err
		}
	}
	n, sz := binary.Uvarint(buf)
	if sz <= 0 {
		return nil, nil, ErrShortBuffer
	}
	buf = buf[sz:]
	members := make([]CatalogKeyMember, 0, n)
	for range n {
		var m CatalogKeyMember
		if buf, err = readBytes16(buf, m.ColumnID[:]); err != nil {
			return nil, nil, err
		}
		ord, sz := binary.Uvarint(buf)
		if sz <= 0 {
			return nil, nil, ErrShortBuffer
		}
		buf = buf[sz:]
		m.Ordinal = int(ord)
		members = append(members, m)
	}
	return []CatalogKey{{KeyID: keyID, Members: members, Coordinated: coordinated, Predicate: predicate}}, buf, nil
}

// appendBool encodes a bool as a single 0/1 byte.
func appendBool(buf []byte, v bool) []byte {
	if v {
		return append(buf, 1)
	}
	return append(buf, 0)
}

// readBool decodes a single 0/1 byte written by appendBool.
func readBool(buf []byte) (bool, []byte, error) {
	if len(buf) < 1 {
		return false, nil, ErrShortBuffer
	}
	return buf[0] != 0, buf[1:], nil
}
