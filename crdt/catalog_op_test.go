package crdt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func tabID(b byte) TableID  { var id TableID; id[0] = b; return id }
func colID(b byte) ColumnID { var id ColumnID; id[0] = b; return id }
func opKeyID(b byte) KeyID  { var id KeyID; id[0] = b; return id }

func TestCatalogOp_RoundTrip_CreateTable(t *testing.T) {
	op := CatalogOp{
		Kind:      OpCreateTable,
		TableID:   tabID(0x11),
		TableName: "users",
		Columns: []CatalogColumn{
			{ID: colID(0xA), Name: "id", Ordinal: 0, Type: "BLOB", NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row"},
			{ID: colID(0xB), Name: "email", Ordinal: 1, Type: "TEXT", ClockGroup: "row", Collation: CollNocase},
			{ID: colID(0xC), Name: "created", Ordinal: 2, Type: "INTEGER", Default: "(strftime('%s','now'))", ClockGroup: "row"},
		},
		Keys: []CatalogKey{
			{KeyID: KeyID{}, Members: []CatalogKeyMember{{ColumnID: colID(0xA), Ordinal: 0}}},
		},
	}
	roundTrip(t, op)
}

func TestCatalogOp_RoundTrip_CreateTablePartialKey(t *testing.T) {
	pred := &PredExpr{Op: PredAnd, Kids: []*PredExpr{
		{Op: PredIsNull, Col: colID(0xC)},
		{Op: PredEq, Col: colID(0xB), Lits: []ColValue{colText("active")}},
	}}
	op := CatalogOp{
		Kind:      OpCreateTable,
		TableID:   tabID(0x11),
		TableName: "users",
		Columns: []CatalogColumn{
			{ID: colID(0xA), Name: "id", Ordinal: 0, Type: "BLOB", NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row"},
			{ID: colID(0xB), Name: "email", Ordinal: 1, Type: "TEXT", NotNull: true, ClockGroup: "row"},
			{ID: colID(0xC), Name: "deleted_at", Ordinal: 2, Type: "INTEGER", ClockGroup: "row"},
		},
		Keys: []CatalogKey{
			{KeyID: KeyID{}, Members: []CatalogKeyMember{{ColumnID: colID(0xA), Ordinal: 0}}},
			{KeyID: opKeyID(0x55), Members: []CatalogKeyMember{{ColumnID: colID(0xB), Ordinal: 0}},
				Coordinated: true, Predicate: UniquePredicate{Root: pred}},
		},
	}
	roundTrip(t, op)
}

func TestCatalogOp_RoundTrip_AddUniqueKeyPartial(t *testing.T) {
	op := CatalogOp{
		Kind:    OpAddUniqueKey,
		TableID: tabID(0x33),
		KeyID:   opKeyID(0x77),
		Keys: []CatalogKey{{KeyID: opKeyID(0x77), Members: []CatalogKeyMember{{ColumnID: colID(0xB), Ordinal: 0}},
			Coordinated: true, Predicate: UniquePredicate{Root: &PredExpr{Op: PredIsNull, Col: colID(0xC)}}}},
	}
	roundTrip(t, op)
}

func TestCatalogOp_RoundTrip_AddColumn(t *testing.T) {
	op := CatalogOp{
		Kind:    OpAddColumn,
		TableID: tabID(0x22),
		Columns: []CatalogColumn{
			{ID: colID(0xD), Name: "phone", Ordinal: 3, Type: "TEXT", ClockGroup: "row"},
		},
	}
	roundTrip(t, op)
}

func TestCatalogOp_RoundTrip_RenameTable(t *testing.T) {
	roundTrip(t, CatalogOp{Kind: OpRenameTable, TableID: tabID(1), TableName: "people"})
}

func TestCatalogOp_RoundTrip_RenameColumn(t *testing.T) {
	roundTrip(t, CatalogOp{
		Kind: OpRenameColumn, TableID: tabID(1),
		ColumnID: colID(0xA), ColumnName: "email_addr",
	})
}

func TestCatalogOp_RoundTrip_DropColumn(t *testing.T) {
	roundTrip(t, CatalogOp{Kind: OpDropColumn, TableID: tabID(1), ColumnID: colID(0xB)})
}

func TestCatalogOp_RoundTrip_DropTable(t *testing.T) {
	roundTrip(t, CatalogOp{Kind: OpDropTable, TableID: tabID(1)})
}

func TestCatalogOp_RoundTrip_AddUniqueKey(t *testing.T) {
	roundTrip(t, CatalogOp{
		Kind:    OpAddUniqueKey,
		TableID: tabID(1),
		KeyID:   opKeyID(0x77),
		Keys: []CatalogKey{{KeyID: opKeyID(0x77), Members: []CatalogKeyMember{
			{ColumnID: colID(0xB), Ordinal: 0},
			{ColumnID: colID(0xC), Ordinal: 1},
		}}},
	})
}

func TestCatalogOp_RoundTrip_DropUniqueKey(t *testing.T) {
	roundTrip(t, CatalogOp{Kind: OpDropUniqueKey, TableID: tabID(1), KeyID: opKeyID(0x77)})
}

func TestCatalogOp_RoundTrip_OpaqueSQL(t *testing.T) {
	for _, kind := range []CatalogOpKind{
		OpCreateIndex, OpDropIndex,
		OpCreateView, OpDropView,
		OpCreateVirtualTable, OpDropVirtualTable,
		OpCreateTrigger, OpDropTrigger,
	} {
		roundTrip(t, CatalogOp{
			Kind:       kind,
			ObjectName: "myobj",
			RawSQL:     "CREATE INDEX foo ON bar(baz)",
		})
	}
}

func TestCatalogOp_RoundTrip_Bundle(t *testing.T) {
	roundTrip(t, CatalogOp{
		Kind: OpBundle,
		SubOps: []CatalogOp{
			{
				Kind: OpCreateTable, TableID: tabID(0x33), TableName: "child",
				Columns: []CatalogColumn{
					{ID: colID(0xA), Name: "id", Ordinal: 0, Type: "INTEGER", IsPK: true, PKPos: 1, ClockGroup: "row"},
				},
				Keys: []CatalogKey{{KeyID: KeyID{}, Members: []CatalogKeyMember{{ColumnID: colID(0xA)}}}},
			},
			{
				Kind:       OpCreateTrigger,
				ObjectName: "_syzy_fkcascade_child_0_d",
				RawSQL:     "CREATE TRIGGER _syzy_fkcascade_child_0_d BEFORE DELETE ON parent FOR EACH ROW BEGIN DELETE FROM child WHERE parent_id = old.id; END",
			},
		},
	})
}

func TestCatalogOp_NestedBundleRejected(t *testing.T) {
	inner := CatalogOp{Kind: OpBundle, SubOps: []CatalogOp{{Kind: OpDropTable, TableID: tabID(1)}}}
	outer := CatalogOp{Kind: OpBundle, SubOps: []CatalogOp{inner}}
	if _, err := EncodeCatalogOp(outer); err == nil {
		t.Errorf("encode nested bundle: want error")
	}
}

func TestCatalogOp_UnknownKindRejected(t *testing.T) {
	if _, err := EncodeCatalogOp(CatalogOp{Kind: OpUnknown}); err == nil {
		t.Errorf("encode Unknown: want error")
	}
	if _, err := DecodeCatalogOp([]byte{0xFE}); err == nil {
		t.Errorf("decode 0xFE: want error")
	}
}

func TestCatalogOp_UnknownColumnFlagsRejected(t *testing.T) {
	tid := tabID(1)
	cid := colID(1)
	var buf []byte
	buf = append(buf, byte(OpAddColumn))
	buf = appendBytes16(buf, tid[:])
	buf = binary.AppendUvarint(buf, 1)
	buf = appendBytes16(buf, cid[:])
	buf = appendString(buf, "a")
	buf = binary.AppendUvarint(buf, 0)
	buf = appendString(buf, "INT")
	buf = append(buf, 1<<6)
	buf = binary.AppendUvarint(buf, 0)
	buf = appendString(buf, "")
	buf = appendString(buf, "row")
	buf = append(buf, byte(CollBinary))
	if _, err := DecodeCatalogOp(buf); err == nil {
		t.Fatal("decode retired column flags: want error")
	}
	// Same body under a framed envelope must be rejected identically.
	framed := []byte{catalogOpSentinel}
	framed = binary.AppendUvarint(framed, catalogOpVersion)
	framed = binary.AppendUvarint(framed, uint64(OpAddColumn))
	framed = append(framed, buf[1:]...)
	if _, err := DecodeCatalogOp(framed); err == nil {
		t.Fatal("decode retired column flags (framed): want error")
	}
}

func TestCatalogOp_LegacyKeyLayoutDecodes(t *testing.T) {
	tid := tabID(0x11)
	kid := opKeyID(0x22)
	cid := colID(0x33)

	// Original key layout: no flags and no coordinated/predicate fields.
	var v1 []byte
	v1 = append(v1, byte(OpAddUniqueKey))
	v1 = append(v1, tid[:]...)
	v1 = append(v1, kid[:]...)
	v1 = binary.AppendUvarint(v1, 1)
	v1 = append(v1, cid[:]...)
	v1 = binary.AppendUvarint(v1, 0)

	op, err := DecodeCatalogOp(v1)
	if err != nil {
		t.Fatalf("decode original layout: %v", err)
	}
	if len(op.Keys) != 1 || op.Keys[0].Coordinated || op.Keys[0].Predicate.Root != nil {
		t.Fatalf("decoded original key = %+v", op.Keys)
	}
}

func TestCatalogOp_VersionGateFailsClosed(t *testing.T) {
	buf := []byte{catalogOpSentinel}
	buf = binary.AppendUvarint(buf, catalogOpMaxVersion+1)
	buf = binary.AppendUvarint(buf, uint64(OpDropTable))
	tid := tabID(1)
	buf = append(buf, tid[:]...)
	if _, err := DecodeCatalogOp(buf); !errors.Is(err, ErrCatalogOpVersion) {
		t.Fatalf("future version: got %v, want ErrCatalogOpVersion", err)
	}
}

func TestCatalogOp_ExtensionSkipped(t *testing.T) {
	op := CatalogOp{Kind: OpDropTable, TableID: tabID(1)}
	buf, err := EncodeCatalogOp(op)
	if err != nil {
		t.Fatal(err)
	}
	buf = binary.AppendUvarint(buf, 7) // unassigned advisory tag
	buf = binary.AppendUvarint(buf, 3)
	buf = append(buf, 0xDE, 0xAD, 0xBF)
	got, err := DecodeCatalogOp(buf)
	if err != nil {
		t.Fatalf("decode with extension: %v", err)
	}
	if !reflect.DeepEqual(got, op) {
		t.Fatalf("extension changed op: %+v", got)
	}
}

func TestCatalogOp_MalformedExtensionRejected(t *testing.T) {
	op := CatalogOp{Kind: OpDropTable, TableID: tabID(1)}
	full, err := EncodeCatalogOp(op)
	if err != nil {
		t.Fatal(err)
	}
	for name, tail := range map[string][]byte{
		"zero tag":      {0x00},
		"truncated len": {0x01},
		"short value":   {0x01, 0x05, 0xAA},
	} {
		if _, err := DecodeCatalogOp(append(append([]byte{}, full...), tail...)); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestCatalogOp_TruncatedRejected(t *testing.T) {
	op := CatalogOp{Kind: OpCreateTable, TableID: tabID(1), TableName: "t",
		Columns: []CatalogColumn{{ID: colID(1), Name: "a", Ordinal: 0, Type: "INT", IsPK: true, PKPos: 1, ClockGroup: "row"}},
		Keys:    []CatalogKey{{KeyID: KeyID{}, Members: []CatalogKeyMember{{ColumnID: colID(1)}}}},
	}
	full, err := EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for i := 1; i < len(full); i++ {
		if _, err := DecodeCatalogOp(full[:i]); err == nil {
			t.Errorf("decode of truncated[%d] succeeded; want error", i)
		}
	}
}

func TestCatalogOp_TrailingBytesRejected(t *testing.T) {
	op := CatalogOp{Kind: OpDropTable, TableID: tabID(1)}
	full, _ := EncodeCatalogOp(op)
	full = append(full, 0xFF)
	if _, err := DecodeCatalogOp(full); err == nil {
		t.Errorf("trailing byte: want error")
	}
}

func TestCatalogOp_DeterministicEncoding(t *testing.T) {
	op := CatalogOp{Kind: OpCreateTable, TableID: tabID(1), TableName: "users",
		Columns: []CatalogColumn{
			{ID: colID(1), Name: "id", Ordinal: 0, Type: "BLOB", NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row"},
		},
		Keys: []CatalogKey{{KeyID: KeyID{}, Members: []CatalogKeyMember{{ColumnID: colID(1)}}}},
	}
	a, _ := EncodeCatalogOp(op)
	b, _ := EncodeCatalogOp(op)
	if !bytes.Equal(a, b) {
		t.Errorf("encoding not deterministic: %x vs %x", a, b)
	}
}

func roundTrip(t *testing.T, op CatalogOp) {
	t.Helper()
	encoded, err := EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("encode %s: %v", op.Kind, err)
	}
	got, err := DecodeCatalogOp(encoded)
	if err != nil {
		t.Fatalf("decode %s: %v", op.Kind, err)
	}
	if !reflect.DeepEqual(op, got) {
		t.Errorf("roundtrip %s mismatch:\n  want %+v\n  got  %+v", op.Kind, op, got)
	}
}
