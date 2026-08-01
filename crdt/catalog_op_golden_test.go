package crdt

// Golden vectors for the CatalogOp durable format. Legacy vectors were
// captured from the pre-envelope encoder and must decode forever
// (syzy_schema_event holds them durably). Framed vectors lock the
// current encoder's bytes; a mismatch means the durable format drifted.

import (
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"testing"
)

type catalogOpGolden struct {
	name   string
	op     CatalogOp
	legacy string // pre-envelope encoder output ("" where the op predates no legacy shape)
	framed string // current encoder output
}

func catalogOpGoldens() []catalogOpGolden {
	return []catalogOpGolden{
		{
			name: "create_table",
			op: CatalogOp{
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
			},
			legacy: "0111000000000000000000000000000000057573657273030a0000000000000000000000000000000269640004424c4f4203010003726f77000b00000000000000000000000000000005656d61696c01045445585400000003726f77010c00000000000000000000000000000007637265617465640207494e5445474552000016287374726674696d6528272573272c276e6f7727292903726f77000100000000000000000000000000000000010a0000000000000000000000000000000000",
			framed: "c0040111000000000000000000000000000000057573657273030a0000000000000000000000000000000269640004424c4f4203010003726f77000b00000000000000000000000000000005656d61696c01045445585400000003726f77010c00000000000000000000000000000007637265617465640207494e5445474552000016287374726674696d6528272573272c276e6f7727292903726f770001000000000000000000000000000000000000010a0000000000000000000000000000000000",
		},
		{
			name: "create_table_coordinated_partial",
			op: CatalogOp{
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
						Coordinated: true, Predicate: UniquePredicate{Root: &PredExpr{Op: PredAnd, Kids: []*PredExpr{
							{Op: PredIsNull, Col: colID(0xC)},
							{Op: PredEq, Col: colID(0xB), Lits: []ColValue{colText("active")}},
						}}}},
				},
			},
			legacy: "c111000000000000000000000000000000057573657273030a0000000000000000000000000000000269640004424c4f4203010003726f77000b00000000000000000000000000000005656d61696c01045445585401000003726f77000c0000000000000000000000000000000a64656c657465645f61740207494e544547455200000003726f770002000000000000000000000000000000000000010a000000000000000000000000000000005500000000000000000000000000000001010b02010c000000000000000000000000000000030b000000000000000000000000000000030661637469766500010b0000000000000000000000000000000000",
			framed: "c0040111000000000000000000000000000000057573657273030a0000000000000000000000000000000269640004424c4f4203010003726f77000b00000000000000000000000000000005656d61696c01045445585401000003726f77000c0000000000000000000000000000000a64656c657465645f61740207494e544547455200000003726f770002000000000000000000000000000000000000010a000000000000000000000000000000005500000000000000000000000000000001010b02010c000000000000000000000000000000030b000000000000000000000000000000030661637469766500010b0000000000000000000000000000000000",
		},
		{
			name: "add_column",
			op: CatalogOp{
				Kind:    OpAddColumn,
				TableID: tabID(0x22),
				Columns: []CatalogColumn{
					{ID: colID(0xD), Name: "phone", Ordinal: 3, Type: "TEXT", ClockGroup: "row"},
				},
			},
			legacy: "0222000000000000000000000000000000010d0000000000000000000000000000000570686f6e6503045445585400000003726f7700",
			framed: "c0040222000000000000000000000000000000010d0000000000000000000000000000000570686f6e6503045445585400000003726f7700",
		},
		{
			name:   "rename_table",
			op:     CatalogOp{Kind: OpRenameTable, TableID: tabID(1), TableName: "people"},
			legacy: "03010000000000000000000000000000000670656f706c65",
			framed: "c00403010000000000000000000000000000000670656f706c65",
		},
		{
			name:   "rename_column",
			op:     CatalogOp{Kind: OpRenameColumn, TableID: tabID(1), ColumnID: colID(0xA), ColumnName: "email_addr"},
			legacy: "04010000000000000000000000000000000a0000000000000000000000000000000a656d61696c5f61646472",
			framed: "c00404010000000000000000000000000000000a0000000000000000000000000000000a656d61696c5f61646472",
		},
		{
			name:   "drop_column",
			op:     CatalogOp{Kind: OpDropColumn, TableID: tabID(1), ColumnID: colID(0xB)},
			legacy: "05010000000000000000000000000000000b000000000000000000000000000000",
			framed: "c00405010000000000000000000000000000000b000000000000000000000000000000",
		},
		{
			name:   "drop_table",
			op:     CatalogOp{Kind: OpDropTable, TableID: tabID(1)},
			legacy: "0601000000000000000000000000000000",
			framed: "c0040601000000000000000000000000000000",
		},
		{
			name:   "set_clock_group",
			op:     CatalogOp{Kind: OpSetClockGroup, TableID: tabID(1), ClockGroup: "cell"},
			legacy: "12010000000000000000000000000000000463656c6c",
			framed: "c00412010000000000000000000000000000000463656c6c",
		},
		{
			name: "add_unique_key",
			op: CatalogOp{
				Kind:    OpAddUniqueKey,
				TableID: tabID(1),
				KeyID:   opKeyID(0x77),
				Keys: []CatalogKey{{KeyID: opKeyID(0x77), Members: []CatalogKeyMember{
					{ColumnID: colID(0xB), Ordinal: 0},
					{ColumnID: colID(0xC), Ordinal: 1},
				}}},
			},
			legacy: "070100000000000000000000000000000077000000000000000000000000000000020b000000000000000000000000000000000c00000000000000000000000000000001",
			framed: "c0040701000000000000000000000000000000770000000000000000000000000000000000020b000000000000000000000000000000000c00000000000000000000000000000001",
		},
		{
			name: "add_unique_key_coordinated",
			op: CatalogOp{
				Kind:    OpAddUniqueKey,
				TableID: tabID(0x33),
				KeyID:   opKeyID(0x77),
				Keys: []CatalogKey{{KeyID: opKeyID(0x77), Members: []CatalogKeyMember{{ColumnID: colID(0xB), Ordinal: 0}},
					Coordinated: true}},
			},
			legacy: "87330000000000000000000000000000007700000000000000000000000000000001010b00000000000000000000000000000000",
			framed: "c0040733000000000000000000000000000000770000000000000000000000000000000100010b00000000000000000000000000000000",
		},
		{
			name: "add_unique_key_partial",
			op: CatalogOp{
				Kind:    OpAddUniqueKey,
				TableID: tabID(0x33),
				KeyID:   opKeyID(0x77),
				Keys: []CatalogKey{{KeyID: opKeyID(0x77), Members: []CatalogKeyMember{{ColumnID: colID(0xB), Ordinal: 0}},
					Coordinated: true, Predicate: UniquePredicate{Root: &PredExpr{Op: PredIsNull, Col: colID(0xC)}}}},
			},
			legacy: "c733000000000000000000000000000000770000000000000000000000000000000101010c000000000000000000000000000000010b00000000000000000000000000000000",
			framed: "c0040733000000000000000000000000000000770000000000000000000000000000000101010c000000000000000000000000000000010b00000000000000000000000000000000",
		},
		{
			name:   "drop_unique_key",
			op:     CatalogOp{Kind: OpDropUniqueKey, TableID: tabID(1), KeyID: opKeyID(0x77)},
			legacy: "080100000000000000000000000000000077000000000000000000000000000000",
			framed: "c004080100000000000000000000000000000077000000000000000000000000000000",
		},
		{
			name:   "create_index",
			op:     CatalogOp{Kind: OpCreateIndex, ObjectName: "myobj", RawSQL: "CREATE INDEX foo ON bar(baz)"},
			legacy: "09056d796f626a1c43524541544520494e44455820666f6f204f4e206261722862617a29",
			framed: "c00409056d796f626a1c43524541544520494e44455820666f6f204f4e206261722862617a29",
		},
		{
			name:   "create_trigger",
			op:     CatalogOp{Kind: OpCreateTrigger, TableID: tabID(2), ObjectName: "trg", RawSQL: "CREATE TRIGGER trg ..."},
			legacy: "0f020000000000000000000000000000000374726716435245415445205452494747455220747267202e2e2e",
			framed: "c0040f020000000000000000000000000000000374726716435245415445205452494747455220747267202e2e2e",
		},
		{
			name: "bundle",
			op: CatalogOp{
				Kind: OpBundle,
				SubOps: []CatalogOp{
					{Kind: OpDropTable, TableID: tabID(9)},
					{Kind: OpDropTrigger, TableID: tabID(9), ObjectName: "trg", RawSQL: "DROP TRIGGER trg"},
				},
			},
			legacy: "1102110609000000000000000000000000000000261009000000000000000000000000000000037472671044524f50205452494747455220747267",
			framed: "c004110213c004060900000000000000000000000000000028c0041009000000000000000000000000000000037472671044524f50205452494747455220747267",
		},
	}
}

// TestCatalogOp_LegacyGolden locks the decode-only legacy format: every
// vector captured from the pre-envelope encoder must keep decoding to
// the same op.
func TestCatalogOp_LegacyGolden(t *testing.T) {
	for _, g := range catalogOpGoldens() {
		raw, err := hex.DecodeString(g.legacy)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", g.name, err)
		}
		got, err := DecodeCatalogOp(raw)
		if err != nil {
			t.Fatalf("%s: decode legacy: %v", g.name, err)
		}
		if !reflect.DeepEqual(got, g.op) {
			t.Errorf("%s: legacy decode mismatch:\n want %+v\n got  %+v", g.name, g.op, got)
		}
	}
}

// TestCatalogOp_FramedGolden locks the current encoder's bytes for the
// same op set. A diff here is a durable-format change — update the
// vectors only as part of a deliberate format rev.
func TestCatalogOp_FramedGolden(t *testing.T) {
	for _, g := range catalogOpGoldens() {
		enc, err := EncodeCatalogOp(g.op)
		if err != nil {
			t.Fatalf("%s: encode: %v", g.name, err)
		}
		if got := hex.EncodeToString(enc); got != g.framed {
			t.Errorf("%s: framed encoding drifted:\n want %s\n got  %s", g.name, g.framed, got)
		}
		dec, err := DecodeCatalogOp(enc)
		if err != nil {
			t.Fatalf("%s: decode framed: %v", g.name, err)
		}
		if !reflect.DeepEqual(dec, g.op) {
			t.Errorf("%s: framed roundtrip mismatch:\n want %+v\n got  %+v", g.name, g.op, dec)
		}
	}
}

// Bundle sub-op decode dispatches per sub-op, so a legacy bundle frame
// carrying framed sub-op bytes decodes too (arises only in hand-built
// data, but falls out of the construction and is locked here).
func TestCatalogOp_MixedBundleDecodes(t *testing.T) {
	sub, err := EncodeCatalogOp(CatalogOp{Kind: OpDropTable, TableID: tabID(9)})
	if err != nil {
		t.Fatal(err)
	}
	buf := []byte{byte(OpBundle)} // legacy bundle frame
	buf = binary.AppendUvarint(buf, 1)
	buf = binary.AppendUvarint(buf, uint64(len(sub)))
	buf = append(buf, sub...)
	op, err := DecodeCatalogOp(buf)
	if err != nil {
		t.Fatalf("mixed bundle: %v", err)
	}
	if len(op.SubOps) != 1 || op.SubOps[0].Kind != OpDropTable {
		t.Fatalf("mixed bundle decoded to %+v", op)
	}
}
