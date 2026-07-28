package metadata

import (
	"bytes"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

func TestLocalDDLIntent_RoundTrip(t *testing.T) {
	intent := LocalDDLIntent{
		StartedAtUs: 12345,
		SchemaSeq:   7,
		ParentSeq:   6,
		CatalogOp:   []byte{0x01, 0x02, 0x03, 0x04, 0x05},
		RawSQL:      "CREATE TABLE t (id BLOB PRIMARY KEY)",
	}
	buf := EncodeLocalDDL(intent)
	if IntentKindOf(buf) != IntentLocalDDL {
		t.Fatalf("kind tag = %d", IntentKindOf(buf))
	}
	got, err := DecodeLocalDDL(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.StartedAtUs != intent.StartedAtUs ||
		got.SchemaSeq != intent.SchemaSeq ||
		got.ParentSeq != intent.ParentSeq ||
		got.RawSQL != intent.RawSQL ||
		!bytes.Equal(got.CatalogOp, intent.CatalogOp) {
		t.Errorf("round-trip mismatch: got %+v", got)
	}
}

func TestLocalDDLIntent_EmptyRawSQL(t *testing.T) {
	intent := LocalDDLIntent{SchemaSeq: 1, CatalogOp: []byte("op")}
	buf := EncodeLocalDDL(intent)
	got, err := DecodeLocalDDL(buf)
	if err != nil || got.RawSQL != "" {
		t.Errorf("empty raw_sql roundtrip: %+v err=%v", got, err)
	}
}

func TestLocalDDLIntent_RejectWrongKind(t *testing.T) {
	if _, err := DecodeLocalDDL([]byte{byte(IntentClone)}); err == nil {
		t.Error("expected error for non-LocalDDL kind")
	}
	if _, err := DecodeLocalDDL(nil); err == nil {
		t.Error("expected error for empty buffer")
	}
}

func TestStore_IntentRoundTrip(t *testing.T) {
	sc, _ := openTemp(t)
	a, b := crdt.Origin(0xA), crdt.Origin(0xB)
	if _, ok, err := sc.GetOriginIntent(a); err != nil || ok {
		t.Errorf("fresh GetOriginIntent = (_, %v, %v); want (_, false, nil)", ok, err)
	}
	intent := LocalDDLIntent{SchemaSeq: 5, CatalogOp: []byte("op"), RawSQL: "DDL"}
	if err := sc.SetOriginIntent(a, EncodeLocalDDL(intent)); err != nil {
		t.Fatalf("SetOriginIntent: %v", err)
	}
	other := LocalDDLIntent{SchemaSeq: 7, CatalogOp: []byte("op2"), RawSQL: "DDL2"}
	if err := sc.SetOriginIntent(b, EncodeLocalDDL(other)); err != nil {
		t.Fatalf("SetOriginIntent(b): %v", err)
	}
	buf, ok, err := sc.GetOriginIntent(a)
	if err != nil || !ok {
		t.Fatalf("GetOriginIntent: ok=%v err=%v", ok, err)
	}
	got, err := DecodeLocalDDL(buf)
	if err != nil || got.SchemaSeq != 5 || got.RawSQL != "DDL" {
		t.Errorf("decoded intent = %+v err=%v", got, err)
	}
	all, err := sc.ListIntents()
	if err != nil || len(all) != 2 {
		t.Fatalf("ListIntents = %d intents, err=%v; want 2", len(all), err)
	}
	if all[0].Origin != a || all[1].Origin != b {
		t.Errorf("ListIntents origins = %v, %v; want %v, %v", all[0].Origin, all[1].Origin, a, b)
	}
	// Clearing a's slot must not touch b's.
	if err := sc.ClearOriginIntent(a); err != nil {
		t.Fatalf("ClearOriginIntent: %v", err)
	}
	if _, ok, _ := sc.GetOriginIntent(a); ok {
		t.Error("ClearOriginIntent did not remove intent")
	}
	if _, ok, _ := sc.GetOriginIntent(b); !ok {
		t.Error("ClearOriginIntent(a) destroyed b's intent")
	}
	if err := sc.WithTx(func(tx *Tx) error { return tx.ClearAllIntents() }); err != nil {
		t.Fatalf("ClearAllIntents: %v", err)
	}
	if all, _ := sc.ListIntents(); len(all) != 0 {
		t.Errorf("ListIntents after ClearAllIntents = %d; want 0", len(all))
	}
}
