package postgres

import (
	"bytes"
	"testing"

	"github.com/wjordan/syzy/crdt"
	sqlitecat "github.com/wjordan/syzy/internal/sqlitecatalog"
)

// typedPK builds the canonical typed PKBlob for a table's PK from text values
// — the test-side twin of decodeRawTuple's PK path.
func typedPK(t testing.TB, e *Engine, table string, texts ...string) crdt.PKBlob {
	t.Helper()
	for _, ti := range e.cat.byID {
		if ti.name != table {
			continue
		}
		if len(texts) != len(ti.pk) {
			t.Fatalf("typedPK: %d values for %d pk columns", len(texts), len(ti.pk))
		}
		cvs := make([]crdt.ColValue, len(texts))
		for i, txt := range texts {
			cv, err := encodeColValue(ti.pk[i].cid, ti.pk[i].typeName, []byte(txt))
			if err != nil {
				t.Fatalf("typedPK: %v", err)
			}
			cvs[i] = cv
		}
		return pkBlobTyped(cvs)
	}
	t.Fatalf("typedPK: table %s not in catalog", table)
	return nil
}

// TestPKBlobMatchesSQLiteCatalog pins the cross-engine identity contract: the
// PG engine's pkBlobTyped and the SQLite side's catalog.EncodePKFromSlice must
// produce byte-identical PK blobs for the same logical row.
func TestPKBlobMatchesSQLiteCatalog(t *testing.T) {
	idCol := crdt.ColumnID{0xAA, 1}
	nameCol := crdt.ColumnID{0xBB, 2}
	tab := &sqlitecat.Table{
		Columns: []sqlitecat.Column{
			{ID: idCol, Name: "id"},
			{ID: nameCol, Name: "name"},
		},
		PK: []sqlitecat.Column{{ID: idCol, Name: "id"}, {ID: nameCol, Name: "name"}},
	}
	intCV, err := encodeColValue(idCol, "bigint", []byte("42"))
	if err != nil {
		t.Fatal(err)
	}
	textCV, err := encodeColValue(nameCol, "text", []byte("héllo"))
	if err != nil {
		t.Fatal(err)
	}
	sqlitePK, err := tab.EncodePKFromSlice(nil, []crdt.ColValue{intCV, textCV})
	if err != nil {
		t.Fatalf("EncodePKFromSlice: %v", err)
	}
	pgPK := pkBlobTyped([]crdt.ColValue{intCV, textCV})
	if !bytes.Equal(sqlitePK, pgPK) {
		t.Fatalf("PK identity diverges:\n sqlite %x\n pg     %x", sqlitePK, pgPK)
	}
}

// TestColValueRoundTrip: text → canonical typed → text survives per class.
func TestColValueRoundTrip(t *testing.T) {
	cid := crdt.ColumnID{1}
	cases := []struct{ typeName, in, out string }{
		{"bigint", "42", "42"},
		{"bigint", "-7", "-7"},
		{"integer", "0", "0"},
		{"boolean", "t", "1"},
		{"boolean", "f", "0"},
		{"double precision", "1.5", "1.5"},
		{"double precision", "-0.25", "-0.25"},
		{"bytea", `\x00ff`, `\x00ff`},
		{"text", "héllo", "héllo"},
		{"timestamp with time zone", "2026-01-01 00:00:00+00", "2026-01-01 00:00:00+00"},
	}
	for _, c := range cases {
		cv, err := encodeColValue(cid, c.typeName, []byte(c.in))
		if err != nil {
			t.Fatalf("%s %q: %v", c.typeName, c.in, err)
		}
		got, err := colValueText(cv)
		if err != nil {
			t.Fatalf("%s %q text: %v", c.typeName, c.in, err)
		}
		if got != c.out {
			t.Fatalf("%s: %q -> %q, want %q", c.typeName, c.in, got, c.out)
		}
	}
	if _, err := encodeColValue(cid, "bigint", []byte("nope")); err == nil {
		t.Fatal("malformed int must error")
	}
}
