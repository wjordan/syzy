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
		pk, err := pkBlobTyped(cvs)
		if err != nil {
			t.Fatalf("typedPK: %v", err)
		}
		return pk
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
	pgPK, err := pkBlobTyped([]crdt.ColValue{intCV, textCV})
	if err != nil {
		t.Fatalf("pkBlobTyped: %v", err)
	}
	if !bytes.Equal(sqlitePK, pgPK) {
		t.Fatalf("PK identity diverges:\n sqlite %x\n pg     %x", sqlitePK, pgPK)
	}
}

func TestValidatePKBlob(t *testing.T) {
	idCol := &colInfo{name: "id", typeName: "bigint", cid: crdt.ColumnID{1}}
	nameCol := &colInfo{name: "name", typeName: "text", cid: crdt.ColumnID{2}}
	ti := &tableInfo{name: "items", pk: []*colInfo{idCol, nameCol}}
	id, err := encodeColValue(idCol.cid, idCol.typeName, []byte("42"))
	if err != nil {
		t.Fatal(err)
	}
	name, err := encodeColValue(nameCol.cid, nameCol.typeName, []byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := pkBlobTyped([]crdt.ColValue{id, name})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		pk   crdt.PKBlob
		ok   bool
	}{
		{name: "valid", pk: valid, ok: true},
		{name: "truncated", pk: valid[:len(valid)-1]},
		{name: "missing member", pk: mustPKBlob(t, id)},
		{name: "wrong order", pk: mustPKBlob(t, name, id)},
		{name: "extra member", pk: mustPKBlob(t, id, name, name)},
		{name: "wrong type", pk: mustPKBlob(t, crdt.ColValue{Column: idCol.cid, TypeTag: crdt.ColText, Bytes: []byte("42")}, name)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePKBlob(ti, tc.pk)
			if tc.ok && err != nil {
				t.Fatalf("validatePKBlob: %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("validatePKBlob accepted malformed input")
			}
		})
	}
}

func mustPKBlob(t testing.TB, values ...crdt.ColValue) crdt.PKBlob {
	t.Helper()
	pk, err := pkBlobTyped(values)
	if err != nil {
		t.Fatalf("pkBlobTyped: %v", err)
	}
	return pk
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
