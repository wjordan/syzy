package syncer

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// touchInsert hand-crafts one C-side touch-journal INSERT record:
// op, oldRowID, newRowID, db, table, ncol, values.
func touchInsert(table string, rowid int64, vals ...crdt.ColValue) []byte {
	var buf bytes.Buffer
	buf.WriteByte(sqliteInsert)
	var i64 [8]byte
	binary.BigEndian.PutUint64(i64[:], 0)
	buf.Write(i64[:])
	binary.BigEndian.PutUint64(i64[:], uint64(rowid))
	buf.Write(i64[:])
	writeShortBytes(&buf, []byte("main"))
	writeShortBytes(&buf, []byte(table))
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(vals)))
	buf.Write(n[:])
	for _, v := range vals {
		switch v.TypeTag {
		case crdt.ColInt:
			buf.WriteByte(1)
			buf.Write(v.Bytes)
		case crdt.ColText:
			buf.WriteByte(3)
			var l [4]byte
			binary.BigEndian.PutUint32(l[:], uint32(len(v.Bytes)))
			buf.Write(l[:])
			buf.Write(v.Bytes)
		default:
			panic("touchInsert: unsupported tag in test")
		}
	}
	return buf.Bytes()
}

func openTestConn(t *testing.T, path, ddl string) *sqlitebridge.Conn {
	t.Helper()
	c, err := sqlitebridge.Open(path, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Exec(ddl); err != nil {
		t.Fatalf("ddl: %v", err)
	}
	return c
}

func intVal(v int64) crdt.ColValue {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return crdt.ColValue{TypeTag: crdt.ColInt, Bytes: b}
}

func textVal(s string) crdt.ColValue {
	return crdt.ColValue{TypeTag: crdt.ColText, Bytes: []byte(s)}
}

// TestBuildRecordEvidenceAcrossDropAddMigration pins the fix for the
// positional-aliasing corruption: a touch record captured at the
// genesis schema (id, keep, dead), still undrained when DROP COLUMN
// dead + ADD COLUMN fresh land (fresh inherits dead's position), must
// materialize its third value under dead's ColumnID — NOT fresh's,
// which is where a current-layout zip puts it and how a dropped
// column's 0/1 ends up as a live column's value on every replica.
func TestBuildRecordEvidenceAcrossDropAddMigration(t *testing.T) {
	dir := t.TempDir()
	app := openTestConn(t, filepath.Join(dir, "app.db"),
		`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, keep TEXT, dead INT)`)
	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	defer sc.Close()
	cat, err := catalog.SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	tab, _ := cat.Table("t")
	deadID := mustColID(t, tab, "dead")
	keepID := mustColID(t, tab, "keep")

	// The record is captured at genesis schema_seq 0 → epoch 1.
	rec := touchInsert("t", 1, intVal(1), textVal("a"), intVal(7))

	// Migration lands before the record drains: DROP dead (seq 1), ADD
	// fresh at the freed ordinal (seq 2).
	if err := sc.WithTx(func(tx *metadata.Tx) error {
		return tx.ApplyCatalogOp(crdt.CatalogOp{Kind: crdt.OpDropColumn, TableID: tab.ID, ColumnID: deadID}, 1)
	}); err != nil {
		t.Fatalf("drop: %v", err)
	}
	freshID := catalog.AllocColumnID()
	if err := sc.WithTx(func(tx *metadata.Tx) error {
		return tx.ApplyCatalogOp(crdt.CatalogOp{
			Kind: crdt.OpAddColumn, TableID: tab.ID,
			Columns: []crdt.CatalogColumn{{ID: freshID, Name: "fresh", Ordinal: 2}},
		}, 2)
	}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := sc.SetSchemaSeq(2); err != nil {
		t.Fatalf("set seq: %v", err)
	}
	if err := cat.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}

	sink := &MetaSink{cat: cat}
	if err := sink.buildRecordEvidence(rec, 1); err != nil {
		t.Fatalf("buildRecordEvidence: %v", err)
	}
	if len(sink.evidence) != 1 {
		t.Fatalf("evidence = %d entries, want 1", len(sink.evidence))
	}
	ev := sink.evidence[0]
	if ev.op != evOpInsert {
		t.Fatalf("op = %d, want insert", ev.op)
	}
	byCol := map[crdt.ColumnID]crdt.ColValue{}
	for _, v := range ev.image {
		byCol[v.Column] = v
	}
	if v, ok := byCol[deadID]; !ok || int64(binary.BigEndian.Uint64(v.Bytes)) != 7 {
		t.Errorf("dead's value not labeled with dead's ColumnID: %+v", byCol)
	}
	if _, ok := byCol[freshID]; ok {
		t.Error("value aliased into fresh's ColumnID — the corruption this fix exists for")
	}
	if v, ok := byCol[keepID]; !ok || string(v.Bytes) != "a" {
		t.Errorf("keep mislabeled: %+v", byCol)
	}

	// A pre-stamp record (epoch 0) keeps the historical behavior:
	// current-layout zip, third value lands under fresh.
	legacy := &MetaSink{cat: cat}
	if err := legacy.buildRecordEvidence(rec, 0); err != nil {
		t.Fatalf("legacy decode: %v", err)
	}
	byCol = map[crdt.ColumnID]crdt.ColValue{}
	for _, v := range legacy.evidence[0].image {
		byCol[v.Column] = v
	}
	if _, ok := byCol[freshID]; !ok {
		t.Error("pre-stamp record must keep the current-layout decode")
	}
}

func mustColID(t *testing.T, tab *catalog.Table, name string) crdt.ColumnID {
	t.Helper()
	c, ok := tab.Column(name)
	if !ok {
		t.Fatalf("column %s missing", name)
	}
	return c.ID
}
