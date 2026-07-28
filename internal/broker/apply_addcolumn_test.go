package broker

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// TestApplyInsertPartialImageUsesColumnDefault pins the schema-evolution fix: a
// record whose Image predates an ADD COLUMN must apply with the absent column's
// DECLARED DEFAULT, not a stale value the cached statement carried over from a
// prior insert. This is the wg_peers poison-record case in miniature — `retired`
// stands in for the column added by a later migration.
func TestApplyInsertPartialImageUsesColumnDefault(t *testing.T) {
	t.Parallel()
	f := newApplierSchema(t, 1, nil,
		`CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT, retired INTEGER NOT NULL DEFAULT 7)`)
	idCol := f.tab.PK[0].ID
	nCol, _ := f.tab.Column("n")
	retCol, _ := f.tab.Column("retired")
	ctx := context.Background()

	// 1) Full-image insert that sets retired to a non-default 99. This leaves 99
	//    in the cached statement's retired placeholder — the stale value the old
	//    code would leak into the next, partial, insert.
	pk1, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x01})})
	full := crdt.Insert{Table: f.tab.ID, PK: pk1, CL: 1, Image: []crdt.ColValue{
		blobCol(idCol, []byte{0x01}), textCol(nCol.ID, "a"), intCol(retCol.ID, 99),
	}}
	csFull, err := crdt.Build(crdt.Dot{Origin: 7, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: 7}, nil, testCluster, []crdt.Record{full})
	if err != nil {
		t.Fatalf("build full: %v", err)
	}
	if err := f.br.applyPayload(ctx, csFull.Encoded()); err != nil {
		t.Fatalf("apply full: %v", err)
	}

	// 2) Partial-image insert (id, n only) — a row journaled before `retired`
	//    existed. Pre-fix this killed the drainer at materialize; here it must
	//    apply with retired = its default.
	pk2, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x02})})
	partial := crdt.Insert{Table: f.tab.ID, PK: pk2, CL: 1, Image: []crdt.ColValue{
		blobCol(idCol, []byte{0x02}), textCol(nCol.ID, "b"),
	}}
	csPartial, err := crdt.Build(crdt.Dot{Origin: 7, Seq: 2},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 2000}, Origin: 7}, nil, testCluster, []crdt.Record{partial})
	if err != nil {
		t.Fatalf("build partial: %v", err)
	}
	if err := f.br.applyPayload(ctx, csPartial.Encoded()); err != nil {
		t.Fatalf("apply partial: %v", err)
	}

	if got := readRetired(t, f.app, []byte{0x02}); got != 7 {
		t.Fatalf("partial-image row retired = %d; want default 7 (stale-bind bug yields 99)", got)
	}
	if got := readNCol(t, f.app, []byte{0x02}); got != "b" {
		t.Fatalf("partial-image row n = %q; want b", got)
	}
	if got := readRetired(t, f.app, []byte{0x01}); got != 99 {
		t.Fatalf("full-image row retired = %d; want 99", got)
	}
}

func readRetired(t *testing.T, app *sqlitebridge.Conn, id []byte) int64 {
	t.Helper()
	stmt, _, err := app.Prepare(`SELECT retired FROM event WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !hasRow {
		t.Fatalf("no row for id %x", id)
	}
	return stmt.ColumnInt64(0)
}
