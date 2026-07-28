package broker

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// TestApplyBlobPatch_CellClockBaselineWins seeds a cell_clock override
// on the body column at a high stamp, then sends a blob_patch at a
// dominated stamp. The patch must lose against the cell baseline (it
// would have won against the row baseline alone) — proving applyBlobPatch
// consults rs.EffectiveStamp(col, ...) rather than rs.Base.
func TestApplyBlobPatch_CellClockBaselineWins(t *testing.T) {
	t.Parallel()
	f := newBlobApplier(t, 1)

	// Seed: INSERT row id=1 with body = 16 bytes of 0xAA at low stamp.
	src := crdt.Origin(7)
	stampBase := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: src}
	idCol := f.tab.PK[0].ID
	bodyCol, _ := f.tab.Column("body")
	body0 := bytes.Repeat([]byte{0xAA}, 16)
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: intCol(idCol, 1)})
	rec := crdt.Insert{
		Table: f.tab.ID, PK: pk, CL: 1,
		Image: []crdt.ColValue{blobCol(bodyCol.ID, body0)},
	}
	cs, err := crdt.Build(crdt.Dot{Origin: src, Seq: 1}, stampBase, nil, testCluster, []crdt.Record{rec})
	if err != nil {
		t.Fatalf("Build seed: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("apply seed: %v", err)
	}

	// Inject a cell_clock override at stampHigh; afterwards a blob_patch
	// at stampMid must lose because it does not strictly dominate the
	// cell baseline.
	stampHigh := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	f.cache.PutCellStamp(f.tab.ID, pk, bodyCol.ID, stampHigh)

	stampMid := crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: src}
	patchBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	bp := crdt.BlobPatch{
		Table: f.tab.ID, PK: pk, Col: bodyCol.ID, CL: 1,
		Ranges: []crdt.BlobPatchRange{{Offset: 4, Bytes: patchBytes}},
	}
	cs2, err := crdt.Build(crdt.Dot{Origin: src, Seq: 2}, stampMid, nil, testCluster, []crdt.Record{bp})
	if err != nil {
		t.Fatalf("Build patch: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs2.Encoded()); err != nil {
		t.Fatalf("apply patch: %v", err)
	}

	got := readBodyBlob(t, f.app, 1)
	if !bytes.Equal(got, body0) {
		t.Errorf("body = %x; want unchanged %x (cell_clock baseline should dominate the patch)", got, body0)
	}
}

func TestApplyBlobPatch_SeedsBlobNotNullPlaceholder(t *testing.T) {
	t.Parallel()
	f := newBlobApplierWithSchema(t, 1, `CREATE TABLE blobrow (
		id    INTEGER PRIMARY KEY,
		body  BLOB NOT NULL
	)`)
	src := crdt.Origin(7)
	idCol := f.tab.PK[0].ID
	bodyCol, _ := f.tab.Column("body")
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: intCol(idCol, 1)})
	patch := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	cs, err := crdt.Build(
		crdt.Dot{Origin: src, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: src},
		nil,
		testCluster,
		[]crdt.Record{crdt.BlobPatch{
			Table: f.tab.ID, PK: pk, CL: 1, Col: bodyCol.ID,
			Ranges: []crdt.BlobPatchRange{{Offset: 0, Bytes: patch}},
		}},
	)
	if err != nil {
		t.Fatalf("Build patch: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("apply patch: %v", err)
	}
	if got := readBodyBlob(t, f.app, 1); !bytes.Equal(got, patch) {
		t.Errorf("body = % x; want % x", got, patch)
	}
}

// blobApplier mirrors uniqueApplier with a (id INTEGER PRIMARY KEY,
// body BLOB) schema for blob_patch tests.
type blobApplier struct {
	app   *sqlitebridge.Conn
	cat   *catalog.Catalog
	cache *nodestate.Cache
	br    *Broker
	tab   *catalog.Table
}

const blobSchema = `CREATE TABLE blobrow (
	id   INTEGER PRIMARY KEY,
	body BLOB
)`

func newBlobApplier(t testing.TB, origin crdt.Origin) *blobApplier {
	return newBlobApplierWithSchema(t, origin, blobSchema)
}

func newBlobApplierWithSchema(t testing.TB, origin crdt.Origin, schema string) *blobApplier {
	t.Helper()
	dir := t.TempDir()
	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Exec(`PRAGMA journal_mode = WAL; ` + schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	if err := sc.SetClusterID(testCluster); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(origin); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}
	cat, err := catalog.SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("SeedFromSchema: %v", err)
	}
	cache := nodestate.New(origin)
	br, err := New(Config{
		AppApply: app,
		Meta:     sc,
		Catalog:  cat,
		Cache:    cache,
	})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	tab, _ := cat.Table("blobrow")
	return &blobApplier{app: app, cat: cat, cache: cache, br: br, tab: tab}
}

func readBodyBlob(t *testing.T, app *sqlitebridge.Conn, id int64) []byte {
	t.Helper()
	stmt, _, err := app.Prepare(`SELECT body FROM blobrow WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		t.Fatalf("Step: hasRow=%v err=%v", hasRow, err)
	}
	return stmt.ColumnBlob(0)
}
