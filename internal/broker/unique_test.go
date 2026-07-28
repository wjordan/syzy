package broker

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// uniqueApplier is applierFixture's twin for a schema with a UNIQUE
// column, so the loser-null arbitration in unique.go has something to
// arbitrate against.
type uniqueApplier struct {
	app   *sqlitebridge.Conn
	cat   *catalog.Catalog
	cache *nodestate.Cache
	br    *Broker
	tab   *catalog.Table
}

const uniqueSchema = `CREATE TABLE u (
	id BLOB PRIMARY KEY NOT NULL,
	slug TEXT,
	n TEXT,
	UNIQUE (slug)
)`

func newUniqueApplier(t testing.TB, origin crdt.Origin) *uniqueApplier {
	t.Helper()
	dir := t.TempDir()

	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Exec(`PRAGMA journal_mode = WAL; ` + uniqueSchema); err != nil {
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
	// SeedFromSchema only seeds the PK; manually upsert the syzy_key
	// rows for the UNIQUE(slug) constraint so the apply path can see
	// the unique key. Production goes through the DDL admission path
	// which writes these on CREATE TABLE.
	tab, _ := cat.Table("u")
	slugCol, _ := tab.Column("slug")
	keyID := crdt.KeyID{0x01}
	if err := sc.WithTx(func(tx *metadata.Tx) error {
		return tx.UpsertKey(metadata.KeyEntry{
			TableID: tab.ID, KeyID: keyID, ColumnID: slugCol.ID,
			Ordinal: 0, State: metadata.StateActive, CreateSeq: 0,
		})
	}); err != nil {
		t.Fatalf("UpsertKey: %v", err)
	}
	if err := cat.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	tab, _ = cat.Table("u")
	if len(tab.UniqueKeys) != 1 || len(tab.UniqueKeys[0].Columns) != 1 {
		t.Fatalf("expected 1 unique key with 1 column; got %+v", tab.UniqueKeys)
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

	return &uniqueApplier{app: app, cat: cat, cache: cache, br: br, tab: tab}
}

func buildUniqueInsert(t testing.TB, tab *catalog.Table, dot crdt.Dot, stamp crdt.Stamp, idVal []byte, slug, name string) *crdt.Changeset {
	t.Helper()
	idCol := tab.PK[0].ID
	pk, err := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{
		idCol: blobCol(idCol, idVal),
	})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	slugCol, _ := tab.Column("slug")
	nCol, _ := tab.Column("n")
	rec := crdt.Insert{
		Table: tab.ID, PK: pk, CL: 1,
		Image: []crdt.ColValue{
			textCol(slugCol.ID, slug),
			textCol(nCol.ID, name),
		},
	}
	cs, err := crdt.Build(dot, stamp, nil, testCluster, []crdt.Record{rec})
	if err != nil {
		t.Fatalf("crdt.Build: %v", err)
	}
	return cs
}

func readSlug(t *testing.T, app *sqlitebridge.Conn, id []byte) (string, bool) {
	return readUColumn(t, app, "slug", id)
}

func readN(t *testing.T, app *sqlitebridge.Conn, id []byte) (string, bool) {
	return readUColumn(t, app, "n", id)
}

func readUColumn(t *testing.T, app *sqlitebridge.Conn, col string, id []byte) (string, bool) {
	t.Helper()
	stmt, _, err := app.Prepare(`SELECT ` + col + ` FROM u WHERE id = ?`)
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
		return "", false
	}
	if stmt.ColumnIsNull(0) {
		return "", true
	}
	return stmt.ColumnText(0), true
}

// TestUniqueArbitration_LoserAfterWinner — incumbent wins, late arrival
// loses. R's writes for the UNIQUE column should be rewritten to NULL
// before SQL apply; the incumbent's row is untouched.
func TestUniqueArbitration_LoserAfterWinner(t *testing.T) {
	t.Parallel()
	f := newUniqueApplier(t, 1)

	winnerOrigin := crdt.Origin(7)
	winnerStamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: winnerOrigin}
	winner := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: winnerOrigin, Seq: 1}, winnerStamp, []byte{0x01}, "alpha", "first")
	if err := f.br.applyPayload(context.Background(), winner.Encoded()); err != nil {
		t.Fatalf("apply winner: %v", err)
	}

	loserOrigin := crdt.Origin(8)
	loserStamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: loserOrigin}
	loser := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: loserOrigin, Seq: 1}, loserStamp, []byte{0x02}, "alpha", "second")
	if err := f.br.applyPayload(context.Background(), loser.Encoded()); err != nil {
		t.Fatalf("apply loser: %v", err)
	}

	gotWinnerSlug, ok := readSlug(t, f.app, []byte{0x01})
	if !ok {
		t.Fatalf("winner row missing")
	}
	if gotWinnerSlug != "alpha" {
		t.Errorf("winner slug = %q; want alpha", gotWinnerSlug)
	}
	gotLoserSlug, ok := readSlug(t, f.app, []byte{0x02})
	if !ok {
		t.Fatalf("loser row missing")
	}
	if gotLoserSlug != "" {
		t.Errorf("loser slug = %q; want NULL", gotLoserSlug)
	}
}

// TestUniqueArbitration_WinnerStealsFromIncumbent — late arrival
// dominates the incumbent. R wins; we NULL the incumbent's UNIQUE
// columns in SQLite and write a per-cell cell_clock override at R.stamp
// for each column of K, so older replays of Q's K columns lose. Q's
// row baseline stays untouched so concurrent non-K writes against Q
// retain their original LWW comparison point.
func TestUniqueArbitration_WinnerStealsFromIncumbent(t *testing.T) {
	t.Parallel()
	f := newUniqueApplier(t, 1)

	earlyOrigin := crdt.Origin(7)
	earlyStamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: earlyOrigin}
	early := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: earlyOrigin, Seq: 1}, earlyStamp, []byte{0x01}, "alpha", "first")
	if err := f.br.applyPayload(context.Background(), early.Encoded()); err != nil {
		t.Fatalf("apply early: %v", err)
	}

	lateOrigin := crdt.Origin(8)
	lateStamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: lateOrigin}
	late := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: lateOrigin, Seq: 1}, lateStamp, []byte{0x02}, "alpha", "second")
	if err := f.br.applyPayload(context.Background(), late.Encoded()); err != nil {
		t.Fatalf("apply late: %v", err)
	}

	gotEarlySlug, ok := readSlug(t, f.app, []byte{0x01})
	if !ok {
		t.Fatalf("early row missing")
	}
	if gotEarlySlug != "" {
		t.Errorf("early slug = %q; want NULL (stolen by late winner)", gotEarlySlug)
	}
	gotLateSlug, ok := readSlug(t, f.app, []byte{0x02})
	if !ok {
		t.Fatalf("late row missing")
	}
	if gotLateSlug != "alpha" {
		t.Errorf("late slug = %q; want alpha", gotLateSlug)
	}

	// Q's row baseline stays at its original value; only the K column
	// gets a cell_clock override at R.stamp.
	idCol := f.tab.PK[0].ID
	slugCol, _ := f.tab.Column("slug")
	earlyPK, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{
		idCol: blobCol(idCol, []byte{0x01}),
	})
	rs := f.cache.RowState(f.tab.ID, earlyPK)
	if rs.Base != earlyStamp {
		t.Errorf("early row baseline = %v; want untouched %v", rs.Base, earlyStamp)
	}
	cell, ok := rs.Cells[slugCol.ID]
	if !ok {
		t.Errorf("missing cell_clock override on stolen K column")
	} else if cell != lateStamp {
		t.Errorf("slug cell_clock = %v; want winner stamp %v", cell, lateStamp)
	}
}

// TestUniqueArbitration_NullValueSkipsArbitration — multi-NULL is
// allowed by the spec; arbitration must skip rows whose UNIQUE tuple
// contains NULL so duplicate-NULL inserts do not stomp each other.
func TestUniqueArbitration_NullValueSkipsArbitration(t *testing.T) {
	t.Parallel()
	f := newUniqueApplier(t, 1)

	idCol := f.tab.PK[0].ID
	slugCol, _ := f.tab.Column("slug")
	nCol, _ := f.tab.Column("n")

	// Two rows with slug=NULL must both retain their identities.
	for i, idVal := range [][]byte{{0x01}, {0x02}} {
		pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, idVal)})
		rec := crdt.Insert{
			Table: f.tab.ID, PK: pk, CL: 1,
			Image: []crdt.ColValue{
				{Column: slugCol.ID, TypeTag: crdt.ColNull},
				textCol(nCol.ID, "x"),
			},
		}
		stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: int64(1000 + i)}, Origin: crdt.Origin(7)}
		cs, err := crdt.Build(crdt.Dot{Origin: crdt.Origin(7), Seq: crdt.Seq(i + 1)}, stamp, nil, testCluster, []crdt.Record{rec})
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	for _, idVal := range [][]byte{{0x01}, {0x02}} {
		if _, ok := readSlug(t, f.app, idVal); !ok {
			t.Errorf("row %x missing", idVal)
		}
	}
}

// TestUniqueArbitration_UpdateStealsFromIncumbent — an UPDATE that sets
// a UNIQUE column to a value already held by another row arbitrates
// just like an Insert.
func TestUniqueArbitration_UpdateStealsFromIncumbent(t *testing.T) {
	t.Parallel()
	f := newUniqueApplier(t, 1)

	// Seed P1 (slug='alpha') and P2 (slug='beta') from origin 7.
	src := crdt.Origin(7)
	stampSeed1 := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: src}
	stampSeed2 := crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: src}
	cs1 := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 1}, stampSeed1, []byte{0x01}, "alpha", "first")
	cs2 := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 2}, stampSeed2, []byte{0x02}, "beta", "second")
	if err := f.br.applyPayload(context.Background(), cs1.Encoded()); err != nil {
		t.Fatalf("seed P1: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs2.Encoded()); err != nil {
		t.Fatalf("seed P2: %v", err)
	}

	// Now UPDATE P2.slug = 'alpha' from origin 8 with a higher stamp.
	// P2 wins the value 'alpha'; P1 must have its slug nulled.
	updOrigin := crdt.Origin(8)
	updStamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: updOrigin}
	idCol := f.tab.PK[0].ID
	slugCol, _ := f.tab.Column("slug")
	pk2, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x02})})
	upd := crdt.Update{Table: f.tab.ID, PK: pk2, CL: 1, Changed: []crdt.ColValue{textCol(slugCol.ID, "alpha")}}
	csU, err := crdt.Build(crdt.Dot{Origin: updOrigin, Seq: 1}, updStamp, nil, testCluster, []crdt.Record{upd})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), csU.Encoded()); err != nil {
		t.Fatalf("apply update: %v", err)
	}

	if got, _ := readSlug(t, f.app, []byte{0x01}); got != "" {
		t.Errorf("P1 slug = %q; want NULL after stolen by UPDATE", got)
	}
	if got, _ := readSlug(t, f.app, []byte{0x02}); got != "alpha" {
		t.Errorf("P2 slug = %q; want alpha", got)
	}
}

// TestUniqueArbitration_SECAcrossInterleavings is the regression test
// for the row-baseline-bump SEC bug. Three records over Q's row:
//
//   - Q.seed: INSERT Q with slug='alpha', n='zero' at stampSeed.
//   - Z: UPDATE Q.n='z-update' at stampZ (mid).
//   - R: INSERT R (different PK) with slug='alpha' at stampR (high).
//
// Q.stampSeed < Z.stamp < R.stamp. Z writes a non-K column.
//
// Apply Q.seed then choose between (R, then Z) and (Z, then R) — both
// orderings must produce byte-identical app.db state. Under the old
// "bump Q's row baseline to R.stamp" approximation, the (R, then Z)
// path would reject Z (because R bumped Q.Base above Z), diverging
// from the (Z, then R) path that applied Z first. The spec form
// (per-cell override on K) leaves Q.Base alone, so Z dominates Q's
// effective stamp on the n column regardless of order.
func TestUniqueArbitration_SECAcrossInterleavings(t *testing.T) {
	t.Parallel()
	apply := func(t *testing.T, order []func(*uniqueApplier)) string {
		t.Helper()
		f := newUniqueApplier(t, 1)
		for _, fn := range order {
			fn(f)
		}
		gotN, ok := readN(t, f.app, []byte{0x01})
		if !ok {
			t.Fatalf("Q row missing in order")
		}
		gotSlugQ, _ := readSlug(t, f.app, []byte{0x01})
		gotSlugR, _ := readSlug(t, f.app, []byte{0x02})
		return fmt.Sprintf("Q.n=%q Q.slug=%q R.slug=%q", gotN, gotSlugQ, gotSlugR)
	}

	idCol := func(f *uniqueApplier) crdt.ColumnID { return f.tab.PK[0].ID }
	pkOf := func(f *uniqueApplier, id byte) crdt.PKBlob {
		pk, err := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{
			idCol(f): blobCol(idCol(f), []byte{id}),
		})
		if err != nil {
			panic(err)
		}
		return pk
	}

	stampQ := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: crdt.Origin(7)}
	stampZ := crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: crdt.Origin(7)}
	stampR := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: crdt.Origin(8)}

	seedQ := func(f *uniqueApplier) {
		cs := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: 7, Seq: 1}, stampQ, []byte{0x01}, "alpha", "zero")
		if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("seedQ: %v", err)
		}
	}
	applyZ := func(f *uniqueApplier) {
		nCol, _ := f.tab.Column("n")
		upd := crdt.Update{
			Table: f.tab.ID, PK: pkOf(f, 0x01), CL: 1,
			Changed: []crdt.ColValue{textCol(nCol.ID, "z-update")},
		}
		cs, err := crdt.Build(crdt.Dot{Origin: 7, Seq: 2}, stampZ, nil, testCluster, []crdt.Record{upd})
		if err != nil {
			t.Fatalf("Build Z: %v", err)
		}
		if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("apply Z: %v", err)
		}
	}
	applyR := func(f *uniqueApplier) {
		cs := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: 8, Seq: 1}, stampR, []byte{0x02}, "alpha", "r-row")
		if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("apply R: %v", err)
		}
	}

	a := apply(t, []func(*uniqueApplier){seedQ, applyR, applyZ})
	b := apply(t, []func(*uniqueApplier){seedQ, applyZ, applyR})
	if a != b {
		t.Errorf("interleavings diverged:\n  R-then-Z: %s\n  Z-then-R: %s", a, b)
	}
	want := `Q.n="z-update" Q.slug="" R.slug="alpha"`
	if a != want {
		t.Errorf("final state = %s; want %s", a, want)
	}
}

// TestUniqueArbitration_NoArbitrationOnSamePK — UPSERT on the same PK
// rewriting its own UNIQUE column must not self-arbitrate. The new
// value sticks; we treat the matched-self case as no contention.
func TestUniqueArbitration_NoArbitrationOnSamePK(t *testing.T) {
	t.Parallel()
	f := newUniqueApplier(t, 1)

	src := crdt.Origin(7)
	stamp1 := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	cs1 := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 1}, stamp1, []byte{0x01}, "alpha", "first")
	if err := f.br.applyPayload(context.Background(), cs1.Encoded()); err != nil {
		t.Fatalf("apply 1: %v", err)
	}

	// Re-insert the same PK with the same slug at a higher stamp. The
	// SELECT for owner finds itself; arbitration must skip.
	stamp2 := crdt.Stamp{Clock: crdt.Clock{WallTime: 2000}, Origin: src}
	cs2 := buildUniqueInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 2}, stamp2, []byte{0x01}, "alpha", "second")
	if err := f.br.applyPayload(context.Background(), cs2.Encoded()); err != nil {
		t.Fatalf("apply 2: %v", err)
	}
	gotSlug, ok := readSlug(t, f.app, []byte{0x01})
	if !ok {
		t.Fatalf("row missing")
	}
	if gotSlug != "alpha" {
		t.Errorf("slug = %q; want alpha (self-UPSERT must not null itself)", gotSlug)
	}
}

// TestUniqueArbitration_CoordinatedRecreateAfterSoftDelete is the regression
// for a delete-then-recreate on a coordinated *partial* unique key (label NOT
// NULL, UNIQUE WHERE gone IS NULL). Before the fix, the apply-side loser-null
// arbitration ran for coordinated keys too, ignored the partial predicate,
// matched the soft-deleted (gone IS NOT NULL) old row as the unique owner, and
// ran UPDATE item SET label = NULL on it — which NOT NULL rejects, wedging the
// changeset in apply_quarantine forever. Coordinated keys are guaranteed unique
// by the pre-commit reservation gate plus every node's physical partial index,
// so apply must skip loser-null arbitration for them.
func TestUniqueArbitration_CoordinatedRecreateAfterSoftDelete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	const schema = `CREATE TABLE item (
		id BLOB PRIMARY KEY NOT NULL,
		label TEXT NOT NULL,
		gone INTEGER
	)`
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
	if err := sc.SetNodeID(crdt.Origin(1)); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}

	cat, err := catalog.SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("SeedFromSchema: %v", err)
	}
	// The literal CREATE UNIQUE INDEX every node replays on apply: a
	// soft-deleted (gone IS NOT NULL) row drops out of the index, so a
	// same-label recreate is physically legal.
	if err := app.Exec(`CREATE UNIQUE INDEX item_label_live ON item(label) WHERE gone IS NULL`); err != nil {
		t.Fatalf("create partial index: %v", err)
	}

	tab, _ := cat.Table("item")
	labelCol, _ := tab.Column("label")
	goneCol, _ := tab.Column("gone")
	pred := crdt.UniquePredicate{Root: &crdt.PredExpr{Op: crdt.PredIsNull, Col: goneCol.ID}}
	if err := sc.WithTx(func(tx *metadata.Tx) error {
		return tx.UpsertKey(metadata.KeyEntry{
			TableID: tab.ID, KeyID: crdt.KeyID{0x01}, ColumnID: labelCol.ID,
			Ordinal: 0, State: metadata.StateActive, CreateSeq: 0,
			Coordinated: true, Predicate: crdt.EncodeUniquePredicate(pred),
		})
	}); err != nil {
		t.Fatalf("UpsertKey: %v", err)
	}
	if err := cat.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	tab, _ = cat.Table("item")
	if len(tab.UniqueKeys) != 1 || !tab.UniqueKeys[0].Coordinated || tab.UniqueKeys[0].Predicate.Root == nil {
		t.Fatalf("expected 1 coordinated partial key; got %+v", tab.UniqueKeys)
	}

	cache := nodestate.New(crdt.Origin(1))
	br, err := New(Config{AppApply: app, Meta: sc, Catalog: cat, Cache: cache})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}

	ctx := context.Background()
	pkOf := func(id byte) crdt.PKBlob {
		pk, err := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{tab.PK[0].ID: blobCol(tab.PK[0].ID, []byte{id})})
		if err != nil {
			t.Fatalf("EncodePK: %v", err)
		}
		return pk
	}
	nullGone := crdt.ColValue{Column: goneCol.ID, TypeTag: crdt.ColNull}
	apply := func(what string, dot crdt.Dot, stamp crdt.Stamp, rec crdt.Record) {
		cs, err := crdt.Build(dot, stamp, nil, testCluster, []crdt.Record{rec})
		if err != nil {
			t.Fatalf("Build %s: %v", what, err)
		}
		if err := br.applyPayload(ctx, cs.Encoded()); err != nil {
			t.Fatalf("apply %s: %v", what, err)
		}
	}

	origA := crdt.Origin(7)
	apply("A insert", crdt.Dot{Origin: origA, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: origA},
		crdt.Insert{Table: tab.ID, PK: pkOf(0x01), CL: 1, Image: []crdt.ColValue{
			textCol(labelCol.ID, "widget"), nullGone,
		}})
	apply("A soft-delete", crdt.Dot{Origin: origA, Seq: 2},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: origA},
		crdt.Update{Table: tab.ID, PK: pkOf(0x01), CL: 1, Changed: []crdt.ColValue{
			intCol(goneCol.ID, 200),
		}})
	// B recreates the same label from a different origin with a dominating
	// stamp — the exact pre-fix trap (steal path nulls the dead row's label).
	origB := crdt.Origin(8)
	apply("B recreate", crdt.Dot{Origin: origB, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: origB},
		crdt.Insert{Table: tab.ID, PK: pkOf(0x02), CL: 1, Image: []crdt.ColValue{
			textCol(labelCol.ID, "widget"), nullGone,
		}})

	q, err := sc.ListQuarantine()
	if err != nil {
		t.Fatalf("ListQuarantine: %v", err)
	}
	if len(q) != 0 {
		t.Fatalf("apply_quarantine must be empty after a legal recreate; got %d: %+v", len(q), q)
	}
	if got := readItemLabel(t, app, []byte{0x01}); got != "widget" {
		t.Errorf("soft-deleted A.label = %q; want widget (must not be nulled)", got)
	}
	if got := readItemLabel(t, app, []byte{0x02}); got != "widget" {
		t.Errorf("recreated B.label = %q; want widget", got)
	}
}

func readItemLabel(t *testing.T, app *sqlitebridge.Conn, id []byte) string {
	t.Helper()
	stmt, _, err := app.Prepare(`SELECT label FROM item WHERE id = ?`)
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
		return "<missing>"
	}
	if stmt.ColumnIsNull(0) {
		return "<null>"
	}
	return stmt.ColumnText(0)
}
