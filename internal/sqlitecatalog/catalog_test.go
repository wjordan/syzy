package catalog

import (
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/sqlitebridge"
)

// fixture opens a fresh app conn + metadata in a temp dir and applies
// the supplied DDL to the app.
func fixture(t *testing.T, ddl string) (*sqlitebridge.Conn, *metadata.Store) {
	t.Helper()
	dir := t.TempDir()
	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if ddl != "" {
		if err := app.Exec(ddl); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}
	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	return app, sc
}

func TestSeedFromSchema_BuildsActiveTables(t *testing.T) {
	app, sc := fixture(t, `
		CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, ts INTEGER, body TEXT);
		CREATE TABLE doc (id INT PRIMARY KEY NOT NULL, body BLOB);
		CREATE TABLE _local (id INTEGER PRIMARY KEY);
	`)
	cat, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	tables := cat.Tables()
	if len(tables) != 2 {
		t.Errorf("Tables = %d; want 2 (skipping _local)", len(tables))
	}
	if _, ok := cat.Table("event"); !ok {
		t.Error("Table(event) missing")
	}
	if _, ok := cat.Table("doc"); !ok {
		t.Error("Table(doc) missing")
	}
	if _, ok := cat.Table("_local"); ok {
		t.Error("Table(_local) present; should be skipped")
	}
	ev, _ := cat.Table("event")
	if len(ev.Columns) != 3 {
		t.Errorf("event Columns = %d; want 3", len(ev.Columns))
	}
	if len(ev.PK) != 1 || ev.PK[0].Name != "id" {
		t.Errorf("event PK = %+v; want [id]", ev.PK)
	}
}

func TestSeedFromSchema_PersistsToMeta(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, n TEXT)`)
	cat1, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	tab1, _ := cat1.Table("t")

	// Re-load directly from the metadata (no second seed); must observe
	// the same IDs the seed allocated.
	cat2, err := LoadFromMeta(sc)
	if err != nil {
		t.Fatalf("LoadFromMeta: %v", err)
	}
	tab2, _ := cat2.Table("t")
	if tab1.ID != tab2.ID {
		t.Errorf("TableID differs after reload: %x vs %x", tab1.ID, tab2.ID)
	}
	for i := range tab1.Columns {
		if tab1.Columns[i].ID != tab2.Columns[i].ID {
			t.Errorf("col %d ID differs after reload", i)
		}
	}
}

func TestSeedFromSchema_IdempotentWhenMetaPopulated(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('t')))`)
	cat1, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("first Seed: %v", err)
	}
	tab1, _ := cat1.Table("t")
	// Calling again must not re-allocate IDs.
	cat2, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("second Seed: %v", err)
	}
	tab2, _ := cat2.Table("t")
	if tab1.ID != tab2.ID {
		t.Errorf("re-seed allocated new TableID: %x vs %x", tab1.ID, tab2.ID)
	}
	if got := tab2.PK[0].PKDefault; got != (PKDefault{Kind: PKDefaultGenID, Arg: "t"}) {
		t.Errorf("warm re-seed PKDefault = %+v, want gen_id('t')", got)
	}
}

func TestCatalog_MultiColumnPK(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE pair (a INT NOT NULL, b INT NOT NULL, c TEXT, PRIMARY KEY (b, a))`)
	cat, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	pair, _ := cat.Table("pair")
	if len(pair.PK) != 2 {
		t.Fatalf("pair PK = %d; want 2", len(pair.PK))
	}
	// PK order is (b, a) per the DDL.
	if pair.PK[0].Name != "b" || pair.PK[1].Name != "a" {
		t.Errorf("pair PK order = (%q, %q); want (b, a)", pair.PK[0].Name, pair.PK[1].Name)
	}
}

func TestCatalog_TableByIDRoundtrip(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`)
	cat, _ := SeedFromSchema(app, sc)
	tab, _ := cat.Table("t")
	got, ok := cat.TableByID(tab.ID)
	if !ok || got.Name != "t" {
		t.Errorf("TableByID = (%v, %v); want (t, true)", got, ok)
	}
}

func TestCatalog_ColumnByID(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, body TEXT)`)
	cat, _ := SeedFromSchema(app, sc)
	tab, _ := cat.Table("t")
	body, ok := tab.Column("body")
	if !ok {
		t.Fatal("Column(body) missing")
	}
	got, ok := tab.ColumnByID(body.ID)
	if !ok || got.Name != "body" {
		t.Errorf("ColumnByID = (%v, %v); want (body, true)", got, ok)
	}
}

func TestSeedFromSchema_RequiresPK(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE pkless (a INT, b TEXT)`)
	if _, err := SeedFromSchema(app, sc); err == nil {
		t.Fatal("expected error for table without PK; got nil")
	}
}

func TestSeedFromSchema_PopulatesPKDefault(t *testing.T) {
	app, sc := fixture(t, `
		CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()), body TEXT);
		CREATE TABLE doc (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('doc')), body BLOB);
		CREATE TABLE plain (id BLOB PRIMARY KEY NOT NULL, body TEXT);
	`)
	cat, err := SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	cases := []struct {
		table string
		col   string
		want  PKDefault
	}{
		{"event", "id", PKDefault{Kind: PKDefaultUUIDv7}},
		{"event", "body", PKDefault{}},
		{"doc", "id", PKDefault{Kind: PKDefaultGenID, Arg: "doc"}},
		{"plain", "id", PKDefault{}},
	}
	for _, c := range cases {
		tab, ok := cat.Table(c.table)
		if !ok {
			t.Fatalf("Table(%q) missing", c.table)
		}
		col, ok := tab.Column(c.col)
		if !ok {
			t.Fatalf("Column(%s.%s) missing", c.table, c.col)
		}
		if col.PKDefault != c.want {
			t.Errorf("%s.%s PKDefault = %+v, want %+v", c.table, c.col, col.PKDefault, c.want)
		}
	}
}

func TestCatalog_RefreshPKDefaults_AfterMetaReload(t *testing.T) {
	app, sc := fixture(t, `CREATE TABLE doc (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('doc')), body BLOB)`)
	if _, err := SeedFromSchema(app, sc); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	// LoadFromMeta alone returns an in-memory catalog with PKDefault
	// stripped (it isn't persisted). RefreshPKDefaults repopulates it
	// from app.db.
	cat, err := LoadFromMeta(sc)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	tab, _ := cat.Table("doc")
	col, _ := tab.Column("id")
	if col.PKDefault.Kind != PKDefaultNone {
		t.Errorf("pre-refresh PKDefault = %+v, want zero", col.PKDefault)
	}
	if err := cat.RefreshPKDefaults(app); err != nil {
		t.Fatalf("RefreshPKDefaults: %v", err)
	}
	tab, _ = cat.Table("doc")
	col, _ = tab.Column("id")
	if col.PKDefault != (PKDefault{Kind: PKDefaultGenID, Arg: "doc"}) {
		t.Errorf("post-refresh PKDefault = %+v, want gen_id('doc')", col.PKDefault)
	}
}

func TestCatalog_DroppedTableHiddenFromActiveLookups(t *testing.T) {
	_, sc := fixture(t, "")
	tab := AllocTableID()
	if err := sc.WithTx(func(tx *metadata.Tx) error {
		return tx.UpsertTable(metadata.TableEntry{
			ID: tab, Name: "tombstone", State: metadata.StateDropped,
			DefaultClockGroup: metadata.ClockGroupRow, CreateSeq: 1, DropSeq: 2,
		})
	}); err != nil {
		t.Fatalf("seed dropped: %v", err)
	}
	cat, err := LoadFromMeta(sc)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := cat.Table("tombstone"); ok {
		t.Error("Table(dropped) should hide via active-name lookup")
	}
	got, ok := cat.TableByID(tab)
	if !ok || !got.Dropped() {
		t.Errorf("TableByID dropped = (%v, ok=%v); want Dropped()=true", got, ok)
	}
}
