package producer

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/internal/metadata"
)

// TestDDL_CounterColumn_AppliesCatalog: `INTEGER COUNTER` declares the
// column's clock_group 'counter' and derives the table's default clock
// group 'cell' (sqlite/docs/DDL.md#counter-columns).
func TestDDL_CounterColumn_AppliesCatalog(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE inv (id BLOB PRIMARY KEY NOT NULL, qty INTEGER COUNTER NOT NULL DEFAULT 0, note TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE with COUNTER column: %v", err)
	}
	f.waitDrain(t)

	tab, ok := f.cat.Table("inv")
	if !ok {
		t.Fatalf("inv missing from catalog")
	}
	if !tab.CellGroup() {
		t.Errorf("table clock group = %q; want cell (derived from counter column)", tab.ClockGroup())
	}
	if !tab.HasCounters() {
		t.Errorf("HasCounters() = false; want true")
	}
	qty, ok := tab.Column("qty")
	if !ok || !qty.Counter() {
		t.Errorf("qty.Counter() = %v (ok=%v); want true", qty.Counter(), ok)
	}
	if note, ok := tab.Column("note"); !ok || note.Counter() {
		t.Errorf("note must stay a register column")
	}
}

// TestDDL_CounterColumn_Rejections: each admission rule fires with the
// schema log left untouched.
func TestDDL_CounterColumn_Rejections(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"nullable", `CREATE TABLE c (id BLOB PRIMARY KEY NOT NULL, qty INTEGER COUNTER DEFAULT 0)`},
		{"non-integer affinity", `CREATE TABLE c (id BLOB PRIMARY KEY NOT NULL, qty COUNTER NOT NULL DEFAULT 0)`},
		{"pk member", `CREATE TABLE c (id INTEGER COUNTER NOT NULL, x TEXT, PRIMARY KEY (id))`},
		{"unique column", `CREATE TABLE c (id BLOB PRIMARY KEY NOT NULL, qty INTEGER COUNTER NOT NULL UNIQUE)`},
		{"unique table clause", `CREATE TABLE c (id BLOB PRIMARY KEY NOT NULL, qty INTEGER COUNTER NOT NULL, UNIQUE(qty))`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDDLFixture(t)
			if err := f.app.Exec(tc.sql); err == nil {
				t.Errorf("accepted; want admission rejection")
			}
			if head, _ := f.log.Head(context.Background()); head != 0 {
				t.Errorf("schema log advanced on rejected DDL: head=%d", head)
			}
		})
	}
}

// TestDDL_CounterAddColumn_RequiresCellGroup: ADD COLUMN … COUNTER on a
// row-group table is rejected; after the table flips to cell it lands.
func TestDDL_CounterAddColumn_RequiresCellGroup(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, x TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	f.waitDrain(t)

	if err := f.app.Exec(`ALTER TABLE t ADD COLUMN hits INTEGER COUNTER NOT NULL DEFAULT 0`); err == nil {
		t.Fatalf("ADD COLUMN COUNTER on a row-group table accepted; want rejection")
	}

	tab, _ := f.cat.Table("t")
	if err := f.sc.WithTx(func(tx *metadata.Tx) error {
		return tx.SetDefaultClockGroup(tab.ID, metadata.ClockGroupCell)
	}); err != nil {
		t.Fatalf("flip to cell: %v", err)
	}
	if err := f.cat.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}

	if err := f.app.Exec(`ALTER TABLE t ADD COLUMN hits INTEGER COUNTER NOT NULL DEFAULT 0`); err != nil {
		t.Fatalf("ADD COLUMN COUNTER on a cell-group table: %v", err)
	}
	f.waitDrain(t)
	tab, _ = f.cat.Table("t")
	if hits, ok := tab.Column("hits"); !ok || !hits.Counter() {
		t.Errorf("hits.Counter() = %v (ok=%v); want true", hits.Counter(), ok)
	}
}
