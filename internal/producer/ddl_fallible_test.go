package producer

import (
	"testing"

	"github.com/wjordan/syzy/sqlitebridge"
)

// DDL forms whose validity depends on table data (CREATE UNIQUE INDEX,
// DROP TABLE under foreign keys) pass SQLite's compiler and only fail
// once the body runs — after trace_v2 has already admitted them. These
// tests pin the invariant that a statement which fails locally never
// reaches the schema log.

func (f *ddlFixture) mustExec(t *testing.T, stmts ...string) {
	t.Helper()
	for _, sql := range stmts {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
}

// dupTable is a table holding two rows that share a value in v, so a
// UNIQUE index on v cannot be built.
func (f *ddlFixture) dupTable(t *testing.T) {
	t.Helper()
	f.mustExec(t,
		`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT)`,
		`INSERT INTO t (id, v) VALUES (x'01', 'dup')`,
		`INSERT INTO t (id, v) VALUES (x'02', 'dup')`,
	)
}

// fkParent is a parent row referenced by a child row, so DROP TABLE p
// fails the foreign-key check.
func (f *ddlFixture) fkParent(t *testing.T) {
	t.Helper()
	f.mustExec(t,
		`PRAGMA foreign_keys = ON`,
		`CREATE TABLE p (id BLOB PRIMARY KEY NOT NULL)`,
		`CREATE TABLE c (id BLOB PRIMARY KEY NOT NULL, pid BLOB REFERENCES p(id))`,
		`INSERT INTO p (id) VALUES (x'01')`,
		`INSERT INTO c (id, pid) VALUES (x'02', x'01')`,
	)
}

func TestDDL_FailedUniqueIndexNotPublished_Autocommit(t *testing.T) {
	f := newDDLFixture(t)
	f.dupTable(t)
	before := f.head(t)

	if err := f.app.Exec(`CREATE UNIQUE INDEX u ON t (v)`); err == nil {
		t.Fatal("CREATE UNIQUE INDEX on duplicate data unexpectedly succeeded")
	}

	if got := f.head(t); got != before {
		t.Errorf("schema log advanced %d -> %d for a statement that failed", before, got)
	}
	if f.intentPresent(t) {
		t.Error("failed DDL left a LocalDDL intent behind")
	}
	tab, ok := f.cat.Table("t")
	if !ok {
		t.Fatal("t missing from catalog")
	}
	if len(tab.UniqueKeys) != 0 {
		t.Errorf("failed CREATE UNIQUE INDEX installed %d unique key(s)", len(tab.UniqueKeys))
	}
}

// A failed statement does not abort an explicit transaction, so an
// application that ignores the error can still COMMIT. The commit-time
// post-state check is what keeps the event out of the log.
func TestDDL_FailedUniqueIndexNotPublished_Txn(t *testing.T) {
	f := newDDLFixture(t)
	f.dupTable(t)
	before := f.head(t)

	f.mustExec(t, `BEGIN`)
	if err := f.app.Exec(`CREATE UNIQUE INDEX u ON t (v)`); err == nil {
		t.Fatal("CREATE UNIQUE INDEX on duplicate data unexpectedly succeeded")
	}
	f.mustExec(t, `COMMIT`) // application swallows the error

	if got := f.head(t); got != before {
		t.Errorf("schema log advanced %d -> %d for a statement that failed", before, got)
	}
	if f.intentPresent(t) {
		t.Error("failed DDL left a LocalDDL intent behind")
	}
	tab, _ := f.cat.Table("t")
	if len(tab.UniqueKeys) != 0 {
		t.Errorf("failed CREATE UNIQUE INDEX installed %d unique key(s)", len(tab.UniqueKeys))
	}
}

func TestDDL_FailedDropTableNotPublished_Autocommit(t *testing.T) {
	f := newDDLFixture(t)
	f.fkParent(t)
	before := f.head(t)

	if err := f.app.Exec(`DROP TABLE p`); err == nil {
		t.Fatal("DROP TABLE with FK dependents unexpectedly succeeded")
	}

	if got := f.head(t); got != before {
		t.Errorf("schema log advanced %d -> %d for a statement that failed", before, got)
	}
	if f.intentPresent(t) {
		t.Error("failed DDL left a LocalDDL intent behind")
	}
	// The table is still here locally; the catalog must still list it.
	exists, err := sqlitebridge.ObjectExists(f.app, "table", "p")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("p vanished from app.db")
	}
	if _, ok := f.cat.Table("p"); !ok {
		t.Error("catalog dropped p even though the DROP failed")
	}
}

func TestDDL_FailedDropTableNotPublished_Txn(t *testing.T) {
	f := newDDLFixture(t)
	f.fkParent(t)
	before := f.head(t)

	f.mustExec(t, `BEGIN`)
	if err := f.app.Exec(`DROP TABLE p`); err == nil {
		t.Fatal("DROP TABLE with FK dependents unexpectedly succeeded")
	}
	f.mustExec(t, `COMMIT`)

	if got := f.head(t); got != before {
		t.Errorf("schema log advanced %d -> %d for a statement that failed", before, got)
	}
	if _, ok := f.cat.Table("p"); !ok {
		t.Error("catalog dropped p even though the DROP failed")
	}
}

// The deferred forms must still replicate normally when their body
// succeeds — the commit-time Append is a reordering, not a removal.
func TestDDL_FallibleFormsStillReplicate(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT)`,
		`INSERT INTO t (id, v) VALUES (x'01', 'a')`,
		`INSERT INTO t (id, v) VALUES (x'02', 'b')`,
	)
	f.mustExec(t, `CREATE UNIQUE INDEX u ON t (v)`)

	if got := f.head(t); got != 2 {
		t.Fatalf("schema log head = %d, want 2 after CREATE TABLE + CREATE UNIQUE INDEX", got)
	}
	if f.intentPresent(t) {
		t.Error("intent not resolved after successful DDL")
	}
	tab, _ := f.cat.Table("t")
	if len(tab.UniqueKeys) != 1 {
		t.Fatalf("catalog has %d unique keys, want 1", len(tab.UniqueKeys))
	}
	if seq, _, _ := f.sc.GetSchemaSeq(); seq != 2 {
		t.Errorf("schema_seq = %d, want 2", seq)
	}

	// And a successful DROP TABLE.
	f.mustExec(t, `DROP TABLE t`)
	if got := f.head(t); got != 3 {
		t.Errorf("schema log head = %d, want 3 after DROP TABLE", got)
	}
	if _, ok := f.cat.Table("t"); ok {
		t.Error("t still active in catalog after DROP")
	}
}

// A failed deferred DDL must not leave pending state that a later,
// unrelated commit would then publish. rollback_hook clears it; this
// pins that the next transaction is unaffected.
func TestDDL_FailedFallibleDDLLeavesNoPendingState(t *testing.T) {
	f := newDDLFixture(t)
	f.dupTable(t)

	if err := f.app.Exec(`CREATE UNIQUE INDEX u ON t (v)`); err == nil {
		t.Fatal("expected failure")
	}
	if f.prod.ddl.txnDDL != nil {
		t.Fatal("pending DDL survived the failed statement")
	}

	// An unrelated DML transaction must not publish anything.
	before := f.head(t)
	f.mustExec(t,
		`BEGIN`,
		`INSERT INTO t (id, v) VALUES (x'03', 'other')`,
		`COMMIT`,
	)
	if got := f.head(t); got != before {
		t.Errorf("unrelated DML commit published a schema event: head %d -> %d", before, got)
	}

	// And the next real DDL still works.
	f.mustExec(t, `CREATE TABLE t2 (id BLOB PRIMARY KEY NOT NULL)`)
	if got := f.head(t); got != before+1 {
		t.Errorf("schema log head = %d, want %d after CREATE TABLE", got, before+1)
	}
}
