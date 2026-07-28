package producer

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// Multi-DDL transactions: every statement admitted in one BEGIN...COMMIT
// rides a single schema-log event (an OpBundle when there is more than
// one), and later statements resolve names the earlier ones created.

// decodeHead returns the catalog op stored at the schema log's head.
func (f *ddlFixture) decodeHead(t *testing.T) crdt.CatalogOp {
	t.Helper()
	head := f.head(t)
	ev, err := f.log.Read(context.Background(), head-1, 1)
	if err != nil {
		t.Fatalf("log.Read: %v", err)
	}
	if len(ev) == 0 {
		t.Fatalf("no event at seq %d", head)
	}
	op, err := crdt.DecodeCatalogOp(ev[0].CatalogOp)
	if err != nil {
		t.Fatalf("DecodeCatalogOp: %v", err)
	}
	return op
}

func TestDDL_TxnMultipleIndependentDDL(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL, v TEXT)`,
		`CREATE TABLE b (id BLOB PRIMARY KEY NOT NULL, v TEXT)`,
		`CREATE TABLE c (id BLOB PRIMARY KEY NOT NULL, v TEXT)`,
		`COMMIT`,
	)
	if head := f.head(t); head != 1 {
		t.Fatalf("schema log head = %d, want 1 (one bundled event)", head)
	}
	op := f.decodeHead(t)
	if op.Kind != crdt.OpBundle {
		t.Fatalf("head op kind = %v, want OpBundle", op.Kind)
	}
	if len(op.SubOps) != 3 {
		t.Errorf("bundle carries %d sub-ops, want 3", len(op.SubOps))
	}
	for _, n := range []string{"a", "b", "c"} {
		if _, ok := f.cat.Table(n); !ok {
			t.Errorf("table %q missing from catalog", n)
		}
	}
	if seq, _, _ := f.sc.GetSchemaSeq(); seq != 1 {
		t.Errorf("schema_seq = %d, want 1", seq)
	}
	if f.intentPresent(t) {
		t.Error("intent not resolved")
	}
}

// The dependent case the overlay exists for: statement 2 names an object
// statement 1 created, which the committed catalog does not yet know.
func TestDDL_TxnDependentDDL(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT, w TEXT)`,
		`CREATE INDEX idx_t_v ON t (v)`,
		`ALTER TABLE t ADD COLUMN extra TEXT`,
		`CREATE UNIQUE INDEX u_t_w ON t (w)`,
		`COMMIT`,
	)
	if head := f.head(t); head != 1 {
		t.Fatalf("schema log head = %d, want 1", head)
	}
	tab, ok := f.cat.Table("t")
	if !ok {
		t.Fatal("t missing from catalog")
	}
	if _, ok := tab.Column("extra"); !ok {
		t.Error("ALTER TABLE ADD COLUMN did not reach the catalog")
	}
	if len(tab.UniqueKeys) != 1 {
		t.Errorf("catalog has %d unique keys, want 1", len(tab.UniqueKeys))
	}
	exists, err := sqlitebridge.ObjectExists(f.app, "index", "idx_t_v")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("idx_t_v missing from app.db")
	}
}

// A rename mid-transaction must shadow the old name for later statements.
func TestDDL_TxnRenameThenUse(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT)`,
		`BEGIN`,
		`ALTER TABLE t RENAME TO t2`,
		`CREATE INDEX idx_t2_v ON t2 (v)`,
		`COMMIT`,
	)
	if _, ok := f.cat.Table("t2"); !ok {
		t.Error("t2 missing from catalog after rename")
	}
	if _, ok := f.cat.Table("t"); ok {
		t.Error("old name t still active in catalog")
	}
	if head := f.head(t); head != 2 {
		t.Errorf("schema log head = %d, want 2", head)
	}
}

// The standard framework-migration shape, now with several DDL
// statements before the bookkeeping DML.
func TestDDL_TxnMultiDDLThenDML(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE posts (id BLOB PRIMARY KEY NOT NULL, title TEXT)`,
		`CREATE TABLE migrations (id BLOB PRIMARY KEY NOT NULL, name TEXT)`,
		`CREATE INDEX idx_posts_title ON posts (title)`,
		`INSERT INTO posts (id, title) VALUES (x'01', 'hello')`,
		`INSERT INTO migrations (id, name) VALUES (x'02', '001_init')`,
		`COMMIT`,
	)
	// The DML must drain under the post-DDL schema_seq: wal_hook resolves
	// the bundle's intent before journaling the transaction's touches.
	f.waitDrain(t)
	if head := f.head(t); head != 1 {
		t.Fatalf("schema log head = %d, want 1", head)
	}
	if seq, _, _ := f.sc.GetSchemaSeq(); seq != 1 {
		t.Errorf("schema_seq = %d, want 1", seq)
	}
	for _, n := range []string{"posts", "migrations"} {
		if _, ok := f.cat.Table(n); !ok {
			t.Errorf("table %q missing from catalog", n)
		}
	}
}

// DML still has to come after all the DDL.
func TestDDL_TxnRejectsDDLAfterDML(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`,
		`INSERT INTO a (id) VALUES (x'01')`,
	)
	if err := f.app.Exec(`CREATE TABLE b (id BLOB PRIMARY KEY NOT NULL)`); err == nil {
		t.Fatal("DDL after DML was admitted; want rejection")
	}
	_ = f.app.Exec(`ROLLBACK`)
	if head := f.head(t); head != 0 {
		t.Errorf("schema log head = %d, want 0", head)
	}
}

func TestDDL_TxnMultiDDLRollback(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`,
		`CREATE TABLE b (id BLOB PRIMARY KEY NOT NULL)`,
		`ROLLBACK`,
	)
	if head := f.head(t); head != 0 {
		t.Errorf("schema log head = %d after rollback, want 0", head)
	}
	if f.intentPresent(t) {
		t.Error("intent left behind by rolled-back txn")
	}
	for _, n := range []string{"a", "b"} {
		if _, ok := f.cat.Table(n); ok {
			t.Errorf("rolled-back table %q leaked into catalog", n)
		}
	}
	if f.prod.ddl.overlay != nil {
		t.Error("overlay survived the rollback")
	}
	// The same transaction must work on retry.
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`,
		`CREATE TABLE b (id BLOB PRIMARY KEY NOT NULL)`,
		`COMMIT`,
	)
	if head := f.head(t); head != 1 {
		t.Errorf("schema log head = %d after retry, want 1", head)
	}
}

// A CAS loss must abort the whole multi-DDL transaction — all or nothing.
func TestDDL_TxnMultiDDLAbortsOnHeadMoved(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`,
		`CREATE TABLE b (id BLOB PRIMARY KEY NOT NULL)`,
	)
	if _, err := f.log.Append(context.Background(), 0, []byte{0x01}, "out-of-band"); err != nil {
		t.Fatalf("out-of-band Append: %v", err)
	}
	if err := f.app.Exec(`COMMIT`); err == nil {
		t.Error("COMMIT succeeded despite head move; want failure")
	}
	if f.intentPresent(t) {
		t.Error("intent left behind by aborted commit")
	}
	for _, n := range []string{"a", "b"} {
		if _, ok := f.cat.Table(n); ok {
			t.Errorf("aborted txn's table %q leaked into catalog", n)
		}
		exists, err := sqlitebridge.ObjectExists(f.app, "table", n)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Errorf("aborted txn's table %q still exists in SQLite", n)
		}
	}
}

// A table created and dropped inside one transaction: the overlay must
// track both, and the post-state check must not misread the drop as
// "the CREATE never landed".
func TestDDL_TxnCreateThenDropSameTable(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE tmp1 (id BLOB PRIMARY KEY NOT NULL)`,
		`CREATE TABLE keep (id BLOB PRIMARY KEY NOT NULL)`,
		`DROP TABLE tmp1`,
		`COMMIT`,
	)
	if head := f.head(t); head != 1 {
		t.Fatalf("schema log head = %d, want 1", head)
	}
	op := f.decodeHead(t)
	if op.Kind != crdt.OpBundle || len(op.SubOps) != 3 {
		t.Fatalf("head op = %v with %d sub-ops, want a 3-op bundle", op.Kind, len(op.SubOps))
	}
	if _, ok := f.cat.Table("keep"); !ok {
		t.Error("keep missing from catalog")
	}
	if _, ok := f.cat.Table("tmp1"); ok {
		t.Error("tmp1 still active in catalog after same-txn DROP")
	}
}

// One statement failing mid-transaction drops only its own op; the
// statements that did take effect still replicate.
func TestDDL_TxnDropsOnlyTheFailedStatement(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT, w TEXT)`,
		`INSERT INTO t (id, v, w) VALUES (x'01', 'dup', 'a')`,
		`INSERT INTO t (id, v, w) VALUES (x'02', 'dup', 'b')`,
	)
	before := f.head(t)

	f.mustExec(t, `BEGIN`, `CREATE INDEX idx_t_w ON t (w)`)
	if err := f.app.Exec(`CREATE UNIQUE INDEX u_t_v ON t (v)`); err == nil {
		t.Fatal("CREATE UNIQUE INDEX on duplicate data unexpectedly succeeded")
	}
	f.mustExec(t, `CREATE INDEX idx_t_v2 ON t (v)`, `COMMIT`)

	if head := f.head(t); head != before+1 {
		t.Fatalf("schema log head = %d, want %d", head, before+1)
	}
	op := f.decodeHead(t)
	if op.Kind != crdt.OpBundle {
		t.Fatalf("head op kind = %v, want OpBundle", op.Kind)
	}
	if len(op.SubOps) != 2 {
		t.Errorf("bundle carries %d sub-ops, want 2 (the failed unique index dropped)", len(op.SubOps))
	}
	for _, sub := range op.SubOps {
		if sub.Kind == crdt.OpAddUniqueKey {
			t.Error("the failed CREATE UNIQUE INDEX was published")
		}
	}
	tab, _ := f.cat.Table("t")
	if len(tab.UniqueKeys) != 0 {
		t.Errorf("failed CREATE UNIQUE INDEX installed %d unique key(s)", len(tab.UniqueKeys))
	}
}

// Savepoints and cascade-FK bundles stay rejected.
func TestDDL_TxnMultiDDLStillRejectsSavepoint(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`,
		`SAVEPOINT sp`,
	)
	if err := f.app.Exec(`CREATE TABLE b (id BLOB PRIMARY KEY NOT NULL)`); err == nil {
		t.Fatal("DDL under SAVEPOINT was admitted; want rejection")
	}
	_ = f.app.Exec(`ROLLBACK`)
	if head := f.head(t); head != 0 {
		t.Errorf("schema log head = %d, want 0", head)
	}
}

// When a statement is dropped because its body failed, the overlay must
// be rebuilt without it: a later statement in the same transaction has
// to see the schema that really exists, not the one the failed
// statement claimed. Here the failed DROP TABLE must leave p visible,
// so re-creating it is correctly rejected as a duplicate.
func TestDDL_TxnOverlayRebuiltAfterDroppedStatement(t *testing.T) {
	f := newDDLFixture(t)
	f.fkParent(t)
	before := f.head(t)

	f.mustExec(t, `BEGIN`)
	if err := f.app.Exec(`DROP TABLE p`); err == nil {
		t.Fatal("DROP TABLE with FK dependents unexpectedly succeeded")
	}
	// The drop did not happen, so p is still there and this must fail.
	if err := f.app.Exec(`CREATE TABLE p (id BLOB PRIMARY KEY NOT NULL)`); err == nil {
		t.Error("re-creating a table whose DROP failed was admitted; the overlay still thought it was dropped")
	}
	_ = f.app.Exec(`ROLLBACK`)

	if head := f.head(t); head != before {
		t.Errorf("schema log head = %d, want %d", head, before)
	}
	if _, ok := f.cat.Table("p"); !ok {
		t.Error("p missing from catalog")
	}
}

// The last statement before COMMIT has only COMMIT's own trace_v2 left to
// settle it; a body that failed there must still drop out of the event.
func TestDDL_TxnLastStatementFailsBeforeCommit(t *testing.T) {
	f := newDDLFixture(t)
	f.dupTable(t)
	before := f.head(t)

	f.mustExec(t, `BEGIN`, `CREATE INDEX idx_t_id ON t (id)`)
	if err := f.app.Exec(`CREATE UNIQUE INDEX u_t_v ON t (v)`); err == nil {
		t.Fatal("CREATE UNIQUE INDEX on duplicate data unexpectedly succeeded")
	}
	f.mustExec(t, `COMMIT`)

	if head := f.head(t); head != before+1 {
		t.Fatalf("schema log head = %d, want %d", head, before+1)
	}
	if op := f.decodeHead(t); op.Kind != crdt.OpCreateIndex {
		t.Errorf("head op = %v, want a lone OpCreateIndex", op.Kind)
	}
}

// Every statement failing leaves nothing to publish: no event, no intent,
// and the COMMIT still succeeds.
func TestDDL_TxnAllStatementsFailPublishesNothing(t *testing.T) {
	f := newDDLFixture(t)
	f.dupTable(t)
	before := f.head(t)

	f.mustExec(t, `BEGIN`)
	for _, sql := range []string{
		`CREATE UNIQUE INDEX u_t_v ON t (v)`,
		`CREATE UNIQUE INDEX u2_t_v ON t (v)`,
	} {
		if err := f.app.Exec(sql); err == nil {
			t.Fatalf("%s on duplicate data unexpectedly succeeded", sql)
		}
	}
	f.mustExec(t, `COMMIT`)

	if head := f.head(t); head != before {
		t.Errorf("schema log head = %d, want %d (nothing published)", head, before)
	}
	if f.intentPresent(t) {
		t.Error("intent left behind")
	}
}

// Renaming and then dropping under the new name exercises overlay
// shadowing in both directions within one transaction.
func TestDDL_TxnRenameThenDrop(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t, `CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`)
	before := f.head(t)

	f.mustExec(t, `BEGIN`, `ALTER TABLE a RENAME TO b`, `DROP TABLE b`, `COMMIT`)

	if head := f.head(t); head != before+1 {
		t.Fatalf("schema log head = %d, want %d", head, before+1)
	}
	op := f.decodeHead(t)
	if op.Kind != crdt.OpBundle || len(op.SubOps) != 2 {
		t.Fatalf("head op = %v with %d sub-ops, want OpBundle with 2", op.Kind, len(op.SubOps))
	}
	for _, n := range []string{"a", "b"} {
		if _, ok := f.cat.Table(n); ok {
			t.Errorf("table %q still in catalog", n)
		}
	}
}

// Dropping and re-creating the same name in one transaction: the second
// CREATE must resolve against the overlay's dropped state and get a fresh
// TableID rather than colliding with the tombstone.
func TestDDL_TxnDropThenRecreateSameName(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t, `CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`)
	old, ok := f.cat.Table("a")
	if !ok {
		t.Fatal("a missing from catalog")
	}
	oldID := old.ID
	before := f.head(t)

	f.mustExec(t,
		`BEGIN`,
		`DROP TABLE a`,
		`CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL, v TEXT)`,
		`COMMIT`,
	)

	if head := f.head(t); head != before+1 {
		t.Fatalf("schema log head = %d, want %d", head, before+1)
	}
	tab, ok := f.cat.Table("a")
	if !ok {
		t.Fatal("a missing from catalog after recreate")
	}
	if tab.ID == oldID {
		t.Error("recreated table reused the dropped TableID")
	}
	if _, ok := tab.Column("v"); !ok {
		t.Error("recreated table missing column v")
	}
}

// A rejected DDL anywhere in the transaction poisons the COMMIT: the
// statements that WERE admitted must not publish on their own.
func TestDDL_TxnRejectedDDLPoisonsCommit(t *testing.T) {
	f := newDDLFixture(t)
	before := f.head(t)

	f.mustExec(t, `BEGIN`, `CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`)
	if err := f.app.Exec(`CREATE TABLE b (id BLOB)`); err == nil {
		t.Fatal("table without a PRIMARY KEY was admitted")
	}
	if err := f.app.Exec(`COMMIT`); err == nil {
		t.Error("COMMIT succeeded after a rejected DDL in the transaction")
	}
	if head := f.head(t); head != before {
		t.Errorf("schema log head = %d, want %d", head, before)
	}
	// The connection is usable again: no per-txn state leaked.
	f.mustExec(t, `CREATE TABLE c (id BLOB PRIMARY KEY NOT NULL)`)
}

// ADD COLUMN IF NOT EXISTS is rewritten by a preprocessor that runs
// before trace_v2; it must see the transaction's own pending DDL, or a
// column this transaction just added reaches SQLite as a duplicate.
func TestDDL_TxnAddColumnIfNotExistsSeesPendingDDL(t *testing.T) {
	f := newDDLFixture(t)
	f.mustExec(t, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`)
	f.mustExec(t,
		`BEGIN`,
		`ALTER TABLE t ADD COLUMN c TEXT`,
		`ALTER TABLE t ADD COLUMN IF NOT EXISTS c TEXT`,
		`COMMIT`,
	)
	tab, ok := f.cat.Table("t")
	if !ok {
		t.Fatal("t missing from catalog")
	}
	if _, ok := tab.Column("c"); !ok {
		t.Error("column c missing")
	}
	if op := f.decodeHead(t); op.Kind != crdt.OpAddColumn {
		t.Errorf("head op = %v, want a lone OpAddColumn (the IF NOT EXISTS form no-ops)", op.Kind)
	}
}

// The redundant-DDL preprocessor queries app.db on the writer connection,
// which SQLite traces back into admission; a multi-DDL transaction must
// still bundle every statement.
func TestDDL_TxnMultiDDLUnderIdempotentDDL(t *testing.T) {
	f := newDDLFixtureCfg(t, Config{IdempotentDDL: true})
	f.mustExec(t,
		`BEGIN`,
		`CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`,
		`CREATE TABLE b (id BLOB PRIMARY KEY NOT NULL)`,
		`ALTER TABLE a ADD COLUMN v TEXT`,
		`CREATE INDEX ia ON a (v)`,
		`COMMIT`,
	)
	if head := f.head(t); head != 1 {
		t.Fatalf("schema log head = %d, want 1", head)
	}
	op := f.decodeHead(t)
	if op.Kind != crdt.OpBundle || len(op.SubOps) != 4 {
		t.Fatalf("head op = %v with %d sub-ops, want OpBundle with 4", op.Kind, len(op.SubOps))
	}
}
