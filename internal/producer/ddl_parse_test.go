package producer

import (
	"errors"
	"testing"
)

func TestClassifyDDL_CreateTable_Basic(t *testing.T) {
	d, err := classifyDDL(`CREATE TABLE users (id BLOB PRIMARY KEY NOT NULL, email TEXT)`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if d.Kind != ddlCreateTable {
		t.Fatalf("Kind = %v", d.Kind)
	}
	if d.Name != "users" {
		t.Errorf("Name = %q; want users", d.Name)
	}
	if len(d.Columns) != 2 {
		t.Fatalf("Columns = %d; want 2", len(d.Columns))
	}
	if d.Columns[0].Name != "id" || d.Columns[0].Type != "BLOB" || !d.Columns[0].NotNull || !d.Columns[0].IsPK {
		t.Errorf("col[0] = %+v", d.Columns[0])
	}
	if d.Columns[1].Name != "email" || d.Columns[1].Type != "TEXT" {
		t.Errorf("col[1] = %+v", d.Columns[1])
	}
	if len(d.PKColumns) != 1 || d.PKColumns[0] != "id" {
		t.Errorf("PKColumns = %v", d.PKColumns)
	}
}

func TestClassifyDDL_CreateTable_TableLevelPK(t *testing.T) {
	d, err := classifyDDL(`CREATE TABLE pair (a INT NOT NULL, b INT NOT NULL, c TEXT, PRIMARY KEY (b, a))`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if d.Kind != ddlCreateTable {
		t.Fatalf("Kind = %v", d.Kind)
	}
	if len(d.PKColumns) != 2 || d.PKColumns[0] != "b" || d.PKColumns[1] != "a" {
		t.Errorf("PKColumns = %v", d.PKColumns)
	}
}

func TestClassifyDDL_CreateTable_IfNotExists(t *testing.T) {
	d, _ := classifyDDL(`CREATE TABLE IF NOT EXISTS t (id BLOB PRIMARY KEY)`)
	if !d.IfNotExists {
		t.Error("IfNotExists not set")
	}
}

func TestClassifyDDL_CreateTable_DefaultExpr(t *testing.T) {
	d, err := classifyDDL(`CREATE TABLE t (id BLOB PRIMARY KEY DEFAULT (uuidv7()), n TEXT)`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// Whitespace inside the parenthesized default expression is
	// preserved verbatim; receivers re-parse the SQL via SQLite which
	// accepts any whitespace between identifier and arg parens.
	if d.Columns[0].Default == "" {
		t.Errorf("Default empty; want non-empty parenthesized expr, got %+v", d.Columns[0])
	}
}

func TestClassifyDDL_CreateTable_Unique(t *testing.T) {
	d, err := classifyDDL(`CREATE TABLE u (id INT PRIMARY KEY, slug TEXT UNIQUE, email TEXT)`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(d.UniqueKeys) != 1 || d.UniqueKeys[0][0] != "slug" {
		t.Errorf("UniqueKeys = %v", d.UniqueKeys)
	}
}

func TestClassifyDDL_CreateTable_TableLevelUnique(t *testing.T) {
	d, _ := classifyDDL(`CREATE TABLE u (a INT PRIMARY KEY, b TEXT, c TEXT, UNIQUE (b, c))`)
	if len(d.UniqueKeys) != 1 || len(d.UniqueKeys[0]) != 2 {
		t.Errorf("UniqueKeys = %v", d.UniqueKeys)
	}
}

func TestClassifyDDL_CreateTable_RejectsCTAS(t *testing.T) {
	if _, err := classifyDDL(`CREATE TABLE t AS SELECT 1`); !errors.Is(err, ErrUnsupportedDDL) {
		t.Errorf("CTAS: err=%v", err)
	}
}

func TestClassifyDDL_AddColumn(t *testing.T) {
	d, err := classifyDDL(`ALTER TABLE t ADD COLUMN extra TEXT NOT NULL DEFAULT 'x'`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if d.Kind != ddlAlterTableAddColumn {
		t.Fatalf("Kind = %v", d.Kind)
	}
	if d.Name != "t" || d.Columns[0].Name != "extra" || !d.Columns[0].NotNull {
		t.Errorf("d = %+v", d)
	}
}

func TestClassifyDDL_RenameTable(t *testing.T) {
	d, _ := classifyDDL(`ALTER TABLE old RENAME TO neu`)
	if d.Kind != ddlAlterTableRenameTo || d.Name != "old" || d.NewName != "neu" {
		t.Errorf("d = %+v", d)
	}
}

func TestClassifyDDL_RenameColumn(t *testing.T) {
	d, _ := classifyDDL(`ALTER TABLE t RENAME COLUMN old TO neu`)
	if d.Kind != ddlAlterTableRenameColumn || d.OldColumn != "old" || d.NewColumn != "neu" {
		t.Errorf("d = %+v", d)
	}
	d, _ = classifyDDL(`ALTER TABLE t RENAME old TO neu`)
	if d.Kind != ddlAlterTableRenameColumn || d.OldColumn != "old" || d.NewColumn != "neu" {
		t.Errorf("implicit COLUMN: %+v", d)
	}
}

func TestClassifyDDL_DropColumn(t *testing.T) {
	d, _ := classifyDDL(`ALTER TABLE t DROP COLUMN old`)
	if d.Kind != ddlAlterTableDropColumn || d.DropColumn != "old" {
		t.Errorf("d = %+v", d)
	}
}

func TestClassifyDDL_DropTable(t *testing.T) {
	d, _ := classifyDDL(`DROP TABLE t`)
	if d.Kind != ddlDropTable || d.Name != "t" {
		t.Errorf("d = %+v", d)
	}
	d, _ = classifyDDL(`DROP TABLE IF EXISTS t`)
	if !d.IfExists {
		t.Error("IfExists not set")
	}
}

func TestClassifyDDL_CreateIndex(t *testing.T) {
	d, _ := classifyDDL(`CREATE INDEX idx_users_email ON users (email)`)
	if d.Kind != ddlCreateIndex || d.Name != "idx_users_email" || d.IndexTable != "users" || d.IndexColumns[0] != "email" {
		t.Errorf("d = %+v", d)
	}
}

func TestClassifyDDL_CreateUniqueIndex(t *testing.T) {
	d, _ := classifyDDL(`CREATE UNIQUE INDEX uq ON users (slug)`)
	if d.Kind != ddlCreateUniqueIndex {
		t.Errorf("d = %+v", d)
	}
}

func TestClassifyDDL_PartialUniqueIndexParsed(t *testing.T) {
	// Partial unique indexes are accepted at classify time and carry the
	// predicate AST; coordinated-mode + grammar validation happens later
	// in buildCatalogOp, where the catalog resolves column IDs.
	d, err := classifyDDL(`CREATE UNIQUE INDEX uq ON t (col) WHERE col IS NOT NULL`)
	if err != nil {
		t.Fatalf("partial: err=%v", err)
	}
	if d.Kind != ddlCreateUniqueIndex {
		t.Fatalf("kind = %v, want ddlCreateUniqueIndex", d.Kind)
	}
	if !d.HasWhereClause || d.WherePred == nil {
		t.Fatalf("expected WHERE clause captured: HasWhereClause=%v WherePred=%v", d.HasWhereClause, d.WherePred)
	}
}

func TestClassifyDDL_DropIndex(t *testing.T) {
	d, _ := classifyDDL(`DROP INDEX idx`)
	if d.Kind != ddlDropIndex || d.Name != "idx" {
		t.Errorf("d = %+v", d)
	}
}

func TestClassifyDDL_View(t *testing.T) {
	d, _ := classifyDDL(`CREATE VIEW v AS SELECT 1`)
	if d.Kind != ddlCreateView || d.Name != "v" {
		t.Errorf("d = %+v", d)
	}
	d, _ = classifyDDL(`DROP VIEW v`)
	if d.Kind != ddlDropView {
		t.Errorf("drop view: %+v", d)
	}
}

func TestClassifyDDL_VTab(t *testing.T) {
	d, _ := classifyDDL(`CREATE VIRTUAL TABLE fts USING fts5(body)`)
	if d.Kind != ddlCreateVirtualTable || d.Name != "fts" {
		t.Errorf("d = %+v", d)
	}
}

func TestClassifyDDL_FK_ColumnLevel(t *testing.T) {
	d, err := classifyDDL(`CREATE TABLE c (id INT PRIMARY KEY, parent_id INT REFERENCES p(id) ON DELETE CASCADE)`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(d.FKs) != 1 {
		t.Fatalf("FKs = %d; want 1", len(d.FKs))
	}
	fk := d.FKs[0]
	if fk.RefTable != "p" || len(fk.Cols) != 1 || fk.Cols[0] != "parent_id" {
		t.Errorf("fk = %+v", fk)
	}
	if len(fk.RefCols) != 1 || fk.RefCols[0] != "id" {
		t.Errorf("RefCols = %v", fk.RefCols)
	}
	if fk.OnDelete != fkCascade {
		t.Errorf("OnDelete = %v; want fkCascade", fk.OnDelete)
	}
}

func TestClassifyDDL_FK_TableLevel(t *testing.T) {
	d, err := classifyDDL(`CREATE TABLE c (a INT, b INT, PRIMARY KEY(a, b),
		FOREIGN KEY (a, b) REFERENCES p(x, y) ON DELETE SET NULL ON UPDATE CASCADE)`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(d.FKs) != 1 {
		t.Fatalf("FKs = %d; want 1", len(d.FKs))
	}
	fk := d.FKs[0]
	if fk.RefTable != "p" || len(fk.Cols) != 2 || len(fk.RefCols) != 2 {
		t.Errorf("fk = %+v", fk)
	}
	if fk.OnDelete != fkSetNull || fk.OnUpdate != fkCascade {
		t.Errorf("actions = %v / %v", fk.OnDelete, fk.OnUpdate)
	}
}

func TestClassifyDDL_FK_NoCascade(t *testing.T) {
	d, _ := classifyDDL(`CREATE TABLE c (id INT PRIMARY KEY, p INT REFERENCES p(id))`)
	if len(d.FKs) != 1 {
		t.Fatalf("FKs = %d; want 1", len(d.FKs))
	}
	if d.FKs[0].HasCascade() {
		t.Errorf("plain FK should not be cascade")
	}
}

func TestClassifyDDL_Trigger(t *testing.T) {
	d, err := classifyDDL(`CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT 1; END`)
	if err != nil || d.Kind != ddlCreateTrigger || d.Name != "tr" {
		t.Errorf("CREATE TRIGGER: d=%+v err=%v", d, err)
	}
	d, err = classifyDDL(`CREATE TRIGGER IF NOT EXISTS tr2 AFTER UPDATE ON t BEGIN SELECT 1; END`)
	if err != nil || d.Kind != ddlCreateTrigger || d.Name != "tr2" || !d.IfNotExists {
		t.Errorf("CREATE TRIGGER IF NOT EXISTS: d=%+v err=%v", d, err)
	}
	d, err = classifyDDL(`DROP TRIGGER tr`)
	if err != nil || d.Kind != ddlDropTrigger || d.Name != "tr" {
		t.Errorf("DROP TRIGGER: d=%+v err=%v", d, err)
	}
}

func TestClassifyDDL_BeginIsTagged(t *testing.T) {
	d, _ := classifyDDL(`BEGIN`)
	if d.Kind != ddlBeginOrSavepoint {
		t.Errorf("BEGIN: %+v", d)
	}
}

func TestClassifyDDL_DML_NotDDL(t *testing.T) {
	for _, sql := range []string{
		`INSERT INTO t VALUES (1)`,
		`UPDATE t SET x = 1`,
		`DELETE FROM t`,
		`SELECT 1`,
	} {
		d, err := classifyDDL(sql)
		if err != nil {
			t.Errorf("%q: err=%v", sql, err)
		}
		if d.Kind != ddlNone {
			t.Errorf("%q: Kind = %v; want ddlNone", sql, d.Kind)
		}
	}
}

func TestClassifyDDL_QuotedNames(t *testing.T) {
	d, err := classifyDDL(`CREATE TABLE "user_data" ("id" BLOB PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if d.Name != "user_data" {
		t.Errorf("Name = %q", d.Name)
	}
	if d.Columns[0].Name != "id" {
		t.Errorf("col name = %q", d.Columns[0].Name)
	}
}

func TestClassifyDDL_TypelessColumn(t *testing.T) {
	d, err := classifyDDL(`CREATE TABLE t (id PRIMARY KEY, body)`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if d.Columns[0].Type != "" || !d.Columns[0].IsPK {
		t.Errorf("col[0] = %+v", d.Columns[0])
	}
}

func TestClassifyDDL_TypeWithLengthArg(t *testing.T) {
	d, _ := classifyDDL(`CREATE TABLE t (id INT PRIMARY KEY, n VARCHAR(255), p DECIMAL(10, 2))`)
	if d.Columns[1].Type != "VARCHAR(255)" {
		t.Errorf("VARCHAR type = %q", d.Columns[1].Type)
	}
	if d.Columns[2].Type != "DECIMAL(10, 2)" {
		t.Errorf("DECIMAL type = %q", d.Columns[2].Type)
	}
}

// Multi-word type names with parenthesized args (CHARACTER VARYING(20)
// is the SQL-standard spelling, and "DOUBLE PRECISION" appears in
// migrations from other dialects) must preserve both the multi-word
// type body and the args.
func TestClassifyDDL_MultiWordTypeWithLengthArg(t *testing.T) {
	d, _ := classifyDDL(`CREATE TABLE t (id INT PRIMARY KEY, n CHARACTER VARYING(20))`)
	if d.Columns[1].Type != "CHARACTER VARYING(20)" {
		t.Errorf("CHARACTER VARYING type = %q", d.Columns[1].Type)
	}
}

func TestClassifyDDL_MultiPKAndUnique(t *testing.T) {
	d, _ := classifyDDL(`CREATE TABLE u (a INT, b INT, c TEXT, PRIMARY KEY (a, b), UNIQUE (c))`)
	if len(d.PKColumns) != 2 {
		t.Errorf("PK = %v", d.PKColumns)
	}
	if len(d.UniqueKeys) != 1 {
		t.Errorf("UNIQUE = %v", d.UniqueKeys)
	}
}

func TestClassifyDDL_GeneratedColumnFlag(t *testing.T) {
	d, _ := classifyDDL(`CREATE TABLE t (a INT PRIMARY KEY, b INT GENERATED ALWAYS AS (a * 2) STORED)`)
	if !d.Columns[1].Generated {
		t.Errorf("col[1].Generated = false")
	}
}
