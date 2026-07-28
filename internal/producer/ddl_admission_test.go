package producer

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/unique"
)

// ddlFixture wires up a producer with DDL admission enabled, against
// a freshly-initialized app.db with no prior schema. Tests run DDL
// statements via the writer connection and inspect the metadata
// catalog + schema log afterward.
type ddlFixture struct {
	dir   string
	app   *sqlitebridge.Conn
	sc    *metadata.Store
	cat   *catalog.Catalog
	cache *nodestate.Cache
	log   *schemalog.Local
	prod  *Producer
}

func newDDLFixture(t *testing.T) *ddlFixture {
	return newDDLFixtureCfg(t, Config{})
}

// newDDLFixtureCfg is the underlying builder. tweak is applied to the
// producer Config after JournalDir/Cache/SchemaLog are filled in, so
// callers can flip optional fields (ReplicateUnderscoreTables, etc.)
// without restating the required wiring.
func newDDLFixtureCfg(t *testing.T, tweak Config) *ddlFixture {
	t.Helper()
	dir := t.TempDir()
	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		t.Fatalf("WAL: %v", err)
	}

	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })

	if err := sc.SetClusterID(crdt.ClusterID{0xCC}); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(crdt.Origin(1)); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}

	cat, err := catalog.LoadFromMeta(sc)
	if err != nil {
		t.Fatalf("LoadFromMeta: %v", err)
	}
	cache := nodestate.New(crdt.Origin(1))
	log := schemalog.NewLocal()
	// Helper connection, as production wires whenever DDL replication is
	// on: cascade-trigger synthesis and coordinated-index normalization
	// run on it.
	helper, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open helper: %v", err)
	}
	t.Cleanup(func() { _ = helper.Close() })
	cfg := tweak
	cfg.JournalDir = filepath.Join(dir, "jrn")
	cfg.Cache = cache
	cfg.SchemaLog = log
	cfg.AppHelper = helper
	prod, err := New(app, sc, cat, cfg)
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	t.Cleanup(func() { _ = prod.Close() })
	// No catch-up authority runs in this fixture; keep the CAS-loss
	// catch-up wait short so head-moved rejection tests stay fast.
	prod.ddl.catchupWait = 50 * time.Millisecond
	return &ddlFixture{
		dir: dir, app: app, sc: sc, cat: cat,
		cache: cache, log: log, prod: prod,
	}
}

// waitDrain spins until the producer's drainer has caught up to the
// journal head, with a short bound. DDL statements append a KindEmpty
// record from wal_hook; tests that inspect the catalog after a DDL
// don't strictly need this (resolve_intent runs synchronously inside
// wal_hook), but it keeps test ordering deterministic.
func (f *ddlFixture) waitDrain(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.prod.WaitForDrain(ctx); err != nil {
		t.Fatalf("WaitForDrain: %v", err)
	}
}

// TestDDL_RejectsNoPK confirms admission rejects a CREATE TABLE with
// no PRIMARY KEY column — the exact case a host-platform persistence smoke
// hits with `CREATE TABLE IF NOT EXISTS t(x INT)`. Documents the
// surface so the smoke's KNOWN-FAIL is traced to admission validation
// (not the schemalog race the handoff doc suspected).
func TestDDL_RejectsNoPK(t *testing.T) {
	f := newDDLFixture(t)
	err := f.app.Exec(`CREATE TABLE IF NOT EXISTS t(x INT)`)
	if err == nil {
		t.Fatalf("expected admission rejection for no-PK CREATE TABLE, got nil")
	}
}

func TestDDL_CreateTable_AppliesCatalog(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE users (id BLOB PRIMARY KEY NOT NULL, email TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	f.waitDrain(t)

	// Schema log advanced.
	head, err := f.log.Head(context.Background())
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head != 1 {
		t.Errorf("schema log head = %d; want 1", head)
	}

	// Meta catalog now contains the table.
	snap, err := f.sc.LoadCatalogSnapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap.Tables) != 1 || snap.Tables[0].Name != "users" {
		t.Errorf("snap.Tables = %+v", snap.Tables)
	}
	if len(snap.Columns) != 2 {
		t.Errorf("snap.Columns = %d; want 2", len(snap.Columns))
	}
	if len(snap.Keys) != 1 {
		t.Errorf("snap.Keys = %d; want 1 (PK)", len(snap.Keys))
	}

	// schema_seq advanced + intent cleared.
	seq, _, _ := f.sc.GetSchemaSeq()
	if seq != 1 {
		t.Errorf("schema_seq = %d; want 1", seq)
	}
	if all, _ := f.sc.ListIntents(); len(all) != 0 {
		t.Errorf("intent not cleared")
	}

	// Catalog reload picked up the new table.
	tab, ok := f.cat.Table("users")
	if !ok {
		t.Errorf("cat.Table(users) missing")
	} else if len(tab.PK) != 1 || tab.PK[0].Name != "id" {
		t.Errorf("PK = %+v", tab.PK)
	}
}

func TestDDL_AddColumn_AppliesCatalog(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := f.app.Exec(`ALTER TABLE t ADD COLUMN extra TEXT`); err != nil {
		t.Fatalf("ALTER: %v", err)
	}
	f.waitDrain(t)
	head, _ := f.log.Head(context.Background())
	if head != 2 {
		t.Errorf("head = %d; want 2", head)
	}
	tab, _ := f.cat.Table("t")
	if _, ok := tab.Column("extra"); !ok {
		t.Errorf("extra column not in catalog")
	}
}

// TestDDL_AddColumnIfNotExists_NoOpWhenPresent exercises the
// preprocessor support for the non-standard
// `ALTER TABLE … ADD COLUMN IF NOT EXISTS …` form. SQLite detects
// duplicate columns at PREPARE time — before trace_v2 / admission fires
// — so without the preprocessor's text-level rewrite a fresh-cluster
// migration whose ALTER targets a column the baseline already shipped
// would surface SQLITE_ERROR ("duplicate column name") and break the
// `recordAllMigrationsApplied` skip path callers rely on.
//
// Two cases:
//   - column already present → preprocessor rewrites to SELECT no-op,
//     Exec succeeds, catalog unchanged.
//   - column absent → preprocessor strips "IF NOT EXISTS", ALTER runs
//     through admission, catalog gets the new column.
func TestDDL_AddColumnIfNotExists_NoOpWhenPresent(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE vms (id TEXT PRIMARY KEY NOT NULL, host_id INTEGER NOT NULL DEFAULT 0)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	f.waitDrain(t)

	// Sanity: catalog has host_id from the CREATE.
	tab, ok := f.cat.Table("vms")
	if !ok {
		t.Fatal("cat.Table(vms) missing")
	}
	if _, ok := tab.Column("host_id"); !ok {
		t.Fatal("catalog missing host_id after CREATE")
	}

	// ADD COLUMN IF NOT EXISTS on the existing column: must succeed
	// as a no-op (preprocessor rewrites to SELECT 1).
	if err := f.app.Exec(`ALTER TABLE vms ADD COLUMN IF NOT EXISTS host_id INTEGER NOT NULL DEFAULT 0`); err != nil {
		t.Fatalf("ADD COLUMN IF NOT EXISTS on existing column: %v", err)
	}

	// ADD COLUMN IF NOT EXISTS on a new column: should add it.
	if err := f.app.Exec(`ALTER TABLE vms ADD COLUMN IF NOT EXISTS region TEXT`); err != nil {
		t.Fatalf("ADD COLUMN IF NOT EXISTS on new column: %v", err)
	}
	f.waitDrain(t)
	tab, _ = f.cat.Table("vms")
	if _, ok := tab.Column("region"); !ok {
		t.Errorf("region column not added to catalog")
	}

	// Plain ADD COLUMN (no IF NOT EXISTS) on the existing column still
	// errors — opt-in idempotence, matching CREATE TABLE [IF NOT EXISTS].
	if err := f.app.Exec(`ALTER TABLE vms ADD COLUMN host_id INTEGER NOT NULL DEFAULT 0`); err == nil {
		t.Errorf("plain ADD COLUMN on existing column: expected error, got nil")
	}
}

// TestDDL_IdempotentDDL_RedundantOpsNoOp: under IdempotentDDL a redundant
// DDL of every form (incl. DROP, which has no idempotent syntax) is a
// no-op success on the writer path.
func TestDDL_IdempotentDDL_RedundantOpsNoOp(t *testing.T) {
	f := newDDLFixtureCfg(t, Config{IdempotentDDL: true})
	exec := func(sql string) {
		t.Helper()
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Multi-line CREATE: the no-op rewrite must not leak the body back as
	// live SQL (a `--` comment ends at the first newline).
	const createT = `CREATE TABLE t (
		id TEXT PRIMARY KEY NOT NULL,
		a  TEXT,
		b  TEXT
	)`
	exec(createT)
	exec(`CREATE INDEX idx_t_a ON t(a)`)
	f.waitDrain(t)

	// Redundant CREATE TABLE / CREATE INDEX (no IF NOT EXISTS) → no-op.
	exec(createT)
	exec(`CREATE INDEX idx_t_a ON t(a)`)
	// Redundant plain ADD COLUMN (column present) → no-op.
	exec(`ALTER TABLE t ADD COLUMN a TEXT`)

	// Real DROP COLUMN applies...
	exec(`ALTER TABLE t DROP COLUMN b`)
	f.waitDrain(t)
	tab, _ := f.cat.Table("t")
	if _, ok := tab.Column("b"); ok {
		t.Fatal("column b still present after DROP COLUMN")
	}
	// ...and a redundant DROP COLUMN (already gone) → no-op. THE motivating case.
	exec(`ALTER TABLE t DROP COLUMN b`)

	// Redundant DROP INDEX / DROP TABLE (no IF EXISTS) after real drops.
	exec(`DROP INDEX idx_t_a`)
	exec(`DROP INDEX idx_t_a`)
	exec(`DROP TABLE t`)
	exec(`DROP TABLE t`)

	// Redundant CREATE/DROP VIEW.
	exec(`CREATE VIEW v AS SELECT 1`)
	exec(`CREATE VIEW v AS SELECT 1`)
	exec(`DROP VIEW v`)
	exec(`DROP VIEW v`)

	// A genuinely new statement still applies (flag doesn't swallow real DDL).
	exec(`CREATE TABLE t2 (id TEXT PRIMARY KEY NOT NULL)`)
	f.waitDrain(t)
	if _, ok := f.cat.Table("t2"); !ok {
		t.Fatal("new table t2 not created under IdempotentDDL")
	}

	// Redundant CREATE/DROP TRIGGER (parity with the receiver's idempotency).
	exec(`CREATE TRIGGER trg AFTER INSERT ON t2 BEGIN SELECT 1; END`)
	exec(`CREATE TRIGGER trg AFTER INSERT ON t2 BEGIN SELECT 1; END`)
	exec(`DROP TRIGGER trg`)
	exec(`DROP TRIGGER trg`)
}

func TestDDL_DropTable_TombstonesEntry(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	tabBefore, _ := f.cat.Table("t")
	if err := f.app.Exec(`DROP TABLE t`); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	f.waitDrain(t)
	if _, ok := f.cat.Table("t"); ok {
		t.Errorf("Table(t) still active after DROP")
	}
	if got, ok := f.cat.TableByID(tabBefore.ID); !ok || !got.Dropped() {
		t.Errorf("TableByID = (%+v, %v); want Dropped()=true", got, ok)
	}
}

func TestDDL_RejectsGenIDArgMismatch(t *testing.T) {
	f := newDDLFixture(t)
	err := f.app.Exec(`CREATE TABLE doc (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('wrong')))`)
	if err == nil {
		t.Errorf("gen_id('wrong') accepted on table doc; want rejection")
	}
	if head, _ := f.log.Head(context.Background()); head != 0 {
		t.Errorf("schema log advanced on rejected DDL: head=%d", head)
	}
}

// Bare gen_id() is auto-qualified by the preprocessor (see
// TestDDL_BareGenIDQualifiedInCreateTable); admission's bypass-path
// rejection of an unqualified call is exercised at the unit level via
// validatePKDefault.
func TestDDL_BareGenIDBypassRejectedAtAdmission(t *testing.T) {
	err := validatePKDefault("doc", parsedColumn{Name: "id", Default: "gen_id()"})
	if err == nil {
		t.Errorf("bare gen_id() bypassing the preprocessor accepted; want rejection")
	}
}

func TestDDL_AcceptsGenIDStrictForm(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE doc (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('doc')))`); err != nil {
		t.Fatalf("strict gen_id('doc') rejected: %v", err)
	}
	f.waitDrain(t)
	tab, ok := f.cat.Table("doc")
	if !ok {
		t.Fatal("doc not in catalog")
	}
	if len(tab.PK) != 1 {
		t.Fatalf("PK len = %d", len(tab.PK))
	}
}

func TestDDL_RejectsNotNullUnique(t *testing.T) {
	// Without a reservation backend (no UniqueRegistry, the default), a
	// NOT NULL UNIQUE (coordinated) key cannot be enforced, so it is
	// rejected at admission.
	f := newDDLFixture(t)
	err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, slug TEXT NOT NULL UNIQUE)`)
	if err == nil {
		t.Errorf("NOT NULL UNIQUE accepted; want rejection (no reservation backend configured)")
	}
	if head, _ := f.log.Head(context.Background()); head != 0 {
		t.Errorf("schema log advanced on rejected DDL: head=%d", head)
	}
}

func TestDDL_RejectsBlobUnique(t *testing.T) {
	f := newDDLFixture(t)
	err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, payload BLOB UNIQUE)`)
	if err == nil {
		t.Errorf("UNIQUE on BLOB accepted; want rejection")
	}
	if head, _ := f.log.Head(context.Background()); head != 0 {
		t.Errorf("schema log advanced on rejected DDL: head=%d", head)
	}
}

func TestDDL_RejectsNotNullUniqueIndex(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, slug TEXT NOT NULL)`); err != nil {
		t.Fatalf("create base table: %v", err)
	}
	f.waitDrain(t)
	err := f.app.Exec(`CREATE UNIQUE INDEX u_slug ON u(slug)`)
	if err == nil {
		t.Errorf("CREATE UNIQUE INDEX on NOT NULL column accepted; want rejection")
	}
}

func TestDDL_AcceptsNullableUnique(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, slug TEXT, UNIQUE(slug))`); err != nil {
		t.Fatalf("nullable UNIQUE rejected: %v", err)
	}
	f.waitDrain(t)
	tab, ok := f.cat.Table("u")
	if !ok {
		t.Fatal("u not in catalog")
	}
	if len(tab.UniqueKeys) != 1 || len(tab.UniqueKeys[0].Columns) != 1 || tab.UniqueKeys[0].Columns[0].Name != "slug" {
		t.Errorf("UniqueKeys = %+v; want one key on slug", tab.UniqueKeys)
	}
	if tab.UniqueKeys[0].Coordinated {
		t.Errorf("nullable UNIQUE marked coordinated; want eventual")
	}
}

// onlyUniqueKey returns the table's sole unique key, failing otherwise.
func onlyUniqueKey(t *testing.T, f *ddlFixture, table string) catalog.UniqueKey {
	t.Helper()
	tab, ok := f.cat.Table(table)
	if !ok {
		t.Fatalf("%s not in catalog", table)
	}
	if len(tab.UniqueKeys) != 1 {
		t.Fatalf("UniqueKeys = %+v; want exactly one", tab.UniqueKeys)
	}
	return tab.UniqueKeys[0]
}

func TestDDL_AcceptsCoordinatedNotNullUnique(t *testing.T) {
	// With a reservation backend available (UniqueRegistry set), a
	// NOT NULL UNIQUE key is admitted and recorded as coordinated.
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: unique.NewLocal()})
	if err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("NOT NULL UNIQUE rejected with backend configured: %v", err)
	}
	f.waitDrain(t)
	uk := onlyUniqueKey(t, f, "u")
	if !uk.Coordinated {
		t.Errorf("NOT NULL UNIQUE key not marked coordinated: %+v", uk)
	}
	if len(uk.Columns) != 1 || uk.Columns[0].Name != "email" {
		t.Errorf("key columns = %+v; want [email]", uk.Columns)
	}
}

func TestDDL_RejectsCoordinatedBlobUnique(t *testing.T) {
	// BLOB members are rejected in coordinated keys too: with no physical
	// index anywhere, nothing stops sqlite3_blob_open from incrementally
	// rewriting a key column whose blob-write fires carry no whole-value
	// image for the reservation scan.
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: unique.NewLocal()})
	if err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, fp BLOB NOT NULL UNIQUE)`); err == nil {
		t.Errorf("coordinated BLOB UNIQUE accepted; want rejection")
	}
}

func TestDDL_AcceptsCoordinatedUniqueIndex(t *testing.T) {
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: unique.NewLocal()})
	if err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, slug TEXT NOT NULL)`); err != nil {
		t.Fatalf("create base table: %v", err)
	}
	f.waitDrain(t)
	if err := f.app.Exec(`CREATE UNIQUE INDEX u_slug ON u(slug)`); err != nil {
		t.Fatalf("coordinated CREATE UNIQUE INDEX rejected: %v", err)
	}
	f.waitDrain(t)
	if uk := onlyUniqueKey(t, f, "u"); !uk.Coordinated {
		t.Errorf("CREATE UNIQUE INDEX on NOT NULL columns not coordinated: %+v", uk)
	}
}

func TestDDL_NullableUniqueStaysEventualWithBackend(t *testing.T) {
	// Even with a backend available, a nullable UNIQUE stays eventual —
	// coordination is selected by NOT NULL, not by backend availability.
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: unique.NewLocal()})
	if err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, slug TEXT, UNIQUE(slug))`); err != nil {
		t.Fatalf("nullable UNIQUE rejected: %v", err)
	}
	f.waitDrain(t)
	if uk := onlyUniqueKey(t, f, "u"); uk.Coordinated {
		t.Errorf("nullable UNIQUE marked coordinated with backend on; want eventual")
	}
}

// TestDDL_RejectionDoesNotPoisonNextCommit guards against a regression
// where the DDL-admission `rejected` flag leaks from a rolled-back
// statement into the next transaction. trace_v2's reject path sets the
// flag and Interrupt()s SQLite, which rolls back the implicit txn
// without ever calling commit_hook. If rollback_hook doesn't clear the
// flag, a subsequent unrelated commit (DML, or another DDL) inherits
// the stale rejection and fails with SQLITE_CONSTRAINT_COMMITHOOK.
func TestDDL_RejectionDoesNotPoisonNextCommit(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE users (id BLOB PRIMARY KEY NOT NULL, name TEXT)`); err != nil {
		t.Fatalf("seed CREATE TABLE: %v", err)
	}
	// Trigger an admission rejection: a gen_id literal naming the
	// wrong table is rejected by admission. This sets the rejected flag.
	if err := f.app.Exec(`CREATE TABLE doc (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('wrong')))`); err == nil {
		t.Fatalf("gen_id('wrong') unexpectedly accepted")
	}
	// The next, unrelated DML commit must succeed. Without
	// rollback_hook clearing the rejected flag, commit_hook returns 1
	// here and this Exec fails with "constraint failed (code 19)".
	if err := f.app.Exec(`INSERT INTO users (id, name) VALUES (x'00', 'alice')`); err != nil {
		t.Fatalf("INSERT after rejected DDL: %v", err)
	}
}

func TestDDL_HeadMovedRejectionDoesNotPoisonNextCommit(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE _local (id INT PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("seed local table: %v", err)
	}

	parsed, err := classifyDDL(`CREATE TABLE winner (id BLOB PRIMARY KEY NOT NULL)`)
	if err != nil {
		t.Fatalf("classify winner DDL: %v", err)
	}
	op, err := (&ddlAdmission{}).buildCreateTableOp(parsed)
	if err != nil {
		t.Fatalf("build winner op: %v", err)
	}
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("encode winner op: %v", err)
	}
	if _, err := f.log.Append(context.Background(), 0, encoded, parsed.RawSQL); err != nil {
		t.Fatalf("seed schema log: %v", err)
	}

	if err := f.app.Exec(`CREATE TABLE doc (id BLOB PRIMARY KEY NOT NULL)`); err == nil {
		t.Fatalf("CREATE TABLE unexpectedly accepted despite schema-log head move")
	}
	if err := f.app.Exec(`INSERT INTO _local (id, name) VALUES (1, 'alice')`); err != nil {
		t.Fatalf("INSERT after head-moved DDL rejection: %v", err)
	}
}

func TestDDL_CreateIndexIfNotExistsNoopDoesNotAppendOrPoison(t *testing.T) {
	f := newDDLFixture(t)
	ctx := context.Background()
	if err := f.app.Exec(`CREATE TABLE users (id BLOB PRIMARY KEY NOT NULL, name TEXT)`); err != nil {
		t.Fatalf("seed CREATE TABLE: %v", err)
	}
	if err := f.app.Exec(`CREATE INDEX IF NOT EXISTS idx_users_name ON users(name)`); err != nil {
		t.Fatalf("first CREATE INDEX: %v", err)
	}
	f.waitDrain(t)
	head, err := f.log.Head(ctx)
	if err != nil {
		t.Fatalf("schema log head: %v", err)
	}

	if err := f.app.Exec(`CREATE INDEX IF NOT EXISTS idx_users_name ON users(name)`); err != nil {
		t.Fatalf("second CREATE INDEX no-op: %v", err)
	}
	headAfter, err := f.log.Head(ctx)
	if err != nil {
		t.Fatalf("schema log head after no-op: %v", err)
	}
	if headAfter != head {
		t.Fatalf("schema log head advanced on no-op CREATE INDEX: got %d, want %d", headAfter, head)
	}
	if err := f.app.Exec(`INSERT INTO users (id, name) VALUES (x'01', 'alice')`); err != nil {
		t.Fatalf("INSERT after no-op CREATE INDEX: %v", err)
	}
}

func TestDDL_RejectsCTAS(t *testing.T) {
	f := newDDLFixture(t)
	err := f.app.Exec(`CREATE TABLE t AS SELECT 1`)
	if err == nil {
		t.Errorf("CTAS accepted; want rejection")
	}
	if head, _ := f.log.Head(context.Background()); head != 0 {
		t.Errorf("schema log advanced on rejected DDL: head=%d", head)
	}
}

// ----- explicit-transaction DDL (commit-time Append; see ddl_multi_test.go
// for multi-statement transactions) -----

func (f *ddlFixture) head(t *testing.T) uint64 {
	t.Helper()
	head, err := f.log.Head(context.Background())
	if err != nil {
		t.Fatalf("log.Head: %v", err)
	}
	return head
}

func (f *ddlFixture) intentPresent(t *testing.T) bool {
	t.Helper()
	all, err := f.sc.ListIntents()
	if err != nil {
		t.Fatalf("ListIntents: %v", err)
	}
	return len(all) > 0
}

func TestDDL_TxnSingleDDLCommit(t *testing.T) {
	f := newDDLFixture(t)
	for _, sql := range []string{`BEGIN`, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, v TEXT)`, `COMMIT`} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if head := f.head(t); head != 1 {
		t.Errorf("schema log head = %d, want 1", head)
	}
	if _, ok := f.cat.Table("t"); !ok {
		t.Errorf("t missing from catalog after commit")
	}
	if f.intentPresent(t) {
		t.Errorf("intent not resolved after commit")
	}
	if seq, _, _ := f.sc.GetSchemaSeq(); seq != 1 {
		t.Errorf("schema_seq = %d, want 1", seq)
	}
}

func TestDDL_TxnRollbackReplicatesNothing(t *testing.T) {
	f := newDDLFixture(t)
	for _, sql := range []string{`BEGIN`, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`, `ROLLBACK`} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if head := f.head(t); head != 0 {
		t.Errorf("schema log head = %d after rollback, want 0", head)
	}
	if f.intentPresent(t) {
		t.Errorf("intent left behind by rolled-back txn")
	}
	if _, ok := f.cat.Table("t"); ok {
		t.Errorf("rolled-back table leaked into catalog")
	}
	// State fully cleared: the same DDL must work in a fresh txn.
	for _, sql := range []string{`BEGIN`, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`, `COMMIT`} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("retry %s: %v", sql, err)
		}
	}
	if head := f.head(t); head != 1 {
		t.Errorf("schema log head = %d after retry, want 1", head)
	}
}

// TestDDL_TxnDDLThenDML is the standard framework-migration shape:
// BEGIN; CREATE TABLE; INSERT (e.g. schema_migrations bookkeeping);
// COMMIT. The DML drains under the post-DDL schema_seq because
// wal_hook resolves the intent before journaling the touch records.
func TestDDL_TxnDDLThenDML(t *testing.T) {
	f := newDDLFixture(t)
	for _, sql := range []string{
		`BEGIN`,
		`CREATE TABLE posts (id BLOB PRIMARY KEY NOT NULL, title TEXT)`,
		`INSERT INTO posts (id, title) VALUES (x'01', 'hello')`,
		`COMMIT`,
	} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	f.waitDrain(t)
	if _, ok := f.cat.Table("posts"); !ok {
		t.Fatalf("posts missing from catalog")
	}
	if head := f.head(t); head != 1 {
		t.Errorf("schema log head = %d, want 1", head)
	}
}

func TestDDL_TxnRejectsDMLBeforeDDL(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("setup CREATE: %v", err)
	}
	if err := f.app.Exec(`BEGIN`); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if err := f.app.Exec(`INSERT INTO t (id) VALUES (x'01')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := f.app.Exec(`CREATE TABLE t2 (id BLOB PRIMARY KEY NOT NULL)`); err == nil {
		t.Errorf("DDL after DML in txn accepted; want rejection")
	}
	_ = f.app.Exec(`ROLLBACK`)
	if head := f.head(t); head != 1 {
		t.Errorf("schema log head = %d, want 1 (setup only)", head)
	}
}

// A second DDL in the same transaction joins the first on one schema
// event rather than being rejected. Multi-DDL coverage lives in
// ddl_multi_test.go; this pins the single-event property.
func TestDDL_TxnSecondDDLJoinsOneEvent(t *testing.T) {
	f := newDDLFixture(t)
	for _, sql := range []string{
		`BEGIN`,
		`CREATE TABLE a (id BLOB PRIMARY KEY NOT NULL)`,
		`CREATE TABLE b (id BLOB PRIMARY KEY NOT NULL)`,
		`COMMIT`,
	} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if head := f.head(t); head != 1 {
		t.Errorf("schema log head = %d, want 1 (both DDL on one event)", head)
	}
	if f.intentPresent(t) {
		t.Errorf("intent left behind")
	}
}

func TestDDL_TxnRejectsSavepointScope(t *testing.T) {
	f := newDDLFixture(t)
	// SAVEPOINT-opened transaction.
	if err := f.app.Exec(`SAVEPOINT sp`); err != nil {
		t.Fatalf("SAVEPOINT: %v", err)
	}
	if err := f.app.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`); err == nil {
		t.Errorf("DDL inside SAVEPOINT txn accepted; want rejection")
	}
	_ = f.app.Exec(`ROLLBACK`)
	// SAVEPOINT inside a BEGIN transaction, before the DDL.
	for _, sql := range []string{`BEGIN`, `SAVEPOINT sp2`} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if err := f.app.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`); err == nil {
		t.Errorf("DDL after SAVEPOINT in txn accepted; want rejection")
	}
	_ = f.app.Exec(`ROLLBACK`)
	if head := f.head(t); head != 0 {
		t.Errorf("schema log head = %d, want 0", head)
	}
}

// TestDDL_TxnSavepointFlagDoesNotLeak: a committed savepoint scope
// (outermost RELEASE == COMMIT) must not poison DDL admission in later
// transactions on the same connection.
func TestDDL_TxnSavepointFlagDoesNotLeak(t *testing.T) {
	f := newDDLFixture(t)
	for _, sql := range []string{`SAVEPOINT sp`, `RELEASE sp`} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, sql := range []string{`BEGIN`, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`, `COMMIT`} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if head := f.head(t); head != 1 {
		t.Errorf("schema log head = %d, want 1", head)
	}
}

func TestDDL_TxnIfNotExistsNoop(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("setup CREATE: %v", err)
	}
	for _, sql := range []string{`BEGIN`, `CREATE TABLE IF NOT EXISTS t (id BLOB PRIMARY KEY NOT NULL)`, `COMMIT`} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if head := f.head(t); head != 1 {
		t.Errorf("schema log head = %d, want 1 (no-op must not append)", head)
	}
}

// TestDDL_TxnCommitAbortsOnHeadMoved simulates another writer's DDL
// landing while the transaction is open: the commit-time freshness
// check (or the Append CAS) must fail the COMMIT and roll the whole
// transaction back with no intent left behind.
func TestDDL_TxnCommitAbortsOnHeadMoved(t *testing.T) {
	f := newDDLFixture(t)
	for _, sql := range []string{`BEGIN`, `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	// Out-of-band append moves the log head past the pending parent.
	if _, err := f.log.Append(context.Background(), 0, []byte{0x01}, "out-of-band"); err != nil {
		t.Fatalf("out-of-band Append: %v", err)
	}
	if err := f.app.Exec(`COMMIT`); err == nil {
		t.Errorf("COMMIT succeeded despite head move; want failure")
	}
	if f.intentPresent(t) {
		t.Errorf("intent left behind by aborted commit")
	}
	if _, ok := f.cat.Table("t"); ok {
		t.Errorf("aborted txn's table leaked into catalog")
	}
	exists, err := sqlitebridge.ObjectExists(f.app, "table", "t")
	if err != nil {
		t.Fatalf("ObjectExists: %v", err)
	}
	if exists {
		t.Errorf("aborted txn's table exists in SQLite")
	}
}

func TestDDL_TxnLocalOnlyDDLAllowed(t *testing.T) {
	f := newDDLFixture(t)
	for _, sql := range []string{
		`BEGIN`,
		`CREATE TABLE _scratch (id INTEGER PRIMARY KEY)`,
		`INSERT INTO _scratch (id) VALUES (1)`,
		`COMMIT`,
	} {
		if err := f.app.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if head := f.head(t); head != 0 {
		t.Errorf("schema log head = %d, want 0 (local-only)", head)
	}
}

func TestDDL_TxnRejectsCascadeFKBundle(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE parent (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("setup parent: %v", err)
	}
	if err := f.app.Exec(`BEGIN`); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	err := f.app.Exec(`CREATE TABLE child (
		id BLOB PRIMARY KEY NOT NULL,
		pid BLOB,
		FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE
	)`)
	if err == nil {
		t.Errorf("cascade-FK CREATE inside txn accepted; want rejection")
	}
	_ = f.app.Exec(`ROLLBACK`)
}

func TestDDL_LocalOnlyTablesUnreplicated(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE _ephemeral (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE _ephemeral: %v", err)
	}
	f.waitDrain(t)
	if head, _ := f.log.Head(context.Background()); head != 0 {
		t.Errorf("schema log advanced for local-only table: head=%d", head)
	}
	if _, ok := f.cat.Table("_ephemeral"); ok {
		t.Errorf("_ephemeral leaked into replicated catalog")
	}
}

// TestDDL_ReplicateUnderscoreTables_PersistedWinsAfterFirstOpen verifies
// the flag is recorded on first producer.New and that subsequent New
// calls always honor the persisted value — Config is only consulted
// on the initial stamp. This lets warm/restore paths pass the zero
// Config without breaking once the slot has chosen a mode. Already-
// materialized tables can't be retroactively re-classified, so
// inheriting persisted is the only safe semantic anyway.
func TestDDL_ReplicateUnderscoreTables_PersistedWinsAfterFirstOpen(t *testing.T) {
	dir := t.TempDir()
	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	defer app.Close()
	if err := app.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		t.Fatalf("WAL: %v", err)
	}
	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	defer sc.Close()
	if err := sc.SetClusterID(crdt.ClusterID{0xCC}); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(crdt.Origin(1)); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}
	cat, err := catalog.LoadFromMeta(sc)
	if err != nil {
		t.Fatalf("LoadFromMeta: %v", err)
	}
	cache := nodestate.New(crdt.Origin(1))
	log := schemalog.NewLocal()

	prod, err := New(app, sc, cat, Config{
		JournalDir:                filepath.Join(dir, "jrn"),
		Cache:                     cache,
		SchemaLog:                 log,
		ReplicateUnderscoreTables: true,
	})
	if err != nil {
		t.Fatalf("first producer.New: %v", err)
	}
	if err := prod.Close(); err != nil {
		t.Fatalf("first prod.Close: %v", err)
	}

	got, ok, err := sc.GetReplicateUnderscoreTables()
	if err != nil || !ok || !got {
		t.Fatalf("replicate_underscore not persisted: got=%v ok=%v err=%v", got, ok, err)
	}

	// Reopening with the same value succeeds.
	prod2, err := New(app, sc, cat, Config{
		JournalDir:                filepath.Join(dir, "jrn"),
		Cache:                     cache,
		SchemaLog:                 log,
		ReplicateUnderscoreTables: true,
	})
	if err != nil {
		t.Fatalf("reopen matching: %v", err)
	}
	_ = prod2.Close()

	// Reopening with the zero Config inherits the persisted value —
	// the warm/restore path doesn't need to remember the original.
	// The underscore table from the first open admits cleanly.
	prod3, err := New(app, sc, cat, Config{
		JournalDir: filepath.Join(dir, "jrn"),
		Cache:      cache,
		SchemaLog:  log,
		// ReplicateUnderscoreTables unset — should inherit true.
	})
	if err != nil {
		t.Fatalf("reopen with zero Config: %v", err)
	}
	if err := app.Exec(`CREATE TABLE _another (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE _another on reopened slot: %v", err)
	}
	_ = prod3.Close()
}

// TestDDL_ReplicateUnderscoreTables_AdmitsUnderscoreName verifies the
// Config opt-in: when ReplicateUnderscoreTables is true, an underscore-
// prefixed CREATE TABLE goes through normal admission (schema log
// advances, catalog tracks the table). sqlite_* still falls back to
// local-only via the unconditional sqlite_ prefix carve-out.
func TestDDL_ReplicateUnderscoreTables_AdmitsUnderscoreName(t *testing.T) {
	f := newDDLFixtureCfg(t, Config{ReplicateUnderscoreTables: true})
	if err := f.app.Exec(`CREATE TABLE _collections (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE _collections: %v", err)
	}
	f.waitDrain(t)
	if head, _ := f.log.Head(context.Background()); head == 0 {
		t.Errorf("schema log did not advance for replicated underscore table")
	}
	if _, ok := f.cat.Table("_collections"); !ok {
		t.Errorf("_collections missing from replicated catalog")
	}
}

func TestDDL_RecoveryResolvesPendingIntent(t *testing.T) {
	dir := t.TempDir()
	// Phase 1: open a fixture, write an intent without resolving (we
	// fake this by writing the intent directly to a pristine metadata
	// before opening the producer).
	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	if err := app.Exec(`PRAGMA journal_mode = WAL; CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	if err := sc.SetClusterID(crdt.ClusterID{0xCC}); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(crdt.Origin(1)); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}

	// Build a CatalogOp that "creates" t (but the table is already
	// present from raw Exec) and write it as a LocalDDL intent.
	tabID := catalog.AllocTableID()
	colID := catalog.AllocColumnID()
	op := crdt.CatalogOp{
		Kind:      crdt.OpCreateTable,
		TableID:   tabID,
		TableName: "t",
		Columns: []crdt.CatalogColumn{
			{ID: colID, Name: "id", Ordinal: 0, Type: "BLOB",
				NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row"},
		},
		Keys: []crdt.CatalogKey{
			{KeyID: crdt.KeyID{}, Members: []crdt.CatalogKeyMember{{ColumnID: colID, Ordinal: 0}}},
		},
	}
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	if err := sc.SetOriginIntent(crdt.Origin(1), metadata.EncodeLocalDDL(metadata.LocalDDLIntent{
		StartedAtUs: 1, SchemaSeq: 1, ParentSeq: 0,
		CatalogOp: encoded, RawSQL: "CREATE TABLE t",
	})); err != nil {
		t.Fatalf("SetOriginIntent: %v", err)
	}
	// The crash being simulated happened AFTER the schemalog Append
	// committed; recovery verifies the intent against the log before
	// resolving, so the event must really be there.
	log := schemalog.NewLocal()
	if _, err := log.Append(context.Background(), 0, encoded, "CREATE TABLE t"); err != nil {
		t.Fatalf("seed log: %v", err)
	}

	// Phase 2: open producer — recovery should apply the intent.
	cat, err := catalog.LoadFromMeta(sc)
	if err != nil {
		t.Fatalf("LoadFromMeta: %v", err)
	}
	cache := nodestate.New(crdt.Origin(1))
	prod, err := New(app, sc, cat, Config{
		JournalDir: filepath.Join(dir, "jrn"),
		Cache:      cache,
		SchemaLog:  log,
	})
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	// LIFO order: app must outlive prod (Close uninstalls hooks via the
	// app conn). Register prod.Close() last so it runs first.
	defer app.Close()
	defer sc.Close()
	defer prod.Close()

	if all, _ := sc.ListIntents(); len(all) != 0 {
		t.Errorf("intent not cleared by recovery: %x", all[0].Buf)
	}
	seq, _, _ := sc.GetSchemaSeq()
	if seq != 1 {
		t.Errorf("schema_seq = %d; want 1", seq)
	}
	if _, ok := cat.TableByID(tabID); !ok {
		t.Errorf("recovery did not insert table into catalog")
	}
}

// TestDDL_RowidAliasRewrite_BareInteger covers the full admission +
// catalog-op round-trip for the canonical case: `INTEGER PRIMARY KEY`
// becomes the multi-writer-safe INT PK + gen_id DEFAULT shape, and
// SQLite ends up with the rewritten schema (not the user's literal).
func TestDDL_RowidAliasRewrite_BareInteger(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	f.waitDrain(t)

	// Local SQLite schema reflects the rewrite.
	assertColumnShape(t, f, "posts", "id",
		"INT", true /*notnull*/, "gen_id('posts')")

	// Catalog op shape carries the rewritten column.
	tab, ok := f.cat.Table("posts")
	if !ok {
		t.Fatalf("posts not in catalog")
	}
	idCol, ok := tab.Column("id")
	if !ok {
		t.Fatalf("posts.id not in catalog")
	}
	if idCol.PKPos != 1 {
		t.Errorf("posts.id PKPos = %d; want 1", idCol.PKPos)
	}
}

// TestDDL_RowidAliasRewrite_AutoIncrement drops AUTOINCREMENT and lands
// the same rewritten shape as the bare case.
func TestDDL_RowidAliasRewrite_AutoIncrement(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	f.waitDrain(t)
	assertColumnShape(t, f, "posts", "id",
		"INT", true, "gen_id('posts')")
	// sqlite_sequence is created lazily for AUTOINCREMENT tables; the
	// rewrite drops AUTOINCREMENT, so the table should not appear there.
	stmt, _, err := f.app.Prepare(`SELECT count(*) FROM sqlite_master WHERE name='sqlite_sequence'`)
	if err != nil {
		t.Fatalf("prep: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	if got := stmt.ColumnInt64(0); got != 0 {
		t.Errorf("sqlite_sequence present after AUTOINCREMENT-rewrite; got %d rows", got)
	}
}

// TestDDL_RowidAliasRewrite_GenIDAllocatesNonRowidValues confirms
// inserts on a rewritten table receive partitioned gen_id values (not
// the small rowid sequence) — the actual multi-writer protection.
func TestDDL_RowidAliasRewrite_GenIDAllocatesNonRowidValues(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := f.app.Exec(`INSERT INTO posts (title) VALUES ('hi')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	stmt, _, err := f.app.Prepare(`SELECT id FROM posts`)
	if err != nil {
		t.Fatalf("prep: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	// gen_id values are in [2^33, 2^63); rowid sequence is 1, 2, 3...
	// so anything > 2^33 confirms the rewrite is in play.
	if got := stmt.ColumnInt64(0); got < (1 << 33) {
		t.Errorf("inserted id = %d; expected gen_id-allocated value >= 2^33", got)
	}
}

// TestDDL_RowidAliasRewrite_PreservesSiblingTypeArgs documents that the
// targeted splice (rather than full re-render) preserves cosmetic
// fidelity on other columns — sibling VARCHAR(255) etc. survives.
func TestDDL_RowidAliasRewrite_PreservesSiblingTypeArgs(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(255))`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	stmt, _, err := f.app.Prepare(`SELECT type FROM pragma_table_info('users') WHERE name='email'`)
	if err != nil {
		t.Fatalf("prep: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	if got := stmt.ColumnText(0); got != "VARCHAR(255)" {
		t.Errorf("email type = %q; want VARCHAR(255) (splice should preserve type args)", got)
	}
}

// TestDDL_RowidAliasRewrite_WithoutRowidLeftAlone documents the
// carve-out for the already-WITHOUT-ROWID form, which is a documented
// recommended pattern and must not be touched.
func TestDDL_RowidAliasRewrite_WithoutRowidLeftAlone(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE inode (ino INTEGER PRIMARY KEY DEFAULT (gen_id('inode')), name TEXT) WITHOUT ROWID`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	// pragma_table_info should show INTEGER (user-supplied) preserved.
	stmt, _, err := f.app.Prepare(`SELECT type FROM pragma_table_info('inode') WHERE name='ino'`)
	if err != nil {
		t.Fatalf("prep: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	if got := stmt.ColumnText(0); got != "INTEGER" {
		t.Errorf("WITHOUT ROWID + INTEGER PK was rewritten (type=%q); should be left alone", got)
	}
}

// TestDDL_RowidAliasRewrite_LocalOnlyLeftAlone exercises the
// underscore-prefixed carve-out: local-only tables never replicate, so
// the rewrite is skipped and the user keeps stock rowid-alias behavior
// (including last_insert_rowid) for purely local schemas.
func TestDDL_RowidAliasRewrite_LocalOnlyLeftAlone(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE _scratch (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	stmt, _, err := f.app.Prepare(`SELECT type, "notnull", dflt_value FROM pragma_table_info('_scratch') WHERE name='id'`)
	if err != nil {
		t.Fatalf("prep: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	gotType := stmt.ColumnText(0)
	gotNotNull := stmt.ColumnInt64(1)
	gotDefault := stmt.ColumnText(2)
	if gotType != "INTEGER" || gotNotNull != 0 || gotDefault != "" {
		t.Errorf("local-only table was rewritten: type=%q notnull=%d default=%q",
			gotType, gotNotNull, gotDefault)
	}
}

// TestDDL_RowidAliasRewrite_ReplicatedUnderscoreTable verifies the
// rewrite path mirrors admission: when ReplicateUnderscoreTables is
// true, a rowid-alias CREATE on an underscore-prefixed name is
// rewritten to the gen_id form. Without this, the preprocessor would
// pass the table through verbatim and the resulting rowid-alias allocs
// would collide across writers.
func TestDDL_RowidAliasRewrite_ReplicatedUnderscoreTable(t *testing.T) {
	f := newDDLFixtureCfg(t, Config{ReplicateUnderscoreTables: true})
	if err := f.app.Exec(`CREATE TABLE _collections (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	stmt, _, err := f.app.Prepare(`SELECT type, "notnull", dflt_value FROM pragma_table_info('_collections') WHERE name='id'`)
	if err != nil {
		t.Fatalf("prep: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	gotType := stmt.ColumnText(0)
	gotNotNull := stmt.ColumnInt64(1)
	gotDefault := stmt.ColumnText(2)
	if gotType != "INT" || gotNotNull != 1 || gotDefault != "gen_id('_collections')" {
		t.Errorf("replicated underscore table missed rewrite: type=%q notnull=%d default=%q",
			gotType, gotNotNull, gotDefault)
	}
}

// assertColumnShape reads pragma_table_info for one column on the
// fixture's app conn and verifies it matches the rewritten shape: the
// declared type, the notnull flag, and the literal default expression
// stripped of its surrounding parens (which pragma_table_info elides).
func assertColumnShape(t *testing.T, f *ddlFixture, table, column, wantType string, wantNotNull bool, wantDefaultInner string) {
	t.Helper()
	stmt, _, err := f.app.Prepare(`SELECT type, "notnull", dflt_value FROM pragma_table_info(?) WHERE name=?`)
	if err != nil {
		t.Fatalf("prep table_info: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, table); err != nil {
		t.Fatalf("bind table: %v", err)
	}
	if err := stmt.BindText(2, column); err != nil {
		t.Fatalf("bind column: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if !hasRow {
		t.Fatalf("no row in pragma_table_info(%s) for %s", table, column)
	}
	if got := stmt.ColumnText(0); got != wantType {
		t.Errorf("type = %q; want %q", got, wantType)
	}
	if got := stmt.ColumnInt64(1) != 0; got != wantNotNull {
		t.Errorf("notnull = %v; want %v", got, wantNotNull)
	}
	if got := stmt.ColumnText(2); got != wantDefaultInner {
		t.Errorf("dflt_value = %q; want %q", got, wantDefaultInner)
	}
}

// TestDDL_RowidAliasRewrite_BackstopRejectsMultiStatementExec covers
// the codex finding that Conn.Exec with multiple semicolon-separated
// statements only preprocesses the first one; the parser stops at the
// embedded ';'. If a later statement is a rowid alias the preprocessor
// never sees it, so admission's validateNoRowidAlias backstop must
// reject the txn rather than silently admit a true rowid alias into
// the replicated catalog.
func TestDDL_RowidAliasRewrite_BackstopRejectsMultiStatementExec(t *testing.T) {
	f := newDDLFixture(t)
	err := f.app.Exec(`CREATE TABLE first (id BLOB PRIMARY KEY NOT NULL); CREATE TABLE second (id INTEGER PRIMARY KEY)`)
	if err == nil {
		t.Fatalf("expected admission to reject the second statement as rowid alias")
	}
	// First table may still have committed before the second was
	// rejected; the test asserts the rowid alias was caught, not that
	// every prior statement rolled back.
	if _, ok := f.cat.Table("second"); ok {
		t.Errorf("rowid-alias 'second' leaked into the replicated catalog")
	}
}

// TestDDL_RowidAliasRewrite_BackstopRejectsCheckOnPK exercises the
// preprocessor's bail-on-unsupported-constraint path: the splice can't
// preserve CHECK on the PK column, so it declines to rewrite, and the
// backstop rejects with an actionable error pointing users to the
// explicit `INT PRIMARY KEY ... gen_id(...)` form.
func TestDDL_RowidAliasRewrite_BackstopRejectsCheckOnPK(t *testing.T) {
	f := newDDLFixture(t)
	err := f.app.Exec(`CREATE TABLE posts (id INTEGER PRIMARY KEY CHECK (id > 0), title TEXT)`)
	if err == nil {
		t.Fatalf("expected admission to reject CHECK-on-PK rowid alias")
	}
	if _, ok := f.cat.Table("posts"); ok {
		t.Errorf("CHECK-on-PK rowid alias leaked into catalog")
	}
}

// TestDDL_RowidAliasRewrite_DescPKNotRewritten covers the DESC carve-
// out: SQLite doesn't alias the rowid for DESC PKs, so the rewrite
// would change semantics. shouldRewriteRowidAlias bails; the backstop
// then rejects because the column is still effectively INTEGER PK on a
// rowid table and we have no precedent for that form.
func TestDDL_RowidAliasRewrite_DescPKNotRewritten(t *testing.T) {
	f := newDDLFixture(t)
	err := f.app.Exec(`CREATE TABLE posts (id INTEGER PRIMARY KEY DESC, title TEXT)`)
	if err == nil {
		t.Fatalf("expected admission to reject INTEGER PRIMARY KEY DESC")
	}
}

// TestDDL_RowidAliasRewrite_QuotedIdentifierRoundTrip exercises the
// unquoteIdent fix: a column name with an embedded escaped double-
// quote round-trips through the splice without double-escaping. The
// rewritten DDL must reference the same identifier SQLite stores in
// sqlite_master.
func TestDDL_RowidAliasRewrite_QuotedIdentifierRoundTrip(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE quoted ("my""id" INTEGER PRIMARY KEY, title TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	stmt, _, err := f.app.Prepare(`SELECT name FROM pragma_table_info('quoted') WHERE pk=1`)
	if err != nil {
		t.Fatalf("prep: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	if got := stmt.ColumnText(0); got != `my"id` {
		t.Errorf(`PK column name = %q; want my"id`, got)
	}
}

// seedOutOfBandEvent appends a CREATE TABLE event for tableName to the
// schema log without going through this fixture's producer — the
// "another writer won the CAS race" precondition for catch-up tests.
// The fixture's metadata/catalog do NOT see the event (schema_seq
// stays behind), exactly like a CAS loser before catch-up.
func seedOutOfBandEvent(t *testing.T, f *ddlFixture, tableName string) {
	t.Helper()
	sql := `CREATE TABLE ` + tableName + ` (id BLOB PRIMARY KEY NOT NULL)`
	parsed, err := classifyDDL(sql)
	if err != nil {
		t.Fatalf("classify out-of-band DDL: %v", err)
	}
	op, err := (&ddlAdmission{}).buildCreateTableOp(parsed)
	if err != nil {
		t.Fatalf("build out-of-band op: %v", err)
	}
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("encode out-of-band op: %v", err)
	}
	if _, err := f.log.Append(context.Background(), 0, encoded, sql); err != nil {
		t.Fatalf("seed schema log: %v", err)
	}
}

// TestDDL_AutocommitHeadMovedRetriesViaCatchupHook: a CAS loss invokes
// the schemaCatchup hook (the broker's synchronous catch-up in a full
// node). Once the hook advances meta.schema_seq to the log head,
// admission rebuilds the op at the new parent and the retried Append
// succeeds — the app never sees the rejection.
func TestDDL_AutocommitHeadMovedRetriesViaCatchupHook(t *testing.T) {
	f := newDDLFixture(t)
	seedOutOfBandEvent(t, f, "winner")
	hookCalls := 0
	f.prod.SetSchemaCatchup(func(ctx context.Context) error {
		hookCalls++
		head, err := f.log.Head(ctx)
		if err != nil {
			return err
		}
		return f.sc.SetSchemaSeq(head)
	})
	if err := f.app.Exec(`CREATE TABLE doc (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE after catch-up: %v", err)
	}
	if hookCalls == 0 {
		t.Fatalf("schemaCatchup hook never invoked")
	}
	if got := f.head(t); got != 2 {
		t.Fatalf("schema log head = %d, want 2", got)
	}
	events, err := f.log.Read(context.Background(), 1, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("read retried event: %v (n=%d)", err, len(events))
	}
	if events[0].ParentSeq != 1 {
		t.Fatalf("retried event ParentSeq = %d, want 1", events[0].ParentSeq)
	}
	if f.intentPresent(t) {
		t.Fatalf("intent left dangling after successful retry")
	}
}

// TestDDL_AutocommitHeadMovedWaitsForExternalAuthority: producer-only
// deployments have no in-process broker; admission polls for an
// external authority (daemon / host node) to advance meta.schema_seq
// before retrying.
func TestDDL_AutocommitHeadMovedWaitsForExternalAuthority(t *testing.T) {
	f := newDDLFixture(t)
	seedOutOfBandEvent(t, f, "winner")
	f.prod.ddl.catchupWait = 2 * time.Second // authority lands in ~15ms
	go func() {
		time.Sleep(15 * time.Millisecond)
		_ = f.sc.SetSchemaSeq(1)
	}()
	if err := f.app.Exec(`CREATE TABLE doc (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE after external catch-up: %v", err)
	}
	if got := f.head(t); got != 2 {
		t.Fatalf("schema log head = %d, want 2", got)
	}
}

// TestDDL_AutocommitHeadMovedSecondLossRejects: only one catch-up +
// retry; if the head moves again between catch-up and the retried
// Append, admission rejects (the app retries the whole statement).
func TestDDL_AutocommitHeadMovedSecondLossRejects(t *testing.T) {
	f := newDDLFixture(t)
	seedOutOfBandEvent(t, f, "winner")
	f.prod.SetSchemaCatchup(func(ctx context.Context) error {
		// Catch up to the head admission saw, then immediately lose
		// the race again: a second out-of-band event lands.
		seq, err := f.log.Head(ctx)
		if err != nil {
			return err
		}
		if err := f.sc.SetSchemaSeq(seq); err != nil {
			return err
		}
		sql := `CREATE TABLE winner2 (id BLOB PRIMARY KEY NOT NULL)`
		parsed, err := classifyDDL(sql)
		if err != nil {
			return err
		}
		op, err := (&ddlAdmission{}).buildCreateTableOp(parsed)
		if err != nil {
			return err
		}
		encoded, err := crdt.EncodeCatalogOp(op)
		if err != nil {
			return err
		}
		_, err = f.log.Append(ctx, seq, encoded, sql)
		return err
	})
	if err := f.app.Exec(`CREATE TABLE doc (id BLOB PRIMARY KEY NOT NULL)`); err == nil {
		t.Fatalf("CREATE TABLE unexpectedly accepted after double CAS loss")
	}
	if f.intentPresent(t) {
		t.Fatalf("intent left dangling after double CAS loss")
	}
	// The conn is not poisoned: unrelated work proceeds.
	if err := f.app.Exec(`CREATE TABLE _local (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("local DDL after rejection: %v", err)
	}
}

// TestDDL_CreateIndexOnUnknownTableRejected: a replicated CREATE INDEX
// must target a catalog table. Critically reachable via the CAS-loss
// retry: catch-up may apply a concurrent DROP TABLE, and re-admission
// must then reject rather than publish an index event no receiver can
// apply.
func TestDDL_CreateIndexOnUnknownTableRejected(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE INDEX idx_ghost ON ghost(name)`); err == nil {
		t.Fatalf("CREATE INDEX on unknown table unexpectedly accepted")
	}
	if got := f.head(t); got != 0 {
		t.Fatalf("schema log head = %d, want 0 (nothing published)", got)
	}
}

// encodeCreateTableOpFor builds the encoded CatalogOp admission would
// produce for a simple CREATE TABLE, for planting intents directly.
func encodeCreateTableOpFor(t *testing.T, tableName string) []byte {
	t.Helper()
	parsed, err := classifyDDL(`CREATE TABLE ` + tableName + ` (id BLOB PRIMARY KEY NOT NULL)`)
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	op, err := (&ddlAdmission{}).buildCreateTableOp(parsed)
	if err != nil {
		t.Fatalf("build op: %v", err)
	}
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("encode op: %v", err)
	}
	return encoded
}

// TestDDL_ForeignIntentIsolation: another producer's intent slot must
// survive this producer's failed admissions (which clear only their
// own slot) and its successful DDL resolutions (wal_hook reads only
// its own slot).
func TestDDL_ForeignIntentIsolation(t *testing.T) {
	f := newDDLFixture(t)
	foreign := crdt.Origin(99)
	foreignBuf := metadata.EncodeLocalDDL(metadata.LocalDDLIntent{
		StartedAtUs: 1, SchemaSeq: 42, ParentSeq: 41,
		CatalogOp: []byte("op"), RawSQL: "CREATE TABLE theirs",
	})
	if err := f.sc.SetOriginIntent(foreign, foreignBuf); err != nil {
		t.Fatalf("SetOriginIntent: %v", err)
	}

	// A rejected admission clears only this producer's slot.
	if err := f.app.Exec(`CREATE TABLE bad (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('wrong')))`); err == nil {
		t.Fatalf("gen_id('wrong') unexpectedly accepted")
	}
	// A successful DDL resolves only this producer's slot.
	if err := f.app.Exec(`CREATE TABLE doc (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	buf, ok, err := f.sc.GetOriginIntent(foreign)
	if err != nil || !ok {
		t.Fatalf("foreign intent destroyed: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(buf, foreignBuf) {
		t.Fatalf("foreign intent mutated")
	}
	// And the foreign intent's seq was NOT resolved into our metadata.
	if seq, _, _ := f.sc.GetSchemaSeq(); seq != 1 {
		t.Fatalf("schema_seq = %d; want 1 (only our own DDL)", seq)
	}
}

// TestRecovery_UnappendedIntentClearedWithoutAdvance is the fork
// guard: a crash between SetIntent and Append leaves an intent whose
// event never reached the log. Recovery must clear it WITHOUT
// advancing schema_seq — resolving it would move schema_seq past the
// log head, an unrepairable fork.
func TestRecovery_UnappendedIntentClearedWithoutAdvance(t *testing.T) {
	f := newDDLFixture(t)
	err := f.sc.SetOriginIntent(crdt.Origin(1), metadata.EncodeLocalDDL(metadata.LocalDDLIntent{
		StartedAtUs: 1, SchemaSeq: 1, ParentSeq: 0,
		CatalogOp: encodeCreateTableOpFor(t, "ghost"), RawSQL: "CREATE TABLE ghost",
	}))
	if err != nil {
		t.Fatalf("SetOriginIntent: %v", err)
	}
	// The log is empty: the Append never happened.
	if err := f.prod.recoverDDLIntent(); err != nil {
		t.Fatalf("recoverDDLIntent: %v", err)
	}
	if f.intentPresent(t) {
		t.Fatalf("unappended intent not cleared")
	}
	if seq, _, _ := f.sc.GetSchemaSeq(); seq != 0 {
		t.Fatalf("schema_seq = %d; want 0 (no fork past log head)", seq)
	}
}

// TestRecovery_MismatchedIntentClearedWithoutResolve: the seq this
// intent reserved was won by a different producer's event. Our DDL
// never executed; resolving our op at that seq would corrupt the
// catalog. Recovery clears the slot and leaves the real event to the
// normal catch-up path.
func TestRecovery_MismatchedIntentClearedWithoutResolve(t *testing.T) {
	f := newDDLFixture(t)
	seedOutOfBandEvent(t, f, "winner") // seq 1 belongs to someone else
	err := f.sc.SetOriginIntent(crdt.Origin(1), metadata.EncodeLocalDDL(metadata.LocalDDLIntent{
		StartedAtUs: 1, SchemaSeq: 1, ParentSeq: 0,
		CatalogOp: encodeCreateTableOpFor(t, "ghost"), RawSQL: "CREATE TABLE ghost",
	}))
	if err != nil {
		t.Fatalf("SetOriginIntent: %v", err)
	}
	if err := f.prod.recoverDDLIntent(); err != nil {
		t.Fatalf("recoverDDLIntent: %v", err)
	}
	if f.intentPresent(t) {
		t.Fatalf("mismatched intent not cleared")
	}
	if seq, _, _ := f.sc.GetSchemaSeq(); seq != 0 {
		t.Fatalf("schema_seq = %d; want 0 (winner's event applies via catch-up)", seq)
	}
}

// TestDDL_BareGenIDQualifiedInCreateTable: `DEFAULT (gen_id())` is
// table-qualified by the preprocessor before SQLite stores the
// DEFAULT. The PK and a sibling column both qualify independently.
func TestDDL_BareGenIDQualifiedInCreateTable(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE g (
		id INT PRIMARY KEY NOT NULL DEFAULT (gen_id()),
		alt INT DEFAULT (gen_id())
	)`); err != nil {
		t.Fatalf("CREATE TABLE with bare gen_id(): %v", err)
	}
	stmt, _, err := f.app.Prepare(`SELECT sql FROM sqlite_master WHERE name = 'g'`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	if ok, err := stmt.Step(); err != nil || !ok {
		t.Fatalf("step: ok=%v err=%v", ok, err)
	}
	sql := stmt.ColumnText(0)
	if got := strings.Count(sql, "gen_id('g')"); got != 2 {
		t.Fatalf("stored DDL has %d qualified gen_id calls, want 2: %s", got, sql)
	}
	if strings.Contains(sql, "gen_id()") {
		t.Fatalf("bare gen_id() survived: %s", sql)
	}
}

// TestDDL_BareGenIDQualifiedInAddColumn covers the ALTER TABLE form
// (no column source spans; whole-statement replace).
func TestDDL_BareGenIDQualifiedInAddColumn(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE h (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("seed table: %v", err)
	}
	if err := f.app.Exec(`ALTER TABLE h ADD COLUMN tag INT DEFAULT (gen_id())`); err != nil {
		t.Fatalf("ADD COLUMN with bare gen_id(): %v", err)
	}
	stmt, _, err := f.app.Prepare(`SELECT sql FROM sqlite_master WHERE name = 'h'`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	if ok, err := stmt.Step(); err != nil || !ok {
		t.Fatalf("step: ok=%v err=%v", ok, err)
	}
	sql := stmt.ColumnText(0)
	if !strings.Contains(sql, "gen_id('h')") || strings.Contains(sql, "gen_id()") {
		t.Fatalf("ADD COLUMN default not qualified: %s", sql)
	}
}
