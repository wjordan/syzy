package sqlite_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

// The catalog repair pass (producer.RepairUniqueKeys, run on every Open)
// drops unique keys the SQLite schema does not back: duplicates (the legacy
// poison class — a duplicate total key permanently reserves soft-deleted
// values) and orphans (DROP INDEX has no catalog effect on the SQLite
// engine). Poison is injected directly into metadata.db between opens.

func openRepairNode(t *testing.T, dir string, be objectstore.Bucket) *syzy.Node {
	t.Helper()
	node, err := syzy.Open(context.Background(), syzy.Config{
		Path:             dir + "/app.db",
		SchemaLog:        schemalog.NewLocal(),
		ObjectBackend:    be,
		UniqueQuarantine: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return node
}

// appsCatalogIDs returns the metadata TableID of 'apps', the ColumnID of
// its 'name' column, and the KeyIDs of the active non-PK keys on it.
func appsCatalogIDs(t *testing.T, metaPath string) (crdt.TableID, crdt.ColumnID, []crdt.KeyID) {
	t.Helper()
	store, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	defer store.Close()
	snap, err := store.LoadCatalogSnapshot()
	if err != nil {
		t.Fatalf("LoadCatalogSnapshot: %v", err)
	}
	var tid crdt.TableID
	found := false
	for _, te := range snap.Tables {
		if te.Name == "apps" && te.State == metadata.StateActive {
			tid, found = te.ID, true
		}
	}
	if !found {
		t.Fatalf("table 'apps' not in catalog")
	}
	var cid crdt.ColumnID
	found = false
	for _, ce := range snap.Columns {
		if ce.TableID == tid && ce.Name == "name" && ce.State == metadata.StateActive {
			cid, found = ce.ColumnID, true
		}
	}
	if !found {
		t.Fatalf("column 'apps.name' not in catalog")
	}
	dropped := map[crdt.KeyID]bool{}
	seen := map[crdt.KeyID]bool{}
	for _, ke := range snap.Keys {
		if ke.TableID != tid || ke.KeyID == metadata.PKKeyID {
			continue
		}
		if ke.State == metadata.StateDropped {
			dropped[ke.KeyID] = true
			continue
		}
		seen[ke.KeyID] = true
	}
	var keys []crdt.KeyID
	for id := range seen {
		if !dropped[id] {
			keys = append(keys, id)
		}
	}
	return tid, cid, keys
}

// activeAppsKeyStats returns "count|predicate-less-count" for active non-PK
// keys on apps, straight from metadata.db.
func activeAppsKeyStats(t *testing.T, metaPath string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", metaPath,
		`SELECT count(DISTINCT hex(k.key_id)), count(DISTINCT CASE WHEN k.predicate IS NULL THEN hex(k.key_id) END)
		 FROM syzy_key k JOIN syzy_table t ON t.table_id=k.table_id
		 WHERE t.name='apps' AND k.state='active'
		   AND hex(k.key_id) != '00000000000000000000000000000000'
		   AND NOT EXISTS (SELECT 1 FROM syzy_key d WHERE d.table_id=k.table_id
		                   AND d.key_id=k.key_id AND d.state='dropped')`).CombinedOutput()
	if err != nil {
		t.Fatalf("inspect catalog: %v: %s", err, out)
	}
	return strings.TrimSpace(string(out))
}

const appsDDL = `CREATE TABLE apps (id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()), name TEXT NOT NULL, deleted_at TEXT)`
const appsIdxDDL = `CREATE UNIQUE INDEX idx_apps_name_live ON apps(name) WHERE deleted_at IS NULL`

// TestCatalogRepair_CollapsesDuplicateTotalKeys reproduces the production
// poison: predicate-less coordinated duplicates alongside the correct
// partial key. Repair on reopen must collapse to exactly the one correct
// key, stably across reopens, and the soft-delete-then-reinsert idiom must
// work.
func TestCatalogRepair_CollapsesDuplicateTotalKeys(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	metaPath := dir + "/app.db-syzy/metadata.db"
	be := testBackend(t)

	node := openRepairNode(t, dir, be)
	if err := node.Exec(appsDDL); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := node.Exec(appsIdxDDL); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	retryInsert(t, node, `INSERT INTO apps (name) VALUES ('foo')`)
	node.Close()

	// Inject 3 predicate-less coordinated keys on apps(name) — the exact
	// row shape legacy CREATE UNIQUE INDEX IF NOT EXISTS replays left.
	tid, cid, good := appsCatalogIDs(t, metaPath)
	if len(good) != 1 {
		t.Fatalf("active keys = %d, want 1", len(good))
	}
	store, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	if err := store.WithTx(func(tx *metadata.Tx) error {
		for i := 0; i < 3; i++ {
			var id crdt.KeyID
			id[0], id[15] = 0xA0, byte(i+1)
			if err := tx.UpsertKey(metadata.KeyEntry{
				TableID: tid, KeyID: id, ColumnID: cid, Ordinal: 0,
				State: metadata.StateActive, Coordinated: true,
				CreateSeq: 500 + uint64(i),
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("poison: %v", err)
	}
	store.Close()
	if got := activeAppsKeyStats(t, metaPath); got != "4|3" {
		t.Fatalf("poisoned catalog = %q, want \"4|3\"", got)
	}

	node = openRepairNode(t, dir, be)
	if got := activeAppsKeyStats(t, metaPath); got != "1|0" {
		t.Fatalf("repaired catalog (count|predicate-less) = %q, want \"1|0\"", got)
	}
	if err := node.Exec(`UPDATE apps SET deleted_at='2026-07-03' WHERE name='foo'`); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	retryInsertUntil(t, node, `INSERT INTO apps (name) VALUES ('foo')`, 5*time.Second)
	assertCount(t, node, `SELECT count(*) FROM apps WHERE name='foo'`, 2)
	node.Close()

	// Healthy reopen is a no-op: same single key survives.
	node = openRepairNode(t, dir, be)
	node.Close()
	_, _, after := appsCatalogIDs(t, metaPath)
	if len(after) != 1 || after[0] != good[0] {
		t.Fatalf("healthy reopen churned keys: got %v, want [%x]", after, good[0][:])
	}
}

// TestCatalogRepair_DropsOrphanedEventualKeyAfterDropIndex: for an
// eventual (nullable-member) key, DROP INDEX replicates as opaque SQL
// with no catalog effect, so the key outlives its index on the
// originator. Repair must drop the orphan on the next open.
func TestCatalogRepair_DropsOrphanedEventualKeyAfterDropIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	metaPath := dir + "/app.db-syzy/metadata.db"
	be := testBackend(t)

	node := openRepairNode(t, dir, be)
	// name is nullable → the key is eventual (loser-null), natively
	// indexed on the originator.
	if err := node.Exec(`CREATE TABLE apps (id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()), name TEXT, deleted_at TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := node.Exec(`CREATE UNIQUE INDEX idx_apps_name ON apps(name)`); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	if err := node.Exec(`INSERT INTO apps (name) VALUES ('foo')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := node.Exec(`DROP INDEX idx_apps_name`); err != nil {
		t.Fatalf("DROP INDEX: %v", err)
	}
	if got := activeAppsKeyStats(t, metaPath); got != "1|1" {
		t.Fatalf("catalog after DROP INDEX = %q, want \"1|1\" (the known orphan)", got)
	}
	node.Close()

	node = openRepairNode(t, dir, be)
	defer node.Close()
	if got := activeAppsKeyStats(t, metaPath); got != "0|0" {
		t.Fatalf("repaired catalog = %q, want \"0|0\"", got)
	}
	// No unique index, no key: duplicate names are legal again.
	if err := node.Exec(`INSERT INTO apps (name) VALUES ('foo')`); err != nil {
		t.Fatalf("duplicate insert after orphan heal: %v", err)
	}
	assertCount(t, node, `SELECT count(*) FROM apps WHERE name='foo'`, 2)
}

// TestCoordinatedDropIndex_RemovesKeyAtDDLTime: a coordinated key's
// DROP INDEX (column-match on its normalized plain index) replicates as
// the typed key-removal op — the catalog key is gone at statement time,
// no reopen or repair involved.
func TestCoordinatedDropIndex_RemovesKeyAtDDLTime(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	metaPath := dir + "/app.db-syzy/metadata.db"
	be := testBackend(t)

	node := openRepairNode(t, dir, be)
	defer node.Close()
	if err := node.Exec(appsDDL); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := node.Exec(`CREATE UNIQUE INDEX idx_apps_name ON apps(name)`); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}
	// Normalization downgraded the index in place: same name, no longer
	// unique, and the coordinated key is catalog metadata.
	assertCount(t, node, `SELECT count(*) FROM pragma_index_list('apps') WHERE "unique" = 1 AND origin != 'pk'`, 0)
	assertCount(t, node, `SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_apps_name'`, 1)
	retryInsert(t, node, `INSERT INTO apps (name) VALUES ('foo')`)

	if err := node.Exec(`DROP INDEX idx_apps_name`); err != nil {
		t.Fatalf("DROP INDEX: %v", err)
	}
	if got := activeAppsKeyStats(t, metaPath); got != "0|0" {
		t.Fatalf("catalog after coordinated DROP INDEX = %q, want \"0|0\" (typed key removal, no reopen)", got)
	}
	assertCount(t, node, `SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_apps_name'`, 0)
	// Key gone: duplicates are legal immediately.
	retryInsertUntil(t, node, `INSERT INTO apps (name) VALUES ('foo')`, 5*time.Second)
	assertCount(t, node, `SELECT count(*) FROM apps WHERE name='foo'`, 2)
}

// TestCoordinatedNormalization_BirthAndLegacyMigration: an inline
// NOT NULL UNIQUE is stripped from the physical schema at birth (the
// gate is the only enforcement), a legacy native UNIQUE index appearing
// on a coordinated key's columns is downgraded at the next open, and the
// key itself survives reopens without any physical backing.
func TestCoordinatedNormalization_BirthAndLegacyMigration(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	be := testBackend(t)

	node := openRepairNode(t, dir, be)
	if err := node.Exec(`CREATE TABLE users (id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()), email TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	// Birth normalization: no UNIQUE in the stored definition, no unique
	// index — yet the gate enforces.
	assertCount(t, node, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='users' AND sql LIKE '%UNIQUE%'`, 0)
	assertCount(t, node, `SELECT count(*) FROM pragma_index_list('users') WHERE "unique" = 1 AND origin != 'pk'`, 0)
	retryInsert(t, node, `INSERT INTO users (email) VALUES ('a@x.com')`)
	if err := node.Exec(`INSERT INTO users (email) VALUES ('a@x.com')`); err == nil {
		t.Fatal("duplicate accepted with no physical index; the gate must reject it")
	}
	node.Close()

	// Legacy state: a native UNIQUE index on the key's columns (what a
	// pre-normalization build left behind). The next open migrates it.
	out, err := exec.Command("sqlite3", dir+"/app.db",
		`CREATE UNIQUE INDEX legacy_idx ON users(email)`).CombinedOutput()
	if err != nil {
		t.Fatalf("inject legacy index: %v: %s", err, out)
	}

	node = openRepairNode(t, dir, be)
	defer node.Close()
	assertCount(t, node, `SELECT count(*) FROM pragma_index_list('users') WHERE "unique" = 1 AND origin != 'pk'`, 0)
	assertCount(t, node, `SELECT count(*) FROM sqlite_master WHERE type='index' AND name='legacy_idx'`, 1)
	// The key survived the reopen (repair keeps coordinated keys) and
	// still enforces.
	if err := node.Exec(`INSERT INTO users (email) VALUES ('a@x.com')`); err == nil {
		t.Fatal("duplicate accepted after reopen")
	}
	retryInsertUntil(t, node, `INSERT INTO users (email) VALUES ('b@x.com')`, 5*time.Second)
}

// retryInsertUntil polls an INSERT past the reservation quarantine window.
func retryInsertUntil(t *testing.T, node *syzy.Node, sql string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := node.Exec(sql)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("INSERT never succeeded: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
