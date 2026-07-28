package sqlite_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

// TestCatalog_UniqueIndexReplayNoDuplicates pins catalog admission
// idempotency: re-running `CREATE UNIQUE INDEX IF NOT EXISTS` (the standard
// migration idiom, replayed on every node boot) must not accumulate duplicate
// coordinated key entries, and the partial predicate must be stored. A
// production catalog was found with ~95 duplicate predicate-less keys on one
// column, each admitted by a replay; total (predicate-less) duplicates never
// release on soft-delete, permanently wedging the value.
func TestCatalog_UniqueIndexReplayNoDuplicates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS apps (id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()), name TEXT NOT NULL, deleted_at TEXT)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_name_live ON apps(name) WHERE deleted_at IS NULL`,
	}
	open := func() *syzy.Node {
		node, err := syzy.Open(ctx, syzy.Config{
			Path:             dir + "/app.db",
			SchemaLog:        schemalog.NewLocal(),
			ObjectBackend:    testBackend(t),
			IdempotentDDL:    true,
			UniqueQuarantine: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for _, q := range ddl {
			if err := node.Exec(q); err != nil {
				t.Fatalf("ddl %q: %v", q, err)
			}
		}
		return node
	}

	node := open()
	// Same-session replay.
	for _, q := range ddl {
		if err := node.Exec(q); err != nil {
			t.Fatalf("same-session replay %q: %v", q, err)
		}
	}
	node.Close()
	// Restart replay (the per-boot migration pass).
	node = open()
	node.Close()

	out, err := exec.Command("sqlite3", dir+"/app.db-syzy/metadata.db",
		`SELECT count(*), sum(predicate IS NULL) FROM syzy_key k JOIN syzy_table t ON t.table_id=k.table_id WHERE t.name='apps' AND k.state='active' AND hex(k.key_id) != '00000000000000000000000000000000'`).CombinedOutput()
	if err != nil {
		t.Fatalf("inspect catalog: %v: %s", err, out)
	}
	got := strings.TrimSpace(string(out))
	if got != "1|0" {
		t.Fatalf("apps coordinated keys (count|predicate-less) = %q, want \"1|0\" (one key, predicate stored)", got)
	}
}
