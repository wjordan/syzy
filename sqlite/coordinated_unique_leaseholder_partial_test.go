package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

// TestCoordinatedUnique_LeaseholderPartialSoftDeleteReinsert reproduces the
// soft-delete-then-reinsert idiom against the REAL leaseholder-backed
// reservation path (the partial-index soft-delete test elsewhere runs on the
// quarantine-free Local registry). A row leaves the partial unique index via
// UPDATE (deleted_at set), which must release its reservation; a later
// transaction inserting the same value under a new PK must be granted once the
// quarantine window has passed.
func TestCoordinatedUnique_LeaseholderPartialSoftDeleteReinsert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	node, err := syzy.Open(ctx, syzy.Config{
		Path:             t.TempDir() + "/app.db",
		SchemaLog:        schemalog.NewLocal(),
		ObjectBackend:    testBackend(t),
		UniqueQuarantine: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if err := node.Exec(`CREATE TABLE apps (id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()), name TEXT NOT NULL, deleted_at TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := node.Exec(`CREATE UNIQUE INDEX idx_apps_name_live ON apps(name) WHERE deleted_at IS NULL`); err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	retryInsert(t, node, `INSERT INTO apps (name) VALUES ('foo')`)
	// Soft-delete: the row leaves the partial index, releasing the value.
	if err := node.Exec(`UPDATE apps SET deleted_at='2026-07-04' WHERE name='foo'`); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	// Reinsert under a NEW PK, cross-transaction. Poll well past the 100ms
	// quarantine: the value must become grantable.
	deadline := time.Now().Add(5 * time.Second)
	var last error
	for {
		last = node.Exec(`INSERT INTO apps (name) VALUES ('foo')`)
		if last == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reinsert after soft delete never granted (release lost?): %v", last)
		}
		time.Sleep(50 * time.Millisecond)
	}

	assertCount(t, node, `SELECT count(*) FROM apps WHERE name='foo'`, 2)
}
