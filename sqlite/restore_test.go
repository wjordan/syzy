package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
)

func TestRestore_NoSources(t *testing.T) {
	t.Parallel()
	dst := filepath.Join(t.TempDir(), "dst.db")
	err := syzy.Restore(context.Background(), dst)
	if !errors.Is(err, syzy.ErrNoSources) {
		t.Fatalf("got %v, want ErrNoSources", err)
	}
}

func TestRestore_UnsupportedScheme(t *testing.T) {
	t.Parallel()
	dst := filepath.Join(t.TempDir(), "dst.db")
	err := syzy.Restore(context.Background(), dst, "http://example.com/foo")
	if err == nil {
		t.Fatalf("expected error for unsupported scheme")
	}
	if !strings.Contains(err.Error(), "unsupported scheme") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestore_FileBackendNoHEAD_FallsThrough(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	dst := filepath.Join(t.TempDir(), "dst.db")

	// FileBackend exists but has no HEAD; tcp peer also unreachable.
	// Both fail; we expect errors.Join with both errors.
	err := syzy.Restore(context.Background(), dst,
		"file://"+bucket,
		"tcp://127.0.0.1:1", // refused; should fail and append to errs
	)
	if err == nil {
		t.Fatalf("expected error when no source had data")
	}
	// Both source URLs should appear in the joined error.
	msg := err.Error()
	if !strings.Contains(msg, "file://") {
		t.Errorf("error missing file:// source: %v", err)
	}
	if !strings.Contains(msg, "tcp://") {
		t.Errorf("error missing tcp:// source: %v", err)
	}
}

func TestRestore_FileBackend_RoundTrip(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	objURL := "file://" + bucket
	ctx := context.Background()

	// Open node A with ObjectBackend pointing at the bucket. Write a
	// small payload, publish a snapshot.
	srcDB := filepath.Join(t.TempDir(), "src.db")
	{
		be, err := objectstore.Open(ctx, objURL)
		if err != nil {
			t.Fatalf("objectstore.Open: %v", err)
		}
		nodeA, err := syzy.Open(ctx, syzy.Config{
			Path:          srcDB,
			ObjectBackend: be,
			SchemaLog:     schemalog.NewLocal(),
		})
		if err != nil {
			t.Fatalf("Open A: %v", err)
		}
		if err := nodeA.Exec(`CREATE TABLE notes (id INT PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
			t.Fatalf("CREATE: %v", err)
		}
		if err := nodeA.Exec(`INSERT INTO notes VALUES (1, 'hello')`); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		if err := nodeA.PublishSnapshot(ctx); err != nil {
			t.Fatalf("PublishSnapshot: %v", err)
		}
		if err := nodeA.Close(); err != nil {
			t.Fatalf("Close A: %v", err)
		}
	}

	// Restore into a fresh dst path; expect file:// source to win.
	dstDB := filepath.Join(t.TempDir(), "dst.db")
	if err := syzy.Restore(ctx, dstDB, objURL); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Open node B at dstDB; verify the restored row is queryable.
	nodeB, err := syzy.Open(ctx, syzy.Config{Path: dstDB, SchemaLog: schemalog.NewLocal()})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer nodeB.Close()

	// Quick read via raw sqlite to keep this test self-contained.
	count := readNotesCount(t, dstDB)
	if count != 1 {
		t.Errorf("notes row count after restore = %d, want 1", count)
	}
}

func TestRestore_TCPSource_FallThroughOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	srcDB := filepath.Join(t.TempDir(), "src.db")
	tx, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewTestTx: %v", err)
	}

	srcNode, err := syzy.Open(ctx, syzy.Config{Path: srcDB, Transport: tx, ObjectBackend: testBackend(t)})
	if err != nil {
		tx.Close()
		t.Fatalf("Open src: %v", err)
	}
	tx.SetBundleHandler(srcNode.ServeBundle)
	// Close transport first to stop accepting bundle requests, then
	// close the node. Reverse-deferring Close on the node would race
	// in-flight bundle handlers against metadata teardown.
	t.Cleanup(func() {
		tx.Close()
		srcNode.Close()
	})

	if err := srcNode.Exec(`CREATE TABLE notes (id INT PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := srcNode.Exec(`INSERT INTO notes (id, body) VALUES (1, 'tcp'), (2, 'tcp'), (3, 'tcp')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Restore via tcp:// with a junk file:// source first to exercise
	// the fall-through path (file:// has no HEAD → fall through to
	// tcp://, which succeeds).
	dstDB := filepath.Join(t.TempDir(), "dst.db")
	junkBucket := t.TempDir()
	if err := syzy.Restore(ctx, dstDB,
		"file://"+junkBucket,
		tx.Endpoint(),
	); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	count := readNotesCount(t, dstDB)
	if count != 3 {
		t.Errorf("notes row count after tcp restore = %d, want 3", count)
	}
}

// readNotesCount opens dbPath read-only and returns the row count of
// the `notes` table (created in the test's source node).
func readNotesCount(t *testing.T, dbPath string) int {
	t.Helper()
	c, err := sqlitebridge.Open(dbPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		t.Fatalf("re-open %s: %v", dbPath, err)
	}
	defer c.Close()
	stmt, _, err := c.Prepare(`SELECT count(*) FROM notes`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	return int(stmt.ColumnInt64(0))
}
