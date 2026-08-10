package sqlite_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	syzy "github.com/wjordan/syzy/sqlite"
)

func TestExternalSQLiteReaderDoesNotDisruptLiveWAL(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI is not installed")
	}
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(context.Background(), syzy.Config{
		Path:          dbPath,
		ObjectBackend: testBackend(t),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer node.Close()
	if err := node.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT); INSERT INTO t VALUES (1, 'one')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	walPath := dbPath + "-wal"
	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat live WAL: %v", err)
	}
	cmd := exec.Command("sqlite3", dbPath, `PRAGMA journal_mode; SELECT count(*) FROM t; PRAGMA quick_check`)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("external reader: %v: %s", err, out)
	}
	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL after external reader: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("external reader replaced the live WAL")
	}
	if err := node.Exec(`INSERT INTO t VALUES (2, 'two')`); err != nil {
		t.Fatalf("write after external reader: %v", err)
	}
}

func TestExternalSQLiteCheckpointDoesNotBreakLiveConnections(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI is not installed")
	}
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(context.Background(), syzy.Config{
		Path:          dbPath,
		ObjectBackend: testBackend(t),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer node.Close()
	if err := node.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT); INSERT INTO t VALUES (1, 'one')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cmd := exec.Command("sqlite3", dbPath, `PRAGMA wal_checkpoint(TRUNCATE)`)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("external checkpoint: %v: %s", err, out)
	}
	if err := node.Exec(`INSERT INTO t VALUES (2, 'two')`); err != nil {
		t.Fatalf("write after external checkpoint: %v", err)
	}
}
