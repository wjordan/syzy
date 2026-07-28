package sqlitebridge

import (
	"errors"
	"io"
	"path/filepath"
	"testing"
)

func TestBackup_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

	src, err := Open(srcPath, 0)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer src.Close()
	if err := src.Exec(`PRAGMA journal_mode = WAL;
		CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);
		INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c');`); err != nil {
		t.Fatalf("seed src: %v", err)
	}

	dst, err := Open(dstPath, 0)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()

	bk, err := BackupInit(dst, "main", src, "main")
	if err != nil {
		t.Fatalf("BackupInit: %v", err)
	}
	// Step in small chunks to exercise the multi-step path.
	for i := 0; i < 100; i++ {
		err := bk.Step(1)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = bk.Finish()
			t.Fatalf("Step: %v", err)
		}
		if i == 99 {
			t.Fatalf("backup did not finish in 100 steps; remaining=%d page_count=%d", bk.Remaining(), bk.PageCount())
		}
	}
	if err := bk.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	stmt, _, err := dst.Prepare(`SELECT id, v FROM t ORDER BY id`)
	if err != nil {
		t.Fatalf("prepare on dst: %v", err)
	}
	defer stmt.Finalize()
	var got []string
	for {
		ok, err := stmt.Step()
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(stmt.ColumnText(1)))
	}
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("dst rows: %v", got)
	}
}

func TestBackup_StepNegativeCopiesAll(t *testing.T) {
	dir := t.TempDir()
	src, err := Open(filepath.Join(dir, "src.db"), 0)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer src.Close()
	if err := src.Exec(`CREATE TABLE t (k INTEGER PRIMARY KEY); INSERT INTO t VALUES (1),(2),(3);`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	dst, err := Open(filepath.Join(dir, "dst.db"), 0)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()

	bk, err := BackupInit(dst, "main", src, "main")
	if err != nil {
		t.Fatalf("BackupInit: %v", err)
	}
	if err := bk.Step(-1); !errors.Is(err, io.EOF) {
		_ = bk.Finish()
		t.Fatalf("Step(-1) = %v, want io.EOF", err)
	}
	if err := bk.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
}
