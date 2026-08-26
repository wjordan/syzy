package clone

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/sqlitebridge"
)

// Pure-codec tests. Integration tests that round-trip through a real
// syzy node live in syzy_test.go at the module root to avoid an import
// cycle (the syzy package imports this one).

func TestAdopt_RejectsMalformedBundle(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"truncated magic", []byte("SY")},
		{"bad magic", []byte("XXXXX")},
		{"wrong version", append(bundleMagic[:], 0xFE)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "x.db")
			_, err := Adopt(bytes.NewReader(tt.body), dst)
			if err == nil {
				t.Fatalf("expected error")
			}
			if _, statErr := os.Stat(dst); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("dst leaked: %v", statErr)
			}
		})
	}
}

func TestAdoptFilesMovesStagedPair(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "staged-metadata.db")
	appPath := filepath.Join(dir, "staged-app.db")
	dstPath := filepath.Join(dir, "dst.db")

	meta, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.Close(); err != nil {
		t.Fatal(err)
	}
	app, err := sqlitebridge.Open(appPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Exec(`CREATE TABLE moved (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	stagedAppInfo, err := os.Stat(appPath)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := AdoptFiles(metaPath, appPath, dstPath); err != nil {
		t.Fatal(err)
	}
	for _, consumed := range []string{metaPath, appPath} {
		if _, err := os.Stat(consumed); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source %s was not consumed: %v", consumed, err)
		}
	}
	for _, installed := range []string{dstPath, layout.MetaDB(dstPath)} {
		if _, err := os.Stat(installed); err != nil {
			t.Fatalf("installed %s: %v", installed, err)
		}
	}
	installedAppInfo, err := os.Stat(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(stagedAppInfo, installedAppInfo) {
		t.Fatal("app.db was copied instead of renamed")
	}

	db, err := sqlitebridge.Open(dstPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmt, _, err := db.Prepare(`SELECT count(*) FROM sqlite_master WHERE name='moved'`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()
	if row, err := stmt.Step(); err != nil {
		t.Fatal(err)
	} else if !row || stmt.ColumnInt64(0) != 1 {
		t.Fatal("adopted app.db did not contain staged schema")
	}
}

func TestPinSnapshots_ExcludesWritesAfterPin(t *testing.T) {
	dir := t.TempDir()
	appPath := filepath.Join(dir, "app.db")
	if err := os.MkdirAll(layout.MetaDir(appPath), 0o755); err != nil {
		t.Fatal(err)
	}
	open := func(path string) *sqlitebridge.Conn {
		c, err := sqlitebridge.Open(path, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		if err := c.Exec(`PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
			CREATE TABLE before_pin (id INTEGER PRIMARY KEY, v BLOB);
			WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100)
			INSERT INTO before_pin SELECT x, randomblob(1000) FROM n`); err != nil {
			t.Fatal(err)
		}
		return c
	}
	app := open(appPath)
	_ = open(layout.MetaDB(appPath))

	pb, err := PinSnapshots(appPath)
	if err != nil {
		t.Fatal(err)
	}
	defer pb.Close()
	// This is the moment the caller releases its writer barrier.
	if err := app.Exec(`CREATE TABLE after_pin (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	_, stagedApp, err := pb.Files()
	if err != nil {
		t.Fatal(err)
	}
	staged, err := sqlitebridge.Open(stagedApp, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	stmt, _, err := staged.Prepare(`SELECT count(*) FROM sqlite_master WHERE name='after_pin'`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()
	if row, err := stmt.Step(); err != nil {
		t.Fatal(err)
	} else if !row {
		t.Fatal("count query returned no row")
	}
	if got := stmt.ColumnInt64(0); got != 0 {
		t.Fatalf("snapshot included %d schema writes committed after PinSnapshots returned", got)
	}
}
