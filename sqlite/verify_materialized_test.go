package sqlite

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/syzy/sqlitebridge"
)

// A materialized image can be structurally intact and still hold rows in the
// wrong order. quick_check does not look: it skips index-content checks, and
// in a WITHOUT ROWID table the rows are the index, so nothing verifies key
// order. These tests pin the restore verifier to a check that can see it.

// swapFirstTwoCells reorders the first two cells on a b-tree page by swapping
// their pointers, leaving every cell's bytes and the page's structure intact.
func swapFirstTwoCells(t *testing.T, path string, pgno, pageSize int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	off := int64(pgno-1) * int64(pageSize)
	page := make([]byte, pageSize)
	if _, err := f.ReadAt(page, off); err != nil {
		t.Fatalf("read page %d: %v", pgno, err)
	}
	// Interior pages carry an extra four-byte right-child pointer before the
	// cell pointer array.
	headerSize := 8
	if page[0] == 0x02 || page[0] == 0x05 {
		headerSize = 12
	}
	ncell := int(binary.BigEndian.Uint16(page[3:5]))
	if ncell < 2 {
		t.Fatalf("page %d has %d cells, need at least 2", pgno, ncell)
	}
	firstPtr := page[headerSize : headerSize+2]
	secondPtr := page[headerSize+2 : headerSize+4]
	first := binary.BigEndian.Uint16(firstPtr)
	binary.BigEndian.PutUint16(firstPtr, binary.BigEndian.Uint16(secondPtr))
	binary.BigEndian.PutUint16(secondPtr, first)
	if _, err := f.WriteAt(page, off); err != nil {
		t.Fatalf("write page %d: %v", pgno, err)
	}
}

// buildCellClockDB writes a metadata.db-shaped database: one WITHOUT ROWID
// table whose rows fit on a single leaf page. It returns the database path and
// the table's root page.
func buildCellClockDB(t *testing.T, dir string, pageSize int) (string, int) {
	t.Helper()
	path := filepath.Join(dir, "metadata.db")
	conn, err := sqlitebridge.Open(path, sqlitebridge.OpenReadWrite|sqlitebridge.OpenCreate|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, stmt := range []string{
		fmt.Sprintf("PRAGMA page_size=%d", pageSize),
		"PRAGMA journal_mode=delete",
		"CREATE TABLE cell_clock(k TEXT PRIMARY KEY, v BLOB NOT NULL) STRICT, WITHOUT ROWID",
		"INSERT INTO cell_clock VALUES ('a',x'01'),('b',x'02'),('c',x'03'),('d',x'04'),('e',x'05')",
	} {
		if err := conn.Exec(stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}
	row, err := conn.QueryInt64Row("SELECT rootpage FROM sqlite_schema WHERE name='cell_clock'")
	if err != nil {
		t.Fatalf("rootpage: %v", err)
	}
	if len(row) != 1 {
		t.Fatalf("rootpage: got %d columns", len(row))
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return path, int(row[0])
}

func TestVerifyMaterializedDBAcceptsHealthyImage(t *testing.T) {
	t.Parallel()
	path, _ := buildCellClockDB(t, t.TempDir(), 4096)
	if err := verifyMaterializedDB(path); err != nil {
		t.Fatalf("verifyMaterializedDB rejected a healthy image: %v", err)
	}
}

func TestVerifyMaterializedDBDetectsPrimaryKeyDisorder(t *testing.T) {
	t.Parallel()
	const pageSize = 4096
	path, rootpage := buildCellClockDB(t, t.TempDir(), pageSize)
	swapFirstTwoCells(t, path, rootpage, pageSize)

	err := verifyMaterializedDB(path)
	if err == nil {
		t.Fatal("verifyMaterializedDB accepted an image with rows out of primary-key order")
	}
	if !strings.Contains(err.Error(), "PRIMARY KEY order") {
		t.Fatalf("unexpected error: %v", err)
	}
}
