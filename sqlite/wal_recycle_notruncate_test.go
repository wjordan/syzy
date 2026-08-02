package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/syzy/internal/layout"
)

// TestWriterWALRestartDoesNotTruncate pins the recycle contract: a commit
// that restarts a fully-backfilled WAL rewinds the write position but must
// NOT shrink the file (journal_size_limit unset — commit-tail truncation
// runs while readers are live and converts a stale wal-index view into
// zero-page hole reads). Only wal_checkpoint(TRUNCATE), which excludes all
// readers, may shrink the WAL.
func TestWriterWALRestartDoesNotTruncate(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.db")
	node, err := Open(context.Background(), Config{Path: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	db := node.WriterDB()
	var lim int64
	if err := db.QueryRow(`PRAGMA journal_size_limit`).Scan(&lim); err != nil {
		t.Fatalf("journal_size_limit: %v", err)
	}
	if lim != -1 {
		t.Fatalf("journal_size_limit = %d, want -1 (unset); commit-tail WAL truncation must stay disabled", lim)
	}

	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	blob := make([]byte, 4096)
	for i := 0; i < 40; i++ {
		if _, err := db.Exec(`INSERT INTO t (v) VALUES (?)`, blob); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	walPath := path + "-wal"
	var busy, nLog, nCkpt int64
	if err := db.QueryRow(`PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &nLog, &nCkpt); err != nil {
		t.Fatalf("passive checkpoint: %v", err)
	}
	if busy != 0 || nLog != nCkpt || nLog == 0 {
		t.Fatalf("checkpoint not clean: busy=%d nLog=%d nCkpt=%d", busy, nLog, nCkpt)
	}
	st, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	sizeBefore := st.Size()

	// The next commit begins on a fully-backfilled WAL: SQLite restarts the
	// log (walRestartLog) and writes from offset 32.
	if _, err := db.Exec(`INSERT INTO t (v) VALUES (x'00')`); err != nil {
		t.Fatalf("restarting insert: %v", err)
	}
	st, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal after restart: %v", err)
	}
	if st.Size() < sizeBefore {
		t.Fatalf("restarting commit truncated the WAL: %d -> %d bytes", sizeBefore, st.Size())
	}

	// Prove the restart happened: the live frame count reset to the new
	// generation's few frames despite the file keeping its high-water size.
	if err := db.QueryRow(`PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &nLog, &nCkpt); err != nil {
		t.Fatalf("post-restart checkpoint: %v", err)
	}
	if nLog >= 40 {
		t.Fatalf("WAL did not restart: %d live frames", nLog)
	}

	// The sanctioned shrink path still works.
	if err := db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &nLog, &nCkpt); err != nil {
		t.Fatalf("truncate checkpoint: %v", err)
	}
	st, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal after truncate: %v", err)
	}
	if st.Size() != 0 {
		t.Fatalf("wal_checkpoint(TRUNCATE) left %d bytes", st.Size())
	}
}

// TestCheckpointFenceRejectsZeroedHeader pins the fail-closed gate: a
// checkpoint fence must refuse to proceed once the on-disk page 1 is no
// longer a SQLite header.
func TestCheckpointFenceRejectsZeroedHeader(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "app.db")
	node, err := Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	db := node.WriterDB()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	var busy, nLog, nCkpt int64
	if err := db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &nLog, &nCkpt); err != nil {
		t.Fatalf("materialize: %v", err)
	}

	if err := node.appCheckpoint(ctx, "PASSIVE", nil); err != nil {
		t.Fatalf("healthy fence: %v", err)
	}

	zeroHeader(t, path)
	err = node.appCheckpoint(ctx, "PASSIVE", nil)
	if err == nil || !strings.Contains(err.Error(), "page 1 header invalid") {
		t.Fatalf("app fence on zeroed header: want page-1 error, got %v", err)
	}

	metaPath := layout.MetaDB(path)
	if err := node.metaCheckpoint(ctx, "TRUNCATE", nil); err != nil {
		t.Fatalf("healthy meta fence: %v", err)
	}
	zeroHeader(t, metaPath)
	err = node.metaCheckpoint(ctx, "PASSIVE", nil)
	if err == nil || !strings.Contains(err.Error(), "page 1 header invalid") {
		t.Fatalf("meta fence on zeroed header: want page-1 error, got %v", err)
	}
}

// zeroHeader simulates the observed corruption: the first 16 bytes of the
// database file overwritten with zeros while the process stays up.
func zeroHeader(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	if _, err := f.WriteAt(make([]byte, 16), 0); err != nil {
		t.Fatalf("zero header %s: %v", path, err)
	}
}
