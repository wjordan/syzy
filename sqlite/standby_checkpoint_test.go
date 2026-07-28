package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func walSize(t *testing.T, p string) int64 {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("stat wal: %v", err)
	}
	return fi.Size()
}

// TestStandbyWALCheckpointTruncates: the checkpoint path the standby loop uses
// (appCheckpoint TRUNCATE with no tailer fence) shrinks a grown app.db WAL,
// and a single-node node never reports holding a publisher lease (so the loop
// would actually run there).
func TestStandbyWALCheckpointTruncates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	node, err := Open(context.Background(), Config{Path: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if node.HoldsPublisherLease() {
		t.Fatal("single-node node should not report holding a publisher lease")
	}

	// Grow the WAL with committed writes; wal_autocheckpoint is 0 and there is
	// no publisher checkpoint loop on a bare single-node node, so it climbs.
	db := node.WriterDB()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	blob := strings.Repeat("x", 4000)
	for i := 0; i < 500; i++ {
		if _, err := db.Exec(`INSERT INTO t (v) VALUES (?)`, blob); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	walPath := path + "-wal"
	before := walSize(t, walPath)
	if before < 200*1024 {
		t.Fatalf("WAL only grew to %d bytes; writes did not accumulate as expected", before)
	}

	if err := node.appCheckpoint(context.Background(), "TRUNCATE", nil); err != nil {
		t.Fatalf("standby checkpoint: %v", err)
	}
	after := walSize(t, walPath)
	if after >= before {
		t.Fatalf("WAL did not shrink: before=%d after=%d", before, after)
	}
}
