package sqlite_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

// TestOpen_RotatesWhenSelfLogIncomplete: a start that claims an origin whose
// persisted senderNextSeq covers seqs the self-log no longer holds (the
// crash-era state a stale clean_shutdown flag hides) must rotate to a fresh
// origin and open, not exit. Before the in-open retry only the NEXT start
// rotated, because the failed one had flipped clean_shutdown to false.
func TestOpen_RotatesWhenSelfLogIncomplete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		SchemaLog:     schemalog.NewLocal(),
		ObjectBackend: testBackend(t), // replicated: self-log capture active
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := node.Exec(`CREATE TABLE kv (k TEXT PRIMARY KEY, v INT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := node.Exec(fmt.Sprintf(`INSERT INTO kv VALUES ('k%d', %d)`, i, i)); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	originA := node.OriginHex()
	if err := node.Close(); err != nil { // clean shutdown: clean_shutdown=true
		t.Fatalf("Close: %v", err)
	}

	// Wipe the origin's self-log while the metadata snapshot still claims its
	// seqs, so the reopen recovers into the promotion guard.
	origin, err := strconv.ParseUint(originA, 16, 64)
	if err != nil {
		t.Fatalf("parse origin %q: %v", originA, err)
	}
	selfLogDir := filepath.Join(dbPath+"-syzy", "mirror", fmt.Sprintf("origin_%d", origin))
	if _, err := os.Stat(selfLogDir); err != nil {
		t.Fatalf("self-log dir %s: %v", selfLogDir, err)
	}
	if err := os.RemoveAll(selfLogDir); err != nil {
		t.Fatalf("remove self-log: %v", err)
	}

	reopened, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		SchemaLog:     schemalog.NewLocal(),
		ObjectBackend: testBackend(t),
	})
	if err != nil {
		t.Fatalf("reopen after self-log loss: %v", err)
	}
	defer reopened.Close()
	if got := reopened.OriginHex(); got == originA {
		t.Fatalf("reopen kept incomplete origin %s; want rotation", got)
	}
	var n int
	if err := reopened.WriterDB().QueryRow(`SELECT count(*) FROM kv`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 5 {
		t.Fatalf("kv rows = %d, want 5", n)
	}
}
