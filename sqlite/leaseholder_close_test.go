package sqlite_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

// TestNodeClose_ReleasesUniqueReadConn guards against the leaseholder's aux
// read connection (opened on the app DB when ObjectBackend is set) leaking
// past Close. A leaked fd keeps the app DB file — and any FUSE mount backing
// it — busy, which broke clean unmount of per-app databases downstream.
func TestNodeClose_ReleasesUniqueReadConn(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("fd-leak assertion reads /proc/self/fd (Linux only)")
	}
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		ObjectBackend: testBackend(t),
		SchemaLog:     schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Sanity: while the node is live, at least one fd references the app DB.
	if n := openFDsFor(t, dbPath); n == 0 {
		t.Fatalf("expected open fds on %s while the node is live", dbPath)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := openFDsFor(t, dbPath); n != 0 {
		t.Fatalf("after Close, %d fd(s) still reference %s (leaked aux conn)", n, dbPath)
	}
}

// openFDsFor counts this process's open file descriptors that resolve to
// path, via /proc/self/fd.
func openFDsFor(t *testing.T, path string) int {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("abs %s: %v", path, err)
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("/proc/self/fd unavailable: %v", err)
	}
	count := 0
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil {
			continue
		}
		if target == abs {
			count++
		}
	}
	return count
}
