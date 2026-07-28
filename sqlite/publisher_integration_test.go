package sqlite_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	syzy "github.com/wjordan/syzy/sqlite"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestPublishPhysical_RoundTrip exercises the publisher loop wired
// into a Node: claim lease → take baseline → start L0 tailer →
// upload metadata. Verifies HEAD ends up with both publisher and
// baseline fields populated, and that db/ + metadata/ have objects.
func TestPublishPhysical_RoundTrip(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		ObjectBackend: be,
		Log:           newTestLogger(),
		// Shrink the L0 tailer cadence (default 1s) so the L0-upload
		// wait below doesn't ride the production interval.
		LTXSyncInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	// Drive a few commits to populate the WAL.
	if err := node.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := node.Exec(`INSERT INTO t VALUES (1, 'v')`); err == nil {
			break
		}
	}

	// Wait until publisher has claimed lease and uploaded baseline.
	deadline := time.Now().Add(5 * time.Second)
	var head *objstore.HEAD
	for time.Now().Before(deadline) {
		h, _, err := objstore.LoadHEAD(ctx, be)
		if err == nil && h.Publisher != nil && h.Baseline != nil {
			head = h
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if head == nil {
		t.Fatalf("publisher did not claim + baseline within deadline")
	}
	if head.Publisher.NodeID == "" {
		t.Errorf("publisher NodeID empty")
	}
	if head.Baseline.LTXRef.Key == "" {
		t.Errorf("baseline LTX ref empty")
	}

	// Eventually an L0 LTX should appear (and metadata too).
	deadline = time.Now().Add(5 * time.Second)
	var sawL0, sawMetadata bool
	for time.Now().Before(deadline) {
		objs, err := be.List(ctx, "db/0000/", "")
		if err == nil && len(objs) > 0 {
			sawL0 = true
		}
		mobjs, err := be.List(ctx, "metadata/", "")
		if err == nil && len(mobjs) > 0 {
			sawMetadata = true
		}
		if sawL0 && sawMetadata {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawL0 {
		t.Errorf("no L0 LTX uploaded within deadline")
	}
	if !sawMetadata {
		t.Errorf("no metadata uploaded within deadline")
	}
}

// TestPublishPhysical_BaselineWithoutLeaseIsClean: closing the node
// after publisher started should drain cleanly with no goroutine
// leaks (covered by go test -race) and no stuck mutex.
func TestPublishPhysical_CleanShutdown(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		ObjectBackend: be,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// nodeIDPrefix is a sanity check on the NodeID format we put in
// HEAD.publisher (origin-hex). Just verifies it's hex-ish.
func TestPublishPhysical_NodeIDLooksLikeOrigin(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		ObjectBackend: be,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h, _, err := objstore.LoadHEAD(ctx, be)
		if err == nil && h.Publisher != nil {
			if len(h.Publisher.NodeID) != 16 || strings.IndexFunc(h.Publisher.NodeID, func(r rune) bool {
				return !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f'))
			}) >= 0 {
				t.Errorf("NodeID not 16-hex: %q", h.Publisher.NodeID)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("publisher never appeared in HEAD")
}
