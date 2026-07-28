package mirror_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/mirror"
)

func segmentCount(t *testing.T, root string, o crdt.Origin) int {
	t.Helper()
	ents, err := os.ReadDir(originDir(root, o))
	if err != nil {
		t.Fatalf("read origin dir: %v", err)
	}
	n := 0
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".wal" || !e.IsDir() {
			n++
		}
	}
	return n
}

// TestRetainSealed: segments whose maxSeq ceiling is at or below the sealed
// tip are dropped (their records are durable in the bucket); the active tail
// segment always survives, appends keep working, and Serve still covers the
// retained range.
func TestRetainSealed(t *testing.T) {
	root := t.TempDir()
	mgr, err := mirror.New(mirror.Config{Root: root, SegmentSize: 1088})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	const o = crdt.Origin(9)
	const n = 200 // 18-byte payloads over 1088-byte segments → several segments
	for i := crdt.Seq(1); i <= n; i++ {
		if err := mgr.Append(o, payload(o, i, byte(i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	drainMirror(t, mgr, o, n)
	before := segmentCount(t, root, o)
	if before < 3 {
		t.Fatalf("test needs >=3 segments, got %d", before)
	}

	// Nothing sealed → no-op.
	if err := mgr.RetainSealed(o, 0); err != nil {
		t.Fatalf("RetainSealed(0): %v", err)
	}
	if got := segmentCount(t, root, o); got != before {
		t.Fatalf("RetainSealed(0) dropped segments: %d -> %d", before, got)
	}

	// Everything sealed → only the active tail segment survives.
	if err := mgr.RetainSealed(o, n); err != nil {
		t.Fatalf("RetainSealed(%d): %v", n, err)
	}
	after := segmentCount(t, root, o)
	if after >= before || after < 1 {
		t.Fatalf("RetainSealed kept %d of %d segments; want fewer, >=1", after, before)
	}

	// Unknown origin is a no-op, appends still work post-truncation.
	if err := mgr.RetainSealed(crdt.Origin(999), 5); err != nil {
		t.Fatalf("RetainSealed(unknown): %v", err)
	}
	if err := mgr.Append(o, payload(o, n+1, 0x7f)); err != nil {
		t.Fatalf("Append post-retain: %v", err)
	}
	drainMirror(t, mgr, o, 1)
}
