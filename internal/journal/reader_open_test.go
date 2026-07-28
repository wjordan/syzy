package journal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestReaderOpenDoesNotCreate pins the reader invariant: opening a journal
// directory with no initialized segment under a read-only handle (segmentSize
// 0) reports ErrNoSegments and plants nothing. Before this, the reader created
// a 0-byte seg-0 and then failed every open with "segmentSize 0 too small",
// which the syncer retried each scan into a warn/conn storm.
func TestReaderOpenDoesNotCreate(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, 0, SyncOff)
	if !errors.Is(err, ErrNoSegments) {
		t.Fatalf("Open(empty, 0) err = %v, want ErrNoSegments", err)
	}
	if j != nil {
		t.Fatal("Open returned a non-nil journal alongside ErrNoSegments")
	}
	if nums, _ := listSegmentNumbers(dir); len(nums) != 0 {
		t.Fatalf("reader created %d segment file(s); a reader must create none", len(nums))
	}
}

func TestHasDrainableSegment(t *testing.T) {
	if HasDrainableSegment(filepath.Join(t.TempDir(), "absent")) {
		t.Error("missing dir: want false")
	}

	zero := t.TempDir()
	if err := os.WriteFile(segmentPath(zero, 0), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if HasDrainableSegment(zero) {
		t.Error("zero-length placeholder segment: want false")
	}

	// A writer-initialized journal is drainable, and a reader can then open it.
	wdir := t.TempDir()
	w, err := Open(wdir, minSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("writer Open: %v", err)
	}
	_ = w.Close()
	if !HasDrainableSegment(wdir) {
		t.Error("writer-initialized journal: want true")
	}
	r, err := Open(wdir, 0, SyncOff)
	if err != nil {
		t.Fatalf("reader Open on a written journal: %v", err)
	}
	_ = r.Close()
}
