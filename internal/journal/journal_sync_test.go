package journal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestSyncOnAppendAndRecover is the user-visible durability contract:
// open SyncOn, append (forcing rotation), close, reopen, recover all.
func TestSyncOnAppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	// Tiny segment so a small number of appends rotate.
	const segSize = fileHeaderSize + 1024
	j, err := Open(dir, segSize, SyncOn)
	if err != nil {
		t.Fatalf("Open(SyncOn): %v", err)
	}
	payload := make([]byte, 96)
	for i := range payload {
		payload[i] = byte(i)
	}
	const N = 50 // enough to rotate several times
	for i := 0; i < N; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i+1), 7, payload); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and walk the records back.
	j2, err := Open(dir, segSize, SyncOn)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer j2.Close()
	it := j2.Iterate(0)
	count := 0
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, ErrPending) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		count++
		if rec.Seq != uint64(count) {
			t.Errorf("record %d seq = %d; want %d", count, rec.Seq, count)
		}
	}
	if count != N {
		t.Errorf("recovered %d records; want %d", count, N)
	}
}

// TestSyncOnIdempotent: repeat Sync() without intervening Append is
// a cheap no-op (syncedHead bookkeeping prevents double-msync).
func TestSyncOnIdempotent(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	if err := j.Sync(); err != nil {
		t.Fatalf("Sync on empty: %v", err)
	}
	if _, _, err := j.Append(KindLocalDML, 1, 1, []byte("payload")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j.Sync(); err != nil {
		t.Fatalf("Sync 1: %v", err)
	}
	// No new appends — syncedHead == head, msync range is empty.
	if err := j.Sync(); err != nil {
		t.Fatalf("Sync 2 (no-op): %v", err)
	}
}

// TestSyncOffIsNoop: Sync() under SyncOff returns nil without msync.
func TestSyncOffIsNoop(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("Open(SyncOff): %v", err)
	}
	defer j.Close()
	if _, _, err := j.Append(KindLocalDML, 1, 1, []byte("p")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Sync is defined but a no-op.
	if err := j.Sync(); err != nil {
		t.Fatalf("Sync(off): %v", err)
	}
}

// TestSyncOnRotationCreatesDurableSegment: rotation under SyncOn
// makes new segment files visible on disk (rotate-time fdatasync +
// parent-dir fsync).
func TestSyncOnRotationCreatesDurableSegment(t *testing.T) {
	dir := t.TempDir()
	// minSegmentSize requires headerSize + 1024 payload reserve; size
	// the segment so a handful of large-payload appends rotates it.
	const segSize = fileHeaderSize + 1024
	j, err := Open(dir, segSize, SyncOn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	payload := make([]byte, 256) // ~300 bytes per record after framing
	// Append enough to force several rotations.
	for i := 0; i < 30; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i+1), 1, payload); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	segs := j.Segments()
	if len(segs) < 2 {
		t.Fatalf("expected at least 2 segments after rotations; got %v", segs)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Every segment file should exist on disk (rotation's
	// fsyncSegmentMeta path makes the directory entry durable).
	for _, n := range segs {
		path := segmentPath(dir, n)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("segment %d missing on disk: %v", n, err)
		}
	}
}

// TestSyncOnDirFileLifecycle: dirFile opens under SyncOn, clears on
// Close, stays nil under SyncOff.
func TestSyncOnDirFileLifecycle(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if j.dirFile == nil {
		t.Errorf("SyncOn: dirFile is nil; want open")
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if j.dirFile != nil {
		t.Errorf("after Close: dirFile not cleared")
	}

	// Reopen (existing journal) under SyncOff; dirFile should remain nil.
	j2, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("Reopen(SyncOff): %v", err)
	}
	defer j2.Close()
	if j2.dirFile != nil {
		t.Errorf("SyncOff: dirFile not nil; want nil")
	}

	// Sanity: directory still exists and contains segments.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) == 0 {
		t.Errorf("journal dir %s is empty after reopen", filepath.Base(dir))
	}
}
