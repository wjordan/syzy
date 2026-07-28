package journal

import (
	"testing"
)

// BenchmarkAppend_SyncOff measures Append cost without any explicit
// fsync — the kernel-page-cache durability mode that is today's
// default. Baseline for the SyncOn comparison below.
//
// Run on a real filesystem; tmpfs/in-memory paths hide msync and
// fdatasync cost. b.TempDir() respects $TMPDIR so configure that to
// the disk under test.
func BenchmarkAppend_SyncOff(b *testing.B) {
	dir := b.TempDir()
	j, err := Open(dir, 4*1024*1024, SyncOff)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer j.Close()
	payload := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 1, payload); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}

// BenchmarkAppend_SyncOn_MsyncOnly measures the precise SyncOn mode:
// msync(MS_SYNC) per Append over the dirty range; no per-commit
// fdatasync (segments are pre-allocated so file metadata only changes
// at rotation). This is the actual producer cost when JournalSync is
// on. Compare to SyncOff for the operator-visible commit overhead.
func BenchmarkAppend_SyncOn_MsyncOnly(b *testing.B) {
	dir := b.TempDir()
	j, err := Open(dir, 4*1024*1024, SyncOn)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer j.Close()
	payload := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 1, payload); err != nil {
			b.Fatalf("Append: %v", err)
		}
		if err := j.Sync(); err != nil {
			b.Fatalf("Sync: %v", err)
		}
	}
}

// BenchmarkAppend_SyncOn_AlwaysFdatasync is the "naive" reference:
// msync + fdatasync per Append. We never run in this mode in
// production (the precision rule skips fdatasync when the segment
// file's metadata hasn't changed), but the bench documents the cost
// gap that the precision buys us.
//
// Implemented inline by reaching into the active segment's fd because
// there's no production code path that takes this shape.
func BenchmarkAppend_SyncOn_AlwaysFdatasync(b *testing.B) {
	dir := b.TempDir()
	j, err := Open(dir, 4*1024*1024, SyncOn)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer j.Close()
	payload := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 1, payload); err != nil {
			b.Fatalf("Append: %v", err)
		}
		if err := j.Sync(); err != nil {
			b.Fatalf("Sync: %v", err)
		}
		s := j.active.Load()
		if s == nil {
			b.Fatal("nil active segment")
		}
		if err := fdatasyncFile(s.file); err != nil {
			b.Fatalf("Fdatasync: %v", err)
		}
	}
}
