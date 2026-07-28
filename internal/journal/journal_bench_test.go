package journal

import (
	"fmt"
	"testing"
)

// BenchmarkAppend measures hot-path Append cost on the producer's
// commit_hook critical path. Should land in the same range as
// AsyncDrainMmap (~50–100 ns) — header encode + memcpy + atomic store.
func BenchmarkAppend(b *testing.B) {
	dir := b.TempDir()
	// Just-under-4-GiB segment. At ~100 ns/op the bench burns through
	// smaller segments well before the default benchtime expires.
	j, err := Open(dir, 0xFFFF_FF00, SyncOff) // 1 GiB so we never hit ErrSegmentFull at b.benchtime
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer j.Close()

	payload := make([]byte, 64) // representative touch-journal record
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 1, payload); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
	b.SetBytes(int64(recordTotalLen(uint32(len(payload)))))
}

// BenchmarkAppendByPayloadSize traces how Append cost scales with
// payload size — the producer's touch journal can vary widely (single
// PK tens of bytes; multi-row update with full row images kilobytes).
// Reports per-op ns and bytes/op so the integration phase can size
// segment expectations against expected workload mix.
func BenchmarkAppendByPayloadSize(b *testing.B) {
	for _, size := range []int{16, 64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("payload-%d", size), func(b *testing.B) {
			dir := b.TempDir()
			j, err := Open(dir, 0xFFFF_FF00, SyncOff)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer j.Close()
			payload := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := j.Append(KindLocalDML, uint64(i), 1, payload); err != nil {
					b.Fatalf("Append: %v", err)
				}
			}
		})
	}
}

// BenchmarkAppendIterate runs an iterator concurrently with Append to
// confirm the memory-ordering path doesn't tank under the bench load.
func BenchmarkAppendIterate(b *testing.B) {
	dir := b.TempDir()
	// Just-under-4-GiB segment. At ~100 ns/op the bench burns through
	// smaller segments well before the default benchtime expires.
	j, err := Open(dir, 0xFFFF_FF00, SyncOff)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer j.Close()

	payload := make([]byte, 64)
	stop := make(chan struct{})
	go func() {
		it := j.Iterate(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, _, err := it.Next(); err != nil {
				continue
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 1, payload); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
	close(stop)
}
