package syncer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/journal"
)

// A sink marker persisted by a previous journal generation can land
// mid-record in the current one. Pre-AlignResume the drainer spun
// forever at the bogus offset (parse yields EOF below the published
// head; the misread publish word is nonzero so WaitAt never sleeps),
// freezing DrainedOffset and wedging every waitAllDrained caller.
func TestDrainerResumesFromMisalignedMarker(t *testing.T) {
	t.Parallel()
	jdir := filepath.Join(t.TempDir(), "jrn")
	j, err := journal.Open(jdir, 1<<20, journal.SyncOff)
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	var lastBoundary journal.Offset
	for i := 1; i <= 3; i++ {
		lastBoundary = journal.Offset(uint64(j.Head()))
		if _, _, err := j.Append(journal.KindLocalDML, uint64(i), 7, make([]byte, 100)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	head := j.Head()

	// Stale-generation marker: 16 bytes into the final record.
	sink := &mockSink{last: lastBoundary + 16}
	dr, err := NewDrainer(j, sink, WithSharedWake(), WithPollInterval(2*time.Millisecond))
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = dr.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, 5*time.Second, func() bool {
		return dr.DrainedOffset() >= head
	}, "drainer to converge past a misaligned resume marker")
	if got := sink.totalRecords(); got != 1 {
		t.Fatalf("sink applied %d records, want 1 (the record containing the stale marker)", got)
	}
}
