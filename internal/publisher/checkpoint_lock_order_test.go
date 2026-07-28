package publisher

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/ltxstream"
)

// A coupled baseline holds the app writer fence while it drains the app
// tailer. A checkpoint must therefore wait for that fence without holding the
// tailer, or the two operations form a lock cycle.
func TestCheckpointStreamWriterFencePrecedesTailer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tailer := ltxstream.New(ltxstream.Config{
		WALPath: filepath.Join(t.TempDir(), "missing-wal"),
	}, ltxstream.Position{})

	var writerFence sync.Mutex
	writerFence.Lock() // Simulate a coupled baseline already holding writeMu.
	var releaseOnce sync.Once
	releaseFence := func() { releaseOnce.Do(writerFence.Unlock) }
	defer releaseFence()

	fenceAttempted := make(chan struct{})
	var signalOnce sync.Once
	checkpointCalled := false
	p := &Publisher{}
	p.app = stream{
		label:  "app",
		tailer: tailer,
		fence: func(_ context.Context, _ string, underFence func(checkpoint func() error) error) error {
			signalOnce.Do(func() { close(fenceAttempted) })
			writerFence.Lock()
			defer writerFence.Unlock()
			checkpoint := func() error {
				checkpointCalled = true
				return nil
			}
			if underFence != nil {
				return underFence(checkpoint)
			}
			return checkpoint()
		},
	}

	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- p.checkpointStream(ctx, &p.app) }()
	select {
	case <-fenceAttempted:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("checkpoint did not request the writer fence")
	}

	// This is the baseline's in-fence tail drain. It must be able to run while
	// the checkpoint waits for writeMu.
	drainDone := make(chan error, 1)
	go func() { drainDone <- tailer.Sync(ctx) }()
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("baseline tail drain: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		releaseFence()
		select {
		case <-checkpointDone:
		case <-time.After(50 * time.Millisecond):
		}
		t.Fatal("checkpoint held the tailer while waiting for the writer fence")
	}

	releaseFence()
	select {
	case err := <-checkpointDone:
		if err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		if !checkpointCalled {
			t.Fatal("writer fence hook did not run the checkpoint")
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("checkpoint did not finish after writer fence release")
	}
}

func TestCheckpointStreamDoesNotCreateHeartbeatProof(t *testing.T) {
	t.Parallel()
	tailer := ltxstream.New(ltxstream.Config{
		WALPath: filepath.Join(t.TempDir(), "missing-wal"),
	}, ltxstream.Position{})
	p := &Publisher{}
	p.app = stream{
		label:  "app",
		tailer: tailer,
		fence: func(_ context.Context, _ string, underFence func(func() error) error) error {
			return underFence(func() error { return nil })
		},
	}
	if err := p.checkpointStream(context.Background(), &p.app); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got := tailer.SuccessfulSyncs(); got != 0 {
		t.Fatalf("coordinated checkpoint created %d heartbeat proofs", got)
	}
}
