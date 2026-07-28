package journal

import (
	"context"
	"testing"
	"time"
)

// TestPollOnlySharedWakeSuppressed: a FUSE/virtiofs-backed journal must not
// install the futex wake (the publish word lives in a DAX mapping); an
// explicit transport via SetWakeFunc still applies.
func TestPollOnlySharedWakeSuppressed(t *testing.T) {
	j, err := Open(t.TempDir(), 1<<16, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	j.pollOnly = true

	j.EnableSharedWake(true)
	if j.wakeFn.Load() != nil {
		t.Fatal("EnableSharedWake installed a futex wake on a pollOnly journal")
	}
	fired := false
	j.SetWakeFunc(func(*uint32) { fired = true })
	if j.wakeFn.Load() == nil {
		t.Fatal("SetWakeFunc must still install on a pollOnly journal")
	}
	if _, _, err := j.Append(KindLocalDML, 1, 1, []byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if !fired {
		t.Fatal("explicit wake transport did not fire on Append")
	}
}

// TestPollOnlyWaitAtHonorsContext: the sleep-poll WaitAt must return
// promptly on ctx cancellation instead of waiting out the timeout.
func TestPollOnlyWaitAtHonorsContext(t *testing.T) {
	j, err := Open(t.TempDir(), 1<<16, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	if _, _, err := j.Append(KindLocalDML, 1, 1, []byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	j.pollOnly = true

	head := j.Head()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- j.WaitAt(ctx, head, 10*time.Second) }()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("WaitAt returned %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pollOnly WaitAt ignored context cancellation")
	}
}

// TestPollOnlyWaitAtSeesPublish: the sleep-poll loop observes a record
// published while it waits, within one poll interval.
func TestPollOnlyWaitAtSeesPublish(t *testing.T) {
	j, err := Open(t.TempDir(), 1<<16, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	if _, _, err := j.Append(KindLocalDML, 1, 1, []byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	j.pollOnly = true

	head := j.Head()
	done := make(chan error, 1)
	go func() { done <- j.WaitAt(context.Background(), head, 5*time.Millisecond) }()
	if _, _, err := j.Append(KindLocalDML, 2, 1, []byte("y")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitAt: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pollOnly WaitAt never observed the publish")
	}
}
