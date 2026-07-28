package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// Node.Close must cancel the publisher before draining writeMu. The
// publisher's takeover baseline (claimOrTakeover → takeCoupledBaselines
// → snapshotPinned) holds writeMu across an unbounded waitAllDrained
// that only its context cancellation can unblock; acquiring writeMu
// first deadlocks Close against it. Regression for the production
// shutdown wedge (2026-06-12): SIGTERM hung past the systemd stop
// timeout, and the SIGKILL'd process wedged unkillable on an exit-time
// fuse_flush against its own FUSE mount.
func TestCloseCancelsPublisherHoldingWriteMu(t *testing.T) {
	t.Parallel()
	node, err := Open(context.Background(), Config{
		Path: filepath.Join(t.TempDir(), "app.db"),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Simulate the wedged publisher: hold writeMu until the publisher
	// context cancels — the exact lock/unblock shape of a takeover
	// baseline stuck in waitAllDrained.
	ctx, cancel := context.WithCancel(context.Background())
	node.publisherCancel = cancel
	node.publisherDone = make(chan struct{})
	locked := make(chan struct{})
	go func() {
		defer close(node.publisherDone)
		node.writeMu.Lock()
		close(locked)
		<-ctx.Done()
		node.writeMu.Unlock()
	}()
	<-locked

	done := make(chan error, 1)
	go func() { done <- node.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close deadlocked against a publisher holding writeMu")
	}
}
