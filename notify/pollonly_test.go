package notify

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/futex"
)

// newPollOnlyPair opens a writer+reader over one feed file and forces the
// reader into pollOnly mode, as if the feed were FUSE/virtiofs-backed.
func newPollOnlyPair(t *testing.T) (*Writer, *Reader) {
	t.Helper()
	path := t.TempDir() + "/notify.feed"
	w, err := NewWriter(WriterConfig{Path: path, NumSlots: 8})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	r, err := NewReader(ReaderConfig{Path: path})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	r.pollOnly = true
	return w, r
}

// TestPollOnlyInterruptUnblocksRead pins the park-safety contract: an
// Interrupt must get a pollOnly Read out of its bounded wait immediately
// (callers join the reading goroutine before unmapping the feed's mount).
func TestPollOnlyInterruptUnblocksRead(t *testing.T) {
	_, r := newPollOnlyPair(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := r.Read(ctx)
		done <- err
	}()
	<-started
	cancel()
	r.Interrupt()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Read returned %v, want context.Canceled", err)
		}
	case <-time.After(readerWakeInterval / 2):
		t.Fatal("Read did not return promptly after Interrupt; still parked in the bounded wait")
	}
}

// TestPollOnlyInterruptDeliversAppend verifies a pollOnly reader observes
// an Append when kicked (the cross-kernel deployment's wake transport
// reduces to Interrupt-or-timeout; the futex wake is suppressed).
func TestPollOnlyInterruptDeliversAppend(t *testing.T) {
	w, r := newPollOnlyPair(t)

	// Stale wake token from an earlier Interrupt must be harmless.
	r.Interrupt()

	done := make(chan []Notification, 1)
	go func() {
		notifs, err := r.Read(context.Background())
		if err != nil {
			t.Errorf("Read: %v", err)
		}
		done <- notifs
	}()
	if err := w.Append([]Change{{Origin: 7, Seq: 1, Op: OpInsert, Table: "t", PK: []byte("k")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	r.Interrupt()
	select {
	case notifs := <-done:
		if len(notifs) != 1 || notifs[0].Lossy || len(notifs[0].Changes) != 1 {
			t.Fatalf("unexpected notifications: %+v", notifs)
		}
	case <-time.After(readerWakeInterval / 2):
		t.Fatal("pollOnly Read did not observe the Append within the interrupt path")
	}
}

// TestFeedEligibleOnLocalFS: on a regular local filesystem the futex fast
// path must stay on (pollOnly false for both ends).
func TestFeedEligibleOnLocalFS(t *testing.T) {
	if !futex.Supported {
		t.Skip("no futex on this platform")
	}
	path := t.TempDir() + "/notify.feed"
	w, err := NewWriter(WriterConfig{Path: path, NumSlots: 8})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()
	r, err := NewReader(ReaderConfig{Path: path})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	if w.pollOnly || r.pollOnly {
		t.Fatalf("pollOnly on local fs: writer=%v reader=%v", w.pollOnly, r.pollOnly)
	}
}
