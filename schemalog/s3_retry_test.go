package schemalog

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
)

// flakyBucket fails the first failGets Get calls with a transient error, then
// delegates — modelling a high-latency link returning a 408/timeout that clears
// on retry. List/Put/etc. pass through.
type flakyBucket struct {
	objectstore.Bucket
	mu       sync.Mutex
	failGets int
	calls    int
	err      error
}

func (b *flakyBucket) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	b.mu.Lock()
	b.calls++
	fail := b.failGets > 0
	if fail {
		b.failGets--
	}
	b.mu.Unlock()
	if fail {
		return nil, "", b.err
	}
	return b.Bucket.Get(ctx, key)
}

func (b *flakyBucket) getCalls() int { b.mu.Lock(); defer b.mu.Unlock(); return b.calls }

// A transient GET failure during catch-up must be retried, not abort catch-up
// (which would wedge the node's schema behind head — see s3.go).
func TestS3_GetEventRetriesTransientGet(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	flaky := &flakyBucket{Bucket: be, failGets: 3, err: errors.New("simulated 408 RequestCanceled")}
	s := NewS3WithBackend(flaky)
	s.getBackoff = time.Millisecond // keep the test fast

	ctx := context.Background()
	seq, err := s.Append(ctx, 0, []byte("op-1"), "/* raw */")
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	evs, err := s.Read(ctx, 0, 10)
	if err != nil {
		t.Fatalf("read should retry through %d transient failures, got: %v", 3, err)
	}
	if len(evs) != 1 || evs[0].SchemaSeq != seq {
		t.Fatalf("got %d events, want 1 at seq %d", len(evs), seq)
	}
	if flaky.failGets != 0 {
		t.Fatalf("expected all transient failures consumed, %d left", flaky.failGets)
	}
}

// Past the attempt budget, a persistent failure must surface (not loop forever).
func TestS3_GetEventGivesUpAfterBudget(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	flaky := &flakyBucket{Bucket: be, failGets: 1 << 30, err: errors.New("persistent backend error")}
	s := NewS3WithBackend(flaky)
	s.getAttempts = 3
	s.getBackoff = time.Millisecond

	if _, err := s.Append(context.Background(), 0, []byte("op"), "raw"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := s.Read(context.Background(), 0, 10); err == nil {
		t.Fatal("want error after the attempt budget is exhausted")
	}
	if c := flaky.getCalls(); c != s.getAttempts {
		t.Fatalf("made %d GET attempts, want exactly %d", c, s.getAttempts)
	}
}

// A cancelled parent context is a real shutdown, not a transient hiccup: stop
// immediately rather than burning the whole retry budget.
func TestS3_GetEventStopsOnCancelledContext(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	flaky := &flakyBucket{Bucket: be, failGets: 1 << 30, err: errors.New("transient")}
	s := NewS3WithBackend(flaky)
	s.getAttempts = 8
	s.getBackoff = time.Millisecond

	if _, err := s.Append(context.Background(), 0, []byte("op"), "raw"); err != nil {
		t.Fatalf("append: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	if _, err := s.Read(ctx, 0, 10); err == nil {
		t.Fatal("want error on a cancelled context")
	}
	if c := flaky.getCalls(); c > 1 {
		t.Fatalf("retried %d times under a cancelled context, want a single attempt", c)
	}
}
