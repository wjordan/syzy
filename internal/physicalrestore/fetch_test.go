package physicalrestore

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
)

// stubBucket fails the first failGets Gets / failLists Lists with err, then
// serves getData / an empty list. A nil embedded Bucket would panic on any
// other method, which is the point: the tests must only exercise Get/List.
type stubBucket struct {
	objectstore.Bucket
	mu        sync.Mutex
	failGets  int
	failLists int
	getCalls  int
	listCalls int
	err       error
	getData   []byte
	hang      bool // Get blocks until ctx is cancelled (a stalled connection)
	hangList  bool // List blocks until ctx is cancelled
}

func (b *stubBucket) Get(ctx context.Context, _ string) (io.ReadCloser, string, error) {
	b.mu.Lock()
	b.getCalls++
	fail := b.failGets > 0
	if fail {
		b.failGets--
	}
	hang := b.hang
	b.mu.Unlock()
	if hang {
		<-ctx.Done()
		return nil, "", ctx.Err()
	}
	if fail {
		return nil, "", b.err
	}
	return io.NopCloser(bytesReader(b.getData)), "et", nil
}

func (b *stubBucket) List(ctx context.Context, _, _ string) ([]objectstore.ObjectInfo, error) {
	b.mu.Lock()
	b.listCalls++
	fail := b.failLists > 0
	if fail {
		b.failLists--
	}
	hang := b.hangList
	b.mu.Unlock()
	if hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if fail {
		return nil, b.err
	}
	return []objectstore.ObjectInfo{{Key: "k"}}, nil
}

func (b *stubBucket) gets() int  { b.mu.Lock(); defer b.mu.Unlock(); return b.getCalls }
func (b *stubBucket) lists() int { b.mu.Lock(); defer b.mu.Unlock(); return b.listCalls }

func bytesReader(p []byte) io.Reader { return &sliceReader{p: p} }

type sliceReader struct {
	p []byte
	i int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.p) {
		return 0, io.EOF
	}
	n := copy(p, r.p[r.i:])
	r.i += n
	return n, nil
}

func newBounded(b objectstore.Bucket, attempts int, timeout, backoff time.Duration) *boundedBucket {
	return &boundedBucket{Bucket: b, attempts: attempts, timeout: timeout, backoff: backoff}
}

// A transient GET failure mid-restore must be retried, not abort the restore.
func TestBoundedBucket_RetriesTransientGet(t *testing.T) {
	t.Parallel()
	st := &stubBucket{failGets: 3, err: errors.New("simulated 408 RequestCanceled"), getData: []byte("frame")}
	bb := newBounded(st, 6, 30*time.Second, time.Millisecond)
	rc, _, err := bb.Get(context.Background(), "db/0001.ltx")
	if err != nil {
		t.Fatalf("Get should retry through transient failures: %v", err)
	}
	got, _ := io.ReadAll(rc)
	if string(got) != "frame" {
		t.Fatalf("got %q, want %q", got, "frame")
	}
	if st.gets() != 4 {
		t.Fatalf("made %d GETs, want 4 (3 transient + 1 success)", st.gets())
	}
}

// A missing object is a genuinely incomplete chain: terminal, no retry storm.
func TestBoundedBucket_NotFoundIsTerminal(t *testing.T) {
	t.Parallel()
	st := &stubBucket{failGets: 1 << 30, err: objectstore.ErrNotFound}
	bb := newBounded(st, 6, 30*time.Second, time.Millisecond)
	if _, _, err := bb.Get(context.Background(), "db/missing.ltx"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if st.gets() != 1 {
		t.Fatalf("made %d GETs, want exactly 1 (no retry on NotFound)", st.gets())
	}
}

// The regression: a hung GET must be bounded by the per-attempt timeout and
// fail after the budget, not block the restore forever.
func TestBoundedBucket_BoundsHungGet(t *testing.T) {
	t.Parallel()
	st := &stubBucket{hang: true}
	bb := newBounded(st, 2, 40*time.Millisecond, time.Millisecond)
	done := make(chan struct{})
	var err error
	go func() {
		_, _, err = bb.Get(context.Background(), "db/hangs.ltx")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not return — a hung GET wedged the restore")
	}
	if err == nil {
		t.Fatal("want an error after the hung GET exhausted the attempt budget")
	}
	if st.gets() != 2 {
		t.Fatalf("made %d GET attempts, want exactly 2", st.gets())
	}
}

// A hung LIST (chain-tip discovery) must be bounded too, not just GET.
func TestBoundedBucket_BoundsHungList(t *testing.T) {
	t.Parallel()
	st := &stubBucket{hangList: true}
	bb := newBounded(st, 2, 40*time.Millisecond, time.Millisecond)
	done := make(chan struct{})
	var err error
	go func() {
		_, err = bb.List(context.Background(), "db/", "")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("List did not return — a hung LIST wedged the restore")
	}
	if err == nil {
		t.Fatal("want an error after the hung LIST exhausted the attempt budget")
	}
	if st.lists() != 2 {
		t.Fatalf("made %d LIST attempts, want exactly 2", st.lists())
	}
}

// A transient LIST failure (chain-tip discovery) must retry too.
func TestBoundedBucket_RetriesTransientList(t *testing.T) {
	t.Parallel()
	st := &stubBucket{failLists: 2, err: errors.New("simulated list stall")}
	bb := newBounded(st, 6, 30*time.Second, time.Millisecond)
	out, err := bb.List(context.Background(), "db/", "")
	if err != nil {
		t.Fatalf("List should retry transient failures: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("got %d entries, want 1", len(out))
	}
	if st.lists() != 3 {
		t.Fatalf("made %d LISTs, want 3 (2 transient + 1 success)", st.lists())
	}
}
