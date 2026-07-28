package memtransport

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/transport"
)

// applyCollect collects each delivered payload and notifies via signal so
// tests can wait deterministically without busy-waiting.
type applyCollect struct {
	mu     sync.Mutex
	got    [][]byte
	signal chan struct{}
}

func newApplyCollect() *applyCollect {
	return &applyCollect{signal: make(chan struct{}, 256)}
}

func (c *applyCollect) fn(_ context.Context, b []byte) error {
	c.mu.Lock()
	c.got = append(c.got, append([]byte(nil), b...))
	c.mu.Unlock()
	c.signal <- struct{}{}
	return nil
}

func (c *applyCollect) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.got...)
}

// waitN blocks until the collector has received at least n payloads or
// the timeout elapses.
func (c *applyCollect) waitN(t *testing.T, n int) [][]byte {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		if len(c.snapshot()) >= n {
			return c.snapshot()
		}
		select {
		case <-c.signal:
		case <-deadline.C:
			t.Fatalf("timeout waiting for %d deliveries; saw %d", n, len(c.snapshot()))
		}
	}
}

func runSubscribe(t *testing.T, p transport.Transport) *applyCollect {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := newApplyCollect()
	done := make(chan error, 1)
	go func() {
		done <- p.Subscribe(ctx, c.fn)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Errorf("Subscribe did not return after cancel")
		}
	})
	return c
}

func TestBroadcastFanOutTwoPeers(t *testing.T) {
	h := NewHub()
	a, b := h.Peer(), h.Peer()
	colA := runSubscribe(t, a)
	colB := runSubscribe(t, b)

	if err := a.Broadcast(context.Background(), []byte("hello")); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	gotA := colA.waitN(t, 1)
	gotB := colB.waitN(t, 1)
	if !bytes.Equal(gotA[0], []byte("hello")) {
		t.Errorf("peer A got %q; want hello", gotA[0])
	}
	if !bytes.Equal(gotB[0], []byte("hello")) {
		t.Errorf("peer B got %q; want hello", gotB[0])
	}
}

func TestBroadcastFromAnyPeer(t *testing.T) {
	h := NewHub()
	a, b := h.Peer(), h.Peer()
	colA := runSubscribe(t, a)
	colB := runSubscribe(t, b)

	if err := b.Broadcast(context.Background(), []byte("from-b")); err != nil {
		t.Fatalf("b.Broadcast: %v", err)
	}
	if err := a.Broadcast(context.Background(), []byte("from-a")); err != nil {
		t.Fatalf("a.Broadcast: %v", err)
	}
	gotA := colA.waitN(t, 2)
	gotB := colB.waitN(t, 2)
	if !bytes.Equal(gotA[0], []byte("from-b")) || !bytes.Equal(gotA[1], []byte("from-a")) {
		t.Errorf("peer A order = %q; want [from-b from-a]", gotA)
	}
	if !bytes.Equal(gotB[0], []byte("from-b")) || !bytes.Equal(gotB[1], []byte("from-a")) {
		t.Errorf("peer B order = %q; want [from-b from-a]", gotB)
	}
}

func TestSubscribeReturnsOnContextCancel(t *testing.T) {
	h := NewHub()
	p := h.Peer()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- p.Subscribe(ctx, func(context.Context, []byte) error { return nil })
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Subscribe err = %v; want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Subscribe did not return after cancel")
	}
}

func TestSubscribeReturnsOnHubClose(t *testing.T) {
	h := NewHub()
	p := h.Peer()
	done := make(chan error, 1)
	go func() {
		done <- p.Subscribe(context.Background(), func(context.Context, []byte) error { return nil })
	}()
	h.Close()
	select {
	case err := <-done:
		if !errors.Is(err, transport.ErrClosed) {
			t.Errorf("Subscribe err on close = %v; want transport.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Subscribe did not return after Close")
	}
}

func TestBroadcastErrsAfterClose(t *testing.T) {
	h := NewHub()
	p := h.Peer()
	h.Close()
	err := p.Broadcast(context.Background(), []byte("nope"))
	if !errors.Is(err, ErrHubClosed) {
		t.Errorf("Broadcast after Close err = %v; want ErrHubClosed", err)
	}
}

func TestApplyErrorPropagates(t *testing.T) {
	h := NewHub()
	p := h.Peer()
	want := errors.New("apply boom")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- p.Subscribe(ctx, func(context.Context, []byte) error { return want })
	}()

	if err := p.Broadcast(context.Background(), []byte("x")); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Errorf("Subscribe err = %v; want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Subscribe did not surface apply error")
	}
}

func TestGapFillerReplaysHistory(t *testing.T) {
	h := NewHub()
	a := h.Peer()
	for _, m := range [][]byte{{1}, {2}, {3}} {
		if err := a.Broadcast(context.Background(), m); err != nil {
			t.Fatalf("Broadcast: %v", err)
		}
	}
	if got := h.HistoryLen(); got != 3 {
		t.Errorf("HistoryLen = %d; want 3", got)
	}

	col := newApplyCollect()
	if err := h.GapFiller().Fetch(context.Background(), nil, col.fn); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	got := col.snapshot()
	if len(got) != 3 {
		t.Fatalf("Fetch delivered %d; want 3", len(got))
	}
	for i, want := range [][]byte{{1}, {2}, {3}} {
		if !bytes.Equal(got[i], want) {
			t.Errorf("Fetch[%d] = %v; want %v", i, got[i], want)
		}
	}
}

func TestBroadcastDefensiveCopy(t *testing.T) {
	h := NewHub()
	p := h.Peer()
	col := runSubscribe(t, p)

	buf := []byte{1, 2, 3}
	if err := p.Broadcast(context.Background(), buf); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	col.waitN(t, 1)

	buf[0] = 99
	got := col.snapshot()
	if got[0][0] == 99 {
		t.Error("subscriber observed mutation through the original slice")
	}
}

// TestBroadcastCloseRace exercises the regression where a Close mid-broadcast
// could panic on a closed delivery channel. Without an unsubscribed peer
// holding the buffer full, broadcasters block in the select; Close must
// release them via the done signal, not by closing the data channel.
func TestBroadcastCloseRace(t *testing.T) {
	h := NewHub()
	// Peer that nobody subscribes to — fills its 64-deep deliver chan after
	// 64 broadcasts and then blocks.
	stalled := h.Peer()
	_ = stalled

	// Peer that's actively reading so most broadcasts succeed quickly.
	live := h.Peer()
	_ = runSubscribe(t, live)

	const N = 200
	errs := make(chan error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := live.Broadcast(context.Background(), []byte{byte(i)})
			if err != nil && !errors.Is(err, ErrHubClosed) {
				errs <- err
			}
		}(i)
	}

	// Race: close mid-broadcasts.
	time.Sleep(time.Millisecond)
	h.Close()
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("unexpected Broadcast error: %v", err)
	}
}

func TestBroadcastReturnsOnContextCancelWhenBufferFull(t *testing.T) {
	h := NewHub()
	defer h.Close()
	stalled := h.Peer()
	_ = stalled

	// Saturate stalled peer's deliver buffer.
	for i := 0; i < deliverQueueSize; i++ {
		if err := stalled.Broadcast(context.Background(), []byte{byte(i)}); err != nil {
			t.Fatalf("priming Broadcast %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- stalled.Broadcast(ctx, []byte("blocks-and-cancels"))
	}()

	// Give the broadcast a moment to enter the blocking select.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Broadcast err = %v; want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Broadcast did not return after ctx cancel")
	}
}
