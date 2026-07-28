package tcpmesh

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/syzy/transport"
)

// twoMuxes returns a pair of muxes on Unix sockets. A.NodeID=1,
// B.NodeID=2; each seeds the other. The helper waits until both
// sides have one ready peer, so callers can immediately Broadcast
// or Subscribe without re-waiting.
func twoMuxes(t *testing.T) (a, b *Mesh) {
	t.Helper()
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")
	bSock := "unix:" + filepath.Join(dir, "b.sock")

	a, err := New(Config{
		Listen:    aSock,
		Seeds:     []string{bSock},
		DialRetry: 25 * time.Millisecond,
		NodeID:    1,
	})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err = New(Config{
		Listen:    bSock,
		Seeds:     []string{aSock},
		DialRetry: 25 * time.Millisecond,
		NodeID:    2,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if !waitForReady(a, 1, 2*time.Second) || !waitForReady(b, 1, 2*time.Second) {
		t.Fatalf("peers did not connect: a=%d b=%d", peerCount(a), peerCount(b))
	}
	return a, b
}

func peerCount(t *Mesh) int {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	return len(t.peersByID)
}

func waitForReady(t *Mesh, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if peerCount(t) >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func waitMembership(t *testing.T, tr *Mesh, remoteID uint64, topic string, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tr.peersMu.Lock()
		p, ok := tr.peersByID[remoteID]
		tr.peersMu.Unlock()
		if ok && p.interestedIn(topic) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	tr.peersMu.Lock()
	p, ok := tr.peersByID[remoteID]
	tr.peersMu.Unlock()
	got := ok && p.interestedIn(topic)
	t.Fatalf("membership for remoteID=%d topic=%q: got=%v want=%v (ready=%v)", remoteID, topic, got, want, ok)
}

// TestLoopback_BasicConnect: both sides settle on exactly one
// ready peer despite symmetric seeding (proves collision tie-break
// converges).
func TestLoopback_BasicConnect(t *testing.T) {
	a, b := twoMuxes(t)
	// Give the dial loops a moment to discover and drop the
	// collision-losing connections.
	time.Sleep(100 * time.Millisecond)
	if got := peerCount(a); got != 1 {
		t.Errorf("A peer count = %d, want 1", got)
	}
	if got := peerCount(b); got != 1 {
		t.Errorf("B peer count = %d, want 1", got)
	}
	// A.NodeID=1 < B.NodeID=2 → the A→B (A outbound) connection
	// survives; the B→A connection loses on both ends.
	a.peersMu.Lock()
	for _, p := range a.peersByID {
		if !p.outbound {
			t.Errorf("A peer should be outbound (winning side), got inbound")
		}
		if p.nodeID != b.NodeID() {
			t.Errorf("A peer NodeID = %d, want %d", p.nodeID, b.NodeID())
		}
	}
	a.peersMu.Unlock()
	b.peersMu.Lock()
	for _, p := range b.peersByID {
		if p.outbound {
			t.Errorf("B peer should be inbound (winning side), got outbound")
		}
		if p.nodeID != a.NodeID() {
			t.Errorf("B peer NodeID = %d, want %d", p.nodeID, a.NodeID())
		}
	}
	b.peersMu.Unlock()
}

// TestLoopback_CrossTopicIsolation: Broadcast on topic-1 reaches
// only topic-1 subscribers; topic-2 subscribers receive nothing.
func TestLoopback_CrossTopicIsolation(t *testing.T) {
	a, b := twoMuxes(t)
	aT1, err := a.Channel("topic-1")
	if err != nil {
		t.Fatalf("a.Channel topic-1: %v", err)
	}
	aT2, err := a.Channel("topic-2")
	if err != nil {
		t.Fatalf("a.Channel topic-2: %v", err)
	}
	bT1, err := b.Channel("topic-1")
	if err != nil {
		t.Fatalf("b.Channel topic-1: %v", err)
	}
	bT2, err := b.Channel("topic-2")
	if err != nil {
		t.Fatalf("b.Channel topic-2: %v", err)
	}

	// Wait for TOPIC_ADD propagation in both directions.
	waitMembership(t, a, b.NodeID(), "topic-1", true, 1*time.Second)
	waitMembership(t, a, b.NodeID(), "topic-2", true, 1*time.Second)
	waitMembership(t, b, a.NodeID(), "topic-1", true, 1*time.Second)
	waitMembership(t, b, a.NodeID(), "topic-2", true, 1*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	gotT1 := make(chan []byte, 4)
	gotT2 := make(chan []byte, 4)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = bT1.Subscribe(ctx, func(_ context.Context, p []byte) error {
			gotT1 <- append([]byte(nil), p...)
			return nil
		})
	}()
	go func() {
		defer wg.Done()
		_ = bT2.Subscribe(ctx, func(_ context.Context, p []byte) error {
			gotT2 <- append([]byte(nil), p...)
			return nil
		})
	}()

	// A broadcasts on topic-1 only.
	if err := aT1.Broadcast(ctx, []byte("on-1")); err != nil {
		t.Fatalf("Broadcast t1: %v", err)
	}
	// Also broadcast on topic-2 to confirm topic-1 subscriber
	// doesn't receive it.
	if err := aT2.Broadcast(ctx, []byte("on-2")); err != nil {
		t.Fatalf("Broadcast t2: %v", err)
	}

	select {
	case got := <-gotT1:
		if string(got) != "on-1" {
			t.Errorf("topic-1 subscriber got %q, want %q", got, "on-1")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("topic-1 subscriber did not receive broadcast")
	}
	select {
	case got := <-gotT2:
		if string(got) != "on-2" {
			t.Errorf("topic-2 subscriber got %q, want %q", got, "on-2")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("topic-2 subscriber did not receive broadcast")
	}
	// Confirm no extra delivery to either subscriber within a short
	// window (cross-topic frames would arrive here if isolation
	// were broken).
	select {
	case payload := <-gotT1:
		t.Errorf("topic-1 subscriber unexpectedly received %q", payload)
	case payload := <-gotT2:
		t.Errorf("topic-2 subscriber unexpectedly received %q", payload)
	case <-time.After(100 * time.Millisecond):
		// good — isolation holds
	}
}

// TestLoopback_NodeIDCollisionEqual: both sides close on equal
// NodeIDs; neither admits a peer.
func TestLoopback_NodeIDCollisionEqual(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")
	bSock := "unix:" + filepath.Join(dir, "b.sock")

	a, err := New(Config{
		Listen:    aSock,
		Seeds:     []string{bSock},
		DialRetry: 25 * time.Millisecond,
		NodeID:    7,
	})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err := New(Config{
		Listen:    bSock,
		Seeds:     []string{aSock},
		DialRetry: 25 * time.Millisecond,
		NodeID:    7,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	// Give multiple dial cycles a chance to settle.
	time.Sleep(200 * time.Millisecond)
	if got := peerCount(a); got != 0 {
		t.Errorf("A peer count = %d, want 0 (NodeID collision should refuse)", got)
	}
	if got := peerCount(b); got != 0 {
		t.Errorf("B peer count = %d, want 0", got)
	}
}

// TestLoopback_UnknownTopicCounter: a fake peer sends a DATA
// frame for a topic the receiver hasn't opened locally. Receiver
// must count + log, not silently drop.
func TestLoopback_UnknownTopicCounter(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")

	// A.NodeID=9999 > fake.NodeID=1, so A keeps inbound and wins
	// the (sole) connection.
	a, err := New(Config{
		Listen: aSock,
		NodeID: 9999,
	})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	if _, err := a.Channel("real-topic"); err != nil {
		t.Fatalf("Channel: %v", err)
	}

	conn, err := net.Dial("unix", filepath.Join(dir, "a.sock"))
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	// Send our hello first (fake NodeID=1, no topics).
	if err := writeHello(conn, Hello{NodeID: 1, Topics: nil}); err != nil {
		t.Fatalf("writeHello: %v", err)
	}
	// Read A's hello.
	got, err := readHello(conn)
	if err != nil {
		t.Fatalf("readHello: %v", err)
	}
	if got.NodeID != 9999 {
		t.Fatalf("A advertised NodeID = %d, want 9999", got.NodeID)
	}

	// Send a DATA frame for a topic A hasn't opened.
	if err := writeFrame(conn, msgData, "topic-doesnt-exist", []byte("ignored")); err != nil {
		t.Fatalf("writeFrame DATA: %v", err)
	}

	// Wait for the counter to increment.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a.Stats().UnknownTopicFrames >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("UnknownTopicFrames did not increment: %+v", a.Stats())
}

// TestLoopback_HelloDeadline: a fake peer dials, never sends
// hello, and gets dropped within the configured HelloDeadline.
func TestLoopback_HelloDeadline(t *testing.T) {
	dir := t.TempDir()
	aSock := filepath.Join(dir, "a.sock")
	const testDeadline = 100 * time.Millisecond
	a, err := New(Config{
		Listen:        "unix:" + aSock,
		NodeID:        99,
		HelloDeadline: testDeadline,
	})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	conn, err := net.Dial("unix", aSock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// A's listener dispatch waits for our first byte before saying
	// anything (it can't know gossip from one-shot until then), so a
	// silent client sees no hello — just a deadline-driven close.
	_ = conn.SetReadDeadline(time.Now().Add(testDeadline + time.Second))
	start := time.Now()
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected EOF after A hello timeout, read returned %d bytes", n)
	}
	if elapsed > testDeadline+500*time.Millisecond {
		t.Errorf("conn closed after %v, want ≤ %v", elapsed, testDeadline+500*time.Millisecond)
	}

	if got := peerCount(a); got != 0 {
		t.Errorf("A peer count after hello timeout = %d, want 0", got)
	}
}

// TestLoopback_TopicAddAfterReady: Channel called after both
// sides are ready emits TOPIC_ADD; remote membership reflects it.
func TestLoopback_TopicAddAfterReady(t *testing.T) {
	a, b := twoMuxes(t)
	// No topics open yet → membership empty.
	a.peersMu.Lock()
	for _, p := range a.peersByID {
		if m := p.membership.Load(); m != nil && len(*m) != 0 {
			t.Errorf("expected empty membership pre-Channel, got %v", *m)
		}
	}
	a.peersMu.Unlock()

	if _, err := a.Channel("late-topic"); err != nil {
		t.Fatalf("Channel: %v", err)
	}
	waitMembership(t, b, a.NodeID(), "late-topic", true, 1*time.Second)
}

// TestLoopback_TopicRemoveOnClose: Channel.Close emits
// TOPIC_REMOVE; remote membership clears.
func TestLoopback_TopicRemoveOnClose(t *testing.T) {
	a, b := twoMuxes(t)
	aT, err := a.Channel("ephemeral")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	waitMembership(t, b, a.NodeID(), "ephemeral", true, 1*time.Second)

	if err := aT.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitMembership(t, b, a.NodeID(), "ephemeral", false, 1*time.Second)
}

// TestChannelCloseReopen: after Channel.Close, Channel with the
// same topic returns a fresh working channel — TOPIC_ADD is
// re-advertised and delivery resumes in BOTH directions. Guards the
// one-way-partition regression where a reopened topic could publish
// but never receive. Closed-handle methods return transport.ErrClosed.
func TestChannelCloseReopen(t *testing.T) {
	a, b := twoMuxes(t)
	aT, err := a.Channel("app-x")
	if err != nil {
		t.Fatalf("Channel a: %v", err)
	}
	bT, err := b.Channel("app-x")
	if err != nil {
		t.Fatalf("Channel b: %v", err)
	}
	waitMembership(t, b, a.NodeID(), "app-x", true, time.Second)
	waitMembership(t, a, b.NodeID(), "app-x", true, time.Second)

	recv := func(c *Channel) (<-chan []byte, context.CancelFunc) {
		out := make(chan []byte, 16)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			_ = c.Subscribe(ctx, func(_ context.Context, p []byte) error {
				out <- append([]byte(nil), p...)
				return nil
			})
		}()
		return out, cancel
	}
	expect := func(ch <-chan []byte, want string) {
		t.Helper()
		select {
		case got := <-ch:
			if string(got) != want {
				t.Fatalf("received %q, want %q", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %q", want)
		}
	}

	bRecv, bCancel := recv(bT)
	defer bCancel()
	if err := aT.Broadcast(context.Background(), []byte("one")); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	expect(bRecv, "one")

	if err := aT.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitMembership(t, b, a.NodeID(), "app-x", false, time.Second)
	if err := aT.Broadcast(context.Background(), []byte("zombie")); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Broadcast on closed channel err = %v, want transport.ErrClosed", err)
	}
	if err := aT.Subscribe(context.Background(), nil); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Subscribe on closed channel err = %v, want transport.ErrClosed", err)
	}

	aT2, err := a.Channel("app-x")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if aT2 == aT {
		t.Fatalf("reopen returned the closed channel instance")
	}
	waitMembership(t, b, a.NodeID(), "app-x", true, time.Second)

	// Outbound from the reopened channel.
	if err := aT2.Broadcast(context.Background(), []byte("two")); err != nil {
		t.Fatalf("Broadcast after reopen: %v", err)
	}
	expect(bRecv, "two")

	// Inbound to the reopened channel — the direction the stale-cache
	// bug silently severed.
	aRecv, aCancel := recv(aT2)
	defer aCancel()
	if err := bT.Broadcast(context.Background(), []byte("three")); err != nil {
		t.Fatalf("Broadcast b→a: %v", err)
	}
	expect(aRecv, "three")
}

// TestLoopback_SubscribeStopsOnClose: Subscribe returns
// transport.ErrClosed when Channel.Close is called.
func TestLoopback_SubscribeStopsOnClose(t *testing.T) {
	dir := t.TempDir()
	a, err := New(Config{Listen: "unix:" + filepath.Join(dir, "a.sock"), NodeID: 5})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("t")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- c.Subscribe(context.Background(), func(_ context.Context, _ []byte) error { return nil })
	}()
	time.Sleep(25 * time.Millisecond)
	_ = c.Close()
	select {
	case err := <-done:
		if !errors.Is(err, transport.ErrClosed) {
			t.Errorf("Subscribe err = %v, want transport.ErrClosed on Close", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Subscribe did not return after Close")
	}
}

// TestLoopback_SubscribeStopsOnTransportClose: Subscribe returns
// when Transport.Close is called (not just channel.Close).
func TestLoopback_SubscribeStopsOnTransportClose(t *testing.T) {
	dir := t.TempDir()
	a, err := New(Config{Listen: "unix:" + filepath.Join(dir, "a.sock"), NodeID: 5})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c, err := a.Channel("t")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- c.Subscribe(context.Background(), func(_ context.Context, _ []byte) error { return nil })
	}()
	time.Sleep(25 * time.Millisecond)
	_ = a.Close()
	select {
	case err := <-done:
		if !errors.Is(err, transport.ErrClosed) {
			t.Errorf("Subscribe err = %v, want transport.ErrClosed on Transport.Close", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Subscribe did not return after Transport.Close")
	}
	if _, err := a.Channel("t2"); !errors.Is(err, transport.ErrClosed) {
		t.Errorf("Channel on closed mesh err = %v, want transport.ErrClosed", err)
	}
}

// TestLoopback_SubscribeHonorsContext: Subscribe returns
// context.Canceled when ctx is cancelled.
func TestLoopback_SubscribeHonorsContext(t *testing.T) {
	dir := t.TempDir()
	a, err := New(Config{Listen: "unix:" + filepath.Join(dir, "a.sock"), NodeID: 5})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("t")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Subscribe(ctx, func(_ context.Context, _ []byte) error { return nil })
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Subscribe err = %v, want Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Subscribe did not return after ctx cancel")
	}
}

// TestLoopback_BroadcastRejectsOversize: payload > MaxFrameSize
// errors at Channel.Broadcast.
func TestLoopback_BroadcastRejectsOversize(t *testing.T) {
	dir := t.TempDir()
	a, err := New(Config{Listen: "unix:" + filepath.Join(dir, "a.sock"), NodeID: 5})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("t")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	huge := make([]byte, MaxFrameSize+1)
	if err := c.Broadcast(context.Background(), huge); err == nil {
		t.Fatalf("Broadcast(oversized) err = nil, want refusal")
	}
}

// TestLoopback_OpenChannelDuringHandshake: opening many channels
// concurrently with peer connect — every topic ends up known at
// the remote, regardless of when the open happened relative to
// hello-build. The setup goroutine's missed-topic re-snapshot
// guarantees this without locking out Channel during the
// hello exchange.
func TestLoopback_OpenChannelDuringHandshake(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")
	bSock := "unix:" + filepath.Join(dir, "b.sock")

	a, err := New(Config{Listen: aSock, NodeID: 1, DialRetry: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := New(Config{Listen: bSock, NodeID: 2, DialRetry: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	const N = 50
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		a.SetSeeds([]string{bSock})
		b.SetSeeds([]string{aSock})
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			if _, err := a.Channel(fmt.Sprintf("topic-%d", i)); err != nil {
				t.Errorf("Channel topic-%d: %v", i, err)
				return
			}
		}
	}()
	wg.Wait()

	if !waitForReady(a, 1, 2*time.Second) || !waitForReady(b, 1, 2*time.Second) {
		t.Fatalf("peers did not connect")
	}
	for i := 0; i < N; i++ {
		waitMembership(t, b, a.NodeID(), fmt.Sprintf("topic-%d", i), true, 2*time.Second)
	}
}

// TestLoopback_ChannelIdempotent: repeated Channel(topic)
// returns the same instance. Concurrent calls collapse to one
// instance and a single TOPIC_ADD per peer.
func TestLoopback_ChannelIdempotent(t *testing.T) {
	a, b := twoMuxes(t)

	const N = 16
	var wg sync.WaitGroup
	results := make([]*Channel, N)
	var openErrs atomic.Int32
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := a.Channel("concurrent-topic")
			if err != nil {
				openErrs.Add(1)
				return
			}
			results[i] = c
		}()
	}
	wg.Wait()

	if openErrs.Load() != 0 {
		t.Fatalf("Channel errored on %d goroutines", openErrs.Load())
	}
	first := results[0]
	for i, c := range results {
		if c != first {
			t.Errorf("Channel call %d returned a different instance", i)
		}
	}
	// Remote sees the topic exactly once. We can't directly inspect
	// "how many TOPIC_ADDs arrived" without instrumentation, but
	// membership reflecting the topic confirms it propagated.
	waitMembership(t, b, a.NodeID(), "concurrent-topic", true, 1*time.Second)
}

// TestListenerInjection: a caller-owned pre-bound listener
// (Config.Listener) serves gossip and one-shot ops exactly like a
// mesh-bound one, and Close closes it.
func TestListenerInjection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	a, err := New(Config{Listener: ln, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c, err := a.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	c.SetBundleHandler(func(w io.Writer) error {
		_, err := w.Write([]byte("bundle-bytes"))
		return err
	})

	b, err := New(Config{
		Seeds:     []string{a.Addr()},
		DialRetry: 25 * time.Millisecond,
		NodeID:    2,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if !waitForReady(b, 1, 2*time.Second) {
		t.Fatalf("B never peered with injected-listener mesh")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var got bytes.Buffer
	if err := FetchBundle(ctx, c.Endpoint(), &got); err != nil {
		t.Fatalf("FetchBundle over injected listener: %v", err)
	}
	if got.String() != "bundle-bytes" {
		t.Fatalf("bundle = %q", got.String())
	}

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ln.Accept(); err == nil {
		t.Fatal("injected listener still accepting after mesh Close")
	}

	// Both set is a config error.
	if _, err := New(Config{Listen: "127.0.0.1:0", Listener: ln, NodeID: 3}); err == nil {
		t.Fatal("New with both Listen and Listener should error")
	}
}
