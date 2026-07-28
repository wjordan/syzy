package tcpmesh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// driveSetup runs one endpoint's setupPeerReturning on its own
// goroutine after an optional random stagger, so a round of
// concurrent setups explores different interleavings.
func driveSetup(wg *sync.WaitGroup, tr *Mesh, conn net.Conn, outbound bool, stagger time.Duration) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		if stagger > 0 {
			time.Sleep(stagger)
		}
		if !outbound {
			// Mirror dispatchConn: the accept path consumes the
			// magic preamble before peer setup.
			var m [4]byte
			if _, err := io.ReadFull(conn, m[:]); err != nil {
				_ = conn.Close()
				return
			}
		}
		_, _ = tr.setupPeerReturning(conn, "", outbound)
	}()
}

// convergedPair polls until both transports hold exactly one peer
// for each other AND both entries identify the same conn (equal
// nonce + dialerLow — conn identity is the (dialerLow, nonce) pair
// from the hello). Returns the agreed nonce, or an error describing
// the terminal state.
func convergedPair(a, b *Mesh, timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	var state string
	for time.Now().Before(deadline) {
		a.peersMu.Lock()
		pa := a.peersByID[b.NodeID()]
		na := len(a.peersByID)
		a.peersMu.Unlock()
		b.peersMu.Lock()
		pb := b.peersByID[a.NodeID()]
		nb := len(b.peersByID)
		b.peersMu.Unlock()
		switch {
		case pa == nil || pb == nil || na != 1 || nb != 1:
			state = fmt.Sprintf("a: %d peers (entry=%v), b: %d peers (entry=%v)", na, pa != nil, nb, pb != nil)
		case pa.nonce != pb.nonce || pa.dialerLow != pb.dialerLow:
			state = fmt.Sprintf("crossed attribution: a on (low=%v nonce=%d), b on (low=%v nonce=%d)",
				pa.dialerLow, pa.nonce, pb.dialerLow, pb.nonce)
		default:
			return pa.nonce, nil
		}
		time.Sleep(2 * time.Millisecond)
	}
	return 0, errors.New(state)
}

// assertClosedConn verifies a superseded conn endpoint was actually
// closed (not just dropped from the ready set): a Read must return
// a closed/EOF error, not block until the deadline.
func assertClosedConn(t *testing.T, name string, c net.Conn) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	_ = c.SetReadDeadline(deadline)
	var buf [1]byte
	for time.Now().Before(deadline) {
		_, err := c.Read(buf[:])
		if err == nil {
			continue // drained a stray byte; keep going
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("%s: still open (Read timed out instead of returning closed/EOF)", name)
		}
		return // closed pipe / EOF — properly torn down
	}
	t.Fatalf("%s: still open after deadline", name)
}

// verifyBidirectionalDelivery broadcasts one payload in each
// direction and asserts both arrive.
func verifyBidirectionalDelivery(t *testing.T, aCh, bCh *Channel) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := aCh.Broadcast(ctx, []byte("a->b")); err != nil {
		t.Fatalf("a Broadcast: %v", err)
	}
	if err := bCh.Broadcast(ctx, []byte("b->a")); err != nil {
		t.Fatalf("b Broadcast: %v", err)
	}
	select {
	case got := <-bCh.deliver:
		if string(got) != "a->b" {
			t.Fatalf("b received %q, want %q", got, "a->b")
		}
	case <-ctx.Done():
		t.Fatalf("a->b broadcast never delivered")
	}
	select {
	case got := <-aCh.deliver:
		if string(got) != "b->a" {
			t.Fatalf("b->a broadcast never delivered to a")
		}
	case <-ctx.Done():
		t.Fatalf("b->a broadcast never delivered")
	}
}

// raceRound builds two bare transports with one open topic each and
// two conn pairs, runs the four setup goroutines with random
// staggers, and asserts (1) both sides converge on the SAME conn,
// (2) delivery works both ways over it, (3) the superseded pair is
// closed on both ends. pair2Outbound selects the second pair's
// direction: dialed by B (opposite-direction collision) or by A
// (same-direction duplicate — the case the old NodeID-only
// tie-break could not order, leaving each side attached to a
// different conn).
func raceRound(t *testing.T, rng *rand.Rand, pair2DialedByA bool) {
	t.Helper()
	a, err := New(Config{NodeID: 1})
	if err != nil {
		t.Fatalf("New a: %v", err)
	}
	defer a.Close()
	b, err := New(Config{NodeID: 2})
	if err != nil {
		t.Fatalf("New b: %v", err)
	}
	defer b.Close()
	aCh, err := a.Channel("t")
	if err != nil {
		t.Fatalf("a Channel: %v", err)
	}
	bCh, err := b.Channel("t")
	if err != nil {
		t.Fatalf("b Channel: %v", err)
	}

	// Pair 1 is always dialed by A: c1a is A's outbound end.
	c1a, c1b := net.Pipe()
	// Pair 2 is dialed by B (opposite-direction) or by A again
	// (same-direction duplicate, e.g. a redial racing its
	// predecessor).
	c2x, c2y := net.Pipe()

	var wg sync.WaitGroup
	stagger := func() time.Duration { return time.Duration(rng.Intn(1500)) * time.Microsecond }
	driveSetup(&wg, a, c1a, true, stagger())
	driveSetup(&wg, b, c1b, false, stagger())
	if pair2DialedByA {
		driveSetup(&wg, a, c2x, true, stagger())
		driveSetup(&wg, b, c2y, false, stagger())
	} else {
		driveSetup(&wg, b, c2x, true, stagger())
		driveSetup(&wg, a, c2y, false, stagger())
	}
	wg.Wait()

	winner, err := convergedPair(a, b, time.Second)
	if err != nil {
		t.Fatalf("no convergence (pair2DialedByA=%v): %v", pair2DialedByA, err)
	}
	verifyBidirectionalDelivery(t, aCh, bCh)

	// Identify the losing pair by the winner's conn identity and
	// assert it was really closed on both endpoints — a superseded
	// conn left half-open is exactly the state that wedges
	// delivery in production.
	a.peersMu.Lock()
	winnerConnA := a.peersByID[b.NodeID()].conn
	a.peersMu.Unlock()
	if winnerConnA == c1a {
		assertClosedConn(t, "losing pair end x", c2x)
		assertClosedConn(t, "losing pair end y", c2y)
	} else {
		assertClosedConn(t, "losing pair end a", c1a)
		assertClosedConn(t, "losing pair end b", c1b)
	}
	if !pair2DialedByA {
		// Opposite-direction collision: the winner must be the
		// conn dialed by the lower NodeID (deterministic, both
		// rounds' interleavings included).
		if winnerConnA != c1a {
			t.Fatalf("opposite-direction winner is not the low-node-dialed conn (nonce=%d)", winner)
		}
	}
}

// TestSimultaneousDial_OppositeDirection: A dials B while B dials
// A. For every interleaving of the four setup goroutines, both
// sides must converge on the conn dialed by the lower NodeID,
// deliver bidirectionally over it, and close the other pair.
func TestSimultaneousDial_OppositeDirection(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 25; i++ {
		raceRound(t, rng, false)
	}
}

// TestSimultaneousDial_SameDirection: two conns both dialed by A
// (a redial racing its predecessor). The old tie-break had no
// order between them — each side kept whichever setup it evaluated
// last, so opposite evaluation orders left A attached to one conn
// and B to the other (both ESTABLISHED, delivery silently dead).
// The nonce rank must make both sides pick the same one.
func TestSimultaneousDial_SameDirection(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 25; i++ {
		raceRound(t, rng, true)
	}
}

// fakePeerConn dials tr's unix listener and completes a hello
// exchange advertising the given topics, returning the raw conn.
func fakePeerConn(t *testing.T, sockPath string, nodeID, nonce uint64, topics []string) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if err := writeHello(conn, Hello{NodeID: nodeID, ConnNonce: nonce, Topics: topics}); err != nil {
		t.Fatalf("writeHello: %v", err)
	}
	if _, err := readHello(conn); err != nil {
		t.Fatalf("readHello: %v", err)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn
}

// TestBroadcastWriteTimeout_RetiresBlockedPeer: a peer that stops
// reading must not wedge Broadcast forever — once its buffers fill,
// the write deadline fires and the peer is retired.
func TestBroadcastWriteTimeout_RetiresBlockedPeer(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "a.sock")
	a, err := New(Config{
		Listen:       "unix:" + sock,
		NodeID:       1,
		WriteTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	ch, err := a.Channel("t")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	// Fake peer advertises the topic, then never reads again.
	fakePeerConn(t, sock, 2, connNonce(), []string{"t"})
	if !waitForReady(a, 1, 2*time.Second) {
		t.Fatalf("fake peer not admitted")
	}

	// Broadcast until the socket buffers fill and the deadline
	// retires the peer. Each blocked write costs ≤ WriteTimeout.
	payload := make([]byte, 256<<10)
	ctx := context.Background()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if peerCount(a) == 0 {
			return
		}
		if err := ch.Broadcast(ctx, payload); err != nil {
			t.Fatalf("Broadcast: %v", err)
		}
	}
	t.Fatalf("blocked peer never retired; peers=%d", peerCount(a))
}

// TestPing_RetiresSilentPeer: a peer that reads everything (so
// writes to it never error) but never sends a frame — the shape of
// a remote that holds the socket open without attributing it — is
// retired by the ping sweep after PingTimeout.
func TestPing_RetiresSilentPeer(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "a.sock")
	a, err := New(Config{
		Listen:       "unix:" + sock,
		NodeID:       1,
		PingInterval: 20 * time.Millisecond,
		PingTimeout:  120 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	conn := fakePeerConn(t, sock, 2, connNonce(), nil)
	// Drain inbound frames (PINGs included) without ever replying.
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
	if !waitForReady(a, 1, 2*time.Second) {
		t.Fatalf("fake peer not admitted")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if peerCount(a) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("silent peer never retired; peers=%d", peerCount(a))
}

// TestPing_KeepsResponsivePeer: two real muxes with aggressive ping
// settings stay connected across many timeout windows (PONGs and
// pings refresh liveness in both directions) and still deliver.
func TestPing_KeepsResponsivePeer(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")
	bSock := "unix:" + filepath.Join(dir, "b.sock")
	mk := func(listen, seed string, id uint64) *Mesh {
		tr, err := New(Config{
			Listen:       listen,
			Seeds:        []string{seed},
			DialRetry:    25 * time.Millisecond,
			NodeID:       id,
			PingInterval: 15 * time.Millisecond,
			PingTimeout:  75 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("New %d: %v", id, err)
		}
		t.Cleanup(func() { _ = tr.Close() })
		return tr
	}
	a := mk(aSock, bSock, 1)
	b := mk(bSock, aSock, 2)
	if !waitForReady(a, 1, 2*time.Second) || !waitForReady(b, 1, 2*time.Second) {
		t.Fatalf("peers did not connect")
	}
	aCh, err := a.Channel("t")
	if err != nil {
		t.Fatalf("Channel a: %v", err)
	}
	bCh, err := b.Channel("t")
	if err != nil {
		t.Fatalf("Channel b: %v", err)
	}
	waitMembership(t, a, b.NodeID(), "t", true, time.Second)
	waitMembership(t, b, a.NodeID(), "t", true, time.Second)

	// Idle across several PingTimeout windows; the conn must survive.
	time.Sleep(250 * time.Millisecond)
	if got := peerCount(a); got != 1 {
		t.Fatalf("a peer count after idle = %d, want 1", got)
	}
	if got := peerCount(b); got != 1 {
		t.Fatalf("b peer count after idle = %d, want 1", got)
	}
	verifyBidirectionalDelivery(t, aCh, bCh)
}
