package tcpmesh

import (
	"path/filepath"
	"testing"
	"time"
)

// TestSetSeeds_AddsNewDialer: adding an address via SetSeeds
// spawns a dialer that connects.
func TestSetSeeds_AddsNewDialer(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")

	a, err := New(Config{
		Listen: aSock,
		NodeID: 100, // > B's 200? doesn't matter, B-only-outbound conn
	})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err := New(Config{DialRetry: 25 * time.Millisecond, NodeID: 200})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if waitForReady(a, 1, 200*time.Millisecond) {
		t.Fatalf("a saw a peer before SetSeeds was called")
	}

	b.SetSeeds([]string{a.Addr()})
	if !waitForReady(a, 1, 1*time.Second) {
		t.Fatalf("a never saw a peer after SetSeeds")
	}
}

// TestSetSeeds_RemovesAbsentDialer: dropped seed has its dial
// loop cancelled; no reconnection after the existing conn is
// forcibly closed.
func TestSetSeeds_RemovesAbsentDialer(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")

	a, err := New(Config{Listen: aSock, NodeID: 10})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err := New(Config{
		Seeds:     []string{a.Addr()},
		DialRetry: 25 * time.Millisecond,
		NodeID:    20,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if !waitForReady(a, 1, 1*time.Second) {
		t.Fatalf("a never saw initial peer")
	}

	b.SetSeeds(nil)
	b.seedsMu.Lock()
	_, stillThere := b.activeSeeds[a.Addr()]
	b.seedsMu.Unlock()
	if stillThere {
		t.Fatalf("activeSeeds still contains dropped addr")
	}

	// Forcibly close from A's side. B's readLoop retires the peer;
	// if the dialer were still alive it would redial within
	// DialRetry. With the cancel applied, no reconnect.
	a.peersMu.Lock()
	for _, p := range a.peersByID {
		_ = p.conn.Close()
	}
	a.peersMu.Unlock()

	drained := false
	for i := 0; i < 50; i++ {
		if peerCount(a) == 0 {
			drained = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !drained {
		t.Fatalf("existing peer never drained from a")
	}

	deadline := time.Now().Add(200 * time.Millisecond) // 8× DialRetry
	for time.Now().Before(deadline) {
		if peerCount(a) > 0 {
			t.Fatalf("a saw a reconnection after seed was dropped")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSetSeeds_Idempotent: same SetSeeds is a no-op.
func TestSetSeeds_Idempotent(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")

	a, err := New(Config{Listen: aSock, NodeID: 11})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err := New(Config{
		Seeds:     []string{a.Addr()},
		DialRetry: 25 * time.Millisecond,
		NodeID:    22,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if !waitForReady(a, 1, 1*time.Second) {
		t.Fatalf("a never saw initial peer")
	}

	b.seedsMu.Lock()
	before := b.activeSeeds[a.Addr()]
	b.seedsMu.Unlock()
	if before == nil {
		t.Fatalf("expected initial dialer entry for a.Addr")
	}

	b.SetSeeds([]string{a.Addr()})
	b.seedsMu.Lock()
	after := b.activeSeeds[a.Addr()]
	b.seedsMu.Unlock()
	if before != after {
		t.Fatalf("dialer entry was replaced on idempotent SetSeeds")
	}
}

// TestSetSeeds_AfterCloseIsNoop: SetSeeds after Close is a no-op
// and doesn't panic.
func TestSetSeeds_AfterCloseIsNoop(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")
	a, err := New(Config{Listen: aSock, NodeID: 1})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	addr := a.Addr()
	_ = a.Close()

	b, err := New(Config{DialRetry: 25 * time.Millisecond, NodeID: 2})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	_ = b.Close()

	b.SetSeeds([]string{addr, "127.0.0.1:65535"})
	b.seedsMu.Lock()
	if b.activeSeeds != nil {
		t.Fatalf("activeSeeds is not nil after Close")
	}
	b.seedsMu.Unlock()
}

// TestSetSeeds_ReplacesDialer: swap A→B and connections follow.
func TestSetSeeds_ReplacesDialer(t *testing.T) {
	dir := t.TempDir()
	a1Sock := "unix:" + filepath.Join(dir, "a1.sock")
	a2Sock := "unix:" + filepath.Join(dir, "a2.sock")

	a1, err := New(Config{Listen: a1Sock, NodeID: 11})
	if err != nil {
		t.Fatalf("New a1: %v", err)
	}
	t.Cleanup(func() { _ = a1.Close() })

	a2, err := New(Config{Listen: a2Sock, NodeID: 12})
	if err != nil {
		t.Fatalf("New a2: %v", err)
	}
	t.Cleanup(func() { _ = a2.Close() })

	b, err := New(Config{
		Seeds:     []string{a1.Addr()},
		DialRetry: 25 * time.Millisecond,
		NodeID:    99,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if !waitForReady(a1, 1, 1*time.Second) {
		t.Fatalf("a1 never saw initial peer")
	}

	b.SetSeeds([]string{a2.Addr()})
	a1.peersMu.Lock()
	for _, p := range a1.peersByID {
		_ = p.conn.Close()
	}
	a1.peersMu.Unlock()

	if !waitForReady(a2, 1, 1*time.Second) {
		t.Fatalf("a2 never saw connection after seed swap")
	}
}

// TestSetSeeds_OverlayRefresh: a single SetSeeds carries a mesh-wide
// peer list; both channels on the mesh see the new peer once. This
// is the overlay-refresh shape: the operator periodically updates
// the seed set; topics must not need per-topic seed management.
func TestSetSeeds_OverlayRefresh(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")
	bSock := "unix:" + filepath.Join(dir, "b.sock")

	a, err := New(Config{Listen: aSock, DialRetry: 25 * time.Millisecond, NodeID: 1})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err := New(Config{Listen: bSock, DialRetry: 25 * time.Millisecond, NodeID: 2})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	// Open two channels on each side BEFORE the seed flips, so
	// when the seed appears, TOPIC_ADD propagation covers both
	// without any per-channel intervention.
	if _, err := a.Channel("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Channel("cdn"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Channel("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Channel("cdn"); err != nil {
		t.Fatal(err)
	}

	// Now flip seeds once on each side — single call, mesh-wide.
	a.SetSeeds([]string{b.Addr()})
	b.SetSeeds([]string{a.Addr()})

	if !waitForReady(a, 1, 1*time.Second) || !waitForReady(b, 1, 1*time.Second) {
		t.Fatalf("peers did not connect after mesh-wide SetSeeds")
	}
	waitMembership(t, a, b.NodeID(), "app", true, 1*time.Second)
	waitMembership(t, a, b.NodeID(), "cdn", true, 1*time.Second)
	waitMembership(t, b, a.NodeID(), "app", true, 1*time.Second)
	waitMembership(t, b, a.NodeID(), "cdn", true, 1*time.Second)
}
