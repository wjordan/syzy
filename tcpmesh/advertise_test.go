package tcpmesh

import (
	"path/filepath"
	"testing"
	"time"
)

// TestAdvertise_PeerSeesAdvertisedAddr: a node with Advertise set sends that
// address (not its bind address) in the hello, so the peer it dials reports the
// routable/advertised addr in PeerStats. This mirrors the 1:1-NAT case: the
// NAT'd node dials out (its source addr is opaque to the accepter), so the
// accepter must learn the node's routable addr from the hello to dial it back
// and match it against cluster inventory (otherwise it reads as offline).
func TestAdvertise_PeerSeesAdvertisedAddr(t *testing.T) {
	dir := t.TempDir()
	bSock := "unix:" + filepath.Join(dir, "b.sock")
	const advertised = "203.0.113.7:7847" // where the NAT'd node is reachable
	// b is the accepter (a seed); a is the NAT'd dialer that advertises.
	b, err := New(Config{Listen: bSock, NodeID: 2})
	if err != nil {
		t.Fatalf("New b: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	a, err := New(Config{Advertise: advertised, DialRetry: 25 * time.Millisecond, NodeID: 1})
	if err != nil {
		t.Fatalf("New a: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	a.SetSeeds([]string{bSock}) // a (NAT'd) dials the seed b
	if !waitForReady(b, 1, time.Second) {
		t.Fatal("b never reached ready with a")
	}
	stats := b.PeerStats()
	if len(stats) != 1 {
		t.Fatalf("want 1 peer, got %d", len(stats))
	}
	if stats[0].Addr != advertised {
		t.Fatalf("accepter saw dialer addr %q, want advertised %q", stats[0].Addr, advertised)
	}
}
