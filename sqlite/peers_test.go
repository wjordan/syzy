package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/transport"
)

// fakeTransport implements transport.Transport + transport.PeerStatter
// for unit-testing Node.Peers / PeerFor / AddrFor without the cost of
// a full Open/Close cycle.
type fakeTransport struct {
	stats []transport.PeerStat
}

func (f *fakeTransport) Broadcast(_ context.Context, _ []byte) error { return nil }
func (f *fakeTransport) Subscribe(_ context.Context, _ transport.ApplyFunc) error {
	return nil
}
func (f *fakeTransport) PeerStats() []transport.PeerStat { return f.stats }

func TestPeersSortOrder(t *testing.T) {
	t.Parallel()
	ft := &fakeTransport{stats: []transport.PeerStat{
		{Addr: "10.0.0.5:7000", RTT: 5 * time.Millisecond},
		{Addr: "10.0.0.3:7000", RTT: 0}, // unavailable
		{Addr: "10.0.0.1:7000", RTT: 2 * time.Millisecond},
		{Addr: "10.0.0.4:7000", RTT: 0}, // unavailable
		{Addr: "10.0.0.2:7000", RTT: 2 * time.Millisecond},
	}}
	n := &Node{transport: ft}
	got := n.Peers()
	if len(got) != 5 {
		t.Fatalf("Peers len = %d, want 5", len(got))
	}
	wantAddrs := []string{
		"10.0.0.1:7000", // 2ms, tie-broken by addr
		"10.0.0.2:7000", // 2ms
		"10.0.0.5:7000", // 5ms
		"10.0.0.3:7000", // 0 (unavailable), tie-broken by addr
		"10.0.0.4:7000", // 0
	}
	for i, want := range wantAddrs {
		if got[i].Addr != want {
			t.Errorf("Peers[%d].Addr = %s, want %s", i, got[i].Addr, want)
		}
	}
}

func TestPeerFor(t *testing.T) {
	t.Parallel()
	ft := &fakeTransport{stats: []transport.PeerStat{
		{Addr: "10.0.0.1:7000", RTT: 2 * time.Millisecond},
	}}
	n := &Node{transport: ft}
	if _, ok := n.PeerFor("10.0.0.9:7000"); ok {
		t.Errorf("PeerFor(unknown) ok=true, want false")
	}
	if _, ok := n.PeerFor(""); ok {
		t.Errorf("PeerFor(empty) ok=true, want false")
	}
	p, ok := n.PeerFor("10.0.0.1:7000")
	if !ok {
		t.Fatalf("PeerFor known addr ok=false, want true")
	}
	if p.RTT != 2*time.Millisecond {
		t.Errorf("PeerFor.RTT = %v, want 2ms", p.RTT)
	}
}

func TestAddrForAndSetOriginAddrs(t *testing.T) {
	t.Parallel()
	n := &Node{}
	if addr, ok := n.AddrFor(42); ok || addr != "" {
		t.Errorf("AddrFor on empty registry = (%q, %v), want (\"\", false)", addr, ok)
	}
	if _, ok := n.AddrFor(0); ok {
		t.Errorf("AddrFor(0) ok=true, want false (zero origin guard)")
	}

	n.SetOriginAddrs(map[uint64]string{
		1: "10.0.0.1:7000",
		2: "10.0.0.2:7000",
		0: "should-be-dropped",
		3: "",
	})
	addr, ok := n.AddrFor(1)
	if !ok || addr != "10.0.0.1:7000" {
		t.Errorf("AddrFor(1) = (%q, %v), want (10.0.0.1:7000, true)", addr, ok)
	}
	if _, ok := n.AddrFor(3); ok {
		t.Errorf("origin 3 with empty addr should not have been registered")
	}
	if _, ok := n.AddrFor(0); ok {
		t.Errorf("origin 0 should not have been registered")
	}

	// Bulk replace prunes origins absent from the new snapshot —
	// this is what makes origin rotation safe.
	n.SetOriginAddrs(map[uint64]string{
		2: "10.0.0.2:7000",
	})
	if _, ok := n.AddrFor(1); ok {
		t.Errorf("after SetOriginAddrs without origin 1, AddrFor(1) ok=true; want false (stale entry pruned)")
	}
	if addr, _ := n.AddrFor(2); addr != "10.0.0.2:7000" {
		t.Errorf("AddrFor(2) = %q after replace; want preserved", addr)
	}

	// Nil clears the registry.
	n.SetOriginAddrs(nil)
	if _, ok := n.AddrFor(2); ok {
		t.Errorf("after SetOriginAddrs(nil), AddrFor(2) ok=true; want false")
	}
}

func TestPeersSingleNodeReturnsNil(t *testing.T) {
	t.Parallel()
	n := &Node{transport: nil}
	if got := n.Peers(); got != nil {
		t.Errorf("Peers() = %v, want nil for single-node Node", got)
	}
}

func TestBridgeOriginToLocality(t *testing.T) {
	t.Parallel()
	// End-to-end shape: consumer observes a Change.Origin, looks up
	// its address, and asks the locality API. AddrFor returning a
	// known addr does not guarantee PeerFor succeeds — the peer
	// might not currently be connected. The two-step lets callers
	// distinguish "origin unknown" from "origin known but offline".
	ft := &fakeTransport{stats: []transport.PeerStat{
		{Addr: "10.0.0.1:7000", RTT: 2 * time.Millisecond},
	}}
	n := &Node{transport: ft}
	n.SetOriginAddrs(map[uint64]string{
		1: "10.0.0.1:7000",
		2: "10.0.0.2:7000", // known origin, no live connection
	})

	addr, ok := n.AddrFor(1)
	if !ok {
		t.Fatalf("AddrFor(1) ok=false")
	}
	if _, ok := n.PeerFor(addr); !ok {
		t.Errorf("PeerFor(%q) ok=false; want live peer", addr)
	}

	addr2, ok := n.AddrFor(2)
	if !ok {
		t.Fatalf("AddrFor(2) ok=false")
	}
	if _, ok := n.PeerFor(addr2); ok {
		t.Errorf("PeerFor(%q) ok=true; want false (no live connection)", addr2)
	}
}
