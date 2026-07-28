package tcpmesh

import (
	"strings"
	"testing"
)

// TestEndpoint_AdvertiseOverride: when Advertise is set (the listener
// binds a wildcard behind 1:1 NAT), Endpoint must publish the
// advertised address — publishing the bound wildcard would make every
// follower dial its own loopback.
func TestEndpoint_AdvertiseOverride(t *testing.T) {
	a, err := New(Config{
		Listen:    "[::]:0",
		Advertise: "203.0.113.7:7000",
		NodeID:    1,
		// Test-only: binds a wildcard with no TLS; never dialed.
		Insecure: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if got, want := c.Endpoint(), "tcp://203.0.113.7:7000?topic=topic-x"; got != want {
		t.Fatalf("Endpoint() = %q; want advertised %q", got, want)
	}
}

// TestEndpoint_NoAdvertiseUsesListener: with no Advertise the published
// endpoint is the concrete bound address (with the OS-assigned port
// resolved, never ":0").
func TestEndpoint_NoAdvertiseUsesListener(t *testing.T) {
	a, err := New(Config{Listen: "127.0.0.1:0", NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	got := c.Endpoint()
	if !strings.HasPrefix(got, "tcp://127.0.0.1:") || strings.Contains(got, ":0?") {
		t.Fatalf("Endpoint() = %q; want concrete bound 127.0.0.1 address with resolved port", got)
	}
	if !strings.HasSuffix(got, "?topic=topic-x") {
		t.Fatalf("Endpoint() = %q; want ?topic=topic-x suffix", got)
	}
}
