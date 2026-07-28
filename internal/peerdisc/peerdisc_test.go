package peerdisc_test

import (
	"context"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/peerdisc"
)

func newBackend(t *testing.T) objectstore.Bucket {
	t.Helper()
	be, err := objectstore.OpenFS(filepath.Join(t.TempDir(), "objs"))
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	return be
}

// peerSink collects OnPeers callbacks. Discoverer only fires when the
// set changes, so the latest snapshot is the live truth.
type peerSink struct {
	mu       sync.Mutex
	peers    []string
	updates  int
	released chan struct{}
}

func newSink() *peerSink {
	return &peerSink{released: make(chan struct{}, 16)}
}

func (s *peerSink) handler(p []string) {
	s.mu.Lock()
	s.peers = append(s.peers[:0], p...)
	s.updates++
	s.mu.Unlock()
	select {
	case s.released <- struct{}{}:
	default:
	}
}

func (s *peerSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.peers...)
}

func (s *peerSink) updateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updates
}

func TestStart_DiscoversPeers(t *testing.T) {
	be := newBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sinkA, sinkB := newSink(), newSink()
	a, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xAA),
		Listen:   "127.0.0.1:7000",
		Interval: 50 * time.Millisecond,
		OnPeers:  sinkA.handler,
	})
	if err != nil {
		t.Fatalf("Start a: %v", err)
	}
	defer a.Close()

	b, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xBB),
		Listen:   "127.0.0.1:7100",
		Interval: 50 * time.Millisecond,
		OnPeers:  sinkB.handler,
	})
	if err != nil {
		t.Fatalf("Start b: %v", err)
	}
	defer b.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pa := sinkA.snapshot()
		pb := sinkB.snapshot()
		if slices.Equal(pa, []string{"127.0.0.1:7100"}) &&
			slices.Equal(pb, []string{"127.0.0.1:7000"}) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peers not discovered: a=%v b=%v", sinkA.snapshot(), sinkB.snapshot())
}

func TestStart_FiltersStaleHeartbeats(t *testing.T) {
	be := newBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stale peer: writes one heartbeat, never refreshes (24h interval),
	// then we wait long enough that its LastModified looks ancient
	// relative to the fresh peer's TTL window.
	stale, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xCC),
		Listen:   "127.0.0.1:9999",
		Interval: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("Start stale: %v", err)
	}
	stale.Close()

	// stale TTL = 3 * 30ms = 90ms; sleep past it.
	time.Sleep(200 * time.Millisecond)

	sink := newSink()
	fresh, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xDD),
		Listen:   "127.0.0.1:9000",
		Interval: 30 * time.Millisecond,
		OnPeers:  sink.handler,
	})
	if err != nil {
		t.Fatalf("Start fresh: %v", err)
	}
	defer fresh.Close()

	got := sink.snapshot()
	sort.Strings(got)
	if len(got) != 0 {
		t.Fatalf("expected no live peers, got %v", got)
	}
}

func TestStart_OnPeersOnlyFiresOnChange(t *testing.T) {
	be := newBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sink := newSink()
	a, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xEE),
		Listen:   "127.0.0.1:8000",
		Interval: 25 * time.Millisecond,
		OnPeers:  sink.handler,
	})
	if err != nil {
		t.Fatalf("Start a: %v", err)
	}
	defer a.Close()

	// One peer in the bucket → first discovery sees self only,
	// peers=[] never fires (no change from initial empty).
	time.Sleep(150 * time.Millisecond)
	if u := sink.updates; u != 0 {
		t.Fatalf("OnPeers fired %d times for an unchanging empty set; want 0", u)
	}
}

func TestBindings(t *testing.T) {
	be := newBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sinkA, sinkB := newSink(), newSink()
	a, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xAA),
		Listen:   "127.0.0.1:7000",
		Interval: 50 * time.Millisecond,
		OnPeers:  sinkA.handler,
	})
	if err != nil {
		t.Fatalf("Start a: %v", err)
	}
	defer a.Close()

	b, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xBB),
		Listen:   "127.0.0.1:7100",
		Interval: 50 * time.Millisecond,
		OnPeers:  sinkB.handler,
	})
	if err != nil {
		t.Fatalf("Start b: %v", err)
	}
	defer b.Close()

	// Wait for cross-discovery.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ba := a.Bindings()
		bb := b.Bindings()
		if ba["127.0.0.1:7100"] == crdt.Origin(0xBB) &&
			bb["127.0.0.1:7000"] == crdt.Origin(0xAA) &&
			len(ba) == 1 && len(bb) == 1 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("Bindings not populated: a=%v b=%v", a.Bindings(), b.Bindings())
}

func TestStart_OnPeersFiresOnBindingChangeAtUnchangedAddr(t *testing.T) {
	// Origin rotation at the same listen address (the unclean-restart
	// scenario) must trigger OnPeers so the daemon can refresh
	// Node.SetOriginAddrs and prune the stale origin.
	be := newBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sink := newSink()
	a, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xAA),
		Listen:   "127.0.0.1:9000",
		Interval: 25 * time.Millisecond,
		OnPeers:  sink.handler,
	})
	if err != nil {
		t.Fatalf("Start a: %v", err)
	}
	defer a.Close()

	b1, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xB1),
		Listen:   "127.0.0.1:9100",
		Interval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Start b1: %v", err)
	}
	// Wait for a to see b1.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bindings := a.Bindings(); bindings["127.0.0.1:9100"] == crdt.Origin(0xB1) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if a.Bindings()["127.0.0.1:9100"] != crdt.Origin(0xB1) {
		t.Fatalf("setup: a never saw b1; bindings=%v", a.Bindings())
	}
	updatesAfterB1 := sink.updateCount()
	_ = b1.Close()

	// Replace b1 with a new origin at the same address — simulate an
	// unclean-restart rotation.
	b2, err := peerdisc.Start(ctx, peerdisc.Config{
		Backend:  be,
		Origin:   crdt.Origin(0xB2),
		Listen:   "127.0.0.1:9100",
		Interval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Start b2: %v", err)
	}
	defer b2.Close()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a.Bindings()["127.0.0.1:9100"] == crdt.Origin(0xB2) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := a.Bindings()["127.0.0.1:9100"]; got != crdt.Origin(0xB2) {
		t.Fatalf("a did not pick up b2 binding; got=%v", got)
	}
	if got := sink.updateCount(); got <= updatesAfterB1 {
		t.Errorf("OnPeers did not fire after origin rotation at unchanged addr; updates=%d (was %d)",
			got, updatesAfterB1)
	}
}

func TestStart_RequiresBackendAndOrigin(t *testing.T) {
	if _, err := peerdisc.Start(context.Background(), peerdisc.Config{Origin: 1}); err == nil {
		t.Fatal("expected error for nil Backend")
	}
	be := newBackend(t)
	if _, err := peerdisc.Start(context.Background(), peerdisc.Config{Backend: be}); err == nil {
		t.Fatal("expected error for zero Origin")
	}
}
