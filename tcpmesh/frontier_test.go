package tcpmesh

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

type staticFrontier map[crdt.Origin]crdt.Seq

func (s staticFrontier) Frontier() map[crdt.Origin]crdt.Seq { return map[crdt.Origin]crdt.Seq(s) }

type staticTips map[crdt.Origin]crdt.Seq

func (s staticTips) DiscoverTips(context.Context) (map[crdt.Origin]crdt.Seq, error) {
	return map[crdt.Origin]crdt.Seq(s), nil
}

// TestFrontier_Roundtrip: a node serves its applied-frontier and a peer reads
// it back verbatim over opFrontier.
func TestFrontier_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	a, err := New(Config{Listen: "unix:" + filepath.Join(dir, "bundle.sock"), NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	want := staticFrontier{7: 5, 9: 12, 42: 1}
	c.SetFrontierSource(want)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := fetchFrontierFromPeer(ctx, a.Addr(), "topic-x", nil, time.Second)
	if err != nil {
		t.Fatalf("fetchFrontierFromPeer: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d origins, want %d", len(got), len(want))
	}
	for o, s := range want {
		if got[o] != s {
			t.Fatalf("origin %d: got seq %d want %d", o, got[o], s)
		}
	}
}

// TestFrontier_NoHandler: a channel with no FrontierSource refuses with
// StatusNoHandler (the same response an un-upgraded peer gives), so the
// client fails over rather than treating it as an empty frontier.
func TestFrontier_NoHandler(t *testing.T) {
	dir := t.TempDir()
	a, err := New(Config{Listen: "unix:" + filepath.Join(dir, "bundle.sock"), NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	if _, err := a.Channel("topic-x"); err != nil {
		t.Fatalf("Channel: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = fetchFrontierFromPeer(ctx, a.Addr(), "topic-x", nil, time.Second)
	var be *BundleError
	if !errors.As(err, &be) || be.Status != StatusNoHandler {
		t.Fatalf("want StatusNoHandler BundleError, got %v", err)
	}
}

// TestPeerFrontierSource_Aggregation: DiscoverTips is the per-origin max across
// peers; AllPeersApplied requires every peer at >= head and is false with no
// peers.
func TestPeerFrontierSource_Aggregation(t *testing.T) {
	p := &PeerFrontierSource{
		perPeer: map[string]frontierObs{
			"peerA": {fr: map[crdt.Origin]crdt.Seq{1: 5, 2: 3}},
			"peerB": {fr: map[crdt.Origin]crdt.Seq{1: 7, 2: 0, 3: 4}},
			// An errored observation is recorded but never
			// constrains aggregation.
			"peerC": {err: errors.New("refused")},
		},
	}
	tips, err := p.DiscoverTips(context.Background())
	// DiscoverTips refreshes first; with a nil channel Refresh is a no-op, so
	// the seeded perPeer stands.
	if err != nil {
		t.Fatalf("DiscoverTips: %v", err)
	}
	for o, want := range map[crdt.Origin]crdt.Seq{1: 7, 2: 3, 3: 4} {
		if tips[o] != want {
			t.Fatalf("tip[%d]=%d want %d", o, tips[o], want)
		}
	}
	cases := []struct {
		o    crdt.Origin
		head crdt.Seq
		want bool
	}{
		{1, 5, true},  // A=5,B=7 both >=5
		{1, 6, false}, // A=5 < 6
		{2, 0, true},  // A=3,B=0 both >=0
		{2, 1, false}, // B=0 < 1
		{3, 4, false}, // A has no entry for 3 (0 < 4)
	}
	for _, tc := range cases {
		if got := p.AllPeersApplied(tc.o, tc.head); got != tc.want {
			t.Fatalf("AllPeersApplied(%d,%d)=%v want %v", tc.o, tc.head, got, tc.want)
		}
	}

	empty := &PeerFrontierSource{perPeer: map[string]frontierObs{}}
	if empty.AllPeersApplied(1, 1) {
		t.Fatalf("AllPeersApplied must be false with no peers")
	}
	// Errored observations alone can't prove safety either.
	onlyErr := &PeerFrontierSource{perPeer: map[string]frontierObs{
		"peerC": {err: errors.New("refused")},
	}}
	if onlyErr.AllPeersApplied(1, 1) {
		t.Fatalf("AllPeersApplied must be false with only errored observations")
	}
}

var _ transport.TipSource = staticTips(nil)

// TestPeerFrontierObservations_States: a connected peer appears as
// unknown before any refresh, error when it refuses the op, and ok
// (with its frontier and an age) once it serves one — never omitted.
func TestPeerFrontierObservations_States(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")
	a, err := New(Config{Listen: aSock, NodeID: 1})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	aCh, err := a.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel A: %v", err)
	}

	b, err := New(Config{Seeds: []string{aSock}, DialRetry: 25 * time.Millisecond, NodeID: 2})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	bCh, err := b.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel B: %v", err)
	}
	if !waitForReady(b, 1, 2*time.Second) {
		t.Fatalf("B never peered")
	}
	waitMembership(t, b, a.NodeID(), "topic-x", true, time.Second)

	pf := bCh.PeerFrontierBuilder()
	one := func() transport.FrontierObservation {
		obs := pf.Observations()
		if len(obs) != 1 {
			t.Fatalf("Observations = %d entries, want 1 (%+v)", len(obs), obs)
		}
		return obs[0]
	}

	// Before any refresh: unknown, present.
	if o := one(); o.State != transport.FrontierUnknown {
		t.Fatalf("pre-refresh state = %q, want unknown", o.State)
	}

	// A has no FrontierSource: refresh errors, and the error is
	// recorded, not omitted.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pf.Refresh(ctx)
	if o := one(); o.State != transport.FrontierError || o.Err == "" {
		t.Fatalf("no-handler observation = %+v, want error state with cause", o)
	}
	if pf.AllPeersApplied(7, 1) {
		t.Fatalf("AllPeersApplied must be false with only errored observations")
	}

	// Serve a frontier: ok with the fetched map.
	aCh.SetFrontierSource(staticFrontier{7: 42})
	pf.Refresh(ctx)
	o := one()
	if o.State != transport.FrontierOK || o.Frontier[7] != 42 {
		t.Fatalf("post-serve observation = %+v, want ok with frontier 7:42", o)
	}
	if !pf.AllPeersApplied(7, 42) || pf.AllPeersApplied(7, 43) {
		t.Fatalf("AllPeersApplied inconsistent with served frontier")
	}
}
