package tcpmesh

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/tcpmesh/internal/catchupwire"
	"github.com/wjordan/syzy/transport"
)

// stubCatchupSource is a minimal CatchupSource for transport-level
// catchup tests. Mirror.Manager is the production implementation;
// here we just need to know the wire works.
type stubCatchupSource struct {
	payloads [][]byte
	served   atomic.Uint32
	gotReq   chan transport.CatchupRequest
}

func newStubCatchupSource(payloads ...[]byte) *stubCatchupSource {
	return &stubCatchupSource{
		payloads: payloads,
		gotReq:   make(chan transport.CatchupRequest, 1),
	}
}

func (s *stubCatchupSource) Serve(_ context.Context, req transport.CatchupRequest, write func(payload []byte) error) error {
	select {
	case s.gotReq <- req:
	default:
	}
	maxN := uint32(len(s.payloads))
	if req.MaxRecords > 0 && req.MaxRecords < maxN {
		maxN = req.MaxRecords
	}
	var bytesSent uint64
	for i := uint32(0); i < maxN; i++ {
		p := s.payloads[i]
		if req.MaxBytes > 0 && bytesSent+uint64(len(p)) > req.MaxBytes && i > 0 {
			return nil
		}
		if err := write(p); err != nil {
			return err
		}
		bytesSent += uint64(len(p))
		s.served.Add(1)
	}
	return nil
}

// wireHeader builds a Changeset wire-prefix slice (origin + seq)
// just so transport-layer round-trip tests have a recognizable
// payload shape.
func wireHeader(origin crdt.Origin, seq crdt.Seq) []byte {
	b := make([]byte, 17)
	b[0] = 1 // version
	binary.BigEndian.PutUint64(b[1:9], uint64(origin))
	binary.BigEndian.PutUint64(b[9:17], uint64(seq))
	return b
}

func TestCatchup_RoundtripsRequestedRanges(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	src := newStubCatchupSource(wireHeader(7, 5), wireHeader(7, 6))
	c.SetCatchupSource(src)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var got [][]byte
	apply := transport.ApplyFunc(func(_ context.Context, p []byte) error {
		got = append(got, append([]byte(nil), p...))
		return nil
	})
	ranges := []transport.Range{{Origin: 7, Lo: 5, Hi: 6}}
	if err := fetchCatchupFromPeer(ctx, a.Addr(), "topic-x", nil, time.Second, ranges, apply); err != nil {
		t.Fatalf("fetchCatchupFromPeer: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d payloads, want 2", len(got))
	}
	for i, want := range src.payloads {
		if string(got[i]) != string(want) {
			t.Fatalf("payload %d mismatch: got %x want %x", i, got[i], want)
		}
	}
	select {
	case req := <-src.gotReq:
		if len(req.Ranges) != 1 || req.Ranges[0].Origin != 7 || req.Ranges[0].Lo != 5 || req.Ranges[0].Hi != 6 {
			t.Fatalf("server saw req=%+v", req)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("source never saw request")
	}
}

func TestCatchup_UnknownTopicStatus(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	// No Channel — topic unknown.

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	apply := transport.ApplyFunc(func(context.Context, []byte) error {
		t.Errorf("apply called against unknown topic")
		return nil
	})
	err = fetchCatchupFromPeer(ctx, a.Addr(), "missing", nil, time.Second,
		[]transport.Range{{Origin: 1, Lo: 1, Hi: 1}}, apply)
	if err == nil {
		t.Fatalf("expected BundleError for unknown topic")
	}
	var be *BundleError
	if !errors.As(err, &be) || be.Status != StatusUnknownTopic {
		t.Fatalf("err = %v, want BundleError{UnknownTopic}", err)
	}
}

func TestCatchup_NoSourceStatus(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	_ = c // no SetCatchupSource

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = fetchCatchupFromPeer(ctx, a.Addr(), "topic-x", nil, time.Second,
		[]transport.Range{{Origin: 1, Lo: 1, Hi: 1}}, func(context.Context, []byte) error {
			t.Errorf("apply called against unregistered source")
			return nil
		})
	if err == nil {
		t.Fatalf("expected BundleError for no source")
	}
	var be *BundleError
	if !errors.As(err, &be) || be.Status != StatusNoHandler {
		t.Fatalf("err = %v, want BundleError{NoHandler}", err)
	}
}

func TestCatchup_MaxRecordsCap(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	src := newStubCatchupSource(wireHeader(7, 1), wireHeader(7, 2), wireHeader(7, 3))
	c.SetCatchupSource(src)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := dialContext(ctx, a.Addr(), nil, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := writeRequestPrefix(conn, opCatchupRequest, "topic-x"); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	if err := catchupwire.Write(conn, transport.CatchupRequest{
		Ranges:     []transport.Range{{Origin: 7, Lo: 1, Hi: 3}},
		MaxRecords: 2,
	}); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := readStatus(conn); err != nil {
		t.Fatalf("read status: %v", err)
	}
	got := 0
	hdr := make([]byte, 4)
	for {
		if _, err := io.ReadFull(conn, hdr); err != nil {
			break
		}
		n := binary.BigEndian.Uint32(hdr)
		if n == 0 {
			continue
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(conn, buf); err != nil {
			break
		}
		got++
	}
	if got != 2 {
		t.Fatalf("MaxRecords=2 applied %d, want 2", got)
	}
}

func TestPeerGapFiller_FailsWhenNoPeers(t *testing.T) {
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
	filler := &PeerGapFiller{Channel: c}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = filler.Fetch(ctx, []transport.Range{{Origin: 1, Lo: 1, Hi: 1}},
		func(context.Context, []byte) error { return nil })
	if err == nil {
		t.Fatalf("expected error when no peers connected")
	}
}

func TestPeerGapFiller_SkipsPeersWithoutTopic(t *testing.T) {
	// A holds catchup source on topic-app.
	// B has gossip connection to A but does NOT advertise topic-app.
	// PeerGapFiller on B's "topic-app" channel must find no
	// dialable peer (filter excludes A).
	dir := t.TempDir()
	aGossip := "unix:" + filepath.Join(dir, "a.gossip.sock")
	a, err := New(Config{
		Listen: aGossip,
		NodeID: 1,
	})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	// A doesn't open topic-app, so it won't advertise it.

	b, err := New(Config{
		Seeds:     []string{aGossip},
		DialRetry: 25 * time.Millisecond,
		NodeID:    2,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	bChan, err := b.Channel("topic-app")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if !waitForReady(b, 1, 1*time.Second) {
		t.Fatalf("B never connected to A")
	}

	filler := &PeerGapFiller{Channel: bChan, Timeout: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = filler.Fetch(ctx, []transport.Range{{Origin: 1, Lo: 1, Hi: 1}},
		func(context.Context, []byte) error { return nil })
	if err == nil {
		t.Fatalf("Fetch should fail (no peer advertises topic-app)")
	}
}

func TestPeerGapFiller_StreamsFromTopicHoldingPeer(t *testing.T) {
	dir := t.TempDir()
	aGossip := "unix:" + filepath.Join(dir, "a.gossip.sock")

	a, err := New(Config{
		Listen: aGossip,
		NodeID: 1,
	})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	aChan, err := a.Channel("topic-app")
	if err != nil {
		t.Fatalf("Channel A: %v", err)
	}
	src := newStubCatchupSource(wireHeader(7, 42))
	aChan.SetCatchupSource(src)

	b, err := New(Config{
		Seeds:     []string{aGossip},
		DialRetry: 25 * time.Millisecond,
		NodeID:    2,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	bChan, err := b.Channel("topic-app")
	if err != nil {
		t.Fatalf("Channel B: %v", err)
	}

	if !waitForReady(b, 1, 1*time.Second) {
		t.Fatalf("B never connected to A")
	}
	// Wait for membership propagation so PeerStats sees A.
	waitMembership(t, b, a.NodeID(), "topic-app", true, 1*time.Second)

	filler := &PeerGapFiller{Channel: bChan, Timeout: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var got int
	if err := filler.Fetch(ctx, []transport.Range{{Origin: 7, Lo: 42, Hi: 42}},
		func(context.Context, []byte) error { got++; return nil }); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got != 1 {
		t.Fatalf("Fetch applied %d, want 1", got)
	}
}

// TestPeerGapFiller_WorksWhenInboundWins: regardless of which side
// won the dial race, a peer must record a dial-back address that
// PeerGapFiller can use. With Hello.ListenAddr in place, an inbound
// peer's addr equals the remote's declared listener — so PeerStats
// surfaces it identically to an outbound peer.
//
// Pre-fix, an inbound peer's addr was the ephemeral source port and
// PeerStats filtered to outbound-only, so when the tie-break left
// the inbound side as the winner the local side had no way to dial
// catchup. The fix is validated here by asserting p.addr equals
// the remote's gossip listener regardless of direction.
func TestPeerGapFiller_WorksWhenInboundWins(t *testing.T) {
	dir := t.TempDir()
	aGossip := "unix:" + filepath.Join(dir, "a.gossip.sock")
	bGossip := "unix:" + filepath.Join(dir, "b.gossip.sock")

	a, err := New(Config{
		Listen:    aGossip,
		Seeds:     []string{bGossip},
		DialRetry: 25 * time.Millisecond,
		NodeID:    1,
	})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	aChan, err := a.Channel("topic-app")
	if err != nil {
		t.Fatalf("Channel A: %v", err)
	}
	aChan.SetCatchupSource(newStubCatchupSource(wireHeader(7, 42)))

	b, err := New(Config{
		Listen:    bGossip,
		Seeds:     []string{aGossip},
		DialRetry: 25 * time.Millisecond,
		NodeID:    2,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	bChan, err := b.Channel("topic-app")
	if err != nil {
		t.Fatalf("Channel B: %v", err)
	}

	if !waitForReady(a, 1, 2*time.Second) || !waitForReady(b, 1, 2*time.Second) {
		t.Fatalf("peers did not converge")
	}
	waitMembership(t, b, a.NodeID(), "topic-app", true, 1*time.Second)

	// Regardless of which conn (inbound or outbound) won the dial
	// race, B's recorded addr for A must be A's gossip listener so
	// PeerStats can return it. The pre-fix bug surfaced as an
	// ephemeral source port here when the inbound side won, which
	// PeerStats then filtered out.
	b.peersMu.Lock()
	p := b.peersByID[a.NodeID()]
	b.peersMu.Unlock()
	if p == nil {
		t.Fatalf("B has no peer for A")
	}
	if p.addr != aGossip {
		t.Fatalf("B's peer addr = %q, want %q (dial-back from Hello.ListenAddr)", p.addr, aGossip)
	}

	filler := &PeerGapFiller{Channel: bChan, Timeout: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var got int
	if err := filler.Fetch(ctx, []transport.Range{{Origin: 7, Lo: 42, Hi: 42}},
		func(context.Context, []byte) error { got++; return nil }); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got != 1 {
		t.Fatalf("Fetch applied %d, want 1", got)
	}
}

func TestCatchup_RequestCodecRoundTrip(t *testing.T) {
	cases := []transport.CatchupRequest{
		{Ranges: nil, MaxRecords: 0, MaxBytes: 0},
		{Ranges: []transport.Range{{Origin: 1, Lo: 2, Hi: 3}}},
		{Ranges: []transport.Range{{Origin: 7, Lo: 100, Hi: 0}}, MaxRecords: 16, MaxBytes: 1 << 20},
	}
	for _, want := range cases {
		var buf bytes.Buffer
		if err := catchupwire.Write(&buf, want); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err := catchupwire.Read(&buf)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if got.MaxRecords != want.MaxRecords || got.MaxBytes != want.MaxBytes {
			t.Errorf("caps: got %+v want %+v", got, want)
		}
		if len(got.Ranges) != len(want.Ranges) {
			t.Errorf("range count: got %d want %d", len(got.Ranges), len(want.Ranges))
			continue
		}
		for i := range want.Ranges {
			if got.Ranges[i] != want.Ranges[i] {
				t.Errorf("range %d: got %+v want %+v", i, got.Ranges[i], want.Ranges[i])
			}
		}
	}
}

// Compile-time: PeerGapFiller satisfies transport.GapFiller.
var _ transport.GapFiller = (*PeerGapFiller)(nil)
