package tcpmesh

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

// Compile-time capability guards.
var (
	_ transport.FrontierRegistrar   = (*Channel)(nil)
	_ transport.PeerFrontierBuilder = (*Channel)(nil)
	_ transport.PeerFrontier        = (*PeerFrontierSource)(nil)
)

// Wire format for op 0x03 (frontier):
//
//	request body (after op + topic prefix): none.
//	response:
//	  byte status (0x00 OK; non-zero per the status table)
//	  if OK: u32 BE count, then count * { u64 BE origin, u64 BE seq }
//
// The frontier is the responding node's applied-frontier for the topic:
// per origin, the highest contiguous applied seq. Peers use it to (a)
// discover origins they never received live and pull them, and (b) decide
// when an origin is fully replicated everywhere and its mirror journal is
// safe to reap. Cheap and idempotent; no side effects on the server.

// maxFrontierOrigins caps a frontier response so a hostile/buggy peer can't
// make us allocate unboundedly.
const maxFrontierOrigins = 1 << 20

// SetFrontierSource installs the server-side frontier provider for this
// channel's topic (transport.FrontierRegistrar). Pass nil to refuse frontier
// queries (the node then answers StatusNoHandler, as an un-upgraded peer does).
// No-op after Close.
func (c *Channel) SetFrontierSource(src transport.FrontierSource) {
	c.setHandler(func() { c.frontierSrc = src })
}

// serveFrontier answers an opFrontier request. Called from
// serveOneShot after the topic prefix resolved c.
func serveFrontier(c *Channel, conn net.Conn) {
	c.mu.Lock()
	src := c.frontierSrc
	c.mu.Unlock()
	if src == nil {
		_ = writeStatus(conn, StatusNoHandler)
		return
	}
	fr := src.Frontier()
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := writeStatus(conn, StatusOK); err != nil {
		return
	}
	_ = writeFrontier(conn, fr)
}

func writeFrontier(w io.Writer, fr map[crdt.Origin]crdt.Seq) error {
	if len(fr) > maxFrontierOrigins {
		return fmt.Errorf("tcpmesh: frontier count %d exceeds %d", len(fr), maxFrontierOrigins)
	}
	buf := make([]byte, 4+16*len(fr))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(fr)))
	off := 4
	for o, s := range fr {
		binary.BigEndian.PutUint64(buf[off:off+8], uint64(o))
		binary.BigEndian.PutUint64(buf[off+8:off+16], uint64(s))
		off += 16
	}
	_, err := w.Write(buf)
	return err
}

func readFrontier(r io.Reader) (map[crdt.Origin]crdt.Seq, error) {
	var cnt [4]byte
	if _, err := io.ReadFull(r, cnt[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(cnt[:])
	if n > maxFrontierOrigins {
		return nil, fmt.Errorf("tcpmesh: frontier count %d exceeds %d", n, maxFrontierOrigins)
	}
	out := make(map[crdt.Origin]crdt.Seq, n)
	rec := make([]byte, 16)
	for i := uint32(0); i < n; i++ {
		if _, err := io.ReadFull(r, rec); err != nil {
			return nil, err
		}
		o := crdt.Origin(binary.BigEndian.Uint64(rec[0:8]))
		s := crdt.Seq(binary.BigEndian.Uint64(rec[8:16]))
		out[o] = s
	}
	return out, nil
}

// fetchFrontierFromPeer dials a peer and reads the topic's
// frontier.
func fetchFrontierFromPeer(ctx context.Context, addr, topic string, tlsCfg *tls.Config, timeout time.Duration) (map[crdt.Origin]crdt.Seq, error) {
	conn, err := dialOp(ctx, addr, topic, opFrontier, tlsCfg, timeout, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return readFrontier(conn)
}

// PeerFrontierSource queries connected peers' applied-frontiers over
// opFrontier and aggregates them. It satisfies transport.TipSource so the
// broker's fetcher pulls origins this node never saw live (proactive
// new-origin discovery without the object-store seal+LIST backstop), and it
// exposes AllPeersApplied so the Node reaper can drop a mirror journal once
// every live peer holds that origin fully (no live consumer needs our copy).
//
// Refresh re-queries every peer and replaces the cache; DiscoverTips and
// AllPeersApplied read the cache without blocking on the network.
type PeerFrontierSource struct {
	channel *Channel
	timeout time.Duration

	mu      sync.Mutex
	perPeer map[string]frontierObs // peer gossip addr -> last observation
}

// frontierObs is one peer's last frontier observation: either a
// fetched frontier or the error that prevented it, stamped with
// when the attempt completed.
type frontierObs struct {
	fr  map[crdt.Origin]crdt.Seq
	err error
	at  time.Time
}

// PeerFrontierBuilder constructs a PeerFrontierSource over this
// channel; peers are dialed at their gossip address. Satisfies
// transport.PeerFrontierBuilder.
func (c *Channel) PeerFrontierBuilder() transport.PeerFrontier {
	return &PeerFrontierSource{
		channel: c,
		timeout: 10 * time.Second,
		perPeer: map[string]frontierObs{},
	}
}

// Refresh queries every connected peer's frontier and atomically replaces the
// cached per-peer observations. Errors are recorded, not omitted (they
// surface via Observations); a departed peer vanishes from the cache, so it
// stops constraining AllPeersApplied, and an errored/un-upgraded peer never
// constrains it either — only fetched frontiers do.
func (p *PeerFrontierSource) Refresh(ctx context.Context) {
	if p == nil || p.channel == nil {
		return
	}
	stats := p.channel.PeerStats()
	tlsCfg := p.channel.mesh.cfg.TLSConfig
	topic := p.channel.topic
	// Query peers concurrently so one hung peer can't stall the whole refresh
	// (and thus the fetcher round / reaper pass) for timeout * N.
	type res struct {
		addr string
		obs  frontierObs
	}
	ch := make(chan res, len(stats))
	var wg sync.WaitGroup
	for _, st := range stats {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			fr, err := fetchFrontierFromPeer(ctx, addr, topic, tlsCfg, p.timeout)
			ch <- res{addr, frontierObs{fr: fr, err: err, at: time.Now()}}
		}(st.Addr)
	}
	go func() { wg.Wait(); close(ch) }()
	next := make(map[string]frontierObs, len(stats))
	for r := range ch {
		next[r.addr] = r.obs
	}
	p.mu.Lock()
	p.perPeer = next
	p.mu.Unlock()
}

// Observations reports one entry per currently-connected topic-holding
// peer: its last observation (ok or error, with age) or unknown when no
// refresh has reached it yet. Connected-but-unhealthy peers are never
// omitted — see docs/TRANSPORT.md "Peer frontiers".
func (p *PeerFrontierSource) Observations() []transport.FrontierObservation {
	if p == nil || p.channel == nil {
		return nil
	}
	stats := p.channel.PeerStats()
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]transport.FrontierObservation, 0, len(stats))
	for _, st := range stats {
		obs, ok := p.perPeer[st.Addr]
		switch {
		case !ok:
			out = append(out, transport.FrontierObservation{
				Addr:  st.Addr,
				State: transport.FrontierUnknown,
			})
		case obs.err != nil:
			out = append(out, transport.FrontierObservation{
				Addr:  st.Addr,
				State: transport.FrontierError,
				Age:   now.Sub(obs.at),
				Err:   obs.err.Error(),
			})
		default:
			out = append(out, transport.FrontierObservation{
				Addr:     st.Addr,
				State:    transport.FrontierOK,
				Frontier: obs.fr,
				Age:      now.Sub(obs.at),
			})
		}
	}
	return out
}

// DiscoverTips implements transport.TipSource: the highest seq any peer holds
// per origin. Refreshes first so a node that never receives a connect callback
// still converges; the fetcher's own cadence bounds how often this runs.
func (p *PeerFrontierSource) DiscoverTips(ctx context.Context) (map[crdt.Origin]crdt.Seq, error) {
	p.Refresh(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[crdt.Origin]crdt.Seq)
	for _, obs := range p.perPeer {
		for o, s := range obs.fr {
			if s > out[o] {
				out[o] = s
			}
		}
	}
	return out, nil
}

// AllPeersApplied reports whether every currently-known peer holds origin o at
// seq >= head. The Node reaper uses it as the GC-safety predicate: if all live
// peers already have an origin fully, none needs to catch it up from our
// mirror journal, so the journal is a reapable, reversible cache copy. Returns
// false when no peers are known (cannot prove safety, so keep the journal).
func (p *PeerFrontierSource) AllPeersApplied(o crdt.Origin, head crdt.Seq) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Only fetched frontiers count: an errored/un-upgraded peer never
	// constrains (matching its pre-observation absence), and with no
	// successful observation at all safety can't be proven.
	answered := 0
	for _, obs := range p.perPeer {
		if obs.err != nil {
			continue
		}
		answered++
		if obs.fr[o] < head {
			return false
		}
	}
	return answered > 0
}
