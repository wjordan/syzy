// Package memtransport is an in-process transport.Transport for tests.
// One Hub serves N peers; Broadcast on any peer fans out to all peers.
// Fetch replays the hub's full history (engine dedupes via frontier).
//
// History grows without bound; the hub is intended for finite test runs.
package memtransport

import (
	"context"
	"fmt"
	"sync"

	"github.com/wjordan/syzy/transport"
)

// ErrHubClosed is returned by Broadcast/Fetch after the Hub has been
// closed. Wraps transport.ErrClosed so callers can errors.Is against
// the transport-neutral sentinel.
var ErrHubClosed = fmt.Errorf("memtransport: hub closed: %w", transport.ErrClosed)

// deliverQueueSize buffers per-peer in-flight broadcasts. A slow
// Subscribe loop blocks Broadcast on the producer once the queue fills;
// 64 is enough headroom for typical test bursts without leaking memory.
const deliverQueueSize = 64

// Hub is the in-memory broker. Peers are not removable; closing the hub
// is the only release path. Construct with NewHub.
type Hub struct {
	mu      sync.Mutex
	peers   []*peer
	history [][]byte
	done    chan struct{}
	closed  bool
}

// NewHub returns a fresh hub.
func NewHub() *Hub { return &Hub{done: make(chan struct{})} }

// Peer registers a new replica and returns its transport. Each Peer holds
// its own delivery queue. Subscribe must be called at most once per Peer
// — multiple concurrent Subscribes race on the same delivery channel.
func (h *Hub) Peer() transport.Transport {
	p := &peer{hub: h, deliver: make(chan []byte, deliverQueueSize)}
	h.mu.Lock()
	h.peers = append(h.peers, p)
	h.mu.Unlock()
	return p
}

// Close shuts the hub down. In-flight Subscribe loops return
// ErrHubClosed, as do future Broadcast/Fetch calls. Safe to call concurrently
// with Broadcast — Broadcast aborts via the same done channel rather than
// observing closed delivery chans.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	close(h.done)
}

// HistoryLen returns the number of changesets the hub has seen.
func (h *Hub) HistoryLen() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.history)
}

type peer struct {
	hub     *Hub
	deliver chan []byte
}

func (p *peer) Broadcast(ctx context.Context, cs []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cp := append([]byte(nil), cs...)

	p.hub.mu.Lock()
	if p.hub.closed {
		p.hub.mu.Unlock()
		return ErrHubClosed
	}
	p.hub.history = append(p.hub.history, cp)
	peers := append([]*peer(nil), p.hub.peers...)
	p.hub.mu.Unlock()

	for _, sub := range peers {
		select {
		case sub.deliver <- cp:
		case <-ctx.Done():
			return ctx.Err()
		case <-p.hub.done:
			return ErrHubClosed
		}
	}
	return nil
}

func (p *peer) Subscribe(ctx context.Context, apply transport.ApplyFunc) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.hub.done:
			return ErrHubClosed
		case b := <-p.deliver:
			if err := apply(ctx, b); err != nil {
				return err
			}
		}
	}
}

// GapFiller returns a transport.GapFiller that replays the hub's full
// history. The Transport contract permits over-delivery; the engine
// dedupes via frontier. Tests that exercise the broker's gap planner
// pass this as broker.Config.GapFiller.
func (h *Hub) GapFiller() transport.GapFiller { return historyFiller{hub: h} }

type historyFiller struct{ hub *Hub }

func (f historyFiller) Fetch(ctx context.Context, _ []transport.Range, apply transport.ApplyFunc) error {
	f.hub.mu.Lock()
	if f.hub.closed {
		f.hub.mu.Unlock()
		return ErrHubClosed
	}
	history := append([][]byte(nil), f.hub.history...)
	f.hub.mu.Unlock()

	for _, cs := range history {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := apply(ctx, cs); err != nil {
			return err
		}
	}
	return nil
}
