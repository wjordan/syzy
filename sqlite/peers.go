package sqlite

import (
	"sort"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

// Peer is the locality view of one directly-connected peer.
//
// RTT and RTTVar come from the kernel's TCP_INFO smoothed estimate
// and are zero when no sample is available (non-TCP socket, OS
// without TCP_INFO, or a connection too fresh for the kernel to
// have computed one). SinceLastRecv is the age of the last received
// data segment; large values mean the RTT estimate may be stale.
//
// To bridge an observed Change.Origin into the locality view, use
// AddrFor(origin) then PeerFor(addr) — addr is the stable identity
// (origins rotate on unclean restart at the same listen address).
type Peer struct {
	Addr          string
	RTT           time.Duration
	RTTVar        time.Duration
	SinceLastRecv time.Duration
}

// Peers returns one Peer per directly-connected outbound neighbor,
// sorted by RTT ascending. Peers with no available RTT sample sort
// last; ties break on Addr.
//
// Returns nil when this Node has no Transport or when the configured
// Transport does not implement transport.PeerStatter.
func (n *Node) Peers() []Peer {
	stats := n.transportPeerStats()
	if stats == nil {
		return nil
	}
	out := make([]Peer, 0, len(stats))
	for _, s := range stats {
		out = append(out, peerFromStat(s))
	}
	sort.Slice(out, func(i, j int) bool {
		ai, aj := out[i].RTT == 0, out[j].RTT == 0
		if ai != aj {
			return !ai
		}
		if out[i].RTT != out[j].RTT {
			return out[i].RTT < out[j].RTT
		}
		return out[i].Addr < out[j].Addr
	})
	return out
}

// PeerFor returns the directly-connected Peer at addr, or
// (_, false) when no live outbound connection matches. addr must be
// the canonical listen address (the form peerdisc heartbeats use and
// SetSeeds expects).
func (n *Node) PeerFor(addr string) (Peer, bool) {
	if addr == "" {
		return Peer{}, false
	}
	for _, s := range n.transportPeerStats() {
		if s.Addr == addr {
			return peerFromStat(s), true
		}
	}
	return Peer{}, false
}

// AddrFor returns the listen address most recently known for origin,
// or ("", false) when no binding has been registered. The binding is
// best-effort — verify by following up with PeerFor, which reflects
// the live transport view.
func (n *Node) AddrFor(origin uint64) (string, bool) {
	if origin == 0 {
		return "", false
	}
	n.originAddrMu.Lock()
	defer n.originAddrMu.Unlock()
	addr, ok := n.originAddr[crdt.Origin(origin)]
	return addr, ok
}

// SetOriginAddrs atomically replaces the origin→addr registry used
// by AddrFor. Pass the full current bindings on every call; omitted
// origins are removed. Passing nil clears the registry.
func (n *Node) SetOriginAddrs(bindings map[uint64]string) {
	clean := make(map[crdt.Origin]string, len(bindings))
	for o, a := range bindings {
		if o == 0 || a == "" {
			continue
		}
		clean[crdt.Origin(o)] = a
	}
	n.originAddrMu.Lock()
	n.originAddr = clean
	n.originAddrMu.Unlock()
}

func peerFromStat(s transport.PeerStat) Peer {
	return Peer{
		Addr:          s.Addr,
		RTT:           s.RTT,
		RTTVar:        s.RTTVar,
		SinceLastRecv: s.SinceLastRecv,
	}
}

func (n *Node) transportPeerStats() []transport.PeerStat {
	if n.transport == nil {
		return nil
	}
	ps, ok := n.transport.(transport.PeerStatter)
	if !ok {
		return nil
	}
	return ps.PeerStats()
}
