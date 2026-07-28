// Package tcpmesh is Syzy's built-in network transport: one Mesh per
// process multiplexes many topics over a single listener — carrying
// gossip and the one-shot ops (clone, catchup, unique RPC, frontier)
// on one port — and one outbound connection per peer pair,
// regardless of how many topics are open.
//
// Mesh.Channel(topic) returns the topic's *Channel — a thin view that
// satisfies transport.Transport plus the optional serving capabilities
// (SetCatchupSource, SetFrontierSource, SetBundleHandler). Repeated
// calls for a live topic return the same instance; after Channel.Close
// the next call opens a fresh one and re-advertises the topic to
// peers. Outbound payloads are stamped with the channel's topic and
// routed only to peers that advertise it; inbound payloads are
// filtered by topic and delivered to a per-channel bounded deliver
// chan.
//
// When the deliver chan fills, frames for that topic are dropped and
// counted; the broker's catchup chain recovers the dropped seqs via
// per-(origin, seq) idempotency. Drops are observability, not failure.
//
// See docs/TRANSPORT.md for the wire protocol, connection identity
// (NodeID + tie-break), hello/topic reconciliation, listener
// dispatch, and the one-shot status table.
package tcpmesh
