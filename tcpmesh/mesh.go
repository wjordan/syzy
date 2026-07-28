package tcpmesh

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/syzylog"
	"github.com/wjordan/syzy/tcpmesh/internal/peerrtt"
	"github.com/wjordan/syzy/transport"
)

// Config configures a *Mesh.
type Config struct {
	// Listen is the mesh bind address; the one listener carries
	// gossip and every one-shot op (clone, catchup, unique RPC,
	// frontier). Empty with a nil Listener disables inbound entirely
	// (the mesh can still dial seeds and consume).
	Listen string

	// Listener, when non-nil, is a caller-owned pre-bound listener
	// used instead of binding Listen (set exactly one). The mesh
	// wraps it with TLS when TLSConfig is set, uses it exactly as it
	// would its own bind, and closes it on Close. Callers use this
	// for FD inheritance across re-exec, socket activation, or
	// custom socket options.
	Listener net.Listener

	// Advertise, when non-empty, is the address sent to peers in the
	// hello frame and published in endpoint URLs — the address peers
	// dial back and match against cluster inventory. Set it when the
	// listener binds a wildcard for 1:1 NAT so peers see the routable
	// public address instead of "0.0.0.0:port" (which they can
	// neither dial nor match, so the node reads as offline). Empty →
	// the listener's own address.
	Advertise string

	// Seeds is the initial list of peer addresses to dial. SetSeeds
	// replaces this at runtime; operators use it to follow overlay
	// peer changes.
	Seeds []string

	// DialRetry caps the wait between failed dial attempts. Failed dials
	// retry with exponential backoff from 100ms up to this cap, so peers
	// booting simultaneously (each dialing the other before its listener
	// binds) mesh in milliseconds rather than a full retry period. The wait
	// after losing an established connection is always the full cap.
	// Zero → 5s.
	DialRetry time.Duration

	// TLSConfig, when non-nil, wraps the listener with
	// tls.NewListener and uses tls.Dial for outbound connections.
	// Unix-socket addresses bypass TLS (peer auth via filesystem
	// perms).
	TLSConfig *tls.Config

	// Insecure acknowledges running plaintext TCP beyond loopback.
	// With TLSConfig nil, New refuses non-loopback TCP listen and
	// seed addresses unless this is set, and SetSeeds drops such
	// seeds with an error log. Loopback TCP and Unix sockets never
	// require it.
	Insecure bool

	// NodeID is process-random nonzero. Zero (default) generates a
	// fresh ID. Tests force ordering by setting explicit values.
	NodeID uint64

	// HelloDeadline bounds the hello handshake. Zero →
	// DefaultHelloDeadline (5s). Tests lower this to keep
	// hello-timeout test cases fast.
	HelloDeadline time.Duration

	// WriteTimeout bounds each frame write to a peer, so a stuck
	// conn surfaces as a write error (→ peer retired) instead of
	// wedging the peer's writer forever. Zero → 30s.
	WriteTimeout time.Duration

	// OutboundQueueFrames / OutboundQueueBytes bound each peer's
	// outbound data queue (see docs/TRANSPORT.md "Outbound
	// delivery"). When either bound is hit, further DATA frames to
	// that peer are dropped and counted; the peer's catch-up chain
	// recovers them. Zero → 1024 frames / 32 MiB.
	OutboundQueueFrames int
	OutboundQueueBytes  int64

	// PingInterval is how often each ready peer is sent a PING
	// liveness probe; PingTimeout is how long a peer may go without
	// ANY inbound frame before it is retired and re-dialed. This
	// catches remotes that hold the socket open (so TCP keepalive
	// passes and low-rate writes succeed into their receive buffer)
	// but never read or attribute it. Zero → 15s / 60s. PingTimeout
	// should be several multiples of PingInterval.
	PingInterval time.Duration
	PingTimeout  time.Duration

	// Logger is the structured logger used for protocol-violation
	// events (unknown-topic frames, NodeID collisions). Nil →
	// syzylog.Default().
	Logger *slog.Logger
}

// Stats is a snapshot of mesh-wide traffic counters. Per-Channel
// counters are available via Channel.Stats.
type Stats struct {
	BytesIn   uint64
	BytesOut  uint64
	FramesIn  uint64
	FramesOut uint64

	// UnknownTopicFrames counts inbound DATA frames received for a
	// topic that isn't held open locally. Non-zero indicates a
	// protocol bug (the remote should have filtered) or a topic
	// closed concurrently with an in-flight frame.
	UnknownTopicFrames uint64

	// DeliverDrops counts inbound DATA frames discarded because the
	// destination channel's deliver buffer was full. Recoverable via
	// the broker's catchup chain.
	DeliverDrops uint64

	// DeliverDropBytes is the wire-byte volume of the frames counted by
	// DeliverDrops.
	DeliverDropBytes uint64

	// OutboundDrops / OutboundDropBytes count DATA frames dropped
	// because a peer's outbound queue was full (per-peer counts are
	// on PeerStat). Recoverable via the receiver's catch-up chain.
	OutboundDrops     uint64
	OutboundDropBytes uint64

	// PeerRetirements counts ready peers retired for any reason
	// (write failure, ping timeout, tie-break supersession, wedged
	// control allowance). The dial loop rebuilds retired conns.
	PeerRetirements uint64
}

// Mesh multiplexes many logical topics over one shared TCP
// listener and one outbound connection per seed. One Mesh per
// process; arbitrarily many Channels per Mesh.
type Mesh struct {
	cfg    Config
	nodeID uint64
	log    *slog.Logger

	listener net.Listener

	openMu   sync.Mutex
	closed   bool
	channels map[string]*Channel

	// peersByID is the ready set, keyed by remote NodeID. Setup
	// goroutines do not enter this map until hello + collision
	// reconciliation complete.
	peersMu   sync.Mutex
	peersByID map[uint64]*peer

	// activeSeeds maps each currently-dialed seed address to its
	// per-seed cancel channel; lifecycle managed by SetSeeds.
	seedsMu     sync.Mutex
	activeSeeds map[string]chan struct{}

	// Counters updated via atomic ops; read via Stats.
	bytesIn            atomic.Uint64
	bytesOut           atomic.Uint64
	framesIn           atomic.Uint64
	framesOut          atomic.Uint64
	unknownTopicFrames atomic.Uint64
	deliverDrops       atomic.Uint64
	deliverDropBytes   atomic.Uint64
	outboundDrops      atomic.Uint64
	outboundDropBytes  atomic.Uint64
	peerRetirements    atomic.Uint64

	// serveConns tracks accepted one-shot conns so Close can break
	// long-running handlers (clone streams, catchup scans) instead
	// of orphaning them past listener shutdown.
	serveConnsMu sync.Mutex
	serveConns   map[net.Conn]struct{}

	closeOnce sync.Once
	done      chan struct{}
}

// New constructs a Transport. When cfg.Listen is non-empty the
// gossip listener binds before returning; bind failure returns an
// error and starts no goroutines. Dialer goroutines for cfg.Seeds
// run in the background regardless of seed reachability.
func New(cfg Config) (*Mesh, error) {
	if cfg.DialRetry <= 0 {
		cfg.DialRetry = 5 * time.Second
	}
	if cfg.HelloDeadline <= 0 {
		cfg.HelloDeadline = DefaultHelloDeadline
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = 15 * time.Second
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = 60 * time.Second
	}
	if cfg.OutboundQueueFrames <= 0 {
		cfg.OutboundQueueFrames = 1024
	}
	if cfg.OutboundQueueBytes <= 0 {
		cfg.OutboundQueueBytes = 32 << 20
	}
	if cfg.Logger == nil {
		cfg.Logger = syzylog.Default()
	}
	nodeID := cfg.NodeID
	if nodeID == 0 {
		nodeID = randomNodeID()
	}
	t := &Mesh{
		cfg:         cfg,
		nodeID:      nodeID,
		log:         cfg.Logger.With("component", "tcpmesh", "nodeID", fmt.Sprintf("%016x", nodeID)),
		channels:    map[string]*Channel{},
		peersByID:   map[uint64]*peer{},
		activeSeeds: map[string]chan struct{}{},
		serveConns:  map[net.Conn]struct{}{},
		done:        make(chan struct{}),
	}
	if cfg.TLSConfig == nil && !cfg.Insecure {
		addrs := append([]string{cfg.Listen}, cfg.Seeds...)
		if cfg.Listener != nil && cfg.Listener.Addr().Network() == "tcp" {
			addrs = append(addrs, cfg.Listener.Addr().String())
		}
		for _, addr := range addrs {
			if addr != "" && !isLoopbackAddr(addr) {
				return nil, fmt.Errorf("tcpmesh: %q is non-loopback TCP with no TLSConfig; configure TLS or acknowledge plaintext with Insecure", addr)
			}
		}
	}
	switch {
	case cfg.Listener != nil && cfg.Listen != "":
		return nil, errors.New("tcpmesh: set Config.Listen or Config.Listener, not both")
	case cfg.Listener != nil:
		ln := cfg.Listener
		if cfg.TLSConfig != nil && ln.Addr().Network() != "unix" {
			ln = tls.NewListener(ln, cfg.TLSConfig)
		}
		t.listener = ln
	case cfg.Listen != "":
		ln, err := listen(cfg.Listen, cfg.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("tcpmesh: listen %q: %w", cfg.Listen, err)
		}
		t.listener = ln
	}
	if t.listener != nil {
		go t.acceptLoop()
	}
	go t.dropReportLoop()
	go t.pingLoop()
	t.SetSeeds(cfg.Seeds)
	return t, nil
}

// dropReportInterval bounds how often frame drops are logged. Within an
// interval, drops are only counted; one coalesced summary per topic and
// direction is emitted per interval, so a burst that sheds thousands of
// frames produces a single line instead of thousands.
const dropReportInterval = 10 * time.Second

// dropTracker coalesces one drop counter pair (frames, bytes) per topic:
// report logs the delta since the last tick, only when frames were dropped.
// Used only from dropReportLoop's goroutine, so no synchronization.
type dropTracker struct {
	msg        string
	lastFrames map[string]uint64
	lastBytes  map[string]uint64
}

func newDropTracker(msg string) *dropTracker {
	return &dropTracker{msg: msg, lastFrames: map[string]uint64{}, lastBytes: map[string]uint64{}}
}

func (d *dropTracker) report(log *slog.Logger, topic string, frames, bytes uint64) {
	// Guard against a topic being closed and reopened (counters
	// reset): treat a backwards step as a fresh cumulative.
	df, db := frames, bytes
	if frames >= d.lastFrames[topic] {
		df = frames - d.lastFrames[topic]
		db = bytes - d.lastBytes[topic]
	}
	d.lastFrames[topic] = frames
	d.lastBytes[topic] = bytes
	if df > 0 {
		log.Warn(d.msg,
			"topic", topic,
			"dropped", df,
			"bytes", db,
			"interval", dropReportInterval,
			"total_dropped", frames)
	}
}

// prune drops topics no longer open so the maps don't grow unbounded.
func (d *dropTracker) prune(live map[string]struct{}) {
	for topic := range d.lastFrames {
		if _, ok := live[topic]; !ok {
			delete(d.lastFrames, topic)
			delete(d.lastBytes, topic)
		}
	}
}

// dropReportLoop emits coalesced per-topic summaries of deliver-buffer
// overflow (inbound) and outbound-queue overflow drops, replacing per-frame
// logging. It reports only the delta since the previous tick, and only for
// topics that dropped frames, so a healthy transport logs nothing. Runs
// until the transport is closed.
func (t *Mesh) dropReportLoop() {
	ticker := time.NewTicker(dropReportInterval)
	defer ticker.Stop()
	deliver := newDropTracker("tcpmesh: deliver buffer overflow; frames dropped (recoverable via catchup)")
	outbound := newDropTracker("tcpmesh: outbound queue overflow; frames dropped (peers recover via catchup)")
	for {
		select {
		case <-t.done:
			return
		case <-ticker.C:
			t.openMu.Lock()
			chans := make([]*Channel, 0, len(t.channels))
			for _, c := range t.channels {
				chans = append(chans, c)
			}
			t.openMu.Unlock()

			live := make(map[string]struct{}, len(chans))
			for _, c := range chans {
				live[c.topic] = struct{}{}
				deliver.report(t.log, c.topic, c.deliverDrops.Load(), c.deliverDropBytes.Load())
				outbound.report(t.log, c.topic, c.outboundDrops.Load(), c.outboundDropBytes.Load())
			}
			deliver.prune(live)
			outbound.prune(live)
		}
	}
}

// NodeID returns the process-random ID advertised in hello frames
// and used for connection-collision tie-break.
func (t *Mesh) NodeID() uint64 { return t.nodeID }

// Addr returns the listener's address, or empty when no listener
// is configured.
func (t *Mesh) Addr() string { return canonicalAddr(t.listener) }

// advertiseAddr is the address sent to peers in the hello frame and
// published in endpoint URLs: the configured Advertise override (1:1
// NAT) when set, else the listener's own address. Addr() stays the
// bind address for self-dial filtering.
func (t *Mesh) advertiseAddr() string {
	if t.cfg.Advertise != "" {
		return t.cfg.Advertise
	}
	return canonicalAddr(t.listener)
}

// SetSeeds replaces the active dialer set. Each address in addrs
// that isn't already being dialed gets a fresh dialer goroutine;
// each active dialer for an address absent from addrs is
// cancelled. The local listener addr is filtered so a node seeded
// with its own address doesn't spin forever colliding with itself.
// Idempotent; no-op after Close.

func (t *Mesh) SetSeeds(addrs []string) {
	self := t.Addr()
	desired := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		if a == "" || a == self {
			continue
		}
		if t.cfg.TLSConfig == nil && !t.cfg.Insecure && !isLoopbackAddr(a) {
			// Same fail-closed rule New enforces, applied to runtime
			// seed refreshes: never dial plaintext beyond loopback
			// without the explicit acknowledgement.
			t.log.Error("tcpmesh: dropping non-loopback plaintext seed (no TLSConfig and Insecure unset)", "addr", a)
			continue
		}
		desired[a] = struct{}{}
	}
	t.seedsMu.Lock()
	defer t.seedsMu.Unlock()
	if t.activeSeeds == nil {
		return
	}
	for addr, cancel := range t.activeSeeds {
		if _, ok := desired[addr]; !ok {
			close(cancel)
			delete(t.activeSeeds, addr)
		}
	}
	for addr := range desired {
		if _, ok := t.activeSeeds[addr]; !ok {
			cancel := make(chan struct{})
			t.activeSeeds[addr] = cancel
			go t.dialLoop(addr, cancel)
		}
	}
}

// ActiveSeeds returns the seed addresses currently being dialed.
func (t *Mesh) ActiveSeeds() []string {
	t.seedsMu.Lock()
	defer t.seedsMu.Unlock()
	out := make([]string, 0, len(t.activeSeeds))
	for a := range t.activeSeeds {
		out = append(out, a)
	}
	return out
}

// PeerAddrs returns the dial addresses of every currently-ready
// peer. Order is unspecified.
func (t *Mesh) PeerAddrs() []string {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	out := make([]string, 0, len(t.peersByID))
	for _, p := range t.peersByID {
		out = append(out, p.addr)
	}
	return out
}

// PeerStats returns one transport.PeerStat per ready peer with a
// known dial-back address. Inbound peers contribute their declared
// Hello.ListenAddr; peers without one (consume-only) are excluded.
func (t *Mesh) PeerStats() []transport.PeerStat {
	t.peersMu.Lock()
	peers := make([]*peer, 0, len(t.peersByID))
	for _, p := range t.peersByID {
		if p.addr == "" {
			continue
		}
		peers = append(peers, p)
	}
	t.peersMu.Unlock()

	out := make([]transport.PeerStat, 0, len(peers))
	for _, p := range peers {
		rtt, rttVar, since := peerrtt.PeerRTT(p.conn)
		out = append(out, transport.PeerStat{
			Addr:          p.addr,
			RTT:           rtt,
			RTTVar:        rttVar,
			SinceLastRecv: since,
			QueuedFrames:  len(p.dataQ),
			QueuedBytes:   p.queuedBytes.Load(),
			OutboundDrops: p.drops.Load(),
		})
	}
	return out
}

// Stats returns mesh-wide bytes/frames counters. Per-Channel
// DATA counters are available via Channel.Stats.
func (t *Mesh) Stats() Stats {
	return Stats{
		BytesIn:            t.bytesIn.Load(),
		BytesOut:           t.bytesOut.Load(),
		FramesIn:           t.framesIn.Load(),
		FramesOut:          t.framesOut.Load(),
		UnknownTopicFrames: t.unknownTopicFrames.Load(),
		DeliverDrops:       t.deliverDrops.Load(),
		DeliverDropBytes:   t.deliverDropBytes.Load(),
		OutboundDrops:      t.outboundDrops.Load(),
		OutboundDropBytes:  t.outboundDropBytes.Load(),
		PeerRetirements:    t.peerRetirements.Load(),
	}
}

// Close stops listeners, breaks ready peer connections, cancels
// dialers, breaks in-flight bundle/catchup conns, and signals all
// goroutines to exit. Idempotent.
//
// Order matters: we flip t.closed under openMu first so any
// in-flight setupPeerReturning that has yet to acquire peersMu sees
// the flag and bails before the peersByID map is torn down.
func (t *Mesh) Close() error {
	t.closeOnce.Do(func() {
		t.openMu.Lock()
		t.closed = true
		channels := t.channels
		t.channels = nil
		t.openMu.Unlock()

		close(t.done)
		if t.listener != nil {
			_ = t.listener.Close()
		}
		t.seedsMu.Lock()
		for _, cancel := range t.activeSeeds {
			close(cancel)
		}
		t.activeSeeds = nil
		t.seedsMu.Unlock()

		t.peersMu.Lock()
		peers := make([]*peer, 0, len(t.peersByID))
		for _, p := range t.peersByID {
			peers = append(peers, p)
		}
		t.peersByID = nil
		t.peersMu.Unlock()
		for _, p := range peers {
			_ = p.conn.Close()
		}

		// Break in-flight one-shot conns so long streams (clone
		// bundles, catchup scans) don't outlive the listener
		// shutdown. Per-conn handlers see EOF on next read/write
		// and unwind.
		t.serveConnsMu.Lock()
		serveConns := t.serveConns
		t.serveConns = nil
		t.serveConnsMu.Unlock()
		for c := range serveConns {
			_ = c.Close()
		}

		for _, c := range channels {
			c.markClosed()
		}
	})
	return nil
}

// trackServeConn registers conn so Close can break it. Returns
// false if Close already ran, in which case caller should reject
// the conn immediately. Pair every successful trackServeConn
// with an untrackServeConn(conn) on handler return.
func (t *Mesh) trackServeConn(conn net.Conn) bool {
	t.serveConnsMu.Lock()
	defer t.serveConnsMu.Unlock()
	if t.serveConns == nil {
		return false
	}
	t.serveConns[conn] = struct{}{}
	return true
}

func (t *Mesh) untrackServeConn(conn net.Conn) {
	t.serveConnsMu.Lock()
	defer t.serveConnsMu.Unlock()
	if t.serveConns == nil {
		return
	}
	delete(t.serveConns, conn)
}

// Channel registers a topic on this mesh and returns the
// corresponding *Channel. Repeated calls with the same topic
// return the same instance. Safe to call concurrently with itself
// and with connection setup.
func (t *Mesh) Channel(topic string) (*Channel, error) {
	if topic == "" {
		return nil, errors.New("tcpmesh: Channel: topic must be non-empty")
	}
	if len(topic) > MaxTopicLen {
		return nil, fmt.Errorf("tcpmesh: Channel: topic length %d exceeds %d", len(topic), MaxTopicLen)
	}

	t.openMu.Lock()
	if t.closed {
		t.openMu.Unlock()
		return nil, fmt.Errorf("tcpmesh: Channel: %w", transport.ErrClosed)
	}
	if c, ok := t.channels[topic]; ok {
		t.openMu.Unlock()
		return c, nil
	}
	c := &Channel{
		mesh:    t,
		topic:   topic,
		deliver: make(chan []byte, channelDeliverSize),
		done:    make(chan struct{}),
	}
	t.channels[topic] = c
	t.openMu.Unlock()

	// Advertise this topic to every currently-ready peer. Network
	// writes happen outside the lock; each per-peer wmu serializes
	// writes to its own connection.
	t.advertiseTopic(topic)
	return c, nil
}

// channelDeliverSize bounds each channel's inbound queue: the buffer that
// decouples the network read loop from the apply consumer. Sized to absorb the
// rate-mismatch spikes of a catch-up storm (a node returning from offline drinks
// its backlog plus the live stream at once) so transient bursts queue instead of
// dropping. Drops past this size are recoverable via the broker's catchup chain,
// but each drop costs a re-fetch, so a deeper buffer that avoids the drop is
// cheaper than the repair. Memory cost is bounded by the payloads actually held
// (a slow consumer falling behind), not the slot count; the consumer normally
// keeps it near-empty.
const channelDeliverSize = 4096

// advertiseTopic sends TOPIC_ADD to every ready peer for the given
// topic. Called by Channel (after the channel is registered)
// and by closeChannel-with-TOPIC_REMOVE (with msgTopicRemove).
func (t *Mesh) advertiseTopic(topic string) {
	t.advertiseTopicFrame(topic, msgTopicAdd)
}

func (t *Mesh) advertiseTopicFrame(topic string, msgType byte) {
	t.peersMu.Lock()
	peers := make([]*peer, 0, len(t.peersByID))
	for _, p := range t.peersByID {
		peers = append(peers, p)
	}
	t.peersMu.Unlock()
	for _, p := range peers {
		t.enqueueControlFrame(p, msgType, topic)
	}
}

// retirePeer removes p from the ready set and closes its
// connection. Idempotent; safe to call from any goroutine.
func (t *Mesh) retirePeer(p *peer) {
	t.peersMu.Lock()
	if t.peersByID != nil && t.peersByID[p.nodeID] == p {
		delete(t.peersByID, p.nodeID)
	}
	t.peersMu.Unlock()
	p.closeOnce.Do(func() {
		_ = p.conn.Close()
		close(p.closed)
		t.peerRetirements.Add(1)
	})
}

// canonicalAddr returns a listener's local address in the form
// callers can dial back.
func canonicalAddr(ln net.Listener) string {
	if ln == nil {
		return ""
	}
	a := ln.Addr()
	if a.Network() == "unix" {
		return "unix:" + a.String()
	}
	return a.String()
}

// isLoopbackAddr reports whether addr never needs transport
// security: Unix sockets (filesystem perms) and TCP bound/dialed on
// a loopback IP or "localhost". Wildcard hosts and unparseable
// addresses report false.
func isLoopbackAddr(addr string) bool {
	network, address := splitAddr(addr)
	if network == "unix" {
		return true
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// splitAddr decomposes "unix:/path" → ("unix", "/path") and any
// other addr → ("tcp", addr).
func splitAddr(addr string) (network, address string) {
	if rest, ok := strings.CutPrefix(addr, "unix:"); ok {
		return "unix", rest
	}
	return "tcp", addr
}

// listen binds addr (with TLS wrapping for tcp when tlsCfg is
// non-nil) and removes stale Unix socket files before binding.
func listen(addr string, tlsCfg *tls.Config) (net.Listener, error) {
	network, address := splitAddr(addr)
	if network == "unix" {
		// bind(2) rejects an overlong path with a bare EINVAL, so
		// check it here to say what actually went wrong.
		if err := layout.CheckUnixSocketPath(address); err != nil {
			return nil, err
		}
		_ = os.Remove(address)
	}
	ln, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil && network == "tcp" {
		ln = tls.NewListener(ln, tlsCfg)
	}
	return ln, nil
}

// dial connects to addr, wrapping with TLS when tlsCfg is non-nil
// and the network is TCP.
func dial(addr string, tlsCfg *tls.Config) (net.Conn, error) {
	network, address := splitAddr(addr)
	if network == "unix" {
		// connect(2) fails the same opaque way bind(2) does.
		if err := layout.CheckUnixSocketPath(address); err != nil {
			return nil, err
		}
	}
	if tlsCfg == nil || network != "tcp" {
		return net.Dial(network, address)
	}
	return tls.Dial(network, address, tlsCfg)
}

// Channel is a topic-scoped view of a Transport.
type Channel struct {
	mesh  *Mesh
	topic string

	deliver chan []byte
	done    chan struct{}

	closeOnce sync.Once

	mu          sync.Mutex
	bundleH     transport.BundleHandler
	catchupSrc  transport.CatchupSource
	frontierSrc transport.FrontierSource
	uniqueH     func(net.Conn)
	onConnect   func()

	bytesIn           atomic.Uint64
	bytesOut          atomic.Uint64
	framesIn          atomic.Uint64
	framesOut         atomic.Uint64
	deliverDrops      atomic.Uint64
	deliverDropBytes  atomic.Uint64
	outboundDrops     atomic.Uint64
	outboundDropBytes atomic.Uint64
}

// Topic returns the channel's topic string.
func (c *Channel) Topic() string { return c.topic }

// Broadcast publishes one local changeset to every ready peer that
// has advertised interest in this channel's topic.
func (c *Channel) Broadcast(ctx context.Context, payload []byte) error {
	// Validate the wire body size — msgType(1) + topicLen(2) +
	// topic + payload — not just payload. A payload near MaxFrameSize
	// can still produce a body that overflows after the topic header
	// is added; rejecting it here keeps a local sizing error from
	// turning into per-peer write failures and mass retirements.
	bodyLen := 1 + 2 + len(c.topic) + len(payload)
	if uint32(bodyLen) > MaxFrameSize {
		return fmt.Errorf("tcpmesh: Broadcast: frame body %d (topic %q + payload %d) exceeds MaxFrameSize %d", bodyLen, c.topic, len(payload), MaxFrameSize)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.done:
		return transport.ErrClosed
	default:
	}
	c.mesh.peersMu.Lock()
	peers := make([]*peer, 0, len(c.mesh.peersByID))
	for _, p := range c.mesh.peersByID {
		peers = append(peers, p)
	}
	c.mesh.peersMu.Unlock()

	// Encode once; every interested peer queues the same buffer.
	frame, err := encodeFrame(msgData, c.topic, payload)
	if err != nil {
		return err
	}
	for _, p := range peers {
		if !p.interestedIn(c.topic) {
			continue
		}
		if !p.enqueueData(frame) {
			// Queue full: drop-and-count (docs/TRANSPORT.md
			// "Outbound delivery"); the peer's catch-up chain
			// recovers the seq.
			c.mesh.outboundDrops.Add(1)
			c.mesh.outboundDropBytes.Add(uint64(len(frame)))
			c.outboundDrops.Add(1)
			c.outboundDropBytes.Add(uint64(len(frame)))
			continue
		}
		c.mesh.bytesOut.Add(uint64(len(frame)))
		c.mesh.framesOut.Add(1)
		c.bytesOut.Add(uint64(len(frame)))
		c.framesOut.Add(1)
	}
	return nil
}

// Subscribe blocks calling apply for each inbound payload until ctx
// cancels (returns ctx.Err()) or the channel/mux closes (returns
// transport.ErrClosed — delivery on this handle is permanently over).
func (c *Channel) Subscribe(ctx context.Context, apply transport.ApplyFunc) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			// Covers both Channel.Close and Transport.Close —
			// the latter marks every channel closed.
			return transport.ErrClosed
		case payload := <-c.deliver:
			if err := apply(ctx, payload); err != nil {
				return err
			}
		}
	}
}

// setHandler runs assign under the channel lock unless the channel is
// already closed. The single guard point for every handler setter:
// teardown closes done inside the same critical section, so a Set*
// call can never re-pin a reference teardown released.
func (c *Channel) setHandler(assign func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
	default:
		assign()
	}
}

// SetBundleHandler installs the source-side bundle producer for
// this channel's topic. Pass nil to refuse incoming clone requests.
// No-op after Close.
func (c *Channel) SetBundleHandler(h transport.BundleHandler) {
	c.setHandler(func() { c.bundleH = h })
}

// SetCatchupSource installs the source-side catchup producer for
// this channel's topic. Pass nil to refuse catchup requests.
// No-op after Close.
func (c *Channel) SetCatchupSource(src transport.CatchupSource) {
	c.setHandler(func() { c.catchupSrc = src })
}

// SetOnPeerConnect installs a callback that fires when a peer
// becomes interested in this channel's topic. No-op after Close.
func (c *Channel) SetOnPeerConnect(fn func()) {
	c.setHandler(func() { c.onConnect = fn })
}

// Endpoint returns the canonical peer-dialable URL of this channel,
// "tcp://host:port?topic=…" or "unix:///path?topic=…", over the
// mesh's one advertised address. Empty when the mesh has no
// listener. Clone URLs and uniqueness-lease records carry this
// form (e.g. via cluster_inventory).
func (c *Channel) Endpoint() string {
	if c.mesh.listener == nil {
		return ""
	}
	return BuildEndpointURL(c.mesh.advertiseAddr(), c.topic)
}

// Fetcher returns a transport.BundleFetcher pre-bound to this channel's
// topic. The returned function takes a bare mesh address
// (host:port or unix:/path) and writes the clone stream
// into w. The topic is implicit.
func (c *Channel) Fetcher() transport.BundleFetcher {
	tlsCfg := c.mesh.cfg.TLSConfig
	topic := c.topic
	return func(ctx context.Context, addr string, w io.Writer) error {
		return fetchBundleAddrTopic(ctx, addr, topic, w, tlsCfg)
	}
}

// PeerStats returns ready peers currently advertising this
// channel's topic. Each peer's addr is its dial-back address
// (outbound's dial target or the inbound peer's declared
// Hello.ListenAddr). RTT comes from the mesh's per-connection
// kernel sample.
func (c *Channel) PeerStats() []transport.PeerStat {
	c.mesh.peersMu.Lock()
	peers := make([]*peer, 0, len(c.mesh.peersByID))
	for _, p := range c.mesh.peersByID {
		if p.addr == "" || !p.interestedIn(c.topic) {
			continue
		}
		peers = append(peers, p)
	}
	c.mesh.peersMu.Unlock()

	out := make([]transport.PeerStat, 0, len(peers))
	for _, p := range peers {
		rtt, rttVar, since := peerrtt.PeerRTT(p.conn)
		out = append(out, transport.PeerStat{
			Addr:          p.addr,
			RTT:           rtt,
			RTTVar:        rttVar,
			SinceLastRecv: since,
			QueuedFrames:  len(p.dataQ),
			QueuedBytes:   p.queuedBytes.Load(),
			OutboundDrops: p.drops.Load(),
		})
	}
	return out
}

// Stats returns this channel's DATA frame counters. TOPIC_ADD /
// TOPIC_REMOVE traffic is mesh-wide and accounted only in
// Transport.Stats.
func (c *Channel) Stats() Stats {
	return Stats{
		BytesIn:           c.bytesIn.Load(),
		BytesOut:          c.bytesOut.Load(),
		FramesIn:          c.framesIn.Load(),
		FramesOut:         c.framesOut.Load(),
		DeliverDrops:      c.deliverDrops.Load(),
		DeliverDropBytes:  c.deliverDropBytes.Load(),
		OutboundDrops:     c.outboundDrops.Load(),
		OutboundDropBytes: c.outboundDropBytes.Load(),
	}
}

// Close is idempotent. It removes the channel from the mesh (a later
// Channel with the same topic returns a fresh channel),
// broadcasts TOPIC_REMOVE to ready peers, signals in-flight
// Subscribe calls to return transport.ErrClosed, and releases
// handler references and queued payloads.
func (c *Channel) Close() error {
	c.closeOnce.Do(func() {
		c.mesh.openMu.Lock()
		if c.mesh.channels != nil {
			delete(c.mesh.channels, c.topic)
		}
		c.mesh.openMu.Unlock()
		c.teardown()
		c.mesh.advertiseTopicFrame(c.topic, msgTopicRemove)
	})
	return nil
}

// markClosed is called by Transport.Close to signal in-flight
// Subscribe loops without sending TOPIC_REMOVE (which can't reach
// already-closed peers anyway).
func (c *Channel) markClosed() {
	c.closeOnce.Do(c.teardown)
}

// teardown flips the channel to closed: signals Subscribe loops,
// releases handler references, and frees queued payloads. Handler
// clearing matters because embedders may retain the *Channel past
// Close; without it a closed channel pins its node's apply state.
// done is closed under mu so setHandler's guard and the handler
// clearing are a single atomic transition. The deliver drain races
// benignly with a concurrent deliverPayload — its leftovers are
// garbage-collected with the channel.
func (c *Channel) teardown() {
	c.mu.Lock()
	c.bundleH = nil
	c.catchupSrc = nil
	c.frontierSrc = nil
	c.uniqueH = nil
	c.onConnect = nil
	close(c.done)
	c.mu.Unlock()
	for {
		select {
		case <-c.deliver:
		default:
			return
		}
	}
}

// fireOnConnect invokes the registered callback if any. Called by
// setupPeer when a peer becomes interested in this topic.
func (c *Channel) fireOnConnect() {
	c.mu.Lock()
	fn := c.onConnect
	c.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// PeerCatchupBuilder returns a transport.GapFiller bound to this
// channel's topic. The filter (topic-advertising peers only) and
// the tie-break-aware peer set both come from the mesh. Satisfies
// transport.PeerCatchupBuilder.
func (c *Channel) PeerCatchupBuilder() transport.GapFiller {
	return &PeerGapFiller{Channel: c}
}

// Compile-time guards.
var (
	_ transport.Transport           = (*Channel)(nil)
	_ transport.PeerStatter         = (*Channel)(nil)
	_ transport.BundleSource        = (*Channel)(nil)
	_ transport.CatchupRegistrar    = (*Channel)(nil)
	_ transport.PeerConnectNotifier = (*Channel)(nil)
	_ transport.PeerCatchupBuilder  = (*Channel)(nil)
	_ transport.BundleFetcher       = FetchBundle
)
