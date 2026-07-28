package tcpmesh

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// peer is one ready connection. Setup goroutines hold pre-ready
// state locally; only post-reconciliation peers enter
// Transport.peersByID.
type peer struct {
	conn net.Conn
	// addr is a dial-back address: the outbound dial target for
	// peers we initiated to, or the remote-declared Hello.ListenAddr
	// for inbound peers. Empty when an inbound peer didn't advertise
	// a listener (consume-only). PeerStats / PeerAddrs surface only
	// peers with non-empty addr.
	addr     string
	outbound bool
	nodeID   uint64 // remote NodeID, fixed after hello

	// Conn identity for the duplicate-connection tie-break. Both
	// endpoints derive the same (dialerLow, nonce) pair from the
	// hello exchange, so rank comparisons are a pure function of
	// the conn — not of which setup goroutine happened to run
	// first on each side. dialerLow: the conn was dialed by the
	// lower-NodeID endpoint. nonce: the dialer's ConnNonce.
	dialerLow bool
	nonce     uint64

	// lastRecv is the UnixNano of the most recent inbound frame
	// (hello counts). pingLoop retires peers silent past
	// Config.PingTimeout.
	lastRecv atomic.Int64

	// writeTimeout bounds each frame write (Config.WriteTimeout).
	writeTimeout time.Duration

	// Outbound path: one writer goroutine (writeLoop) fed by two
	// bounded queues. ctrlQ (TOPIC_ADD/REMOVE, PING/PONG,
	// hello-reconcile) always drains first; dataQ carries DATA
	// frames, bounded by frame count and by queuedBytes ≤
	// maxQueuedBytes. Spec: docs/TRANSPORT.md "Outbound delivery".
	dataQ          chan []byte
	ctrlQ          chan []byte
	maxQueuedBytes int64
	queuedBytes    atomic.Int64
	drops          atomic.Uint64
	dropBytes      atomic.Uint64

	// membership is a copy-on-write set of topics the remote has
	// open. interestedIn / addMembership / removeMembership are
	// the only mutators.
	membership atomic.Pointer[map[string]struct{}]

	closeOnce sync.Once
	closed    chan struct{}
}

// ctrlEnqueueTimeout bounds how long a control-frame enqueue may
// block when the peer's control allowance is full. A peer that
// can't accept a control frame within it is retired — never
// silently dropped, never unboundedly queued.
const ctrlEnqueueTimeout = 5 * time.Second

// ctrlQueueLen is the control-frame allowance per peer. Control
// traffic is tiny (membership deltas, pings); the writer drains it
// with priority, so this fills only when the socket is wedged.
const ctrlQueueLen = 64

// enqueueData queues one pre-encoded DATA frame, never blocking:
// when either queue bound is exceeded the frame is dropped and
// counted (the receiver's catch-up chain recovers). Returns false
// on drop.
func (p *peer) enqueueData(frame []byte) bool {
	n := int64(len(frame))
	if p.queuedBytes.Add(n) <= p.maxQueuedBytes {
		select {
		case p.dataQ <- frame:
			return true
		default:
		}
	}
	p.queuedBytes.Add(-n)
	p.drops.Add(1)
	p.dropBytes.Add(uint64(n))
	return false
}

// enqueueControl queues one pre-encoded control frame, blocking up
// to ctrlEnqueueTimeout when the allowance is full. Returns false
// when the peer must be retired (allowance wedged or peer closed).
func (p *peer) enqueueControl(frame []byte) bool {
	select {
	case p.ctrlQ <- frame:
		return true
	default:
	}
	t := time.NewTimer(ctrlEnqueueTimeout)
	defer t.Stop()
	select {
	case p.ctrlQ <- frame:
		return true
	case <-p.closed:
		return false
	case <-t.C:
		return false
	}
}

// writeLoop is the peer's single writer: control frames first, then
// data. A write error or timeout retires the peer; retirement (via
// p.closed) ends the loop and abandons queued frames — the redial's
// hello rebuilds membership and catch-up repairs the data gap.
func (t *Mesh) writeLoop(p *peer) {
	write := func(frame []byte) bool {
		_ = p.conn.SetWriteDeadline(time.Now().Add(p.writeTimeout))
		_, err := p.conn.Write(frame)
		_ = p.conn.SetWriteDeadline(time.Time{})
		if err != nil {
			t.retirePeer(p)
			return false
		}
		return true
	}
	for {
		select {
		case frame := <-p.ctrlQ:
			if !write(frame) {
				return
			}
			continue
		default:
		}
		select {
		case <-p.closed:
			return
		case frame := <-p.ctrlQ:
			if !write(frame) {
				return
			}
		case frame := <-p.dataQ:
			p.queuedBytes.Add(-int64(len(frame)))
			if !write(frame) {
				return
			}
		}
	}
}

func (p *peer) interestedIn(topic string) bool {
	m := p.membership.Load()
	if m == nil {
		return false
	}
	_, ok := (*m)[topic]
	return ok
}

// addMembership atomically adds topic to the remote's interest
// set. Returns (added, overflow). added=true means newly added
// (caller may fire on-connect hooks); overflow=true means the
// peer exceeded MaxPeerTopics and should be retired.
func (p *peer) addMembership(topic string) (added, overflow bool) {
	for {
		old := p.membership.Load()
		var cur map[string]struct{}
		if old != nil {
			cur = *old
		}
		if _, ok := cur[topic]; ok {
			return false, false
		}
		if len(cur) >= MaxPeerTopics {
			return false, true
		}
		next := make(map[string]struct{}, len(cur)+1)
		for k := range cur {
			next[k] = struct{}{}
		}
		next[topic] = struct{}{}
		if p.membership.CompareAndSwap(old, &next) {
			return true, false
		}
	}
}

// removeMembership atomically removes topic from the remote's
// interest set. Returns true if it was present.
func (p *peer) removeMembership(topic string) bool {
	for {
		old := p.membership.Load()
		if old == nil {
			return false
		}
		cur := *old
		if _, ok := cur[topic]; !ok {
			return false
		}
		next := make(map[string]struct{}, len(cur)-1)
		for k := range cur {
			if k == topic {
				continue
			}
			next[k] = struct{}{}
		}
		if p.membership.CompareAndSwap(old, &next) {
			return true
		}
	}
}

// outranks reports whether p wins the duplicate-connection
// tie-break against q (both conns to the same remote NodeID).
// Primary: the conn dialed by the lower-NodeID endpoint wins
// (preserves the original direction rule, so opposite-direction
// simultaneous dials converge on the lower node's outbound conn).
// Same direction (both dialed by the same endpoint): the higher
// nonce — the newer dial — wins. Both endpoints compute identical
// (dialerLow, nonce) from the hello, so the order is total and the
// verdict identical on both sides for every interleaving.
func (p *peer) outranks(q *peer) bool {
	if p.dialerLow != q.dialerLow {
		return p.dialerLow
	}
	return p.nonce > q.nonce
}

// isClosed reports whether the peer's conn has been retired.
func (p *peer) isClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}

// enqueueControlFrame encodes a control frame and queues it with
// priority; on a wedged allowance the peer is retired. Returns
// false when the peer was retired.
func (t *Mesh) enqueueControlFrame(p *peer, msgType byte, topic string) bool {
	frame, err := encodeFrame(msgType, topic, nil)
	if err != nil {
		// Only reachable via an oversized topic, which Channel
		// validation prevents; retire rather than wedge.
		t.retirePeer(p)
		return false
	}
	if !p.enqueueControl(frame) {
		t.log.Warn("tcpmesh: control allowance wedged; retiring peer",
			"remoteID", fmt.Sprintf("%016x", p.nodeID), "addr", p.addr)
		t.retirePeer(p)
		return false
	}
	return true
}

// acceptLoop accepts inbound connections and hands each off to
// dispatchConn on its own goroutine.
func (t *Mesh) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.done:
				return
			default:
			}
			// Transient accept error; brief backoff.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		go t.dispatchConn(conn)
	}
}

// dispatchConn classifies a fresh inbound connection by its first
// byte (spec: docs/TRANSPORT.md "Listener dispatch"): the gossip
// magic leads to peer setup; a one-shot op byte (reserved < 0x40)
// leads to the op dispatcher; anything else is loudly closed.
func (t *Mesh) dispatchConn(conn net.Conn) {
	addr := conn.RemoteAddr().String()
	_ = conn.SetReadDeadline(time.Now().Add(t.cfg.HelloDeadline))
	var first [4]byte
	if _, err := io.ReadFull(conn, first[:1]); err != nil {
		_ = conn.Close()
		return
	}
	switch {
	case first[0] == byte(Magic>>24):
		// Gossip: verify the remaining magic bytes, then run peer
		// setup with the preamble consumed.
		if _, err := io.ReadFull(conn, first[1:]); err != nil || binary.BigEndian.Uint32(first[:]) != Magic {
			t.log.Error("tcpmesh: inbound magic mismatch", "addr", addr)
			_ = conn.Close()
			return
		}
		if err := t.setupPeer(conn, addr, false); err != nil {
			t.log.Debug("tcpmesh: inbound setup failed", "addr", addr, "err", err)
		}
	case first[0] < opReservedLimit:
		t.serveOneShot(conn, first[0])
	default:
		t.log.Error("tcpmesh: unrecognized first byte on mesh listener", "byte", fmt.Sprintf("0x%02x", first[0]), "addr", addr)
		_ = conn.Close()
	}
}

// dialLoop maintains an outbound connection to addr. It dials,
// runs setupPeer, then waits for the peer to retire before
// redialing. Cancelled by t.done or the per-seed cancel channel.
func (t *Mesh) dialLoop(addr string, cancel <-chan struct{}) {
	retry := min(100*time.Millisecond, t.cfg.DialRetry)
	for {
		select {
		case <-t.done:
			return
		case <-cancel:
			return
		default:
		}
		conn, err := dial(addr, t.cfg.TLSConfig)
		if err != nil {
			select {
			case <-t.done:
				return
			case <-cancel:
				return
			case <-time.After(retry):
				retry = min(retry*2, t.cfg.DialRetry)
				continue
			}
		}
		retry = min(100*time.Millisecond, t.cfg.DialRetry)
		p, setupErr := t.setupPeerReturning(conn, addr, true)
		if setupErr != nil {
			t.log.Debug("tcpmesh: outbound setup failed", "addr", addr, "err", setupErr)
		}
		// Wait for retire before redial. When we lost the
		// duplicate tie-break, p is the surviving
		// incumbent conn — parking on its closure (instead of
		// blind retries) stops the loser side from re-dialing into
		// a rejection every DialRetry while a healthy conn exists.
		// p is nil only on setup error; fall through to the retry
		// interval directly.
		if p != nil {
			select {
			case <-t.done:
				return
			case <-cancel:
				return
			case <-p.closed:
			}
		}
		select {
		case <-t.done:
			return
		case <-cancel:
			return
		case <-time.After(t.cfg.DialRetry):
		}
	}
}

// setupPeer is the no-return-value wrapper acceptLoop uses.
func (t *Mesh) setupPeer(conn net.Conn, addr string, outbound bool) error {
	_, err := t.setupPeerReturning(conn, addr, outbound)
	return err
}

// setupPeerReturning drives the hello exchange, applies the
// duplicate-connection tie-break, and admits the winner to the
// ready set. It returns the admitted peer — or, when this conn
// lost the tie-break, the surviving incumbent — so dialLoop can
// wait on its closure. nil only on setup error.
//
// Setup steps:
//  1. Snapshot local topics under openMu (for the hello payload).
//  2. Concurrently writeHello and readHello with HelloDeadline.
//     The dialer's hello carries a fresh ConnNonce; the acceptor
//     adopts it, giving both endpoints one identity per conn.
//  3. Apply the duplicate rule under peersMu+openMu:
//     - localID == remoteID → both sides close (random collision).
//     - otherwise conns between the pair are totally ordered by
//     (dialed-by-lower-NodeID, dialer nonce) and the maximum
//     wins — see peer.outranks. A retired (closed) incumbent
//     is always superseded.
//  4. Winner takes peersByID[remoteID], possibly retiring a prior
//     entry. Records "missed" topics added between hello-build
//     and now into a pending-advertise list.
//  5. Releases openMu. Sends pending TOPIC_ADD frames outside the
//     lock; fires per-channel onConnect hooks for topics the
//     remote already advertises.
//  6. Launches readLoop.
func (t *Mesh) setupPeerReturning(conn net.Conn, addr string, outbound bool) (*peer, error) {
	_ = conn.SetDeadline(time.Now().Add(t.cfg.HelloDeadline))

	t.openMu.Lock()
	if t.closed {
		t.openMu.Unlock()
		_ = conn.Close()
		return nil, errors.New("tcpmesh: closed")
	}
	helloTopics := make([]string, 0, len(t.channels))
	for k := range t.channels {
		helloTopics = append(helloTopics, k)
	}
	t.openMu.Unlock()

	type helloResult struct {
		h   Hello
		err error
	}
	recv := make(chan helloResult, 1)
	go func() {
		// Inbound conns arrive via dispatchConn, which already
		// consumed and verified the magic preamble.
		var h Hello
		var err error
		if outbound {
			h, err = readHello(conn)
		} else {
			h, err = readHelloFrame(conn)
		}
		recv <- helloResult{h, err}
	}()
	// The dialer mints the conn's identity nonce; the acceptor
	// adopts the remote's (its own hello nonce is ignored).
	var localNonce uint64
	if outbound {
		localNonce = connNonce()
	}
	hello := Hello{
		NodeID:     t.nodeID,
		ConnNonce:  localNonce,
		ListenAddr: t.advertiseAddr(),
		Topics:     helloTopics,
	}
	if err := writeHello(conn, hello); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("tcpmesh: hello write: %w", err)
	}
	res := <-recv
	if res.err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("tcpmesh: hello read: %w", res.err)
	}
	remote := res.h
	if remote.NodeID == 0 {
		_ = conn.Close()
		return nil, errors.New("tcpmesh: hello: remote NodeID is zero")
	}
	_ = conn.SetDeadline(time.Time{})

	if t.nodeID == remote.NodeID {
		t.log.Error("tcpmesh: NodeID collision; closing connection",
			"local", t.nodeID, "remote", remote.NodeID, "addr", addr)
		_ = conn.Close()
		return nil, fmt.Errorf("tcpmesh: NodeID collision %x", t.nodeID)
	}
	// Liveness on the winning connection relies on TCP keepalive:
	// a dead peer's read/write errors propagate within ~3×period,
	// dropping the connection so the dial loop reconnects. Unix
	// sockets ignore the option.
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	// Build the peer + initial membership before acquiring locks.
	initial := make(map[string]struct{}, len(remote.Topics))
	for _, top := range remote.Topics {
		initial[top] = struct{}{}
	}
	// dialBackAddr: for outbound peers we already know a working
	// dial target. For inbound peers, the only address we trust is
	// the one the remote advertised in its hello — the ephemeral
	// RemoteAddr() reflects the source port, not a listener.
	dialBackAddr := addr
	if !outbound {
		dialBackAddr = remote.ListenAddr
	}
	nonce := localNonce
	dialerLow := t.nodeID < remote.NodeID
	if !outbound {
		nonce = remote.ConnNonce
		dialerLow = remote.NodeID < t.nodeID
	}
	p := &peer{
		conn:           conn,
		addr:           dialBackAddr,
		outbound:       outbound,
		nodeID:         remote.NodeID,
		dialerLow:      dialerLow,
		nonce:          nonce,
		writeTimeout:   t.cfg.WriteTimeout,
		dataQ:          make(chan []byte, t.cfg.OutboundQueueFrames),
		ctrlQ:          make(chan []byte, ctrlQueueLen),
		maxQueuedBytes: t.cfg.OutboundQueueBytes,
		closed:         make(chan struct{}),
	}
	p.lastRecv.Store(time.Now().UnixNano())
	p.membership.Store(&initial)

	// Admit + reconcile under openMu so Channel can't slip in
	// between admission and missed-topic snapshot.
	t.openMu.Lock()
	if t.closed {
		t.openMu.Unlock()
		_ = conn.Close()
		return nil, errors.New("tcpmesh: closed")
	}
	helloSet := make(map[string]struct{}, len(helloTopics))
	for _, top := range helloTopics {
		helloSet[top] = struct{}{}
	}
	var missed []string
	for top := range t.channels {
		if _, was := helloSet[top]; !was {
			missed = append(missed, top)
		}
	}
	// Topics that were in our hello payload but are no longer open
	// (the channel closed during the hello exchange) need an
	// explicit TOPIC_REMOVE — the peer otherwise thinks we still
	// hold them.
	var dropped []string
	for top := range helloSet {
		if _, still := t.channels[top]; !still {
			dropped = append(dropped, top)
		}
	}
	// Reconcile against any existing peer with the same remote
	// NodeID. The single-dial case (no existing entry) admits
	// immediately. Duplicates are resolved by conn rank (see
	// peer.outranks): a total order both endpoints compute
	// identically, so for any interleaving of the setup goroutines
	// the two sides converge on the same conn. The old rule ("we
	// win iff (outbound && local<remote) || (inbound && local>
	// remote)") gave the same verdict for opposite-direction pairs
	// but had no order for same-direction duplicates — each side
	// kept whichever it evaluated last, which could cross: A
	// attached to conn1, B to conn2, both sockets ESTABLISHED,
	// each side broadcasting into a conn the other no longer read.
	// An incumbent that is already retired never vetoes its
	// replacement, whatever its rank.
	t.peersMu.Lock()
	var retired *peer
	if existing, ok := t.peersByID[remote.NodeID]; ok {
		if !existing.isClosed() && !p.outranks(existing) {
			t.peersMu.Unlock()
			t.openMu.Unlock()
			_ = conn.Close()
			// Hand the surviving incumbent to dialLoop so it parks
			// on that conn's closure instead of redialing into a
			// fresh rejection every DialRetry.
			return existing, nil
		}
		retired = existing
	}
	t.peersByID[remote.NodeID] = p
	t.peersMu.Unlock()

	// Snapshot channels that should fire onConnect for this peer.
	var hooks []*Channel
	for topic := range initial {
		if c, ok := t.channels[topic]; ok {
			hooks = append(hooks, c)
		}
	}
	t.openMu.Unlock()

	if retired != nil {
		retired.closeOnce.Do(func() {
			_ = retired.conn.Close()
			close(retired.closed)
		})
	}

	// The writer must run before the reconcile frames enqueue so a
	// full allowance can drain.
	go t.writeLoop(p)

	// Queue pending TOPIC_ADD outside the lock, and TOPIC_REMOVE for
	// any topic that was open at hello-build time but has since
	// closed.
	for _, top := range missed {
		if !t.enqueueControlFrame(p, msgTopicAdd, top) {
			return nil, fmt.Errorf("tcpmesh: send missed TOPIC_ADD %q: peer retired", top)
		}
	}
	for _, top := range dropped {
		if !t.enqueueControlFrame(p, msgTopicRemove, top) {
			return nil, fmt.Errorf("tcpmesh: send dropped TOPIC_REMOVE %q: peer retired", top)
		}
	}

	for _, c := range hooks {
		c.fireOnConnect()
	}

	go t.readLoop(p)
	return p, nil
}

// readLoop reads frames from p.conn and dispatches them. Exits
// when the connection closes; retires p on exit.
func (t *Mesh) readLoop(p *peer) {
	defer t.retirePeer(p)
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(p.conn, lenBuf[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		if n == 0 || n > MaxFrameSize {
			return
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(p.conn, body); err != nil {
			return
		}
		t.bytesIn.Add(uint64(4) + uint64(n))
		t.framesIn.Add(1)
		if len(body) < 3 {
			return
		}
		msgType := body[0]
		tlen := int(binary.BigEndian.Uint16(body[1:3]))
		if tlen > MaxTopicLen || 3+tlen > len(body) {
			return
		}
		topic := string(body[3 : 3+tlen])
		payload := body[3+tlen:]
		p.lastRecv.Store(time.Now().UnixNano())

		switch msgType {
		case msgData:
			t.deliverPayload(p, topic, payload)
		case msgTopicAdd:
			// Membership changes only apply to the current peer
			// entry. If this conn has been superseded in
			// peersByID, recording membership here would strand
			// it on a retired object while Broadcast consults the
			// replacement; the conn is already condemned (the
			// winner closes it), so just exit.
			if !t.isCurrentPeer(p) {
				return
			}
			added, overflow := p.addMembership(topic)
			if overflow {
				t.log.Error("tcpmesh: peer exceeded MaxPeerTopics; retiring",
					"remoteID", p.nodeID, "addr", p.addr, "max", MaxPeerTopics)
				return
			}
			if added {
				t.openMu.Lock()
				c := t.channels[topic]
				t.openMu.Unlock()
				if c != nil {
					c.fireOnConnect()
				}
			}
		case msgTopicRemove:
			if !t.isCurrentPeer(p) {
				return
			}
			p.removeMembership(topic)
		case msgPing:
			if !t.enqueueControlFrame(p, msgPong, "") {
				return
			}
		case msgPong:
			// lastRecv already stamped above; nothing else to do.
		default:
			// Unknown msgType: close the connection. wire.go reserves
			// 0x05 for a future TOPIC_ASSIGN extension; anything else
			// is a protocol bug or version skew.
			t.log.Error("tcpmesh: unknown msgType on data channel",
				"msgType", fmt.Sprintf("0x%02x", msgType), "remoteID", p.nodeID, "topic", topic)
			return
		}
	}
}

// isCurrentPeer reports whether p is still the ready-set entry for
// its remote NodeID (i.e. has not been superseded or retired).
func (t *Mesh) isCurrentPeer(p *peer) bool {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	return t.peersByID != nil && t.peersByID[p.nodeID] == p
}

// pingLoop is the liveness backstop for conns whose failure mode is
// silence rather than an error. TCP keepalive and the per-write
// deadline both miss the case where the remote process holds the
// socket open but never reads it: the remote kernel keeps ACKing,
// so keepalive passes, and our writes succeed until the send buffer
// fills — with low-rate topics that can take forever, leaving a
// growing Send-Q and silently-vanishing broadcasts. Every
// PingInterval each ready peer gets a PING (sent on its own
// goroutine so one blocked writer can't stall the sweep); any
// inbound frame refreshes lastRecv, and a peer silent past
// PingTimeout is retired so its dial loop rebuilds the conn.
func (t *Mesh) pingLoop() {
	ticker := time.NewTicker(t.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.done:
			return
		case <-ticker.C:
		}
		t.peersMu.Lock()
		peers := make([]*peer, 0, len(t.peersByID))
		for _, p := range t.peersByID {
			peers = append(peers, p)
		}
		t.peersMu.Unlock()
		now := time.Now()
		for _, p := range peers {
			idle := now.Sub(time.Unix(0, p.lastRecv.Load()))
			if idle > t.cfg.PingTimeout {
				t.log.Warn("tcpmesh: peer silent past PingTimeout; retiring",
					"remoteID", fmt.Sprintf("%016x", p.nodeID), "addr", p.addr, "idle", idle)
				t.retirePeer(p)
				continue
			}
			go t.enqueueControlFrame(p, msgPing, "")
		}
	}
}

// deliverPayload routes an inbound DATA frame to its topic's
// channel. Unknown-topic frames increment the protocol-violation
// counter and emit a structured log; they are not silently dropped.
// Frames for known topics whose deliver buffer is full are dropped
// and counted (the broker's catchup chain recovers). Drops are NOT
// logged per-frame here: an overflow burst can shed thousands of
// frames in a moment, so dropReportLoop emits a coalesced summary
// instead of flooding the log one line per dropped frame.
func (t *Mesh) deliverPayload(p *peer, topic string, payload []byte) {
	t.openMu.Lock()
	c, ok := t.channels[topic]
	t.openMu.Unlock()
	if !ok {
		t.unknownTopicFrames.Add(1)
		t.log.Error("tcpmesh: unknown-topic DATA frame received",
			"topic", topic, "remoteID", p.nodeID, "addr", p.addr, "bytes", len(payload))
		return
	}
	wireBytes := uint64(4 + 1 + 2 + len(topic) + len(payload))
	select {
	case c.deliver <- payload:
		c.bytesIn.Add(wireBytes)
		c.framesIn.Add(1)
	default:
		t.deliverDrops.Add(1)
		t.deliverDropBytes.Add(wireBytes)
		c.deliverDrops.Add(1)
		c.deliverDropBytes.Add(wireBytes)
	}
}
