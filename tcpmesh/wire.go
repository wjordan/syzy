package tcpmesh

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Magic is the 4-byte preamble before the first (hello) frame on
// every mux connection. The value is chosen so a legacy pre-mux
// peer that misreads it as a frame length sees a number well above
// MaxFrameSize and rejects the framing immediately — making
// accidental v1↔v2 wire mismatches fail loudly instead of
// quasi-randomly mis-parsing payloads.
const Magic uint32 = 0x53595A32 // "SYZ2"

// Message types. msgHello is preceded by Magic; all other frames are
// length-prefixed and follow hello exchange.
const (
	msgHello       byte = 0x01
	msgData        byte = 0x02
	msgTopicAdd    byte = 0x03
	msgTopicRemove byte = 0x04
	// 0x05 reserved for a future TOPIC_ASSIGN that swaps topic
	// strings for small integer ids post-hello.

	// msgPing/msgPong are the application-level liveness probe.
	// TCP keepalive only proves the remote KERNEL is alive; a remote
	// process that holds the socket open but never reads it (wedged
	// setup, crossed connection attribution) keeps ACKing probes
	// while our frames rot in its receive buffer. PING demands a
	// read-side response: a peer that doesn't answer within
	// Config.PingTimeout is retired so the dial loop rebuilds the
	// conn. Topic is empty; payload is empty.
	msgPing byte = 0x06
	msgPong byte = 0x07
)

// MaxFrameSize caps any single inbound frame body at 16 MiB.
const MaxFrameSize uint32 = 16 << 20

// MaxTopicLen caps topic names. Topics are UTF-8, case-sensitive,
// otherwise opaque; the cap exists so a malformed peer can't make
// us allocate unbounded read buffers from a single u16 topicLen.
const MaxTopicLen = 256

// MaxListenAddrLen caps the listen-addr string a peer advertises in
// hello. Long enough for unix socket paths; bounded so a malformed
// peer can't force unbounded reads from a single u16 length.
const MaxListenAddrLen = 1024

// MaxPeerTopics caps how many topics a single peer may advertise
// across (hello + TOPIC_ADDs). A malicious or buggy remote can't
// blow up local memory with unbounded membership entries. Far
// above any plausible legitimate workload; on overflow the peer
// is retired with a structured log.
const MaxPeerTopics = 65536

// DefaultHelloDeadline bounds how long a connection may take to
// send and receive its hello. Connections that miss this are
// closed before joining the broadcast set. Override via
// Config.HelloDeadline; tests use a shorter value.
const DefaultHelloDeadline = 5 * time.Second

// Bundle/catchup response status table. The status byte is
// the FIRST byte of any bundle or catchup response; non-zero
// values close the connection immediately with no further bytes.
const (
	StatusOK           byte = 0x00
	StatusUnknownTopic byte = 0x01
	StatusNoHandler    byte = 0x02
	// 0x03 unassigned (was "closed"; never emitted — a closed channel
	// deregisters from the topic map and answers unknown-topic).
	StatusBadRequest    byte = 0x04
	StatusInternalError byte = 0x05
)

// Hello is the connection-establishment frame's content.
type Hello struct {
	// NodeID is process-random nonzero. Two ends with the same
	// NodeID collide; the duplicate-connection resolution rule
	// (see docs/TRANSPORT.md) closes both sides on collision.
	NodeID uint64
	// ListenAddr is the local gossip listener's canonical dial
	// address ("host:port" or "unix:/path"), or empty for nodes
	// without an inbound listener. Lets the peer dial back even
	// when the collision tie-break keeps the inbound side of the
	// connection — required for PeerStats / PeerGapFiller to work
	// from either endpoint.
	ListenAddr string
	// ConnNonce identifies this specific connection attempt. Only
	// the DIALING side's value is meaningful: the acceptor adopts
	// the remote hello's nonce, so both endpoints agree on one
	// identity per conn. Together with "which endpoint dialed",
	// it forms a total order over all conns between a node pair —
	// the duplicate-connection tie-break keeps the maximum, making
	// the verdict a pure function of the conn instead of the
	// order the two setup goroutines happen to run on each side.
	// Monotonic per process (see connNonce) so a redial always
	// outranks the conn it replaces.
	ConnNonce uint64
	// Topics is the set of locally-held topics at hello-build
	// time. Topics added between hello-send and ready-transition
	// are reconciled via TOPIC_ADD inside the setup goroutine.
	Topics []string
}

// writeHello writes the magic preamble followed by a length-prefixed
// hello frame to w. The whole thing is sent in one Write so a peer
// that's about to read sees a complete frame in one syscall on
// typical setups.
func writeHello(w io.Writer, h Hello) error {
	body, err := encodeHelloBody(h)
	if err != nil {
		return err
	}
	if uint32(len(body)) > MaxFrameSize {
		return fmt.Errorf("tcpmesh: hello body %d exceeds MaxFrameSize %d", len(body), MaxFrameSize)
	}
	buf := make([]byte, 4+4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], Magic)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(body)))
	copy(buf[8:], body)
	_, err = w.Write(buf)
	return err
}

// readHello reads the magic preamble and hello frame from r. A
// magic mismatch returns an error mentioning "magic" so callers can
// distinguish "legacy peer" from "torn frame."
func readHello(r io.Reader) (Hello, error) {
	var m [4]byte
	if _, err := io.ReadFull(r, m[:]); err != nil {
		return Hello{}, err
	}
	if got := binary.BigEndian.Uint32(m[:]); got != Magic {
		return Hello{}, fmt.Errorf("tcpmesh: hello: magic mismatch (got 0x%08x, want 0x%08x)", got, Magic)
	}
	return readHelloFrame(r)
}

// readHelloFrame reads the length-prefixed hello frame, magic
// already consumed — the listener-dispatch path sniffs the magic
// itself (see Mesh.dispatchConn).
func readHelloFrame(r io.Reader) (Hello, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Hello{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameSize {
		return Hello{}, fmt.Errorf("tcpmesh: hello: frame %d exceeds MaxFrameSize %d", n, MaxFrameSize)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return Hello{}, err
	}
	return decodeHelloBody(body)
}

// Hello wire body layout (after msgType):
//
//	nodeID(u64) | connNonce(u64) | listenAddrLen(u16) | listenAddr | nTopics(u32) | topics...
//
// Each topic is (topicLen u16, bytes). helloMinBody covers the
// fixed-size fields with zero-length listenAddr and zero topics.
const helloMinBody = 1 + 8 + 8 + 2 + 4 // msgType + nodeID + connNonce + listenAddrLen + nTopics

func encodeHelloBody(h Hello) ([]byte, error) {
	if len(h.ListenAddr) > MaxListenAddrLen {
		return nil, fmt.Errorf("tcpmesh: hello: listen addr length %d exceeds %d", len(h.ListenAddr), MaxListenAddrLen)
	}
	size := helloMinBody + len(h.ListenAddr)
	for _, t := range h.Topics {
		if len(t) > MaxTopicLen {
			return nil, fmt.Errorf("tcpmesh: hello: topic %q length %d exceeds %d", t, len(t), MaxTopicLen)
		}
		size += 2 + len(t)
	}
	body := make([]byte, size)
	body[0] = msgHello
	binary.BigEndian.PutUint64(body[1:9], h.NodeID)
	binary.BigEndian.PutUint64(body[9:17], h.ConnNonce)
	binary.BigEndian.PutUint16(body[17:19], uint16(len(h.ListenAddr)))
	off := 19
	copy(body[off:off+len(h.ListenAddr)], h.ListenAddr)
	off += len(h.ListenAddr)
	binary.BigEndian.PutUint32(body[off:off+4], uint32(len(h.Topics)))
	off += 4
	for _, t := range h.Topics {
		binary.BigEndian.PutUint16(body[off:off+2], uint16(len(t)))
		off += 2
		copy(body[off:off+len(t)], t)
		off += len(t)
	}
	return body, nil
}

func decodeHelloBody(body []byte) (Hello, error) {
	if len(body) < helloMinBody {
		return Hello{}, fmt.Errorf("tcpmesh: hello: body %d shorter than minimum %d", len(body), helloMinBody)
	}
	if body[0] != msgHello {
		return Hello{}, fmt.Errorf("tcpmesh: hello: msgType 0x%02x, want 0x%02x", body[0], msgHello)
	}
	var h Hello
	h.NodeID = binary.BigEndian.Uint64(body[1:9])
	h.ConnNonce = binary.BigEndian.Uint64(body[9:17])
	alen := int(binary.BigEndian.Uint16(body[17:19]))
	if alen > MaxListenAddrLen {
		return Hello{}, fmt.Errorf("tcpmesh: hello: listen addr length %d exceeds %d", alen, MaxListenAddrLen)
	}
	off := 19
	if off+alen > len(body) {
		return Hello{}, fmt.Errorf("tcpmesh: hello: truncated listen addr (need %d, have %d)", alen, len(body)-off)
	}
	h.ListenAddr = string(body[off : off+alen])
	off += alen
	if off+4 > len(body) {
		return Hello{}, fmt.Errorf("tcpmesh: hello: truncated nTopics at offset %d", off)
	}
	nTopics := binary.BigEndian.Uint32(body[off : off+4])
	off += 4
	if nTopics > MaxPeerTopics {
		return Hello{}, fmt.Errorf("tcpmesh: hello: nTopics %d exceeds MaxPeerTopics %d", nTopics, MaxPeerTopics)
	}
	if nTopics > 0 {
		h.Topics = make([]string, 0, nTopics)
	}
	for i := uint32(0); i < nTopics; i++ {
		if off+2 > len(body) {
			return Hello{}, fmt.Errorf("tcpmesh: hello: truncated topic-length at offset %d", off)
		}
		tlen := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2
		if tlen > MaxTopicLen {
			return Hello{}, fmt.Errorf("tcpmesh: hello: topic length %d exceeds %d", tlen, MaxTopicLen)
		}
		if off+tlen > len(body) {
			return Hello{}, fmt.Errorf("tcpmesh: hello: truncated topic body (need %d, have %d)", tlen, len(body)-off)
		}
		h.Topics = append(h.Topics, string(body[off:off+tlen]))
		off += tlen
	}
	if off != len(body) {
		return Hello{}, fmt.Errorf("tcpmesh: hello: %d trailing bytes after topics", len(body)-off)
	}
	return h, nil
}

// writeFrame writes a length-prefixed (msgType, topic, payload)
// frame. Used for DATA, TOPIC_ADD, TOPIC_REMOVE after hello
// exchange. payload may be nil for TOPIC_ADD/TOPIC_REMOVE.
func writeFrame(w io.Writer, msgType byte, topic string, payload []byte) error {
	buf, err := encodeFrame(msgType, topic, payload)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// encodeFrame renders one complete wire frame (length prefix
// included), so Broadcast can encode once and enqueue the same
// buffer on every interested peer's writer.
func encodeFrame(msgType byte, topic string, payload []byte) ([]byte, error) {
	if len(topic) > MaxTopicLen {
		return nil, fmt.Errorf("tcpmesh: frame: topic length %d exceeds %d", len(topic), MaxTopicLen)
	}
	bodyLen := 1 + 2 + len(topic) + len(payload)
	if uint32(bodyLen) > MaxFrameSize {
		return nil, fmt.Errorf("tcpmesh: frame: body %d exceeds MaxFrameSize %d", bodyLen, MaxFrameSize)
	}
	buf := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodyLen))
	buf[4] = msgType
	binary.BigEndian.PutUint16(buf[5:7], uint16(len(topic)))
	copy(buf[7:7+len(topic)], topic)
	copy(buf[7+len(topic):], payload)
	return buf, nil
}

// readFrame reads one (msgType, topic, payload) frame from r. The
// returned payload aliases internal buffer storage; callers must
// copy if they retain it past the next read.
func readFrame(r io.Reader) (msgType byte, topic string, payload []byte, err error) {
	var hdr [4]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, "", nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return 0, "", nil, fmt.Errorf("tcpmesh: frame: zero-length body")
	}
	if n > MaxFrameSize {
		return 0, "", nil, fmt.Errorf("tcpmesh: frame: %d exceeds MaxFrameSize %d", n, MaxFrameSize)
	}
	body := make([]byte, n)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, "", nil, err
	}
	const minHdr = 1 + 2 // msgType + topicLen
	if len(body) < minHdr {
		return 0, "", nil, fmt.Errorf("tcpmesh: frame: body %d shorter than %d", len(body), minHdr)
	}
	msgType = body[0]
	tlen := int(binary.BigEndian.Uint16(body[1:3]))
	if tlen > MaxTopicLen {
		return 0, "", nil, fmt.Errorf("tcpmesh: frame: topic length %d exceeds %d", tlen, MaxTopicLen)
	}
	if minHdr+tlen > len(body) {
		return 0, "", nil, fmt.Errorf("tcpmesh: frame: truncated topic (need %d, have %d)", tlen, len(body)-minHdr)
	}
	topic = string(body[minHdr : minHdr+tlen])
	payload = body[minHdr+tlen:]
	return msgType, topic, payload, nil
}

// lastConnNonce backs connNonce's strictly-increasing guarantee.
var lastConnNonce atomic.Uint64

// connNonce returns a per-dial connection nonce: wall-clock
// nanoseconds, bumped to stay strictly increasing within the
// process. Monotonicity is what matters — the tie-break keeps the
// highest-nonce conn among same-direction duplicates, so a fresh
// dial always supersedes the conn it replaces, never the reverse.
// (Cross-process comparability is irrelevant: nonces are only ever
// compared between conns dialed by the same endpoint.)
func connNonce() uint64 {
	for {
		now := uint64(time.Now().UnixNano())
		old := lastConnNonce.Load()
		if now <= old {
			now = old + 1
		}
		if lastConnNonce.CompareAndSwap(old, now) {
			return now
		}
	}
}

// randomNodeID returns a process-random nonzero uint64 suitable for
// the hello frame and connection-collision tie-break. Reads from
// crypto/rand so two processes spawned at the same instant don't
// collide by accident.
func randomNodeID() uint64 {
	for {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			// crypto/rand on Linux is /dev/urandom-backed and can
			// only fail in pathological environments. Fall through
			// to a low-entropy fallback derived from time so the
			// transport still starts; the collision-tie-break rule
			// still applies.
			now := uint64(time.Now().UnixNano())
			if now != 0 {
				return now
			}
			return 1
		}
		v := binary.BigEndian.Uint64(b[:])
		if v != 0 {
			return v
		}
	}
}
