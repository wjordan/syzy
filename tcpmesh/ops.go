package tcpmesh

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"
)

// One-shot request/response ops sharing the mesh listener with
// gossip, dispatched by the connection's first byte (see
// Mesh.dispatchConn and docs/TRANSPORT.md "Listener dispatch").
// Riding the already-peer-connected, firewall-open mesh port means
// followers reach a uniqueness leaseholder — and peers pull
// frontiers and clone bundles — with no extra port.
//
// Request: op byte, u16 BE topic length, topic bytes, then an
// op-specific body. Response: one status byte, then an op-specific
// payload on StatusOK.
const (
	opBundleStream   byte = 0x00
	opCatchupRequest byte = 0x01
	// opUniqueRPC carries a coordinated-uniqueness reservation RPC to the
	// channel's registered unique handler. After the (op, topic) prefix and
	// an OK status byte the connection is handed raw to the handler (the
	// leaseholder drives net/rpc over it). See unique.go.
	opUniqueRPC byte = 0x02
	// opFrontier requests the channel's applied-frontier (origin -> highest
	// contiguous applied seq) for proactive new-origin discovery and GC-safety
	// decisions. Request body is empty; response is a framed origin/seq map.
	// See frontier.go.
	opFrontier byte = 0x03

	// opReservedLimit bounds the one-shot op space forever: ops stay
	// below 0x40 so they can never collide with the gossip magic's
	// first byte (0x53) on the shared listener.
	opReservedLimit byte = 0x40
)

// opHandshakeTimeout bounds how long the server waits for the
// (topic prefix + request body) before timing out an idle client.
// Streaming responses clear the deadline before writing.
const opHandshakeTimeout = 10 * time.Second

// opDialTimeout is the default client-side bound on the dial +
// handshake when the caller's context has no tighter deadline.
const opDialTimeout = 10 * time.Second

// serveOneShot handles a single one-shot request whose op byte was
// consumed by dispatchConn: it reads the topic prefix, resolves the
// channel, and hands off to the op's serve method. The shared
// prefix/lookup/status handling lives here so each op contributes
// only its body codec and serve loop.
func (t *Mesh) serveOneShot(conn net.Conn, op byte) {
	defer conn.Close()
	if !t.trackServeConn(conn) {
		return // Mesh already closed.
	}
	defer t.untrackServeConn(conn)
	_ = conn.SetReadDeadline(time.Now().Add(opHandshakeTimeout))
	topic, err := readTopicPrefix(conn)
	if err != nil || topic == "" {
		_ = writeStatus(conn, StatusBadRequest)
		return
	}
	t.openMu.Lock()
	c, ok := t.channels[topic]
	t.openMu.Unlock()
	if !ok {
		_ = writeStatus(conn, StatusUnknownTopic)
		return
	}
	switch op {
	case opBundleStream:
		serveBundleStream(c, conn)
	case opCatchupRequest:
		serveCatchup(c, conn)
	case opFrontier:
		serveFrontier(c, conn)
	case opUniqueRPC:
		serveUniqueRPC(c, conn)
	default:
		_ = writeStatus(conn, StatusBadRequest)
	}
}

// breakOnClose force-closes conn when the channel is closed, so an
// in-flight serve for a torn-down topic unwinds instead of running to
// completion against dead handler state. (Mesh-wide Close already
// breaks every serve conn via the tracked-conn set.) Callers defer
// the returned stop func to release the watcher when the serve ends.
func (c *Channel) breakOnClose(conn net.Conn) (stop func()) {
	served := make(chan struct{})
	go func() {
		select {
		case <-c.done:
			_ = conn.Close()
		case <-served:
		}
	}()
	return func() { close(served) }
}

// dialOp dials addr ("host:port" or "unix:/path"), performs the
// uniform one-shot handshake — TLS when configured, op + topic
// prefix, optional request body, status byte — and returns the
// connection ready for the op's response, its deadline still set to
// timeout. writeBody may be nil. On any error the conn is closed.
func dialOp(ctx context.Context, addr, topic string, op byte, tlsCfg *tls.Config, timeout time.Duration, writeBody func(net.Conn) error) (net.Conn, error) {
	if topic == "" {
		return nil, fmt.Errorf("tcpmesh: op 0x%02x: topic required", op)
	}
	conn, err := dialContext(ctx, addr, tlsCfg, timeout)
	if err != nil {
		return nil, fmt.Errorf("tcpmesh: dial %s: %w", addr, err)
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	err = writeRequestPrefix(conn, op, topic)
	if err == nil && writeBody != nil {
		err = writeBody(conn)
	}
	if err == nil {
		err = readStatus(conn)
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// dialContext is the context-aware dialer used by one-shot clients.
func dialContext(ctx context.Context, addr string, tlsCfg *tls.Config, timeout time.Duration) (net.Conn, error) {
	network, address := splitAddr(addr)
	d := net.Dialer{Timeout: timeout}
	if tlsCfg == nil || network != "tcp" {
		return d.DialContext(ctx, network, address)
	}
	td := tls.Dialer{NetDialer: &d, Config: tlsCfg}
	return td.DialContext(ctx, network, address)
}

// readTopicPrefix reads the u16 BE length-prefixed topic that
// follows the op byte on a one-shot connection.
func readTopicPrefix(r io.Reader) (string, error) {
	var tlBuf [2]byte
	if _, err := io.ReadFull(r, tlBuf[:]); err != nil {
		return "", err
	}
	tlen := int(binary.BigEndian.Uint16(tlBuf[:]))
	if tlen > MaxTopicLen {
		return "", fmt.Errorf("tcpmesh: one-shot request: topic length %d exceeds %d", tlen, MaxTopicLen)
	}
	if tlen == 0 {
		return "", nil
	}
	body := make([]byte, tlen)
	if _, err := io.ReadFull(r, body); err != nil {
		return "", err
	}
	return string(body), nil
}

// writeRequestPrefix writes the (op, topic) prefix.
func writeRequestPrefix(w io.Writer, op byte, topic string) error {
	if len(topic) > MaxTopicLen {
		return fmt.Errorf("tcpmesh: one-shot request: topic length %d exceeds %d", len(topic), MaxTopicLen)
	}
	buf := make([]byte, 1+2+len(topic))
	buf[0] = op
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(topic)))
	copy(buf[3:], topic)
	_, err := w.Write(buf)
	return err
}

// writeStatus writes a single status byte. Non-zero statuses tell
// the client to expect no further bytes; OK tells it to read the
// framed payload stream that follows.
func writeStatus(w io.Writer, status byte) error {
	_, err := w.Write([]byte{status})
	return err
}

// readStatus reads the response status byte and returns a typed
// error (BundleError) for non-zero values so callers can fail over
// to the next peer instead of treating refusal as empty success.
func readStatus(r io.Reader) error {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return fmt.Errorf("tcpmesh: read status: %w", err)
	}
	if b[0] == StatusOK {
		return nil
	}
	return &BundleError{Status: b[0]}
}

// BundleError is the typed response of a non-OK one-shot status
// byte. PeerGapFiller-style chains use it to fail over.
type BundleError struct {
	Status byte
}

func (e *BundleError) Error() string {
	switch e.Status {
	case StatusUnknownTopic:
		return "tcpmesh: op refused: unknown topic"
	case StatusNoHandler:
		return "tcpmesh: op refused: no handler"
	case StatusBadRequest:
		return "tcpmesh: op refused: bad request"
	case StatusInternalError:
		return "tcpmesh: op refused: internal server error"
	default:
		return fmt.Sprintf("tcpmesh: op refused: unknown status 0x%02x", e.Status)
	}
}

// ParseEndpointURL extracts (addr, topic) from a channel endpoint
// URL of the form "tcp://host:port?topic=…" or
// "unix:///abs/path?topic=…". The topic is required and non-empty.
func ParseEndpointURL(raw string) (addr, topic string, err error) {
	u, perr := url.Parse(raw)
	if perr != nil {
		return "", "", fmt.Errorf("tcpmesh: parse url %q: %w", raw, perr)
	}
	topic = u.Query().Get("topic")
	if topic == "" {
		return "", "", fmt.Errorf("tcpmesh: endpoint url missing ?topic=: %q", raw)
	}
	switch u.Scheme {
	case "tcp":
		if u.Host == "" {
			return "", "", fmt.Errorf("tcpmesh: endpoint url missing host: %q", raw)
		}
		addr = u.Host
	case "unix":
		// "unix:///abs/path" → u.Path = "/abs/path".
		// "unix:/abs/path" (no leading //) is also accepted by
		// url.Parse but yields Opaque, not Path. Handle both.
		if u.Path != "" {
			addr = "unix:" + u.Path
		} else if u.Opaque != "" {
			addr = "unix:" + u.Opaque
		} else {
			return "", "", fmt.Errorf("tcpmesh: endpoint url missing unix path: %q", raw)
		}
	default:
		return "", "", fmt.Errorf("tcpmesh: unsupported scheme %q", u.Scheme)
	}
	return addr, topic, nil
}

// BuildEndpointURL produces the canonical endpoint URL for a
// channel (topic) reachable at addr, the mesh's one advertised
// address. addr may be "host:port" or "unix:/path".
func BuildEndpointURL(addr, topic string) string {
	if addr == "" || topic == "" {
		return ""
	}
	q := url.Values{}
	q.Set("topic", topic)
	if strings.HasPrefix(addr, "unix:") {
		path := strings.TrimPrefix(addr, "unix:")
		u := url.URL{Scheme: "unix", Path: path, RawQuery: q.Encode()}
		return u.String()
	}
	u := url.URL{Scheme: "tcp", Host: addr, RawQuery: q.Encode()}
	return u.String()
}
