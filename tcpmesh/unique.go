package tcpmesh

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/wjordan/syzy/unique"
)

// This file routes coordinated-uniqueness reservation RPCs over the
// mesh listener (op opUniqueRPC). The leaseholder registers a
// per-connection handler via Channel.SetUniqueHandler; a follower reaches
// it by dialing the channel's endpoint URL (UniqueDial). Both reuse the
// already-peer-connected, firewall-open mesh listener, so cross-node
// reservation needs no extra port or address advertisement — the lease
// record just carries the leaseholder's endpoint URL.

// SetUniqueHandler installs the per-connection handler for opUniqueRPC on
// this channel's topic. The handler is invoked with the raw connection
// after the request prefix and an OK status byte have been written; it
// owns the connection (and must close it). Pass nil to refuse.
// No-op after Close.
func (c *Channel) SetUniqueHandler(h func(net.Conn)) {
	c.setHandler(func() { c.uniqueH = h })
}

// serveUniqueRPC writes a status byte, then (if OK) hands the raw
// connection to the registered unique handler. Called from
// serveOneShot after the topic prefix resolved c.
func serveUniqueRPC(c *Channel, conn net.Conn) {
	c.mu.Lock()
	h := c.uniqueH
	c.mu.Unlock()
	if h == nil {
		_ = writeStatus(conn, StatusNoHandler)
		return
	}
	// The exchange is request/response on a raw conn (net/rpc), so clear
	// the handshake read deadline before handing it off.
	_ = conn.SetReadDeadline(time.Time{})
	if err := writeStatus(conn, StatusOK); err != nil {
		return
	}
	defer c.breakOnClose(conn)()
	h(conn)
}

// UniqueDial dials the leaseholder published at rawURL (a channel
// endpoint URL, "tcp://host:port?topic=…" or "unix:///path?topic=…"),
// performs the opUniqueRPC handshake, and returns the raw connection
// ready for the caller to run net/rpc over. tlsCfg matches the mesh's
// listener (nil for plaintext / unix). The caller owns the returned
// conn.
func UniqueDial(ctx context.Context, rawURL string, tlsCfg *tls.Config) (net.Conn, error) {
	addr, topic, err := ParseEndpointURL(rawURL)
	if err != nil {
		return nil, err
	}
	conn, err := dialOp(ctx, addr, topic, opUniqueRPC, tlsCfg, opDialTimeout, nil)
	if err != nil {
		return nil, err
	}
	// Hand back a clean conn: clear the handshake deadline so the
	// caller's net/rpc traffic isn't tripped by it.
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

// UniqueTransport adapts a *Channel into the serve+dial carrier the
// unique leaseholder/client expect (it structurally satisfies
// unique.ServeTransport and unique.DialTransport without mux importing
// unique). Serve registers the handler and publishes the channel's
// endpoint URL; Dial reaches the leaseholder named by that URL over the mesh.
type UniqueTransport struct {
	ch     *Channel
	tlsCfg *tls.Config
}

// NewUniqueTransport binds a UniqueTransport to ch. tlsCfg must be the
// mesh's TLS config (it dials peer mesh listeners).
func NewUniqueTransport(ch *Channel, tlsCfg *tls.Config) *UniqueTransport {
	return &UniqueTransport{ch: ch, tlsCfg: tlsCfg}
}

// Serve registers handler on the channel and returns the endpoint URL
// peers dial to reach it. Errors if the mesh has no listener
// (a clustered node without one cannot publish a reachable address).
func (u *UniqueTransport) Serve(handler func(net.Conn)) (string, error) {
	addr := u.ch.Endpoint()
	if addr == "" {
		return "", fmt.Errorf("tcpmesh: unique transport: channel %q has no reachable endpoint; set Listen", u.ch.topic)
	}
	u.ch.SetUniqueHandler(handler)
	return addr, nil
}

// Close unregisters the handler. The listener is owned by the mesh
// and is not closed here.
func (u *UniqueTransport) Close() error {
	u.ch.SetUniqueHandler(nil)
	return nil
}

// Dial reaches the leaseholder published at rawURL over the mesh.
func (u *UniqueTransport) Dial(ctx context.Context, rawURL string) (net.Conn, error) {
	return UniqueDial(ctx, rawURL, u.tlsCfg)
}

// UniqueServeTransport returns the leaseholder-side carrier bound to this
// channel. Together with UniqueDialTransport it makes *Channel satisfy
// unique.TransportProvider, so sqlite.Open routes reservation RPCs over the
// mesh when cfg.Transport is a mesh channel.
func (c *Channel) UniqueServeTransport() unique.ServeTransport {
	return NewUniqueTransport(c, c.mesh.cfg.TLSConfig)
}

// UniqueDialTransport returns the client-side carrier bound to this
// channel (same matched pair as UniqueServeTransport).
func (c *Channel) UniqueDialTransport() unique.DialTransport {
	return NewUniqueTransport(c, c.mesh.cfg.TLSConfig)
}

// Compile-time guard: *Channel carries reservation RPCs over the mesh.
var _ unique.TransportProvider = (*Channel)(nil)
