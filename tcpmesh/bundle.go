package tcpmesh

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"
)

// Op 0x00 (bundle clone stream): request body is empty; on StatusOK
// the server streams the channel's registered
// transport.BundleHandler output (a full-database clone bundle)
// until clean EOF.

// serveBundleStream drives the channel's bundle handler over conn.
// Called from serveOneShot after the topic prefix resolved c.
func serveBundleStream(c *Channel, conn net.Conn) {
	c.mu.Lock()
	h := c.bundleH
	c.mu.Unlock()
	if h == nil {
		_ = writeStatus(conn, StatusNoHandler)
		return
	}
	// Bundle stream is server-driven; clear the read deadline so a
	// slow client doesn't trip the handshake timeout mid-stream.
	_ = conn.SetReadDeadline(time.Time{})
	if err := writeStatus(conn, StatusOK); err != nil {
		return
	}
	defer c.breakOnClose(conn)()
	_ = h(conn)
}

// FetchBundle dials the endpoint URL's address, writes a clone
// request for the URL's ?topic= value, reads the status byte, and
// copies the payload stream into w. The URL form is
// "tcp://host:port?topic=…" or "unix:///abs/path?topic=…". For a
// TLS-configured mesh, use its channel's Fetcher method.
func FetchBundle(ctx context.Context, rawURL string, w io.Writer) error {
	addr, topic, err := ParseEndpointURL(rawURL)
	if err != nil {
		return err
	}
	return fetchBundleAddrTopic(ctx, addr, topic, w, nil)
}

func fetchBundleAddrTopic(ctx context.Context, addr, topic string, w io.Writer, tlsCfg *tls.Config) error {
	conn, err := dialOp(ctx, addr, topic, opBundleStream, tlsCfg, opDialTimeout, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	// The stream may legitimately take longer than the handshake
	// bound (a large database over a slow link); no overall deadline.
	_ = conn.SetDeadline(time.Time{})
	if _, err := io.Copy(w, conn); err != nil {
		return fmt.Errorf("tcpmesh: read bundle stream: %w", err)
	}
	return nil
}
