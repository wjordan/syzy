package unique

import (
	"context"
	"fmt"
	"net"
)

// ServeTransport is the leaseholder's side of the reservation-RPC carrier.
// Serve registers a per-connection handler and returns the address peers
// publish/dial to reach it (written into the lease record's Addr). The
// returned address MUST be reachable by every node whose LeaseClient may
// route to this leaseholder — a loopback address only works in-process.
//
// A mesh channel satisfies this over the already-peer-connected,
// firewall-open mesh, so a clustered leaseholder needs no extra port. The
// default (loopbackTransport) binds a private loopback listener and is for
// single-node / in-process use only.
type ServeTransport interface {
	// Serve registers handler, invoked once per accepted reservation-RPC
	// connection (the leaseholder drives net/rpc over it), and returns the
	// peer-dialable address to publish.
	Serve(handler func(net.Conn)) (addr string, err error)
	// Close stops serving. Safe to call more than once.
	Close() error
}

// DialTransport is the client's side: dial the leaseholder published at
// addr and return a connection the LeaseClient runs net/rpc over. It must
// understand the same addr form ServeTransport.Serve returns.
type DialTransport interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// TransportProvider is the optional capability a syzy transport advertises
// to carry reservation RPCs over the mesh. sqlite.Open type-asserts
// cfg.Transport to it (mirroring transport.CatchupRegistrar): when present,
// the leaseholder and LeaseClient route over the mesh instead of the
// loopback default, so cross-node reservation works. *mux.Channel
// satisfies it. The returned Serve and Dial MUST be a matched pair (the
// address Serve publishes is one Dial understands).
type TransportProvider interface {
	UniqueServeTransport() ServeTransport
	UniqueDialTransport() DialTransport
}

// loopbackTransport is the default carrier: a private loopback net/rpc
// listener on the server side and a plain TCP dial on the client side. It
// preserves the pre-mesh behavior and is correct only when client and
// server share a process (the published 127.0.0.1 address is not
// peer-reachable). Single-node sqlite.Open and same-process tests use it.
type loopbackTransport struct {
	addr string // bind address; "" => 127.0.0.1:0

	ln net.Listener
}

// newLoopbackServe returns a ServeTransport binding addr ("" => 127.0.0.1:0).
func newLoopbackServe(addr string) *loopbackTransport {
	return &loopbackTransport{addr: addr}
}

func (t *loopbackTransport) Serve(handler func(net.Conn)) (string, error) {
	addr := t.addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("unique: leaseholder listen: %w", err)
	}
	t.ln = ln
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go handler(conn)
		}
	}()
	return ln.Addr().String(), nil
}

func (t *loopbackTransport) Close() error {
	if t.ln != nil {
		return t.ln.Close()
	}
	return nil
}

// loopbackDial is the default DialTransport: a plain TCP dial honoring ctx.
type loopbackDial struct{}

func (loopbackDial) Dial(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}
