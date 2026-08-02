package unique

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Registry proxy: a host-side server that fronts an existing Registry on a
// plain stream listener, and a client Registry that reaches it over an
// embedder-supplied dialer. It exists for secondary producers that cannot
// resolve the reservation backend themselves — no object-storage access to
// read the lease and no mesh membership to dial the leaseholder — but do
// have a reliable stream to a co-located node that has both (the same
// situation the schema-log dial serves). The proxy node forwards Reserve
// through its own Registry, so a proxied claim is arbitrated exactly like
// one of the node's own.
//
// The proxy carries claims, not lease state: generation handling, handover
// retries, and leader routing all stay inside the proxying node's client.
// Every transport or backend failure surfaces as ErrUnavailable — retryable
// by contract, never a silent conflict.

// proxyServiceName is the net/rpc service the proxy server registers,
// distinct from the leaseholder's lease-scoped "Unique" service: proxy
// requests carry no generation and are re-arbitrated by the server's own
// Registry.
const proxyServiceName = "UniqueProxy"

// proxyCallTimeout caps a single proxied round-trip when the caller's ctx
// carries no deadline. The commit hook's retry loop only re-checks its
// budget between attempts, so an unbounded call here would wedge the
// SQLite writer thread behind a dead peer.
const proxyCallTimeout = 10 * time.Second

// proxyRedialInterval throttles reconnect attempts after a transport
// failure so a retrying commit hook does not hammer a dead endpoint.
const proxyRedialInterval = 250 * time.Millisecond

// ProxyReserveArgs is the proxied Reserve request: the claims, verbatim.
type ProxyReserveArgs struct {
	Claims []Claim
}

// ProxyReserveReply is the proxied Reserve response. Unavailable maps the
// backend's non-nil error (ErrUnavailable by the Registry contract) across
// the wire with its cause; OK/Conflict mirror Registry.Reserve.
type ProxyReserveReply struct {
	OK          bool
	Conflict    *Claim
	Unavailable bool
	Cause       string
}

// ProxyReleaseArgs is the proxied Release request.
type ProxyReleaseArgs struct {
	Claims []Claim
}

// ProxyReleaseReply is the (empty) proxied Release response. Release is
// advisory, so backend errors are dropped server-side rather than carried.
type ProxyReleaseReply struct{}

// proxyRPC is the server-side net/rpc receiver.
type proxyRPC struct {
	reg Registry
	ctx context.Context // server close ctx; unblocks backend calls on Close
}

func (p *proxyRPC) Reserve(args ProxyReserveArgs, reply *ProxyReserveReply) error {
	ok, conflict, err := p.reg.Reserve(p.ctx, args.Claims)
	if err != nil {
		reply.Unavailable = true
		reply.Cause = err.Error()
		return nil
	}
	reply.OK, reply.Conflict = ok, conflict
	return nil
}

func (p *proxyRPC) Release(args ProxyReleaseArgs, _ *ProxyReleaseReply) error {
	_ = p.reg.Release(p.ctx, args.Claims)
	return nil
}

// ProxyServer fronts a Registry on a stream listener. One goroutine per
// accepted conn; net/rpc handles per-conn request framing.
type ProxyServer struct {
	srv      *rpc.Server
	listener net.Listener

	closeCtx    context.Context
	cancelClose context.CancelFunc

	wg sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}

	closeOnce sync.Once
	closeErr  error
}

// ServeProxy serves reg on an already-bound listener. The returned server
// owns and closes the listener.
func ServeProxy(listener net.Listener, reg Registry) (*ProxyServer, error) {
	if reg == nil {
		return nil, errors.New("unique: ServeProxy: nil registry")
	}
	if listener == nil {
		return nil, errors.New("unique: ServeProxy: nil listener")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &ProxyServer{
		srv:         rpc.NewServer(),
		listener:    listener,
		closeCtx:    ctx,
		cancelClose: cancel,
		conns:       map[net.Conn]struct{}{},
	}
	if err := s.srv.RegisterName(proxyServiceName, &proxyRPC{reg: reg, ctx: ctx}); err != nil {
		cancel()
		return nil, fmt.Errorf("unique: register proxy service: %w", err)
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr returns the bound listener address.
func (s *ProxyServer) Addr() string { return s.listener.Addr().String() }

// Close stops accepting, closes every live conn, and waits for handler
// goroutines to drain. Idempotent.
func (s *ProxyServer) Close() error {
	s.closeOnce.Do(func() {
		s.cancelClose()
		s.closeErr = s.listener.Close()
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s.closeErr
}

func (s *ProxyServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closeCtx.Err() != nil {
				return
			}
			select {
			case <-s.closeCtx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.srv.ServeConn(conn)
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
			_ = conn.Close()
		}()
	}
}

// ProxyClient is a Registry backed by a remote ProxyServer. Lazy-connect,
// redial after errors with proxyRedialInterval throttle; safe for
// concurrent use. Every failure to reach the server or a server-reported
// backend outage returns ErrUnavailable with the cause attached, so the
// commit hook's retry loop treats it as transient, never as a conflict.
type ProxyClient struct {
	name string
	dial func(context.Context) (net.Conn, error)

	closeCtx    context.Context
	cancelClose context.CancelFunc

	mu       sync.Mutex
	conn     *rpc.Client
	lastDial time.Time
}

// NewProxyClient returns a Registry that reaches a ProxyServer via dial.
// name labels transport errors.
func NewProxyClient(name string, dial func(context.Context) (net.Conn, error)) (*ProxyClient, error) {
	if dial == nil {
		return nil, errors.New("unique: NewProxyClient: nil dial function")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ProxyClient{
		name:        name,
		dial:        dial,
		closeCtx:    ctx,
		cancelClose: cancel,
	}, nil
}

// Reserve implements Registry by forwarding the claims to the proxy server.
func (c *ProxyClient) Reserve(ctx context.Context, claims []Claim) (bool, *Claim, error) {
	if len(claims) == 0 {
		return true, nil, nil
	}
	var reply ProxyReserveReply
	if err := c.callProxy(ctx, "Reserve", ProxyReserveArgs{Claims: claims}, &reply); err != nil {
		return false, nil, fmt.Errorf("%w: reserve via %s: %v", ErrUnavailable, c.name, err)
	}
	if reply.Unavailable {
		return false, nil, fmt.Errorf("%w: %s: %s", ErrUnavailable, c.name, reply.Cause)
	}
	return reply.OK, reply.Conflict, nil
}

// Release implements Registry by forwarding the claims. Release is
// advisory, so transport failures are reported but need no retry.
func (c *ProxyClient) Release(ctx context.Context, claims []Claim) error {
	if len(claims) == 0 {
		return nil
	}
	var reply ProxyReleaseReply
	if err := c.callProxy(ctx, "Release", ProxyReleaseArgs{Claims: claims}, &reply); err != nil {
		return fmt.Errorf("%w: release via %s: %v", ErrUnavailable, c.name, err)
	}
	return nil
}

// Close drops the cached connection and blocks future reconnects.
// Idempotent.
func (c *ProxyClient) Close() error {
	c.cancelClose()
	c.reset()
	return nil
}

// callProxy runs one RPC with a bounded deadline, dropping the cached
// conn on any failure so the next call redials.
func (c *ProxyClient) callProxy(ctx context.Context, method string, args, reply any) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, proxyCallTimeout)
		defer cancel()
	}
	conn, err := c.connect(ctx)
	if err != nil {
		return err
	}
	done := conn.Go(proxyServiceName+"."+method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-ctx.Done():
		// The call may still be in flight on a healthy conn, but with no
		// way to tell that from a wedged peer, drop the conn so the next
		// attempt starts clean.
		c.reset()
		return ctx.Err()
	case <-c.closeCtx.Done():
		return errors.New("client closed")
	case res := <-done.Done:
		if res.Error != nil {
			c.reset()
		}
		return res.Error
	}
}

// connect returns the cached conn or dials a fresh one, throttled by
// proxyRedialInterval after a drop.
func (c *ProxyClient) connect(ctx context.Context) (*rpc.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.closeCtx.Err(); err != nil {
		return nil, errors.New("client closed")
	}
	if c.conn != nil {
		return c.conn, nil
	}
	if wait := proxyRedialInterval - time.Since(c.lastDial); wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closeCtx.Done():
			return nil, errors.New("client closed")
		}
	}
	c.lastDial = time.Now()
	netConn, err := c.dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial: %v", err)
	}
	c.conn = rpc.NewClient(netConn)
	return c.conn, nil
}

// reset drops the cached conn so the next call redials.
func (c *ProxyClient) reset() {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
}
