package unique

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// A proxy lets a secondary producer reach the Registry owned by a
// co-located node without learning lease generations or joining the peer
// mesh. Calls use separate streams: reservations already pay a synchronous
// coordination round trip, and avoiding a cached connection keeps
// cancellation, reconnect, and request-size accounting unambiguous.
const (
	proxyServiceName     = "UniqueProxy"
	proxyCallTimeout     = 2 * time.Second
	maxProxyRequestBytes = 64 << 20
)

type ProxyReserveReply struct {
	OK          bool
	Conflict    *Claim
	Unavailable string
}

type proxyRPC struct {
	reg Registry
	ctx context.Context
}

func (p *proxyRPC) Reserve(args ReserveArgs, reply *ProxyReserveReply) error {
	ctx, cancel := context.WithTimeout(p.ctx, proxyCallTimeout)
	defer cancel()
	ok, conflict, err := p.reg.Reserve(ctx, args.Claims)
	if err != nil {
		reply.Unavailable = err.Error()
		return nil
	}
	reply.OK, reply.Conflict = ok, conflict
	return nil
}

func (p *proxyRPC) Release(claims []Claim, reply *string) error {
	ctx, cancel := context.WithTimeout(p.ctx, proxyCallTimeout)
	defer cancel()
	if err := p.reg.Release(ctx, claims); err != nil {
		*reply = err.Error()
	}
	return nil
}

// ProxyServer fronts a Registry on a reliable stream listener.
type ProxyServer struct {
	srv      *rpc.Server
	listener net.Listener

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// ServeProxy serves reg on listener. The returned server owns listener.
func ServeProxy(listener net.Listener, reg Registry) (*ProxyServer, error) {
	if listener == nil {
		return nil, errors.New("unique: ServeProxy: nil listener")
	}
	if reg == nil {
		return nil, errors.New("unique: ServeProxy: nil registry")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &ProxyServer{
		srv:      rpc.NewServer(),
		listener: listener,
		ctx:      ctx,
		cancel:   cancel,
		conns:    make(map[net.Conn]struct{}),
	}
	if err := s.srv.RegisterName(proxyServiceName, &proxyRPC{reg: reg, ctx: ctx}); err != nil {
		cancel()
		return nil, fmt.Errorf("unique: register proxy service: %w", err)
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

func (s *ProxyServer) Addr() string { return s.listener.Addr().String() }

// Close stops accepting, closes live streams, and waits for handlers.
func (s *ProxyServer) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.mu.Lock()
		s.closed = true
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.closeErr = s.listener.Close()
		s.wg.Wait()
	})
	return s.closeErr
}

func (s *ProxyServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(conn)
	}
}

func (s *ProxyServer) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()
	_ = conn.SetReadDeadline(time.Now().Add(proxyCallTimeout))
	s.srv.ServeConn(struct {
		io.Reader
		io.Writer
		io.Closer
	}{Reader: io.LimitReader(conn, maxProxyRequestBytes), Writer: conn, Closer: conn})
}

// ProxyClient is a Registry backed by a remote ProxyServer.
type ProxyClient struct {
	name   string
	dial   func(context.Context) (net.Conn, error)
	closed atomic.Bool
}

func NewProxyClient(name string, dial func(context.Context) (net.Conn, error)) (*ProxyClient, error) {
	if dial == nil {
		return nil, errors.New("unique: NewProxyClient: nil dial function")
	}
	return &ProxyClient{name: name, dial: dial}, nil
}

// Probe verifies that the configured stream serves the proxy protocol.
func (c *ProxyClient) Probe(ctx context.Context) error {
	return c.call(ctx, "Reserve", ReserveArgs{}, &ProxyReserveReply{})
}

func (c *ProxyClient) Reserve(ctx context.Context, claims []Claim) (bool, *Claim, error) {
	if len(claims) == 0 {
		return true, nil, nil
	}
	var reply ProxyReserveReply
	if err := c.call(ctx, "Reserve", ReserveArgs{Claims: claims}, &reply); err != nil {
		return false, nil, fmt.Errorf("%w: reserve via %s: %v", ErrUnavailable, c.name, err)
	}
	if reply.Unavailable != "" {
		return false, nil, fmt.Errorf("%w: %s: %s", ErrUnavailable, c.name, reply.Unavailable)
	}
	return reply.OK, reply.Conflict, nil
}

func (c *ProxyClient) Release(ctx context.Context, claims []Claim) error {
	if len(claims) == 0 {
		return nil
	}
	var unavailable string
	if err := c.call(ctx, "Release", claims, &unavailable); err != nil {
		return fmt.Errorf("%w: release via %s: %v", ErrUnavailable, c.name, err)
	}
	if unavailable != "" {
		return fmt.Errorf("%w: %s: %s", ErrUnavailable, c.name, unavailable)
	}
	return nil
}

func withProxyDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, proxyCallTimeout)
}

func (c *ProxyClient) call(ctx context.Context, method string, args, reply any) error {
	if c.closed.Load() {
		return errors.New("client closed")
	}
	ctx, cancel := withProxyDeadline(ctx)
	defer cancel()
	conn, err := c.dial(ctx)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	done := client.Go(proxyServiceName+"."+method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-ctx.Done():
		_ = client.Close()
		return ctx.Err()
	case result := <-done.Done:
		return result.Error
	}
}

func (c *ProxyClient) Close() error {
	c.closed.Store(true)
	return nil
}
