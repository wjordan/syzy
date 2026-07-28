// Package vsock provides a cross-kernel wake transport for syzy
// journals. A producer in one kernel (typically a guest VM) sends one
// byte per published record over a long-lived connection; a consumer
// in another kernel (the host syzy daemon) accepts those connections,
// reads a per-producer hello identifying the origin, and dispatches
// each subsequent byte to a Waiter registered for that origin.
//
// The transport is "vsock" in spirit — a typical host runtime wires
// the host-side listener as a Unix socket bridged to AF_VSOCK by
// cloud-hypervisor — but this package operates against any
// net.Listener / dial function.
// Tests use AF_UNIX pairs; production uses AF_VSOCK via CH's
// per-port Unix-socket bridge.
package vsock

import (
	"bufio"
	"context"
	"net"
	"sync"
	"time"

	"github.com/wjordan/syzy/internal/futex"
	"github.com/wjordan/syzy/wake"
)

// helloMaxLen caps the producer's hello line. Origin hex is 16 bytes
// (one uint64) → 32 ASCII chars plus the newline; round up for
// breathing room and to catch malformed clients.
const helloMaxLen = 64

// NewWaker dials addr lazily on the first Wake call and sends one
// byte per Wake over the resulting connection. originHex identifies
// this producer in the listener's per-origin Waiter table; the first
// byte sent after dial is the hello "<originHex>\n".
//
// Errors during Wake reset the connection so the next Wake redials.
// All errors are swallowed: durability is the journal's job, this is
// only a notification optimization.
func NewWaker(dial func() (net.Conn, error), originHex string) wake.Waker {
	return &waker{dial: dial, hello: append([]byte(originHex), '\n')}
}

type waker struct {
	dial  func() (net.Conn, error)
	hello []byte

	mu     sync.Mutex
	conn   net.Conn
	closed bool
}

func (w *waker) Wake(_ *uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	if w.conn == nil {
		c, err := w.dial()
		if err != nil {
			return
		}
		if _, err := c.Write(w.hello); err != nil {
			_ = c.Close()
			return
		}
		w.conn = c
	}
	if _, err := w.conn.Write([]byte{1}); err != nil {
		_ = w.conn.Close()
		w.conn = nil
	}
}

func (w *waker) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	if w.conn != nil {
		err := w.conn.Close()
		w.conn = nil
		return err
	}
	return nil
}

// Listener accepts producer connections, reads each one's hello, and
// dispatches subsequent wake bytes to the Waiter registered for that
// origin. One Listener per syzy daemon (one host-side endpoint shared
// by every origin that wakes via this transport).
type Listener struct {
	ln net.Listener

	mu      sync.Mutex
	waiters map[string]*chanWaiter // origin hex → waiter
	conns   map[net.Conn]struct{}  // active per-conn serve goroutines
	closed  bool
	wg      sync.WaitGroup
}

// NewListener starts the accept loop on ln. Close stops it; callers
// own ln's lifecycle (NewListener does not close it explicitly, but
// Close calls ln.Close to unblock Accept).
func NewListener(ln net.Listener) *Listener {
	l := &Listener{
		ln:      ln,
		waiters: map[string]*chanWaiter{},
		conns:   map[net.Conn]struct{}{},
	}
	l.wg.Add(1)
	go l.acceptLoop()
	return l
}

// Register returns a Waiter that observes wake bytes from any
// connection whose hello announces originHex. Repeated Register calls
// for the same originHex return the same Waiter; the daemon owns
// origin lifecycle via Unregister.
//
// After Close, Register returns a dead-on-arrival Waiter (Wait
// returns ErrTimeout once and Close is a no-op) so an unlucky
// caller observing a race with shutdown doesn't panic.
func (l *Listener) Register(originHex string) wake.Waiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.waiters == nil {
		dead := newChanWaiter()
		_ = dead.Close()
		return dead
	}
	if w, ok := l.waiters[originHex]; ok {
		return w
	}
	w := newChanWaiter()
	l.waiters[originHex] = w
	return w
}

// Unregister removes the Waiter for originHex. Connections from
// producers announcing the unregistered origin will be dropped on
// next accept. Idempotent.
func (l *Listener) Unregister(originHex string) {
	l.mu.Lock()
	w, ok := l.waiters[originHex]
	if ok {
		delete(l.waiters, originHex)
	}
	l.mu.Unlock()
	if ok {
		_ = w.Close()
	}
}

// Close stops the accept loop, closes all open connections, and
// closes all registered Waiters. Idempotent.
func (l *Listener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	waiters := l.waiters
	conns := l.conns
	l.waiters = nil
	l.conns = nil
	l.mu.Unlock()

	err := l.ln.Close()
	// Unblock serve goroutines parked on conn.Read by closing their
	// connections; each goroutine cleans up via its own defer.
	for c := range conns {
		_ = c.Close()
	}
	for _, w := range waiters {
		_ = w.Close()
	}
	l.wg.Wait()
	return err
}

func (l *Listener) acceptLoop() {
	defer l.wg.Done()
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			return
		}
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			_ = conn.Close()
			return
		}
		l.conns[conn] = struct{}{}
		l.mu.Unlock()
		l.wg.Add(1)
		go l.serve(conn)
	}
}

func (l *Listener) serve(conn net.Conn) {
	defer l.wg.Done()
	defer func() {
		_ = conn.Close()
		l.mu.Lock()
		if l.conns != nil {
			delete(l.conns, conn)
		}
		l.mu.Unlock()
	}()

	// Read hello: a single line "<originHex>\n", at most helloMaxLen
	// bytes. Bounded to prevent a malicious client from forcing
	// unlimited buffering.
	r := bufio.NewReaderSize(conn, helloMaxLen)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err == nil {
		defer conn.SetReadDeadline(time.Time{})
	}
	line, err := r.ReadSlice('\n')
	if err != nil {
		return
	}
	if len(line) <= 1 || len(line) > helloMaxLen {
		return
	}
	origin := string(line[:len(line)-1])

	l.mu.Lock()
	w, ok := l.waiters[origin]
	l.mu.Unlock()
	if !ok {
		return
	}

	// Clear the read deadline so subsequent reads block.
	_ = conn.SetReadDeadline(time.Time{})

	var buf [64]byte
	for {
		n, err := r.Read(buf[:])
		if n > 0 {
			w.signal()
		}
		if err != nil {
			return
		}
	}
}

// chanWaiter is the consumer side of a single origin's wake stream.
// Wait blocks on signal or timeout.
type chanWaiter struct {
	ch     chan struct{}
	mu     sync.Mutex
	closed bool
}

func newChanWaiter() *chanWaiter {
	return &chanWaiter{ch: make(chan struct{}, 1)}
}

// signal pokes Wait. Non-blocking: a pending signal is enough (the
// drainer will scan to tail, so coalescing many wakes into one wake
// loses nothing). Drops if Closed.
func (w *chanWaiter) signal() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	select {
	case w.ch <- struct{}{}:
	default:
	}
}

func (w *chanWaiter) Wait(ctx context.Context, _ *uint32, _ uint32, timeout time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case _, ok := <-w.ch:
		if !ok {
			// Channel closed: treat as a timeout so the drainer's
			// loop re-checks the journal tail. Returning nil would
			// falsely signal a wake.
			return futex.ErrTimeout
		}
		return nil
	case <-time.After(timeout):
		return futex.ErrTimeout
	}
}

// Close unblocks any in-flight Wait by closing the wake channel.
// Subsequent signal() calls become no-ops. Idempotent.
func (w *chanWaiter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	close(w.ch)
	return nil
}

// Ensure interface satisfaction at compile time.
var (
	_ wake.Waker    = (*waker)(nil)
	_ wake.Waiter   = (*chanWaiter)(nil)
	_ wake.Listener = (*Listener)(nil)
)
