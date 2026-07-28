// Stream transport for the schema log. Length-prefixed binary frames
// run over one reliable net.Conn per client; ListenTCP and DialTCP are
// the standard wrappers.
// Only ErrHeadMoved and ErrBelowHorizon round-trip as typed status
// bytes; every other error flattens to a string. No TLS, auth, mux,
// or failover — see docs/SCHEMA.md for the operating model.

package schemalog

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	// maxFrameSize bounds the body of a single request or response.
	// 64 MiB comfortably accommodates a Read batch of 64 events with
	// multi-megabyte SQL bodies, while preventing a hostile peer from
	// forcing a giant allocation off a single length prefix.
	maxFrameSize = 64 << 20

	// callTimeout caps any single round-trip when the caller's ctx has
	// no deadline of its own. The producer's DDL admission runs on the
	// SQLite writer thread; an unbounded wait would wedge it.
	callTimeout = 30 * time.Second

	// minRedialInterval throttles reconnect attempts after a transport
	// failure. The broker's catchup loop fires every 500ms by default,
	// so without this a dead leader gets hammered four-plus times a
	// second per follower.
	minRedialInterval = 1 * time.Second
)

// Wire opcodes (request).
const (
	opAppend byte = 1
	opRead   byte = 2
	opHead   byte = 3
)

// Wire status codes (response).
const (
	statusOK              byte = 0
	statusErrHeadMoved    byte = 1
	statusErrBelowHorizon byte = 2
	statusErrString       byte = 3
)

// StreamClient is a Log backed by a remote Serve endpoint. Lazy-connect,
// redial after errors with minRedialInterval throttle. Safe for
// concurrent use; calls serialize on the single underlying conn.
type StreamClient struct {
	name string
	dial func(context.Context) (net.Conn, error)

	// closeCtx wakes the deadline-watcher goroutine when Close fires
	// while a call is mid-flight.
	closeCtx    context.Context
	cancelClose context.CancelFunc

	mu       sync.Mutex
	conn     net.Conn
	lastDial time.Time
}

// DialTCP validates addr and returns a client that lazily connects on
// the first call; address typos surface here rather than at first DDL.
func DialTCP(addr string) (*StreamClient, error) {
	if _, err := net.ResolveTCPAddr("tcp", addr); err != nil {
		return nil, fmt.Errorf("schemalog: resolve %q: %w", addr, err)
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	return DialFunc("tcp:"+addr, func(ctx context.Context) (net.Conn, error) {
		return d.DialContext(ctx, "tcp", addr)
	})
}

// DialFunc constructs a schema-log client over an embedder-supplied
// reliable stream dialer. name is used in transport errors.
func DialFunc(name string, dial func(context.Context) (net.Conn, error)) (*StreamClient, error) {
	if dial == nil {
		return nil, errors.New("schemalog: nil dial function")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &StreamClient{
		name:        name,
		dial:        dial,
		closeCtx:    ctx,
		cancelClose: cancel,
	}, nil
}

// Close releases the conn and blocks future reconnects. Idempotent.
func (c *StreamClient) Close() error {
	c.cancelClose()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *StreamClient) Append(ctx context.Context, parentSeq uint64, op []byte, raw string) (uint64, error) {
	body := make([]byte, 1+8+4+4+len(op)+len(raw))
	body[0] = opAppend
	binary.BigEndian.PutUint64(body[1:9], parentSeq)
	binary.BigEndian.PutUint32(body[9:13], uint32(len(op)))
	binary.BigEndian.PutUint32(body[13:17], uint32(len(raw)))
	copy(body[17:17+len(op)], op)
	copy(body[17+len(op):], raw)

	rspBody, status, err := c.call(ctx, body)
	if err != nil {
		return 0, err
	}
	switch status {
	case statusOK:
		if len(rspBody) < 8 {
			return 0, fmt.Errorf("schemalog: short Append response (%d bytes)", len(rspBody))
		}
		return binary.BigEndian.Uint64(rspBody[:8]), nil
	case statusErrHeadMoved:
		return 0, ErrHeadMoved
	case statusErrString:
		return 0, errors.New(string(rspBody))
	default:
		return 0, fmt.Errorf("schemalog: unexpected Append status %d", status)
	}
}

func (c *StreamClient) Read(ctx context.Context, fromSeq uint64, limit int) ([]Event, error) {
	if limit < 0 {
		limit = 0
	}
	body := make([]byte, 1+8+4)
	body[0] = opRead
	binary.BigEndian.PutUint64(body[1:9], fromSeq)
	binary.BigEndian.PutUint32(body[9:13], uint32(limit))

	rspBody, status, err := c.call(ctx, body)
	if err != nil {
		return nil, err
	}
	switch status {
	case statusOK:
		return decodeEventList(rspBody)
	case statusErrBelowHorizon:
		return nil, ErrBelowHorizon
	case statusErrString:
		return nil, errors.New(string(rspBody))
	default:
		return nil, fmt.Errorf("schemalog: unexpected Read status %d", status)
	}
}

func (c *StreamClient) Head(ctx context.Context) (uint64, error) {
	rspBody, status, err := c.call(ctx, []byte{opHead})
	if err != nil {
		return 0, err
	}
	switch status {
	case statusOK:
		if len(rspBody) < 8 {
			return 0, fmt.Errorf("schemalog: short Head response (%d bytes)", len(rspBody))
		}
		return binary.BigEndian.Uint64(rspBody[:8]), nil
	case statusErrString:
		return 0, errors.New(string(rspBody))
	default:
		return 0, fmt.Errorf("schemalog: unexpected Head status %d", status)
	}
}

// call serializes one round-trip on the single conn. Cancellation jams
// the conn's deadline; the next call redials.
func (c *StreamClient) call(ctx context.Context, req []byte) (body []byte, status byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.closeCtx.Err(); err != nil {
		return nil, 0, fmt.Errorf("schemalog: client closed: %w", err)
	}
	if err := c.ensureConnLocked(ctx); err != nil {
		return nil, 0, err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(callTimeout)
	}
	_ = c.conn.SetDeadline(deadline)

	// Capture the conn value: the goroutine may be scheduled after call
	// returns and Close nils c.conn (select picks randomly among ready
	// cases, so <-done being closed does not stop it choosing a Done
	// branch). SetDeadline on an already-closed conn just errors.
	conn := c.conn
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Unix(1, 0))
		case <-c.closeCtx.Done():
			_ = conn.SetDeadline(time.Unix(1, 0))
		case <-done:
		}
	}()

	if err := writeFrame(c.conn, req); err != nil {
		c.dropConnLocked()
		return nil, 0, fmt.Errorf("schemalog: write: %w", joinCtxErr(ctx, err))
	}
	rsp, err := readFrame(c.conn)
	if err != nil {
		c.dropConnLocked()
		return nil, 0, fmt.Errorf("schemalog: read: %w", joinCtxErr(ctx, err))
	}
	if len(rsp) < 1 {
		c.dropConnLocked()
		return nil, 0, errors.New("schemalog: empty response frame")
	}
	return rsp[1:], rsp[0], nil
}

func (c *StreamClient) ensureConnLocked(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	if wait := minRedialInterval - time.Since(c.lastDial); wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closeCtx.Done():
			return c.closeCtx.Err()
		}
	}
	c.lastDial = time.Now()
	conn, err := c.dial(ctx)
	if err != nil {
		return fmt.Errorf("schemalog: dial %s: %w", c.name, err)
	}
	c.conn = conn
	return nil
}

func (c *StreamClient) dropConnLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

// joinCtxErr returns ctx.Err() if it's set (the network error was
// caused by our own deadline-jam after cancellation), otherwise the
// underlying network error.
func joinCtxErr(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	return err
}

// StreamServer hosts a backend Log over a stream listener. One goroutine
// per accepted conn; requests on each conn are handled sequentially.
// Cross-conn serialization comes from the backend's own mutex.
type StreamServer struct {
	backend  Log
	listener net.Listener

	closeCtx    context.Context
	cancelClose context.CancelFunc

	wg sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// ListenTCP starts a server that demuxes requests onto backend.
// Returns once the listener is bound; accept happens in a goroutine.
func ListenTCP(addr string, backend Log) (*StreamServer, error) {
	if backend == nil {
		return nil, errors.New("schemalog: ListenTCP: nil backend")
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("schemalog: listen %q: %w", addr, err)
	}
	return Serve(l, backend)
}

// Serve starts a schema-log server on an already-bound stream listener.
// The returned server owns and closes listener.
func Serve(listener net.Listener, backend Log) (*StreamServer, error) {
	if backend == nil {
		return nil, errors.New("schemalog: Serve: nil backend")
	}
	if listener == nil {
		return nil, errors.New("schemalog: Serve: nil listener")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &StreamServer{
		backend:     backend,
		listener:    listener,
		closeCtx:    ctx,
		cancelClose: cancel,
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr returns the bound listener address.
func (s *StreamServer) Addr() string { return s.listener.Addr().String() }

// Close stops accepting, closes the listener and all live conns, and
// waits for handler goroutines to drain. Idempotent.
func (s *StreamServer) Close() error {
	s.closeOnce.Do(func() {
		s.cancelClose()
		s.closeErr = s.listener.Close()
		s.wg.Wait()
	})
	return s.closeErr
}

func (s *StreamServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closeCtx.Err() != nil {
				return
			}
			// Transient accept errors back off briefly so a bad
			// descriptor doesn't spin the loop hot.
			select {
			case <-s.closeCtx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *StreamServer) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	// On Close, jam the deadline to unblock any in-flight read.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-s.closeCtx.Done():
			_ = conn.SetDeadline(time.Unix(1, 0))
		case <-done:
		}
	}()

	for {
		req, err := readFrame(conn)
		if err != nil {
			return
		}
		if len(req) < 1 {
			return
		}
		rsp := s.dispatch(req)
		if err := writeFrame(conn, rsp); err != nil {
			return
		}
	}
}

// dispatch returns one response frame body. The backend runs under
// closeCtx so Close unblocks any context-aware backend mid-handler.
func (s *StreamServer) dispatch(req []byte) []byte {
	op := req[0]
	body := req[1:]
	ctx := s.closeCtx

	switch op {
	case opAppend:
		if len(body) < 16 {
			return errStringResponse("Append: short request")
		}
		parentSeq := binary.BigEndian.Uint64(body[:8])
		opLen := binary.BigEndian.Uint32(body[8:12])
		rawLen := binary.BigEndian.Uint32(body[12:16])
		if uint64(16)+uint64(opLen)+uint64(rawLen) != uint64(len(body)) {
			return errStringResponse("Append: length mismatch")
		}
		opBytes := body[16 : 16+opLen]
		rawBytes := body[16+opLen:]
		seq, err := s.backend.Append(ctx, parentSeq, opBytes, string(rawBytes))
		switch {
		case err == nil:
			out := make([]byte, 1+8)
			out[0] = statusOK
			binary.BigEndian.PutUint64(out[1:], seq)
			return out
		case errors.Is(err, ErrHeadMoved):
			return []byte{statusErrHeadMoved}
		default:
			return errStringResponse(err.Error())
		}

	case opRead:
		if len(body) != 12 {
			return errStringResponse("Read: short request")
		}
		fromSeq := binary.BigEndian.Uint64(body[:8])
		limit := int(binary.BigEndian.Uint32(body[8:12]))
		evs, err := s.backend.Read(ctx, fromSeq, limit)
		switch {
		case err == nil:
			return append([]byte{statusOK}, encodeEventList(evs)...)
		case errors.Is(err, ErrBelowHorizon):
			return []byte{statusErrBelowHorizon}
		default:
			return errStringResponse(err.Error())
		}

	case opHead:
		head, err := s.backend.Head(ctx)
		if err != nil {
			return errStringResponse(err.Error())
		}
		out := make([]byte, 1+8)
		out[0] = statusOK
		binary.BigEndian.PutUint64(out[1:], head)
		return out
	}
	return errStringResponse(fmt.Sprintf("unknown op %d", op))
}

func errStringResponse(msg string) []byte {
	out := make([]byte, 1+len(msg))
	out[0] = statusErrString
	copy(out[1:], msg)
	return out
}

// Event wire layout: seq u64, parentSeq u64, opLen u32, rawLen u32,
// opBytes, rawBytes. Length prefixes use uint32 to match the
// frame-level cap. Shared by the TCP Read response and the S3 backend's
// per-object body.
const eventHeaderSize = 8 + 8 + 4 + 4

func eventEncodedSize(e Event) int {
	return eventHeaderSize + len(e.CatalogOp) + len(e.RawSQL)
}

func encodeEvent(out []byte, e Event) int {
	binary.BigEndian.PutUint64(out[0:8], e.SchemaSeq)
	binary.BigEndian.PutUint64(out[8:16], e.ParentSeq)
	binary.BigEndian.PutUint32(out[16:20], uint32(len(e.CatalogOp)))
	binary.BigEndian.PutUint32(out[20:24], uint32(len(e.RawSQL)))
	n := eventHeaderSize
	n += copy(out[n:], e.CatalogOp)
	n += copy(out[n:], e.RawSQL)
	return n
}

// decodeEvent reads one Event starting at body[off] and returns the
// new offset. CatalogOp is defensively copied so a caller can hold the
// Event past the lifetime of body.
func decodeEvent(body []byte, off int) (Event, int, error) {
	if len(body)-off < eventHeaderSize {
		return Event{}, off, errors.New("schemalog: truncated event header")
	}
	seq := binary.BigEndian.Uint64(body[off : off+8])
	parent := binary.BigEndian.Uint64(body[off+8 : off+16])
	opLen := int(binary.BigEndian.Uint32(body[off+16 : off+20]))
	rawLen := int(binary.BigEndian.Uint32(body[off+20 : off+24]))
	off += eventHeaderSize
	if len(body)-off < opLen+rawLen {
		return Event{}, off, errors.New("schemalog: truncated event body")
	}
	op := append([]byte(nil), body[off:off+opLen]...)
	off += opLen
	raw := string(body[off : off+rawLen])
	off += rawLen
	return Event{SchemaSeq: seq, ParentSeq: parent, CatalogOp: op, RawSQL: raw}, off, nil
}

func encodeEventList(evs []Event) []byte {
	size := 4
	for _, e := range evs {
		size += eventEncodedSize(e)
	}
	out := make([]byte, size)
	binary.BigEndian.PutUint32(out[:4], uint32(len(evs)))
	off := 4
	for _, e := range evs {
		off += encodeEvent(out[off:], e)
	}
	return out
}

func decodeEventList(body []byte) ([]Event, error) {
	if len(body) < 4 {
		return nil, errors.New("schemalog: short event list")
	}
	n := binary.BigEndian.Uint32(body[:4])
	off := 4
	out := make([]Event, 0, n)
	for i := uint32(0); i < n; i++ {
		ev, next, err := decodeEvent(body, off)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
		off = next
	}
	return out, nil
}

func writeFrame(w io.Writer, body []byte) error {
	if len(body) > maxFrameSize {
		return fmt.Errorf("schemalog: frame too large (%d > %d)", len(body), maxFrameSize)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// readFrame rejects oversize prefixes before allocating the body.
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrameSize {
		return nil, fmt.Errorf("schemalog: oversize frame (%d > %d)", n, maxFrameSize)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}
