package ctrlsock

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wjordan/syzy/internal/buildinfo"
)

// HelloMsg is the only client→server message in v1. Sent immediately
// after connect; the server validates db_path matches its own and
// replies with HelloAck.
type HelloMsg struct {
	Type   string `json:"type"` // must be "hello"
	DBPath string `json:"db_path,omitempty"`
	// Version is the client's buildinfo.Version. The CLI and the
	// loadable extension are two artifacts of one build that share the
	// journal layout and this protocol, and the extension auto-spawns
	// the CLI's daemon from $PATH — so a half-upgraded pair is easy to
	// produce. Both sides compare and refuse a mismatch.
	Version string `json:"version,omitempty"`
}

// HelloAck is the server's reply. Type is "hello" on success or "error"
// with Msg populated on failure. BundleAddr, when set, advertises the
// daemon's bundle endpoint as a syzy.Restore-compatible source URL
// (e.g. "tcp://host:port" or "unix:/path") so a co-located `syzy clone`
// can route through the running daemon instead of failing the local
// streaming path.
type HelloAck struct {
	Type       string `json:"type"`
	Origin     string `json:"origin,omitempty"`
	ClusterID  string `json:"cluster_id,omitempty"`
	BundleAddr string `json:"bundle_addr,omitempty"`
	Version    string `json:"version,omitempty"`
	Msg        string `json:"msg,omitempty"`
}

// Request is a post-hello client→server message. Type names the op;
// v1 has one, "wait".
type Request struct {
	Type      string `json:"type"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

// Response is the reply to a Request: Type echoes the op on success,
// or is "error" with Msg populated.
type Response struct {
	Type string `json:"type"`
	Msg  string `json:"msg,omitempty"`
}

// WaitFunc blocks until the database has applied everything its peers
// held when the call began, or ctx expires.
type WaitFunc func(ctx context.Context) error

// Server is the daemon side of the control socket. It accepts client
// connections, validates each via the hello handshake, tracks the live
// client count, and reports idle status.
type Server struct {
	dbPath     string
	origin     string
	clusterID  string
	bundleAddr string

	listener net.Listener
	clients  atomic.Int64
	lastIdle atomic.Int64 // unix nano of last transition to clients==0
	closing  atomic.Bool
	wg       sync.WaitGroup

	waitFn atomic.Pointer[WaitFunc]
}

// SetWaitFunc installs the handler for the "wait" op. Unset, the op
// reports that this daemon cannot serve it.
func (s *Server) SetWaitFunc(fn WaitFunc) { s.waitFn.Store(&fn) }

// Listen binds the per-DB socket and returns a Server ready to accept
// clients. cleanup removes the socket file at the path. bundleAddr
// is advertised in HelloAck so co-located CLI tools can route through
// the daemon's bundle endpoint; pass "" when the daemon has no
// clone endpoint.
func Listen(dbPath, origin, clusterID, bundleAddr string) (*Server, error) {
	path, err := SocketPath(dbPath)
	if err != nil {
		return nil, err
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("ctrlsock: listen %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("ctrlsock: chmod %s: %w", path, err)
	}
	s := &Server{
		dbPath:     dbPath,
		origin:     origin,
		clusterID:  clusterID,
		bundleAddr: bundleAddr,
		listener:   ln,
	}
	s.lastIdle.Store(time.Now().UnixNano())
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr returns the bound socket path.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Clients returns the current count of attached clients.
func (s *Server) Clients() int64 { return s.clients.Load() }

// IdleSince returns the moment the client count last hit zero. When
// clients > 0, returns the zero time. Use to drive idle-exit timing.
func (s *Server) IdleSince() time.Time {
	if s.clients.Load() > 0 {
		return time.Time{}
	}
	return time.Unix(0, s.lastIdle.Load())
}

// Close stops accepting and waits for any in-flight handlers. Idempotent.
func (s *Server) Close() error {
	if !s.closing.CompareAndSwap(false, true) {
		return nil
	}
	err := s.listener.Close()
	s.wg.Wait()
	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closing.Load() {
				return
			}
			// Transient accept error — back off briefly.
			time.Sleep(10 * time.Millisecond)
			continue
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	// Bound the hello deadline so a buggy or malicious client can't
	// pin a connection slot indefinitely without committing.
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return
	}
	r := bufio.NewReader(conn)
	line, err := r.ReadBytes('\n')
	if err != nil {
		return
	}
	var hello HelloMsg
	if err := json.Unmarshal(line, &hello); err != nil || hello.Type != "hello" {
		writeJSON(conn, HelloAck{Type: "error", Msg: "expected hello"})
		return
	}
	if hello.DBPath != "" && hello.DBPath != s.dbPath {
		writeJSON(conn, HelloAck{
			Type: "error",
			Msg:  fmt.Sprintf("db mismatch: this daemon serves %q", s.dbPath),
		})
		return
	}
	if err := CheckVersion(hello.Version); err != nil {
		writeJSON(conn, HelloAck{Type: "error", Version: buildinfo.Version(), Msg: err.Error()})
		return
	}
	if err := writeJSON(conn, HelloAck{
		Type:       "hello",
		Origin:     s.origin,
		ClusterID:  s.clusterID,
		BundleAddr: s.bundleAddr,
		Version:    buildinfo.Version(),
	}); err != nil {
		return
	}
	// Hello completed; client is registered. Clear the read deadline
	// — the connection now lives until the client drops it.
	_ = conn.SetReadDeadline(time.Time{})
	s.clients.Add(1)
	defer func() {
		if s.clients.Add(-1) == 0 {
			s.lastIdle.Store(time.Now().UnixNano())
		}
	}()

	// Serve requests until the client drops the connection. The
	// extension sends nothing and simply holds the connection open as a
	// liveness witness, so this blocks on read for its whole session.
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			writeJSON(conn, Response{Type: "error", Msg: "malformed request"})
			continue
		}
		s.serveRequest(conn, req)
	}
}

func (s *Server) serveRequest(conn net.Conn, req Request) {
	switch req.Type {
	case "wait":
		fn := s.waitFn.Load()
		if fn == nil {
			writeJSON(conn, Response{Type: "error", Msg: "this daemon does not serve wait"})
			return
		}
		ctx := context.Background()
		if req.TimeoutMS > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond)
			defer cancel()
		}
		if err := (*fn)(ctx); err != nil {
			writeJSON(conn, Response{Type: "error", Msg: err.Error()})
			return
		}
		writeJSON(conn, Response{Type: "wait"})
	default:
		writeJSON(conn, Response{Type: "error", Msg: fmt.Sprintf("unknown request %q", req.Type)})
	}
}

func writeJSON(conn net.Conn, msg any) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Write(b)
	return err
}

// IdleWatcher returns a Done channel that closes when the server has
// had zero clients continuously for at least timeout. Cancel via
// ctx. Sleeps in tickInterval increments; both default-zero values
// are sane — pick small ticks for tests, larger for prod.
func (s *Server) IdleWatcher(ctx context.Context, timeout, tickInterval time.Duration) <-chan struct{} {
	if timeout <= 0 {
		// Disabled — return a channel that never closes.
		return make(chan struct{})
	}
	if tickInterval <= 0 {
		tickInterval = 30 * time.Second
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(tickInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				since := s.IdleSince()
				if since.IsZero() {
					continue
				}
				if time.Since(since) >= timeout {
					return
				}
			}
		}
	}()
	return done
}
