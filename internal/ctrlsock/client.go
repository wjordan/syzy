package ctrlsock

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wjordan/syzy/internal/buildinfo"
)

// ErrNoDaemon means no daemon is currently bound to the per-DB socket.
// Auto-spawn callers treat this as the trigger to fork+exec a daemon.
var ErrNoDaemon = errors.New("ctrlsock: no daemon")

// Client is a held-open connection to a daemon's control socket. The
// extension keeps one for the life of its SQLite connection so the
// daemon counts it as a live client (preventing idle-exit). Closing
// drops the connection and lets the daemon's idle timer start.
type Client struct {
	conn net.Conn
	// r is the sole reader over conn. A second bufio.Reader on the same
	// connection could swallow bytes the first had already buffered, so
	// every read on this client goes through this one.
	r          *bufio.Reader
	Origin     string
	ClusterID  string
	BundleAddr string
}

// Dial opens the per-DB socket and completes the hello handshake.
// Returns ErrNoDaemon if the socket is missing or refuses connection
// — callers fall through to auto-spawn.
func Dial(dbPath string) (*Client, error) {
	path, err := SocketPath(dbPath)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		if isMissing(err) {
			return nil, ErrNoDaemon
		}
		return nil, fmt.Errorf("ctrlsock: dial %s: %w", path, err)
	}

	// Absolutize so a relative invocation (e.g. `syzy clone a.db b.db`
	// from the DB's directory) matches the daemon's stored abs path on
	// the server-side hello check.
	helloPath := dbPath
	if abs, absErr := filepath.Abs(dbPath); absErr == nil {
		helloPath = abs
	}
	hello := HelloMsg{Type: "hello", DBPath: helloPath, Version: buildinfo.Version()}
	b, err := json.Marshal(hello)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	b = append(b, '\n')
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(b); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ctrlsock: send hello: %w", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	line, err := r.ReadBytes('\n')
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ctrlsock: read hello ack: %w", err)
	}
	var ack HelloAck
	if err := json.Unmarshal(line, &ack); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ctrlsock: decode hello ack: %w", err)
	}
	if ack.Type == "error" {
		_ = conn.Close()
		return nil, fmt.Errorf("ctrlsock: %s", ack.Msg)
	}
	// Also check from this side: a daemon predating the version
	// handshake accepts the hello without comparing anything.
	if err := CheckVersion(ack.Version); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ctrlsock: %w", err)
	}
	// Hello complete — clear deadlines for the long-lived connection.
	_ = conn.SetReadDeadline(time.Time{})
	_ = conn.SetWriteDeadline(time.Time{})
	return &Client{
		conn:       conn,
		r:          r,
		Origin:     ack.Origin,
		ClusterID:  ack.ClusterID,
		BundleAddr: ack.BundleAddr,
	}, nil
}

// Wait asks the daemon to block until its database has applied
// everything its peers held when the call began. timeout bounds the
// daemon-side wait; zero means no bound.
func (c *Client) Wait(timeout time.Duration) error {
	b, err := json.Marshal(Request{Type: "wait", TimeoutMS: timeout.Milliseconds()})
	if err != nil {
		return err
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.conn.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("ctrlsock: send wait: %w", err)
	}

	// The daemon replies only once it has caught up or given up, so the
	// read deadline has to outlast its own timeout rather than race it.
	if timeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(timeout + 5*time.Second))
		defer c.conn.SetReadDeadline(time.Time{})
	}
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("ctrlsock: read wait reply: %w", err)
	}
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return fmt.Errorf("ctrlsock: decode wait reply: %w", err)
	}
	if resp.Type == "error" {
		return errors.New(resp.Msg)
	}
	return nil
}

// Close drops the connection. Idempotent.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// isMissing reports whether the error indicates the socket does not
// exist or is not currently being listened on. Used to distinguish
// "spawn the daemon" from "real network error."
func isMissing(err error) bool {
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	// Unix domain "connection refused" surfaces as syscall.ECONNREFUSED,
	// which net errors wrap. Use the string form to avoid platform
	// import noise.
	s := err.Error()
	return strings.Contains(s, "connection refused") || strings.Contains(s, "no such file")
}
