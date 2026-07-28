// Package ctrlsock is the per-DB control socket between a syzy daemon
// and its extension/CLI clients. Conventionally located at:
//
//	$XDG_RUNTIME_DIR/syzy/<db-hash>.sock     (user-level)
//	$TMPDIR/syzy-<uid>/<db-hash>.sock        (fallback)
//
// Two responsibilities:
//
//  1. Liveness probe. The extension's `.load syzy` flow tries to
//     connect; success means a daemon is already serving this DB.
//     Connection failure (ENOENT / ECONNREFUSED) is the auto-spawn
//     trigger.
//  2. Idle-exit anchor. The daemon counts active client connections;
//     once the count drops to zero and stays there for the configured
//     idle timeout, the daemon exits cleanly. Closing the SQLite
//     connection in the host process drops its ctrl-socket FD, which
//     starts the daemon's idle countdown.
//
// Wire protocol is line-delimited JSON. Two RPCs in v1:
//
//	→ {"type":"hello","db_path":"<abs>"}
//	← {"type":"hello","origin":"<hex>","cluster_id":"<hex>"}
//	  (or {"type":"error","msg":"..."})
//
// The connection then stays open as a liveness witness until the
// client drops it. No keepalives — TCP/Unix close detection is
// sufficient.
package ctrlsock

import (
	"errors"
	"fmt"

	"github.com/wjordan/syzy/internal/buildinfo"
	"github.com/wjordan/syzy/internal/layout"
)

// ErrVersionMismatch reports a daemon and client from different builds.
// Retrying never fixes it: callers fail fast rather than backing off.
var ErrVersionMismatch = errors.New("version mismatch")

// CheckVersion reports whether peer is the same build as this binary.
//
// The syzy CLI and the loadable extension ship as one release and share
// the journal layout, the on-disk metadata schema, and this protocol.
// The extension auto-spawns `syzy daemon` from $PATH, so upgrading one
// without the other is easy to do by accident and produces failures far
// from their cause. Refusing the pair up front is the cheap fix.
func CheckVersion(peer string) error {
	self := buildinfo.Version()
	if peer == self {
		return nil
	}
	if peer == "" {
		peer = "an unversioned build"
	}
	return fmt.Errorf("%w: daemon is %s, client is %s — "+
		"the syzy CLI and the loadable extension ship together and must be "+
		"the same build; reinstall both", ErrVersionMismatch, self, peer)
}

// SocketPath returns the conventional Unix socket path for the given
// absolute db path. Creates the parent directory with mode 0700 so
// other users on the host can't connect to or remove the socket.
func SocketPath(dbPath string) (string, error) {
	path, err := layout.ShortSocketPath(dbPath, "")
	if err != nil {
		return "", fmt.Errorf("ctrlsock: %w", err)
	}
	return path, nil
}
