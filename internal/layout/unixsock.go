package layout

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
)

// maxUnixSocketPath is the longest Unix-socket path this platform can
// bind or dial, excluding the NUL terminator. The kernel copies the
// path into sockaddr_un.sun_path, a fixed-size array: 108 bytes on
// Linux, 104 on darwin.
//
// Overflowing it fails bind(2) and connect(2) with EINVAL — "invalid
// argument" — which says nothing about the actual cause. Everything
// that builds a socket path checks it here instead.
func maxUnixSocketPath() int {
	if runtime.GOOS == "darwin" {
		return 103
	}
	return 107
}

// ErrSocketPathTooLong is returned when a socket path cannot fit in
// sun_path. Retrying never fixes it: callers fail fast rather than
// backing off.
var ErrSocketPathTooLong = errors.New("unix socket path too long")

// CheckUnixSocketPath reports whether path fits in sun_path, with an
// error that names the limit and the overage.
func CheckUnixSocketPath(path string) error {
	if max := maxUnixSocketPath(); len(path) > max {
		return fmt.Errorf("%w: %d bytes, %d over this platform's %d-byte limit: %s",
			ErrSocketPathTooLong, len(path), len(path)-max, max, path)
	}
	return nil
}

// socketDir returns a short per-user directory for socket files,
// created with mode 0700 so other users on the host can neither connect
// to nor remove the sockets inside it. XDG-conformant when possible;
// falls back to $TMPDIR/syzy-<uid> in stripped environments (CI,
// minimal containers).
//
// Socket paths under a database directory are the readable default, but
// a deep database path overflows sun_path. This directory is the short
// fallback: callers relocate here rather than failing to bind.
func socketDir() (string, error) {
	dir := filepath.Join(os.TempDir(), "syzy-"+strconv.Itoa(os.Getuid()))
	if d := os.Getenv("XDG_RUNTIME_DIR"); d != "" {
		dir = filepath.Join(d, "syzy")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("mkdir socket dir %s: %w", dir, err)
	}
	return dir, nil
}

// ShortSocketPath returns a short socket path for the database at
// dbPath, for use when the natural path under the database directory
// would overflow sun_path. The name is a hash of dbPath, so repeated
// calls for the same database land on the same path.
func ShortSocketPath(dbPath, suffix string) (string, error) {
	return ShortSocketPathForHash(PathHash(dbPath), suffix)
}

// ShortSocketPathForHash is ShortSocketPath for callers that already
// hold the database's PathHash.
//
// The name must derive from the database path, never from something
// coarser like the cluster root: databases in one cluster share a root,
// and two daemons landing on one socket path means the second unlinks
// and steals the first's listener.
func ShortSocketPathForHash(hash, suffix string) (string, error) {
	dir, err := socketDir()
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, hash+suffix+".sock")
	if err := CheckUnixSocketPath(path); err != nil {
		// $XDG_RUNTIME_DIR itself is pathologically deep. Nothing left
		// to fall back to, so say so plainly.
		return "", fmt.Errorf("no usable socket path: %w", err)
	}
	return path, nil
}

// PathHash returns a short stable hex hash of an absolute path. Used
// for socket file names so repeated starts against the same database
// land on the same path (predictable for cleanup, ls-able).
func PathHash(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	sum := sha256.Sum256([]byte(abs))
	return hex.EncodeToString(sum[:8])
}
