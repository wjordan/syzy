package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/wjordan/syzy/internal/clone"
	"github.com/wjordan/syzy/internal/ctrlsock"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/syzylog"
	syzy "github.com/wjordan/syzy/sqlite"
)

const cloneUsage = `usage: syzy clone <src> <dst>

  <src>  source of the bundle:
           tcp://host:port           pull from a running daemon's --listen port
           s3://bucket/<cluster-id>  pull the latest snapshot from object storage
           file:///abs/path          pull from a local FileBackend bucket (testing)
           path/to/app.db            copy from a stopped local syzy database
  <dst>  new app.db path; refuses if <dst> or <dst>-syzy/ already exists

Examples:
  syzy clone tcp://peer:7000 ./newnode/app.db
  syzy clone s3://my-bucket/01ARZ3NDEK ./newnode/app.db
  syzy clone /backups/2026-05-01/app.db ./restored/app.db

CONSISTENCY: tcp:// clone uses the source daemon's writer-barrier
snapshot and is safe against live writers. s3:// clone reads the latest
HEAD-pinned snapshot. The local-path form requires a stopped daemon
because it streams files directly.
`

func cloneCmd(args []string) error {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print(cloneUsage)
		return nil
	}
	if len(args) != 2 {
		fmt.Fprint(os.Stderr, cloneUsage)
		return errors.New("expected exactly two positional args: <src> <dst>")
	}
	src, dst := args[0], args[1]

	// URL forms go through the public syzy.Restore; the bare local-path
	// form bypasses it because the library doesn't expose stopped-source
	// streaming (and doesn't need to — restoring from a local backup is
	// an operator-only flow).
	if isRestoreURL(src) {
		if err := syzy.Restore(context.Background(), dst, src); err != nil {
			return err
		}
		fmt.Printf("cloned %s → %s\n", src, dst)
		return nil
	}

	r, cleanup, err := openLocalPathSource(src)
	if err != nil {
		var live errLiveDaemon
		if errors.As(err, &live) {
			if bundleURL := bundleAddrFromDaemon(src); bundleURL != "" {
				if err := syzy.Restore(context.Background(), dst, bundleURL); err != nil {
					return fmt.Errorf("restore via running daemon at %s: %w", bundleURL, err)
				}
				syzylog.Debugf("clone %s pulled the bundle from %s", src, bundleURL)
				fmt.Printf("cloned %s → %s\n", src, dst)
				return nil
			}
			return fmt.Errorf("local source %s has a running daemon with no bundle endpoint; stop the daemon or pass an explicit URL source", src)
		}
		return err
	}
	defer cleanup()

	newOrigin, err := clone.Adopt(r, dst)
	if err != nil {
		return err
	}
	fmt.Printf("cloned %s → %s (new origin %s)\n", src, dst, layout.OriginHex(newOrigin))
	return nil
}

func isRestoreURL(s string) bool {
	return strings.HasPrefix(s, "tcp://") ||
		strings.HasPrefix(s, "s3://") ||
		strings.HasPrefix(s, "file://")
}

// openLocalPathSource resolves a stopped-daemon local app.db path to
// an io.Reader yielding a clone bundle. URL forms are handled
// upstream via syzy.Restore; this branch covers the "operator-only"
// case of restoring from a backup file on disk.
//
// Returns (nil, nil, redirectURL) when the source has a running
// daemon advertising a bundle endpoint — the caller routes through
// syzy.Restore for a writer-barrier-pinned snapshot, which is what
// the README quickstart relies on for `syzy clone a.db b.db`.
//
// Otherwise refuses if the daemon.lock is held without an
// advertised endpoint: a running daemon might be mid-write, and
// offline streaming bypasses the snapshot flush.
func openLocalPathSource(src string) (io.Reader, func(), error) {
	claim, err := layout.ClaimDaemon(src)
	if err == nil {
		_ = claim.Release()
	} else if errors.Is(err, layout.ErrDaemonLocked) {
		return nil, nil, errLiveDaemon{src: src}
	} else {
		return nil, func() {}, fmt.Errorf("inspect daemon lock at %s: %w", src, err)
	}
	pr, pw := io.Pipe()
	go func() {
		err := clone.Stream(pw, src)
		_ = pw.CloseWithError(err)
	}()
	return pr, func() { _ = pr.Close() }, nil
}

// errLiveDaemon signals the caller to retry via the daemon's bundle
// endpoint instead of local streaming.
type errLiveDaemon struct{ src string }

func (e errLiveDaemon) Error() string {
	return fmt.Sprintf("local source %s has a running daemon; route through bundle endpoint", e.src)
}

// bundleAddrFromDaemon returns the running daemon's bundle source URL
// for src, or "" if no daemon answers or it advertises no bundle.
func bundleAddrFromDaemon(src string) string {
	c, err := ctrlsock.Dial(src)
	if err != nil {
		return ""
	}
	defer c.Close()
	return c.BundleAddr
}
