package syzyext

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/unique"
	"github.com/wjordan/syzy/wake/vsock"
)

// uniqueProbeTimeout bounds the one synchronous dial that decides whether
// the reservation proxy exists. The endpoint is process-local (a unix
// socket or a vsock bridge to the host), so a healthy listener answers in
// microseconds; anything slower is treated as absent.
const uniqueProbeTimeout = 2 * time.Second

// openUniqueRegistry resolves the coordinated-uniqueness reservation
// backend for an attached producer from SYZY_UNIQUE_DIAL ("unix:<path>" or
// "vsock:<cid>:<port>", the forms vsock.DialAddr accepts). Unset, or set
// but unreachable, returns nil — the producer then rejects coordinated
// (NOT NULL UNIQUE) DDL, exactly as if no registry were configured.
//
// The probe is what keeps this fail-closed: the serving side exposes the
// proxy only when its node actually has a reservation backend, so listener
// presence is the capability signal. Skipping the probe would hand the
// producer a non-nil registry unconditionally, admitting coordinated DDL
// on clusters that can never grant its reservations.
func openUniqueRegistry(log *slog.Logger) (unique.Registry, io.Closer) {
	spec := strings.TrimSpace(os.Getenv("SYZY_UNIQUE_DIAL"))
	if spec == "" {
		return nil, nil
	}
	dialAddr, err := vsock.DialAddr(spec)
	if err != nil {
		log.Warn("syzyext: SYZY_UNIQUE_DIAL invalid; coordinated unique keys stay rejected", "spec", spec, "error", err)
		return nil, nil
	}
	probe := make(chan error, 1)
	go func() {
		conn, err := dialAddr()
		if conn != nil {
			_ = conn.Close()
		}
		probe <- err
	}()
	select {
	case err := <-probe:
		if err != nil {
			log.Warn("syzyext: unique reservation endpoint unreachable; coordinated unique keys stay rejected", "spec", spec, "error", err)
			return nil, nil
		}
	case <-time.After(uniqueProbeTimeout):
		log.Warn("syzyext: unique reservation probe timed out; coordinated unique keys stay rejected", "spec", spec)
		return nil, nil
	}
	client, err := unique.NewProxyClient(spec, func(ctx context.Context) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return dialAddr()
	})
	if err != nil {
		log.Warn("syzyext: unique reservation client failed; coordinated unique keys stay rejected", "spec", spec, "error", err)
		return nil, nil
	}
	return client, client
}

// openHelperConn opens the aux connection DDL admission needs to
// normalize a coordinated key's implicit index (producer.Config.AppHelper).
// Only opened when a reservation registry is wired, so a producer without
// coordinated keys pays no extra fd. Pragmas mirror the daemon's aux
// conns minus mmap, which admission's rare schema reads do not need.
func openHelperConn(dbPath string) (*sqlitebridge.Conn, error) {
	c, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		return nil, fmt.Errorf("open helper conn: %w", err)
	}
	if err := c.Exec(`PRAGMA busy_timeout = 5000; PRAGMA temp_store = MEMORY`); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("configure helper conn: %w", err)
	}
	return c, nil
}
