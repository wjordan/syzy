// Package syzyext implements the in-process producer setup shared
// between the syzy.so SQLite loadable extension (cmd/syzy-ext) and
// regular Go binaries that want producer functionality without going
// through dlopen.
//
// The flow is the same regardless of how the caller obtained its
// sqlitebridge.Conn: acquire an origin claim under <DBPath>-syzy/,
// open the meta store and schema log, load the catalog, load the per-process
// nodestate cache, create a producer that hooks
// the conn's writes, optionally wire a cross-kernel wake transport,
// and register the syzy_changes virtual table. Close releases the
// origin claim, closes the producer, and tears down per-conn state.
package syzyext

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/ctrlsock"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/producer"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/internal/syzylog"
	"github.com/wjordan/syzy/notify"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/wake/vsock"
)

// SQLITE_PROTOCOL = 15. WAL writer race that surfaces after the inner
// busy_timeout retries are exhausted; recoverable by retrying the whole
// metadata open. Hot-attaching many VMs against a shared metadata.db
// over virtio-fs DAX makes this race likely under burst load.
const sqliteProtocol = 15

// openMetadataWithRetry retries metadata.Open when the error is
// SQLITE_PROTOCOL. The inner pragmaSQL already has busy_timeout=5000,
// so each attempt may spend up to ~5s under heavy WAL writer-lock
// contention; cap at 3 attempts to keep total wall-clock under 16s
// (3 × ~5s + sub-second sleeps), which fits inside the smoke's curl
// budget. Most failures clear on the second try since the winner's
// pragma completes in <100ms after we sleep.
func openMetadataWithRetry(path string, log *slog.Logger) (*metadata.Store, error) {
	const maxAttempts = 3
	delay := 100 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		sc, err := metadata.Open(path)
		if err == nil {
			if attempt > 1 {
				log.Info("syzyext: metadata open recovered", "attempts", attempt)
			}
			return sc, nil
		}
		if !sqlitebridge.IsCode(err, sqliteProtocol) {
			return nil, err
		}
		lastErr = err
		log.Warn("syzyext: metadata SQLITE_PROTOCOL, retrying", "attempt", attempt, "delay", delay, "error", err)
		time.Sleep(delay)
		delay *= 2
	}
	return nil, lastErr
}

// AttachWithRetry runs Attach with a short bounded retry. Right after a
// VM restore, the shared dir's FUSE/DAX caches churn under cross-writer
// invalidation and basic operations transiently misbehave — observed as
// SQLITE_PROTOCOL from the wal-index handshake, SQLITE_IOERR on reads, and
// even stat/mkdir lying about existing directories (all recover within
// milliseconds; each has been seen breaking a first open in the wild).
// Attach's rollback stack fully unwinds on failure, so re-invoking is safe.
// Retrying on any error keeps this robust to the next transient flavor; a
// genuine persistent failure costs four extra attempts (~1.5s) and logs
// each one. The budget is sized to outlast the invalidation storm, which
// runs as long as concurrent cross-VM writes keep churning attributes —
// 300ms was measured too short under a 4-writer burst.
func AttachWithRetry(conn *sqlitebridge.Conn, cfg Config) (*Attached, error) {
	const maxAttempts = 5
	logger := cfg.Log
	if logger == nil {
		logger = syzylog.Default()
	}
	delay := 100 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attached, err := Attach(conn, cfg)
		if err == nil {
			if attempt > 1 {
				logger.Info("syzyext: attach recovered", "attempts", attempt)
			}
			return attached, nil
		}
		lastErr = err
		if permanent(err) {
			// Backing off changes nothing here, and the retries bury the
			// real cause under four warnings and 40 seconds of silence.
			return nil, err
		}
		if attempt < maxAttempts {
			logger.Warn("syzyext: attach failed, retrying", "attempt", attempt, "delay", delay, "error", err)
			time.Sleep(delay)
			delay *= 2
		}
	}
	return nil, lastErr
}

// permanent reports whether err is a misconfiguration rather than a
// race. The retry loop exists for startup races — the daemon spawning
// concurrently, a busy database, attribute churn under concurrent
// writers — none of which describe a socket path that cannot fit in
// sun_path or a half-upgraded install.
func permanent(err error) bool {
	return errors.Is(err, ctrlsock.ErrVersionMismatch) ||
		errors.Is(err, layout.ErrSocketPathTooLong)
}

// Config describes how to attach a producer to an already-open
// sqlitebridge.Conn. The conn's lifecycle is the caller's; Attach
// installs hooks on it but Close does not close the conn itself.
type Config struct {
	// DBPath is the application DB file path. The syzy state directory
	// (<DBPath>-syzy/) holds the origin claim, journal, and metadata.
	DBPath string

	// AutoSpawn enables forking a syzy daemon if none is running.
	// Disabled in-guest, where the host runtime owns the daemon. The
	// producer still works when no daemon is reachable; journal entries
	// just queue until one connects.
	AutoSpawn bool

	// Log receives operational logs. nil → syzylog.Default().
	Log *slog.Logger
}

// Attached is the runtime state of one attached producer. Close
// releases the origin claim, closes the producer, and tears down
// per-conn state in the right order.
type Attached struct {
	Origin crdt.Origin

	originClaim *layout.OriginClaim
	schemaShut  io.Closer
	uniqueShut  io.Closer
	helperConn  *sqlitebridge.Conn
	producer    *producer.Producer
	meta        *metadata.Store
	ctrl        *ctrlsock.Client
	conn        *sqlitebridge.Conn
}

// Attach sets up an in-process producer on conn:
//
//  1. Connects to or spawns the cluster daemon (per cfg.AutoSpawn)
//  2. Acquires an origin claim
//  3. Opens metadata (read-only)
//  4. Seeds the catalog from the live schema
//  5. Loads the nodestate cache
//  6. Resolves the schema-log handle without Head, Read, or Append
//  7. Resolves the coordinated-unique reservation registry if
//     SYZY_UNIQUE_DIAL is set and its endpoint answers
//  8. Creates the producer (preupdate hooks + journal writes go through
//     this conn from now on)
//  9. Wires SYZY_WAKE_VSOCK if set
//  10. Registers the syzy_changes virtual table
//
// The conn is expected to already be open (sqlitebridge.Open or
// WrapHandle); the producer installs its hooks on it.
func Attach(conn *sqlitebridge.Conn, cfg Config) (*Attached, error) {
	if conn == nil {
		return nil, fmt.Errorf("syzyext: Attach requires non-nil Conn")
	}
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("syzyext: Config.DBPath required")
	}
	logger := cfg.Log
	if logger == nil {
		logger = syzylog.Default()
	}

	// Rollback stack: each successful step pushes its undo. The success
	// path returns the Attached (so undo goes out of scope unused); the
	// error path runs the stack in LIFO order via fail.
	var undo []func()
	fail := func(format string, args ...any) (*Attached, error) {
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
		return nil, fmt.Errorf(format, args...)
	}

	// Per-step wall-clock, reported on the final "attached" line so startup
	// regressions identify the phase responsible.
	var steps []any
	stepStart := time.Now()
	step := func(name string) {
		now := time.Now()
		steps = append(steps, name, now.Sub(stepStart).Microseconds())
		stepStart = now
	}

	ctrl, err := attachOrSpawnDaemon(cfg.DBPath, cfg.AutoSpawn)
	if err != nil {
		return nil, fmt.Errorf("attach daemon: %w", err)
	}
	if ctrl != nil {
		undo = append(undo, func() { _ = ctrl.Close() })
	}
	step("daemon_us")

	sc, err := openMetadataWithRetry(layout.MetaDB(cfg.DBPath), logger)
	if err != nil {
		return fail("open metadata: %w", err)
	}
	undo = append(undo, func() { _ = sc.Close() })
	step("meta_us")

	// Exclude the host daemon's origin (persisted as node_id) when
	// claiming ours. Across the pmem/virtiofs boundary the host's
	// origin-dir flock is invisible to us, so without this an in-guest
	// writer could recycle the host's live origin; the host then drops
	// our records, skipping its own origin when it drains secondaries.
	hostOrigin, _, _ := sc.GetNodeID()
	originClaim, err := layout.Acquire(cfg.DBPath, 0, hostOrigin)
	if err != nil {
		return fail("acquire origin: %w", err)
	}
	undo = append(undo, func() { _ = originClaim.Release() })
	step("origin_us")

	schemaLog, schemaShut, err := OpenSchemaLog(cfg.DBPath)
	if err != nil {
		return fail("schema log: %w", err)
	}
	if schemaShut != nil {
		undo = append(undo, func() { _ = schemaShut.Close() })
	}
	step("schemalog_us")

	cat, err := catalog.LoadForRuntime(conn, sc)
	if err != nil {
		return fail("load catalog: %w", err)
	}
	step("catalog_us")

	cache := nodestate.New(originClaim.Origin)
	if err := cache.LoadFromMeta(sc); err != nil {
		return fail("cache load: %w", err)
	}
	step("nodestate_us")

	uniqueReg, uniqueShut := openUniqueRegistry(logger)
	if uniqueShut != nil {
		undo = append(undo, func() { _ = uniqueShut.Close() })
	}
	var helper *sqlitebridge.Conn
	if uniqueReg != nil {
		helper, err = openHelperConn(cfg.DBPath)
		if err != nil {
			return fail("unique helper: %w", err)
		}
		undo = append(undo, func() { _ = helper.Close() })
	}
	step("unique_us")

	prod, err := producer.New(conn, sc, cat, producer.Config{
		JournalDir:     layout.JournalDir(cfg.DBPath, originClaim.Origin),
		Cache:          cache,
		Origin:         originClaim.Origin,
		ProducerOnly:   true,
		SchemaLog:      schemaLog,
		AppHelper:      helper,
		UniqueRegistry: uniqueReg,
	})
	if err != nil {
		return fail("producer: %w", err)
	}
	undo = append(undo, func() { _ = prod.Close() })
	step("producer_us")

	if spec := strings.TrimSpace(os.Getenv("SYZY_WAKE_VSOCK")); spec != "" {
		dial, err := vsock.DialAddr(spec)
		if err != nil {
			syzylog.Printf("SYZY_WAKE_VSOCK %q: %v (keeping futex wake)", spec, err)
		} else {
			waker := vsock.NewWaker(dial, layout.OriginHex(originClaim.Origin))
			prod.Journal().SetWakeFunc(waker.Wake)
		}
	}

	provider := newChangesProvider(uint64(originClaim.Origin), cat)
	if err := sqlitebridge.RegisterChangesVTab(conn,
		notify.FeedPath(cfg.DBPath), provider); err != nil {
		return fail("register syzy_changes: %w", err)
	}
	step("vtab_us")

	logger.Info("syzyext: attached", append([]any{"db", cfg.DBPath, "origin", layout.OriginHex(originClaim.Origin)}, steps...)...)
	return &Attached{
		Origin:      originClaim.Origin,
		originClaim: originClaim,
		schemaShut:  schemaShut,
		uniqueShut:  uniqueShut,
		helperConn:  helper,
		producer:    prod,
		meta:        sc,
		ctrl:        ctrl,
		conn:        conn,
	}, nil
}

// Close tears down the producer attachment in reverse Attach order.
// Idempotent.
func (a *Attached) Close() error {
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if a.conn != nil {
		a.conn = nil
	}
	if a.producer != nil {
		record(a.producer.Close())
		a.producer = nil
	}
	// After the producer: its close path may still release pending claims.
	if a.uniqueShut != nil {
		record(a.uniqueShut.Close())
		a.uniqueShut = nil
	}
	if a.helperConn != nil {
		record(a.helperConn.Close())
		a.helperConn = nil
	}
	if a.schemaShut != nil {
		record(a.schemaShut.Close())
		a.schemaShut = nil
	}
	if a.meta != nil {
		record(a.meta.Close())
		a.meta = nil
	}
	if a.originClaim != nil {
		record(a.originClaim.Release())
		a.originClaim = nil
	}
	if a.ctrl != nil {
		record(a.ctrl.Close())
		a.ctrl = nil
	}
	return firstErr
}
