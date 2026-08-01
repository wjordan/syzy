package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Advisory-lock keys for the DDL gate (§6). They are per-database
// (each node's Postgres is independent), so fixed constants are safe; both are
// < 2^31 so a 1-arg bigint advisory lock records them in pg_locks as
// classid=0, objid=key, objsubid=1 (the bigint splits key>>32 into classid,
// key&0xffffffff into objid; objsubid=1 marks the 1-arg form, =2 the 2-arg).
// The probes match all three to avoid counting an unrelated 2-arg lock (or a
// large-key lock) whose low 32 bits collide with one of these keys.
const (
	ddlLocalLockKey int64 = 1937007001 // serialize a node's own concurrent DDL backends
	ddlGateLockKey  int64 = 1937007002 // the sidecar holds this; releasing it admits one DDL
)

// ddlGatePoll is how often the watcher polls pg_locks for a waiting DDL backend;
// ddlGateHeartbeat is how often it renews the lease while holding it; and
// ddlGateIdle is how long an idle holder retains the lease to amortize an
// autocommit DDL burst. Vars so tests can shrink them for tighter timing.
var (
	ddlGatePoll      = 25 * time.Millisecond
	ddlGateHeartbeat = 2 * time.Second
	ddlGateIdle      = 250 * time.Millisecond
)

// gateTriggerSQL builds the ddl_command_start gate trigger from the lock-key
// constants so the SQL and the Go watcher cannot drift. It gates only the FIRST
// DDL of a transaction (txn-local syzy.ddl_gated flag, like syzy.ddl_ordinal),
// so a multi-statement migration runs every later command without re-gating.
// ENABLE ORIGIN + the syzy.internal guard keep it silent for applied/internal DDL.
func gateTriggerSQL() string {
	return fmt.Sprintf(`
CREATE OR REPLACE FUNCTION syzy_ddl_command_start() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF current_setting('syzy.internal', true) = 'on' THEN RETURN; END IF;
    IF current_setting('syzy.ddl_gated', true) = 'on' THEN RETURN; END IF;
    PERFORM set_config('syzy.ddl_gated', 'on', true);
    PERFORM pg_advisory_xact_lock(%d);
    PERFORM pg_advisory_xact_lock(%d);
END $$;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'syzy_ddl_start') THEN
        CREATE EVENT TRIGGER syzy_ddl_start ON ddl_command_start
            EXECUTE FUNCTION syzy_ddl_command_start();
    END IF;
END $$;`, ddlLocalLockKey, ddlGateLockKey)
}

// installDDLGate installs the gate trigger on conn under the syzy.internal guard
// (its own DDL must neither fire the gate nor write intent rows).
func installDDLGate(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, `SET syzy.internal = 'on'`); err != nil {
		return err
	}
	_, execErr := conn.Exec(ctx, gateTriggerSQL())
	if _, err := conn.Exec(ctx, `SET syzy.internal = 'off'`); err != nil && execErr == nil {
		execErr = err
	}
	return execErr
}

// gateManager is the sidecar half of the DDL lease (§6). On a
// dedicated connection it holds GATE_KEY ("gate closed"); when a local DDL
// backend blocks on GATE_KEY (the ddl_command_start trigger), the watcher
// acquires the cluster lease, has the orchestrator apply pending peer DDL, then
// releases GATE_KEY to admit that one transaction — re-closing only after
// observing the user actually acquired it (so it cannot steal the lock back,
// needing no advisory-lock FIFO guarantee). It holds the lease until the admitted
// DDL has been APPENDED to the schema log (observed as syzy_ddl_intent draining
// to empty), not merely committed: append-after-commit means releasing at commit
// would let a peer append at the same parent and the originator's pending append
// would lose the CAS.
//
// catchUpSchema mutates the shared catalog, which only the orchestrator goroutine
// may touch (D4), so the watcher requests catch-up over catchUpReq rather than
// calling it directly. All gate connection + lease state lives on the single
// watcher goroutine.
type gateManager struct {
	lease      Lease
	holder     string
	conn       *pgx.Conn
	connURL    string // to reconnect + re-close the gate if conn dies (fail-safe)
	pid        int
	catchUpReq chan chan error
	poll       time.Duration
	hbEvery    time.Duration
	idleFor    time.Duration

	holds     bool      // do we currently hold the cluster lease?
	caughtUp  bool      // caught up since acquiring the uninterrupted lease grant?
	lastHB    time.Time // last lease heartbeat, to throttle Heartbeat calls
	idleSince time.Time // spool first observed empty with no local waiter
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	closed    bool // GATE_KEY held (gate shut)
	started   bool // watcher goroutine launched
	stopped   bool
}

// closeGate acquires GATE_KEY so the gate is shut the moment Open returns —
// before any user DDL can run. (Acquired in Open, not start, so there is no
// window where DDL slips through ungated between Open and Run.) The watcher that
// admits waiters is launched later by start.
func (g *gateManager) closeGate(ctx context.Context) error {
	if err := g.conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&g.pid); err != nil {
		return fmt.Errorf("gate backend pid: %w", err)
	}
	if _, err := g.conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, ddlGateLockKey); err != nil {
		return fmt.Errorf("close gate: %w", err)
	}
	g.closed = true
	return nil
}

// start launches the watcher goroutine that admits waiting DDL. The gate is
// already shut (closeGate ran in Open), so a DDL that arrived before Run simply
// waits until the watcher admits it.
func (g *gateManager) start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	g.cancel = cancel
	g.started = true
	g.wg.Add(1)
	go func() { defer g.wg.Done(); g.watch(ctx) }()
}

// stop cancels the watcher, releases the lease and gate, and closes the gate
// connection. Idempotent and safe to call on a never-started manager (so both
// Run's defer and Engine.Close can call it).
func (g *gateManager) stop() {
	if g.stopped {
		return
	}
	g.stopped = true
	if g.started {
		g.cancel()
		g.wg.Wait()
	}
	bg := context.Background()
	if g.holds {
		_ = g.lease.Release(bg, g.holder)
	}
	if g.closed {
		_, _ = g.conn.Exec(bg, `SELECT pg_advisory_unlock($1)`, ddlGateLockKey)
	}
	if g.conn != nil {
		_ = g.conn.Close(bg)
	}
}

func (g *gateManager) watch(ctx context.Context) {
	t := time.NewTicker(g.poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := g.waiterCount(ctx)
		if err != nil {
			g.recover(ctx) // reconnect + re-close if the gate connection died
			continue       // otherwise transient (e.g. ctx race); retry next tick
		}
		if n > 0 {
			g.idleSince = time.Time{}
			g.admitOne(ctx)
			continue
		}
		// Idle: release the lease only once all local DDL has been appended to the
		// schema log (intent spool empty), so a peer can take over; otherwise keep
		// it alive while a committed-but-unappended DDL is pending.
		if g.holds {
			if empty, err := g.intentEmpty(ctx); err == nil && empty {
				if g.idleSince.IsZero() {
					g.idleSince = time.Now()
				}
				if time.Since(g.idleSince) >= g.idleFor {
					_ = g.lease.Release(ctx, g.holder)
					g.holds = false
					g.caughtUp = false
					g.idleSince = time.Time{}
				} else {
					g.heartbeat(ctx)
				}
			} else {
				g.idleSince = time.Time{}
				g.heartbeat(ctx)
			}
		}
	}
}

// admitOne lets exactly one waiting DDL transaction through: hold the cluster
// lease, have the orchestrator apply pending peer DDL, open the gate, wait until
// the waiter has acquired it, then re-close after the admitted txn ends.
func (g *gateManager) admitOne(ctx context.Context) {
	if !g.holds {
		if _, err := g.lease.Acquire(ctx, g.holder); err != nil {
			return // a peer holds the lease; leave the waiter blocked, retry next tick
		}
		g.holds = true
		g.caughtUp = false
		g.lastHB = time.Now()
	} else if !g.heartbeat(ctx) {
		return
	}
	if !g.caughtUp {
		if !g.requestCatchUp(ctx) {
			return // catch-up failed/cancelled; keep the gate closed, retry next tick
		}
		g.caughtUp = true
	}
	if _, err := g.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, ddlGateLockKey); err != nil {
		return
	}
	g.closed = false
	g.awaitUserHolds(ctx)
	g.recloseGate(ctx)
}

// recloseGate polls instead of blocking in pg_advisory_lock so a long-running
// DDL transaction cannot prevent lease heartbeats while it holds GATE_KEY.
func (g *gateManager) recloseGate(ctx context.Context) {
	for {
		var locked bool
		if err := g.conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, ddlGateLockKey).Scan(&locked); err != nil {
			return
		}
		if locked {
			g.closed = true
			return
		}
		if g.holds {
			g.heartbeat(ctx)
		}
		if !g.waitPoll(ctx) {
			return
		}
	}
}

// recover re-closes the gate after its connection dies. A session advisory lock
// is released by Postgres when its backend exits, so a dead gate connection
// leaves GATE_KEY free and the gate open — local DDL could then commit ungated.
// We reconnect and re-acquire GATE_KEY. The schema-log CAS (a stale append loses
// to the live owner and surfaces failed_local) is the correctness backstop for
// the unavoidable window before the re-close; this just restores the liveness
// gate so the common case stays serialized. The cluster lease lives in its own
// store, not on this connection, so it is unaffected — g.holds stands.
func (g *gateManager) recover(ctx context.Context) {
	if g.conn != nil && !g.conn.IsClosed() {
		return // a transient query error, not a dead connection
	}
	conn, err := pgx.Connect(ctx, g.connURL)
	if err != nil {
		return // retry on the next tick
	}
	g.conn = conn
	g.closed = false
	_ = g.closeGate(ctx) // re-acquire GATE_KEY (sets g.closed and the new g.pid)
}

// requestCatchUp asks the orchestrator goroutine (the sole catalog writer) to
// apply pending schema events before the gate opens. Returns false on error or
// cancellation.
func (g *gateManager) requestCatchUp(ctx context.Context) bool {
	reply := make(chan error, 1)
	select {
	case g.catchUpReq <- reply:
	case <-ctx.Done():
		return false
	}
	select {
	case err := <-reply:
		return err == nil
	case <-ctx.Done():
		return false
	}
}

// awaitUserHolds blocks until the released GATE_KEY is held by another backend
// (the admitted DDL acquired it) or no one waits/holds (the waiter gave up via
// lock_timeout). Re-acquiring only after this means the watcher never steals the
// lock back from the queued waiter.
func (g *gateManager) awaitUserHolds(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		var heldByOther, waiting int
		err := g.conn.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE granted AND pid <> $2),
			       count(*) FILTER (WHERE NOT granted)
			FROM pg_locks
			WHERE locktype='advisory' AND classid=0 AND objid=$1 AND objsubid=1
			  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
			ddlGateLockKey, g.pid).Scan(&heldByOther, &waiting)
		if err != nil || heldByOther > 0 || waiting == 0 {
			return
		}
		if !g.heartbeat(ctx) || !g.waitPoll(ctx) {
			return
		}
	}
}

func (g *gateManager) waitPoll(ctx context.Context) bool {
	t := time.NewTimer(g.poll)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (g *gateManager) heartbeat(ctx context.Context) bool {
	if time.Since(g.lastHB) < g.hbEvery {
		return true
	}
	switch _, err := g.lease.Heartbeat(ctx, g.holder); {
	case err == nil:
		g.lastHB = time.Now()
		return true
	case errors.Is(err, ErrLeaseLost):
		// A partition let our TTL lapse and a peer took the lease. Drop ownership
		// so the next admit re-Acquires (bumping the fencing epoch) instead of
		// admitting DDL while believing we still hold it. The gate stays closed
		// meanwhile, so no DDL slips through ungated.
		g.holds = false
		g.caughtUp = false
		return false
	default:
		// A lease-store error makes ownership unverifiable. Keep the gate shut and
		// retry; admitting DDL is safe only after a successful heartbeat.
		return false
	}
}

// waiterCount is the number of backends blocked on GATE_KEY (granted=false) —
// i.e. local DDL transactions stopped at the gate.
func (g *gateManager) waiterCount(ctx context.Context) (int, error) {
	var n int
	err := g.conn.QueryRow(ctx, `
		SELECT count(*) FROM pg_locks
		WHERE locktype='advisory' AND classid=0 AND objid=$1 AND objsubid=1 AND NOT granted
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
		ddlGateLockKey).Scan(&n)
	return n, err
}

// intentEmpty reports whether the DDL intent spool has no committed-but-unappended
// rows — the signal that all local DDL has reached the schema log.
func (g *gateManager) intentEmpty(ctx context.Context) (bool, error) {
	var empty bool
	err := g.conn.QueryRow(ctx, `SELECT NOT EXISTS (SELECT 1 FROM syzy_ddl_intent)`).Scan(&empty)
	return empty, err
}
