// Package producer implements the local commit pipeline: hook capture,
// commit-time evidence build, journal append, and async drain. wal_hook
// fires after each WAL commit fsync, so journal records are durable
// when written; the drainer reads the journal in batches and feeds them
// to the sink, which builds Changesets, advances nodestate.Cache, and
// fires OnEncoded for the broker to broadcast.
package producer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/internal/syncer"
	"github.com/wjordan/syzy/internal/syzylog"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/unique"
)

// DefaultJournalSegmentSize is the default size of one journal segment
// file. New segments are allocated on rotation; older ones are reaped
// only by snapshotter GC after durable snapshot markers make them safe.
const DefaultJournalSegmentSize = uint32(1 << 20)

// defaultReserveRetryBudget caps how long commit_hook waits out a transient
// unique.ErrUnavailable before failing the commit. The wait runs INSIDE the
// commit, holding the app.db write lock — which starves the broker's
// inbound apply (and with it the node's replication freshness) for its
// full duration. So the in-lock budget only absorbs brief blips (a
// graceful leaseholder handoff, a re-dial); anything longer — a
// crash-failover drain, a partition — fails the commit with
// SQLITE_CONSTRAINT_COMMITHOOK wrapping unique.ErrUnavailable, and the
// embedder retries that transaction off the writer.
const defaultReserveRetryBudget = 2 * time.Second

// hlcStamper is the only nodestate.Cache surface walHook needs.
type hlcStamper interface {
	StampHLC(wall int64) crdt.Clock
}

// Producer captures local DML on app.db, writes commit evidence into
// the deferred-drain journal, and drives a background drainer that
// builds Changesets and advances nodestate.Cache. One Producer is
// bound to one writer connection; callers serialize their own writes
// through that connection.
type Producer struct {
	app *sqlitebridge.Conn
	sc  *metadata.Store
	cat *catalog.Catalog

	cluster crdt.ClusterID
	origin  crdt.Origin

	journal *journal.Journal
	sink    *syncer.MetaSink
	drainer *syncer.Drainer

	drainCancel context.CancelFunc
	drainDone   chan struct{}
	// drainErr is set non-nil if drainer.Run returned an error and exited.
	// WaitForDrain reads this so callers learn the drainer is dead instead
	// of busy-looping until ctx-deadline.
	drainErr atomic.Pointer[error]

	// mu serializes wal_hook bookkeeping (the touch-buffer scratch
	// slice). SQLite's writer lock already serializes
	// wal_hook firings; the mutex makes the invariant explicit and
	// covers the producer's own scratch state.
	mu sync.Mutex

	nowMS func() int64

	// payloadBuf is a scratch buffer reused across walHook calls for
	// journal payload encoding. Protected by p.mu (walHook serializes
	// through it). The buffer is consumed by journal.Append, which copies
	// into the mmap; safe to reuse on next call.
	payloadBuf []byte

	hlc hlcStamper

	// ddl is the DDL admission state used by the trace_v2 hook. nil when
	// no SchemaLog was supplied; in that case DDL is disabled (any DDL
	// against the writer connection is rejected).
	ddl *ddlAdmission

	// reg is the coordinated-uniqueness reservation backend (Config.
	// UniqueRegistry). nil disables coordinated keys. commit_hook reserves
	// against it before commit; walHook releases vacated values after.
	reg unique.Registry

	// reserveRetryBudget bounds commit_hook's retry of a reservation that
	// returns unique.ErrUnavailable (Config.ReserveRetryBudget).
	reserveRetryBudget time.Duration

	// bgCtx is cancelled by Close. It carries the reserve RPC and aborts an
	// in-flight reserve-retry backoff, so shutdown never blocks behind a
	// reservation waiting out a leaseholder handover.
	bgCtx    context.Context
	bgCancel context.CancelFunc

	// pendingReleases holds the coordinated values a just-committed txn
	// vacated, computed at commit_hook and released at walHook (post-
	// commit, so a rollback never frees a value the row still holds).
	// Release is advisory: the leaseholder backend ignores it (it observes
	// vacated values in the replicated rows); in-process backends free
	// immediately. The producer is single-writer, so no lock is needed
	// beyond p.mu's coverage of the wal_hook path. Value/Owner bytes are
	// Go-owned copies.
	pendingReleases []unique.Claim

	// closed makes Close idempotent. p.mu guards.
	closed bool
}

// Config configures a Producer. JournalDir and Cache are required;
// other fields take sane defaults.
type Config struct {
	// JournalDir is the directory where the deferred-drain journal
	// segments live. Created if absent.
	JournalDir string
	// SegmentSize overrides the journal segment size. 0 → default.
	SegmentSize uint32
	// Cache is the source of truth for self-side runtime state
	// (sender_next_seq, hlc_last, row_clock). The sink reads
	// Cache.SenderNextSeq + Cache.RowState, advances Cache.PutRowState
	// on apply, and skips any per-batch metadata tx — the snapshotter
	// persists state asynchronously. Cache must be seeded
	// (LoadFromMeta or recovery replay) before producer.New is
	// called.
	Cache *nodestate.Cache

	// SchemaLog, when non-nil, enables direct DDL replication on the
	// writer connection. Producer installs a trace_v2 hook that
	// classifies each statement, calls SchemaLog.Append for replicated
	// DDL forms, and writes a LocalDDL intent in the metadata before
	// SQLite executes the body. wal_hook resolves the intent
	// (UPSERTs the catalog, advances meta.schema_seq, clears the
	// intent). When nil, DDL is not replicated.
	SchemaLog schemalog.Log

	// AppHelper is an optional second connection to the same app DB
	// used to execute synthesized cascade-trigger DDL on the
	// originator. The trigger SQL must run on a connection that is
	// not the writer's trace_v2 stack (nested DDL during trace_v2
	// silently fails to persist). When AppHelper is nil and an FK
	// with a cascade action is declared, the originator's local
	// SQLite will not have the synth trigger installed (receivers do,
	// via the broker apply path) — cascade-on-DELETE behavior
	// degrades to receivers-only for that node. Production deployments
	// should pass a dedicated connection.
	AppHelper *sqlitebridge.Conn

	// JournalSync selects the self-journal sync mode. Default
	// (JournalSyncAuto) derives from the writer's PRAGMA
	// main.synchronous at New time: FULL/EXTRA → SyncOn, else
	// SyncOff. Set the pragma before producer.New; changes after
	// are not honored. ForceOn/ForceOff cover measurement and
	// asymmetric modes. See sqlite/docs/ARCHITECTURE.md "Host-Level Desync".
	JournalSync JournalSyncSetting

	// ProducerOnly skips the sink + drainer pipeline. Used by the
	// loadable-extension build: the extension installs hooks and
	// writes the journal but the daemon (separate process) drains
	// the journal via syncer.SecondaryDrainer. Cache is still
	// required for HLC stamping; in producer-only mode the cache's
	// senderNextSeq is never touched. WaitForDrain becomes a no-op.
	ProducerOnly bool

	// Origin overrides the per-process origin id stamped onto every
	// journal record. Zero falls back to the metadata's persisted
	// node_id (the existing single-writer behavior). The
	// loadable-extension code path passes an explicit origin from
	// its own layout.OriginClaim — the metadata's node_id is the
	// daemon's, not the extension's.
	Origin crdt.Origin

	// BlobRead is an optional read-only app.db connection used by the
	// drainer to materialize blob_patch records (read post-commit NEW
	// bytes via sqlite3_blob_open). Required to capture sqlite3_blob_write
	// mutations as blob_patch; nil silently drops them. The connection
	// must be a separate handle from the writer (Conn isn't safe for
	// concurrent use across the writer + drainer goroutines).
	BlobRead *sqlitebridge.Conn

	// ReplicateUnderscoreTables, if true, treats underscore-prefixed
	// table names as ordinary replicated tables instead of local-only.
	// sqlite_* names remain local-only unconditionally (SQLite requires
	// them to be process-local). Off by default to preserve the
	// underscore-as-local user convention.
	//
	// This is intended to be set once at slot-creation time and never
	// changed: admission decisions are made at DDL time, so tables
	// already materialized under one mode are not retroactively promoted
	// or demoted when the flag flips.
	ReplicateUnderscoreTables bool

	// IdempotentDDL makes the writer path treat a DDL whose effect is
	// already present as a no-op success (rewritten to SELECT 1 before
	// prepare) instead of erroring, mirroring the receiver's
	// opAlreadyAppliedInSQLite. Lets multiple writers replay the same DDL
	// without IF [NOT] EXISTS on every form. Off by default.
	IdempotentDDL bool

	// UniqueRegistry is the reservation backend for coordinated (NOT NULL
	// UNIQUE) keys. When non-nil, such keys are admitted at DDL time and
	// enforced by a reserve-before-commit round-trip; when nil (the
	// default) NOT NULL UNIQUE is rejected at admission, since there is no
	// way to enforce by-construction uniqueness without one. See
	// sqlite/docs/ARCHITECTURE.md#coordinated-uniqueness.
	UniqueRegistry unique.Registry

	// ReserveRetryBudget bounds how long commit_hook retries a coordinated
	// reservation that returns unique.ErrUnavailable before failing the
	// commit. The retry blocks the single writer AND the broker's inbound
	// apply (both need the app.db write lock), so keep it short: it should
	// absorb a graceful handover blip, not a failover drain. Past the
	// budget the commit fails (SQLITE_CONSTRAINT_COMMITHOOK wrapping
	// unique.ErrUnavailable) and the caller retries off the writer.
	// 0 ⇒ defaultReserveRetryBudget.
	ReserveRetryBudget time.Duration

	// SelfLog, when set, is the durable capture boundary for locally
	// produced changesets: the drainer appends every built changeset here
	// and fsyncs the batch before publishing (sqlite/docs/ARCHITECTURE.md "Self-log").
	// Nil disables durable self-log capture.
	SelfLog syncer.SelfLog

	// RecoverSelf, when set, runs once during New — after DDL-intent
	// recovery and BEFORE the drainer starts — to replay the self-log into
	// the Cache (nodestate.RecoverSelf), restoring seq/clock state and the
	// self-journal resume marker from durably captured bytes. It is handed
	// the sink as the blob-clock replayer. Must precede the drainer so the
	// drainer resumes past the self-log tip instead of re-deriving.
	RecoverSelf func(blobs nodestate.SelfLogReplayer) error
}

// JournalSyncSetting selects the producer's journal sync mode at
// New time. Zero value is Auto. See Config.JournalSync.
type JournalSyncSetting int

const (
	JournalSyncAuto JournalSyncSetting = iota
	JournalSyncForceOff
	JournalSyncForceOn
)

// New wires up a Producer. cluster_id and node_id must already be
// present in metadata meta (Init seeds them).
//
// New starts a background drainer goroutine that consumes the journal,
// builds Changesets, and (in Cache mode) advances nodestate.Cache. Use
// Close to stop it.
func New(app *sqlitebridge.Conn, sc *metadata.Store, cat *catalog.Catalog, cfg Config) (*Producer, error) {
	if cfg.JournalDir == "" {
		return nil, errors.New("producer: Config.JournalDir required")
	}
	segSize := cfg.SegmentSize
	if segSize == 0 {
		segSize = DefaultJournalSegmentSize
	}

	cluster, ok, err := sc.GetClusterID()
	if err != nil {
		return nil, fmt.Errorf("producer: read cluster_id: %w", err)
	}
	if !ok {
		return nil, errors.New("producer: metadata has no cluster_id (run Init)")
	}
	origin := cfg.Origin
	if origin == 0 {
		got, ok, err := sc.GetNodeID()
		if err != nil {
			return nil, fmt.Errorf("producer: read node_id: %w", err)
		}
		if !ok {
			return nil, errors.New("producer: metadata has no node_id and Config.Origin not set")
		}
		origin = got
	}
	// Resolve ReplicateUnderscoreTables. The flag is stamped on the
	// slot's first producer.New; after that the persisted value always
	// wins (already-materialized tables can't be retroactively
	// re-classified, so a later Config flip would only produce a
	// partially-replicated slot). Callers on the warm path can pass
	// the zero value and inherit. AdoptFork / AdoptClone propagate the
	// flag through metadata (neither method clears it explicitly), so
	// a fork inherits its parent's setting.
	replicateUnderscore := cfg.ReplicateUnderscoreTables
	persisted, hasPersisted, err := sc.GetReplicateUnderscoreTables()
	if err != nil {
		return nil, fmt.Errorf("producer: read replicate_underscore: %w", err)
	}
	if hasPersisted {
		replicateUnderscore = persisted
	} else {
		if err := sc.SetReplicateUnderscoreTables(cfg.ReplicateUnderscoreTables); err != nil {
			return nil, fmt.Errorf("producer: stamp replicate_underscore: %w", err)
		}
	}

	syncMode, syncReason, err := resolveJournalSyncMode(app, cfg.JournalSync)
	if err != nil {
		// The auto probe reads PRAGMA main.synchronous on the app's own
		// connection, whose view of the shared file can be transiently (and
		// per-connection stickily) broken right after a restore under
		// cross-writer DAX invalidation churn. The probe is configuration
		// sensing, not correctness: fall back to the durable side rather
		// than failing the whole attach over a perf knob.
		syncMode, syncReason = journal.SyncOn, "main.synchronous unreadable, defaulting durable"
		syzylog.Printf("producer: journal sync probe failed (%v); defaulting to %s", err, syncMode)
	}
	syzylog.Debugf("producer: journal sync = %s (%s)", syncMode, syncReason)
	j, err := journal.Open(cfg.JournalDir, segSize, syncMode)
	if err != nil {
		return nil, fmt.Errorf("producer: open journal: %w", err)
	}
	if cfg.ProducerOnly {
		j.EnableSharedWake(true)
	}

	if cfg.Cache == nil {
		return nil, errors.New("producer: Config.Cache required")
	}

	p := &Producer{
		app:     app,
		sc:      sc,
		cat:     cat,
		cluster: cluster,
		origin:  origin,
		journal: j,
		nowMS:   func() int64 { return time.Now().UnixMilli() },
		hlc:     cfg.Cache,
		reg:     cfg.UniqueRegistry,

		reserveRetryBudget: cfg.ReserveRetryBudget,
	}
	if p.reserveRetryBudget == 0 {
		p.reserveRetryBudget = defaultReserveRetryBudget
	}
	p.bgCtx, p.bgCancel = context.WithCancel(context.Background())
	if !cfg.ProducerOnly {
		sink := syncer.NewMetaSink(sc, cat, cluster, origin,
			func() int64 { return time.Now().UnixMicro() })
		sink.SetCache(cfg.Cache)
		if cfg.BlobRead != nil {
			sink.SetBlobRead(cfg.BlobRead)
		}
		if cfg.SelfLog != nil {
			sink.SetSelfLog(cfg.SelfLog)
		}
		p.sink = sink
		// The drainer is constructed below, after RecoverSelf, because it
		// snapshots LastDrainedOffset (the self snapshot marker) at
		// construction and RecoverSelf advances that marker to the self-log
		// tip. Building it here would pin the stale pre-recovery marker and
		// re-derive already-captured seqs.
	}
	if cfg.SchemaLog != nil {
		p.ddl = newDDLAdmission(app, sc, cat, cfg.SchemaLog, cfg.AppHelper,
			origin, func() int64 { return time.Now().UnixMicro() },
			replicateUnderscore, cfg.UniqueRegistry != nil)
	}
	if cfg.UniqueRegistry == nil && cat.HasCoordinatedKeys() {
		// The reservation gate is the only enforcement of a coordinated
		// key — no node holds a physical UNIQUE index for one — so this
		// writer will commit duplicates. Loud rather than fatal: refusing
		// the attach outright is a deployment decision this layer does
		// not own.
		syzylog.Printf("producer: WARNING: this database has coordinated (NOT NULL UNIQUE) keys but no reservation backend is configured; writes through this producer are NOT gated and can commit duplicate values")
	}

	// Resolve any pending LocalDDL intent before accepting writes. This
	// brings the catalog in line with an Append that committed
	// cluster-wide before a crash interrupted the wal_hook commit.
	if err := p.recoverDDLIntent(); err != nil {
		// Best-effort: the intent format mismatch on a fresh metadata
		// (no intent) is fine. A real failure surfaces here.
		_ = j.Close()
		return nil, fmt.Errorf("producer: recover DDL intent: %w", err)
	}

	// Replay the self-log into the Cache before the drainer is built, so it
	// resumes past the self-log tip (verbatim-captured bytes) instead of
	// re-deriving already-published seqs. After recoverDDLIntent so the
	// catalog is current for cell-group replay.
	if p.sink != nil && cfg.RecoverSelf != nil {
		if err := cfg.RecoverSelf(p.sink); err != nil {
			_ = j.Close()
			return nil, fmt.Errorf("producer: recover self-log: %w", err)
		}
	}

	// Build the drainer now — it snapshots the (recovered) self snapshot
	// marker as its resume offset, so it drains only the never-captured tail.
	if p.sink != nil {
		dr, err := syncer.NewDrainer(j, p.sink)
		if err != nil {
			_ = j.Close()
			return nil, fmt.Errorf("producer: new drainer: %w", err)
		}
		p.drainer = dr
	}

	if p.drainer != nil {
		dCtx, dCancel := context.WithCancel(context.Background())
		p.drainCancel = dCancel
		p.drainDone = make(chan struct{})
		go func() {
			if err := p.drainer.Run(dCtx); err != nil {
				p.drainErr.Store(&err)
			}
			close(p.drainDone)
		}()
	}

	app.EnableTouchJournal()
	// SetProducerWALHook installs a specialized C trampoline that
	// reads + clears the touch journal in C and passes the slice
	// directly into walHook — no TouchJournalTake cgo crossing needed.
	app.SetProducerWALHook(p.walHook)
	// commit_hook carries the coordinated-uniqueness gate as well as DDL
	// rejection, and the gate is the only enforcement of a coordinated
	// key — so it is installed for every producer, including one with no
	// SchemaLog. rollback_hook pairs with it to clear per-txn state.
	app.SetCommitHook(p.commitHook)
	app.SetRollbackHook(p.rollbackHook)
	if p.ddl != nil {
		// trace_v2 fires before SQLite executes a statement. The hook
		// classifies and (for replicated DDL) calls schemalog.Append +
		// SetIntent. On reject it sets the rejected flag; commit_hook
		// turns that into a nonzero return so the txn rolls back.
		app.SetTraceHook(sqlitebridge.TraceStmt, p.traceHook)
		// SQL preprocessor rewrites rowid-alias INTEGER PRIMARY KEY DDL
		// into the multi-writer-safe gen_id() shape, accepts the
		// non-standard ADD COLUMN IF NOT EXISTS form, and (under
		// IdempotentDDL) no-ops any redundant DDL. See ddl_rewrite.go.
		app.SetSQLPreprocessor(makeSQLPreprocessor(app, p.ddl.lookupTable, replicateUnderscore, cfg.IdempotentDDL))
	}
	return p, nil
}

// rollbackHook fires after SQLite rolls back the in-flight statement.
// If the trace hook had written a LocalDDL intent for this txn, the
// schemalog.Append already committed cluster-wide; the intent must
// stay so wal_hook (next time, on the catch-up path) resolves it. But
// if the rollback came from our own reject (trace hook returned 1),
// the intent — if any — is from a *different* prior DDL still in
// flight; leave it alone too. The intent state is no-op on rollback.
//
// However, we MUST clear the DDL admission `rejected` flag here.
// trace_v2's reject() sets it and then calls Interrupt(); SQLite aborts
// the statement with SQLITE_INTERRUPT and rolls back the implicit
// (autocommit) transaction. commit_hook is never invoked on rollback,
// so rejectedAndClear() never runs, and the flag leaks into the next
// transaction — where commit_hook reads it and rejects an unrelated
// commit with SQLITE_CONSTRAINT_COMMITHOOK. Clearing here closes that
// gap: rollback consumes the rejection just like commit does.
func (p *Producer) rollbackHook() {
	// A rolled-back txn never reaches walHook, so drop any releases
	// staged by a commit_hook that then rejected.
	p.pendingReleases = nil
	if p.ddl != nil {
		p.ddl.rejectedAndClear()
		// A rolled-back explicit transaction takes its un-Appended
		// pending DDL with it — nothing reached the schema log, so
		// there is nothing to reconcile. (If commit_hook had already
		// Appended and the WAL write then failed, the intent slot —
		// deliberately untouched here — carries the recovery.)
		p.ddl.clearTxnState()
	}
}

// recoverDDLIntent applies this origin's pending LocalDDL intent to
// the metadata catalog. Called once during New. No-op if no intent is
// present. Other origins' slots are deliberately not touched: their
// owners (or the broker's stale-intent catch-up) recover them.
func (p *Producer) recoverDDLIntent() error {
	buf, ok, err := p.sc.GetOriginIntent(p.origin)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if metadata.IntentKindOf(buf) != metadata.IntentLocalDDL {
		// Other intent kinds (Clone) are a startup integrity error per
		// spec — operator must complete or roll back via the admin
		// surface. v1 surfaces this as an error.
		return fmt.Errorf("producer: non-LocalDDL intent kind %d found at startup", buf[0])
	}
	intent, err := metadata.DecodeLocalDDL(buf)
	if err != nil {
		return err
	}
	return resolveLocalDDL(p.app, p.sc, p.cat, p.schemaLog(), p.origin,
		intent, time.Now().UnixMicro())
}

// schemaLog returns the producer's configured schema log, or nil when
// DDL admission isn't wired. Only recovery passes it to
// resolveLocalDDL: the live wal_hook path skips verification (the
// Append just succeeded synchronously in the same statement flow), so
// a slow or transiently failing log backend can never stall or panic
// a commit.
func (p *Producer) schemaLog() schemalog.Log {
	if p.ddl == nil {
		return nil
	}
	return p.ddl.log
}

// commitHook is the producer's commit_hook callback. Returns nonzero
// when DDL admission rejected an in-flight statement (so SQLite rolls
// the txn back), or when commit-time admission of an explicit-
// transaction DDL fails its schemalog.Append (CAS loss, schema log
// unreachable) — the COMMIT then fails and the transaction rolls back
// with nothing replicated.
func (p *Producer) commitHook() int {
	// A wrapped rejection is consumed when SQLite returns from this commit.
	// Clear first so non-coordination vetoes never inherit a stale cause.
	p.app.SetCommitHookCause(nil)
	// The coordinated gate does NOT depend on DDL admission: it is the
	// only enforcement of a coordinated key, and a producer configured
	// without a SchemaLog (p.ddl nil) still writes rows. Returning early
	// on p.ddl == nil used to skip the gate entirely for such a writer.
	if p.ddl != nil && p.ddl.rejectedAndClear() {
		p.ddl.clearTxnState()
		p.pendingReleases = nil
		return 1
	}
	if rc := p.rejectCoordinatedTxnDML(); rc != 0 {
		p.abortTxn()
		return rc
	}
	// Coordinated-uniqueness: reserve the txn's net coordinated values
	// before the commit is allowed to finalize. A conflict or unavailable
	// backend rejects the commit (the app sees SQLITE_CONSTRAINT). Runs
	// before the deferred-DDL Append so a uniqueness conflict never
	// publishes a schema change.
	if rc := p.reserveCoordinated(); rc != 0 {
		p.abortTxn()
		return rc
	}
	if p.ddl == nil {
		return 0
	}
	return p.ddl.commitPendingTxnDDL()
}

// abortTxn drops the per-transaction state a rejected commit leaves behind.
func (p *Producer) abortTxn() {
	if p.ddl != nil {
		p.ddl.clearTxnState()
	}
	p.pendingReleases = nil
}

// rejectCoordinatedTxnDML rejects two transaction shapes the reservation
// gate cannot derive correct claims for. Both fail closed at commit; the
// gate is the only enforcement point, so reserving the wrong value is
// indistinguishable from not reserving at all.
//
//   - Adding a coordinated key and writing the key's table in one
//     transaction: claims derive from the committed catalog, which cannot
//     see the pending key, so the DML's values would commit unreserved.
//   - `ROLLBACK TO <savepoint>` in a transaction touching a coordinated
//     table: SQLite already reported the undone row changes through the
//     preupdate hook and does not un-report them, so the journal's last
//     image for a row can be a value the commit never lands — the gate
//     would reserve the phantom and leave the committed value free.
func (p *Producer) rejectCoordinatedTxnDML() int {
	if p.ddl == nil {
		return 0 // no statement-level visibility; neither shape is detectable
	}
	pending := p.ddl.pendingCoordinatedKeyTables()
	rolledBack := p.ddl.savepointRolledBack
	if len(pending) == 0 && !rolledBack {
		return 0
	}
	touched, err := syncer.TouchedTables(p.app.TouchJournal())
	if err != nil {
		syzylog.Printf("producer: coordinated DDL+DML check: %v", err)
		return 1
	}
	for _, tbl := range touched {
		if pending[string(tbl)] {
			syzylog.Printf("producer: commit rejected: transaction adds a coordinated (NOT NULL UNIQUE) key on %q and writes that table; the reservation gate cannot see the pending key — issue the DDL and the DML in separate transactions", tbl)
			return 1
		}
		if rolledBack {
			if tab, ok := p.cat.TableBytes(tbl); ok && tableHasCoordinatedKey(tab) {
				syzylog.Printf("producer: commit rejected: transaction used ROLLBACK TO a savepoint and wrote %q, which has a coordinated (NOT NULL UNIQUE) key; the undone writes remain in the change capture, so the reservation gate cannot tell which values the commit lands — retry without savepoints", tbl)
				return 1
			}
		}
	}
	return 0
}

// reserveCoordinated peeks the in-flight touch buffer, reserves the net
// coordinated key values the transaction's rows now hold, and stashes the
// values they vacated for release at walHook (post-commit). Returns 0 to
// allow the commit, 1 to reject it (a cross-node conflict or an
// unavailable backend). A no-op (returns 0) when no registry is wired or
// the schema has no coordinated keys.
func (p *Producer) reserveCoordinated() int {
	p.pendingReleases = nil
	if p.reg == nil || !p.cat.HasCoordinatedKeys() {
		return 0
	}
	touch := p.app.TouchJournal() // peek; walHook reads + clears later
	if len(touch) == 0 {
		return 0
	}
	reserves, releases, err := syncer.CoordinatedClaims(p.cat, touch)
	if err != nil {
		syzylog.Printf("producer: coordinated claims: %v", err)
		return 1
	}
	if len(reserves) > 0 {
		ok, conflict, retries, err := reserveWithRetry(p.bgCtx, p.reg, reserves, p.reserveRetryBudget, p.reserveBackoffSleep)
		if err != nil {
			syzylog.Printf("producer: coordinated reserve unavailable after %d retries: %v", retries, err)
			if errors.Is(err, unique.ErrUnavailable) {
				p.app.SetCommitHookCause(err)
			}
			return 1
		}
		if retries > 0 {
			syzylog.Printf("producer: coordinated reserve recovered after %d transient retries", retries)
		}
		if !ok {
			syzylog.Printf("producer: coordinated reserve conflict on table=%x key=%x",
				conflict.Table, conflict.Key)
			p.app.SetCommitHookCause(unique.ErrConflict)
			return 1
		}
	}
	p.pendingReleases = copyClaims(releases)
	return 0
}

const (
	reserveInitialBackoff = 50 * time.Millisecond
	reserveMaxBackoff     = 1 * time.Second
)

// reserveWithRetry calls reg.Reserve, retrying on unique.ErrUnavailable with
// capped exponential backoff until budget elapses or ctx is cancelled. A
// definitive answer — success, or a genuine uniqueness conflict (ok=false,
// err=nil) — and any non-transient error return immediately; only the
// retryable ErrUnavailable (handover / drain / brief partition) loops, so a
// healthy reservation is never delayed and a real conflict is never masked.
// sleep performs the ctx-interruptible backoff wait. Returns the number of
// transient failures observed, for logging.
func reserveWithRetry(ctx context.Context, reg unique.Registry, reserves []unique.Claim, budget time.Duration, sleep func(time.Duration)) (bool, *unique.Claim, int, error) {
	deadline := time.Now().Add(budget)
	backoff := reserveInitialBackoff
	retries := 0
	for {
		ok, conflict, err := reg.Reserve(ctx, reserves)
		if err == nil {
			return ok, conflict, retries, nil
		}
		if !errors.Is(err, unique.ErrUnavailable) {
			return false, nil, retries, err
		}
		retries++
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			return false, nil, retries, err
		}
		sleep(backoff)
		backoff *= 2
		if backoff > reserveMaxBackoff {
			backoff = reserveMaxBackoff
		}
	}
}

// reserveBackoffSleep waits d, returning early if the producer is closing so
// shutdown never blocks behind a reservation waiting out a handover.
func (p *Producer) reserveBackoffSleep(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-p.bgCtx.Done():
	}
}

// copyClaims deep-copies claims into Go-owned memory. The source claims'
// Value/Owner alias the C touch buffer, which walHook clears; the stashed
// releases must outlive that.
func copyClaims(in []unique.Claim) []unique.Claim {
	if len(in) == 0 {
		return nil
	}
	out := make([]unique.Claim, len(in))
	for i, c := range in {
		out[i] = unique.Claim{
			Table: c.Table, Key: c.Key,
			Value: append([]byte(nil), c.Value...),
			Owner: append([]byte(nil), c.Owner...),
		}
	}
	return out
}

// traceHook is the producer's trace_v2 callback. For TraceStmt events
// it dispatches to the DDL admission classifier; non-TraceStmt events
// are ignored.
func (p *Producer) traceHook(evt sqlitebridge.TraceEvent, sql string) int {
	if evt != sqlitebridge.TraceStmt || p.ddl == nil {
		return 0
	}
	return p.ddl.handleStmt(sql)
}

// OnCommit registers fn to be called after each successfully drained
// local transaction. Listeners run on the drainer goroutine; they must
// not block (broker.Notify is the canonical example: a non-blocking send
// on a buffered channel). No-op in producer-only mode (no drainer).
func (p *Producer) OnCommit(fn func()) {
	if p.sink == nil {
		return
	}
	p.sink.OnCommit(fn)
}

// Journal returns the producer's self-origin journal handle. Callers
// (snapshotter, GC) must NOT close it — the producer owns the lifecycle.
func (p *Producer) Journal() *journal.Journal { return p.journal }

// OnEncoded registers fn to be called on the drainer goroutine
// immediately after each changeset is encoded by crdt.Build, with the
// wire-format payload bytes. Production wiring routes this directly to
// the transport so broadcast happens off the commit-thread latency
// path. Listeners must not block; the byte slice aliases sink-owned
// state and must not be retained past the call (callers wanting
// durable retention copy).
func (p *Producer) OnEncoded(fn func(payload []byte)) {
	if p.sink == nil {
		return
	}
	p.sink.OnEncoded(fn)
}

// OnRecords registers fn to fire on the drainer goroutine after each
// committed changeset, with the typed record slice (Insert / Update /
// Delete / BlobPatch). Used by the notify dispatcher. Records alias
// sink-owned scratch; copy to retain. No-op in producer-only mode.
func (p *Producer) OnRecords(fn func(dot crdt.Dot, records []crdt.Record)) {
	if p.sink == nil {
		return
	}
	p.sink.OnRecords(fn)
}

// SetReassert wires the broker's local-commit re-assert hook into the
// drain (see broker.ReassertLocal). Wired after construction because
// the broker is built after the producer. No-op in producer-only mode.
func (p *Producer) SetReassert(fn func(records []crdt.Record, stamp crdt.Stamp) error) {
	if p.sink == nil {
		return
	}
	p.sink.SetReassert(fn)
}

// SetSchemaCatchup installs a synchronous schema catch-up hook for DDL
// admission's Append-CAS-loss retry (apply pending schema-log events,
// then admission rebuilds its op and retries once). Wired after
// construction because the broker that backs it is built after the
// producer — but it MUST be wired before the writer connection runs
// statements: the field is read from trace callbacks with no
// synchronization. No-op without DDL admission (nil SchemaLog).
func (p *Producer) SetSchemaCatchup(fn func(context.Context) error) {
	if p.ddl != nil {
		p.ddl.schemaCatchup = fn
	}
}

// Close uninstalls hooks, stops the drainer, and releases resources.
// The underlying app/metadata connections are owned by the caller.
// Idempotent: subsequent calls are no-ops.
func (p *Producer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()
	if p.bgCancel != nil {
		p.bgCancel() // abort any in-flight reserve retry
	}
	p.app.SetProducerWALHook(nil)
	p.app.DisableTouchJournal()
	if p.drainCancel != nil {
		p.drainCancel()
	}
	if p.drainDone != nil {
		<-p.drainDone
	}
	if p.journal != nil {
		_ = p.journal.Close()
	}
	return nil
}

// StopDrainer cancels the drain goroutine and waits for it to exit, leaving
// the journal and the WAL hook live: subsequent commits still append to the
// journal (head advances) but no longer reach the sink (drained freezes below
// head) — the on-disk shape of a stalled/lagging drainer. Used by teardown-path
// tests to drive a node into that state deterministically before a Detach.
// Idempotent; safe to call before Close (which re-cancels harmlessly).
func (p *Producer) StopDrainer() {
	if p.drainCancel != nil {
		p.drainCancel()
	}
	if p.drainDone != nil {
		<-p.drainDone
	}
}

// DrainProgress reports the drainer's committed offset and the journal
// head. Diagnostic accessor for drain-stall logging.
func (p *Producer) DrainProgress() (drained, head uint64) {
	if p.drainer == nil || p.journal == nil {
		return 0, 0
	}
	return uint64(p.drainer.DrainedOffset()), uint64(p.journal.Head())
}

// WaitForDrain blocks until the drainer has flushed every record up to
// the journal's current head, or ctx is cancelled. Tests use it to
// pause until async drain completes. No-op in producer-only mode
// (no drainer in this process).
func (p *Producer) WaitForDrain(ctx context.Context) error {
	if p.drainer == nil {
		return nil
	}
	for {
		// Converged-first: a dead drainer that already consumed
		// everything observable satisfies the wait (mirrors
		// SecondaryDrainer.WaitForDrain).
		head := p.journal.Head()
		if p.drainer.DrainedOffset() >= head {
			return nil
		}
		if errp := p.drainErr.Load(); errp != nil {
			return fmt.Errorf("drainer dead: %w", *errp)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// walHook fires from SQLite's WAL pipeline after the writer's commit
// frame has been fsync'd, so any record we append to the journal here
// is durable by definition — no separate confirmation step needed.
// Hot-path responsibility is minimal: stamp HLC, copy the C-side touch
// journal bytes into the journal mmap as the record's opaque payload,
// and clear the touch buffer.
//
// All evidence parsing, PK encoding, payload assembly, and per-record
// CRDT work is deferred to the drainer goroutine, which decodes the
// touch-journal payload at apply time. With the C touch journal now
// capturing both OLD and NEW values for UPDATE (in addition to INSERT
// NEW and DELETE OLD), no app.db reads are needed at any stage.
//
// Why wal_hook and not commit_hook (despite commit_hook firing
// before fsync, theoretically saving latency): SQLite fires
// commit_hook for every COMMIT including no-op same-value UPDATEs
// that don't dirty any pages, while wal_hook only fires when a WAL
// frame is actually written. wal_hook firing is therefore 1:1 with
// durable transactions, which lets the drainer trust the journal
// head as the durability frontier without any cross-check
// infrastructure.
//
// Record kinds:
//   - KindLocalDML : non-empty touch buffer; payload is raw touch-
//     journal bytes. Drainer decodes and may filter to nothing if all
//     touches were against non-replicated tables.
//   - KindEmpty    : empty touch buffer (DDL, no preupdate fires).
//     Drainer skips, but the record is still appended to keep the
//     journal sequence dense.
func (p *Producer) walHook(touch []byte, _ int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.walHookLocked(touch)
}

func (p *Producer) stampHLC(wall int64) crdt.Clock {
	return p.hlc.StampHLC(wall)
}

// schemaEpoch is the schema-chain stamp written into journal records:
// schema_seq + 1, so a record captured at the genesis schema (seq 0)
// is distinguishable from a pre-stamp record (0). The sink subtracts
// the 1 back out (see MetaSink.captureTable).
func (p *Producer) schemaEpoch() uint32 {
	return uint32(p.cat.SchemaSeq()) + 1
}

// kindForTouch maps a touch buffer to its journal record kind. Empty
// (no preupdate fires; e.g. DDL) becomes KindEmpty so the journal
// sequence stays dense; non-empty is KindLocalDML.
func kindForTouch(touch []byte) journal.Kind {
	if len(touch) == 0 {
		return journal.KindEmpty
	}
	return journal.KindLocalDML
}

// resolveJournalSyncMode returns the journal sync mode and a short
// human-readable reason describing how it was chosen, given the
// writer connection and the operator's setting. JournalSyncAuto
// reads PRAGMA main.synchronous on the writer; values ≥2 (FULL or
// EXTRA) → SyncOn, otherwise SyncOff.
func resolveJournalSyncMode(app *sqlitebridge.Conn, setting JournalSyncSetting) (journal.SyncMode, string, error) {
	switch setting {
	case JournalSyncForceOn:
		return journal.SyncOn, "forced on via Config.JournalSync", nil
	case JournalSyncForceOff:
		return journal.SyncOff, "forced off via Config.JournalSync", nil
	case JournalSyncAuto:
		level, name, err := readMainSynchronous(app)
		if err != nil {
			return journal.SyncOff, "", err
		}
		mode := journal.SyncOff
		if level >= 2 {
			mode = journal.SyncOn
		}
		return mode, fmt.Sprintf("derived from main.synchronous=%s", name), nil
	default:
		return journal.SyncOff, "", fmt.Errorf("unknown JournalSyncSetting %d", setting)
	}
}

// readMainSynchronous returns PRAGMA main.synchronous on the writer
// connection as both its integer value (0=OFF, 1=NORMAL, 2=FULL,
// 3=EXTRA) and the corresponding name.
func readMainSynchronous(app *sqlitebridge.Conn) (int, string, error) {
	stmt, _, err := app.Prepare(`PRAGMA main.synchronous`)
	if err != nil {
		return 0, "", fmt.Errorf("prepare PRAGMA: %w", err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil {
		return 0, "", fmt.Errorf("step PRAGMA: %w", err)
	}
	if !hasRow {
		return 0, "", errors.New("PRAGMA main.synchronous returned no row")
	}
	level := int(stmt.ColumnInt64(0))
	switch level {
	case 0:
		return level, "OFF", nil
	case 1:
		return level, "NORMAL", nil
	case 2:
		return level, "FULL", nil
	case 3:
		return level, "EXTRA", nil
	default:
		return level, fmt.Sprintf("?%d", level), nil
	}
}

// walHookLocked is the hook body; p.mu is held. The touch
// slice aliases the C-side touch journal buffer; the producer wal_hook
// trampoline (syzy_tramp_wal_producer) reads + clears the buffer in C
// and passes the slice as an argument, avoiding a cgo crossing.
func (p *Producer) walHookLocked(touch []byte) int {
	if p.ddl != nil {
		// DDL branch: when a LocalDDL intent is sitting in the
		// metadata, the user statement just executed was the DDL body.
		// Apply the catalog mutation, advance schema_seq, clear the
		// intent — all transactionally — then fall through to append
		// a KindEmpty record so the journal sequence stays dense.
		if err := p.maybeResolveDDL(); err != nil {
			panic(fmt.Errorf("producer: resolve DDL: %w", err))
		}
	}
	clk := p.stampHLC(p.nowMS())
	kind := kindForTouch(touch)
	if _, _, err := p.journal.AppendWithSchemaSeq(kind, clk.Pack(), uint64(p.origin), p.schemaEpoch(), touch); err != nil {
		panic(fmt.Errorf("producer: append journal record: %w", err))
	}
	if err := p.journal.Sync(); err != nil {
		panic(fmt.Errorf("producer: sync journal record: %w", err))
	}
	p.releaseCoordinated()
	return 0
}

// releaseCoordinated frees the coordinated values the just-committed txn
// vacated (value-change or delete). Runs post-commit so a rollback never
// frees a value the row still holds. Advisory: the leaseholder backend
// no-ops it (a vacated value exits through its release hold when the
// rows show it gone), so only in-process backends act on it, and a
// failure is at worst a liveness nuisance — never a safety hole.
func (p *Producer) releaseCoordinated() {
	if len(p.pendingReleases) == 0 {
		return
	}
	if p.reg != nil {
		if err := p.reg.Release(context.Background(), p.pendingReleases); err != nil {
			syzylog.Printf("producer: coordinated release: %v (value stays reserved until GC)", err)
		}
	}
	p.pendingReleases = nil
}

// maybeResolveDDL applies this origin's pending LocalDDL intent if
// present. Called from walHookLocked when DDL admission is wired.
// Idempotent: no-op when there's no intent, which is the steady-state
// DML path. Foreign origins' intents are invisible here — resolving
// them mid-flight is exactly the multi-producer race the origin
// scoping exists to prevent.
func (p *Producer) maybeResolveDDL() error {
	buf, ok, err := p.sc.GetOriginIntent(p.origin)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if metadata.IntentKindOf(buf) != metadata.IntentLocalDDL {
		return nil
	}
	intent, err := metadata.DecodeLocalDDL(buf)
	if err != nil {
		return err
	}
	// Physical normalization runs BEFORE resolution advances schema_seq:
	// a coordinated key must never be active while this node still holds
	// a native UNIQUE index enforcing it (the index would arbitrate on
	// the apply path — docs/CRDT.md#unique-keys). The user's DDL has
	// committed and its locks are released, so the helper connection can
	// run the swap; the intermediate state (index present, key not yet
	// active) is stricter, never weaker. Failure is fail-closed: the
	// error propagates to walHookLocked's panic, the intent survives,
	// and open-time convergence normalizes before recovery resolves it.
	if err := p.normalizeCoordinatedDDL(intent); err != nil {
		return fmt.Errorf("normalize coordinated key: %w", err)
	}
	// nil schema log = no durability verification: on this live path
	// the intent was written by the admission whose Append succeeded
	// moments ago in the same statement flow. Verification belongs to
	// startup recovery (recoverDDLIntent), where the intent may
	// predate a crash.
	return resolveLocalDDL(p.app, p.sc, p.cat, nil, p.origin,
		intent, time.Now().UnixMicro())
}

// normalizeCoordinatedDDL strips native UNIQUE enforcement for any
// coordinated key the intent's op declares (no-op otherwise; see
// ddl_normalize.go).
func (p *Producer) normalizeCoordinatedDDL(intent metadata.LocalDDLIntent) error {
	op, err := crdt.DecodeCatalogOp(intent.CatalogOp)
	if err != nil {
		return err
	}
	targets := coordinatedKeyTargets(op, p.cat)
	if len(targets) == 0 {
		return nil
	}
	if p.ddl.helper == nil {
		return errors.New("coordinated key requires an AppHelper connection for index normalization")
	}
	for table, sets := range targets {
		if _, err := NormalizeCoordinatedIndexes(p.ddl.helper, table, sets); err != nil {
			return fmt.Errorf("table %q: %w", table, err)
		}
	}
	return nil
}
