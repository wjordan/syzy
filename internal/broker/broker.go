// Package broker bridges the local cache to peers: a Subscribe loop
// applies inbound changesets through the apply path. Locally-produced
// changesets are broadcast directly by the producer's OnEncoded hook
// off the commit-thread latency path.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/antientropy"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/quarantine"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/transport"
)

// Config bundles the dependencies a Broker needs.
type Config struct {
	// AppApply is a separate SQLite connection on the app database used
	// by the inbound apply path. It must NOT have producer hooks
	// installed; otherwise inbound writes would be captured as local DML
	// and re-broadcast.
	AppApply  *sqlitebridge.Conn
	Meta      *metadata.Store
	Catalog   *catalog.Catalog
	Transport transport.Transport

	// Log receives subscribe-loop diagnostics: inbound apply failures
	// (a dropped, non-retryable payload) and an unexpected subscription
	// closure while the broker is still running. Without it these are
	// only recorded in LastSubscribeError, which nothing polls — a
	// silently wedged inbound apply path is then invisible. nil → discard.
	Log *slog.Logger

	// QuarantineCap bounds resident quarantine entries per origin before the
	// broker stops advancing past constraint failures (reverts to hard-block)
	// for that origin. 0 → quarantine.DefaultCap.
	QuarantineCap int
	// Cache is the apply path's in-memory CRDT state: frontier +
	// applied_gaps for idempotency, row_clock for LWW. The hot path
	// reads and writes only the cache; the snapshotter persists state
	// asynchronously. Required.
	Cache *nodestate.Cache
	// MirrorJournals (per-origin) holds inbound-mirror journals keyed by
	// origin. When set together with Cache, the apply path appends each
	// successfully-applied changeset's encoded payload to the matching
	// origin's journal post-app.db commit, before fireApplied. Recovery
	// later replays these. May be nil to skip mirror writes during
	// tests or single-writer benches.
	MirrorJournals MirrorJournals

	// SchemaLog + Catalog enable schema-chain catch-up. When both are
	// set and SchemaCatchupInterval > 0, the broker spawns a goroutine
	// that periodically polls SchemaLog.Read for events past
	// meta.schema_seq and applies them locally (metadata catalog upserts
	// + Catalog.Reload). Inbound DML carrying a Deps[SchemaChain]
	// greater than the local schema_seq is held and retried by the
	// subscribe loop until catch-up lands.
	SchemaLog             schemalog.Log
	SchemaCatchupInterval time.Duration

	// GapFiller backfills per-origin sequence ranges that the live
	// Transport may not have delivered. Implemented by s3fetch.Source
	// on top of the object store; future peer-mirror RPCs would also
	// satisfy this. Nil disables gap repair — the engine still
	// converges via live broadcasts and frontier-based idempotency, but
	// a returning-from-offline node can't pull peers' historical writes
	// without one.
	GapFiller transport.GapFiller

	// TipSource is an optional source of (origin, tip) pairs the broker
	// hasn't observed via live applies. The fetcher merges these into
	// its missing-range plan so a node returning from offline (or
	// freshly cloned) can pull historical writes a peer made while we
	// were dark. Implemented by s3fetch.Source on top of objects/. Nil
	// disables discovery — fetcher only repairs gaps for already-known
	// origins.
	TipSource transport.TipSource
}

// MirrorJournals routes per-origin inbound journal appends and exposes
// the resulting handle so the apply path can publish a snapshot marker
// (recovery resumes from this offset). Append must be cheap on the hot
// path — typically a chan send to an async writer goroutine.
type MirrorJournals interface {
	Append(origin crdt.Origin, payload []byte) error
	Journal(origin crdt.Origin) (*journal.Journal, error)
}

// Broker owns the subscribe loop. Construct with New, run with Start,
// stop with Close.
type Broker struct {
	cfg     Config
	cluster crdt.ClusterID

	wg sync.WaitGroup

	startOnce sync.Once
	closeOnce sync.Once
	cancel    context.CancelFunc

	// retryBackoff bounds retry latency after Subscribe/apply errors.
	retryBackoff time.Duration

	// appliedListeners fire after the apply path's app.db commit + Cache
	// state advance, with the (origin, seq) of the just-applied changeset.
	// testcluster.Node builds WaitApplied on top. applyStartListeners fire
	// at the top of applyPayload (before decode); used by latency tracing.
	// applyRecordsListeners fire alongside appliedListeners with the
	// just-applied record slice; the notify dispatcher uses this.
	mu                    sync.Mutex
	appliedListeners      []func(crdt.Origin, crdt.Seq)
	applyStartListeners   []func(payload []byte)
	applyRecordsListeners []func(crdt.Origin, crdt.Seq, []crdt.Record)

	// applyTrace, if non-nil, fires at intermediate points inside the
	// apply path for fine-grained latency tracing. Set via
	// SetApplyTrace before Start.
	applyTrace func(stage string)

	log           *slog.Logger
	quarantineCap int

	errMu            sync.Mutex
	lastSubscribeErr error

	// lockedStreak is the current consecutive "database is locked"
	// retry count on the payload the apply-retry loop is holding; 0 when
	// inbound apply is healthy. selfHeals counts immediate and retry-threshold
	// AppApply connection-state repairs. Both feed InboundHealth.
	lockedStreak atomic.Int64
	selfHeals    atomic.Uint64

	// fetchErr* rate-limit the fetch-round WARN logs (GapFiller.Fetch /
	// DiscoverTips failures): log on error-string change or once per
	// fetchErrLogEvery, not per 30s round.
	fetchErrMu  sync.Mutex
	fetchErrMsg string
	fetchErrAt  time.Time

	// catalogReloadSeq tracks the most recent schema_seq the broker has
	// reloaded the in-memory catalog at. Used to skip redundant reloads
	// in the schema-catchup loop and to detect cross-process advances
	// (e.g. the loadable extension's wal_hook updating metadata
	// schema_seq while the daemon's catalog stays put).
	catalogReloadSeq atomic.Uint64

	// applyStmts caches per-table prepared statements for the apply path
	// (Insert and Delete). UPDATE varies by changed-column shape and is
	// not cached. Guarded by stmtsMu so direct test calls of applyPayload
	// can't race with the subscribe goroutine.
	stmtsMu          sync.Mutex
	applyInsertStmts map[crdt.TableID]*sqlitebridge.Stmt
	applyDeleteStmts map[crdt.TableID]*sqlitebridge.Stmt
	uniqSelectStmts  map[uniqStmtKey]*sqlitebridge.Stmt
	uniqNullStmts    map[uniqStmtKey]*sqlitebridge.Stmt
	uniqReadStmts    map[uniqStmtKey]*sqlitebridge.Stmt

	// markerTableReady is sticky-true once _syzy_applied is known to
	// exist in app.db (counter applied-marker, sqlite/docs/DDL.md#counter-columns).
	// Guarded by applyMu like every other AppApply access.
	markerTableReady bool

	// applyMu serializes every write through AppApply. The subscribe
	// loop, the gap-fill fetcher loop, and the schema-catchup loop all
	// share one Conn (sqlitebridge.Conn isn't safe for concurrent use),
	// so a single mutex covering applyPayloadCache + runSchemaCatchup is
	// the single source of truth for "who holds the AppApply txn".
	// Without it, two goroutines racing on BEGIN IMMEDIATE produce
	// "cannot start a transaction within a transaction" and corrupt
	// downstream prepared-statement state.
	applyMu sync.Mutex

	// appliedMu guards applied. Tracks the highest applied seq per
	// origin (plus its HLC and wall-clock apply time for InboundHealth),
	// updated on every successful applyPayload. Provides WaitApplied a
	// metadata-free signal.
	appliedMu sync.RWMutex
	applied   map[crdt.Origin]appliedInfo

	// fetchWake nudges the gap-fill loop on new gaps (seq > frontier+1).
	// Cap-1 buffered for coalescing. Non-nil only while a fetcher runs.
	fetchWake chan struct{}

	// Tunables for the gap planner; defaults applied in fetcherLoop.
	fetcherInterval    time.Duration
	fetcherMaxInterval time.Duration
	fetcherMaxRanges   int

	// gapProbe paces unserveable-range probes (see antientropy.Prober).
	gapProbe antientropy.Prober

	// blobClockMu guards blobClockHas. Cache of "does blob_range_clock
	// have any rows for table_id?" — the apply path skips the per-row
	// Get + post-commit DELETE when the answer is false. Map presence
	// means "probed"; the bool is the cached answer. Sticky-true:
	// flipped on apply of any blob_patch or reconciled DML; never
	// flipped back. First probe is one SELECT 1 ... LIMIT 1 against
	// the metadata; subsequent calls are an O(1) map read.
	blobClockMu  sync.Mutex
	blobClockHas map[crdt.TableID]bool
}

// New configures a Broker. Meta must already have cluster_id set
// and Cache must be non-nil.
func New(cfg Config) (*Broker, error) {
	if cfg.Cache == nil {
		return nil, errors.New("broker: Config.Cache required")
	}
	cluster, ok, err := cfg.Meta.GetClusterID()
	if err != nil {
		return nil, fmt.Errorf("broker: read cluster_id: %w", err)
	}
	if !ok {
		return nil, errors.New("broker: metadata has no cluster_id")
	}
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	quarantineCap := cfg.QuarantineCap
	if quarantineCap <= 0 {
		quarantineCap = quarantine.DefaultCap
	}
	return &Broker{
		cfg:           cfg,
		cluster:       cluster,
		log:           log,
		quarantineCap: quarantineCap,
		retryBackoff:  250 * time.Millisecond,
		applied:       map[crdt.Origin]appliedInfo{},
		blobClockHas:  map[crdt.TableID]bool{},
		// Initialize at construction time so KickFetcher / kickFetcher
		// is race-free against Start: the channel send is a no-op
		// before the fetcher goroutine spawns (cap-1 coalescing) and a
		// real wake afterward. Plain-field publication via Start would
		// race with the transport's OnPeerConnect callback installed
		// before Start.
		fetchWake: make(chan struct{}, 1),
	}, nil
}

// tableHasBlobClock reports whether blob_range_clock has any entries
// for table. Result is cached per-broker. First call probes the
// metadata with one bounded SELECT; subsequent calls return the cached
// answer. Meta errors are not cached — next call retries.
func (b *Broker) tableHasBlobClock(table crdt.TableID) bool {
	b.blobClockMu.Lock()
	v, probed := b.blobClockHas[table]
	b.blobClockMu.Unlock()
	if probed {
		return v
	}
	if b.cfg.Meta == nil {
		return false
	}
	any, err := b.cfg.Meta.HasAnyBlobRangeClock(table)
	if err != nil {
		return false
	}
	b.blobClockMu.Lock()
	b.blobClockHas[table] = any
	b.blobClockMu.Unlock()
	return any
}

// markTableHasBlobClock flips the cache to "yes" — called after any
// path that writes blob_range_clock entries (blob_patch apply,
// reconciled DML). Sticky-true: never clears.
func (b *Broker) markTableHasBlobClock(table crdt.TableID) {
	b.blobClockMu.Lock()
	b.blobClockHas[table] = true
	b.blobClockMu.Unlock()
}

// appliedInfo is one origin's last-applied bookkeeping: highest applied
// seq, the HLC it carried, and the wall-clock time of the apply.
type appliedInfo struct {
	seq crdt.Seq
	hlc crdt.Clock
	at  time.Time
}

// AppliedSeq returns the highest seq this broker has applied for origin,
// or ok=false if none. Updated on every successful applyPayload. Lets
// WaitApplied work without consulting the metadata.
func (b *Broker) AppliedSeq(origin crdt.Origin) (crdt.Seq, bool) {
	b.appliedMu.RLock()
	info, ok := b.applied[origin]
	b.appliedMu.RUnlock()
	return info.seq, ok
}

func (b *Broker) recordApplied(origin crdt.Origin, seq crdt.Seq, hlc crdt.Clock) {
	b.appliedMu.Lock()
	if cur := b.applied[origin]; seq > cur.seq {
		b.applied[origin] = appliedInfo{seq: seq, hlc: hlc, at: time.Now()}
	}
	b.appliedMu.Unlock()
}

// kickFetcher does a non-blocking send on fetchWake. fetchWake is
// allocated in New so a pre-Start call coalesces against the cap-1
// buffer and is observed by the fetcher goroutine when (and if) it
// spawns.
func (b *Broker) kickFetcher() {
	select {
	case b.fetchWake <- struct{}{}:
	default:
	}
}

// KickFetcher is the exported, non-blocking wake hook for callers that
// want a peer-attach event or other external signal to trigger a
// gap-fill round. Safe before Start. A kicked round also re-probes
// unserveable ranges: the usual trigger is a peer (re)connecting, and a
// new peer's journal is exactly where bucket-lost frames might surface.
func (b *Broker) KickFetcher() {
	b.gapProbe.Kick()
	b.kickFetcher()
}

// OnApplied registers fn to be called after each successful applyPayload
// commit, with the (origin, seq) of the just-applied changeset. testcluster
// uses this to build deterministic WaitApplied. Listeners must not block.
func (b *Broker) OnApplied(fn func(crdt.Origin, crdt.Seq)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.appliedListeners = append(b.appliedListeners, fn)
}

// OnApplyRecords registers fn to fire after each successful inbound
// apply, with the just-applied changeset's record slice. Used by the
// notify dispatcher. Records belong to the decoded Changeset and are
// stable for the duration of the call. Listeners must not block.
func (b *Broker) OnApplyRecords(fn func(crdt.Origin, crdt.Seq, []crdt.Record)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.applyRecordsListeners = append(b.applyRecordsListeners, fn)
}

// OnApplyStart registers fn to fire at the very top of applyPayload,
// before any decoding, with the raw payload bytes. Used by latency
// tracing to timestamp subscribe-side arrival. Listeners must not
// block. Single-listener-typical; payload aliases the transport's
// buffer.
func (b *Broker) OnApplyStart(fn func(payload []byte)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.applyStartListeners = append(b.applyStartListeners, fn)
}

func (b *Broker) fireApplyStart(payload []byte) {
	b.mu.Lock()
	listeners := b.applyStartListeners
	b.mu.Unlock()
	for _, fn := range listeners {
		fn(payload)
	}
}

// SetApplyTrace installs an optional fine-grained tracer for the apply
// path. fn fires twice per call: stage="post-decode" right after
// crdt.Decode, stage="post-dml" right after the applyRecords helper
// returns. Used by latency tracing tests; nil means no tracing. Set
// before the broker starts; not safe to change at runtime.
func (b *Broker) SetApplyTrace(fn func(stage string)) { b.applyTrace = fn }

func (b *Broker) fireApplied(origin crdt.Origin, seq crdt.Seq) {
	b.mu.Lock()
	listeners := b.appliedListeners
	b.mu.Unlock()
	for _, fn := range listeners {
		fn(origin, seq)
	}
}

func (b *Broker) fireApplyRecords(origin crdt.Origin, seq crdt.Seq, records []crdt.Record) {
	b.mu.Lock()
	listeners := b.applyRecordsListeners
	b.mu.Unlock()
	for _, fn := range listeners {
		fn(origin, seq, records)
	}
}

// LastSubscribeError returns the most recent non-cancellation error
// returned by the inbound transport subscription loop, if any.
func (b *Broker) LastSubscribeError() error {
	b.errMu.Lock()
	defer b.errMu.Unlock()
	return b.lastSubscribeErr
}

func (b *Broker) setLastSubscribeError(err error) {
	b.errMu.Lock()
	b.lastSubscribeErr = err
	b.errMu.Unlock()
}

// OriginInboundHealth is one remote origin's inbound-apply state:
// highest applied seq, the cache's applied tip (>= AppliedSeq when
// gaps exist), the HLC carried by the last apply, and its wall-clock
// time (zero until the first apply this process).
type OriginInboundHealth struct {
	Origin      crdt.Origin
	AppliedSeq  crdt.Seq
	AppliedTip  crdt.Seq
	LastHLC     crdt.Clock
	LastApplied time.Time
}

// InboundHealth is a poll-friendly snapshot of the inbound apply path.
// ConsecutiveLocked is the retry loop's current "database is locked"
// streak on the payload it is holding (0 = healthy); ApplyStalled is
// that streak crossing applyStallWarnAfter — the wedge indicator.
// SelfHeals counts AppApply connection-state resets since start.
// QuarantineResident is the number of received-but-unapplied changesets
// parked in apply_quarantine (0 = fully applied through the frontier);
// QuarantineOldest is the oldest resident entry's quarantine time and
// QuarantineMaxAttempts its highest re-apply attempt count — a
// steady-state non-empty quarantine with climbing attempts means an
// entry is waiting on a dependency that is never arriving and needs
// operator attention.
type InboundHealth struct {
	Origins               []OriginInboundHealth
	LastSubscribeError    string
	SchemaUnhealthy       bool
	SchemaUnhealthySeq    uint64
	SchemaUnhealthyReason string
	ConsecutiveLocked     int
	ApplyStalled          bool
	SelfHeals             uint64
	QuarantineResident    int
	QuarantineOldest      time.Time
	QuarantineMaxAttempts int64
}

// InboundHealth snapshots per-origin apply progress plus the retry
// loop's stall state. One slice allocation per call; intended to be
// polled every ~30s by an embedding orchestrator.
func (b *Broker) InboundHealth() InboundHealth {
	h := InboundHealth{
		ConsecutiveLocked: int(b.lockedStreak.Load()),
		SelfHeals:         b.selfHeals.Load(),
	}
	h.ApplyStalled = h.ConsecutiveLocked >= applyStallWarnAfter
	if err := b.LastSubscribeError(); err != nil {
		h.LastSubscribeError = err.Error()
	}
	b.appliedMu.RLock()
	if len(b.applied) > 0 {
		h.Origins = make([]OriginInboundHealth, 0, len(b.applied))
		for o, info := range b.applied {
			h.Origins = append(h.Origins, OriginInboundHealth{
				Origin: o, AppliedSeq: info.seq,
				LastHLC: info.hlc, LastApplied: info.at,
			})
		}
	}
	b.appliedMu.RUnlock()
	if b.cfg.Cache != nil {
		for i := range h.Origins {
			h.Origins[i].AppliedTip = b.cfg.Cache.AppliedTip(h.Origins[i].Origin)
		}
	}
	if b.cfg.Meta != nil {
		if health, unhealthy, err := b.cfg.Meta.GetSchemaHealth(); err == nil && unhealthy {
			h.SchemaUnhealthy = true
			h.SchemaUnhealthySeq = health.Seq
			h.SchemaUnhealthyReason = health.Reason
		}
		if qs, err := b.cfg.Meta.QuarantineStats(); err == nil {
			h.QuarantineResident = qs.Resident
			h.QuarantineMaxAttempts = qs.MaxAttempts
			if qs.OldestUs > 0 {
				h.QuarantineOldest = time.UnixMicro(qs.OldestUs)
			}
		}
	}
	return h
}

// Start spawns the subscribe goroutine. Returns immediately; the
// goroutine runs until ctx is cancelled or Close is called. Calling
// Start more than once is a programming error and returns an error;
// Close after Start is idempotent.
func (b *Broker) Start(ctx context.Context) error {
	if b.cfg.Transport == nil {
		return errors.New("broker: nil transport")
	}
	started := false
	b.startOnce.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		b.cancel = cancel
		b.wg.Add(1)
		go b.subscribeLoop(runCtx)
		if b.cfg.SchemaLog != nil && b.cfg.SchemaCatchupInterval > 0 {
			b.wg.Add(1)
			go b.schemaCatchupLoop(runCtx)
		}
		// Gap-fill planner: spawned only when a GapFiller is wired.
		// Tips are read directly from the local cache (origins we've
		// observed from any peer); cfg.TipSource (when set) supplies
		// tips for origins the cache has never seen.
		if b.cfg.GapFiller != nil {
			b.kickFetcher() // boot wake: run one round immediately
			b.wg.Add(1)
			go b.fetcherLoop(runCtx)
		}
		started = true
	})
	if !started {
		return errors.New("broker: already started")
	}
	return nil
}

// Close stops the broker. Blocks until the subscribe loop exits. Safe
// to call before Start (no-op) and safe to call multiple times.
//
// Cancel alone doesn't unblock a goroutine that's mid-cgo inside
// SQLite (e.g. fetcherLoop applying a payload), so after cancelling
// we also fire sqlite3_interrupt on AppApply. That forces any
// inflight statement to return SQLITE_INTERRUPT, which propagates
// up the apply path and lets the round unwind so the goroutine can
// observe ctx.Done. Without it, broker.Close hangs the entire
// Node.Close sequence on the wg.Wait below whenever a fetch round
// is mid-flight under -race or other slow-cgo conditions.
func (b *Broker) Close() error {
	b.closeOnce.Do(func() {
		if b.cancel != nil {
			b.cancel()
		}
		if b.cfg.AppApply != nil {
			b.cfg.AppApply.Interrupt()
		}
		b.wg.Wait()
		b.finalizeCachedStmts()
	})
	return nil
}

// finalizeCachedStmts finalizes every cached prepared statement (per-table
// apply DML and unique-arbitration reads/writes) and empties the caches so the
// next use re-prepares. Finalizing also implicitly resets a
// statement abandoned at SQLITE_ROW, releasing the read snapshot it pinned
// on AppApply. Shared by Close and selfHealApplyConn.
func (b *Broker) finalizeCachedStmts() {
	b.stmtsMu.Lock()
	defer b.stmtsMu.Unlock()
	for id, stmt := range b.applyInsertStmts {
		_ = stmt.Finalize()
		delete(b.applyInsertStmts, id)
	}
	for id, stmt := range b.applyDeleteStmts {
		_ = stmt.Finalize()
		delete(b.applyDeleteStmts, id)
	}
	for k, stmt := range b.uniqSelectStmts {
		_ = stmt.Finalize()
		delete(b.uniqSelectStmts, k)
	}
	for k, stmt := range b.uniqNullStmts {
		_ = stmt.Finalize()
		delete(b.uniqNullStmts, k)
	}
	for k, stmt := range b.uniqReadStmts {
		_ = stmt.Finalize()
		delete(b.uniqReadStmts, k)
	}
}

// selfHealApplyConn resets the broker-owned sqlite3-level state on AppApply
// after a sustained "database is locked" streak: finalizes every cached
// prepared statement (a cached SELECT abandoned at SQLITE_ROW pins a read
// snapshot on AppApply; once any other connection advances the WAL, every
// BEGIN IMMEDIATE fails SQLITE_BUSY_SNAPSHOT forever) and rolls back any
// transaction left open on the connection.
//
// A full close+reopen is deliberately NOT attempted: Config.AppApply is
// caller-owned (open flags, pragmas, no producer hooks by contract, the
// pointer is shared with Node-side helpers) and sqlitebridge has no reopen.
// Finalize-all + ROLLBACK releases every connection-level resource the
// broker itself can have leaked, which is the strongest reset the API
// allows without invalidating the shared *Conn.
//
// Takes applyMu so no apply / catchup / reconcile pass is mid-statement
// while its statement is finalized. Callers must NOT hold applyMu.
func (b *Broker) selfHealApplyConn() {
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
	b.finalizeCachedStmts()
	if b.cfg.AppApply != nil && !b.cfg.AppApply.InAutocommit() {
		if err := b.cfg.AppApply.Exec("ROLLBACK"); err != nil {
			b.log.Warn("broker: self-heal rollback of open AppApply txn failed", "err", err)
		}
	}
}

// errSubscribeClosed marks the transport subscription reporting closure
// (transport.ErrClosed, or a nil return from transports predating it)
// while the broker is still running — the mesh channel closed out
// from under the loop, so live inbound apply has stopped. Surfaced via
// LastSubscribeError + an error log so this can't wedge inbound silently.
var errSubscribeClosed = errors.New("broker: transport subscription closed while broker still running")

func (b *Broker) subscribeLoop(ctx context.Context) {
	defer b.wg.Done()
	for {
		err := b.cfg.Transport.Subscribe(ctx, func(applyCtx context.Context, payload []byte) error {
			return b.applyPayloadWithRetry(applyCtx, payload)
		})
		if ctx.Err() != nil {
			return // shutdown: ctx cancelled
		}
		if err == nil || errors.Is(err, transport.ErrClosed) {
			// The transport (mesh channel or mesh) closed under us while the
			// broker is still running. Inbound apply is now permanently
			// stopped — the broker doesn't own channel re-open, so
			// re-subscribing would busy-loop on the same immediate
			// ErrClosed. Surface it instead of dying silent (the prod
			// failure mode that froze live-only rows like host_capacity).
			// Log before recording: LastSubscribeError becoming visible must
			// imply the Error line already exists (observers poll the former).
			b.log.Error("broker: inbound subscription closed while running; live apply stopped",
				"err", errSubscribeClosed)
			b.setLastSubscribeError(errSubscribeClosed)
			return
		}
		// Subscribe ended with an error while still running: a transport
		// error, or a non-retryable apply error propagated up from
		// applyPayloadWithRetry (which already logged that case with decoded
		// origin/seq context). Record it and re-subscribe. Logged at Debug
		// to avoid duplicating the apply-path Warn for every dropped payload.
		b.setLastSubscribeError(err)
		b.log.Debug("broker: inbound subscription ended; re-subscribing", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(b.retryBackoff):
		}
	}
}

// decodeDot best-effort extracts (origin, seq) from a payload for logging an
// apply failure. Returns zeroes when the payload itself failed to decode.
func decodeDot(payload []byte) (crdt.Origin, crdt.Seq) {
	cs, err := crdt.Decode(payload)
	if err != nil {
		return 0, 0
	}
	return cs.Dot.Origin, cs.Dot.Seq
}

// applyStallWarnAfter is how many consecutive retryable-apply retries on one
// payload elapse before the stall is surfaced at WARN. At the default
// retryBackoff (250ms) this is ~10s — comfortably past the busy_timeout a
// genuinely transient SQLITE_BUSY clears within, so crossing it signals a real
// wedge rather than ordinary contention.
const applyStallWarnAfter = 40

// applySelfHealAfter is how many consecutive retryable "database is locked"
// failures on ONE payload elapse before the broker resets AppApply's
// connection state (selfHealApplyConn). At the default retryBackoff (250ms)
// this is ~30s — past applyStallWarnAfter, so a heal only fires on a stall
// that has already been surfaced at WARN. Re-fires every further
// applySelfHealAfter locked retries in case the first heal didn't take.
const applySelfHealAfter = 120

func (b *Broker) applyPayloadWithRetry(ctx context.Context, payload []byte) error {
	attempts := 0
	lockedStreak := 0
	healed := false
	defer b.lockedStreak.Store(0)
	for {
		err := b.applyPayload(ctx, payload)
		if err == nil {
			origin, seq := decodeDot(payload)
			switch {
			case healed:
				b.log.Info("broker: inbound apply recovered after AppApply self-heal",
					"origin", fmt.Sprintf("%016x", uint64(origin)), "seq", uint64(seq), "attempts", attempts)
			case attempts >= applyStallWarnAfter:
				b.log.Warn("broker: inbound apply recovered after a sustained stall",
					"origin", fmt.Sprintf("%016x", uint64(origin)), "seq", uint64(seq), "attempts", attempts)
			}
			return nil
		}
		origin, seq := decodeDot(payload)
		if !retryableApplyError(err) {
			// Non-retryable (e.g. cluster_id mismatch): the payload is
			// dropped. Log with decoded context — the err itself carries the
			// cluster ids / required-vs-local schema_seq, so the next incident
			// shows which origin/seq stalled and exactly why.
			b.setLastSubscribeError(err)
			b.log.Warn("broker: inbound apply rejected; payload dropped",
				"origin", fmt.Sprintf("%016x", uint64(origin)), "seq", uint64(seq), "err", err)
			return err
		}
		// Retryable (schema-behind / database locked / no such table): held
		// and retried here until it lands or ctx ends. A genuine transient
		// (SQLITE_BUSY clearing within busy_timeout, or a schema gap catch-up
		// is about to close) recovers in a few rounds, so the per-round log
		// stays Debug. But the retry holds this payload AND stops the
		// subscribe loop from draining its deliver buffer — a retryable error
		// that never clears (a stranded write lock, or a schema gap catch-up
		// cannot close) silently wedges the whole topic. Escalate to WARN once
		// past applyStallWarnAfter so that wedge is visible at the default log
		// level instead of only here at Debug + an unpolled LastSubscribeError.
		b.setLastSubscribeError(err)
		attempts++
		if isLockedApplyError(err) {
			lockedStreak++
		} else {
			lockedStreak = 0
		}
		b.lockedStreak.Store(int64(lockedStreak))
		if attempts == applyStallWarnAfter {
			b.log.Warn("broker: inbound apply stalled; a retryable error is not clearing (stranded write lock or unclosed schema gap?)",
				"origin", fmt.Sprintf("%016x", uint64(origin)), "seq", uint64(seq), "err", err)
		} else {
			b.log.Debug("broker: inbound apply deferred; retrying",
				"origin", fmt.Sprintf("%016x", uint64(origin)), "seq", uint64(seq), "err", err)
		}
		// Self-heal: a locked streak this long is never ordinary contention
		// (busy_timeout clears that in a round or two) — it is stranded
		// connection state on AppApply, e.g. a cached statement abandoned at
		// SQLITE_ROW pinning a read snapshot (the 2026-06 prod wedge) or an
		// open txn a failed COMMIT left behind. Reset the broker-owned state
		// so the loop can never wedge forever regardless of cause.
		if lockedStreak > 0 && lockedStreak%applySelfHealAfter == 0 {
			b.log.Warn("broker: inbound apply locked past self-heal threshold; resetting AppApply connection state (finalize cached stmts + rollback open txn)",
				"origin", fmt.Sprintf("%016x", uint64(origin)), "seq", uint64(seq),
				"consecutive_locked", lockedStreak, "err", err)
			b.selfHealApplyConn()
			b.selfHeals.Add(1)
			healed = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(b.retryBackoff):
		}
	}
}

func retryableApplyError(err error) bool {
	if errors.Is(err, errSchemaBehind) {
		return true
	}
	if sqlitebridge.IsCode(err, sqlitebridge.ResultFull) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "no such table") ||
		strings.Contains(s, "database is locked")
}

// isLockedApplyError matches the SQLITE_BUSY / SQLITE_BUSY_SNAPSHOT
// message — the only retryable class the AppApply self-heal can fix.
func isLockedApplyError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "database is locked")
}

func packU64(v uint64) []byte {
	var b [8]byte
	for i := 7; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
	return b[:]
}
