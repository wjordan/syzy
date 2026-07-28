// Package publisher orchestrates one node's role as the bucket's
// physical-stream publisher. It holds a CAS-renewed lease in HEAD and
// runs two LTX tailers — one over app.db's WAL (db/0000/), one over
// metadata.db's WAL (metadata/0000/) — uploading L0 deltas to S3.
//
// WAL recycle discipline (Litestream-style): SQLite's auto-checkpoint is
// disabled; the publisher pre-drains each tailer, acquires the corresponding
// writer fence, then takes the tailer lock for the last-mile drain, PRAGMA
// wal_checkpoint(TRUNCATE), and position reset. The writer-fence-before-tailer
// order composes with baseline snapshots, while CheckpointUnderLock keeps a
// concurrent Sync from observing the recycled WAL against the stale position.
// Steady state has zero rebaselines because the recycle is gated on tailer
// catch-up by construction.
//
// Lease semantics (described in internal/objstore/layout.go):
//   - Heartbeat every HeartbeatInterval (default 60s) renews
//     ExpiresAtUS via CAS If-Match on HEAD's current ETag.
//   - On startup, if HEAD's publisher is nil or expired, attempt
//     CAS-takeover. Successful takeover requires a fresh baseline LTX
//     before resuming L0 — byte-divergent histories must not mix.
//   - On CAS failure (lease stolen), the controller stops uploading
//     and exits the loop. Caller's responsibility to react (e.g.,
//     restart Node or operator-flagged shutdown).
//   - On clean shutdown, the lease holder CAS-expires HEAD.publisher
//     after final tailer drains/checkpoints so a replacement process
//     can claim immediately while still seeing the previous holder
//     identity. Crashes still rely on LeaseExpiry.
package publisher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/syzylog"
)

// Defaults tuned to match Litestream where applicable.
const (
	DefaultHeartbeatInterval = time.Minute
	DefaultLeaseExpiry       = 5 * time.Minute
	DefaultLTXSyncInterval   = time.Second
	DefaultCompactInterval   = 30 * time.Minute
	DefaultRetentionGrace    = 24 * time.Hour
	// DefaultRebaselineChainBytesRatio takes a fresh coupled baseline once
	// the db delta chain above the current baseline reaches this multiple
	// of the baseline's own size. Bounds cold-restore replay to ~(1+ratio)x
	// the DB. Structural (size-driven), not wall-clock.
	DefaultRebaselineChainBytesRatio = 1.0
	// DefaultRebaselineMaxChainObjects is a failsafe: rebaseline once the
	// chain exceeds this many objects regardless of bytes, so a tiny
	// baseline whose bytes-ratio rarely trips can't let the object count
	// (one restore GET each) balloon unbounded.
	DefaultRebaselineMaxChainObjects = 5000
	// DefaultRebaselineMaxBaselineSkew is a failsafe: rebaseline once the
	// meta baseline leads the db baseline by this many TXIDs, re-coupling
	// the streams. Out-of-band meta WAL recycles (onMetaRecycle) advance
	// only the meta baseline, so repeated recycles skew them apart and a
	// cold restore reconstructs the two streams to different points
	// (row_clock<->app.db orphans). This caps that skew.
	DefaultRebaselineMaxBaselineSkew = 100000
	// DefaultCheckpointInterval is the cadence at which the publisher
	// drains its tailer and issues a coordinated WAL TRUNCATE to keep
	// the WAL bounded. Matches SQLite's default auto-checkpoint
	// cadence at ~1 commit/sec; tune higher under bursty load.
	DefaultCheckpointInterval = time.Minute
	// ltxPublishTimeout bounds a single L0 LTX upload. Anything past
	// this is almost certainly a stuck S3 request (network blackhole,
	// throttling) rather than a slow PUT — bailing lets the next Sync
	// re-encode and retry instead of wedging the producer's commit
	// pipeline behind it.
	ltxPublishTimeout = time.Minute
)

// metadata.db.meta keys owned by this package.
const (
	// MetaKeyParentAppTXID is stamped by the snapshotter inside every
	// metadata.db tx: the app.db bucket TXID at which the snapshot's
	// frontier/clock state was captured. Restore reads this from the
	// applied metadata.db chain and ensures the app.db chain has
	// advanced through it before considering the restore consistent.
	MetaKeyParentAppTXID = "parent_app_txid"
)

// BaselineFunc encodes both app.db and metadata.db at a single
// writer-barrier-pinned moment as snapshot LTXes stamped with
// MaxTXID=txid. The publisher allocates txid above both stream
// counters BEFORE invoking it (allocate-before-pin): every L0 drained
// after the allocation sorts above the baseline, and every L0 at or
// below it holds only commits the later pin also captures. Caller
// must invoke cleanup when the staged buffers are no longer needed.
//
// Used on initial claim, lease takeover, structural rebaseline, and
// online/operator publication (Node.PublishSnapshot).
type BaselineFunc func(ctx context.Context, txid uint64) (appLTX, metaLTX []byte, cleanup func(), err error)

// MetaBaselineFunc encodes metadata.db alone as a snapshot LTX
// stamped with MaxTXID=txid, under the same allocate-before-pin
// contract. Used on an out-of-band metadata WAL recycle
// (onMetaRecycle), where only the metadata stream needs a fresh
// anchor.
type MetaBaselineFunc func(ctx context.Context, txid uint64) (metaLTX []byte, cleanup func(), err error)

// CheckpointFunc runs PRAGMA wal_checkpoint(<mode>) under the writer fence
// appropriate for that connection (writeMu for app.db, Store.mu for
// metadata.db). underFence, when non-nil, runs inside that fence and receives
// the checkpoint operation so it can place the LTX tailer's last-mile drain,
// checkpoint, and position reset under the tailer lock without inverting the
// global writer-fence-before-tailer order. It must call checkpoint exactly once
// or return an error without calling it. mode is one of "PASSIVE", "FULL",
// "RESTART", "TRUNCATE" (case-insensitive).
type CheckpointFunc func(ctx context.Context, mode string, underFence func(checkpoint func() error) error) error

// Config wires a Publisher to its dependencies.
type Config struct {
	Backend   objectstore.Bucket
	ClusterID string // hex-encoded
	NodeID    string // identity in the lease (e.g. node hostname or origin hex)

	// WALPath is app.db's WAL — tailed for the db/ stream.
	WALPath string
	// MetaWALPath is metadata.db's WAL — tailed for the metadata/ stream.
	MetaWALPath string

	// Baseline produces both stream baselines under one barrier-pin.
	// Called on every lease claim (fresh, takeover, or resume).
	Baseline BaselineFunc

	// MetaBaseline produces metadata.db's baseline LTX alone. Called
	// on an out-of-band metadata WAL recycle.
	MetaBaseline MetaBaselineFunc

	// LocalFreshAtOpen marks a node that opened with an empty local DB
	// AND has no peer transport to catch up from — i.e. a single-node
	// bucket deployment whose local state is the only source. A foreign
	// lease takeover then refuses to rebaseline (ErrBehindBucket) when
	// the bucket already has a baseline, since publishing the empty
	// snapshot would abandon the bucket's data; the caller must restore
	// first. Cleared once the local DB is non-empty (restored) or a
	// transport exists (a fresh peer catches up via the broker before
	// it could take over, so its rebaseline reflects real data).
	LocalFreshAtOpen bool

	// AppCheckpoint and MetaCheckpoint run PRAGMA wal_checkpoint on
	// the corresponding connection under its writer fence. Required
	// for the publisher's checkpoint loop to coordinate WAL recycling
	// with tailer catch-up. Their under-fence hook lets the publisher
	// hold the tailer lock across the last drain, checkpoint, and position
	// reset, after the writer fence has already been acquired.
	AppCheckpoint  CheckpointFunc
	MetaCheckpoint CheckpointFunc

	HeartbeatInterval time.Duration
	LeaseExpiry       time.Duration
	LTXSyncInterval   time.Duration

	// ClaimSettle, when >0, makes a successful lease-claim CAS wait
	// this long and then re-read HEAD, proceeding only if this node
	// is still the recorded holder. Object stores with multi-region
	// replication (e.g. Tigris) can accept conflicting conditional
	// writes in different regions and resolve them last-writer-wins
	// afterward — observed in production as three nodes all
	// "winning" the same lease generation and interleaving baseline
	// and L0 uploads at colliding keys (a torn, unrestorable chain).
	// The settle re-read happens after the provider's convergence
	// window, so exactly the LWW survivor proceeds to publish.
	ClaimSettle time.Duration

	// CheckpointInterval is the cadence at which the publisher drains
	// each tailer and issues a coordinated wal_checkpoint(TRUNCATE) to
	// keep WALs bounded. Zero → DefaultCheckpointInterval.
	CheckpointInterval time.Duration

	// CompactInterval is the cadence at which L0 files are merged
	// into L1, applied to both streams. Zero disables compaction.
	// Default 30min.
	CompactInterval time.Duration

	// RetentionGrace is how long superseded objects (L0 covered by
	// L1, L1 below baseline) stay in the bucket before deletion.
	// Default 24h.
	RetentionGrace time.Duration

	// RebaselineChainBytesRatio, RebaselineMaxChainObjects, and
	// RebaselineMaxBaselineSkew govern the structural coupled-rebaseline
	// trigger (see maybeRebaseline). Each zero value falls back to the
	// matching Default*. The check runs on the compactor cadence.
	RebaselineChainBytesRatio float64
	RebaselineMaxChainObjects int
	RebaselineMaxBaselineSkew uint64

	Logger *slog.Logger
}

func (c *Config) defaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if c.LeaseExpiry == 0 {
		c.LeaseExpiry = DefaultLeaseExpiry
	}
	if c.LTXSyncInterval == 0 {
		c.LTXSyncInterval = DefaultLTXSyncInterval
	}
	if c.CheckpointInterval == 0 {
		c.CheckpointInterval = DefaultCheckpointInterval
	}
	if c.CompactInterval == 0 {
		c.CompactInterval = DefaultCompactInterval
	}
	if c.RetentionGrace == 0 {
		c.RetentionGrace = DefaultRetentionGrace
	}
	if c.RebaselineChainBytesRatio == 0 {
		c.RebaselineChainBytesRatio = DefaultRebaselineChainBytesRatio
	}
	if c.RebaselineMaxChainObjects == 0 {
		c.RebaselineMaxChainObjects = DefaultRebaselineMaxChainObjects
	}
	if c.RebaselineMaxBaselineSkew == 0 {
		c.RebaselineMaxBaselineSkew = DefaultRebaselineMaxBaselineSkew
	}
	if c.Logger == nil {
		c.Logger = syzylog.Default()
	}
}

// Publisher is the lease-holding controller. One per Node opened
// with a non-nil ObjectBackend.
//
// Two LTX tailers run side by side under one lease (tailer for
// app.db → db/0000/, metaTailer for metadata.db → metadata/0000/).
// Both have in-memory TXID counters seeded from the bucket at
// takeover; the publisher's checkpoint loop coordinates WAL recycles
// with tailer catch-up so no state needs to persist across restarts.
type Publisher struct {
	cfg Config
	now func() time.Time

	// leading is true while this controller holds the lease and is
	// actively publishing (between a successful claimOrTakeover and Run's
	// return). Read by Node.HoldsPublisherLease to gate the standby WAL
	// checkpoint, which must not run a bare TRUNCATE while the app tailer
	// is live.
	leading atomic.Bool

	// retainOnStop, when set before this controller's context is cancelled,
	// suppresses the lease release on exit. A daemon-role handoff sets it so a
	// same-NodeID successor resumes the still-valid lease (the resume branch in
	// claimOrTakeover) without a window in which a peer sees the lease expired
	// and force-takes-over with a full rebaseline.
	retainOnStop atomic.Bool

	mu                 sync.Mutex
	generation         uint64
	leaseExpiresAt     time.Time
	appSyncsAtRenewal  uint64
	metaSyncsAtRenewal uint64
	// leadCancel belongs to generation and lets any fenced leader callback
	// terminate the whole claim immediately when it observes ownership loss.
	leadCancel  context.CancelCauseFunc
	leadCtx     context.Context
	leadOps     *sync.WaitGroup
	acceptOps   bool
	watchdog    *time.Timer
	watchdogSeq uint64
	// baselineMu serializes capture order with pointer promotion order across
	// coupled, metadata-only, structural, and external baselines.
	baselineMu sync.Mutex

	// app and meta are the two WAL→LTX→bucket pipelines this lease
	// drives. Identical machinery; the stream struct carries the
	// bindings that differ.
	app  stream
	meta stream

	metaTXIDCounter atomic.Uint64
	txidMu          sync.Mutex

	// lastBucketTXID is the in-memory app-stream TXID counter. Seeded
	// from MaxLTXTXID(db/) at lease claim/takeover; advanced by every
	// allocBucketTXID call (L0 emit and baseline alike). seeded closes
	// only after the claim baseline and generation operation gate are
	// ready, so external snapshots cannot race ahead of either.
	lastBucketTXID atomic.Uint64
	seeded         chan struct{}
	runDone        chan struct{}
	runDoneOnce    sync.Once

	// recycleMu serializes WAL-recycle rebaseline across the two
	// tailers (each can detect a recycle independently). recycling
	// gates SyncAppStream so beforeTx doesn't recurse back into the
	// app tailer's broken Sync while a recycle handler is mid-flight.
	recycleMu sync.Mutex
	recycling atomic.Bool

	stats statsTracker
}

// stream is one WAL→LTX→bucket pipeline. The publisher runs two under
// a single lease — app.db → db/0000/ and metadata.db → metadata/0000/.
// The machinery (start/stop/publish/recycle/checkpoint) is shared;
// this struct carries what differs between the two.
type stream struct {
	label    string // "app" | "meta", for logs and errors
	walPath  string
	prefix   string        // objstore stream prefix (db/ or metadata/)
	nextTXID func() uint64 // per-stream monotonic TXID counter
	// record sinks one L0 publish outcome into the stats tracker.
	record func(minTXID, maxTXID uint64, size int64, dur time.Duration, err error)
	// rebaseline re-anchors after an out-of-band WAL recycle: coupled
	// (both streams) for app.db, metadata-only for metadata.db.
	rebaseline func(ctx context.Context) error
	// fence runs wal_checkpoint under the connection's writer fence;
	// nil when checkpointing isn't wired (no coordinated recycle).
	fence CheckpointFunc

	// Live tailer state. Guarded by Publisher.mu.
	tailer *ltxstream.Tailer
	stop   context.CancelFunc
	done   chan struct{}
}

// SyncAppStream drains the app.db LTX tailer, blocking until any
// pending WAL frames have been encoded and shipped. Snapshotter calls
// this before stamping parent_app_txid so the stamp covers everything
// reachable from the cache state being snapshotted.
//
// Returns nil immediately if a defensive rebaseline is in flight (the
// out-of-band onAppRecycle / onMetaRecycle paths). parent_app_txid is
// monotonic via lastBucketTXID, so a one-off skip during recycle
// doesn't break ordering.
func (p *Publisher) SyncAppStream(ctx context.Context) error {
	if p.recycling.Load() {
		return nil
	}
	opCtx, done, err := p.beginLeadershipOp(ctx)
	if errors.Is(err, errLeaseLost) {
		return nil
	}
	if err != nil {
		return err
	}
	defer done()
	t := p.streamTailer(&p.app)
	if t == nil {
		return nil
	}
	err = t.Sync(opCtx)
	// Leadership loss is a standby transition, not a fatal snapshotter error.
	// Preserve cancellation from the actual caller and genuine tailer errors;
	// only hide cancellation injected by the admitted generation context.
	if err != nil && ctx.Err() == nil && opCtx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return nil
	}
	return err
}

// LastBucketTXID returns the most recently allocated app-stream TXID
// — the high-water mark of the app.db LTX stream. Snapshotter stamps
// this into parent_app_txid. Returns 0 before any allocation.
func (p *Publisher) LastBucketTXID() uint64 { return p.lastBucketTXID.Load() }

// Stats returns a snapshot of the publisher's local state. Safe for
// concurrent use.
func (p *Publisher) Stats() Stats { return p.stats.snapshot() }

// Leading reports whether this controller currently holds the lease and is
// actively publishing (app tailer live). False while waiting to acquire the
// lease or after it is lost.
func (p *Publisher) Leading() bool { return p.leading.Load() }

func (p *Publisher) clockNow() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

// publisherIdentity returns the exact lease generation currently assigned to
// this controller. The generation is installed immediately after claim CAS,
// before any baseline pointer is allowed to move.
func (p *Publisher) publisherIdentity() (objstore.PublisherIdentity, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.generation == 0 {
		return objstore.PublisherIdentity{}, false
	}
	return objstore.PublisherIdentity{NodeID: p.cfg.NodeID, Generation: p.generation}, true
}

func (p *Publisher) cancelLeadership(err error) {
	p.mu.Lock()
	cancel := p.leadCancel
	p.mu.Unlock()
	if cancel != nil {
		cancel(err)
	}
}

// beginLeadershipOp admits one externally initiated lease-scoped mutation.
// Closing acceptOps under p.mu linearizes teardown against WaitGroup.Add, so
// teardown can cancel and join every admitted operation before releasing HEAD.
func (p *Publisher) beginLeadershipOp(ctx context.Context) (context.Context, func(), error) {
	p.mu.Lock()
	if !p.acceptOps || p.leadCtx == nil || p.leadOps == nil {
		p.mu.Unlock()
		return nil, nil, errLeaseLost
	}
	leadCtx, ops := p.leadCtx, p.leadOps
	ops.Add(1)
	p.mu.Unlock()

	opCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(leadCtx, func() { cancel(context.Cause(leadCtx)) })
	mutationCtx, finishMutation, err := p.leaseMutationContext(opCtx)
	if err != nil {
		stop()
		cancel(nil)
		ops.Done()
		return nil, nil, err
	}
	done := func() {
		finishMutation()
		stop()
		cancel(nil)
		ops.Done()
	}
	return mutationCtx, done, nil
}

// leaseMutationContext rejects new immutable uploads and destructive
// maintenance once fewer than one heartbeat interval remains. Active
// generation operations derive from leadCtx, whose watchdog is reset after
// every successful renewal. The initial claim baseline runs before that
// watchdog exists, so it receives its own deadline.
func (p *Publisher) leaseMutationContext(parent context.Context) (context.Context, func(), error) {
	p.mu.Lock()
	leaseExpiresAt := p.leaseExpiresAt
	watchdogActive := p.leadCtx != nil
	p.mu.Unlock()
	now := p.clockNow()
	deadline := leaseExpiresAt.Add(-p.cfg.HeartbeatInterval)
	if leaseExpiresAt.IsZero() || !now.Before(deadline) {
		return nil, nil, p.fatalPipelineHealth(now, leaseExpiresAt, errors.New("lease mutation safety window closed"))
	}
	if watchdogActive {
		return parent, func() {}, nil
	}
	ctx, cancel := context.WithTimeout(parent, deadline.Sub(now))
	return ctx, cancel, nil
}

// mutationBackend places the wall-clock fence at the last locally controlled
// point before each remote mutation. This closes the process-pause race where
// a timer callback and a resumed operation could otherwise run in either order.
// A request already accepted by the backend remains irrevocable.
func (p *Publisher) mutationBackend() objectstore.Bucket {
	return &objstore.GuardedBucket{Bucket: p.cfg.Backend, Check: p.checkMutationWindow}
}

func (p *Publisher) checkMutationWindow() error {
	p.mu.Lock()
	leaseExpiresAt := p.leaseExpiresAt
	leadCtx := p.leadCtx
	p.mu.Unlock()
	if leadCtx != nil && leadCtx.Err() != nil {
		if cause := context.Cause(leadCtx); errors.Is(cause, errLeaseLost) || errors.Is(cause, errPublisherUnhealthy) {
			return cause
		}
	}
	now := p.clockNow()
	if leaseExpiresAt.IsZero() || !now.Before(leaseExpiresAt.Add(-p.cfg.HeartbeatInterval)) {
		return p.fatalPipelineHealth(now, leaseExpiresAt, errors.New("lease mutation safety window closed before backend mutation"))
	}
	return nil
}

// resetLeaseWatchdog arms the generation-wide self-fence at one heartbeat
// interval before local expiry. A sequence token makes callbacks from replaced
// timers harmless.
func (p *Publisher) resetLeaseWatchdog() {
	p.mu.Lock()
	p.resetLeaseWatchdogLocked()
	p.mu.Unlock()
}

func (p *Publisher) resetLeaseWatchdogLocked() {
	if p.leadCancel == nil {
		return
	}
	p.watchdogSeq++
	seq := p.watchdogSeq
	if p.watchdog != nil {
		p.watchdog.Stop()
	}
	gen := p.generation
	expiresAt := p.leaseExpiresAt
	delay := expiresAt.Add(-p.cfg.HeartbeatInterval).Sub(p.clockNow())
	if delay < 0 {
		delay = 0
	}
	p.watchdog = time.AfterFunc(delay, func() { p.leaseWatchdogFired(seq, gen) })
}

func (p *Publisher) leaseWatchdogFired(seq, gen uint64) {
	p.mu.Lock()
	if seq != p.watchdogSeq || gen != p.generation || p.leadCancel == nil {
		p.mu.Unlock()
		return
	}
	now := p.clockNow()
	expiresAt := p.leaseExpiresAt
	deadline := expiresAt.Add(-p.cfg.HeartbeatInterval)
	if now.Before(deadline) {
		// The clock moved backward. Re-arm against the same current expiry.
		p.watchdog = time.AfterFunc(deadline.Sub(now), func() { p.leaseWatchdogFired(seq, gen) })
		p.mu.Unlock()
		return
	}
	cancel := p.leadCancel
	err := pipelineHealthError(now, expiresAt, errors.New("lease watchdog reached mutation safety deadline"))
	p.mu.Unlock()
	cancel(err)
}

func (p *Publisher) stopLeaseWatchdog() {
	p.mu.Lock()
	p.watchdogSeq++
	if p.watchdog != nil {
		p.watchdog.Stop()
		p.watchdog = nil
	}
	p.mu.Unlock()
}

// PublishCoupledBaseline admits one externally initiated coupled baseline
// (encode, immutable upload, HEAD promotion) into the active generation
// operation gate. The publisher allocates the baseline TXID before prepare
// pins, so a concurrently drained L0 can never sort below the baseline while
// carrying commits the pin missed.
func (p *Publisher) PublishCoupledBaseline(ctx context.Context, prepare BaselineFunc) error {
	if prepare == nil {
		return errors.New("publisher: coupled baseline preparer required")
	}
	select {
	case <-p.seeded:
	case <-p.runDone:
		return errLeaseLost
	case <-ctx.Done():
		return ctx.Err()
	}
	mutationCtx, done, err := p.beginLeadershipOp(ctx)
	if err != nil {
		return err
	}
	defer done()
	err = func() error {
		p.baselineMu.Lock()
		defer p.baselineMu.Unlock()
		txid := p.allocBaselineTXID()
		appLTX, metaLTX, cleanup, err := prepare(mutationCtx, txid)
		if err != nil {
			return err
		}
		defer cleanup()
		return p.publishCoupledBaselines(mutationCtx, txid, appLTX, metaLTX)
	}()
	// Cancellation injected by the generation (lease lost, unhealthy) must
	// surface as its cause, not as the bare context error an interrupted
	// object-store call happens to return.
	if err != nil && ctx.Err() == nil && mutationCtx.Err() != nil {
		if cause := context.Cause(mutationCtx); errors.Is(cause, errLeaseLost) || errors.Is(cause, errPublisherUnhealthy) {
			return cause
		}
	}
	return err
}

// publishCoupledBaselines also serves the initial claim baseline, before Run
// exposes Leading=true. Both paths use the same generation fence.
func (p *Publisher) publishCoupledBaselines(ctx context.Context, txid uint64, appLTX, metaLTX []byte) error {
	expected, ok := p.publisherIdentity()
	if !ok {
		return errLeaseLost
	}
	err := objstore.PublishCoupledBaselinesOwned(ctx, p.mutationBackend(), p.cfg.ClusterID, expected, txid, appLTX, metaLTX)
	return p.fenceMutationError(err)
}

// New returns a Publisher ready to Run. Validates required fields.
func New(cfg Config) (*Publisher, error) {
	cfg.defaults()
	if cfg.HeartbeatInterval <= 0 {
		return nil, errors.New("publisher: HeartbeatInterval must be positive")
	}
	if cfg.LeaseExpiry <= 0 {
		return nil, errors.New("publisher: LeaseExpiry must be positive")
	}
	// The first heartbeat occurs one interval after claim and the local
	// mutation fence closes one interval before expiry. Keep those points
	// strictly ordered so every lease has at least one renewal opportunity.
	if cfg.LeaseExpiry-cfg.HeartbeatInterval <= cfg.HeartbeatInterval {
		return nil, errors.New("publisher: LeaseExpiry must exceed twice HeartbeatInterval")
	}
	if cfg.Backend == nil {
		return nil, errors.New("publisher: Backend required")
	}
	if cfg.ClusterID == "" {
		return nil, errors.New("publisher: ClusterID required")
	}
	if cfg.NodeID == "" {
		return nil, errors.New("publisher: NodeID required")
	}
	if cfg.WALPath == "" {
		return nil, errors.New("publisher: WALPath required")
	}
	if cfg.MetaWALPath == "" {
		return nil, errors.New("publisher: MetaWALPath required")
	}
	if cfg.Baseline == nil {
		return nil, errors.New("publisher: Baseline required")
	}
	if cfg.MetaBaseline == nil {
		return nil, errors.New("publisher: MetaBaseline required")
	}
	p := &Publisher{cfg: cfg, now: time.Now, seeded: make(chan struct{}), runDone: make(chan struct{})}
	p.app = stream{
		label:      "app",
		walPath:    cfg.WALPath,
		prefix:     objstore.DBPrefix,
		nextTXID:   p.allocBucketTXID,
		record:     p.stats.recordL0,
		rebaseline: p.takeCoupledBaselines,
		fence:      cfg.AppCheckpoint,
	}
	p.meta = stream{
		label:      "meta",
		walPath:    cfg.MetaWALPath,
		prefix:     objstore.MetadataPrefix,
		nextTXID:   p.allocMetaTXID,
		record:     p.stats.recordMetaL0,
		rebaseline: p.takeMetaBaselineOnly,
		fence:      cfg.MetaCheckpoint,
	}
	return p, nil
}

// Run drives the publisher loop until ctx is canceled. Returns nil
// on clean shutdown, or an error if a fatal condition was hit (e.g.,
// cluster_id mismatch). Recoverable errors (transient backend
// failures, contended lease) are logged and retried.
// RetainLeaseOnStop makes the next Run exit leave the lease in HEAD intact
// instead of expiring it, so a daemon-role handoff successor (same NodeID)
// resumes it. Set before the controller's context is cancelled.
func (p *Publisher) RetainLeaseOnStop() { p.retainOnStop.Store(true) }

func (p *Publisher) Run(ctx context.Context) error {
	defer p.runDoneOnce.Do(func() { close(p.runDone) })
	if err := p.claimOrTakeover(ctx); err != nil {
		return err
	}
	// One context owns every asynchronous activity for this exact claim. It is
	// intentionally created only after claimOrTakeover has installed the
	// generation and its initial fenced baseline.
	leadCtx, leadCancel := context.WithCancelCause(ctx)
	leadOps := &sync.WaitGroup{}
	p.mu.Lock()
	p.leadCancel = leadCancel
	p.leadCtx = leadCtx
	p.leadOps = leadOps
	p.acceptOps = true
	p.mu.Unlock()
	p.resetLeaseWatchdog()
	var leadWG sync.WaitGroup
	startLeader := func(run func(context.Context)) {
		leadWG.Add(1)
		go func() {
			defer leadWG.Done()
			run(leadCtx)
		}()
	}

	// We hold the lease and own the app tailer from here until Run returns;
	// the standby checkpoint must stand down for the duration.
	p.leading.Store(true)
	p.startStream(leadCtx, &p.app)
	p.startStream(leadCtx, &p.meta)
	// External snapshot preparation may drain the app tailer. Publish readiness
	// therefore becomes visible only after both physical streams are installed.
	select {
	case <-p.seeded:
	default:
		close(p.seeded)
	}
	startLeader(func(ctx context.Context) { p.runCompactorLoop(ctx, p.cfg.CompactInterval) })
	// Sweep cadence is decoupled from the grace window: the cutoff is
	// still Grace (objects must age past it before deletion), but the
	// loop runs hourly so a publisher reclaims aged objects within the
	// hour instead of once per (default 24h) grace window.
	startLeader(func(ctx context.Context) {
		p.runRetentionLoop(ctx, p.cfg.RetentionGrace, retentionSweepInterval(p.cfg.RetentionGrace))
	})
	startLeader(p.runCheckpointLoop)

	heartbeatTick := time.NewTicker(p.cfg.HeartbeatInterval)
	var runErr error
run:
	for {
		select {
		case <-leadCtx.Done():
			if cause := context.Cause(leadCtx); errors.Is(cause, errLeaseLost) || errors.Is(cause, errPublisherUnhealthy) {
				runErr = cause
			}
			break run
		case <-heartbeatTick.C:
			if err := p.heartbeat(leadCtx); err != nil {
				switch {
				case errors.Is(err, errLeaseLost):
					p.cfg.Logger.Warn("publisher: lease lost; stopping")
					leadCancel(err)
					runErr = err
					break run
				case errors.Is(err, errPublisherUnhealthy):
					p.cfg.Logger.Error("publisher: pipeline unhealthy; leaving lease to expire", "err", err)
					leadCancel(err)
					runErr = err
					break run
				}
				p.cfg.Logger.Warn("publisher: heartbeat failed", "err", err)
			}
		}
	}
	heartbeatTick.Stop()

	// Fence the old generation locally before doing any shutdown work. Joining
	// maintenance first prevents compaction, retention, or checkpoint callbacks
	// from overlapping final checkpointing or a successor claim.
	p.mu.Lock()
	p.acceptOps = false
	p.mu.Unlock()
	p.stopLeaseWatchdog()
	leadCancel(nil)
	leadWG.Wait()
	leadOps.Wait()

	if runErr == nil {
		// Best-effort final drain/TRUNCATE reduces restart work and WAL size. The
		// next claim's mandatory coupled baseline is the correctness boundary; a
		// lost or unhealthy owner must not attempt this cleanup.
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := p.checkpointStream(shutCtx, &p.app); err != nil {
			p.logFinalCheckpoint("app", err)
		}
		if err := p.checkpointStream(shutCtx, &p.meta); err != nil {
			p.logFinalCheckpoint("meta", err)
		}
		cancel()
	}
	// Keep stopped tailers addressable through the final coordinated
	// checkpoint above. Their mutexes serialize against Run passes already
	// exiting on leadCtx; only after checkpointing do we join and clear them.
	p.stopStream(&p.app)
	p.stopStream(&p.meta)
	p.mu.Lock()
	p.leadCancel = nil
	p.leadCtx = nil
	p.leadOps = nil
	p.mu.Unlock()

	// Pipeline-health failure leaves the recorded lease untouched. Natural
	// expiry quarantines immutable uploads that may already be in flight.
	if errors.Is(runErr, errPublisherUnhealthy) {
		p.mu.Lock()
		expiresAt := p.leaseExpiresAt
		p.mu.Unlock()
		p.cfg.Logger.Warn("publisher: unhealthy lease retained for natural expiry", "expires_at", expiresAt)
	} else if p.retainOnStop.Load() {
		// Handoff leaves the lease intact so a same-NodeID successor resumes it.
		p.cfg.Logger.Info("publisher: retaining lease across handoff", "generation", p.generation)
	} else {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := p.releaseLease(releaseCtx); err != nil {
			p.cfg.Logger.Warn("publisher: release lease failed", "err", err)
		}
		cancel()
	}
	p.leading.Store(false)
	return runErr
}

// runCheckpointLoop drives a periodic coordinated WAL TRUNCATE for
// both streams. Disabled when neither AppCheckpoint nor MetaCheckpoint
// is wired (single-node mode without a publisher).
func (p *Publisher) runCheckpointLoop(ctx context.Context) {
	if p.cfg.AppCheckpoint == nil && p.cfg.MetaCheckpoint == nil {
		return
	}
	tick := time.NewTicker(p.cfg.CheckpointInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for _, s := range []*stream{&p.app, &p.meta} {
				if err := p.checkpointStream(ctx, s); err != nil {
					p.cfg.Logger.Warn("publisher: "+s.label+" checkpoint", "err", err)
				}
			}
		}
	}
}

// logFinalCheckpoint reports the outcome of a best-effort shutdown checkpoint. A
// WAL recycled out from under the tailer (PrevFrameMismatchError) is the
// expected fallback — the next open takes a fresh baseline — so it logs at INFO;
// any other failure is a real WARN.
func (p *Publisher) logFinalCheckpoint(label string, err error) {
	var pre *ltxstream.PrevFrameMismatchError
	if errors.As(err, &pre) {
		p.cfg.Logger.Info("publisher: final "+label+" checkpoint skipped; next open will rebaseline", "err", err)
		return
	}
	p.cfg.Logger.Warn("publisher: final "+label+" checkpoint", "err", err)
}

// checkpointStream drains s's tailer and issues a coordinated
// wal_checkpoint(TRUNCATE) under the connection's writer fence, then resets
// the tailer position so the next Sync reads the fresh WAL header. No-op when
// s.fence isn't wired or the tailer isn't running.
//
// The bulk pre-drain runs before the fence. The fence is then acquired before
// the tailer's position mutex; under both, the last-mile drain, TRUNCATE, and
// post-recycle position reset run as one unit (CheckpointUnderLock). That lock
// order prevents a cycle with a baseline that holds the writer fence while
// draining the tailer. Resetting the position outside the tailer lock would let
// the Run-loop Sync observe the recycled WAL against the pre-checkpoint
// position and rebaseline on every checkpoint. recycleMu still serializes the
// two streams' recycles against an out-of-band onRecycle rebaseline.
func (p *Publisher) checkpointStream(ctx context.Context, s *stream) error {
	if s.fence == nil {
		return nil
	}
	p.recycleMu.Lock()
	defer p.recycleMu.Unlock()
	t := p.streamTailer(s)
	if t == nil {
		return nil
	}
	// Soak up the bulk of any pending WAL before taking the writer fence. A
	// final drain still runs under both locks to catch commits in this gap.
	if err := t.Drain(ctx); err != nil {
		return fmt.Errorf("pre-drain checkpoint %s tailer: %w", s.label, err)
	}
	// TRUNCATE, not RESTART: RESTART leaves the old frames in the file and the
	// old salt in the header until the next write, so the tailer re-adopts a
	// stale salt and trips a mismatch when the next write bumps it. TRUNCATE
	// empties the WAL, so the next write lays down a fresh header whose salt the
	// tailer adopts cleanly.
	if err := s.fence(ctx, "TRUNCATE", func(checkpoint func() error) error {
		return t.CheckpointUnderLock(ctx, checkpoint)
	}); err != nil {
		return fmt.Errorf("checkpoint %s tailer: %w", s.label, err)
	}
	return nil
}

// streamTailer returns s's live tailer or nil. Read under p.mu.
func (p *Publisher) streamTailer(s *stream) *ltxstream.Tailer {
	p.mu.Lock()
	defer p.mu.Unlock()
	return s.tailer
}

var (
	errLeaseLost          = objstore.ErrPublisherOwnershipLost
	errPublisherUnhealthy = errors.New("publisher: physical pipeline unhealthy near lease expiry")
)

// ErrBehindBucket is returned when a foreign lease takeover would
// rebaseline over an existing bucket baseline from an empty local DB
// (LocalFreshAtOpen). Publishing that baseline would abandon the
// bucket's real data; the caller must restore from the bucket (or
// clear it for a deliberate fresh start) before taking over.
var ErrBehindBucket = errors.New("publisher: local DB is empty but bucket has a baseline; restore before takeover")

// claimOrTakeover attempts to make us the publisher. Returns nil on
// success, or a fatal error. Backs off and retries on transient
// failures; gives up only on cluster_id mismatch or ctx cancel.
func (p *Publisher) claimOrTakeover(ctx context.Context) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		head, etag, err := objstore.LoadHEAD(ctx, p.cfg.Backend)
		var ifMatch *string
		var oldGeneration uint64

		switch {
		case errors.Is(err, objstore.ErrNoHEAD):
			// Empty bucket — first publisher. Resolve cluster_id and
			// retry; the resolve flow stamps a beacon HEAD which the
			// next iteration will read.
			if _, rerr := objstore.ResolveClusterID(ctx, p.cfg.Backend); rerr != nil {
				return fmt.Errorf("resolve cluster_id: %w", rerr)
			}
			continue
		case err != nil:
			p.cfg.Logger.Warn("publisher: load HEAD", "err", err)
			if !sleepWithCtx(ctx, backoff(attempt)) {
				return ctx.Err()
			}
			continue
		}

		if head.ClusterID != p.cfg.ClusterID {
			return fmt.Errorf("%w: HEAD has %s, we are %s", objstore.ErrClusterIDMismatch, head.ClusterID, p.cfg.ClusterID)
		}

		now := p.clockNow().UnixMicro()
		resume := false
		switch {
		case head.Publisher == nil:
			// No publisher recorded (beacon-only bucket) — fresh claim.
			ifMatch = &etag
		case head.Publisher.NodeID == p.cfg.NodeID:
			// We're the previous holder (process restart). The in-memory
			// tailer position died with the predecessor, and the WAL may
			// have been checkpointed since its last ship (the standby
			// TRUNCATE loop, or the predecessor's shutdown truncate racing
			// writes) — committed-but-unshipped txns then live only in the
			// DB file, invisible to a fresh WAL-header read, and the L0
			// chain silently loses their pages. Continuity cannot be
			// proven across a restart, so re-anchor with a coupled
			// baseline exactly like a foreign takeover.
			ifMatch = &etag
			resume = true
			oldGeneration = head.Publisher.Generation
		case head.Publisher.ExpiresAtUS <= now:
			// Lease expired — takeover requires fresh baseline.
			ifMatch = &etag
			oldGeneration = head.Publisher.Generation
		default:
			// Lease still valid and held by someone else — wait.
			wait := time.Duration(head.Publisher.ExpiresAtUS-now)*time.Microsecond + p.cfg.HeartbeatInterval
			p.cfg.Logger.Info("publisher: waiting on lease",
				"holder", head.Publisher.NodeID,
				"wait", wait)
			if !sleepWithCtx(ctx, wait) {
				return ctx.Err()
			}
			continue
		}

		// Refuse to clobber: rebaselining from an empty local DB
		// (LocalFreshAtOpen) would publish an empty snapshot over the
		// bucket's existing baseline, abandoning its data. Checked
		// before the CAS so we don't claim a lease we're about to
		// abandon. A node that restored/replicated the bucket has
		// schema_seq>0 and passes; the only block is the empty disk.
		// A same-node resume is exempt: holding our own origin/daemon
		// state means the local DB is the one that produced the bucket
		// chain (schema_seq can be 0 on single-node configs whose DDL
		// never routes through the schemalog).
		if !resume && head.Baseline != nil && p.cfg.LocalFreshAtOpen {
			return ErrBehindBucket
		}

		newHead := *head
		newHead.Publisher = &objstore.Publisher{
			NodeID:      p.cfg.NodeID,
			Generation:  oldGeneration + 1,
			ExpiresAtUS: now + p.cfg.LeaseExpiry.Microseconds(),
		}
		_, err = objstore.CASHead(ctx, p.cfg.Backend, &newHead, ifMatch)
		if err != nil {
			if errors.Is(err, objectstore.ErrPreconditionFailed) {
				p.cfg.Logger.Info("publisher: CAS contended; retrying")
				if !sleepWithCtx(ctx, backoff(attempt)) {
					return ctx.Err()
				}
				continue
			}
			return fmt.Errorf("CAS HEAD: %w", err)
		}
		claimedExpiresAt := time.UnixMicro(newHead.Publisher.ExpiresAtUS)

		// Settle-verify (Config.ClaimSettle): under multi-region LWW
		// replication the CAS above can "succeed" in several regions
		// at once; re-read after the convergence window and proceed
		// only as the surviving holder.
		if p.cfg.ClaimSettle > 0 {
			if !sleepWithCtx(ctx, p.cfg.ClaimSettle) {
				return ctx.Err()
			}
			cur, _, err := objstore.LoadHEAD(ctx, p.cfg.Backend)
			if err != nil {
				return fmt.Errorf("settle-verify HEAD: %w", err)
			}
			if cur.Publisher == nil || cur.Publisher.NodeID != p.cfg.NodeID ||
				cur.Publisher.Generation != newHead.Publisher.Generation {
				p.cfg.Logger.Warn("publisher: lease claim lost in settle window; rewaiting",
					"claimed_generation", newHead.Publisher.Generation)
				if !sleepWithCtx(ctx, backoff(attempt)) {
					return ctx.Err()
				}
				continue
			}
			claimedExpiresAt = time.UnixMicro(cur.Publisher.ExpiresAtUS)
		}

		p.mu.Lock()
		p.generation = newHead.Publisher.Generation
		p.leaseExpiresAt = claimedExpiresAt
		p.appSyncsAtRenewal = 0
		p.metaSyncsAtRenewal = 0
		p.mu.Unlock()
		p.stats.recordLeaseClaim(newHead.Publisher.Generation)

		// Seed both TXID counters from the bucket BEFORE allocating any
		// new TXIDs (baseline or L0). This is the single source of
		// monotonicity — counters are not persisted in metadata.db.
		if err := p.seedTXIDCounters(ctx); err != nil {
			return fmt.Errorf("seed txid counters: %w", err)
		}
		// Every claim re-anchors both streams: no claimant — fresh,
		// takeover, or same-node resume — can prove its local WAL is
		// contiguous with the bucket chain, and an unproven resume can
		// silently drop checkpointed-but-unshipped txns from db/.
		if err := p.takeCoupledBaselines(ctx); err != nil {
			return fmt.Errorf("coupled baseline on claim: %w", err)
		}
		p.cfg.Logger.Info("publisher: lease claimed",
			"generation", newHead.Publisher.Generation,
			"app_baseline", true,
			"meta_baseline", true)
		return nil
	}
}

// takeCoupledBaselines allocates one TXID, encodes both app.db and
// metadata.db as snapshot LTXes (under one barrier-pin), uploads them
// to db/0009/ and metadata/0009/, then CASes HEAD with both pointers.
// Used on initial claim and lease takeover.
func (p *Publisher) takeCoupledBaselines(ctx context.Context) error {
	t0 := time.Now()
	p.baselineMu.Lock()
	defer p.baselineMu.Unlock()
	mutationCtx, done, err := p.leaseMutationContext(ctx)
	if err != nil {
		return err
	}
	defer done()
	txid := p.allocBaselineTXID()
	appLTX, metaLTX, cleanup, err := p.cfg.Baseline(mutationCtx, txid)
	if err != nil {
		dur := time.Since(t0)
		p.stats.recordAppBaseline(txid, 0, dur, err)
		p.stats.recordMetaBaseline(txid, 0, dur, err)
		return fmt.Errorf("encode coupled baseline: %w", err)
	}
	defer cleanup()
	if err := p.publishCoupledBaselines(mutationCtx, txid, appLTX, metaLTX); err != nil {
		dur := time.Since(t0)
		p.stats.recordAppBaseline(txid, int64(len(appLTX)), dur, err)
		p.stats.recordMetaBaseline(txid, int64(len(metaLTX)), dur, err)
		return fmt.Errorf("publish coupled baselines: %w", err)
	}
	dur := time.Since(t0)
	p.stats.recordAppBaseline(txid, int64(len(appLTX)), dur, nil)
	p.stats.recordMetaBaseline(txid, int64(len(metaLTX)), dur, nil)
	return nil
}

// maybeRebaseline takes a fresh coupled (app+meta) baseline when the db
// delta chain above the current baseline has outgrown a structural
// threshold. Three triggers, in priority order:
//
//   - chain_bytes: chain bytes >= ratio * baseline bytes. The primary,
//     size-driven trigger — bounds cold-restore replay to ~(1+ratio)x the
//     DB regardless of write rate or how long the publisher has been up.
//   - chain_objects: chain object count >= failsafe. Guards a tiny
//     baseline whose bytes-ratio rarely trips while object count (one
//     restore GET each) balloons.
//   - baseline_skew: meta baseline leads the db baseline by >= failsafe
//     TXIDs. Out-of-band meta WAL recycles advance only the meta baseline,
//     so this re-couples the streams and caps the window in which a cold
//     restore could reconstruct them to inconsistent points (orphans).
//
// Runs while leading, on the compactor cadence, after both streams have
// compacted (so the chain it measures is already collapsed). Safe beside
// the live tailers: takeCoupledBaselines pins via the same writer-barrier
// drain protocol as Node.PublishSnapshot, holding writeMu only across the
// pin window. No-op until the init/takeover path has written the first
// baseline.
func (p *Publisher) maybeRebaseline(ctx context.Context) error {
	head, _, err := objstore.LoadHEAD(ctx, p.cfg.Backend)
	if err != nil {
		return err
	}
	if head.Baseline == nil {
		return nil
	}
	dbBaseTXID := head.Baseline.TXID
	dbBaseBytes := head.Baseline.LTXRef.Size

	var chainObjects int
	var chainBytes int64
	for _, level := range []int{objstore.L0Level, objstore.L1Level} {
		files, err := objstore.ListLTX(ctx, p.cfg.Backend, objstore.DBPrefix, level)
		if err != nil {
			return err
		}
		for _, f := range files {
			if f.MaxTXID > dbBaseTXID {
				chainObjects++
				chainBytes += f.Size
			}
		}
	}
	var skew uint64
	if head.MetaBaseline != nil && head.MetaBaseline.TXID > dbBaseTXID {
		skew = head.MetaBaseline.TXID - dbBaseTXID
	}

	reason := ""
	switch {
	case dbBaseBytes > 0 && float64(chainBytes) >= p.cfg.RebaselineChainBytesRatio*float64(dbBaseBytes):
		reason = "chain_bytes"
	case chainObjects >= p.cfg.RebaselineMaxChainObjects:
		reason = "chain_objects"
	case skew >= p.cfg.RebaselineMaxBaselineSkew:
		reason = "baseline_skew"
	default:
		return nil
	}
	p.cfg.Logger.Info("publisher: structural rebaseline",
		"reason", reason,
		"db_baseline_txid", dbBaseTXID,
		"db_baseline_bytes", dbBaseBytes,
		"chain_objects", chainObjects,
		"chain_bytes", chainBytes,
		"baseline_skew", skew)
	return p.takeCoupledBaselines(ctx)
}

// takeMetaBaselineOnly encodes metadata.db as a snapshot LTX and
// CASes HEAD updating only MetaBaseline. Used on an out-of-band
// metadata WAL recycle, where the meta stream alone lost frames and
// needs a fresh anchor.
func (p *Publisher) takeMetaBaselineOnly(ctx context.Context) error {
	t0 := time.Now()
	p.baselineMu.Lock()
	defer p.baselineMu.Unlock()
	mutationCtx, done, err := p.leaseMutationContext(ctx)
	if err != nil {
		return err
	}
	defer done()
	expected, ok := p.publisherIdentity()
	if !ok {
		return errLeaseLost
	}
	txid := p.allocBaselineTXID()
	metaLTX, cleanup, err := p.cfg.MetaBaseline(mutationCtx, txid)
	if err != nil {
		p.stats.recordMetaBaseline(txid, 0, time.Since(t0), err)
		return fmt.Errorf("encode meta baseline: %w", err)
	}
	defer cleanup()
	err = objstore.PublishMetadataBaselineOwned(
		mutationCtx, p.mutationBackend(), p.cfg.ClusterID, expected, txid, metaLTX,
	)
	if err != nil {
		err = p.fenceMutationError(err)
		p.stats.recordMetaBaseline(txid, int64(len(metaLTX)), time.Since(t0), err)
		return fmt.Errorf("publish metadata baseline: %w", err)
	}
	p.stats.recordMetaBaseline(txid, int64(len(metaLTX)), time.Since(t0), nil)
	return nil
}

// allocBaselineTXID picks a TXID strictly greater than every L0/L1/baseline
// TXID seen by both the app and metadata streams. This is what makes a
// baseline the new chain root: the receiver's chain logic skips any L0/L1
// record with MaxTXID <= baseline.TXID, so anything previously shipped on
// either stream sorts BEFORE this baseline and is correctly excluded.
//
// Without this, a baseline that uses only the app counter can land
// at a TXID below the metadata stream's tip — receivers then replay
// pre-baseline metadata L0 records on top of the (newer) baseline and
// time-travel pages backwards, corrupting multi-page overflow chains.
func (p *Publisher) allocBaselineTXID() uint64 {
	p.txidMu.Lock()
	defer p.txidMu.Unlock()
	next := p.lastBucketTXID.Load() + 1
	if metaNext := p.metaTXIDCounter.Load() + 1; metaNext > next {
		next = metaNext
	}
	p.lastBucketTXID.Store(next)
	p.metaTXIDCounter.Store(next)
	return next
}

// seedTXIDCounters primes the in-memory app and metadata TXID counters
// from the bucket's actual L0/L1/baseline tip. Called after lease
// claim/takeover so the next allocation produces a TXID strictly greater
// than anything already shipped — even if a prior publisher crashed
// without persisting any state.
func (p *Publisher) seedTXIDCounters(ctx context.Context) error {
	if err := seedFromBucket(ctx, p.cfg.Backend, objstore.DBPrefix, &p.lastBucketTXID); err != nil {
		return fmt.Errorf("seed app TXID: %w", err)
	}
	if err := seedFromBucket(ctx, p.cfg.Backend, objstore.MetadataPrefix, &p.metaTXIDCounter); err != nil {
		return fmt.Errorf("seed meta TXID: %w", err)
	}
	return nil
}

// seedFromBucket raises counter to max(counter, MaxLTXTXID(prefix,
// all-levels)) so the next Add(1) is guaranteed past anything shipped.
func seedFromBucket(ctx context.Context, be objectstore.Bucket, prefix string, counter *atomic.Uint64) error {
	for _, level := range []int{objstore.L0Level, objstore.L1Level, objstore.BaselineLevel} {
		max, err := objstore.MaxLTXTXID(ctx, be, prefix, level)
		if err != nil {
			return err
		}
		bumpAtomic(counter, max)
	}
	return nil
}

// bumpAtomic raises *counter to at least target. CAS-loops on contention.
func bumpAtomic(counter *atomic.Uint64, target uint64) {
	for {
		cur := counter.Load()
		if target <= cur {
			return
		}
		if counter.CompareAndSwap(cur, target) {
			return
		}
	}
}

// heartbeat verifies exact ownership on every call, then renews only when both
// physical tailers have completed a fresh successful Sync since the preceding
// successful renewal. Proof is consumed only after the renewal CAS lands.
func (p *Publisher) heartbeat(ctx context.Context) error {
	p.mu.Lock()
	gen := p.generation
	leaseExpiresAt := p.leaseExpiresAt
	appTailer, metaTailer := p.app.tailer, p.meta.tailer
	appConsumed, metaConsumed := p.appSyncsAtRenewal, p.metaSyncsAtRenewal
	p.mu.Unlock()

	now := p.clockNow()
	if leaseExpiresAt.IsZero() || !now.Before(leaseExpiresAt) {
		return errLeaseLost
	}
	safetyDeadline := leaseExpiresAt.Add(-p.cfg.HeartbeatInterval)
	if !now.Before(safetyDeadline) {
		return p.fatalPipelineHealth(now, leaseExpiresAt, errors.New("heartbeat reached lease mutation safety deadline"))
	}
	attemptLimit := p.cfg.HeartbeatInterval
	endsAtSafety := false
	if untilSafety := safetyDeadline.Sub(now); untilSafety <= attemptLimit {
		attemptLimit = untilSafety
		endsAtSafety = true
	}
	attemptCtx, cancel := context.WithTimeout(ctx, attemptLimit)
	defer cancel()

	var appSyncs, metaSyncs uint64
	if appTailer != nil {
		appSyncs = appTailer.SuccessfulSyncs()
	}
	if metaTailer != nil {
		metaSyncs = metaTailer.SuccessfulSyncs()
	}
	appFresh, metaFresh := appSyncs > appConsumed, metaSyncs > metaConsumed

	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		head, etag, err := objstore.LoadHEAD(attemptCtx, p.cfg.Backend)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if endsAtSafety && attemptCtx.Err() != nil {
				return p.fatalPipelineHealth(p.clockNow(), leaseExpiresAt, fmt.Errorf("load HEAD before safety deadline: %w", err))
			}
			return p.transientHeartbeatFailure(leaseExpiresAt, fmt.Errorf("load HEAD: %w", err))
		}
		now = p.clockNow()
		if head.Publisher == nil || head.Publisher.NodeID != p.cfg.NodeID ||
			head.Publisher.Generation != gen || head.Publisher.ExpiresAtUS <= now.UnixMicro() {
			return errLeaseLost
		}
		// Ownership is checked on every tick even when proof is missing.
		if !appFresh || !metaFresh {
			if p.renewalWindowClosed(now, leaseExpiresAt) {
				return p.fatalPipelineHealth(now, leaseExpiresAt,
					fmt.Errorf("fresh Sync proof missing (app=%t meta=%t)", appFresh, metaFresh))
			}
			return nil
		}
		// A pause after the HEAD read must not resurrect a generation once its
		// local mutation window has closed. This is the last local check before
		// the conditional write; a request already accepted remotely cannot be
		// revoked.
		now = p.clockNow()
		if p.renewalWindowClosed(now, leaseExpiresAt) {
			return p.fatalPipelineHealth(now, leaseExpiresAt, errors.New("heartbeat safety deadline reached before CAS"))
		}
		if head.Publisher.ExpiresAtUS <= now.UnixMicro() {
			return errLeaseLost
		}

		expiresAt := now.Add(p.cfg.LeaseExpiry)
		newHead := *head
		newPublisher := *head.Publisher
		newPublisher.ExpiresAtUS = expiresAt.UnixMicro()
		newHead.Publisher = &newPublisher
		_, err = objstore.CASHead(attemptCtx, p.mutationBackend(), &newHead, &etag)
		if errors.Is(err, objectstore.ErrPreconditionFailed) {
			// A same-generation baseline may have moved HEAD after our read.
			// Re-read and distinguish that benign race from a real successor.
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if endsAtSafety && attemptCtx.Err() != nil {
				return p.fatalPipelineHealth(p.clockNow(), leaseExpiresAt, fmt.Errorf("CAS HEAD before safety deadline: %w", err))
			}
			return p.transientHeartbeatFailure(leaseExpiresAt, fmt.Errorf("CAS HEAD: %w", err))
		}
		renewed := false
		p.mu.Lock()
		if p.generation == gen {
			p.leaseExpiresAt = expiresAt
			p.appSyncsAtRenewal = appSyncs
			p.metaSyncsAtRenewal = metaSyncs
			p.resetLeaseWatchdogLocked()
			renewed = true
		}
		p.mu.Unlock()
		if !renewed {
			return errLeaseLost
		}
		return nil
	}
	return p.transientHeartbeatFailure(leaseExpiresAt, errors.New("publisher: heartbeat HEAD CAS remained contended"))
}

func (p *Publisher) renewalWindowClosed(now, leaseExpiresAt time.Time) bool {
	return leaseExpiresAt.IsZero() || !now.Add(p.cfg.HeartbeatInterval).Before(leaseExpiresAt)
}

func (p *Publisher) transientHeartbeatFailure(leaseExpiresAt time.Time, cause error) error {
	now := p.clockNow()
	if p.renewalWindowClosed(now, leaseExpiresAt) {
		return p.fatalPipelineHealth(now, leaseExpiresAt, cause)
	}
	return cause
}

func pipelineHealthError(now, leaseExpiresAt time.Time, cause error) error {
	return fmt.Errorf("%w: now=%s lease_expires_at=%s: %w",
		errPublisherUnhealthy, now.UTC().Format(time.RFC3339Nano), leaseExpiresAt.UTC().Format(time.RFC3339Nano), cause)
}

func (p *Publisher) fatalPipelineHealth(now, leaseExpiresAt time.Time, cause error) error {
	err := pipelineHealthError(now, leaseExpiresAt, cause)
	p.cancelLeadership(err)
	return err
}

// fenceMutationError turns immutable-key or pointer-order conflicts into a
// generation-wide health failure. Continuing to renew after either condition
// would let a physically ambiguous chain remain authoritative.
func (p *Publisher) fenceMutationError(err error) error {
	if err == nil || errors.Is(err, errPublisherUnhealthy) {
		return err
	}
	if errors.Is(err, errLeaseLost) {
		p.cancelLeadership(err)
		return err
	}
	if !errors.Is(err, objstore.ErrLTXConflict) && !errors.Is(err, objstore.ErrBaselineRegression) {
		return err
	}
	p.mu.Lock()
	expiresAt := p.leaseExpiresAt
	p.mu.Unlock()
	return p.fatalPipelineHealth(p.clockNow(), expiresAt, fmt.Errorf("physical publication integrity: %w", err))
}

// releaseLease expires HEAD.publisher if this process still owns the
// exact lease generation it claimed. The holder identity is preserved
// so the next claimant can tell a restart from a foreign takeover,
// though every claim re-anchors with a coupled baseline either way
// (see takeCoupledBaselines). A crash leaves the lease in place for
// normal expiry; a CAS conflict means another publisher already moved
// HEAD.
func (p *Publisher) releaseLease(ctx context.Context) error {
	p.mu.Lock()
	gen := p.generation
	p.mu.Unlock()
	if gen == 0 {
		return nil
	}

	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		head, etag, err := objstore.LoadHEAD(ctx, p.cfg.Backend)
		if err != nil {
			if errors.Is(err, objstore.ErrNoHEAD) {
				return nil
			}
			return fmt.Errorf("load HEAD: %w", err)
		}
		if head.Publisher == nil {
			return nil
		}
		if head.Publisher.NodeID != p.cfg.NodeID || head.Publisher.Generation != gen {
			return nil
		}

		next := *head
		next.Publisher = &objstore.Publisher{
			NodeID:      head.Publisher.NodeID,
			Generation:  head.Publisher.Generation,
			ExpiresAtUS: 0,
		}
		if _, err := objstore.CASHead(ctx, p.cfg.Backend, &next, &etag); err != nil {
			if errors.Is(err, objectstore.ErrPreconditionFailed) {
				continue
			}
			return fmt.Errorf("CAS HEAD: %w", err)
		}
		p.cfg.Logger.Info("publisher: lease released", "generation", gen)
		return nil
	}
	return errors.New("publisher: release lease CAS retries exhausted")
}

// startTailer wires the app.db LTX tailer with NextTXID and OnLTX
// callbacks. Always starts from a zero Position — the tailer reads the
// current WAL header on first Sync and emits LTXes for any in-WAL
// frames under TXIDs from the seeded counter. Idempotent at the
// receiver: re-shipped pages overwrite to the same final state.
// startStream wires and runs s's LTX tailer. Always starts from a
// zero Position: the claim/takeover path takes a fresh baseline before
// this fires, so the tailer reads the new WAL header on its first
// Sync. Tailer positions are in-memory only; on publisher restart,
// seedTXIDCounters + a fresh-WAL-header read recover a
// contiguous-with-bucket position.
func (p *Publisher) startStream(parent context.Context, s *stream) {
	cfg := ltxstream.Config{
		WALPath:      s.walPath,
		SyncInterval: p.cfg.LTXSyncInterval,
		Logger:       p.cfg.Logger,
		NextTXID:     s.nextTXID,
		OnLTX: func(ctx context.Context, hdr ltx.Header, body []byte) error {
			return p.publishL0(ctx, s, hdr, body)
		},
		OnRecycle: func(ctx context.Context) (ltxstream.Position, error) { return p.onRecycle(ctx, s) },
	}
	t := ltxstream.New(cfg, ltxstream.Position{})
	ctx, cancel := context.WithCancel(parent)
	p.mu.Lock()
	s.tailer = t
	s.stop = cancel
	s.done = make(chan struct{})
	p.mu.Unlock()
	go func() {
		defer close(s.done)
		_ = t.Run(ctx)
	}()
}

func (p *Publisher) stopStream(s *stream) {
	p.mu.Lock()
	stop, done := s.stop, s.done
	p.mu.Unlock()
	if stop == nil {
		return
	}
	stop()
	<-done
	p.mu.Lock()
	if s.done == done {
		s.tailer = nil
		s.stop = nil
		s.done = nil
	}
	p.mu.Unlock()
}

// publishL0 uploads one L0 LTX to s's stream prefix and records the
// outcome in the stats tracker.
func (p *Publisher) publishL0(ctx context.Context, s *stream, hdr ltx.Header, body []byte) error {
	mutationCtx, done, err := p.leaseMutationContext(ctx)
	if err != nil {
		return err
	}
	defer done()
	ctx, cancel := context.WithTimeout(mutationCtx, ltxPublishTimeout)
	defer cancel()
	t0 := time.Now()
	_, err = objstore.PublishLTX(ctx, p.mutationBackend(), s.prefix, objstore.L0Level, uint64(hdr.MinTXID), uint64(hdr.MaxTXID), body)
	err = p.fenceMutationError(err)
	s.record(uint64(hdr.MinTXID), uint64(hdr.MaxTXID), int64(len(body)), time.Since(t0), err)
	return err
}

// onRecycle is the defensive recovery path for a WAL recycle the
// publisher did not initiate (the steady-state path runs through the
// coordinated checkpoint with the tailer drained first). If we land
// here, frames may have been lost between commit and Sync, so a fresh
// baseline re-anchors the chain: coupled for app.db (both streams),
// metadata-only for metadata.db.
func (p *Publisher) onRecycle(ctx context.Context, s *stream) (ltxstream.Position, error) {
	p.recycleMu.Lock()
	defer p.recycleMu.Unlock()
	p.recycling.Store(true)
	defer p.recycling.Store(false)
	p.cfg.Logger.Warn("publisher: " + s.label + " WAL recycled out of band; rebaselining")
	if err := s.rebaseline(ctx); err != nil {
		return ltxstream.Position{}, err
	}
	// Position{} primes a fresh-WAL-header read on the next Sync.
	return ltxstream.Position{}, nil
}

// allocBucketTXID returns the next monotonic TXID for the app stream.
// The shared allocator mutex prevents a baseline or metadata allocation from
// reserving the same stream TXID concurrently. Values remain in memory only;
// seedTXIDCounters primes lastBucketTXID from the
// bucket's actual L0/L1/baseline tip on takeover, so a crashed
// publisher that lost in-memory state resumes contiguous with what's
// already shipped (re-shipping at most the in-WAL-frames worth of
// content under fresh TXIDs; receivers converge via page-overwrite).
func (p *Publisher) allocBucketTXID() uint64 {
	p.txidMu.Lock()
	defer p.txidMu.Unlock()
	return p.lastBucketTXID.Add(1)
}

func (p *Publisher) allocMetaTXID() uint64 {
	p.txidMu.Lock()
	defer p.txidMu.Unlock()
	return p.metaTXIDCounter.Add(1)
}

// sleepWithCtx returns false if ctx fires first.
func sleepWithCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// backoff returns a bounded duration that grows with attempt count.
func backoff(attempt int) time.Duration {
	const base = 500 * time.Millisecond
	const cap = 30 * time.Second
	d := base * time.Duration(1<<min(attempt, 6))
	if d > cap {
		d = cap
	}
	return d
}
