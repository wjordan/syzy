package unique

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/wjordan/syzy/internal/syzylog"
)

// LeaseholderConfig configures a Leaseholder server.
type LeaseholderConfig struct {
	Store      *LeaseStore // lease object store (objectstore-backed)
	Owner      string      // this node's identity (lease owner)
	ListenAddr string      // RPC listen address ("" => an OS-assigned port); ignored when Transport is set

	// Handoff, when set, enables the graceful-shutdown fast path: a leader
	// publishes its taken-set here on clean shutdown and a successor that took
	// over directly adopts it and serves immediately, skipping the failover
	// drain. nil ⇒ every acquisition rebuilds from the replica behind the
	// drain (the crash-failover path). Correctness never depends on it.
	Handoff *HandoffStore

	// Transport supplies the reservation-RPC carrier. When nil the
	// leaseholder binds a private loopback net/rpc listener (single-node /
	// in-process; the published Addr is localhost and only reachable
	// in-process). A clustered node MUST inject a peer-reachable transport
	// (e.g. a mesh channel) so the address published into the
	// lease is one every follower can dial — a localhost address makes
	// cross-node reservation impossible. See ServeTransport (transport.go).
	Transport ServeTransport

	// Enumerate returns one snapshot of the node's local replica (full
	// replication ⇒ authoritative): every active coordinated key identity
	// — including keys with no participating rows — plus the row-backed
	// claims under them. The leaseholder derives its whole taken-set from
	// it every maintenance tick; see reservationTable.ingest.
	Enumerate func(context.Context) (Snapshot, error)

	TTLUS        int64 // lease duration
	DrainUS      int64 // wait after acquisition before serving (failover drain)
	QuarantineUS int64 // reclaim quarantine window
	GraceUS      int64 // GC grace before a row-less reservation is reclaimed
	NowUS        func() int64
}

func (c *LeaseholderConfig) defaults() {
	if c.NowUS == nil {
		c.NowUS = func() int64 { return time.Now().UnixMicro() }
	}
	if c.TTLUS == 0 {
		c.TTLUS = 15_000_000 // 15s
	}
	if c.QuarantineUS == 0 {
		c.QuarantineUS = 30_000_000 // 30s
	}
	if c.GraceUS == 0 {
		// GC reclaims a row-less reservation only after this grace; it must
		// exceed the time for a committed reservation's row to replicate to
		// this leaseholder's replica, so default it to the quarantine bound.
		c.GraceUS = c.QuarantineUS
	}
	if c.DrainUS == 0 {
		// The failover drain must be at least the quarantine window: on
		// takeover, rebuild clears quarantine, so by the time the new leader
		// serves, every value released before the takeover must already be
		// cluster-stable. drain >= quarantine guarantees that.
		c.DrainUS = c.QuarantineUS
	}
}

// Leaseholder serves coordinated-uniqueness reservations while it holds
// the lease. It is the low-latency default backend: clients reach it by
// one mesh round-trip (see LeaseClient). Leadership, durability, and
// failover follow docs/SCHEMA.md#unique-keys — the rows are
// the durable truth, the taken-set is soft state rebuilt on acquisition.
type Leaseholder struct {
	cfg   LeaseholderConfig
	table *reservationTable
	tr    ServeTransport
	addr  string // published reservation-RPC address (from tr.Serve)
	srv   *rpc.Server

	// kick coalesces "a reserve named a key the last enumeration did not
	// cover" signals into one prompt maintenance pass, so a just-created
	// key activates on the client's first retry instead of waiting out
	// the scheduled tick. Buffered 1; extra kicks drop (the retrying
	// client re-kicks until served).
	kick chan struct{}

	mu           sync.Mutex
	isLeader     bool
	needRebuild  bool
	serveAfterUS int64
	lease        LeaseRecord
	etag         string
	dupReported  int              // last duplicate count logged; -1 = nothing logged yet
	dups         []DuplicateValue // last tick's observed duplicates (bounded), for health
}

// DuplicateValue is one coordinated value observed held by more than one
// live row — an out-of-gate duplicate the reservation system cannot
// repair. Grants for the value are fenced until an enumeration shows a
// single owner again; an operator resolves it by deleting or fixing the
// extra row(s), identified by Owners (their PK encodings).
type DuplicateValue struct {
	Table  [16]byte
	Key    [16]byte
	Value  []byte
	Owners [][]byte
}

// maxDuplicateDiagnostics bounds the per-tick duplicate diagnostics kept
// for the health surface; a mass divergence reports the first N values.
const maxDuplicateDiagnostics = 32

// DuplicateValues returns the coordinated values the last enumeration
// observed held by more than one live row (bounded; empty when healthy).
func (l *Leaseholder) DuplicateValues() []DuplicateValue {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dups
}

// NewLeaseholder constructs a Leaseholder. Call Start to bind the RPC
// listener, then RunMaintenance (or drive tick) to contend for the lease.
func NewLeaseholder(cfg LeaseholderConfig) *Leaseholder {
	cfg.defaults()
	return &Leaseholder{
		cfg:         cfg,
		table:       newReservationTable(cfg.NowUS, cfg.QuarantineUS),
		kick:        make(chan struct{}, 1),
		dupReported: -1,
	}
}

// Start binds the reservation-RPC carrier and serves connections. The
// server answers NotLeader until tick/RunMaintenance establishes
// leadership. The published address (Addr) is whatever the transport
// reports — for the mesh transport a peer-dialable bundle URL; for the
// loopback default a local 127.0.0.1 address (in-process only).
func (l *Leaseholder) Start() error {
	l.srv = rpc.NewServer()
	if err := l.srv.RegisterName(rpcServiceName, &leaseRPC{lh: l}); err != nil {
		return fmt.Errorf("unique: leaseholder register: %w", err)
	}
	l.tr = l.cfg.Transport
	if l.tr == nil {
		l.tr = newLoopbackServe(l.cfg.ListenAddr)
	}
	addr, err := l.tr.Serve(func(conn net.Conn) { l.srv.ServeConn(conn) })
	if err != nil {
		return err
	}
	l.addr = addr
	return nil
}

// Addr returns the published reservation-RPC address — the value written
// into the lease record and dialed by every follower's LeaseClient.
func (l *Leaseholder) Addr() string { return l.addr }

// reportDuplicates records and logs coordinated values held by more than
// one live row. Nothing else in the system can observe the condition: no
// node carries a physical index for a coordinated key, apply skips
// arbitration for one, and ingest fences the value rather than absorb it
// silently. The gate cannot repair it; it only refuses further claims on
// the affected values until an enumeration shows a single owner again.
// claims is the full snapshot, dup the second-and-later claimants
// duplicateClaims found in it; the health diagnostics list every owner
// per affected value (bounded).
//
// Logged on transitions only, so a standing fault does not flood the tick.
func (l *Leaseholder) reportDuplicates(claims, dup []Claim) {
	diags := collectDuplicates(claims, dup)
	l.mu.Lock()
	changed := len(dup) != l.dupReported
	l.dupReported = len(dup)
	l.dups = diags
	l.mu.Unlock()
	switch {
	case !changed:
	case len(dup) == 0:
		syzylog.Printf("unique: duplicate coordinated values cleared; fence lifted")
	default:
		c := dup[0]
		syzylog.Printf("unique: %d coordinated value(s) held by more than one live row "+
			"(first: table=%x key=%x owner=%x); grants for these values are fenced — "+
			"reservation cannot repair this; resolve the extra row(s) by hand "+
			"(owners via Node health / Leaseholder.DuplicateValues)",
			len(dup), c.Table, c.Key, c.Owner)
	}
}

// collectDuplicates assembles the health diagnostics for dup: for each
// affected (table, key, value), every owner the snapshot shows holding
// it, capped at maxDuplicateDiagnostics values.
func collectDuplicates(claims, dup []Claim) []DuplicateValue {
	if len(dup) == 0 {
		return nil
	}
	affected := map[string]int{} // claimKey -> index into out
	var out []DuplicateValue
	for i := range dup {
		ck := claimKey(dup[i])
		if _, ok := affected[ck]; ok {
			continue
		}
		if len(out) == maxDuplicateDiagnostics {
			break
		}
		affected[ck] = len(out)
		out = append(out, DuplicateValue{
			Table: dup[i].Table, Key: dup[i].Key,
			Value: append([]byte(nil), dup[i].Value...),
		})
	}
	for i := range claims {
		if idx, ok := affected[claimKey(claims[i])]; ok {
			out[idx].Owners = append(out[idx].Owners, append([]byte(nil), claims[i].Owner...))
		}
	}
	return out
}

// RunMaintenance drives the lease loop until ctx is cancelled: contend
// for the lease, renew it, rebuild/GC the taken-set. On exit it releases
// the lease so a successor can take over immediately.
func (l *Leaseholder) RunMaintenance(ctx context.Context) {
	// The tick is both the lease-renewal cadence and the release-
	// observation cadence: a vacated value starts its release hold only
	// when a tick's enumeration observes the row gone. A quarantine
	// shorter than the lease cadence signals the operator wants faster
	// reuse than TTL/3 would give, so tick at least that often (floored —
	// each tick costs a lease-store round-trip plus an enumeration).
	interval := time.Duration(l.cfg.TTLUS/3) * time.Microsecond
	if q := time.Duration(l.cfg.QuarantineUS) * time.Microsecond; q > 0 && q < interval {
		interval = q
	}
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		l.tick(ctx)
		select {
		case <-ctx.Done():
			l.releaseLease(context.Background())
			return
		case <-t.C:
		case <-l.kick:
		}
	}
}

// tick performs one lease-maintenance step: acquire or renew the lease,
// then (once any drain has elapsed) rebuild the taken-set or GC it.
func (l *Leaseholder) tick(ctx context.Context) {
	now := l.cfg.NowUS()
	l.mu.Lock()
	leader := l.isLeader
	lease, etag := l.lease, l.etag
	l.mu.Unlock()

	if !leader {
		rec, newEtag, err := l.cfg.Store.Acquire(ctx, l.cfg.Owner, l.Addr(), now, l.cfg.TTLUS)
		if errors.Is(err, ErrLeaseHeld) {
			return // a peer leads; our clients route to it via the lease
		}
		if err != nil {
			return // CAS race or transient; retry next tick
		}
		// The failover drain only matters when taking over from a prior
		// generation that may have granted reservations not yet replicated
		// here. The very first leaseholder (generation 1, empty lease) has
		// no predecessor, so it serves as soon as it rebuilds.
		drain := int64(0)
		if rec.Generation > 1 {
			drain = l.cfg.DrainUS
		}
		// Graceful-handoff fast path: if the immediately-prior leader published
		// its taken-set on a clean shutdown, adopt it and serve at once — no
		// rebuild, no drain. Only a direct baton pass (tag == our gen-1)
		// qualifies; anything else falls through to rebuild+drain.
		adopted := l.tryAdoptHandoff(ctx, rec.Generation)
		l.mu.Lock()
		l.isLeader, l.lease, l.etag = true, rec, newEtag
		if adopted {
			l.serveAfterUS, l.needRebuild = now, false
		} else {
			l.serveAfterUS, l.needRebuild = now+drain, true
		}
		l.mu.Unlock()
		if adopted {
			syzylog.Printf("unique: leaseholder adopted handoff at gen=%d (serving immediately, no drain)", rec.Generation)
		} else {
			syzylog.Printf("unique: leaseholder acquired lease gen=%d addr=%s drain=%dus", rec.Generation, l.Addr(), drain)
		}
	} else {
		rec, newEtag, err := l.cfg.Store.Renew(ctx, lease, etag, now, l.cfg.TTLUS)
		if errors.Is(err, ErrFenced) {
			l.mu.Lock()
			l.isLeader, l.needRebuild = false, false
			l.mu.Unlock()
			syzylog.Printf("unique: leaseholder fenced (lost lease) at gen=%d", lease.Generation)
			return
		}
		if err != nil {
			return // transient; the lease may still be ours until it expires
		}
		l.mu.Lock()
		l.lease, l.etag = rec, newEtag
		l.mu.Unlock()
	}
	l.maintain(ctx, now)
}

// maintain derives the taken-set from one enumeration snapshot once the
// drain has elapsed (takeover and steady state are the same path; a
// fresh leadership just clears prior state first), then sweeps
// quarantine. See reservationTable.ingest for the derivation invariant.
func (l *Leaseholder) maintain(ctx context.Context, now int64) {
	l.mu.Lock()
	serving := l.isLeader && now >= l.serveAfterUS
	fresh := serving && l.needRebuild
	l.mu.Unlock()
	if !serving {
		l.table.sweep()
		return
	}

	snap, err := l.cfg.Enumerate(ctx)
	if err != nil {
		syzylog.Printf("unique: leaseholder enumerate: %v", err)
		l.table.sweep()
		return
	}
	// The duplicate check consumes the same snapshot every tick, not only
	// at takeover — a duplicate that replicates in under a stable
	// leaseholder is fenced and reported when it lands.
	dup := duplicateClaims(snap.Claims)
	l.reportDuplicates(snap.Claims, dup)
	if fresh {
		// First derivation of a new leadership: drop anything left from a
		// prior stint. The failover drain already guaranteed every value
		// released before the takeover is cluster-stable, so the
		// quarantine restarts empty.
		l.table.clear()
	}
	l.table.ingest(snap, l.cfg.GraceUS, dup)
	if fresh {
		l.mu.Lock()
		l.needRebuild = false
		gen := l.lease.Generation
		l.mu.Unlock()
		syzylog.Printf("unique: leaseholder serving gen=%d (derived %d reservations, %d keys)",
			gen, len(snap.Claims), len(snap.Keys))
	}
	l.table.sweep()
}

// canServe reports whether this node is the serving leaseholder for gen.
// It includes a soft expiry fence (now < ExpiresAtUS): if a renew has been
// failing transiently and the lease has lapsed, this node stops serving
// even before its next renew tick clears isLeader — so a successor that
// took over after expiry never overlaps with a stale leader. The hard
// generation fence (the renew CAS) is the ultimate guarantee; this closes
// the window between expiry and the next tick.
func (l *Leaseholder) canServe(gen uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.cfg.NowUS()
	return l.isLeader && !l.needRebuild &&
		l.lease.Generation == gen && now >= l.serveAfterUS && now < l.lease.ExpiresAtUS
}

// tryAdoptHandoff loads a predecessor's published taken-set when this
// acquisition is a direct baton pass from it (handoff tag == myGen-1), letting
// the new leader serve immediately. Returns false (→ rebuild+drain) when no
// handoff store is configured, none is published, the read fails, or the tag
// is not exactly the immediately-prior generation.
func (l *Leaseholder) tryAdoptHandoff(ctx context.Context, myGen uint64) bool {
	if l.cfg.Handoff == nil || myGen <= 1 {
		return false
	}
	snap, gen, ok, err := l.cfg.Handoff.Read(ctx)
	if err != nil {
		syzylog.Printf("unique: leaseholder read handoff: %v", err)
		return false
	}
	if !ok || gen != myGen-1 {
		return false
	}
	l.table.load(snap)
	return true
}

func (l *Leaseholder) releaseLease(ctx context.Context) {
	l.mu.Lock()
	leader, lease, etag := l.isLeader, l.lease, l.etag
	l.isLeader = false
	l.mu.Unlock()
	if !leader {
		return
	}
	// Clean shutdown while leading: stop granting and publish the exact
	// taken-set under one lock, so the successor that takes over directly can
	// adopt it and serve with no drain. Then relinquish the lease. A write
	// failure is non-fatal — the successor just rebuilds behind the drain.
	if l.cfg.Handoff != nil {
		snap := l.table.snapshotAndStop()
		if err := l.cfg.Handoff.Write(ctx, lease.Generation, snap); err != nil {
			syzylog.Printf("unique: leaseholder write handoff: %v", err)
		}
	}
	_ = l.cfg.Store.Release(ctx, lease, etag)
}

// Close stops serving and releases the lease.
func (l *Leaseholder) Close() error {
	l.releaseLease(context.Background())
	if l.tr != nil {
		return l.tr.Close()
	}
	return nil
}

// leaseRPC is the net/rpc service the leaseholder exposes.
type leaseRPC struct{ lh *Leaseholder }

func (h *leaseRPC) Reserve(args ReserveArgs, reply *ReserveReply) error {
	if !h.lh.canServe(args.Gen) {
		reply.NotLeader = true
		return nil
	}
	ok, conflict := h.lh.table.reserve(args.Claims)
	if !ok && conflict == nil {
		// Refused without a conflict: a claim named a key outside the last
		// enumeration snapshot (typically a just-created key), or the table
		// stopped mid-handoff. Kick maintenance so a new key activates on
		// the client's first retry rather than the next scheduled tick, and
		// answer NotLeader so the client re-resolves and retries.
		select {
		case h.lh.kick <- struct{}{}:
		default:
		}
		reply.NotLeader = true
		return nil
	}
	reply.OK, reply.Conflict = ok, conflict
	return nil
}

// Owner returns this leaseholder's identity — the value it writes as the
// lease record's Owner. A co-located LeaseClient compares it against the
// live lease to decide whether it can serve reservations in-process.
func (l *Leaseholder) Owner() string { return l.cfg.Owner }

// ReserveLocal serves a reservation in-process, for a LeaseClient sharing
// this leader's process. It runs the identical generation gate and taken-set
// path as the network RPC handler (the same leaseRPC logic), so co-location
// changes only the carrier, never the coordination semantics. The caller
// uses it only when the live lease names this node as Owner: the published
// address is advertised for remote peers and need not be reachable from the
// leader's own host under 1:1 NAT (no hairpin), but the leader and its client
// are one process, so no dial is needed. notLeader is true when this node
// cannot currently serve gen (drain, fence, or mid-handover); the caller
// treats it as retryable, exactly as it treats the RPC NotLeader reply.
func (l *Leaseholder) ReserveLocal(gen uint64, claims []Claim) (ok bool, conflict *Claim, notLeader bool) {
	var reply ReserveReply
	_ = (&leaseRPC{lh: l}).Reserve(ReserveArgs{Gen: gen, Claims: claims}, &reply)
	return reply.OK, reply.Conflict, reply.NotLeader
}
