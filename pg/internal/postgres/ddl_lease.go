package postgres

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrLeaseHeld is returned by Lease.Acquire when a live peer owns the lease.
var ErrLeaseHeld = errors.New("postgres: ddl lease held by another node")

// ErrLeaseLost is returned by Lease.Heartbeat when the holder no longer owns
// the lease (its TTL lapsed and a peer took over).
var ErrLeaseLost = errors.New("postgres: ddl lease lost")

// Lease serializes cluster-wide DDL so schema-log appends never conflict: at
// most one node holds it at a time (§6). Because Postgres commits DDL before
// the sidecar appends the schema event (append-after-commit), two concurrent
// originators could otherwise both commit divergent local DDL and then race to
// append — a conflict that cannot be rolled back. The lease prevents that by
// serializing DDL *before* it commits (via the ddl_command_start gate).
//
// Production backs it with the existing S3 CAS primitive; tests use the
// in-memory backend. Acquire/Heartbeat/Release operate on a shared object
// guarded by a TTL — a crashed holder's lease simply expires and another node
// takes over — and each ownership change bumps a monotonic fencing epoch so a
// paused/zombie holder that lost the lease cannot later be mistaken for the
// owner.
type Lease interface {
	// Acquire claims the lease for holder, returning the fencing epoch held
	// after the claim. It succeeds when the lease is free, already held by
	// holder (renewal, epoch unchanged), or expired (TTL lapsed). It returns
	// ErrLeaseHeld when a live peer owns it.
	Acquire(ctx context.Context, holder string) (epoch uint64, err error)

	// Heartbeat extends holder's TTL while its DDL transaction is alive,
	// returning the current epoch. ErrLeaseLost if holder no longer owns it.
	Heartbeat(ctx context.Context, holder string) (epoch uint64, err error)

	// Release frees the lease iff still held by holder. Idempotent and
	// best-effort: a no-op if holder does not currently own it.
	Release(ctx context.Context, holder string) error
}

// memLease is an in-memory Lease for single-process, multi-engine tests (and a
// single-node default). It models the same TTL + fencing-epoch semantics the S3
// backend enforces via CAS, so engine code that drives a lease is exercised
// without object storage.
type memLease struct {
	mu      sync.Mutex
	ttl     time.Duration
	now     func() time.Time // overridable for deterministic expiry tests
	holder  string
	epoch   uint64
	expires time.Time
}

// NewMemLease returns an in-memory lease whose grants expire ttl after the last
// Acquire/Heartbeat.
func NewMemLease(ttl time.Duration) *memLease { return &memLease{ttl: ttl, now: time.Now} }

func (l *memLease) Acquire(_ context.Context, holder string) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	free := l.holder == "" || now.After(l.expires)
	if !free && l.holder != holder {
		return 0, ErrLeaseHeld
	}
	if l.holder != holder {
		l.epoch++ // ownership change (from free/expired/peer) bumps the fence
		l.holder = holder
	}
	l.expires = now.Add(l.ttl)
	return l.epoch, nil
}

func (l *memLease) Heartbeat(_ context.Context, holder string) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder != holder || l.now().After(l.expires) {
		return 0, ErrLeaseLost
	}
	l.expires = l.now().Add(l.ttl)
	return l.epoch, nil
}

func (l *memLease) Release(_ context.Context, holder string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder == holder {
		l.holder = ""
	}
	return nil
}
