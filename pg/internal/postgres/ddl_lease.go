package postgres

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/unique"
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

// bucketLease adapts the shared object-store lease primitive to the DDL gate.
// It retains the last record and ETag so heartbeats renew the same fencing
// generation; a restart acquires a new generation even when holder is unchanged.
type bucketLease struct {
	mu    sync.Mutex
	store *unique.LeaseStore
	ttlUS int64
	now   func() time.Time
	rec   unique.LeaseRecord
	etag  string
}

// NewBucketLease returns a durable DDL lease stored at key in bucket.
func NewBucketLease(bucket objectstore.Bucket, key string, ttl time.Duration) Lease {
	return &bucketLease{
		store: unique.OpenLease(bucket, key),
		ttlUS: ttl.Microseconds(),
		now:   time.Now,
	}
}

func (l *bucketLease) Acquire(ctx context.Context, holder string) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	nowUS := l.now().UnixMicro()
	if l.rec.Owner == holder && l.rec.Generation != 0 && l.rec.ExpiresAtUS > nowUS {
		rec, etag, err := l.store.Renew(ctx, l.rec, l.etag, nowUS, l.ttlUS)
		if err == nil {
			l.rec, l.etag = rec, etag
			return rec.Generation, nil
		}
		if errors.Is(err, unique.ErrFenced) || errors.Is(err, objectstore.ErrPreconditionFailed) {
			l.rec, l.etag = unique.LeaseRecord{}, ""
			return 0, ErrLeaseHeld
		}
		return 0, err
	}
	if l.rec.Owner == holder {
		l.rec, l.etag = unique.LeaseRecord{}, ""
	}
	rec, etag, err := l.store.Acquire(ctx, holder, "", nowUS, l.ttlUS)
	if errors.Is(err, unique.ErrLeaseHeld) || errors.Is(err, objectstore.ErrPreconditionFailed) {
		return 0, ErrLeaseHeld
	}
	if err != nil {
		return 0, err
	}
	l.rec, l.etag = rec, etag
	return rec.Generation, nil
}

func (l *bucketLease) Heartbeat(ctx context.Context, holder string) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	nowUS := l.now().UnixMicro()
	if l.rec.Owner != holder || l.rec.Generation == 0 || l.rec.ExpiresAtUS <= nowUS {
		if l.rec.Owner == holder {
			l.rec, l.etag = unique.LeaseRecord{}, ""
		}
		return 0, ErrLeaseLost
	}
	rec, etag, err := l.store.Renew(ctx, l.rec, l.etag, nowUS, l.ttlUS)
	if errors.Is(err, unique.ErrFenced) || errors.Is(err, objectstore.ErrPreconditionFailed) {
		l.rec, l.etag = unique.LeaseRecord{}, ""
		return 0, ErrLeaseLost
	}
	if err != nil {
		return 0, err
	}
	l.rec, l.etag = rec, etag
	return rec.Generation, nil
}

func (l *bucketLease) Release(ctx context.Context, holder string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rec.Owner != holder || l.rec.Generation == 0 {
		return nil
	}
	err := l.store.Release(ctx, l.rec, l.etag)
	l.rec, l.etag = unique.LeaseRecord{}, ""
	return err
}

// memLease is an in-memory Lease for single-process, multi-engine tests (and a
// single-node default). It models the same TTL + fencing-epoch semantics the
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
