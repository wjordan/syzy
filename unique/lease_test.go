package unique

import (
	"context"
	"errors"
	"testing"

	"github.com/wjordan/objectstore"
)

const leaseTTL = 30_000_000 // 30s in µs

func leaseStore(t *testing.T) *LeaseStore {
	t.Helper()
	b, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	return OpenLease(b, "unique/leader")
}

func TestLease_AcquireOnEmpty(t *testing.T) {
	s := leaseStore(t)
	rec, _, err := s.Acquire(context.Background(), "node-a", "a:9000", 1000, leaseTTL)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if rec.Owner != "node-a" || rec.Generation != 1 || rec.Addr != "a:9000" {
		t.Fatalf("rec = %+v; want node-a gen 1 a:9000", rec)
	}
}

func TestLease_HeldByAnotherIsRejected(t *testing.T) {
	s := leaseStore(t)
	now := int64(1000)
	if _, _, err := s.Acquire(context.Background(), "node-a", "a:9000", now, leaseTTL); err != nil {
		t.Fatalf("a Acquire: %v", err)
	}
	// b tries while a's lease is live.
	_, _, err := s.Acquire(context.Background(), "node-b", "b:9000", now+1, leaseTTL)
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("b Acquire err = %v; want ErrLeaseHeld", err)
	}
}

func TestLease_TakeoverAfterExpiryBumpsGeneration(t *testing.T) {
	s := leaseStore(t)
	if _, _, err := s.Acquire(context.Background(), "node-a", "a:9000", 1000, leaseTTL); err != nil {
		t.Fatalf("a Acquire: %v", err)
	}
	// b takes over well after a's lease expired; generation must advance
	// (the fence) so a's stale writes are rejected.
	rec, _, err := s.Acquire(context.Background(), "node-b", "b:9000", 1000+leaseTTL+1, leaseTTL)
	if err != nil {
		t.Fatalf("b takeover: %v", err)
	}
	if rec.Owner != "node-b" || rec.Generation != 2 {
		t.Fatalf("rec = %+v; want node-b gen 2", rec)
	}
}

func TestLease_RenewExtendsExpiry(t *testing.T) {
	s := leaseStore(t)
	rec, etag, err := s.Acquire(context.Background(), "node-a", "a:9000", 1000, leaseTTL)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	renewed, _, err := s.Renew(context.Background(), rec, etag, 5000, leaseTTL)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if renewed.ExpiresAtUS != 5000+leaseTTL || renewed.Generation != 1 {
		t.Fatalf("renewed = %+v; want expiry %d gen 1", renewed, 5000+leaseTTL)
	}
}

func TestLease_RenewAfterFenceFails(t *testing.T) {
	s := leaseStore(t)
	recA, etagA, err := s.Acquire(context.Background(), "node-a", "a:9000", 1000, leaseTTL)
	if err != nil {
		t.Fatalf("a Acquire: %v", err)
	}
	// b takes over after expiry (generation 2).
	if _, _, err := s.Acquire(context.Background(), "node-b", "b:9000", 1000+leaseTTL+1, leaseTTL); err != nil {
		t.Fatalf("b takeover: %v", err)
	}
	// a's renew is fenced.
	if _, _, err := s.Renew(context.Background(), recA, etagA, 2000+leaseTTL, leaseTTL); !errors.Is(err, ErrFenced) {
		t.Fatalf("a Renew err = %v; want ErrFenced", err)
	}
}

func TestLease_ReleaseAllowsImmediateTakeover(t *testing.T) {
	s := leaseStore(t)
	rec, etag, err := s.Acquire(context.Background(), "node-a", "a:9000", 1000, leaseTTL)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := s.Release(context.Background(), rec, etag); err != nil {
		t.Fatalf("Release: %v", err)
	}
	// b takes over immediately (well before a's original expiry).
	rec2, _, err := s.Acquire(context.Background(), "node-b", "b:9000", 2000, leaseTTL)
	if err != nil {
		t.Fatalf("b Acquire after release: %v", err)
	}
	if rec2.Owner != "node-b" || rec2.Generation != 2 {
		t.Fatalf("rec2 = %+v; want node-b gen 2", rec2)
	}
}

func TestLease_ConcurrentAcquireOneWins(t *testing.T) {
	s := leaseStore(t)
	// Both read the empty lease, then both CAS with IfAbsent; exactly one
	// wins, the other gets ErrPreconditionFailed.
	_, _, err1 := s.Acquire(context.Background(), "node-a", "a:9000", 1000, leaseTTL)
	if err1 != nil {
		t.Fatalf("a Acquire: %v", err1)
	}
	// Simulate b having read the empty state before a wrote: force an
	// IfAbsent CAS against a now-present object.
	_, err := s.cas(context.Background(), LeaseRecord{Owner: "node-b", Generation: 1, ExpiresAtUS: 1000 + leaseTTL}, "")
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("racing IfAbsent CAS err = %v; want ErrPreconditionFailed", err)
	}
}
