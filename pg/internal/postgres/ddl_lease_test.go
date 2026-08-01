package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
)

// TestMemLease exercises the in-memory lease's TTL + fencing-epoch semantics: a
// live holder excludes peers, a renewal keeps the epoch, an expired lease can be
// taken over (bumping the epoch), and a heartbeat from a node that lost the lease
// fails. The clock is injected so expiry is deterministic (no sleeps).
func TestMemLease(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	l := NewMemLease(10 * time.Second)
	l.now = func() time.Time { return now }

	// A acquires from free → epoch 1.
	ep, err := l.Acquire(ctx, "A")
	if err != nil || ep != 1 {
		t.Fatalf("A acquire: epoch=%d err=%v, want 1,nil", ep, err)
	}
	// A renews → same epoch (no ownership change).
	if ep, err := l.Acquire(ctx, "A"); err != nil || ep != 1 {
		t.Fatalf("A renew: epoch=%d err=%v, want 1,nil", ep, err)
	}
	// B is excluded while A's lease is live.
	if _, err := l.Acquire(ctx, "B"); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("B acquire while A live: err=%v, want ErrLeaseHeld", err)
	}
	// A heartbeats, extending its TTL.
	if ep, err := l.Heartbeat(ctx, "A"); err != nil || ep != 1 {
		t.Fatalf("A heartbeat: epoch=%d err=%v, want 1,nil", ep, err)
	}

	// Time passes beyond the (heartbeat-extended) TTL → the lease is free.
	now = now.Add(11 * time.Second)
	// A's heartbeat now fails — its grant lapsed.
	if _, err := l.Heartbeat(ctx, "A"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("A heartbeat after expiry: err=%v, want ErrLeaseLost", err)
	}
	// B takes over the expired lease → epoch bumps to 2 (fence).
	if ep, err := l.Acquire(ctx, "B"); err != nil || ep != 2 {
		t.Fatalf("B takeover: epoch=%d err=%v, want 2,nil", ep, err)
	}
	// A, having lost the lease, can no longer heartbeat it.
	if _, err := l.Heartbeat(ctx, "A"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("A heartbeat after takeover: err=%v, want ErrLeaseLost", err)
	}

	// Release by the holder frees it; a Release by a non-holder is a no-op.
	if err := l.Release(ctx, "A"); err != nil {
		t.Fatalf("A release (non-holder no-op): %v", err)
	}
	if _, err := l.Acquire(ctx, "B"); err != nil { // still held by B after A's no-op release
		t.Fatalf("B re-acquire after A no-op release: %v", err)
	}
	if err := l.Release(ctx, "B"); err != nil {
		t.Fatalf("B release: %v", err)
	}
	// Now free → A acquires, epoch bumps to 3.
	if ep, err := l.Acquire(ctx, "A"); err != nil || ep != 3 {
		t.Fatalf("A acquire after release: epoch=%d err=%v, want 3,nil", ep, err)
	}
}

func TestBucketLease(t *testing.T) {
	ctx := context.Background()
	bucket, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	now := time.Unix(1000, 0)
	a := NewBucketLease(bucket, "ddl/lease", 10*time.Second).(*bucketLease)
	b := NewBucketLease(bucket, "ddl/lease", 10*time.Second).(*bucketLease)
	a.now = func() time.Time { return now }
	b.now = func() time.Time { return now }

	if ep, err := a.Acquire(ctx, "A"); err != nil || ep != 1 {
		t.Fatalf("A acquire: epoch=%d err=%v, want 1,nil", ep, err)
	}
	if ep, err := a.Acquire(ctx, "A"); err != nil || ep != 1 {
		t.Fatalf("A reacquire: epoch=%d err=%v, want 1,nil", ep, err)
	}
	if _, err := b.Acquire(ctx, "B"); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("B acquire while A live: err=%v, want ErrLeaseHeld", err)
	}

	now = now.Add(11 * time.Second)
	if ep, err := b.Acquire(ctx, "B"); err != nil || ep != 2 {
		t.Fatalf("B takeover: epoch=%d err=%v, want 2,nil", ep, err)
	}
	if _, err := a.Heartbeat(ctx, "A"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("A heartbeat after takeover: err=%v, want ErrLeaseLost", err)
	}
	if err := b.Release(ctx, "B"); err != nil {
		t.Fatalf("B release: %v", err)
	}
	if ep, err := a.Acquire(ctx, "A"); err != nil || ep != 3 {
		t.Fatalf("A acquire after release: epoch=%d err=%v, want 3,nil", ep, err)
	}
}
