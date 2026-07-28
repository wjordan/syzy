package physicalrestore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPrefetchOrdered_AppliesInOrder: apply must see indices 0..n-1 strictly in
// order even though fetches complete out of order (later indices resolve first).
func TestPrefetchOrdered_AppliesInOrder(t *testing.T) {
	const n = 50
	var applied []int
	err := PrefetchOrdered(context.Background(), n, 8,
		func(ctx context.Context, i int) (int, error) {
			// Earlier indices sleep longer, so fetches finish in reverse-ish
			// order; the apply loop must still serialize them ascending.
			time.Sleep(time.Duration(n-i) * 100 * time.Microsecond)
			return i, nil
		},
		func(i, v int) error {
			if v != i {
				t.Errorf("apply got value %d for index %d", v, i)
			}
			applied = append(applied, i)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("PrefetchOrdered: %v", err)
	}
	if len(applied) != n {
		t.Fatalf("applied %d items, want %d", len(applied), n)
	}
	for i := range applied {
		if applied[i] != i {
			t.Fatalf("apply order wrong at %d: got %d", i, applied[i])
		}
	}
}

// TestPrefetchOrdered_FetchesConcurrently: fetches overlap up to the window, so a
// chain of latency-bound fetches finishes far faster than n sequential RTTs.
func TestPrefetchOrdered_FetchesConcurrently(t *testing.T) {
	const n, window = 32, 8
	var inflight, maxInflight int32
	const rtt = 5 * time.Millisecond
	start := time.Now()
	err := PrefetchOrdered(context.Background(), n, window,
		func(ctx context.Context, i int) (int, error) {
			cur := atomic.AddInt32(&inflight, 1)
			for {
				m := atomic.LoadInt32(&maxInflight)
				if cur <= m || atomic.CompareAndSwapInt32(&maxInflight, m, cur) {
					break
				}
			}
			time.Sleep(rtt) // simulate an object-store round-trip
			atomic.AddInt32(&inflight, -1)
			return i, nil
		},
		func(i, v int) error { return nil },
	)
	if err != nil {
		t.Fatalf("PrefetchOrdered: %v", err)
	}
	if got := atomic.LoadInt32(&maxInflight); got < 2 {
		t.Fatalf("max in-flight fetches = %d, want concurrency > 1", got)
	}
	// Sequential would be n*rtt; with the window it should be roughly
	// (n/window)*rtt. Assert it beat half the sequential bound — a wide margin
	// that still proves real overlap without being timing-flaky.
	if elapsed := time.Since(start); elapsed > time.Duration(n)*rtt/2 {
		t.Fatalf("elapsed %v not much faster than sequential %v — fetches not overlapping", elapsed, time.Duration(n)*rtt)
	}
}

// TestPrefetchOrdered_FetchErrorAborts: a fetch error is returned and stops the
// run; no apply runs at or after the failing index.
func TestPrefetchOrdered_FetchErrorAborts(t *testing.T) {
	sentinel := errors.New("boom")
	const n, bad = 20, 7
	var mu sync.Mutex
	var appliedAtOrAfterBad bool
	err := PrefetchOrdered(context.Background(), n, 4,
		func(ctx context.Context, i int) (int, error) {
			if i == bad {
				return 0, sentinel
			}
			return i, nil
		},
		func(i, v int) error {
			mu.Lock()
			if i >= bad {
				appliedAtOrAfterBad = true
			}
			mu.Unlock()
			return nil
		},
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if appliedAtOrAfterBad {
		t.Fatalf("apply ran at or after the failing index %d", bad)
	}
}

// TestPrefetchOrdered_ApplyErrorAborts: an apply error is returned and stops the
// loop at that index.
func TestPrefetchOrdered_ApplyErrorAborts(t *testing.T) {
	sentinel := errors.New("apply-boom")
	const n, bad = 20, 5
	var lastApplied int32 = -1
	err := PrefetchOrdered(context.Background(), n, 4,
		func(ctx context.Context, i int) (int, error) { return i, nil },
		func(i, v int) error {
			if i == bad {
				return sentinel
			}
			atomic.StoreInt32(&lastApplied, int32(i))
			return nil
		},
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if got := atomic.LoadInt32(&lastApplied); got != bad-1 {
		t.Fatalf("last applied index = %d, want %d (stop at first apply error)", got, bad-1)
	}
}

// TestPrefetchOrdered_ContextCancel: a cancelled context aborts with the context
// error rather than hanging.
func TestPrefetchOrdered_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := PrefetchOrdered(ctx, 10, 4,
		func(ctx context.Context, i int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
		func(i, v int) error { return nil },
	)
	if err == nil {
		t.Fatal("want error from cancelled context, got nil")
	}
}

// TestPrefetchOrdered_Empty: n=0 is a no-op.
func TestPrefetchOrdered_Empty(t *testing.T) {
	called := false
	err := PrefetchOrdered(context.Background(), 0, 4,
		func(ctx context.Context, i int) (int, error) { called = true; return 0, nil },
		func(i, v int) error { called = true; return nil },
	)
	if err != nil || called {
		t.Fatalf("empty run: err=%v called=%v", err, called)
	}
}
