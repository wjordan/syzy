//go:build linux

package lazyrestore

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestBitmap_SetIsSetClearRoundTrip(t *testing.T) {
	b := newPageBitmap(128)
	if b.isSet(1) {
		t.Fatalf("fresh bitmap reports pgno=1 as set")
	}
	b.set(1)
	if !b.isSet(1) {
		t.Fatalf("Set(1) did not stick")
	}
	if !b.clear(1) {
		t.Fatalf("Clear(1) on set bit returned false")
	}
	if b.isSet(1) {
		t.Fatalf("Clear(1) did not stick")
	}
	if b.clear(1) {
		t.Fatalf("Clear(1) on already-clear bit returned true")
	}
}

func TestBitmap_OutOfRange(t *testing.T) {
	b := newPageBitmap(8)
	for _, pgno := range []uint32{0, 9, 1000} {
		if b.isSet(pgno) {
			t.Errorf("IsSet(%d) returned true for out-of-range", pgno)
		}
		if b.clear(pgno) {
			t.Errorf("Clear(%d) returned true for out-of-range", pgno)
		}
		if b.trySet(pgno) {
			t.Errorf("TrySet(%d) returned true for out-of-range", pgno)
		}
	}
}

func TestBitmap_TrySetClear_AtomicityUnderContention(t *testing.T) {
	const (
		pages   = 1024
		workers = 8
		passes  = 200
	)
	b := newPageBitmap(pages)
	var setCalls atomic.Int64
	var clearCalls atomic.Int64

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(seed uint32) {
			defer wg.Done()
			for p := uint32(0); p < passes; p++ {
				// Pick a deterministic page per pass so workers
				// hammer overlapping bits.
				pgno := ((p*7 + seed) % pages) + 1
				if p%2 == 0 {
					if b.trySet(pgno) {
						setCalls.Add(1)
					}
				} else {
					if b.clear(pgno) {
						clearCalls.Add(1)
					}
				}
			}
		}(uint32(w))
	}
	wg.Wait()

	// Quiescent state: PresentCount must equal (TrySet wins) -
	// (Clear wins). Bookkeeping alone proves CAS atomicity — no
	// double-set / double-clear can sneak past.
	if got, want := int64(b.presentCount()), setCalls.Load()-clearCalls.Load(); got != want {
		t.Fatalf("PresentCount=%d; set=%d clear=%d (set-clear=%d)", got, setCalls.Load(), clearCalls.Load(), want)
	}
}
