package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/broker"
)

// quiescentTestNode returns a Node with a non-nil broker so
// WaitApplyQuiescent exercises its wait loop rather than the single-node
// short-circuit. The broker's internals are never touched by the wait.
func quiescentTestNode() *Node {
	return &Node{broker: &broker.Broker{}}
}

// TestWaitApplyQuiescent_NoApplies: with no apply ever recorded, the wait
// returns after roughly one quiet window (nothing to drain).
func TestWaitApplyQuiescent_NoApplies(t *testing.T) {
	n := quiescentTestNode()
	start := time.Now()
	if err := n.WaitApplyQuiescent(context.Background(), 20*time.Millisecond, 500*time.Millisecond); err != nil {
		t.Fatalf("err: %v", err)
	}
	if el := time.Since(start); el > 200*time.Millisecond {
		t.Fatalf("returned after %v, expected ~one quiet window", el)
	}
}

// TestWaitApplyQuiescent_DrainsThenReturns: a burst of applies that stops
// should let the wait return shortly after the last apply, not at max.
func TestWaitApplyQuiescent_DrainsThenReturns(t *testing.T) {
	n := quiescentTestNode()
	const burst = 60 * time.Millisecond
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(5 * time.Millisecond)
		defer tk.Stop()
		deadline := time.Now().Add(burst)
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				if time.Now().After(deadline) {
					return
				}
				n.signalApplied()
			}
		}
	}()
	defer close(stop)

	start := time.Now()
	if err := n.WaitApplyQuiescent(context.Background(), 20*time.Millisecond, 2*time.Second); err != nil {
		t.Fatalf("err: %v", err)
	}
	el := time.Since(start)
	if el < burst {
		t.Fatalf("returned after %v, before the burst ended (%v)", el, burst)
	}
	if el > time.Second {
		t.Fatalf("returned after %v, far past quiescence (near max?)", el)
	}
}

// TestWaitApplyQuiescent_MaxCap: applies that never stop force a return at
// the max cap, with nil (the caller proceeds against steady load).
func TestWaitApplyQuiescent_MaxCap(t *testing.T) {
	n := quiescentTestNode()
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(3 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				n.signalApplied()
			}
		}
	}()
	defer close(stop)

	const max = 80 * time.Millisecond
	start := time.Now()
	if err := n.WaitApplyQuiescent(context.Background(), 20*time.Millisecond, max); err != nil {
		t.Fatalf("err: %v", err)
	}
	el := time.Since(start)
	if el < max-10*time.Millisecond {
		t.Fatalf("returned after %v, before the max cap %v", el, max)
	}
	if el > 5*max {
		t.Fatalf("returned after %v, far past the max cap %v", el, max)
	}
}

// TestWaitApplyQuiescent_NoBrokerImmediate: a single-node node (no broker)
// has no inbound apply path, so the wait returns at once regardless of the
// quiet window — keeping single-node bootstrap (and tests) free of the wait.
func TestWaitApplyQuiescent_NoBrokerImmediate(t *testing.T) {
	n := &Node{} // broker nil
	start := time.Now()
	if err := n.WaitApplyQuiescent(context.Background(), time.Second, 10*time.Second); err != nil {
		t.Fatalf("err: %v", err)
	}
	if el := time.Since(start); el > 50*time.Millisecond {
		t.Fatalf("single-node wait took %v, expected immediate", el)
	}
}

// TestWaitApplyQuiescent_CtxCancel: a cancelled context returns its error.
func TestWaitApplyQuiescent_CtxCancel(t *testing.T) {
	n := quiescentTestNode()
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(3 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				n.signalApplied()
			}
		}
	}()
	defer close(stop)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := n.WaitApplyQuiescent(ctx, 50*time.Millisecond, 5*time.Second); err == nil {
		t.Fatal("expected ctx error, got nil")
	}
}
