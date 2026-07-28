package sqlite

import (
	"context"
	"time"
)

// signalApplied stamps the apply clock that WaitApplyQuiescent reads.
// Wired to broker.OnApplied, so it fires after each successful inbound
// apply.
func (n *Node) signalApplied() {
	n.lastApplyNanos.Store(time.Now().UnixNano())
}

// WaitApplyQuiescent blocks until no inbound apply has landed for the quiet
// duration, or max total has elapsed, or ctx is done. It is the "initial
// catchup has drained" signal: a node restarting into a live cluster applies
// a backlog burst on the broker goroutine, and a caller that is about to take
// the SQLite writer (e.g. schema bootstrap on an existing cluster) waits for
// that burst to settle first, so its DDL does not fight the apply loop for the
// single writer lock.
//
// It waits at least one quiet window before its first check, so a catchup that
// has not yet produced an apply when the call begins is still observed. If
// apply never goes quiet within max (steady live traffic), it returns nil at
// the cap: the caller then proceeds against light steady load, which is not
// the burst this guards against. A node with nothing to apply (single-node, or
// already converged) returns after the first window.
func (n *Node) WaitApplyQuiescent(ctx context.Context, quiet, max time.Duration) error {
	if quiet <= 0 || n.broker == nil {
		// No broker ⇒ no inbound apply path (single-node), so there is
		// never a catchup burst to wait out: return immediately.
		return nil
	}
	deadline := time.Now().Add(max)
	for {
		wait := quiet
		if rem := time.Until(deadline); rem < wait {
			wait = rem
		}
		if wait <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		last := n.lastApplyNanos.Load()
		if last == 0 || time.Since(time.Unix(0, last)) >= quiet {
			return nil
		}
		if !time.Now().Before(deadline) {
			return nil
		}
	}
}
