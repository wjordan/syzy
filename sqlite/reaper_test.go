package sqlite

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/nodestate"
)

// TestForgetDeadOrigins pins the frontier half of origin GC: an origin is
// evicted only when it is not self, has no live mirror journal, and is absent
// from the bucket tips (retention swept it). Live-journal or still-durable
// origins are kept, and a failed/absent bucket listing evicts nothing.
func TestForgetDeadOrigins(t *testing.T) {
	self := crdt.Origin(7)
	newNode := func(origins ...crdt.Origin) (*Node, *nodestate.Cache) {
		c := nodestate.New(self)
		for _, o := range origins {
			c.MarkApplied(o, 1, crdt.Clock{WallTime: 100})
		}
		return &Node{cache: c, log: slog.New(slog.NewTextHandler(io.Discard, nil))}, c
	}
	kept := func(t *testing.T, c *nodestate.Cache, o crdt.Origin) {
		t.Helper()
		if _, ok := c.FrontierFor(o); !ok {
			t.Errorf("origin %d should have been kept", o)
		}
	}
	gone := func(t *testing.T, c *nodestate.Cache, o crdt.Origin) {
		t.Helper()
		if _, ok := c.FrontierFor(o); ok {
			t.Errorf("origin %d should have been forgotten", o)
		}
	}

	// 11 live journal, 22 bucket-present, 33+44 swept (bucket-absent).
	n, c := newNode(11, 22, 33, 44)
	bucket := map[crdt.Origin]crdt.Seq{22: 1}
	n.forgetDeadOrigins(context.Background(), bucket, true, true, self, []crdt.Origin{11})
	kept(t, c, 11) // live journal
	kept(t, c, 22) // epochs still durable
	gone(t, c, 33)
	gone(t, c, 44)

	// tipsOK=false: a failed DiscoverTips must evict nothing.
	n, c = newNode(55)
	n.forgetDeadOrigins(context.Background(), nil, true, false, self, nil)
	kept(t, c, 55)

	// no-bucket mode: no durable "dead" signal, evict nothing.
	n, c = newNode(66)
	n.forgetDeadOrigins(context.Background(), nil, false, false, self, nil)
	kept(t, c, 66)
}

// TestReapable pins the mirror-journal reap-safety predicate. The regression it
// guards: an UNSEALED origin must NOT be reaped on the all-peers-applied signal
// alone when a durable bucket exists, because that signal is liveness-scoped
// (connected peers only) and a transiently-absent member would be stranded with
// no recovery path (records gone from every mirror, never sealed). It may be
// reaped that way ONLY in best-effort no-bucket mode.
func TestReapable(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		sealed, replicated, bucket bool
		want                       bool
	}{
		{"sealed, bucket", true, false, true, true},
		{"sealed, no bucket", true, false, false, true},
		{"sealed beats everything", true, true, true, true},

		// The bug: unsealed + all-CONNECTED-peers-applied + a bucket exists.
		// Must be KEPT — liveness cannot prove an absent member is caught up, and
		// the unsealed records have no durable copy to recover from.
		{"unsealed+replicated+bucket -> KEEP (the bug)", false, true, true, false},

		// No bucket: there is no durability tier to wait for, so all-peers-applied
		// is the only available signal; best-effort reaping is allowed.
		{"unsealed+replicated, no bucket -> reap", false, true, false, true},

		// Not durable and not even all-connected-peers caught up: always keep.
		{"unsealed, not replicated, bucket", false, false, true, false},
		{"unsealed, not replicated, no bucket", false, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := reapable(tc.sealed, tc.replicated, tc.bucket); got != tc.want {
				t.Errorf("reapable(sealed=%v, replicated=%v, hasBucket=%v) = %v, want %v",
					tc.sealed, tc.replicated, tc.bucket, got, tc.want)
			}
		})
	}
}
