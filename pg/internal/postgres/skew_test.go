package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
)

// TestSkewGuardCapsRemoteClock: the cap is applied to a far-future stamp and
// nothing else is touched.
func TestSkewGuardCapsRemoteClock(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	g := newSkewGuard(30 * time.Second)
	g.now = func() time.Time { return now }

	// Within the bound (including a stamp slightly ahead): passed through whole,
	// logical counter included.
	for _, ms := range []int64{-5_000, 0, 29_000} {
		clk := crdt.Clock{WallTime: now.UnixMilli() + ms, Logical: 7}
		if got := g.admit(3, clk); got != clk {
			t.Errorf("stamp %+d ms: admit = %+v, want it untouched", ms, got)
		}
	}
	// Beyond it: capped to now+bound, and the logical tick goes with the wall
	// time it no longer names.
	far := crdt.Clock{WallTime: now.UnixMilli() + 365*24*3600*1000, Logical: 9}
	got := g.admit(3, far)
	if want := (crdt.Clock{WallTime: now.UnixMilli() + 30_000}); got != want {
		t.Errorf("year-ahead stamp: admit = %+v, want %+v", got, want)
	}
	// Disabled explicitly.
	off := newSkewGuard(-1)
	off.now = func() time.Time { return now }
	if got := off.admit(3, far); got != far {
		t.Errorf("disabled guard altered the clock: %+v", got)
	}
}

// TestSkewedPeerDoesNotPoisonLocalClock: a peer whose wall clock is a year
// ahead wins the row it wrote — that is last-writer-wins doing its job — but
// this node's own clock must stay in the present, or every later local write
// on every node inherits the bad time permanently.
func TestSkewedPeerDoesNotPoisonLocalClock(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xd9}
	a := openEngine(t, ctx, "syzy_skewa", 95, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, a)
	b := openEngine(t, ctx, "syzy_skewb", 96, cluster, schemaKV, []string{"public.kv"})
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_skewa", `INSERT INTO public.kv VALUES (1,'seed')`)
	seed := captureAll(t, ctx, a)
	if err := b.appl.Apply(ctx, seed[0]); err != nil {
		t.Fatalf("B apply seed: %v", err)
	}
	// Rebuild A's changeset with a wall time a year in the future, as a node with
	// a broken clock would send it.
	appExec(t, "syzy_skewa", `UPDATE public.kv SET val = 'future' WHERE id = 1`)
	csA := captureAll(t, ctx, a)[0]
	yearMs := int64(365 * 24 * 3600 * 1000)
	skewed, err := crdt.Build(csA.Dot,
		crdt.Stamp{Clock: crdt.Clock{WallTime: csA.Stamp.WallTime + yearMs}, Origin: csA.Stamp.Origin},
		csA.Deps, csA.ClusterID, csA.Records)
	if err != nil {
		t.Fatalf("build skewed changeset: %v", err)
	}
	if err := b.appl.Apply(ctx, skewed); err != nil {
		t.Fatalf("B apply skewed: %v", err)
	}
	if got := dumpKV(t, "syzy_skewb")[1]; got != "future" {
		t.Errorf("B kv[1] = %q, want the skewed peer's value (LWW still applies)", got)
	}
	// B's clock stayed in the present: the HLC absorbed the cap, not the peer's
	// year-ahead wall time.
	bound := time.Now().UnixMilli() + int64(defaultMaxClockSkew/time.Millisecond) + 1000
	if hlc := b.cfg.Cache.HLCLast(); hlc.WallTime > bound {
		t.Fatalf("B's HLC = %d, beyond now+bound %d — a peer's clock became ours", hlc.WallTime, bound)
	}
	// And so does every later local write. Row 2 rather than row 1: the skewed
	// row is legitimately owned by the peer now, and a local write to it would be
	// reverted by winner-repair (which is the correct outcome, just not what this
	// test is measuring).
	appExec(t, "syzy_skewb", `INSERT INTO public.kv VALUES (2,'local')`)
	local := captureAll(t, ctx, b)
	if len(local) != 1 {
		t.Fatalf("local write produced %d changesets, want 1", len(local))
	}
	if local[0].Stamp.WallTime > bound {
		t.Errorf("B's local write stamped %d, beyond now+bound %d", local[0].Stamp.WallTime, bound)
	}
}
