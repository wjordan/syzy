package pgtestcluster

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestClusterLWWLastWriterWins drives a clean LWW round: node 0 writes
// PK=1 first, then after a pause node 1 writes PK=1. Both nodes converge
// on node 1's value (strictly later wall-time stamp, so HLC arbitration
// picks it unambiguously).
//
// The aggressive "both nodes hammer PK=1 simultaneously" case is the §9
// stamp-inflation corner — it converges with winner-repair when a later
// peer message arrives to trigger the stash, but a quiesced contention
// burst can leave each side holding its own tail. That edge is its own
// test family; this one exercises the unambiguous LWW path.
func TestClusterLWWLastWriterWins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	c := New(t, Config{
		N:        2,
		DBPrefix: "syzy_clu_lww",
		Schema:   `CREATE TABLE public.kv (id bigint PRIMARY KEY, val text)`,
		Tables:   []string{"public.kv"},
	})
	c.Start(ctx)

	// Node 0 writes first; wait for node 1 to see it; then node 1 writes,
	// strictly later in wall time.
	c.Nodes[0].AppExec(t, `INSERT INTO public.kv VALUES (1,'from-n0')
		ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val`)
	if err := c.WaitConverge(15 * time.Second); err != nil {
		t.Fatalf("WaitConverge after n0 write: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // ensure strictly-later wall clock
	c.Nodes[1].AppExec(t, `INSERT INTO public.kv VALUES (1,'from-n1')
		ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val`)
	if err := c.WaitConverge(15 * time.Second); err != nil {
		t.Fatalf("WaitConverge after n1 write: %v", err)
	}

	want := map[int64]string{1: "from-n1"}
	for _, n := range c.Nodes {
		got := n.DumpKV(t)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", n.DB, got, want)
		}
	}
}

// TestClusterLWWAgreement is the deliberately-stressful version: every node
// hammers the same PK at once, then all nodes must AGREE on some value
// (which one is not prescribed — that is LWW's business). This is the bare
// convergence invariant the whole CRDT model exists to provide.
//
// The burst has to be genuinely symmetric to be worth anything: writes issued
// alternately and synchronously are just a totally-ordered sequence on one
// clock, and the last one trivially wins. Concurrent writers on the same
// millisecond are what produce real ties, and the tail of such a burst is the
// case winner-repair exists for — the losing side's own committed row has to
// be pulled back to the winner's image with no further peer traffic to
// prompt it.
func TestClusterLWWAgreement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c := New(t, Config{
		N:        3,
		DBPrefix: "syzy_clu_lww_a",
		Schema:   `CREATE TABLE public.kv (id bigint PRIMARY KEY, val text)`,
		Tables:   []string{"public.kv"},
	})
	c.Start(ctx)

	var wg sync.WaitGroup
	for n := range c.Nodes {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				c.Nodes[n].AppExec(t, fmt.Sprintf(`INSERT INTO public.kv VALUES (1,'n%d-i%d')
					ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val`, n, i))
			}
		}(n)
	}
	wg.Wait()

	// Idle, not merely converged, and observed without writing: a burst tail
	// still sitting in the WAL is in no producer head, and a marker write to
	// fence against it would itself be the peer message that triggers the repair
	// this test exists to check happens on its own.
	if err := c.WaitIdle(750*time.Millisecond, 30*time.Second); err != nil {
		t.Fatalf("WaitIdle: %v", err)
	}
	first := c.Nodes[0].DumpKV(t)
	if len(first) != 1 {
		t.Fatalf("node 0 holds %v, want exactly the one contended row", first)
	}
	for _, n := range c.Nodes[1:] {
		got := n.DumpKV(t)
		if !reflect.DeepEqual(got, first) {
			t.Errorf("%s diverged from node 0:\n got  %v\n want %v", n.DB, got, first)
		}
	}
}
