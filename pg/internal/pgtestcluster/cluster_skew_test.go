package pgtestcluster

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
)

// TestClusterConvergesUnderClockSkew is the simulated-clock half of the skew
// work: the admission guard bounds what a peer's clock can do to ours, but the
// question it does not answer is whether the cluster still CONVERGES when one
// node's writes systematically outrank everyone's.
//
// A node whose clock is an hour ahead stamps every write above its peers, so it
// wins every arbitration it takes part in and its peers' local folds lose
// theirs. That is last-writer-wins behaving correctly — the point is that
// losing is not the same as diverging: each losing node has to end up holding
// the skewed node's value, which is winner-repair's whole job, and the losing
// side is the side with the least reason to notice on its own.
//
// The skew is injected by advancing one node's HLC directly, which is what a
// clock an hour ahead produces: HLC never moves backwards, so every subsequent
// local stamp on that node is at least that high. Real cross-machine skew
// cannot be reproduced on one host — both databases read the same clock.
func TestClusterConvergesUnderClockSkew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c := New(t, Config{
		N:        3,
		DBPrefix: "syzy_clu_skew",
		Schema:   `CREATE TABLE public.kv (id bigint PRIMARY KEY, val text)`,
		Tables:   []string{"public.kv"},
	})
	c.Start(ctx)

	// Node 1's clock is an hour ahead of the cluster.
	skewed := c.Nodes[1]
	ahead := crdt.Clock{WallTime: time.Now().Add(time.Hour).UnixMilli()}
	skewed.Cache.ObserveHLC(ahead)

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

	if err := c.WaitIdle(750*time.Millisecond, 30*time.Second); err != nil {
		t.Fatalf("WaitIdle: %v", err)
	}
	first := c.Nodes[0].DumpKV(t)
	for _, n := range c.Nodes[1:] {
		if got := n.DumpKV(t); !reflect.DeepEqual(got, first) {
			t.Errorf("%s diverged from node 0 under clock skew:\n got  %v\n want %v", n.DB, got, first)
		}
	}
	// The skewed node's stamps dominate, so the value the cluster settles on is
	// one of its writes. If it were not, the skew was not actually in effect and
	// this test proved nothing.
	if v := first[1]; len(v) < 3 || v[:3] != "n1-" {
		t.Errorf("cluster settled on %q, want a write from the skewed node — "+
			"the skew did not take effect, so convergence under it is untested", v)
	}
}
