package pgtestcluster

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TestClusterKVConvergence is the smoke test for the whole productization
// path: two PG engines, each producing disjoint inserts, end up byte-for-
// byte identical on every node. Exercises capture -> orchestrator -> self-
// log -> publisher -> memtransport -> peer inbox -> apply -> mirror.
func TestClusterKVConvergence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	c := New(t, Config{
		N:        2,
		DBPrefix: "syzy_clu_kv",
		Schema:   `CREATE TABLE public.kv (id bigint PRIMARY KEY, val text)`,
		Tables:   []string{"public.kv"},
	})
	c.Start(ctx)

	// Node 0 writes ids 1..10, Node 1 writes ids 11..20.
	for i := 1; i <= 10; i++ {
		c.Nodes[0].AppExec(t, fmt.Sprintf(`INSERT INTO public.kv VALUES (%d, 'a%d')`, i, i))
	}
	for i := 11; i <= 20; i++ {
		c.Nodes[1].AppExec(t, fmt.Sprintf(`INSERT INTO public.kv VALUES (%d, 'b%d')`, i, i))
	}

	if err := c.WaitConverge(30 * time.Second); err != nil {
		// Diagnostics: each node's view of every peer's frontier + any
		// engine.Run errors that already surfaced + hub history depth.
		t.Logf("hub history len = %d (cumulative broadcasts)", c.Hub.HistoryLen())
		for _, n := range c.Nodes {
			t.Logf("node %s (origin %d) inbox-depth=%d:", n.DB, n.Origin, n.InboxDepth())
			for _, peer := range c.Nodes {
				next := n.Cache.SenderNextSeq(peer.Origin)
				front, _ := n.Cache.FrontierFor(peer.Origin)
				t.Logf("  origin %d → senderNext=%d frontier.lastSeq=%d", peer.Origin, next, front.LastSeq)
			}
			if err, ok := n.RunErr(); ok {
				t.Logf("  Engine.Run already returned: %v", err)
			}
		}
		t.Fatalf("WaitConverge: %v", err)
	}

	want := map[int64]string{}
	for i := 1; i <= 10; i++ {
		want[int64(i)] = fmt.Sprintf("a%d", i)
	}
	for i := 11; i <= 20; i++ {
		want[int64(i)] = fmt.Sprintf("b%d", i)
	}
	for _, n := range c.Nodes {
		got := n.DumpKV(t)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s content mismatch:\n got  %v\n want %v", n.DB, got, want)
		}
	}
}
