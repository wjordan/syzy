package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
)

// TestLiveHammeringConvergence: both nodes upsert the SAME PK in a tight loop
// over a persistent connection, concurrently — the sustained same-key contention
// that once diverged (the capture-vs-apply two-writer race + stamp inflation).
// Post orchestrator-cutover (single serialized writer) + commit-timestamp
// stamping, both nodes converge to one LWW winner. Regression guard for that
// guarantee; the only remaining theoretical residual is genuine cross-node
// wall-clock skew beyond HLC tolerance, which a single-machine test cannot
// produce (see docs/postgres.md §9).
func TestLiveHammeringConvergence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x44, 0x55}

	a := newTestEngine(t, ctx, "syzy_hama", 41, cluster)
	defer closeEngine(t, ctx, a)
	b := newTestEngine(t, ctx, "syzy_hamb", 42, cluster)
	defer closeEngine(t, ctx, b)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	aInbox := make(chan *crdt.Changeset, 8192)
	bInbox := make(chan *crdt.Changeset, 8192)
	run := func(node *Engine, inbox <-chan *crdt.Changeset, peer chan<- *crdt.Changeset) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			broadcast := func(ctx context.Context, cs *crdt.Changeset) error {
				select {
				case peer <- cs:
				case <-ctx.Done():
				}
				return nil
			}
			if err := node.Run(runCtx, inbox, broadcast); err != nil && runCtx.Err() == nil {
				t.Errorf("%s orchestrator: %v", node.cfg.Name, err)
			}
		}()
	}
	run(a, aInbox, bInbox)
	run(b, bInbox, aInbox)

	const iters = 200
	var writers sync.WaitGroup
	hammer := func(db, tag string) {
		writers.Add(1)
		go func() {
			defer writers.Done()
			conn, err := pgx.Connect(ctx, dbURL(db))
			if err != nil {
				t.Errorf("hammer connect %s: %v", db, err)
				return
			}
			defer conn.Close(ctx)
			for i := 0; i < iters; i++ {
				if _, err := conn.Exec(ctx, fmt.Sprintf(
					`INSERT INTO public.kv VALUES (100,'%s-%d') ON CONFLICT (id) DO UPDATE SET val=excluded.val`, tag, i)); err != nil {
					t.Errorf("hammer exec %s: %v", db, err)
					return
				}
			}
		}()
	}
	hammer("syzy_hama", "A")
	hammer("syzy_hamb", "B")
	writers.Wait()

	deadline := time.Now().Add(25 * time.Second)
	var da, dbm map[int64]string
	for time.Now().Before(deadline) {
		da, dbm = dumpKV(t, "syzy_hama"), dumpKV(t, "syzy_hamb")
		if da[100] == dbm[100] && da[100] != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	t.Logf("final: A[100]=%q B[100]=%q", da[100], dbm[100])
	if da[100] != dbm[100] {
		t.Fatalf("DIVERGED: A[100]=%q B[100]=%q", da[100], dbm[100])
	}
}
