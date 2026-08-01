package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

// fakeGapFiller serves catch-up from a fixed changeset set, like a peer's
// CatchupSource would.
type fakeGapFiller struct {
	css   []*crdt.Changeset
	calls atomic.Int32
}

func (f *fakeGapFiller) Fetch(ctx context.Context, ranges []transport.Range, apply transport.ApplyFunc) error {
	f.calls.Add(1)
	for _, r := range ranges {
		for _, cs := range f.css {
			hi := r.Hi
			if hi == 0 {
				hi = ^crdt.Seq(0)
			}
			if cs.Dot.Origin == r.Origin && cs.Dot.Seq >= r.Lo && cs.Dot.Seq <= hi {
				if err := apply(ctx, cs.Encoded()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// TestFetcherHealsMissedDelivery: live broadcast is best-effort, so a skipped
// seq must be pulled back via the GapFiller. Delivering 1 then 3 leaves a gap
// at 2; the out-of-order apply kicks the fetcher (no timer wait), which plans
// [2,2] from the Cache and routes the fetched changeset through the
// orchestrator. The node converges to all three rows.
func TestFetcherHealsMissedDelivery(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0xfe, 0x7c}

	src := newTestEngine(t, ctx, "syzy_fsrc", 11, cluster)
	defer closeEngine(t, ctx, src)

	appExec(t, "syzy_fsrc", `INSERT INTO public.kv VALUES (1,'one')`)
	appExec(t, "syzy_fsrc", `INSERT INTO public.kv VALUES (2,'two')`)
	appExec(t, "syzy_fsrc", `INSERT INTO public.kv VALUES (3,'three')`)
	css := captureBacklog(t, ctx, src, 3)

	filler := &fakeGapFiller{css: css}
	dst := openEngineWithFiller(t, ctx, "syzy_fdst", 12, cluster, filler)
	defer closeEngine(t, ctx, dst)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	inbox := make(chan *crdt.Changeset, 4)
	runDone := make(chan error, 1)
	go func() {
		runDone <- dst.Run(runCtx, inbox, func(context.Context, *crdt.Changeset) error { return nil })
	}()

	inbox <- css[0] // seq 1
	inbox <- css[2] // seq 3 — creates the gap, kicks the fetcher

	deadline := time.Now().Add(5 * time.Second)
	for {
		if fr, ok := dst.cfg.Cache.FrontierFor(11); ok && fr.LastSeq == 3 {
			break
		}
		select {
		case err := <-runDone:
			t.Fatalf("Run exited early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			fr, _ := dst.cfg.Cache.FrontierFor(11)
			t.Fatalf("gap not healed: frontier=%+v, fetch calls=%d", fr, filler.calls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := dumpKV(t, "syzy_fdst"); len(got) != 3 || got[2] != "two" {
		t.Fatalf("converged state = %v; want all three rows", got)
	}
	if filler.calls.Load() == 0 {
		t.Fatal("GapFiller was never invoked")
	}

	cancel()
	if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
}

func openEngineWithFiller(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, filler transport.GapFiller) *Engine {
	t.Helper()
	createTestDB(t, ctx, db, schemaKV)
	cfg := baseTestConfig(db, origin, cluster)
	cfg.GapFiller = filler
	e, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	return e
}
