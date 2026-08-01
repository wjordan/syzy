package postgres

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
)

// openSelfLog opens a durable engine with the self-origin log enabled on an
// EXISTING database (no recreate), so a restart test preserves the slot and
// replays the self-log. CheckpointEvery is large so a short run never folds a
// compaction checkpoint — the self-log carries the recovery, which is the point.
func openSelfLog(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, cache *nodestate.Cache, meta *metadata.Store, journalDir string) *Engine {
	t.Helper()
	e, err := Open(ctx, Config{
		Name:            db,
		Origin:          origin,
		Cluster:         cluster,
		Cache:           cache,
		ConnURL:         dbURL(db),
		ReplConnURL:     replURL(db),
		Publication:     "syzy_pub",
		Slot:            slotName(db),
		OriginName:      originName(db),
		Tables:          []string{"public.kv"},
		Meta:            meta,
		JournalDir:      journalDir,
		CheckpointEvery: 1000,
	})
	if err != nil {
		t.Fatalf("open self-log %s: %v", db, err)
	}
	return e
}

// runShipping runs the orchestrator live (nil inbox: no remote applies),
// performs the writes, and collects everything the publisher broadcasts until
// done(collected) holds, then cleanly stops and returns the collected
// changesets. The publisher ships from the self-log, so on a restart it
// re-ships every retained entry (idempotent at peers by Dot) before the new
// ones — done lets a caller wait for a specific commit rather than a raw count.
func runShipping(t *testing.T, ctx context.Context, e *Engine, db string, writes []string, done func([]*crdt.Changeset) bool) []*crdt.Changeset {
	t.Helper()
	var mu sync.Mutex
	var got []*crdt.Changeset
	runCtx, cancel := context.WithCancel(ctx)
	finished := make(chan struct{})
	go func() {
		broadcast := func(_ context.Context, cs *crdt.Changeset) error {
			mu.Lock()
			got = append(got, cs)
			mu.Unlock()
			return nil
		}
		_ = e.Run(runCtx, nil, broadcast)
		close(finished)
	}()
	for _, w := range writes {
		appExec(t, db, w)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		snap := append([]*crdt.Changeset(nil), got...)
		mu.Unlock()
		if done(snap) || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-finished
	mu.Lock()
	defer mu.Unlock()
	return append([]*crdt.Changeset(nil), got...)
}

// shippedCount returns a done-predicate that waits for n broadcasts.
func shippedCount(n int) func([]*crdt.Changeset) bool {
	return func(g []*crdt.Changeset) bool { return len(g) >= n }
}

// shippedSeq returns a done-predicate that waits until a changeset with the
// given self-origin Seq has been broadcast.
func shippedSeq(seq crdt.Seq) func([]*crdt.Changeset) bool {
	return func(g []*crdt.Changeset) bool {
		for _, cs := range g {
			if cs.Dot.Seq == seq {
				return true
			}
		}
		return false
	}
}

// TestSelfLogDurableRestart proves the §3 durability boundary end-to-end: with
// the self-origin log, the live orchestrator advances the slot to the shipped
// position, and after a restart the Cache rehydrates (snapshot + self-log
// replay) so capture neither re-emits an already-shipped commit nor reuses a
// Seq.
func TestSelfLogDurableRestart(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x5e, 0x1f}
	const db = "syzy_selflog"
	const origin = crdt.Origin(43)

	createTestDB(t, ctx, db, schemaKV)
	tmp := t.TempDir()
	metaPath := filepath.Join(tmp, "meta.db")
	journalDir := filepath.Join(tmp, "journal")

	// --- run 1: ship 3 local commits ---
	meta1, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta1: %v", err)
	}
	cache1 := nodestate.New(origin)
	e1 := openSelfLog(t, ctx, db, origin, cluster, cache1, meta1, journalDir)
	got1 := runShipping(t, ctx, e1, db, []string{
		`INSERT INTO public.kv VALUES (1,'v1')`,
		`INSERT INTO public.kv VALUES (2,'v2')`,
		`INSERT INTO public.kv VALUES (3,'v3')`,
	}, shippedCount(3))
	if len(got1) != 3 {
		t.Fatalf("run1 shipped %d changesets, want 3", len(got1))
	}
	if next := cache1.SenderNextSeq(origin); next != 4 {
		t.Fatalf("run1 senderNextSeq=%d want 4", next)
	}
	// Index run-1 bytes by Seq so run 2 can prove EXACT-byte replay.
	bytesBySeq := map[crdt.Seq][]byte{}
	for _, cs := range got1 {
		bytesBySeq[cs.Dot.Seq] = cs.Encoded()
	}
	_ = e1.Close()
	if err := meta1.Close(); err != nil {
		t.Fatalf("meta1 close: %v", err)
	}
	waitSlotInactive(t, ctx, dbURL(db), slotName(db))

	// --- run 2: fresh Cache + reopened metadata, same db/slot/journal ---
	meta2, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta2: %v", err)
	}
	cache2 := nodestate.New(origin)
	e2 := openSelfLog(t, ctx, db, origin, cluster, cache2, meta2, journalDir)
	// Rehydration restored the Seq counter (snapshot + self-log replay).
	if next := cache2.SenderNextSeq(origin); next != 4 {
		t.Fatalf("after restart senderNextSeq=%d want 4 (not rehydrated)", next)
	}
	// A new local write resumes past the recovered commits and allocates Seq 4.
	// The publisher re-ships the retained self-log (Seqs 1-3) before the new
	// commit — that re-delivery is the anti-entropy path (peers dedup by Dot),
	// so wait for Seq 4 rather than a raw count.
	got2 := runShipping(t, ctx, e2, db, []string{`INSERT INTO public.kv VALUES (4,'v4')`}, shippedSeq(4))

	seen := map[crdt.Seq]int{}
	for _, cs := range got2 {
		seen[cs.Dot.Seq]++
		// Re-shipped commits must be byte-identical to run 1 — recovery replays
		// exact bytes, never re-derives Dot/Stamp.
		if want, ok := bytesBySeq[cs.Dot.Seq]; ok && !bytes.Equal(cs.Encoded(), want) {
			t.Fatalf("Seq %d re-shipped with different bytes (re-derived, not replayed)", cs.Dot.Seq)
		}
	}
	if seen[4] == 0 {
		t.Fatalf("run2 never shipped Seq 4 (new commit not emitted)")
	}
	for s := crdt.Seq(1); s <= 4; s++ {
		if seen[s] > 1 {
			t.Fatalf("Seq %d shipped %d times within one run (duplicate emission)", s, seen[s])
		}
	}
	if next := cache2.SenderNextSeq(origin); next != 5 {
		t.Fatalf("after new commit senderNextSeq=%d want 5 (Seq continued, not reused)", next)
	}

	if err := e2.DropSlot(ctx); err != nil {
		t.Logf("drop slot: %v", err)
	}
	_ = e2.Close()
	_ = meta2.Close()
}
