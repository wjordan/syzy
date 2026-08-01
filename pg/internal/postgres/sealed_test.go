package postgres

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/s3fetch"
	"github.com/wjordan/syzy/internal/sealer"
)

// TestBucketSealAndOfflineCatchup drives the whole object-storage durability
// loop: a source node's publisher feeds the sealer (Config.OnPublished), the
// sealer uploads changeset epochs to the bucket, and a brand-new node — which
// never saw a single live delivery and has no peer catch-up — converges purely
// from the bucket via s3fetch (TipSource discovers the origin, GapFiller pulls
// the epochs). This is the offline-peer / restore-from-bucket story that makes
// journal truncation safe.
func TestBucketSealAndOfflineCatchup(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x5e, 0xa1}

	be, err := objectstore.Open(ctx, "file://"+t.TempDir())
	if err != nil {
		t.Fatalf("objectstore.Open: %v", err)
	}
	sl := sealer.New(be, sealer.Config{MaxBytes: 1, MaxAge: 10 * time.Millisecond})
	sealerDone := make(chan struct{})
	go func() { defer close(sealerDone); _ = sl.Run(ctx) }()
	defer func() { sl.Stop(); <-sealerDone }()

	// Source: durable engine (self-log) whose publisher feeds the sealer.
	const srcDB = "syzy_sealsrc"
	const srcOrigin = crdt.Origin(21)
	createTestDB(t, ctx, srcDB, schemaKV)
	srcMeta, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("meta open: %v", err)
	}
	defer srcMeta.Close()
	srcCfg := baseTestConfig(srcDB, srcOrigin, cluster)
	srcCfg.Meta = srcMeta
	srcCfg.JournalDir = t.TempDir()
	srcCfg.OnPublished = sl.OnEncoded
	srcCfg.SealedSelfSeq = func() crdt.Seq {
		return crdt.Seq(sl.UploadedSeq(uint64(srcOrigin)))
	}
	src, err := Open(ctx, srcCfg)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer closeEngine(t, ctx, src)

	srcCtx, srcCancel := context.WithCancel(ctx)
	srcDone := make(chan error, 1)
	go func() {
		srcDone <- src.Run(srcCtx, make(chan *crdt.Changeset),
			func(context.Context, *crdt.Changeset) error { return nil })
	}()

	appExec(t, srcDB, `INSERT INTO public.kv VALUES (1,'sealed-one')`)
	appExec(t, srcDB, `INSERT INTO public.kv VALUES (2,'sealed-two')`)
	appExec(t, srcDB, `INSERT INTO public.kv VALUES (3,'sealed-three')`)

	deadline := time.Now().Add(10 * time.Second)
	for sl.UploadedSeq(uint64(srcOrigin)) < 3 {
		select {
		case err := <-srcDone:
			t.Fatalf("src Run exited: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("sealer stuck at seq %d", sl.UploadedSeq(uint64(srcOrigin)))
		}
		time.Sleep(10 * time.Millisecond)
	}
	srcCancel()
	if err := <-srcDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("src Run: %v", err)
	}

	// Fresh node: no live delivery, no peers — bucket only.
	fetchSrc := s3fetch.NewSource(be)
	dst := openEngineWithFillerAndTips(t, ctx, "syzy_sealdst", 22, cluster, fetchSrc, fetchSrc)
	defer closeEngine(t, ctx, dst)

	dstCtx, dstCancel := context.WithCancel(ctx)
	defer dstCancel()
	dstDone := make(chan error, 1)
	go func() {
		dstDone <- dst.Run(dstCtx, make(chan *crdt.Changeset),
			func(context.Context, *crdt.Changeset) error { return nil })
	}()
	// No gap kick fires (nothing was ever delivered live) — nudge the fetcher
	// instead of waiting out its 30s timer. The round's DiscoverTips finds the
	// origin; the ranges pull the sealed epochs.
	dst.orch.kickFetch()

	deadline = time.Now().Add(10 * time.Second)
	for {
		if fr, ok := dst.cfg.Cache.FrontierFor(srcOrigin); ok && fr.LastSeq == 3 {
			break
		}
		select {
		case err := <-dstDone:
			t.Fatalf("dst Run exited: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			fr, _ := dst.cfg.Cache.FrontierFor(srcOrigin)
			t.Fatalf("bucket catch-up incomplete: frontier=%+v", fr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := dumpKV(t, "syzy_sealdst"); len(got) != 3 || got[2] != "sealed-two" {
		t.Fatalf("converged state = %v; want 3 sealed rows", got)
	}

	dstCancel()
	if err := <-dstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("dst Run: %v", err)
	}
}

func openEngineWithFillerAndTips(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, filler *s3fetch.Source, tips *s3fetch.Source) *Engine {
	t.Helper()
	createTestDB(t, ctx, db, schemaKV)
	cfg := baseTestConfig(db, origin, cluster)
	cfg.GapFiller = filler
	cfg.TipSource = tips
	e, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	return e
}
