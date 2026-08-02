package publisher_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/publisher"
)

// stubBaseline returns the BaselineFunc used by takeover. Returns
// two arbitrary LTX byte buffers (PublishCoupledBaselines doesn't
// decode them).
func stubBaseline(t *testing.T) (publisher.BaselineFunc, *int) {
	t.Helper()
	calls := 0
	return func(ctx context.Context, txid uint64) (publisher.EncodedBaseline, publisher.EncodedBaseline, func(), error) {
		calls++
		return publisher.EncodedBaseline{LTX: []byte("fake-app-baseline-ltx")}, publisher.EncodedBaseline{LTX: []byte("fake-meta-baseline-ltx")}, func() {}, nil
	}, &calls
}

// stubMetaBaseline returns the MetaBaselineFunc used on resume.
func stubMetaBaseline(t *testing.T) (publisher.MetaBaselineFunc, *int) {
	t.Helper()
	calls := 0
	return func(ctx context.Context, txid uint64) (publisher.EncodedBaseline, func(), error) {
		calls++
		return publisher.EncodedBaseline{LTX: []byte("fake-meta-baseline-ltx")}, func() {}, nil
	}, &calls
}

// stubMetaBaselineFn returns just the func (no calls counter — for
// Config literals that don't observe resume baselines).
func stubMetaBaselineFn(t *testing.T) publisher.MetaBaselineFunc {
	fn, _ := stubMetaBaseline(t)
	return fn
}

// TestInitialClaim: empty bucket → claim succeeds, baseline taken,
// clean shutdown leaves an expired holder identity.
func TestInitialClaim(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	baseline, baselineCalls := stubBaseline(t)

	// Pre-stamp HEAD with the cluster_id beacon (what ResolveClusterID
	// would do on first Open). We sidestep that here for test focus.
	clusterID := "cafef00d"
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: clusterID,
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}

	// We don't need a real WAL here; the tailer's Sync will return
	// nil for missing/empty WAL.
	walPath := filepath.Join(t.TempDir(), "missing-app.db-wal")

	p, err := publisher.New(publisher.Config{
		Backend:           be,
		ClusterID:         clusterID,
		NodeID:            "nodeA",
		WALPath:           walPath,
		MetaWALPath:       walPath,
		Baseline:          baseline,
		MetaBaseline:      stubMetaBaselineFn(t),
		HeartbeatInterval: 10 * time.Millisecond,
		LeaseExpiry:       50 * time.Millisecond,
		LTXSyncInterval:   5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	waitForPublisher(t, be, "nodeA", 500*time.Millisecond)
	waitForBaselineAfter(t, be, false, 0, 500*time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if *baselineCalls == 0 {
		t.Errorf("expected baseline to be called on initial claim")
	}
	head, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.Publisher == nil || head.Publisher.NodeID != "nodeA" || head.Publisher.ExpiresAtUS != 0 {
		t.Errorf("HEAD.Publisher = %+v, want expired nodeA holder after clean shutdown", head.Publisher)
	}
	if head.Baseline == nil {
		t.Errorf("HEAD.Baseline missing after claim")
	}
}

// TestResumeAfterRestart: same NodeID re-claims WITH a coupled
// rebaseline. The restarted process cannot prove its WAL is contiguous
// with the bucket chain (a checkpoint between the predecessor's last
// ship and the successor's first WAL read strands committed txns), so
// resume re-anchors both streams like a takeover.
func TestResumeAfterRestart(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	clusterID := "deadbeef"
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: clusterID,
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	baseline1, calls1 := stubBaseline(t)
	walPath := filepath.Join(t.TempDir(), "no-wal")
	mkPub := func(baseline publisher.BaselineFunc) *publisher.Publisher {
		p, err := publisher.New(publisher.Config{
			Backend:           be,
			ClusterID:         clusterID,
			NodeID:            "nodeA",
			WALPath:           walPath,
			MetaWALPath:       walPath,
			Baseline:          baseline,
			MetaBaseline:      stubMetaBaselineFn(t),
			HeartbeatInterval: 10 * time.Millisecond,
			LeaseExpiry:       100 * time.Millisecond,
			LTXSyncInterval:   5 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return p
	}
	// First run claims + baselines.
	ctx1, cancel1 := context.WithCancel(context.Background())
	p1 := mkPub(baseline1)
	done1 := make(chan error, 1)
	go func() { done1 <- p1.Run(ctx1) }()
	waitForPublisher(t, be, "nodeA", 500*time.Millisecond)
	waitForBaselineAfter(t, be, false, 0, 500*time.Millisecond)
	cancel1()
	if err := <-done1; err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if *calls1 == 0 {
		t.Fatalf("first run did not baseline")
	}
	head1, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD after first run: %v", err)
	}
	if head1.Baseline == nil {
		t.Fatalf("first run left no app baseline")
	}
	appTXID := head1.Baseline.TXID
	// Second run with same NodeID must take a fresh coupled baseline.
	baseline2, calls2 := stubBaseline(t)
	ctx2, cancel2 := context.WithCancel(context.Background())
	p2 := mkPub(baseline2)
	done2 := make(chan error, 1)
	go func() { done2 <- p2.Run(ctx2) }()
	waitForPublisher(t, be, "nodeA", 500*time.Millisecond)
	waitForBaselineAfter(t, be, false, appTXID, 500*time.Millisecond)
	cancel2()
	if err := <-done2; err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if *calls2 == 0 {
		t.Errorf("expected coupled baseline on resume; got none")
	}
	head2, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD after second run: %v", err)
	}
	if head2.Baseline == nil || head2.Baseline.TXID <= appTXID {
		t.Errorf("app baseline did not advance on resume: before=%d after=%+v", appTXID, head2.Baseline)
	}
	if head2.MetaBaseline == nil || head2.Baseline == nil || head2.MetaBaseline.TXID != head2.Baseline.TXID {
		t.Errorf("resume baselines not coupled: app=%+v meta=%+v", head2.Baseline, head2.MetaBaseline)
	}
}

// TestTakeoverFromExpired: nodeB takes over after nodeA's lease
// expires; baseline gets called.
func TestTakeoverFromExpired(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	clusterID := "f00ba1"
	appRef, err := objstore.PublishLTX(context.Background(), be, objstore.DBPrefix, objstore.BaselineLevel, 1, 1, []byte("old-app-baseline-ltx"))
	if err != nil {
		t.Fatalf("seed app baseline LTX: %v", err)
	}
	metaRef, err := objstore.PublishLTX(context.Background(), be, objstore.MetadataPrefix, objstore.BaselineLevel, 1, 1, []byte("old-meta-baseline-ltx"))
	if err != nil {
		t.Fatalf("seed meta baseline LTX: %v", err)
	}
	// Plant a stale publisher in HEAD (expires_at_us in the past).
	stale := &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: clusterID,
		Publisher: &objstore.Publisher{
			NodeID:      "nodeA",
			Generation:  3,
			ExpiresAtUS: time.Now().Add(-time.Hour).UnixMicro(),
		},
		Baseline: &objstore.Baseline{
			TXID:      1,
			LTXRef:    appRef,
			BuiltAtUS: time.Now().UnixMicro(),
		},
		MetaBaseline: &objstore.Baseline{
			TXID:      1,
			LTXRef:    metaRef,
			BuiltAtUS: time.Now().UnixMicro(),
		},
	}
	if _, err := objstore.CASHead(context.Background(), be, stale, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	baseline, calls := stubBaseline(t)
	walPath := filepath.Join(t.TempDir(), "no-wal")
	p, err := publisher.New(publisher.Config{
		Backend:           be,
		ClusterID:         clusterID,
		NodeID:            "nodeB",
		WALPath:           walPath,
		MetaWALPath:       walPath,
		Baseline:          baseline,
		MetaBaseline:      stubMetaBaselineFn(t),
		HeartbeatInterval: 10 * time.Millisecond,
		LeaseExpiry:       100 * time.Millisecond,
		LTXSyncInterval:   5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	waitForPublisher(t, be, "nodeB", 500*time.Millisecond)
	waitForBaselineAfter(t, be, false, 1, 500*time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if *calls == 0 {
		t.Errorf("takeover should have called baseline")
	}
	head, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.Publisher == nil || head.Publisher.NodeID != "nodeB" || head.Publisher.ExpiresAtUS != 0 {
		t.Errorf("HEAD.Publisher = %+v, want expired nodeB holder after clean shutdown", head.Publisher)
	}
}

// TestTakeoverRefusesEmptyClobber: a node that opened with an empty
// local DB (LocalFreshAtOpen) must NOT take over an expired foreign
// lease and rebaseline over the bucket's existing baseline — that would
// abandon the bucket's data. It fails loud with ErrBehindBucket and
// leaves HEAD's holder untouched (no stale lease claimed).
func TestTakeoverRefusesEmptyClobber(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	clusterID := "c10bbe11"
	appRef, err := objstore.PublishLTX(context.Background(), be, objstore.DBPrefix, objstore.BaselineLevel, 1, 1, []byte("old-app-baseline-ltx"))
	if err != nil {
		t.Fatalf("seed app baseline LTX: %v", err)
	}
	metaRef, err := objstore.PublishLTX(context.Background(), be, objstore.MetadataPrefix, objstore.BaselineLevel, 1, 1, []byte("old-meta-baseline-ltx"))
	if err != nil {
		t.Fatalf("seed meta baseline LTX: %v", err)
	}
	stale := &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: clusterID,
		Publisher: &objstore.Publisher{
			NodeID:      "nodeA",
			Generation:  3,
			ExpiresAtUS: time.Now().Add(-time.Hour).UnixMicro(),
		},
		Baseline:     &objstore.Baseline{TXID: 1, LTXRef: appRef, BuiltAtUS: time.Now().UnixMicro()},
		MetaBaseline: &objstore.Baseline{TXID: 1, LTXRef: metaRef, BuiltAtUS: time.Now().UnixMicro()},
	}
	if _, err := objstore.CASHead(context.Background(), be, stale, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	baseline, calls := stubBaseline(t)
	walPath := filepath.Join(t.TempDir(), "no-wal")
	p, err := publisher.New(publisher.Config{
		Backend:           be,
		ClusterID:         clusterID,
		NodeID:            "nodeB",
		WALPath:           walPath,
		MetaWALPath:       walPath,
		Baseline:          baseline,
		MetaBaseline:      stubMetaBaselineFn(t),
		HeartbeatInterval: 10 * time.Millisecond,
		LeaseExpiry:       100 * time.Millisecond,
		LTXSyncInterval:   5 * time.Millisecond,
		LocalFreshAtOpen:  true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Run(ctx); !errors.Is(err, publisher.ErrBehindBucket) {
		t.Fatalf("Run = %v, want ErrBehindBucket", err)
	}
	if *calls != 0 {
		t.Errorf("baseline called %d times; want 0 (must not clobber)", *calls)
	}
	head, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.Publisher == nil || head.Publisher.NodeID != "nodeA" || head.Publisher.Generation != 3 {
		t.Errorf("HEAD.Publisher = %+v, want untouched nodeA gen 3 (no stale claim)", head.Publisher)
	}
}

// TestTakeoverFreshAllowedWithoutBaseline: the empty-DB guard fires only
// when the bucket has data to lose. With no baseline yet, a fresh node
// taking over an expired holder still claims and bootstraps the first
// baseline — fresh-cluster bootstrap must not be blocked.
func TestTakeoverFreshAllowedWithoutBaseline(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	clusterID := "f8e54"
	stale := &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: clusterID,
		Publisher: &objstore.Publisher{
			NodeID:      "nodeA",
			Generation:  2,
			ExpiresAtUS: time.Now().Add(-time.Hour).UnixMicro(),
		},
		// No Baseline: a prior holder existed but never wrote one.
	}
	if _, err := objstore.CASHead(context.Background(), be, stale, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	baseline, calls := stubBaseline(t)
	walPath := filepath.Join(t.TempDir(), "no-wal")
	p, err := publisher.New(publisher.Config{
		Backend:           be,
		ClusterID:         clusterID,
		NodeID:            "nodeB",
		WALPath:           walPath,
		MetaWALPath:       walPath,
		Baseline:          baseline,
		MetaBaseline:      stubMetaBaselineFn(t),
		HeartbeatInterval: 10 * time.Millisecond,
		LeaseExpiry:       100 * time.Millisecond,
		LTXSyncInterval:   5 * time.Millisecond,
		LocalFreshAtOpen:  true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	waitForPublisher(t, be, "nodeB", 500*time.Millisecond)
	// Wait for the bootstrapped baseline before cancelling: the lease claim
	// becomes visible before seedTXIDCounters + the baseline run, so cancelling
	// on the claim alone races the seed (Run would return context.Canceled and
	// the baseline would never fire). Matches every other baseline-asserting test.
	waitForBaselineAfter(t, be, false, 0, 500*time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if *calls == 0 {
		t.Errorf("expected baseline bootstrap on fresh claim without prior baseline")
	}
}

func TestCleanHandoffDifferentNodeRequiresCoupledBaseline(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	clusterID := "c1ea11"
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: clusterID,
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	walPath := filepath.Join(t.TempDir(), "no-wal")
	mkPub := func(nodeID string, baseline publisher.BaselineFunc) *publisher.Publisher {
		p, err := publisher.New(publisher.Config{
			Backend:           be,
			ClusterID:         clusterID,
			NodeID:            nodeID,
			WALPath:           walPath,
			MetaWALPath:       walPath,
			Baseline:          baseline,
			MetaBaseline:      stubMetaBaselineFn(t),
			HeartbeatInterval: 10 * time.Millisecond,
			LeaseExpiry:       100 * time.Millisecond,
			LTXSyncInterval:   5 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("New %s: %v", nodeID, err)
		}
		return p
	}

	baselineA, callsA := stubBaseline(t)
	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan error, 1)
	go func() { doneA <- mkPub("nodeA", baselineA).Run(ctxA) }()
	waitForPublisher(t, be, "nodeA", 500*time.Millisecond)
	waitForBaselineAfter(t, be, false, 0, 500*time.Millisecond)
	cancelA()
	if err := <-doneA; err != nil {
		t.Fatalf("nodeA Run: %v", err)
	}
	if *callsA == 0 {
		t.Fatalf("nodeA did not publish initial baseline")
	}
	headA, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD after nodeA: %v", err)
	}
	if headA.Publisher == nil || headA.Publisher.NodeID != "nodeA" || headA.Publisher.ExpiresAtUS != 0 {
		t.Fatalf("released publisher = %+v, want expired nodeA identity", headA.Publisher)
	}

	baselineB, callsB := stubBaseline(t)
	ctxB, cancelB := context.WithCancel(context.Background())
	doneB := make(chan error, 1)
	go func() { doneB <- mkPub("nodeB", baselineB).Run(ctxB) }()
	waitForPublisher(t, be, "nodeB", 500*time.Millisecond)
	waitForBaselineAfter(t, be, false, headA.Baseline.TXID, 500*time.Millisecond)
	cancelB()
	if err := <-doneB; err != nil {
		t.Fatalf("nodeB Run: %v", err)
	}
	if *callsB == 0 {
		t.Fatalf("different-node clean handoff did not publish coupled baseline")
	}
}

// TestClusterIDMismatch: publisher refuses if HEAD has a different cluster_id.
func TestClusterIDMismatch(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: "abc",
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	baseline, _ := stubBaseline(t)
	walPath := filepath.Join(t.TempDir(), "no-wal")
	p, _ := publisher.New(publisher.Config{
		Backend:           be,
		ClusterID:         "xyz", // different
		NodeID:            "nodeA",
		WALPath:           walPath,
		MetaWALPath:       walPath,
		Baseline:          baseline,
		MetaBaseline:      stubMetaBaselineFn(t),
		HeartbeatInterval: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := p.Run(ctx)
	if !errors.Is(err, objstore.ErrClusterIDMismatch) {
		t.Fatalf("expected ErrClusterIDMismatch, got %v", err)
	}
}

func waitForPublisher(t *testing.T, be objectstore.Bucket, nodeID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		head, _, err := objstore.LoadHEAD(context.Background(), be)
		if err != nil {
			t.Fatalf("LoadHEAD: %v", err)
		}
		if head.Publisher != nil && head.Publisher.NodeID == nodeID {
			return
		}
		time.Sleep(time.Millisecond)
	}
	head, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD after timeout: %v", err)
	}
	t.Fatalf("timeout waiting for publisher %q, HEAD.Publisher=%+v", nodeID, head.Publisher)
}

func waitForBaselineAfter(t *testing.T, be objectstore.Bucket, meta bool, txid uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		head, _, err := objstore.LoadHEAD(context.Background(), be)
		if err != nil {
			t.Fatalf("LoadHEAD: %v", err)
		}
		var baseline *objstore.Baseline
		if meta {
			baseline = head.MetaBaseline
		} else {
			baseline = head.Baseline
		}
		if baseline != nil && baseline.TXID > txid {
			return
		}
		time.Sleep(time.Millisecond)
	}
	head, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD after timeout: %v", err)
	}
	t.Fatalf("timeout waiting for baseline meta=%v after txid %d, baseline=%+v meta_baseline=%+v",
		meta, txid, head.Baseline, head.MetaBaseline)
}
