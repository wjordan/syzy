package publisher

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/sqlitebridge"
)

func activateTestGeneration(p *Publisher) (context.Context, context.CancelCauseFunc, *sync.WaitGroup) {
	leadCtx, leadCancel := context.WithCancelCause(context.Background())
	ops := &sync.WaitGroup{}
	p.mu.Lock()
	p.leadCtx = leadCtx
	p.leadCancel = leadCancel
	p.leadOps = ops
	p.acceptOps = true
	p.mu.Unlock()
	select {
	case <-p.seeded:
	default:
		close(p.seeded)
	}
	return leadCtx, leadCancel, ops
}

func TestGenerationOperationGateCancelsJoinsAndRejects(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := newLeaseGenerationPublisher(t, be, Config{})
	_, leadCancel, ops := activateTestGeneration(p)

	entered := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- p.PublishCoupledBaseline(context.Background(), func(ctx context.Context, _ uint64) ([]byte, []byte, func(), error) {
			close(entered)
			<-release
			<-ctx.Done()
			return nil, nil, nil, context.Cause(ctx)
		})
	}()
	<-entered

	p.mu.Lock()
	p.acceptOps = false
	p.mu.Unlock()
	leadCancel(errLeaseLost)
	joined := make(chan struct{})
	go func() {
		ops.Wait()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("generation join returned while admitted operation was running")
	default:
	}
	close(release)
	if err := <-result; !errors.Is(err, errLeaseLost) {
		t.Fatalf("admitted operation error = %v, want lease lost", err)
	}
	select {
	case <-joined:
	case <-time.After(20 * time.Millisecond):
		t.Fatal("generation operation did not join")
	}
	if err := publishTestCoupledBaseline(context.Background(), p, []byte("app"), []byte("meta")); !errors.Is(err, errLeaseLost) {
		t.Fatalf("post-stop operation error = %v, want lease lost", err)
	}
}

func TestSyncAppStreamGenerationCancellationIsStandbyNoOp(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := newLeaseGenerationPublisher(t, be, Config{})
	_, leadCancel, _ := activateTestGeneration(p)

	dbPath := filepath.Join(t.TempDir(), "app.db")
	conn, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, sql := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA wal_autocheckpoint=0`,
		`CREATE TABLE t (id INTEGER PRIMARY KEY)`,
		`INSERT INTO t VALUES (1)`,
	} {
		if err := conn.Exec(sql); err != nil {
			t.Fatalf("Exec %q: %v", sql, err)
		}
	}
	started := make(chan struct{})
	var once sync.Once
	p.app.tailer = ltxstream.New(ltxstream.Config{
		WALPath:  dbPath + "-wal",
		NextTXID: func() uint64 { return 1 },
		OnLTX: func(ctx context.Context, _ ltx.Header, _ []byte) error {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return ctx.Err()
		},
	}, ltxstream.Position{})

	done := make(chan error, 1)
	go func() { done <- p.SyncAppStream(context.Background()) }()
	select {
	case <-started:
	case <-time.After(20 * time.Millisecond):
		t.Fatal("app Sync did not reach upload callback")
	}
	leadCancel(errLeaseLost)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("generation cancellation surfaced as snapshotter error: %v", err)
		}
	case <-time.After(20 * time.Millisecond):
		t.Fatal("canceled app Sync did not return")
	}
}

func TestCoupledBaselinesSerializeCaptureAndPromotion(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: "cafe",
		Publisher: &objstore.Publisher{NodeID: "node-a", Generation: 1, ExpiresAtUS: expires.UnixMicro()},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatal(err)
	}
	p := newLeaseGenerationPublisher(t, be, Config{})
	p.leaseExpiresAt = expires
	activateTestGeneration(p)

	olderEntered := make(chan struct{})
	releaseOlder := make(chan struct{})
	newerEntered := make(chan struct{})
	var olderTXID, newerTXID atomic.Uint64
	olderDone := make(chan error, 1)
	go func() {
		olderDone <- p.PublishCoupledBaseline(context.Background(), func(_ context.Context, txid uint64) ([]byte, []byte, func(), error) {
			close(olderEntered)
			<-releaseOlder
			olderTXID.Store(txid)
			return []byte("older-app"), []byte("older-meta"), func() {}, nil
		})
	}()
	<-olderEntered
	newerDone := make(chan error, 1)
	go func() {
		newerDone <- p.PublishCoupledBaseline(context.Background(), func(_ context.Context, txid uint64) ([]byte, []byte, func(), error) {
			close(newerEntered)
			newerTXID.Store(txid)
			return []byte("newer-app"), []byte("newer-meta"), func() {}, nil
		})
	}()
	select {
	case <-newerEntered:
		t.Fatal("newer capture entered while older capture was still in flight")
	case <-time.After(2 * time.Millisecond):
	}
	close(releaseOlder)
	if err := <-olderDone; err != nil {
		t.Fatalf("older publish: %v", err)
	}
	if err := <-newerDone; err != nil {
		t.Fatalf("newer publish: %v", err)
	}
	if olderTXID.Load() >= newerTXID.Load() {
		t.Fatalf("capture order TXIDs = old:%d new:%d", olderTXID.Load(), newerTXID.Load())
	}
	head, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatal(err)
	}
	if head.Baseline == nil || head.MetaBaseline == nil || head.Baseline.TXID != newerTXID.Load() || head.MetaBaseline.TXID != newerTXID.Load() {
		t.Fatalf("final HEAD did not represent newest serialized capture: %+v", head)
	}
}

func TestTXIDAllocatorsDoNotCollideWithBaseline(t *testing.T) {
	const iterations = 1000
	for i := 0; i < iterations; i++ {
		p := &Publisher{}
		p.lastBucketTXID.Store(100)
		p.metaTXIDCounter.Store(100)
		start := make(chan struct{})
		var meta, baseline uint64
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; meta = p.allocMetaTXID() }()
		go func() { defer wg.Done(); <-start; baseline = p.allocBaselineTXID() }()
		close(start)
		wg.Wait()
		if meta == baseline {
			t.Fatalf("iteration %d: meta and baseline both allocated %d", i, meta)
		}
	}
}

func TestNewValidatesLeaseRenewalWindow(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := Config{
		Backend:     be,
		ClusterID:   "cafe",
		NodeID:      "node-a",
		WALPath:     filepath.Join(t.TempDir(), "app-wal"),
		MetaWALPath: filepath.Join(t.TempDir(), "meta-wal"),
		Baseline: func(context.Context, uint64) ([]byte, []byte, func(), error) {
			return nil, nil, func() {}, nil
		},
		MetaBaseline: func(context.Context, uint64) ([]byte, func(), error) {
			return nil, func() {}, nil
		},
	}
	for _, tc := range []struct {
		name  string
		hb    time.Duration
		lease time.Duration
	}{
		{name: "negative heartbeat", hb: -time.Second, lease: 10 * time.Second},
		{name: "negative lease", hb: time.Second, lease: -time.Second},
		{name: "no renewal opportunity", hb: 10 * time.Millisecond, lease: 20 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.HeartbeatInterval, cfg.LeaseExpiry = tc.hb, tc.lease
			if _, err := New(cfg); err == nil {
				t.Fatal("New accepted unusable lease timing")
			}
		})
	}
	base.HeartbeatInterval = 10 * time.Millisecond
	base.LeaseExpiry = 20*time.Millisecond + time.Nanosecond
	if _, err := New(base); err != nil {
		t.Fatalf("New rejected strict >2x timing: %v", err)
	}
}

// The baseline TXID must be allocated before the prepare callback pins the
// databases: an L0 the tailers drain while prepare runs must sort above the
// baseline, or restore filtering would hide its commits (they are only in the
// pin if they happened before it).
func TestPublishCoupledBaselineAllocatesTXIDBeforePin(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour)
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: "cafe",
		Publisher: &objstore.Publisher{NodeID: "node-a", Generation: 1, ExpiresAtUS: expires.UnixMicro()},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatal(err)
	}
	p := newLeaseGenerationPublisher(t, be, Config{})
	p.leaseExpiresAt = expires
	activateTestGeneration(p)
	p.lastBucketTXID.Store(10)
	p.metaTXIDCounter.Store(20)

	var baselineTXID, concurrentL0 uint64
	err = p.PublishCoupledBaseline(context.Background(), func(_ context.Context, txid uint64) ([]byte, []byte, func(), error) {
		baselineTXID = txid
		// A tailer drain racing the pin allocates only above the baseline.
		concurrentL0 = p.allocBucketTXID()
		return []byte("app"), []byte("meta"), func() {}, nil
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if baselineTXID != 21 {
		t.Fatalf("baseline TXID = %d, want above both stream counters (21)", baselineTXID)
	}
	if concurrentL0 <= baselineTXID {
		t.Fatalf("concurrent L0 TXID %d not above baseline %d", concurrentL0, baselineTXID)
	}
}

func TestPublicationConflictFencesGeneration(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := newLeaseGenerationPublisher(t, be, Config{})
	leadCtx, _, _ := activateTestGeneration(p)
	err = p.fenceMutationError(fmt.Errorf("compact L1: %w", objstore.ErrLTXConflict))
	if !errors.Is(err, errPublisherUnhealthy) {
		t.Fatalf("fenceMutationError = %v, want unhealthy", err)
	}
	if cause := context.Cause(leadCtx); !errors.Is(cause, errPublisherUnhealthy) {
		t.Fatalf("generation cause = %v, want unhealthy", cause)
	}
}

func startRunTestPublisher(t *testing.T, mutate func(*Config)) (*Publisher, objectstore.Bucket, context.CancelFunc, <-chan error) {
	t.Helper()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: "cafe",
	}, objectstore.IfAbsent()); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Backend:            be,
		ClusterID:          "cafe",
		NodeID:             "node-a",
		WALPath:            filepath.Join(t.TempDir(), "missing-app-wal"),
		MetaWALPath:        filepath.Join(t.TempDir(), "missing-meta-wal"),
		HeartbeatInterval:  100 * time.Millisecond,
		LeaseExpiry:        time.Second,
		LTXSyncInterval:    time.Hour,
		CheckpointInterval: time.Hour,
		CompactInterval:    time.Hour,
		RetentionGrace:     time.Hour,
		Baseline: func(context.Context, uint64) ([]byte, []byte, func(), error) {
			return []byte("app-baseline"), []byte("meta-baseline"), func() {}, nil
		},
		MetaBaseline: func(context.Context, uint64) ([]byte, func(), error) {
			return []byte("meta-baseline"), func() {}, nil
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(runCtx) }()
	select {
	case <-p.seeded:
	case err := <-done:
		t.Fatalf("publisher exited before readiness: %v", err)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("publisher did not become ready")
	}
	return p, be, cancel, done
}

func TestRunLeaseReleaseMatrix(t *testing.T) {
	t.Run("normal stop releases", func(t *testing.T) {
		_, be, cancel, done := startRunTestPublisher(t, nil)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
		head, _, err := objstore.LoadHEAD(context.Background(), be)
		if err != nil {
			t.Fatal(err)
		}
		if head.Publisher == nil || head.Publisher.ExpiresAtUS != 0 {
			t.Fatalf("normal stop retained lease: %+v", head.Publisher)
		}
	})

	t.Run("handoff retains", func(t *testing.T) {
		p, be, cancel, done := startRunTestPublisher(t, nil)
		p.RetainLeaseOnStop()
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
		head, _, err := objstore.LoadHEAD(context.Background(), be)
		if err != nil {
			t.Fatal(err)
		}
		if head.Publisher == nil || head.Publisher.ExpiresAtUS == 0 {
			t.Fatalf("handoff released lease: %+v", head.Publisher)
		}
	})

	t.Run("lost owner leaves successor", func(t *testing.T) {
		p, be, cancel, done := startRunTestPublisher(t, nil)
		defer cancel()
		head, etag, err := objstore.LoadHEAD(context.Background(), be)
		if err != nil {
			t.Fatal(err)
		}
		next := *head
		next.Publisher = &objstore.Publisher{NodeID: "node-b", Generation: head.Publisher.Generation + 1, ExpiresAtUS: time.Now().Add(time.Minute).UnixMicro()}
		if _, err := objstore.CASHead(context.Background(), be, &next, &etag); err != nil {
			t.Fatal(err)
		}
		p.cancelLeadership(errLeaseLost)
		if err := <-done; !errors.Is(err, errLeaseLost) {
			t.Fatalf("Run error = %v, want lease lost", err)
		}
		got, _, err := objstore.LoadHEAD(context.Background(), be)
		if err != nil {
			t.Fatal(err)
		}
		if got.Publisher == nil || got.Publisher.NodeID != "node-b" || got.Publisher.ExpiresAtUS == 0 {
			t.Fatalf("lost owner disturbed successor: %+v", got.Publisher)
		}
	})

	t.Run("unhealthy leaves natural expiry", func(t *testing.T) {
		p, be, cancel, done := startRunTestPublisher(t, nil)
		defer cancel()
		before, _, err := objstore.LoadHEAD(context.Background(), be)
		if err != nil {
			t.Fatal(err)
		}
		p.cancelLeadership(pipelineHealthError(time.Now(), p.leaseExpiresAt, errors.New("test unhealthy")))
		if err := <-done; !errors.Is(err, errPublisherUnhealthy) {
			t.Fatalf("Run error = %v, want unhealthy", err)
		}
		after, _, err := objstore.LoadHEAD(context.Background(), be)
		if err != nil {
			t.Fatal(err)
		}
		if after.Publisher == nil || after.Publisher.ExpiresAtUS != before.Publisher.ExpiresAtUS {
			t.Fatalf("unhealthy stop changed lease: before=%+v after=%+v", before.Publisher, after.Publisher)
		}
	})
}

func TestRunFinalCheckpointShipsPendingWALBeforeRelease(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "app.db")
	conn, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, sql := range []string{`PRAGMA journal_mode=WAL`, `PRAGMA wal_autocheckpoint=0`} {
		if err := conn.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	var checkpointCalls atomic.Int64
	p, be, cancel, done := startRunTestPublisher(t, func(cfg *Config) {
		cfg.WALPath = dbPath + "-wal"
		cfg.AppCheckpoint = func(_ context.Context, _ string, underFence func(func() error) error) error {
			return underFence(func() error {
				checkpointCalls.Add(1)
				return nil
			})
		}
	})
	// Establish that the Run tailer has completed its initial pass and is now
	// waiting on its long interval before creating the pending WAL frames.
	if err := p.SyncAppStream(context.Background()); err != nil {
		t.Fatalf("initial idle sync: %v", err)
	}
	for _, sql := range []string{`CREATE TABLE t (id INTEGER PRIMARY KEY)`, `INSERT INTO t VALUES (1)`} {
		if err := conn.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	files, err := objstore.ListLTX(context.Background(), be, objstore.DBPrefix, objstore.L0Level)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("final checkpoint did not ship pending WAL frames")
	}
	if checkpointCalls.Load() == 0 {
		t.Fatal("final coordinated checkpoint callback did not execute")
	}
	if tailer := p.streamTailer(&p.app); tailer != nil {
		t.Fatal("Run retained stopped app tailer after final checkpoint")
	}
	if err := p.SyncAppStream(context.Background()); err != nil {
		t.Fatalf("post-Run SyncAppStream should be a standby no-op: %v", err)
	}
}
