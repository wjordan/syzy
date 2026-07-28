package publisher

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
)

func newLeaseGenerationPublisher(t *testing.T, be objectstore.Bucket, cfg Config) *Publisher {
	t.Helper()
	cfg.Backend = be
	cfg.ClusterID = "cafe"
	cfg.NodeID = "node-a"
	cfg.WALPath = filepath.Join(t.TempDir(), "app.db-wal")
	cfg.MetaWALPath = filepath.Join(t.TempDir(), "metadata.db-wal")
	cfg.Baseline = func(context.Context, uint64) ([]byte, []byte, func(), error) {
		return []byte("old-app"), []byte("old-meta"), func() {}, nil
	}
	cfg.MetaBaseline = func(context.Context, uint64) ([]byte, func(), error) {
		return []byte("old-meta"), func() {}, nil
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.generation = 1
	p.leaseExpiresAt = time.Now().Add(p.cfg.LeaseExpiry)
	return p
}

func successorHEAD() *objstore.HEAD {
	return &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: "cafe",
		Publisher: &objstore.Publisher{NodeID: "node-a", Generation: 2, ExpiresAtUS: time.Now().Add(time.Minute).UnixMicro()},
		Baseline: &objstore.Baseline{
			TXID:   100,
			LTXRef: objstore.FileRef{Key: "db/0009/successor.ltx", Sha256: "successor-app"},
		},
		MetaBaseline: &objstore.Baseline{
			TXID:   100,
			LTXRef: objstore.FileRef{Key: "metadata/0009/successor.ltx", Sha256: "successor-meta"},
		},
	}
}

func publishTestCoupledBaseline(ctx context.Context, p *Publisher, app, meta []byte) error {
	return p.PublishCoupledBaseline(ctx, func(context.Context, uint64) ([]byte, []byte, func(), error) {
		return app, meta, func() {}, nil
	})
}

// Baseline pointer updates must reject an old generation before the
// already-covered shortcut. The immutable upload may be orphaned, but HEAD must
// remain byte-for-byte the successor's manifest.
func TestBaselineMutationsRejectOldGeneration(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(context.Context, *Publisher) error
	}{
		{name: "coupled", run: func(ctx context.Context, p *Publisher) error {
			return p.takeCoupledBaselines(ctx)
		}},
		{name: "external coupled", run: func(ctx context.Context, p *Publisher) error {
			leadCtx, leadCancel := context.WithCancelCause(context.Background())
			p.mu.Lock()
			p.leadCtx = leadCtx
			p.leadCancel = leadCancel
			p.leadOps = &sync.WaitGroup{}
			p.acceptOps = true
			p.mu.Unlock()
			close(p.seeded)
			return publishTestCoupledBaseline(ctx, p, []byte("external-app"), []byte("external-meta"))
		}},
		{name: "metadata only", run: func(ctx context.Context, p *Publisher) error {
			return p.takeMetaBaselineOnly(ctx)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			be, err := objectstore.OpenFS(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			want := successorHEAD()
			if _, err := objstore.CASHead(context.Background(), be, want, objectstore.IfAbsent()); err != nil {
				t.Fatalf("seed successor HEAD: %v", err)
			}
			p := newLeaseGenerationPublisher(t, be, Config{})

			err = tc.run(context.Background(), p)
			if !errors.Is(err, errLeaseLost) {
				t.Fatalf("error = %v, want ownership loss", err)
			}
			got, _, err := objstore.LoadHEAD(context.Background(), be)
			if err != nil {
				t.Fatalf("LoadHEAD: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("stale generation moved HEAD:\n got  %+v\n want %+v", got, want)
			}
		})
	}
}

// Run must cancel and join every leader loop before returning on ownership
// loss. A checkpoint callback blocked on its claim context makes that ordering
// observable without sleeps.
func TestRunCancelsLeaderGoroutinesBeforeReturn(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: "cafe",
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}

	entered := make(chan struct{})
	exited := make(chan struct{})
	var enterOnce, exitOnce sync.Once
	p := newLeaseGenerationPublisher(t, be, Config{
		HeartbeatInterval:  100 * time.Millisecond,
		LeaseExpiry:        time.Second,
		LTXSyncInterval:    time.Hour,
		CheckpointInterval: time.Millisecond,
		CompactInterval:    time.Hour,
		RetentionGrace:     time.Hour,
		AppCheckpoint: func(ctx context.Context, _ string, underFence func(func() error) error) error {
			return underFence(func() error {
				enterOnce.Do(func() { close(entered) })
				<-ctx.Done()
				exitOnce.Do(func() { close(exited) })
				return ctx.Err()
			})
		},
	})
	// claimOrTakeover owns generation assignment; the helper's test-only seed
	// must not pre-install one for a real Run.
	p.generation = 0

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(runCtx) }()

	select {
	case <-entered:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("checkpoint leader goroutine did not start")
	}

	// Replace the claim with a successor. Retry only CAS contention with the
	// heartbeat; no timing sleep is needed.
	replaced := false
	for i := 0; i < 100 && !replaced; i++ {
		head, etag, err := objstore.LoadHEAD(context.Background(), be)
		if err != nil {
			t.Fatalf("LoadHEAD: %v", err)
		}
		next := *head
		next.Publisher = &objstore.Publisher{
			NodeID: "node-b", Generation: head.Publisher.Generation + 1,
			ExpiresAtUS: time.Now().Add(time.Minute).UnixMicro(),
		}
		_, err = objstore.CASHead(context.Background(), be, &next, &etag)
		switch {
		case err == nil:
			replaced = true
		case errors.Is(err, objectstore.ErrPreconditionFailed):
		default:
			t.Fatalf("replace publisher: %v", err)
		}
	}
	if !replaced {
		t.Fatal("could not replace publisher claim")
	}
	if err := publishTestCoupledBaseline(context.Background(), p, []byte("stale-app"), []byte("stale-meta")); !errors.Is(err, errLeaseLost) {
		t.Fatalf("stale baseline error = %v, want ownership loss", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, errLeaseLost) {
			t.Fatalf("Run error = %v, want ownership loss", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Run did not stop after ownership loss")
	}
	select {
	case <-exited:
	default:
		t.Fatal("Run returned before leader goroutine observed cancellation")
	}
}
