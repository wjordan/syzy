package publisher

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/internal/objstore"
)

type heartbeatBucket struct {
	objectstore.Bucket
	mu          sync.Mutex
	headGets    int
	headPuts    int
	failHeadPut error
	beforePut   func()
	blockGet    bool
}

func (b *heartbeatBucket) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	if key == objstore.HEADKey {
		b.mu.Lock()
		b.headGets++
		block := b.blockGet
		b.mu.Unlock()
		if block {
			<-ctx.Done()
			return nil, "", ctx.Err()
		}
	}
	return b.Bucket.Get(ctx, key)
}

func (b *heartbeatBucket) Put(ctx context.Context, key string, body io.Reader, length int64, ifMatch *string) (string, error) {
	if key == objstore.HEADKey && ifMatch != nil {
		b.mu.Lock()
		b.headPuts++
		fail, hook := b.failHeadPut, b.beforePut
		b.beforePut = nil
		b.mu.Unlock()
		if hook != nil {
			hook()
		}
		if fail != nil {
			return "", fail
		}
	}
	return b.Bucket.Put(ctx, key, body, length, ifMatch)
}

func (b *heartbeatBucket) counts() (gets, puts int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.headGets, b.headPuts
}

type heartbeatFixture struct {
	p      *Publisher
	raw    objectstore.Bucket
	bucket *heartbeatBucket
	now    time.Time
	expiry time.Time
}

func newHeartbeatFixture(t *testing.T) *heartbeatFixture {
	t.Helper()
	raw, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	expiry := now.Add(30 * time.Second)
	if _, err := objstore.CASHead(context.Background(), raw, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: "cafe",
		Publisher: &objstore.Publisher{NodeID: "node-a", Generation: 1, ExpiresAtUS: expiry.UnixMicro()},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	b := &heartbeatBucket{Bucket: raw}
	p := newLeaseGenerationPublisher(t, b, Config{
		HeartbeatInterval: 10 * time.Second,
		LeaseExpiry:       40 * time.Second,
	})
	f := &heartbeatFixture{p: p, raw: raw, bucket: b, now: now, expiry: expiry}
	p.now = func() time.Time { return f.now }
	p.leaseExpiresAt = expiry
	p.app.tailer = ltxstream.New(ltxstream.Config{WALPath: filepath.Join(t.TempDir(), "missing-app-wal")}, ltxstream.Position{})
	p.meta.tailer = ltxstream.New(ltxstream.Config{WALPath: filepath.Join(t.TempDir(), "missing-meta-wal")}, ltxstream.Position{})
	return f
}

func (f *heartbeatFixture) syncApp(t *testing.T) {
	t.Helper()
	if err := f.p.app.tailer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (f *heartbeatFixture) syncMeta(t *testing.T) {
	t.Helper()
	if err := f.p.meta.tailer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (f *heartbeatFixture) head(t *testing.T) *objstore.HEAD {
	t.Helper()
	head, _, err := objstore.LoadHEAD(context.Background(), f.raw)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func TestHeartbeatRequiresFreshCoupledSyncProof(t *testing.T) {
	t.Run("one stream missing", func(t *testing.T) {
		f := newHeartbeatFixture(t)
		f.syncApp(t)
		if err := f.p.heartbeat(context.Background()); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
		gets, puts := f.bucket.counts()
		if gets != 1 || puts != 0 {
			t.Fatalf("HEAD operations = get:%d put:%d, want 1/0", gets, puts)
		}
		if got := f.head(t).Publisher.ExpiresAtUS; got != f.expiry.UnixMicro() {
			t.Fatalf("expiry moved without meta proof: %d", got)
		}
	})

	t.Run("both streams renew once", func(t *testing.T) {
		f := newHeartbeatFixture(t)
		f.syncApp(t)
		f.syncMeta(t)
		if err := f.p.heartbeat(context.Background()); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
		wantExpiry := f.now.Add(f.p.cfg.LeaseExpiry).UnixMicro()
		if got := f.head(t).Publisher.ExpiresAtUS; got != wantExpiry {
			t.Fatalf("renewed expiry = %d, want %d", got, wantExpiry)
		}
		f.now = f.now.Add(time.Second)
		if err := f.p.heartbeat(context.Background()); err != nil {
			t.Fatalf("proof reuse heartbeat: %v", err)
		}
		gets, puts := f.bucket.counts()
		if gets != 2 || puts != 1 {
			t.Fatalf("HEAD operations after proof reuse = get:%d put:%d, want 2/1", gets, puts)
		}
		if got := f.head(t).Publisher.ExpiresAtUS; got != wantExpiry {
			t.Fatalf("proof reuse moved expiry: %d", got)
		}
	})

	t.Run("failed CAS retains proof", func(t *testing.T) {
		f := newHeartbeatFixture(t)
		f.syncApp(t)
		f.syncMeta(t)
		injected := errors.New("injected heartbeat write failure")
		f.bucket.mu.Lock()
		f.bucket.failHeadPut = injected
		f.bucket.mu.Unlock()
		if err := f.p.heartbeat(context.Background()); !errors.Is(err, injected) {
			t.Fatalf("heartbeat error = %v, want injected", err)
		}
		if f.p.appSyncsAtRenewal != 0 || f.p.metaSyncsAtRenewal != 0 {
			t.Fatal("failed CAS consumed Sync proof")
		}
		f.bucket.mu.Lock()
		f.bucket.failHeadPut = nil
		f.bucket.mu.Unlock()
		if err := f.p.heartbeat(context.Background()); err != nil {
			t.Fatalf("retry with retained proof: %v", err)
		}
		if f.p.appSyncsAtRenewal != 1 || f.p.metaSyncsAtRenewal != 1 {
			t.Fatalf("consumed counters = %d/%d, want 1/1", f.p.appSyncsAtRenewal, f.p.metaSyncsAtRenewal)
		}
	})
}

func TestHeartbeatOwnershipAndSafetyFences(t *testing.T) {
	t.Run("expired exact identity is lost", func(t *testing.T) {
		f := newHeartbeatFixture(t)
		head, etag, err := objstore.LoadHEAD(context.Background(), f.raw)
		if err != nil {
			t.Fatal(err)
		}
		next := *head
		next.Publisher = &objstore.Publisher{NodeID: "node-a", Generation: 1, ExpiresAtUS: f.now.UnixMicro()}
		if _, err := objstore.CASHead(context.Background(), f.raw, &next, &etag); err != nil {
			t.Fatal(err)
		}
		f.syncApp(t)
		f.syncMeta(t)
		if err := f.p.heartbeat(context.Background()); !errors.Is(err, errLeaseLost) {
			t.Fatalf("heartbeat error = %v, want lease lost", err)
		}
		_, puts := f.bucket.counts()
		if puts != 0 {
			t.Fatalf("expired identity attempted %d renewal writes", puts)
		}
	})

	t.Run("same owner contention retries", func(t *testing.T) {
		f := newHeartbeatFixture(t)
		f.syncApp(t)
		f.syncMeta(t)
		f.bucket.mu.Lock()
		f.bucket.beforePut = func() {
			head, etag, err := objstore.LoadHEAD(context.Background(), f.raw)
			if err != nil {
				t.Errorf("race LoadHEAD: %v", err)
				return
			}
			next := *head
			next.Baseline = &objstore.Baseline{TXID: 7}
			if _, err := objstore.CASHead(context.Background(), f.raw, &next, &etag); err != nil {
				t.Errorf("race CASHead: %v", err)
			}
		}
		f.bucket.mu.Unlock()
		if err := f.p.heartbeat(context.Background()); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
		_, puts := f.bucket.counts()
		if puts != 2 {
			t.Fatalf("HEAD puts = %d, want contended+retry", puts)
		}
		if got := f.head(t).Baseline; got == nil || got.TXID != 7 {
			t.Fatalf("same-generation baseline update lost: %+v", got)
		}
	})

	t.Run("safety deadline cancels generation", func(t *testing.T) {
		f := newHeartbeatFixture(t)
		f.syncApp(t)
		f.syncMeta(t)
		leadCtx, cancel := context.WithCancelCause(context.Background())
		f.p.mu.Lock()
		f.p.leadCtx = leadCtx
		f.p.leadCancel = cancel
		f.p.mu.Unlock()
		f.now = f.expiry.Add(-f.p.cfg.HeartbeatInterval)
		err := f.p.heartbeat(context.Background())
		if !errors.Is(err, errPublisherUnhealthy) || !errors.Is(context.Cause(leadCtx), errPublisherUnhealthy) {
			t.Fatalf("heartbeat/cause = %v / %v, want unhealthy", err, context.Cause(leadCtx))
		}
		_, puts := f.bucket.counts()
		if puts != 0 {
			t.Fatalf("safety-deadline heartbeat wrote HEAD %d times", puts)
		}
	})

	t.Run("watchdog uses unhealthy cause", func(t *testing.T) {
		f := newHeartbeatFixture(t)
		leadCtx, cancel := context.WithCancelCause(context.Background())
		f.p.mu.Lock()
		f.p.leadCtx = leadCtx
		f.p.leadCancel = cancel
		f.p.watchdogSeq = 7
		f.p.mu.Unlock()
		f.now = f.expiry.Add(-f.p.cfg.HeartbeatInterval)
		f.p.leaseWatchdogFired(7, 1)
		if cause := context.Cause(leadCtx); !errors.Is(cause, errPublisherUnhealthy) {
			t.Fatalf("watchdog cause = %v, want unhealthy", cause)
		}
	})

	t.Run("hung HEAD read is bounded by safety deadline", func(t *testing.T) {
		f := newHeartbeatFixture(t)
		f.syncApp(t)
		f.syncMeta(t)
		f.now = f.expiry.Add(-f.p.cfg.HeartbeatInterval - 2*time.Millisecond)
		f.bucket.mu.Lock()
		f.bucket.blockGet = true
		f.bucket.mu.Unlock()
		started := time.Now()
		err := f.p.heartbeat(context.Background())
		if !errors.Is(err, errPublisherUnhealthy) {
			t.Fatalf("heartbeat error = %v, want unhealthy", err)
		}
		if elapsed := time.Since(started); elapsed > 20*time.Millisecond {
			t.Fatalf("hung heartbeat took %s", elapsed)
		}
	})
}

func TestMutationGuardRechecksClockImmediatelyBeforePut(t *testing.T) {
	f := newHeartbeatFixture(t)
	safe := f.expiry.Add(-f.p.cfg.HeartbeatInterval - time.Second)
	closed := f.expiry.Add(-f.p.cfg.HeartbeatInterval)
	var calls int
	f.p.now = func() time.Time {
		calls++
		if calls == 1 {
			return safe
		}
		return closed
	}
	err := f.p.publishL0(context.Background(), &f.p.app, ltx.Header{MinTXID: 1, MaxTXID: 1}, []byte("ltx"))
	if !errors.Is(err, errPublisherUnhealthy) {
		t.Fatalf("publishL0 error = %v, want unhealthy fence", err)
	}
	files, listErr := objstore.ListLTX(context.Background(), f.raw, objstore.DBPrefix, objstore.L0Level)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(files) != 0 {
		t.Fatalf("post-pause mutation uploaded %d objects", len(files))
	}
}
