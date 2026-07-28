package publisher_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/publisher"
)

// A lease claim that loses to a concurrent claimant during the settle
// window (multi-region LWW CAS resolution) must re-wait instead of
// publishing. Production incident: three regions all "won" the same
// generation and interleaved baseline/L0 uploads at colliding keys,
// leaving an unrestorable chain.
func TestClaimSettleDetectsLostClaim(t *testing.T) {
	t.Parallel()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	clusterID := "cafef00d"
	// Expired foreign holder → takeover path.
	if _, err := objstore.CASHead(ctx, be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: clusterID,
		Publisher: &objstore.Publisher{NodeID: "dead-node", Generation: 5, ExpiresAtUS: 1},
	}, nil); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}

	baseline, baselineCalls := stubBaseline(t)
	dir := t.TempDir()
	p, err := publisher.New(publisher.Config{
		Backend:      be,
		ClusterID:    clusterID,
		NodeID:       "victim",
		WALPath:      filepath.Join(dir, "app.db-wal"),
		MetaWALPath:  filepath.Join(dir, "metadata.db-wal"),
		Baseline:     baseline,
		MetaBaseline: stubMetaBaselineFn(t),
		ClaimSettle:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(runCtx) }()

	// Simulate the LWW winner landing during the settle window: as soon
	// as the victim's claim is visible, overwrite it with the winner's.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		head, _, err := objstore.LoadHEAD(ctx, be)
		if err == nil && head.Publisher != nil && head.Publisher.NodeID == "victim" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := objstore.CASHead(ctx, be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: clusterID,
		Publisher: &objstore.Publisher{NodeID: "winner", Generation: 6, ExpiresAtUS: time.Now().Add(time.Hour).UnixMicro()},
	}, nil); err != nil {
		t.Fatalf("overwrite as winner: %v", err)
	}

	// Give the victim's settle re-read time to fire and (incorrectly)
	// publish if the guard were missing (4x the settle window).
	time.Sleep(400 * time.Millisecond)
	if *baselineCalls != 0 {
		t.Fatalf("victim published a baseline after losing the claim (calls=%d)", *baselineCalls)
	}
	cancel()
	<-done
}
