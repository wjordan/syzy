package objstore_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
)

// TestHEADRoundTrip exercises the coupled-baseline HEAD JSON shape.
func TestHEADRoundTrip(t *testing.T) {
	t.Parallel()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("FileBackend: %v", err)
	}
	ctx := context.Background()

	head := &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: "cafef00d",
		Baseline: &objstore.Baseline{
			TXID:      42,
			LTXRef:    objstore.FileRef{Key: "db/0009/000...000.ltx", Size: 4096, Sha256: "abc"},
			BuiltAtUS: 1700000000_000000,
		},
	}
	if _, err := objstore.CASHead(ctx, be, head, objectstore.IfAbsent()); err != nil {
		t.Fatalf("CASHead: %v", err)
	}
	got, _, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if got.ClusterID != head.ClusterID {
		t.Errorf("ClusterID round-trip: got %q want %q", got.ClusterID, head.ClusterID)
	}
	if got.Baseline == nil || got.Baseline.TXID != 42 {
		t.Errorf("Baseline TXID round-trip: got %+v", got.Baseline)
	}
	if got.Version != objstore.HEADVersion {
		t.Errorf("Version: got %d want %d", got.Version, objstore.HEADVersion)
	}
}

// TestLayoutKeys verifies the path conventions for both LTX streams.
// db/ matches Litestream's key grammar; metadata/<level>/ mirrors it
// for syzy's metadata.db stream.
func TestLayoutKeys(t *testing.T) {
	if got := objstore.LTXKey(objstore.DBPrefix, 0, 1, 1); got != "db/0000/0000000000000001-0000000000000001.ltx" {
		t.Errorf("LTXKey(db, L0): %s", got)
	}
	if got := objstore.LTXKey(objstore.DBPrefix, objstore.BaselineLevel, 7, 7); got != "db/0009/0000000000000007-0000000000000007.ltx" {
		t.Errorf("LTXKey(db, baseline): %s", got)
	}
	if got := objstore.LTXLevelPrefix(objstore.DBPrefix, 1); got != "db/0001/" {
		t.Errorf("LTXLevelPrefix(db): %s", got)
	}
	if got := objstore.LTXKey(objstore.MetadataPrefix, 0, 1, 1); got != "metadata/0000/0000000000000001-0000000000000001.ltx" {
		t.Errorf("LTXKey(meta, L0): %s", got)
	}
}

// TestPublishLTX_PutAndIdempotent verifies PublishLTX is idempotent
// against the same key — duplicate PUT returns nil (immutable
// content addressed).
func TestPublishLTX_PutAndIdempotent(t *testing.T) {
	t.Parallel()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("FileBackend: %v", err)
	}
	ctx := context.Background()
	body := []byte("not really LTX but we don't decode here")
	ref1, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, 5, 5, body)
	if err != nil {
		t.Fatalf("PublishLTX: %v", err)
	}
	if !strings.HasPrefix(ref1.Key, "db/0000/") {
		t.Errorf("ref Key: %s", ref1.Key)
	}
	// Re-PUT same key with same content: success.
	ref2, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, 5, 5, body)
	if err != nil {
		t.Fatalf("PublishLTX (duplicate): %v", err)
	}
	if ref1.Sha256 != ref2.Sha256 {
		t.Errorf("sha256 mismatch on idempotent re-PUT")
	}
}

// TestMaintenanceCoupledBaselines_CASGuards verifies cluster-id mismatch is
// caught and that an existing equivalent baseline isn't overwritten.
func TestMaintenanceCoupledBaselines_CASGuards(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("FileBackend: %v", err)
	}
	ctx := context.Background()
	clusterID := "deadbeef"

	app1 := []byte("app-baseline-1")
	meta1 := []byte("meta-baseline-1")
	app2 := []byte("app-baseline-2")
	meta2 := []byte("meta-baseline-2")
	publish := func(clusterID string, txid uint64, app, meta []byte) (retErr error) {
		reservation, err := objstore.AcquireMaintenanceReservation(ctx, be, clusterID, time.Minute)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, reservation.Release(context.Background())) }()
		return reservation.PublishCoupledBaselines(ctx, txid, app, meta)
	}

	// First publish: succeeds.
	if err := publish(clusterID, 1, app1, meta1); err != nil {
		t.Fatalf("first maintenance publish: %v", err)
	}

	// Same cluster id, lower TXID: idempotent (HEAD already covers).
	if err := publish(clusterID, 1, app1, meta1); err != nil {
		t.Fatalf("idempotent re-publish: %v", err)
	}

	// Different cluster id targeting same bucket: refused.
	err = publish("feedface", 2, app2, meta2)
	if !errors.Is(err, objstore.ErrClusterIDMismatch) {
		t.Errorf("expected ErrClusterIDMismatch, got %v", err)
	}

	// Same cluster, higher TXID: replaces both baselines.
	if err := publish(clusterID, 2, app2, meta2); err != nil {
		t.Fatalf("higher-TXID publish: %v", err)
	}
	head, _, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.Baseline.TXID != 2 {
		t.Errorf("Baseline TXID: got %d want 2", head.Baseline.TXID)
	}
	if head.MetaBaseline == nil || head.MetaBaseline.TXID != 2 {
		t.Errorf("MetaBaseline: got %+v want TXID=2", head.MetaBaseline)
	}
}

// TestResolveClusterID covers the rendezvous primitive: a fresh
// bucket gets a stub HEAD with a fresh cluster_id; subsequent
// callers read that id back.
func TestResolveClusterID(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("FileBackend: %v", err)
	}
	ctx := context.Background()
	cid1, err := objstore.ResolveClusterID(ctx, be)
	if err != nil {
		t.Fatalf("ResolveClusterID: %v", err)
	}
	cid2, err := objstore.ResolveClusterID(ctx, be)
	if err != nil {
		t.Fatalf("ResolveClusterID(second): %v", err)
	}
	if cid1 != cid2 {
		t.Errorf("rendezvous failed: %s vs %s", cid1, cid2)
	}
	if len(cid1) != 32 {
		t.Errorf("expected 32-char hex, got %q", cid1)
	}
}
