package publisher_test

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/publisher"
)

// TestMetaBaselineTXIDOvertakesMetaCounter is the red-green test for
// the metadata corruption observed in the Fly demo.
//
// Bug shape: takeMetaBaselineOnly historically allocated its TXID
// from the app stream counter (lastBucketTXID) only. When meta writes
// outpaced app writes, metaTXIDCounter would already be ahead of the
// next app TXID — the baseline landed at a TXID *below* recently
// shipped meta L0 records. Receivers' chain logic
// (`MaxTXID <= baseline.TXID` → skip) then *included* those older L0
// records as if they were past-baseline deltas, replayed them on top
// of the newer baseline, time-traveled pages backwards, and corrupted
// multi-page overflow chains.
//
// Fix: allocBaselineTXID picks max(lastBucketTXID, metaTXIDCounter)+1.
//
// The test pre-publishes a meta L0 record whose TXID exceeds the
// next app TXID, then drives the resume-baseline path. The resulting
// HEAD.MetaBaseline.TXID must exceed the meta L0 record's TXID so
// chain logic excludes the pre-baseline record on cold restore.
func TestMetaBaselineTXIDOvertakesMetaCounter(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	clusterID := "deadc0de"
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: clusterID,
		Publisher: &objstore.Publisher{
			NodeID:      "nodeA",
			Generation:  1,
			ExpiresAtUS: time.Now().UnixMicro() + int64(time.Minute/time.Microsecond),
		},
		Baseline: &objstore.Baseline{
			TXID:      1,
			LTXRef:    objstore.FileRef{Key: "db/0009/x.ltx"},
			BuiltAtUS: time.Now().UnixMicro(),
		},
		MetaBaseline: &objstore.Baseline{
			TXID:      1,
			LTXRef:    objstore.FileRef{Key: "metadata/0009/x.ltx"},
			BuiltAtUS: time.Now().UnixMicro(),
		},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}

	// Pre-publish a meta L0 record at a TXID HIGHER than the
	// app-stream tip (which is 1, from the seeded baseline). This
	// mimics production: meta writes outpaced app writes between
	// publisher restarts, so the meta stream's max TXID is ahead.
	const metaAheadTXID = 200
	dummyLTX := []byte("dummy-meta-l0-ltx")
	if _, err := objstore.PublishLTX(context.Background(), be,
		objstore.MetadataPrefix, objstore.L0Level, metaAheadTXID, metaAheadTXID, dummyLTX); err != nil {
		t.Fatalf("publish meta L0 at txid %d: %v", metaAheadTXID, err)
	}

	// Capture the TXID the publisher assigns to the resume baseline.
	var observedBaselineTXID atomic.Uint64
	walPath := filepath.Join(t.TempDir(), "no-wal")
	p, err := publisher.New(publisher.Config{
		Backend:     be,
		ClusterID:   clusterID,
		NodeID:      "nodeA", // same as seeded → resume path
		WALPath:     walPath,
		MetaWALPath: walPath,
		// Resume takes a coupled baseline; observe its TXID there.
		Baseline: func(_ context.Context, txid uint64) ([]byte, []byte, func(), error) {
			observedBaselineTXID.Store(txid)
			return []byte("app-baseline"), []byte("meta-baseline"), func() {}, nil
		},
		MetaBaseline: func(_ context.Context, txid uint64) ([]byte, func(), error) {
			return []byte("meta-baseline"), func() {}, nil
		},
		HeartbeatInterval: 10 * time.Millisecond,
		LeaseExpiry:       100 * time.Millisecond,
		LTXSyncInterval:   5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	got := observedBaselineTXID.Load()
	if got <= metaAheadTXID {
		t.Fatalf("resume baseline TXID = %d; want > %d (max meta L0 TXID).\n"+
			"this means receivers' chain logic will INCLUDE the pre-baseline "+
			"meta L0 records and replay them on top of the newer baseline, "+
			"corrupting overflow chains.",
			got, metaAheadTXID)
	}

	// Also assert HEAD.MetaBaseline.TXID matches what we observed,
	// so the bucket reflects the correct anchoring.
	head, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.MetaBaseline == nil || head.MetaBaseline.TXID != got {
		t.Errorf("HEAD.MetaBaseline = %+v; want TXID=%d", head.MetaBaseline, got)
	}
}
