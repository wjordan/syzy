package publisher

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
)

// rebTinyLTX encodes a valid 1-page NoChecksum baseline LTX at txid.
func rebTinyLTX(t *testing.T, txid uint64) []byte {
	t.Helper()
	const pageSize = 4096
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    1,
		MinTXID:   ltx.TXID(txid),
		MaxTXID:   ltx.TXID(txid),
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("EncodeHeader: %v", err)
	}
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, bytes.Repeat([]byte{byte(txid)}, pageSize)); err != nil {
		t.Fatalf("EncodePage: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// newRebaselinePub builds a Publisher whose Baseline/MetaBaseline funcs
// emit tiny valid baseline LTXes — enough to exercise takeCoupledBaselines
// without a real Node/snapshotter.
func newRebaselinePub(t *testing.T, be objectstore.Bucket, cfg Config) *Publisher {
	t.Helper()
	cfg.Backend = be
	cfg.ClusterID = "cafe"
	cfg.NodeID = "node-a"
	cfg.WALPath = "/x/app.db-wal"
	cfg.MetaWALPath = "/x/meta.db-wal"
	cfg.Baseline = func(_ context.Context, txid uint64) ([]byte, []byte, func(), error) {
		return rebTinyLTX(t, txid), rebTinyLTX(t, txid), func() {}, nil
	}
	cfg.MetaBaseline = func(_ context.Context, txid uint64) ([]byte, func(), error) {
		return rebTinyLTX(t, txid), func() {}, nil
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: cfg.ClusterID,
		Publisher: &objstore.Publisher{NodeID: cfg.NodeID, Generation: 1, ExpiresAtUS: time.Now().Add(time.Hour).UnixMicro()},
	}, objectstore.IfAbsent())
	if err != nil {
		t.Fatalf("seed publisher HEAD: %v", err)
	}
	p.generation = 1
	p.leaseExpiresAt = time.Now().Add(time.Hour)
	return p
}

func dbChainAbove(t *testing.T, ctx context.Context, be objectstore.Bucket, base uint64) int {
	t.Helper()
	n := 0
	for _, lvl := range []int{objstore.L0Level, objstore.L1Level} {
		fs, err := objstore.ListLTX(ctx, be, objstore.DBPrefix, lvl)
		if err != nil {
			t.Fatalf("ListLTX L%d: %v", lvl, err)
		}
		for _, f := range fs {
			if f.MaxTXID > base {
				n++
			}
		}
	}
	return n
}

// TestMaybeRebaseline_ChainBytes: once the db delta chain above the
// baseline reaches the baseline's own size, a fresh coupled baseline is
// taken and the chain above the new baseline is empty.
func TestMaybeRebaseline_ChainBytes(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Isolate the bytes trigger: object/skew failsafes effectively off.
	p := newRebaselinePub(t, be, Config{
		RebaselineChainBytesRatio: 1.0,
		RebaselineMaxChainObjects: 1 << 30,
		RebaselineMaxBaselineSkew: 1 << 60,
	})
	if err := p.seedTXIDCounters(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.takeCoupledBaselines(ctx); err != nil {
		t.Fatalf("init baseline: %v", err)
	}
	head, _, _ := objstore.LoadHEAD(ctx, be)
	base1 := head.Baseline.TXID

	// Publish L0 deltas above the baseline — each ~baseline-sized, so the
	// chain quickly exceeds the 1.0 bytes ratio.
	for i := base1 + 1; i <= base1+3; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, rebTinyLTX(t, i)); err != nil {
			t.Fatalf("publish L0 %d: %v", i, err)
		}
	}
	// In prod the live tailer keeps the counters above the chain tip; here
	// the deltas were published directly, so reseed before the rebaseline.
	if err := p.seedTXIDCounters(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.maybeRebaseline(ctx); err != nil {
		t.Fatalf("maybeRebaseline: %v", err)
	}
	head2, _, _ := objstore.LoadHEAD(ctx, be)
	if head2.Baseline.TXID <= base1 {
		t.Fatalf("db baseline did not advance: %d -> %d", base1, head2.Baseline.TXID)
	}
	if head2.Baseline.TXID != head2.MetaBaseline.TXID {
		t.Fatalf("rebaseline not coupled: db=%d meta=%d", head2.Baseline.TXID, head2.MetaBaseline.TXID)
	}
	if got := dbChainAbove(t, ctx, be, head2.Baseline.TXID); got != 0 {
		t.Fatalf("chain above new baseline = %d, want 0", got)
	}
}

// TestMaybeRebaseline_NoTriggerWhenSmall: an empty/below-threshold chain
// does not rebaseline.
func TestMaybeRebaseline_NoTriggerWhenSmall(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p := newRebaselinePub(t, be, Config{})
	if err := p.seedTXIDCounters(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.takeCoupledBaselines(ctx); err != nil {
		t.Fatalf("init baseline: %v", err)
	}
	head, _, _ := objstore.LoadHEAD(ctx, be)
	base1 := head.Baseline.TXID

	if err := p.maybeRebaseline(ctx); err != nil {
		t.Fatalf("maybeRebaseline: %v", err)
	}
	head2, _, _ := objstore.LoadHEAD(ctx, be)
	if head2.Baseline.TXID != base1 {
		t.Fatalf("rebaseline fired on empty chain: %d -> %d", base1, head2.Baseline.TXID)
	}
}

// TestMaybeRebaseline_BaselineSkew: when the meta baseline races ahead of
// the db baseline (the resume-path asymmetry) past the skew failsafe, a
// coupled rebaseline re-couples the two streams even with no db chain.
func TestMaybeRebaseline_BaselineSkew(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Disable bytes/object triggers; only skew can fire.
	p := newRebaselinePub(t, be, Config{
		RebaselineChainBytesRatio: 1e9,
		RebaselineMaxChainObjects: 1 << 30,
		RebaselineMaxBaselineSkew: 100,
	})
	if err := p.seedTXIDCounters(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.takeCoupledBaselines(ctx); err != nil {
		t.Fatalf("init baseline: %v", err)
	}
	head, _, _ := objstore.LoadHEAD(ctx, be)
	base1 := head.Baseline.TXID

	// Simulate takeMetaBaselineOnly: advance only the meta baseline.
	metaTXID := base1 + 500
	ref, err := objstore.PublishLTX(ctx, be, objstore.MetadataPrefix, objstore.BaselineLevel, metaTXID, metaTXID, rebTinyLTX(t, metaTXID))
	if err != nil {
		t.Fatalf("publish meta baseline: %v", err)
	}
	cur, etag, _ := objstore.LoadHEAD(ctx, be)
	cur.MetaBaseline = &objstore.Baseline{TXID: metaTXID, LTXRef: ref}
	if _, err := objstore.CASHead(ctx, be, cur, &etag); err != nil {
		t.Fatalf("CAS skewed HEAD: %v", err)
	}
	if err := p.seedTXIDCounters(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.maybeRebaseline(ctx); err != nil {
		t.Fatalf("maybeRebaseline: %v", err)
	}
	head2, _, _ := objstore.LoadHEAD(ctx, be)
	if head2.Baseline.TXID <= base1 {
		t.Fatalf("skew did not trigger rebaseline: db baseline still %d", head2.Baseline.TXID)
	}
	if head2.Baseline.TXID != head2.MetaBaseline.TXID {
		t.Fatalf("streams not re-coupled: db=%d meta=%d", head2.Baseline.TXID, head2.MetaBaseline.TXID)
	}
}
