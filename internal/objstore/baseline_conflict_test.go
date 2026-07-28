package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
)

// A maintenance reservation must refuse to treat an equal-TXID baseline
// pointer with foreign bytes as coverage: under a concurrent
// double-claim another node can publish to the same txid keys, and
// adopting its pointers stitches two nodes' snapshots together.
func TestMaintenanceReservationRejectsForeignSameTXID(t *testing.T) {
	t.Parallel()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	const txid = 42
	if _, err := CASHead(ctx, be, &HEAD{
		Version:   HEADVersion,
		ClusterID: "cafef00d",
		Baseline: &Baseline{
			TXID:      txid,
			LTXRef:    FileRef{Key: LTXKey(DBPrefix, BaselineLevel, txid, txid), Sha256: strings.Repeat("ab", 32)},
			BuiltAtUS: time.Now().UnixMicro(),
		},
		MetaBaseline: &Baseline{
			TXID:      txid,
			LTXRef:    FileRef{Key: LTXKey(MetadataPrefix, BaselineLevel, txid, txid), Sha256: strings.Repeat("cd", 32)},
			BuiltAtUS: time.Now().UnixMicro(),
		},
	}, nil); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}

	reservation := acquireTestMaintenance(t, ctx, be, "cafef00d")
	defer reservation.Release(context.Background())
	err = reservation.PublishCoupledBaselines(ctx, txid, []byte("our-app-bytes"), []byte("our-meta-bytes"))
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("want baseline-conflict error, got %v", err)
	}
}

func TestPublishLTXRejectsForeignKeyCollision(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		proposed []byte
	}{
		{name: "same size", proposed: []byte("xxxxxx")},
		{name: "different size", proposed: []byte("different")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			be, err := objectstore.OpenFS(t.TempDir())
			if err != nil {
				t.Fatalf("OpenFS: %v", err)
			}
			ctx := context.Background()
			stored := []byte("stored")
			ref, err := PublishLTX(ctx, be, DBPrefix, L0Level, 7, 7, stored)
			if err != nil {
				t.Fatalf("seed LTX: %v", err)
			}

			if _, err := PublishLTX(ctx, be, DBPrefix, L0Level, 7, 7, tt.proposed); !errors.Is(err, ErrLTXConflict) {
				t.Fatalf("want ErrLTXConflict, got %v", err)
			}
			if got := mustReadObject(t, ctx, be, ref.Key); !bytes.Equal(got, stored) {
				t.Fatalf("stored object changed: got %q want %q", got, stored)
			}
		})
	}
}

func TestPublishCoupledBaselinesRejectsMixedCoverageRegression(t *testing.T) {
	t.Parallel()
	for _, coverage := range []struct {
		name         string
		appTXID      uint64
		metadataTXID uint64
	}{
		{name: "app ahead", appTXID: 10, metadataTXID: 5},
		{name: "metadata ahead", appTXID: 5, metadataTXID: 10},
	} {
		for _, owned := range []bool{false, true} {
			name := coverage.name + "/maintenance"
			if owned {
				name = coverage.name + "/owned"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				be, err := objectstore.OpenFS(t.TempDir())
				if err != nil {
					t.Fatalf("OpenFS: %v", err)
				}
				ctx := context.Background()
				identity := PublisherIdentity{NodeID: "node-a", Generation: 9}
				expiresAtUS := int64(0)
				if owned {
					expiresAtUS = 9_000_000_000_000_000
				}
				before := seedBaselineHEAD(t, ctx, be, identity, expiresAtUS, coverage.appTXID, coverage.metadataTXID)

				if owned {
					err = PublishCoupledBaselinesOwned(ctx, be, "cafef00d", identity, 7, []byte("candidate-app"), []byte("candidate-meta"))
				} else {
					reservation := acquireTestMaintenance(t, ctx, be, "cafef00d")
					err = reservation.PublishCoupledBaselines(ctx, 7, []byte("candidate-app"), []byte("candidate-meta"))
					if releaseErr := reservation.Release(context.Background()); releaseErr != nil {
						t.Fatalf("release maintenance reservation: %v", releaseErr)
					}
				}
				if !errors.Is(err, ErrBaselineRegression) {
					t.Fatalf("want ErrBaselineRegression, got %v", err)
				}
				if owned {
					if after := mustReadObject(t, ctx, be, HEADKey); !bytes.Equal(after, before) {
						t.Fatalf("HEAD changed on rejected mixed-coverage promotion:\nbefore=%s\nafter=%s", before, after)
					}
				} else {
					head, _, loadErr := LoadHEAD(ctx, be)
					if loadErr != nil {
						t.Fatal(loadErr)
					}
					if head.Baseline.TXID != coverage.appTXID || head.MetaBaseline.TXID != coverage.metadataTXID {
						t.Fatalf("maintenance conflict moved baseline pointers: %+v", head)
					}
				}
			})
		}
	}
}

func TestAcquireMaintenanceReservationRejectsActiveLeaseBeforeUploads(t *testing.T) {
	t.Parallel()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	identity := PublisherIdentity{NodeID: "node-a", Generation: 9}
	before := seedBaselineHEAD(t, ctx, be, identity, time.Now().Add(time.Hour).UnixMicro(), 6, 6)

	_, err = AcquireMaintenanceReservation(ctx, be, "cafef00d", time.Minute)
	if !errors.Is(err, ErrPublisherLeaseActive) {
		t.Fatalf("want ErrPublisherLeaseActive, got %v", err)
	}
	if after := mustReadObject(t, ctx, be, HEADKey); !bytes.Equal(after, before) {
		t.Fatalf("active publisher HEAD changed:\nbefore=%s\nafter=%s", before, after)
	}
	for _, prefix := range []string{DBPrefix, MetadataPrefix} {
		files, listErr := ListLTX(ctx, be, prefix, BaselineLevel)
		if listErr != nil {
			t.Fatalf("ListLTX %s: %v", prefix, listErr)
		}
		if len(files) != 0 {
			t.Fatalf("active publisher rejection uploaded %d %s baselines", len(files), prefix)
		}
	}
}

func TestMaintenanceReservationLosesAcquisitionRaceBeforeUploads(t *testing.T) {
	t.Parallel()
	base, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	identity := PublisherIdentity{NodeID: "old-node", Generation: 4}
	seedBaselineHEAD(t, ctx, base, identity, 0, 6, 6)
	bucket := &failFirstHEADCASBucket{Bucket: base}
	bucket.beforeFail = func() {
		head, etag, loadErr := LoadHEAD(ctx, base)
		if loadErr != nil {
			t.Fatalf("claim LoadHEAD: %v", loadErr)
		}
		next := *head
		next.Publisher = &Publisher{NodeID: "new-node", Generation: 5, ExpiresAtUS: time.Now().Add(time.Hour).UnixMicro()}
		if _, casErr := CASHead(ctx, base, &next, &etag); casErr != nil {
			t.Fatalf("claim CASHead: %v", casErr)
		}
	}

	_, err = acquireMaintenanceReservation(ctx, bucket, "cafef00d", "maintenance:test", time.Minute, time.Now)
	if !errors.Is(err, ErrPublisherLeaseActive) {
		t.Fatalf("want ErrPublisherLeaseActive, got %v", err)
	}
	head, _, err := LoadHEAD(ctx, base)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.Publisher == nil || head.Publisher.NodeID != "new-node" {
		t.Fatalf("publisher claim lost: %+v", head.Publisher)
	}
	if head.Baseline == nil || head.Baseline.TXID != 6 || head.MetaBaseline == nil || head.MetaBaseline.TXID != 6 {
		t.Fatalf("offline publication moved baseline pointers: app=%+v metadata=%+v", head.Baseline, head.MetaBaseline)
	}
	for _, prefix := range []string{DBPrefix, MetadataPrefix} {
		files, listErr := ListLTX(ctx, base, prefix, BaselineLevel)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(files) != 0 {
			t.Fatalf("lost reservation race uploaded %d %s baselines", len(files), prefix)
		}
	}
}

func TestPublishCoupledBaselinesOwnedRejectsExpiredIdentity(t *testing.T) {
	t.Parallel()
	const nowUS = int64(1_700_000_000_000_000)
	for _, tt := range []struct {
		name         string
		baselineTXID uint64
	}{
		{name: "candidate not covered", baselineTXID: 6},
		{name: "candidate already covered", baselineTXID: 8},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			be, err := objectstore.OpenFS(t.TempDir())
			if err != nil {
				t.Fatalf("OpenFS: %v", err)
			}
			ctx := context.Background()
			identity := PublisherIdentity{NodeID: "node-a", Generation: 9}
			before := seedBaselineHEAD(t, ctx, be, identity, nowUS, tt.baselineTXID, tt.baselineTXID)

			clock := func() time.Time { return time.UnixMicro(nowUS) }
			err = publishCoupledBaselines(ctx, be, "cafef00d", identity, clock, 7, []byte("candidate-app"), []byte("candidate-meta"))
			if !errors.Is(err, ErrPublisherOwnershipLost) {
				t.Fatalf("want ErrPublisherOwnershipLost, got %v", err)
			}
			if after := mustReadObject(t, ctx, be, HEADKey); !bytes.Equal(after, before) {
				t.Fatalf("HEAD changed for expired publisher:\nbefore=%s\nafter=%s", before, after)
			}
		})
	}
}

func TestPublishCoupledBaselinesOwnedRechecksExpiryAfterCASConflict(t *testing.T) {
	t.Parallel()
	const nowUS = int64(1_700_000_000_000_000)
	base, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	identity := PublisherIdentity{NodeID: "node-a", Generation: 9}
	before := seedBaselineHEAD(t, ctx, base, identity, nowUS+1, 6, 6)
	bucket := &failFirstHEADCASBucket{Bucket: base}
	clockCalls := 0
	clock := func() time.Time {
		clockCalls++
		return time.UnixMicro(nowUS + int64(clockCalls-1))
	}

	err = publishCoupledBaselines(ctx, bucket, "cafef00d", identity, clock, 7, []byte("candidate-app"), []byte("candidate-meta"))
	if !errors.Is(err, ErrPublisherOwnershipLost) {
		t.Fatalf("want ErrPublisherOwnershipLost, got %v", err)
	}
	if clockCalls != 2 {
		t.Fatalf("ownership clock calls: got %d want 2", clockCalls)
	}
	if after := mustReadObject(t, ctx, base, HEADKey); !bytes.Equal(after, before) {
		t.Fatalf("HEAD changed after lease expired during CAS retry:\nbefore=%s\nafter=%s", before, after)
	}
}

func TestPublishMetadataBaselineOwnedRetriesUnderExactLease(t *testing.T) {
	t.Parallel()
	const nowUS = int64(1_700_000_000_000_000)
	base, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	identity := PublisherIdentity{NodeID: "node-a", Generation: 9}
	seedBaselineHEAD(t, ctx, base, identity, nowUS+1_000_000, 6, 6)
	bucket := &failFirstHEADCASBucket{Bucket: base}

	err = publishMetadataBaselineOwned(
		ctx, bucket, "cafef00d", identity, func() time.Time { return time.UnixMicro(nowUS) }, 7, []byte("metadata-7"),
	)
	if err != nil {
		t.Fatalf("publish metadata baseline: %v", err)
	}
	head, _, err := LoadHEAD(ctx, base)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.Baseline == nil || head.Baseline.TXID != 6 {
		t.Fatalf("app baseline moved: %+v", head.Baseline)
	}
	if head.MetaBaseline == nil || head.MetaBaseline.TXID != 7 {
		t.Fatalf("metadata baseline not promoted: %+v", head.MetaBaseline)
	}
}

func TestPublishMetadataBaselineOwnedRejectsExpiredLease(t *testing.T) {
	t.Parallel()
	const nowUS = int64(1_700_000_000_000_000)
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	identity := PublisherIdentity{NodeID: "node-a", Generation: 9}
	before := seedBaselineHEAD(t, ctx, be, identity, nowUS, 6, 6)

	err = publishMetadataBaselineOwned(
		ctx, be, "cafef00d", identity, func() time.Time { return time.UnixMicro(nowUS) }, 7, []byte("metadata-7"),
	)
	if !errors.Is(err, ErrPublisherOwnershipLost) {
		t.Fatalf("want ErrPublisherOwnershipLost, got %v", err)
	}
	if after := mustReadObject(t, ctx, be, HEADKey); !bytes.Equal(after, before) {
		t.Fatalf("expired publisher changed HEAD:\nbefore=%s\nafter=%s", before, after)
	}
}

func TestPublishMetadataBaselineOwnedRechecksExpiryAfterCASConflict(t *testing.T) {
	t.Parallel()
	const nowUS = int64(1_700_000_000_000_000)
	base, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	identity := PublisherIdentity{NodeID: "node-a", Generation: 9}
	before := seedBaselineHEAD(t, ctx, base, identity, nowUS+1, 6, 6)
	bucket := &failFirstHEADCASBucket{Bucket: base}
	clockCalls := 0
	clock := func() time.Time {
		clockCalls++
		return time.UnixMicro(nowUS + int64(clockCalls-1))
	}

	err = publishMetadataBaselineOwned(ctx, bucket, "cafef00d", identity, clock, 7, []byte("metadata-7"))
	if !errors.Is(err, ErrPublisherOwnershipLost) {
		t.Fatalf("want ErrPublisherOwnershipLost, got %v", err)
	}
	if clockCalls != 2 {
		t.Fatalf("ownership clock calls: got %d want 2", clockCalls)
	}
	if after := mustReadObject(t, ctx, base, HEADKey); !bytes.Equal(after, before) {
		t.Fatalf("HEAD changed after metadata lease expired during CAS retry:\nbefore=%s\nafter=%s", before, after)
	}
}

func TestPublishMetadataBaselineOwnedRejectsForeignHEADDigest(t *testing.T) {
	t.Parallel()
	const nowUS = int64(1_700_000_000_000_000)
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	identity := PublisherIdentity{NodeID: "node-a", Generation: 9}
	before := seedBaselineHEAD(t, ctx, be, identity, nowUS+1_000_000, 6, 7)

	err = publishMetadataBaselineOwned(
		ctx, be, "cafef00d", identity, func() time.Time { return time.UnixMicro(nowUS) }, 7, []byte("our-metadata-7"),
	)
	if !errors.Is(err, ErrLTXConflict) {
		t.Fatalf("want ErrLTXConflict, got %v", err)
	}
	if after := mustReadObject(t, ctx, be, HEADKey); !bytes.Equal(after, before) {
		t.Fatalf("foreign metadata pointer changed:\nbefore=%s\nafter=%s", before, after)
	}
}

type failFirstHEADCASBucket struct {
	objectstore.Bucket
	failed     bool
	beforeFail func()
}

func acquireTestMaintenance(t testing.TB, ctx context.Context, b objectstore.Bucket, clusterID string) *MaintenanceReservation {
	t.Helper()
	r, err := acquireMaintenanceReservation(ctx, b, clusterID, "maintenance:test", time.Minute, time.Now)
	if err != nil {
		t.Fatalf("acquire maintenance reservation: %v", err)
	}
	return r
}

func (b *failFirstHEADCASBucket) Put(ctx context.Context, key string, body io.Reader, length int64, ifMatch *string) (string, error) {
	if key == HEADKey && ifMatch != nil && *ifMatch != "" && !b.failed {
		b.failed = true
		if b.beforeFail != nil {
			b.beforeFail()
		}
		if _, err := io.Copy(io.Discard, body); err != nil {
			return "", err
		}
		return "", objectstore.ErrPreconditionFailed
	}
	return b.Bucket.Put(ctx, key, body, length, ifMatch)
}

func seedBaselineHEAD(t testing.TB, ctx context.Context, b objectstore.Bucket, identity PublisherIdentity, expiresAtUS int64, appTXID, metadataTXID uint64) []byte {
	t.Helper()
	if _, err := CASHead(ctx, b, &HEAD{
		Version:   HEADVersion,
		ClusterID: "cafef00d",
		Publisher: &Publisher{NodeID: identity.NodeID, Generation: identity.Generation, ExpiresAtUS: expiresAtUS},
		Baseline: &Baseline{
			TXID:   appTXID,
			LTXRef: FileRef{Key: LTXKey(DBPrefix, BaselineLevel, appTXID, appTXID), Sha256: strings.Repeat("ab", 32)},
		},
		MetaBaseline: &Baseline{
			TXID:   metadataTXID,
			LTXRef: FileRef{Key: LTXKey(MetadataPrefix, BaselineLevel, metadataTXID, metadataTXID), Sha256: strings.Repeat("cd", 32)},
		},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	return mustReadObject(t, ctx, b, HEADKey)
}

func mustReadObject(t testing.TB, ctx context.Context, b objectstore.Bucket, key string) []byte {
	t.Helper()
	rc, _, err := b.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get %s: %v", key, err)
	}
	body, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	if readErr != nil {
		t.Fatalf("read %s: %v", key, readErr)
	}
	if closeErr != nil {
		t.Fatalf("close %s: %v", key, closeErr)
	}
	return body
}
