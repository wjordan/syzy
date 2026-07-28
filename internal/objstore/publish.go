package objstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/wjordan/objectstore"
)

// ErrLTXConflict reports an immutable-key collision with different bytes.
var ErrLTXConflict = errors.New("objstore: LTX object conflict")

// ErrBaselineRegression reports a coupled promotion that would move one of
// HEAD's baseline pointers backward.
var ErrBaselineRegression = errors.New("objstore: baseline promotion would regress HEAD")

// ErrPublisherLeaseActive reports an offline baseline publication attempted
// while a live publisher still owns HEAD. Offline publication requires an
// exclusive target; fencing a live publisher is a separate operator action.
var ErrPublisherLeaseActive = errors.New("objstore: publisher lease is active")

// PublishLTX uploads ltxBody as an immutable object under
// streamPrefix/<level>/<minTXID>-<maxTXID>.ltx. Returns a FileRef
// with size + sha256 over the LTX bytes. A PUT collision on the same
// key is treated as an idempotent success only after the stored object
// is verified to have the same size and SHA-256.
//
// streamPrefix is one of DBPrefix or MetadataPrefix.
func PublishLTX(
	ctx context.Context,
	b objectstore.Bucket,
	streamPrefix string,
	level int,
	minTXID, maxTXID uint64,
	ltxBody []byte,
) (FileRef, error) {
	key := LTXKey(streamPrefix, level, minTXID, maxTXID)
	sum := sha256.Sum256(ltxBody)
	digest := hex.EncodeToString(sum[:])
	_, err := b.Put(ctx, key, bytes.NewReader(ltxBody), int64(len(ltxBody)), objectstore.IfAbsent())
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		if err := VerifyLTXCollision(ctx, b, key, int64(len(ltxBody)), digest); err != nil {
			return FileRef{}, err
		}
	} else if err != nil {
		return FileRef{}, fmt.Errorf("objstore: PUT %s: %w", key, err)
	}
	return FileRef{
		Key:    key,
		Size:   int64(len(ltxBody)),
		Sha256: digest,
	}, nil
}

// VerifyLTXCollision confirms that an object found after an immutable-key
// collision contains exactly the proposed bytes.
func VerifyLTXCollision(ctx context.Context, b objectstore.Bucket, key string, expectedSize int64, expectedSHA string) error {
	rc, _, err := b.Get(objectstore.WithConsistentRead(ctx), key)
	if err != nil {
		return fmt.Errorf("objstore: GET %s after PUT collision: %w", key, err)
	}
	h := sha256.New()
	actualSize, readErr := io.Copy(h, rc)
	closeErr := rc.Close()
	if readErr != nil {
		return fmt.Errorf("objstore: read %s after PUT collision: %w", key, readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("objstore: close %s after PUT collision: %w", key, closeErr)
	}
	actualSHA := hex.EncodeToString(h.Sum(nil))
	if actualSize != expectedSize || actualSHA != expectedSHA {
		return fmt.Errorf("%w: %s has size %d sha256 %s, proposed size %d sha256 %s",
			ErrLTXConflict, key, actualSize, actualSHA, expectedSize, expectedSHA)
	}
	return nil
}

// PublishCoupledBaselinesOwned is the lease-scoped form of coupled baseline
// publication. It updates HEAD only while expected still exactly matches
// HEAD.publisher and that lease remains active. A stale or expired generation
// may have uploaded immutable baseline objects, but it cannot move HEAD's
// pointers.
func PublishCoupledBaselinesOwned(
	ctx context.Context,
	b objectstore.Bucket,
	clusterID string,
	expected PublisherIdentity,
	txid uint64,
	appBaselineLTX, metaBaselineLTX []byte,
) error {
	return publishCoupledBaselines(ctx, b, clusterID, expected, time.Now, txid, appBaselineLTX, metaBaselineLTX)
}

// PublishMetadataBaselineOwned uploads and promotes one metadata baseline while
// expected remains the exact active publisher generation in HEAD.
func PublishMetadataBaselineOwned(
	ctx context.Context,
	b objectstore.Bucket,
	clusterID string,
	expected PublisherIdentity,
	txid uint64,
	metaBaselineLTX []byte,
) error {
	return publishMetadataBaselineOwned(ctx, b, clusterID, expected, time.Now, txid, metaBaselineLTX)
}

func publishMetadataBaselineOwned(
	ctx context.Context,
	b objectstore.Bucket,
	clusterID string,
	expected PublisherIdentity,
	clock func() time.Time,
	txid uint64,
	metaBaselineLTX []byte,
) error {
	if clusterID == "" {
		return errors.New("objstore: ClusterID required")
	}
	ref, err := PublishLTX(ctx, b, MetadataPrefix, BaselineLevel, txid, txid, metaBaselineLTX)
	if err != nil {
		return fmt.Errorf("publish metadata baseline LTX: %w", err)
	}
	return mutateOwnedHEAD(ctx, b, clusterID, expected, clock, func(cur *HEAD, nowUS int64) (*HEAD, error) {
		if err := checkBaselineDigest(cur.MetaBaseline, "metadata", txid, ref); err != nil {
			return nil, err
		}
		if cur.MetaBaseline != nil && cur.MetaBaseline.TXID >= txid {
			return nil, nil
		}
		next := *cur
		next.MetaBaseline = &Baseline{TXID: txid, LTXRef: ref, BuiltAtUS: nowUS}
		return &next, nil
	})
}

// publishCoupledBaselines requires an exact active owner. Live publishing and
// temporary offline maintenance both acquire HEAD.publisher before TXID
// allocation or immutable uploads; there is deliberately no unfenced form.
func publishCoupledBaselines(
	ctx context.Context,
	b objectstore.Bucket,
	clusterID string,
	expected PublisherIdentity,
	clock func() time.Time,
	txid uint64,
	appBaselineLTX, metaBaselineLTX []byte,
) error {
	if clusterID == "" {
		return errors.New("objstore: ClusterID required")
	}
	appRef, err := PublishLTX(ctx, b, DBPrefix, BaselineLevel, txid, txid, appBaselineLTX)
	if err != nil {
		return fmt.Errorf("publish app baseline LTX: %w", err)
	}
	metaRef, err := PublishLTX(ctx, b, MetadataPrefix, BaselineLevel, txid, txid, metaBaselineLTX)
	if err != nil {
		return fmt.Errorf("publish metadata baseline LTX: %w", err)
	}
	return mutateOwnedHEAD(ctx, b, clusterID, expected, clock, func(cur *HEAD, nowUS int64) (*HEAD, error) {
		if err := checkBaselineDigest(cur.Baseline, "app", txid, appRef); err != nil {
			return nil, err
		}
		if err := checkBaselineDigest(cur.MetaBaseline, "metadata", txid, metaRef); err != nil {
			return nil, err
		}
		appTXID, metaTXID := baselineTXID(cur.Baseline), baselineTXID(cur.MetaBaseline)
		if appTXID >= txid && metaTXID >= txid {
			return nil, nil
		}
		// Monotonic per stream: one pointer already ahead while the other is
		// not must fail closed rather than regress the ahead stream.
		if appTXID > txid || metaTXID > txid {
			return nil, fmt.Errorf("%w: candidate txid %d, app txid %d, metadata txid %d",
				ErrBaselineRegression, txid, appTXID, metaTXID)
		}
		next := *cur
		next.Version = HEADVersion
		next.ClusterID = clusterID
		next.Baseline = &Baseline{TXID: txid, LTXRef: appRef, BuiltAtUS: nowUS}
		next.MetaBaseline = &Baseline{TXID: txid, LTXRef: metaRef, BuiltAtUS: nowUS}
		return &next, nil
	})
}

// mutateOwnedHEAD CAS-loops one lease-scoped HEAD update. Every attempt
// re-reads HEAD and verifies the exact, unexpired owner — a newer generation
// may already cover the update, but that does not make an old generation's
// mutation successful — then apply builds the next HEAD, or returns nil to
// report the update already covered (idempotent success). A missing HEAD is
// ownership loss: every owner path creates HEAD at claim time.
func mutateOwnedHEAD(
	ctx context.Context,
	b objectstore.Bucket,
	clusterID string,
	expected PublisherIdentity,
	clock func() time.Time,
	apply func(cur *HEAD, nowUS int64) (*HEAD, error),
) error {
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		cur, etag, err := LoadHEAD(ctx, b)
		if errors.Is(err, ErrNoHEAD) {
			return publisherOwnershipLost(expected, nil, clock().UnixMicro())
		}
		if err != nil {
			return fmt.Errorf("load HEAD: %w", err)
		}
		nowUS := clock().UnixMicro()
		if err := validateOwnedHEAD(cur, clusterID, expected, nowUS); err != nil {
			return err
		}
		next, err := apply(cur, nowUS)
		if err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		if _, err := CASHead(ctx, b, next, &etag); errors.Is(err, objectstore.ErrPreconditionFailed) {
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
	return errors.New("objstore: owned HEAD CAS retries exhausted")
}

// checkBaselineDigest rejects an equal-TXID pointer with foreign bytes.
// "Covered" must mean OUR bytes, not just our TXID: under a concurrent
// double-claim (multi-region LWW CAS) another claimant can publish a baseline
// at the SAME txid to the SAME keys, overwriting ours. Adopting a foreign
// pointer stitches two nodes' snapshots into one unrestorable chain.
func checkBaselineDigest(cur *Baseline, stream string, txid uint64, ref FileRef) error {
	if cur != nil && cur.TXID == txid && cur.LTXRef.Sha256 != ref.Sha256 {
		return fmt.Errorf("%w: %s baseline HEAD digest differs at txid %d", ErrLTXConflict, stream, txid)
	}
	return nil
}

func baselineTXID(b *Baseline) uint64 {
	if b == nil {
		return 0
	}
	return b.TXID
}

func validateOwnedHEAD(head *HEAD, clusterID string, expected PublisherIdentity, nowUS int64) error {
	if head.ClusterID != "" && head.ClusterID != clusterID {
		return fmt.Errorf("%w: HEAD has %s, publisher is %s", ErrClusterIDMismatch, head.ClusterID, clusterID)
	}
	if !expected.ActiveAt(head.Publisher, nowUS) {
		return publisherOwnershipLost(expected, head.Publisher, nowUS)
	}
	return nil
}

func publisherOwnershipLost(expected PublisherIdentity, actual *Publisher, nowUS int64) error {
	if actual == nil {
		return fmt.Errorf("%w: expected %s generation %d, HEAD has no publisher", ErrPublisherOwnershipLost, expected.NodeID, expected.Generation)
	}
	if expected.Matches(actual) && actual.ExpiresAtUS <= nowUS {
		return fmt.Errorf("%w: %s generation %d expired at %d (operation time %d)",
			ErrPublisherOwnershipLost, expected.NodeID, expected.Generation, actual.ExpiresAtUS, nowUS)
	}
	return fmt.Errorf("%w: expected %s generation %d, HEAD has %s generation %d",
		ErrPublisherOwnershipLost, expected.NodeID, expected.Generation, actual.NodeID, actual.Generation)
}

// LTXFile is one parsed entry from ListLTX.
type LTXFile struct {
	Key      string
	MinTXID  uint64
	MaxTXID  uint64
	Modified time.Time
	Size     int64
}

// ListLTX paginates through streamPrefix/<level>/ and returns parsed
// entries. streamPrefix is one of DBPrefix or MetadataPrefix.
func ListLTX(ctx context.Context, b objectstore.Bucket, streamPrefix string, level int) ([]LTXFile, error) {
	return listLTXFrom(ctx, b, streamPrefix, level, "")
}

// ListLTXAfter returns LTX entries whose MinTXID sorts after txid.
// Callers should only use txid values known to be stream boundaries:
// this intentionally skips keys with MinTXID <= txid.
func ListLTXAfter(ctx context.Context, b objectstore.Bucket, streamPrefix string, level int, txid uint64) ([]LTXFile, error) {
	if txid == 0 {
		return ListLTX(ctx, b, streamPrefix, level)
	}
	return listLTXFrom(ctx, b, streamPrefix, level, LTXKey(streamPrefix, level, txid, ^uint64(0)))
}

func listLTXFrom(ctx context.Context, b objectstore.Bucket, streamPrefix string, level int, startAfter string) ([]LTXFile, error) {
	prefix := LTXLevelPrefix(streamPrefix, level)
	var out []LTXFile
	for {
		page, err := b.List(ctx, prefix, startAfter)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		if len(page) == 0 {
			return out, nil
		}
		for _, o := range page {
			lo, hi, ok := ParseLTXKey(o.Key)
			if !ok {
				continue
			}
			out = append(out, LTXFile{Key: o.Key, MinTXID: lo, MaxTXID: hi, Modified: o.LastModified, Size: o.Size})
		}
		if len(page) < 1000 {
			return out, nil
		}
		startAfter = page[len(page)-1].Key
	}
}

// MaxLTXTXID returns the highest maxTXID across streamPrefix/<level>/.
// Used to seed a fresh publisher's TXID counter on takeover.
func MaxLTXTXID(ctx context.Context, b objectstore.Bucket, streamPrefix string, level int) (uint64, error) {
	files, err := ListLTX(ctx, b, streamPrefix, level)
	if err != nil {
		return 0, err
	}
	var max uint64
	for _, f := range files {
		if f.MaxTXID > max {
			max = f.MaxTXID
		}
	}
	return max, nil
}

// ParseLTXKey extracts (minTXID, maxTXID) from a
// <streamPrefix><level-4hex>/<min>-<max>.ltx key. Accepts both
// db/ and metadata/ prefixes.
func ParseLTXKey(key string) (lo, hi uint64, ok bool) {
	var prefix string
	switch {
	case len(key) > len(DBPrefix) && key[:len(DBPrefix)] == DBPrefix:
		prefix = DBPrefix
	case len(key) > len(MetadataPrefix) && key[:len(MetadataPrefix)] == MetadataPrefix:
		prefix = MetadataPrefix
	default:
		return 0, 0, false
	}
	const suffix = ".ltx"
	if len(key) <= len(prefix)+5+len(suffix) {
		return 0, 0, false
	}
	if key[len(key)-len(suffix):] != suffix {
		return 0, 0, false
	}
	rest := key[len(prefix) : len(key)-len(suffix)]
	slash := -1
	for i, c := range rest {
		if c == '/' {
			slash = i
			break
		}
	}
	if slash < 0 {
		return 0, 0, false
	}
	filename := rest[slash+1:]
	if len(filename) != 16+1+16 || filename[16] != '-' {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(filename, "%016x-%016x", &lo, &hi); err != nil {
		return 0, 0, false
	}
	return lo, hi, true
}
