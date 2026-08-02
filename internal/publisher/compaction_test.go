package publisher_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/publisher"
)

// makeTinyLTX produces a valid 1-page NoChecksum LTX file for testing.
// txid is stamped as both Min and Max.
func makeTinyLTX(t *testing.T, txid uint64, fill byte) []byte {
	t.Helper()
	const pageSize = 4096
	pageData := bytes.Repeat([]byte{fill}, pageSize)

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
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, pageData); err != nil {
		t.Fatalf("EncodePage: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("encoder Close: %v", err)
	}
	return buf.Bytes()
}

func compactLTXBodies(t *testing.T, bodies ...[]byte) []byte {
	t.Helper()
	readers := make([]io.Reader, len(bodies))
	for i := range bodies {
		readers[i] = bytes.NewReader(bodies[i])
	}
	var buf bytes.Buffer
	c, err := ltx.NewCompactor(&buf, readers)
	if err != nil {
		t.Fatalf("NewCompactor: %v", err)
	}
	c.HeaderFlags = ltx.HeaderFlagNoChecksum
	if err := c.Compact(context.Background()); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	return buf.Bytes()
}

// TestCompactor_BelowMinNoOp: fewer than MinFiles inputs → nothing produced.
func TestCompactor_BelowMinNoOp(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	// Seed a couple of L0 LTX files.
	if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, 1, 1, makeTinyLTX(t, 1, 0x01)); err != nil {
		t.Fatalf("publish1: %v", err)
	}
	if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, 2, 2, makeTinyLTX(t, 2, 0x02)); err != nil {
		t.Fatalf("publish2: %v", err)
	}
	c := &publisher.Compactor{Backend: be, StreamPrefix: objstore.DBPrefix, MinFiles: 5}
	produced, err := c.CompactOnce(ctx)
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if produced != 0 {
		t.Errorf("expected 0 L1 below MinFiles, got %d", produced)
	}
}

// TestCompactor_MergesContiguous: with enough contiguous L0s, one
// L1 covering the full range is produced.
func TestCompactor_MergesContiguous(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	for i := uint64(1); i <= 3; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	c := &publisher.Compactor{Backend: be, StreamPrefix: objstore.DBPrefix, MinFiles: 3}
	produced, err := c.CompactOnce(ctx)
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if produced != 1 {
		t.Fatalf("expected 1 L1 produced, got %d", produced)
	}
	// L1 should exist at db/0001/0000000000000001-0000000000000003.ltx.
	objs, err := be.List(ctx, "db/0001/", "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected 1 L1 obj, got %d", len(objs))
	}
}

func TestCompactor_L1CollisionRejectsDifferentContent(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	for i := uint64(1); i <= 3; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish L0 %d: %v", i, err)
		}
	}
	key := objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 1, 3)
	foreign := compactLTXBodies(t,
		makeTinyLTX(t, 1, 0xf1),
		makeTinyLTX(t, 2, 0xf2),
		makeTinyLTX(t, 3, 0xf3),
	)
	if _, err := be.Put(ctx, key, bytes.NewReader(foreign), int64(len(foreign)), objectstore.IfAbsent()); err != nil {
		t.Fatalf("publish foreign L1: %v", err)
	}

	c := &publisher.Compactor{
		Backend:      &hideL1ListBucket{Bucket: be},
		StreamPrefix: objstore.DBPrefix,
		MinFiles:     3,
	}
	if _, err := c.CompactOnce(ctx); !errors.Is(err, objstore.ErrLTXConflict) {
		t.Fatalf("CompactOnce error = %v, want ErrLTXConflict", err)
	}
	if got := readBucketObject(t, ctx, be, key); !bytes.Equal(got, foreign) {
		t.Fatal("foreign L1 changed after rejected collision")
	}
}

func TestCompactor_L1CollisionAcceptsSameContent(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	for i := uint64(1); i <= 3; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish L0 %d: %v", i, err)
		}
	}
	c := &publisher.Compactor{Backend: be, StreamPrefix: objstore.DBPrefix, MinFiles: 3}
	if _, err := c.CompactOnce(ctx); err != nil {
		t.Fatalf("initial CompactOnce: %v", err)
	}
	key := objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 1, 3)
	want := readBucketObject(t, ctx, be, key)

	c.Backend = &hideL1ListBucket{Bucket: be}
	if _, err := c.CompactOnce(ctx); err != nil {
		t.Fatalf("idempotent CompactOnce: %v", err)
	}
	if got := readBucketObject(t, ctx, be, key); !bytes.Equal(got, want) {
		t.Fatal("L1 changed after idempotent collision")
	}
}

func TestCompactor_StreamsInputsAfterOpeningRun(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	for i := uint64(1); i <= 3; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	gb := &gatedGetBucket{Bucket: be, expectedGets: 3}
	c := &publisher.Compactor{Backend: gb, StreamPrefix: objstore.DBPrefix, MinFiles: 3}
	produced, err := c.CompactOnce(ctx)
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if produced != 1 {
		t.Fatalf("expected 1 L1 produced, got %d", produced)
	}
	if got := gb.gets.Load(); got != 3 {
		t.Fatalf("Get calls = %d, want 3", got)
	}
	if got := gb.closes.Load(); got != 3 {
		t.Fatalf("closed bodies = %d, want 3", got)
	}
}

func TestCompactor_ChunksLongRuns(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	for i := uint64(1); i <= 13; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	c := &publisher.Compactor{
		Backend:       be,
		StreamPrefix:  objstore.DBPrefix,
		MinFiles:      3,
		MaxInputFiles: 5,
	}
	produced, err := c.CompactOnce(ctx)
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if produced != 3 {
		t.Fatalf("expected 3 L1 files produced, got %d", produced)
	}
	for _, key := range []string{
		objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 1, 5),
		objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 6, 10),
		objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 11, 13),
	} {
		if _, _, err := be.Get(ctx, key); err != nil {
			t.Fatalf("expected L1 %s: %v", key, err)
		}
	}
}

// TestCompactor_NoOrphanTail: a run whose length is not a multiple of
// MaxInputFiles must be fully covered, with no sub-MinFiles tail left
// behind. The old aligned-chunk logic compacted [1:10] and abandoned the
// 3-file tail [11:13]; that tail is a sub-MinFiles eligible run forever
// skipped on later passes, stalling the contiguous-from-baseline frontier
// and forcing cold restore to replay the raw L0. Regression for that.
func TestCompactor_NoOrphanTail(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	const n = 13
	for i := uint64(1); i <= n; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	c := &publisher.Compactor{Backend: be, StreamPrefix: objstore.DBPrefix, MinFiles: 5, MaxInputFiles: 10}
	if _, err := c.CompactOnce(ctx); err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	// Second pass must find nothing left: every L0 is now L1-covered and
	// the frontier reached the tip. Under the orphan-tail bug the 3-file
	// tail stays eligible and the frontier stalls at 10.
	res, err := (&publisher.Compactor{Backend: be, StreamPrefix: objstore.DBPrefix, MinFiles: 5, MaxInputFiles: 10}).CompactOnceDetailed(ctx)
	if err != nil {
		t.Fatalf("CompactOnceDetailed: %v", err)
	}
	if res.EligibleFiles != 0 {
		t.Fatalf("orphan tail left uncompacted: second-pass EligibleFiles = %d, want 0", res.EligibleFiles)
	}
	if res.L0ScanAfterTXID != n {
		t.Fatalf("L1 contiguous frontier = %d, want %d (tail not covered)", res.L0ScanAfterTXID, n)
	}
}

func TestCompactor_MaxRunsPerPassBoundsWork(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	for i := uint64(1); i <= 10; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	c := &publisher.Compactor{
		Backend:        be,
		StreamPrefix:   objstore.DBPrefix,
		MinFiles:       2,
		MaxInputFiles:  2,
		MaxRunsPerPass: 2,
	}
	res, err := c.CompactOnceDetailed(ctx)
	if err != nil {
		t.Fatalf("CompactOnceDetailed: %v", err)
	}
	if res.Runs != 2 || res.InputFiles != 4 || res.EligibleFiles != 10 {
		t.Fatalf("runs/input/eligible = %d/%d/%d, want 2/4/10", res.Runs, res.InputFiles, res.EligibleFiles)
	}
	for _, key := range []string{
		objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 1, 2),
		objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 3, 4),
	} {
		if _, _, err := be.Get(ctx, key); err != nil {
			t.Fatalf("expected L1 %s: %v", key, err)
		}
	}
	if _, _, err := be.Get(ctx, objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 5, 6)); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("L1 5..6 exists or unexpected err = %v, want not found", err)
	}
}

func TestCompactor_SkipsL0CoveredByAdjacentL1Ranges(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, 1, 1, makeTinyLTX(t, 1, 0x01)); err != nil {
		t.Fatalf("publish gap L0: %v", err)
	}
	if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, 2, 4, makeTinyLTX(t, 2, 0x02)); err != nil {
		t.Fatalf("publish L0: %v", err)
	}
	for _, rg := range [][2]uint64{{2, 3}, {4, 4}} {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L1Level, rg[0], rg[1], makeTinyLTX(t, rg[0], byte(rg[0]))); err != nil {
			t.Fatalf("publish L1 %d..%d: %v", rg[0], rg[1], err)
		}
	}

	cb := &countingGetBucket{Bucket: be}
	c := &publisher.Compactor{Backend: cb, StreamPrefix: objstore.DBPrefix, MinFiles: 2}
	res, err := c.CompactOnceDetailed(ctx)
	if err != nil {
		t.Fatalf("CompactOnceDetailed: %v", err)
	}
	if res.CoveredSkipped != 1 || res.Runs != 0 {
		t.Fatalf("covered/runs = %d/%d, want 1/0", res.CoveredSkipped, res.Runs)
	}
	if got := cb.gets.Load(); got != 0 {
		t.Fatalf("Get calls = %d, want 0", got)
	}
}

func TestCompactor_SkipsCoveredL0OnSecondPass(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	for i := uint64(1); i <= 6; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	c := &publisher.Compactor{Backend: be, StreamPrefix: objstore.DBPrefix, MinFiles: 3}
	first, err := c.CompactOnceDetailed(ctx)
	if err != nil {
		t.Fatalf("first CompactOnceDetailed: %v", err)
	}
	if first.Runs != 1 || first.InputFiles != 6 {
		t.Fatalf("first pass runs/input = %d/%d, want 1/6", first.Runs, first.InputFiles)
	}

	cb := &countingGetBucket{Bucket: be}
	c = &publisher.Compactor{Backend: cb, StreamPrefix: objstore.DBPrefix, MinFiles: 3}
	second, err := c.CompactOnceDetailed(ctx)
	if err != nil {
		t.Fatalf("second CompactOnceDetailed: %v", err)
	}
	if second.Runs != 0 {
		t.Fatalf("second pass produced %d runs, want 0", second.Runs)
	}
	if second.L0ScanAfterTXID != 6 || second.L0Files != 0 || second.EligibleFiles != 0 {
		t.Fatalf("second pass scan_after/l0/eligible = %d/%d/%d, want 6/0/0",
			second.L0ScanAfterTXID, second.L0Files, second.EligibleFiles)
	}
	if got := cb.gets.Load(); got != 0 {
		t.Fatalf("second pass Get calls = %d, want 0", got)
	}
}

func TestCompactor_SkipsBaselineCoveredL0(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	if _, err := objstore.CASHead(ctx, be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: "cafe",
		Baseline:  &objstore.Baseline{TXID: 10},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	for i := uint64(1); i <= 12; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	lb := &listingBucket{Bucket: be}
	c := &publisher.Compactor{
		Backend:      lb,
		StreamPrefix: objstore.DBPrefix,
		MinFiles:     2,
	}
	res, err := c.CompactOnceDetailed(ctx)
	if err != nil {
		t.Fatalf("CompactOnceDetailed: %v", err)
	}
	if res.BaselineTXID != 10 || res.L0ScanAfterTXID != 10 || res.L0Files != 2 || res.BaselineSkipped != 0 {
		t.Fatalf("baseline/scan_after/l0/skipped = %d/%d/%d/%d, want 10/10/2/0",
			res.BaselineTXID, res.L0ScanAfterTXID, res.L0Files, res.BaselineSkipped)
	}
	if res.Runs != 1 || res.InputFiles != 2 || res.EligibleFiles != 2 {
		t.Fatalf("runs/input/eligible = %d/%d/%d, want 1/2/2", res.Runs, res.InputFiles, res.EligibleFiles)
	}
	if got, want := lb.startAfterFor(objstore.LTXLevelPrefix(objstore.DBPrefix, objstore.L0Level)), objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 10, ^uint64(0)); got != want {
		t.Fatalf("L0 list startAfter = %q, want %q", got, want)
	}
	if _, _, err := be.Get(ctx, objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 11, 12)); err != nil {
		t.Fatalf("expected post-baseline L1: %v", err)
	}
}

type gatedGetBucket struct {
	objectstore.Bucket
	expectedGets int64
	gets         atomic.Int64
	closes       atomic.Int64
}

type hideL1ListBucket struct {
	objectstore.Bucket
}

func (b *hideL1ListBucket) List(ctx context.Context, prefix, startAfter string) ([]objectstore.ObjectInfo, error) {
	if prefix == objstore.LTXLevelPrefix(objstore.DBPrefix, objstore.L1Level) {
		return nil, nil
	}
	return b.Bucket.List(ctx, prefix, startAfter)
}

func readBucketObject(t *testing.T, ctx context.Context, b objectstore.Bucket, key string) []byte {
	t.Helper()
	rc, _, err := b.Get(ctx, key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
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

func (b *gatedGetBucket) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	rc, etag, err := b.Bucket.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	b.gets.Add(1)
	return &gatedReadCloser{
		ReadCloser: rc,
		key:        key,
		expected:   b.expectedGets,
		gets:       &b.gets,
		closes:     &b.closes,
	}, etag, nil
}

type gatedReadCloser struct {
	io.ReadCloser
	key      string
	expected int64
	gets     *atomic.Int64
	closes   *atomic.Int64
}

func (rc *gatedReadCloser) Read(p []byte) (int, error) {
	if got := rc.gets.Load(); got < rc.expected {
		return 0, fmt.Errorf("read %s before full compaction run opened: got %d gets, want %d", rc.key, got, rc.expected)
	}
	return rc.ReadCloser.Read(p)
}

func (rc *gatedReadCloser) Close() error {
	rc.closes.Add(1)
	return rc.ReadCloser.Close()
}

type countingGetBucket struct {
	objectstore.Bucket
	gets atomic.Int64
}

func (b *countingGetBucket) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	rc, etag, err := b.Bucket.Get(ctx, key)
	if err == nil && key != objstore.HEADKey {
		b.gets.Add(1)
	}
	return rc, etag, err
}

type listCall struct {
	prefix     string
	startAfter string
}

type listingBucket struct {
	objectstore.Bucket
	calls []listCall
}

func (b *listingBucket) List(ctx context.Context, prefix, startAfter string) ([]objectstore.ObjectInfo, error) {
	b.calls = append(b.calls, listCall{prefix: prefix, startAfter: startAfter})
	return b.Bucket.List(ctx, prefix, startAfter)
}

func (b *listingBucket) startAfterFor(prefix string) string {
	for _, c := range b.calls {
		if c.prefix == prefix {
			return c.startAfter
		}
	}
	return ""
}

// TestRetention_DeletesCovered: an L0 covered by an L1 and older
// than grace gets deleted.
func TestRetention_DeletesCovered(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	ctx := context.Background()
	// Plant a HEAD with a baseline at TXID=10 so retention has a
	// horizon.
	if _, err := objstore.CASHead(ctx, be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: "cafe",
		Baseline:  &objstore.Baseline{TXID: 10},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	// And an L1 covering 1..3. Retention only checks coverage by TXID
	// range, not LTX internals.
	if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L1Level, 1, 3, makeTinyLTX(t, 1, 0x55)); err != nil {
		t.Fatalf("publish L1: %v", err)
	}

	r := &publisher.Retention{Backend: be, Grace: time.Nanosecond /* effectively immediate */}
	res, err := r.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.L0Deleted != 3 {
		t.Errorf("L0Deleted: got %d want 3", res.L0Deleted)
	}
}

func TestRetention_DeletesL0CoveredByAdjacentL1Ranges(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	if _, err := objstore.CASHead(ctx, be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: "cafe",
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, 1, 4, makeTinyLTX(t, 1, 0x01)); err != nil {
		t.Fatalf("publish L0: %v", err)
	}
	for _, rg := range [][2]uint64{{1, 2}, {3, 4}} {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L1Level, rg[0], rg[1], makeTinyLTX(t, rg[0], byte(rg[0]))); err != nil {
			t.Fatalf("publish L1 %d..%d: %v", rg[0], rg[1], err)
		}
	}

	r := &publisher.Retention{Backend: be, Grace: time.Nanosecond}
	res, err := r.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.L0Deleted != 1 {
		t.Fatalf("L0Deleted = %d, want 1", res.L0Deleted)
	}
	if _, _, err := be.Get(ctx, objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 1, 4)); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("covered L0 exists or unexpected err = %v, want not found", err)
	}
}

func TestRetention_DeletesBaselineCoveredL0AfterGrace(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	if _, err := objstore.CASHead(ctx, be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: "cafe",
		Baseline: &objstore.Baseline{
			TXID:      10,
			BuiltAtUS: time.Now().Add(-time.Hour).UnixMicro(),
		},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	for _, txid := range []uint64{1, 11} {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, txid, txid, makeTinyLTX(t, txid, byte(txid))); err != nil {
			t.Fatalf("publish %d: %v", txid, err)
		}
	}

	r := &publisher.Retention{Backend: be, Grace: time.Millisecond}
	res, err := r.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.L0Deleted != 1 {
		t.Fatalf("L0Deleted = %d, want 1", res.L0Deleted)
	}
	if _, _, err := be.Get(ctx, objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 1, 1)); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("baseline-covered L0 exists or unexpected err = %v, want not found", err)
	}
	if _, _, err := be.Get(ctx, objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 11, 11)); err != nil {
		t.Fatalf("post-baseline L0 deleted: %v", err)
	}
}

func TestRetention_KeepsBaselineCoveredL0UntilGrace(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	if _, err := objstore.CASHead(ctx, be, &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: "cafe",
		Baseline: &objstore.Baseline{
			TXID:      10,
			BuiltAtUS: time.Now().UnixMicro(),
		},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, 1, 1, makeTinyLTX(t, 1, 0x01)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	r := &publisher.Retention{Backend: be, Grace: time.Hour}
	res, err := r.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.L0Deleted != 0 {
		t.Fatalf("L0Deleted = %d, want 0", res.L0Deleted)
	}
	if _, _, err := be.Get(ctx, objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 1, 1)); err != nil {
		t.Fatalf("baseline-covered L0 deleted before grace: %v", err)
	}
}

// TestRetention_DryRun: nothing gets deleted but counts are
// accurate.
func TestRetention_DryRun(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	ctx := context.Background()
	if _, err := objstore.CASHead(ctx, be, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: "fade",
		Baseline: &objstore.Baseline{TXID: 10},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}
	body := makeTinyLTX(t, 1, 0x77)
	if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, 1, 1, body); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L1Level, 1, 1, body); err != nil {
		t.Fatalf("publish L1: %v", err)
	}
	r := &publisher.Retention{Backend: be, Grace: time.Nanosecond, DryRun: true}
	res, err := r.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.L0Deleted != 1 {
		t.Errorf("L0Deleted reported: %d (dry-run)", res.L0Deleted)
	}
	// The actual file should still be present.
	if _, _, err := be.Get(ctx, objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 1, 1)); err != nil {
		t.Errorf("dry-run deleted file: %v", err)
	}
}

// avoid unused-import warning when test file is the only consumer of filepath
var _ = filepath.Join

// makeAttestedChain builds n contiguous checksummed L0s (txids
// first..first+n-1) chained from a seeded state, each rewriting page 1.
func makeAttestedChain(t *testing.T, first uint64, n int) [][]byte {
	t.Helper()
	const pageSize = 4096
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "src.db")
	page := bytes.Repeat([]byte{0xB0}, pageSize)
	page[16], page[17] = 0x10, 0x00 // page size 4096
	if err := os.WriteFile(dbPath, page, 0o644); err != nil {
		t.Fatalf("write fake db: %v", err)
	}
	var baseBuf bytes.Buffer
	_, state, err := ltxstream.EncodeBaseline(context.Background(), &baseBuf, dbPath, first-1)
	if err != nil {
		t.Fatalf("EncodeBaseline: %v", err)
	}
	bodies := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		txid := first + uint64(i)
		pageMap := map[uint32][]byte{1: bytes.Repeat([]byte{byte(0xC0 + i)}, pageSize)}
		pageMap[1][16], pageMap[1][17] = 0x10, 0x00
		st := state.Stage(pageMap, 1)
		hdr := ltx.Header{
			Version: ltx.Version, PageSize: pageSize, Commit: 1,
			MinTXID: ltx.TXID(txid), MaxTXID: ltx.TXID(txid),
			Timestamp: time.Now().UnixMilli(), PreApplyChecksum: st.Pre,
		}
		var buf bytes.Buffer
		if err := ltxstream.EncodeIncremental(context.Background(), &buf, pageMap, hdr, st.Post); err != nil {
			t.Fatalf("encode attested L0 %d: %v", txid, err)
		}
		st.Commit()
		bodies = append(bodies, buf.Bytes())
	}
	return bodies
}

// TestCompactor_PropagatesAttestations: an all-attested run must merge
// into an L1 that keeps checksum tracking — first input's pre-apply,
// last input's post-apply.
func TestCompactor_PropagatesAttestations(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	bodies := makeAttestedChain(t, 2, 3)
	for i, body := range bodies {
		txid := uint64(2 + i)
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, txid, txid, body); err != nil {
			t.Fatalf("publish %d: %v", txid, err)
		}
	}
	c := &publisher.Compactor{Backend: be, StreamPrefix: objstore.DBPrefix, MinFiles: 3}
	if produced, err := c.CompactOnce(ctx); err != nil || produced != 1 {
		t.Fatalf("CompactOnce: produced=%d err=%v", produced, err)
	}
	merged := readBucketObject(t, ctx, be, "db/0001/0000000000000002-0000000000000004.ltx")
	dec := ltx.NewDecoder(bytes.NewReader(merged))
	if err := dec.Verify(); err != nil {
		t.Fatalf("verify merged L1: %v", err)
	}
	if dec.Header().NoChecksum() {
		t.Fatalf("merged L1 dropped checksum tracking")
	}
	firstDec := ltx.NewDecoder(bytes.NewReader(bodies[0]))
	if err := firstDec.Verify(); err != nil {
		t.Fatalf("verify first input: %v", err)
	}
	lastDec := ltx.NewDecoder(bytes.NewReader(bodies[2]))
	if err := lastDec.Verify(); err != nil {
		t.Fatalf("verify last input: %v", err)
	}
	if got, want := dec.Header().PreApplyChecksum, firstDec.Header().PreApplyChecksum; got != want {
		t.Fatalf("merged pre-apply %s != first input's %s", got, want)
	}
	if got, want := dec.Trailer().PostApplyChecksum, lastDec.Trailer().PostApplyChecksum; got != want {
		t.Fatalf("merged post-apply %s != last input's %s", got, want)
	}
}

// TestCompactor_SplitsRunAtAttestationBoundary: a run mixing legacy
// (NoChecksum) and attested inputs cannot merge into one valid L1; the
// compactor must truncate to the leading uniform segment rather than
// fail or emit an invalid object.
func TestCompactor_SplitsRunAtAttestationBoundary(t *testing.T) {
	t.Parallel()
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	for i := uint64(1); i <= 2; i++ {
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, i, i, makeTinyLTX(t, i, byte(i))); err != nil {
			t.Fatalf("publish legacy %d: %v", i, err)
		}
	}
	bodies := makeAttestedChain(t, 3, 2)
	for i, body := range bodies {
		txid := uint64(3 + i)
		if _, err := objstore.PublishLTX(ctx, be, objstore.DBPrefix, objstore.L0Level, txid, txid, body); err != nil {
			t.Fatalf("publish attested %d: %v", txid, err)
		}
	}
	c := &publisher.Compactor{Backend: be, StreamPrefix: objstore.DBPrefix, MinFiles: 4}
	if produced, err := c.CompactOnce(ctx); err != nil || produced != 1 {
		t.Fatalf("CompactOnce: produced=%d err=%v", produced, err)
	}
	// Only the legacy prefix [1..2] merges; the attested tail stays L0.
	merged := readBucketObject(t, ctx, be, "db/0001/0000000000000001-0000000000000002.ltx")
	dec := ltx.NewDecoder(bytes.NewReader(merged))
	if err := dec.Verify(); err != nil {
		t.Fatalf("verify merged L1: %v", err)
	}
	if !dec.Header().NoChecksum() {
		t.Fatalf("legacy-prefix L1 unexpectedly claims checksum tracking")
	}
}
