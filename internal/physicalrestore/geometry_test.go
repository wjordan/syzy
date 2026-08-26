package physicalrestore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
)

// These tests pin the materialize-geometry failure class: an L0 chain
// that lost the frame which grew the database (e.g. a publisher restart
// stranding a checkpointed-but-unshipped txn) still stamps the larger
// page count into page 1 via later frames, so the materialized file is
// shorter than its own header claims — SQLITE_CORRUPT at first open.
// MaterializeStream must refuse to hand such an image to clone.Adopt.

// encodeTestLTX builds a minimal NoChecksum LTX whose pages are filled
// with a marker byte. pgnos lists the page writes; commit stamps the
// database size in pages.
func encodeTestLTX(t *testing.T, pageSize uint32, commit uint32, minTXID, maxTXID uint64, marker byte, pgnos ...uint32) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatalf("ltx encoder: %v", err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    commit,
		MinTXID:   ltx.TXID(minTXID),
		MaxTXID:   ltx.TXID(maxTXID),
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("encode header: %v", err)
	}
	page := bytes.Repeat([]byte{marker}, int(pageSize))
	for _, pgno := range pgnos {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			t.Fatalf("encode page %d: %v", pgno, err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("close encoder: %v", err)
	}
	return buf.Bytes()
}

func publishTestLTX(t *testing.T, be objectstore.Bucket, level int, minTXID, maxTXID uint64, body []byte) objstore.FileRef {
	t.Helper()
	ref, err := objstore.PublishLTX(context.Background(), be, objstore.DBPrefix, level, minTXID, maxTXID, body)
	if err != nil {
		t.Fatalf("publish LTX [%d..%d]: %v", minTXID, maxTXID, err)
	}
	return ref
}

type listCountingBucket struct {
	objectstore.Bucket
	lists int
}

func (b *listCountingBucket) List(ctx context.Context, prefix, startAfter string) ([]objectstore.ObjectInfo, error) {
	b.lists++
	return b.Bucket.List(ctx, prefix, startAfter)
}

// TestRestoreLTXStreamDetectsLostGrowthFrame reproduces the incident
// shape: baseline commits 3 pages; the frame that grew the database to
// 5 pages (writing pages 4 and 5) never reached the bucket; a later
// frame rewrites page 1 with commit=5. The materialized file is 3 pages
// with a 5-page header — the restore must fail, not adopt.
func TestRestoreLTXStreamDetectsLostGrowthFrame(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	const pageSize = 512

	baselineLTX := encodeTestLTX(t, pageSize, 3, 1, 1, 0xB1, 1, 2, 3)
	baselineRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, baselineLTX)

	// txid 2 — the growth frame (pages 4,5, commit=5) — is deliberately
	// absent: it was checkpointed out of the publisher's WAL before it
	// could ship. txid 3 arrives with commit=5 but touches only page 1.
	publishTestLTX(t, be, objstore.L0Level, 3, 3, encodeTestLTX(t, pageSize, 5, 3, 3, 0xC3, 1))

	dst := filepath.Join(t.TempDir(), "app.db")
	err = MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baselineRef}, dst, 0)
	if err == nil {
		t.Fatalf("MaterializeStream accepted a chain missing its growth frame")
	}
	if !strings.Contains(err.Error(), "missing the page writes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRestoreLTXStreamGeometryOK is the healthy inverse: with the
// growth frame present the stream materializes to exactly commit pages.
func TestRestoreLTXStreamGeometryOK(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	const pageSize = 512

	baselineLTX := encodeTestLTX(t, pageSize, 3, 1, 1, 0xB1, 1, 2, 3)
	baselineRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, baselineLTX)
	publishTestLTX(t, be, objstore.L0Level, 2, 2, encodeTestLTX(t, pageSize, 5, 2, 2, 0xC2, 1, 4, 5))
	publishTestLTX(t, be, objstore.L0Level, 3, 3, encodeTestLTX(t, pageSize, 5, 3, 3, 0xC3, 1))

	dst := filepath.Join(t.TempDir(), "app.db")
	if err := MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baselineRef}, dst, 0); err != nil {
		t.Fatalf("MaterializeStream: %v", err)
	}
	fi, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if want := int64(5 * pageSize); fi.Size() != want {
		t.Fatalf("materialized %d bytes, want %d", fi.Size(), want)
	}
}

// TestMaterializeStreamSyncsCompletedImageOnce pins the restore durability
// boundary: baseline and delta frames build one private scratch image, and only
// the complete verified result is synced. Per-frame fsync turns a long object
// chain into the same number of serial storage barriers without making any
// intermediate image installable.
func TestMaterializeStreamSyncsCompletedImageOnce(t *testing.T) {
	ctx := context.Background()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	const pageSize = 512

	baselineRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1,
		encodeTestLTX(t, pageSize, 3, 1, 1, 0xB1, 1, 2, 3))
	publishTestLTX(t, be, objstore.L0Level, 2, 2,
		encodeTestLTX(t, pageSize, 5, 2, 2, 0xC2, 1, 4, 5))
	publishTestLTX(t, be, objstore.L0Level, 3, 3,
		encodeTestLTX(t, pageSize, 5, 3, 3, 0xC3, 1))

	dst := filepath.Join(t.TempDir(), "app.db")
	syncs := 0
	counted := &listCountingBucket{Bucket: be}
	err = materializeStream(ctx, counted, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baselineRef}, dst, 0, time.Time{},
		func(f *os.File) error {
			syncs++
			fi, err := f.Stat()
			if err != nil {
				return err
			}
			if want := int64(5 * pageSize); fi.Size() != want {
				return fmt.Errorf("sync saw %d bytes, want completed %d-byte image", fi.Size(), want)
			}
			page := make([]byte, pageSize)
			if _, err := f.ReadAt(page, 0); err != nil {
				return err
			}
			if page[0] != 0xC3 {
				return fmt.Errorf("sync saw page 1 marker %#x, want final frame %#x", page[0], 0xC3)
			}
			return f.Sync()
		})
	if err != nil {
		t.Fatalf("materializeStream: %v", err)
	}
	if syncs != 1 {
		t.Fatalf("sync calls = %d, want 1", syncs)
	}
	if counted.lists != 2 {
		t.Fatalf("bucket LIST calls = %d, want one L0 and one L1 listing", counted.lists)
	}
}

func TestMaterializeStreamStableBeforeExcludesChangingTail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	const pageSize = 512
	baselineRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1,
		encodeTestLTX(t, pageSize, 3, 1, 1, 0xB1, 1, 2, 3))
	// This recent tail cannot be applied coherently because its growth frame is
	// absent. A settled-tail restore excludes it and retains the valid baseline.
	publishTestLTX(t, be, objstore.L0Level, 3, 3,
		encodeTestLTX(t, pageSize, 5, 3, 3, 0xC3, 1))

	dst := filepath.Join(t.TempDir(), "app.db")
	err = MaterializeStreamStableBefore(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baselineRef}, dst, 0, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("MaterializeStreamStableBefore: %v", err)
	}
	fi, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(3 * pageSize); fi.Size() != want {
		t.Fatalf("materialized %d bytes, want settled baseline size %d", fi.Size(), want)
	}
}

// TestRestoreLTXStreamTruncatesShrink: a chain whose final frame commits
// fewer pages than an earlier frame wrote must trim the file back down
// (non-fresh applies never truncate on their own).
func TestRestoreLTXStreamTruncatesShrink(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	const pageSize = 512

	baselineLTX := encodeTestLTX(t, pageSize, 3, 1, 1, 0xB1, 1, 2, 3)
	baselineRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, baselineLTX)
	publishTestLTX(t, be, objstore.L0Level, 2, 2, encodeTestLTX(t, pageSize, 5, 2, 2, 0xC2, 1, 4, 5))
	// txid 3 shrinks the database back to 3 pages.
	publishTestLTX(t, be, objstore.L0Level, 3, 3, encodeTestLTX(t, pageSize, 3, 3, 3, 0xC3, 1))

	dst := filepath.Join(t.TempDir(), "app.db")
	if err := MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baselineRef}, dst, 0); err != nil {
		t.Fatalf("MaterializeStream: %v", err)
	}
	fi, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if want := int64(3 * pageSize); fi.Size() != want {
		t.Fatalf("materialized %d bytes, want %d", fi.Size(), want)
	}
}
