package physicalrestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/internal/objstore"
)

// These tests pin the attestation checks on the restore path: LTX
// files that carry pre/post-apply checksums must be verified as they
// are applied, and a chain whose materialized bytes diverge from the
// attested database state must fail the restore instead of being
// adopted silently.

const attestPageSize = 512

// fakeDB writes a database-shaped file: page 1 carries the page size
// at SQLite's header offset (all EncodeBaseline reads), and every page
// is filled with a distinct marker. Returns the path and page contents.
func fakeDB(t *testing.T, pages int) (string, [][]byte) {
	t.Helper()
	content := make([][]byte, pages)
	var buf bytes.Buffer
	for i := range content {
		page := bytes.Repeat([]byte{byte(0xA0 + i)}, attestPageSize)
		if i == 0 {
			binary.BigEndian.PutUint16(page[16:18], attestPageSize)
		}
		content[i] = page
		buf.Write(page)
	}
	path := filepath.Join(t.TempDir(), "src.db")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write fake db: %v", err)
	}
	return path, content
}

// attestedBaseline encodes path as a checksummed snapshot LTX at txid
// and returns its bytes plus the seeded checksum state.
func attestedBaseline(t *testing.T, path string, txid uint64) ([]byte, *ltxstream.ChecksumState) {
	t.Helper()
	var buf bytes.Buffer
	_, state, err := ltxstream.EncodeBaseline(context.Background(), &buf, path, txid)
	if err != nil {
		t.Fatalf("encode baseline: %v", err)
	}
	return buf.Bytes(), state
}

// attestedL0 stages pageMap against state, encodes an attested L0, and
// commits the stage (mirroring a successful publish). encodeMap, when
// non-nil, is what actually gets encoded — passing bytes that differ
// from the staged pageMap fabricates a corrupt-but-attested object.
func attestedL0(t *testing.T, state *ltxstream.ChecksumState, pageMap map[uint32][]byte, commit uint32, minTXID, maxTXID uint64, encodeMap map[uint32][]byte) []byte {
	t.Helper()
	att := state.Stage(pageMap, commit)
	hdr := ltx.Header{
		Version:          ltx.Version,
		PageSize:         attestPageSize,
		Commit:           commit,
		MinTXID:          ltx.TXID(minTXID),
		MaxTXID:          ltx.TXID(maxTXID),
		Timestamp:        time.Now().UnixMilli(),
		PreApplyChecksum: att.Pre,
	}
	if encodeMap == nil {
		encodeMap = pageMap
	}
	var buf bytes.Buffer
	if err := ltxstream.EncodeIncremental(context.Background(), &buf, encodeMap, hdr, att.Post); err != nil {
		t.Fatalf("encode L0 [%d..%d]: %v", minTXID, maxTXID, err)
	}
	att.Commit()
	return buf.Bytes()
}

func page(marker byte) []byte { return bytes.Repeat([]byte{marker}, attestPageSize) }

func openTestBucket(t *testing.T) objectstore.Bucket {
	t.Helper()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	return be
}

// TestRestoreAttestedRoundtrip is the happy path: checksummed baseline
// plus checksummed L0s restore cleanly and the materialized bytes match
// the state the chain attests.
func TestRestoreAttestedRoundtrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be := openTestBucket(t)
	dbPath, content := fakeDB(t, 3)

	base, state := attestedBaseline(t, dbPath, 1)
	baseRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, base)

	// txid 2 rewrites page 2; txid 3 grows the database to 4 pages.
	p2, p4 := page(0xC2), page(0xC4)
	publishTestLTX(t, be, objstore.L0Level, 2, 2,
		attestedL0(t, state, map[uint32][]byte{2: p2}, 3, 2, 2, nil))
	publishTestLTX(t, be, objstore.L0Level, 3, 3,
		attestedL0(t, state, map[uint32][]byte{4: p4}, 4, 3, 3, nil))

	dst := filepath.Join(t.TempDir(), "app.db")
	if err := MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baseRef}, dst, 0); err != nil {
		t.Fatalf("MaterializeStream: %v", err)
	}
	want := bytes.Join([][]byte{content[0], p2, content[2], p4}, nil)
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read restored db: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("restored bytes diverge from attested state (len %d vs %d)", len(got), len(want))
	}
}

// TestRestoreRejectsCorruptAttestedPage: an L0 whose encoded page bytes
// differ from the state its trailer attests (a flipped byte upstream of
// encoding, so the file's own FileChecksum is consistent) must fail the
// restore. Before attestation checks this restored silently.
func TestRestoreRejectsCorruptAttestedPage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be := openTestBucket(t)
	dbPath, _ := fakeDB(t, 3)

	base, state := attestedBaseline(t, dbPath, 1)
	baseRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, base)

	truth := page(0xC2)
	corrupt := append([]byte(nil), truth...)
	corrupt[100] ^= 0xFF // flipped byte relative to the attested content
	publishTestLTX(t, be, objstore.L0Level, 2, 2,
		attestedL0(t, state, map[uint32][]byte{2: truth}, 3, 2, 2, map[uint32][]byte{2: corrupt}))

	dst := filepath.Join(t.TempDir(), "app.db")
	err := MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baseRef}, dst, 0)
	if err == nil {
		t.Fatalf("MaterializeStream adopted a page that contradicts its post-apply attestation")
	}
	if !strings.Contains(err.Error(), "post-apply") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRestoreRejectsAttestedContentHole: an attested chain missing a
// middle L0 (a TXID gap that also drops page content) must fail — the
// surviving frames' checksums no longer describe the materialized
// state. Before attestation checks the hole restored silently whenever
// the last frame's commit still matched the file size.
func TestRestoreRejectsAttestedContentHole(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be := openTestBucket(t)
	dbPath, _ := fakeDB(t, 3)

	base, state := attestedBaseline(t, dbPath, 1)
	baseRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, base)

	publishTestLTX(t, be, objstore.L0Level, 2, 2,
		attestedL0(t, state, map[uint32][]byte{2: page(0xC2)}, 3, 2, 2, nil))
	// txid 3 (rewrites page 3) is staged into the chain but never
	// published: a content hole.
	attestedL0(t, state, map[uint32][]byte{3: page(0xC3)}, 3, 3, 3, nil)
	publishTestLTX(t, be, objstore.L0Level, 4, 4,
		attestedL0(t, state, map[uint32][]byte{1: page(0xC4)}, 3, 4, 4, nil))

	dst := filepath.Join(t.TempDir(), "app.db")
	err := MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baseRef}, dst, 0)
	if err == nil {
		t.Fatalf("MaterializeStream adopted a chain with a missing attested frame")
	}
}

// TestRestoreRejectsOverlappingChain: two L0s with overlapping TXID
// ranges (a double-claim interleaving or torn listing) must be refused
// before any pages apply. Previously both applied in MinTXID order,
// silently time-traveling pages.
func TestRestoreRejectsOverlappingChain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be := openTestBucket(t)

	baselineLTX := encodeTestLTX(t, attestPageSize, 3, 1, 1, 0xB1, 1, 2, 3)
	baseRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, baselineLTX)
	publishTestLTX(t, be, objstore.L0Level, 2, 3, encodeTestLTX(t, attestPageSize, 3, 2, 3, 0xC2, 2))
	publishTestLTX(t, be, objstore.L0Level, 3, 4, encodeTestLTX(t, attestPageSize, 3, 3, 4, 0xC3, 3))

	dst := filepath.Join(t.TempDir(), "app.db")
	err := MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baseRef}, dst, 0)
	if err == nil {
		t.Fatalf("MaterializeStream applied overlapping TXID ranges")
	}
	if !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRestoreRejectsTruncatedFinalFrame: a final frame whose object
// bytes were cut short must fail the restore (the LTX framing and
// trailer FileChecksum catch it at decode).
func TestRestoreRejectsTruncatedFinalFrame(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be := openTestBucket(t)
	dbPath, _ := fakeDB(t, 3)

	base, state := attestedBaseline(t, dbPath, 1)
	baseRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, base)

	body := attestedL0(t, state, map[uint32][]byte{2: page(0xC2)}, 3, 2, 2, nil)
	publishTestLTX(t, be, objstore.L0Level, 2, 2, body[:len(body)/2])

	dst := filepath.Join(t.TempDir(), "app.db")
	err := MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baseRef}, dst, 0)
	if err == nil {
		t.Fatalf("MaterializeStream adopted a truncated final frame")
	}
}

// TestRestoreToleratesReEmittedDuplicateContent pins the designed-in
// publisher behavior the checks must not break: a failed publish
// re-ships the same WAL content under fresh TXIDs, and the original
// may still land as an orphan. The duplicate's pre-apply checksum
// doesn't match the state it applies onto, but its application
// converges to exactly the attested post state — restore must accept.
func TestRestoreToleratesReEmittedDuplicateContent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be := openTestBucket(t)
	dbPath, _ := fakeDB(t, 3)

	base, state := attestedBaseline(t, dbPath, 1)
	baseRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, base)

	// Orphan: staged but the stage is abandoned (publish "failed" after
	// the object landed), so the re-emit stages from the same pre-state.
	pageMap := map[uint32][]byte{2: page(0xC2)}
	orphan := state.Stage(pageMap, 3)
	hdr := ltx.Header{
		Version: ltx.Version, PageSize: attestPageSize, Commit: 3,
		MinTXID: 2, MaxTXID: 2, Timestamp: time.Now().UnixMilli(),
		PreApplyChecksum: orphan.Pre,
	}
	var buf bytes.Buffer
	if err := ltxstream.EncodeIncremental(ctx, &buf, pageMap, hdr, orphan.Post); err != nil {
		t.Fatalf("encode orphan: %v", err)
	}
	publishTestLTX(t, be, objstore.L0Level, 2, 2, buf.Bytes())
	// Re-emit under fresh TXIDs (4..4, leaking 3): same content, same
	// pre-state.
	publishTestLTX(t, be, objstore.L0Level, 4, 4,
		attestedL0(t, state, pageMap, 3, 4, 4, nil))

	dst := filepath.Join(t.TempDir(), "app.db")
	if err := MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baseRef}, dst, 0); err != nil {
		t.Fatalf("MaterializeStream rejected a convergent re-emitted duplicate: %v", err)
	}
}

// TestRestoreVerifiesAttestedChainOverLegacyBaseline: attestation
// checks must also engage when the chain starts from an unattested
// (pre-checksum) baseline — the verifier seeds its rolling checksum by
// scanning the materialized state before the first attested frame.
func TestRestoreVerifiesAttestedChainOverLegacyBaseline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be := openTestBucket(t)
	dbPath, _ := fakeDB(t, 3)

	// Legacy baseline: same content, encoded without checksums.
	_, state := attestedBaseline(t, dbPath, 1)
	raw, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read fake db: %v", err)
	}
	var legacy bytes.Buffer
	enc, err := ltx.NewEncoder(&legacy)
	if err != nil {
		t.Fatalf("ltx encoder: %v", err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum,
		PageSize: attestPageSize, Commit: 3, MinTXID: 1, MaxTXID: 1,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("encode legacy header: %v", err)
	}
	for pgno := uint32(1); pgno <= 3; pgno++ {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, raw[(pgno-1)*attestPageSize:pgno*attestPageSize]); err != nil {
			t.Fatalf("encode legacy page %d: %v", pgno, err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("close legacy encoder: %v", err)
	}
	baseRef := publishTestLTX(t, be, objstore.BaselineLevel, 1, 1, legacy.Bytes())

	// A corrupt attested frame on top must still be caught.
	truth := page(0xC2)
	corrupt := append([]byte(nil), truth...)
	corrupt[7] ^= 0x01
	publishTestLTX(t, be, objstore.L0Level, 2, 2,
		attestedL0(t, state, map[uint32][]byte{2: truth}, 3, 2, 2, map[uint32][]byte{2: corrupt}))

	dst := filepath.Join(t.TempDir(), "app.db")
	err = MaterializeStream(ctx, be, objstore.DBPrefix,
		&objstore.Baseline{TXID: 1, LTXRef: baseRef}, dst, 0)
	if err == nil {
		t.Fatalf("MaterializeStream adopted a corrupt attested frame over a legacy baseline")
	}
	if !strings.Contains(err.Error(), "post-apply") {
		t.Fatalf("unexpected error: %v", err)
	}
}
