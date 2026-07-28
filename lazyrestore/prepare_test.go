//go:build linux

package lazyrestore_test

import (
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/lazyrestore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

type legacyLazyManifest struct {
	PageSize    uint32
	CommitPages uint32
	PinnedTXID  uint64
	Pages       map[uint32]legacyLazyPage
}

type legacyLazyPage struct {
	Key    string
	Offset int64
	Size   int64
}

// publishSourceBucket opens a node against bucket, writes a few
// commits, calls PublishSnapshot (which fires the publisher-coupled
// snapshotter and stamps parent_app_txid into metadata.db), and
// closes. The bucket ends up with HEAD.{Baseline,MetaBaseline} set
// and metadata.db at the meta baseline carrying parent_app_txid.
func publishSourceBucket(t *testing.T, bucket objectstore.Bucket, dbPath string) {
	t.Helper()
	ctx := context.Background()
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		ObjectBackend: bucket,
		SchemaLog:     schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open source: %v", err)
	}
	if err := node.Exec(`CREATE TABLE notes (id INT PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
		_ = node.Close()
		t.Fatalf("CREATE: %v", err)
	}
	for i := 1; i <= 8; i++ {
		body := "row-" + string(rune('A'+i-1))
		stmt := fmt.Sprintf(`INSERT INTO notes VALUES (%d, '%s')`, i, body)
		if err := node.Exec(stmt); err != nil {
			_ = node.Close()
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	// Wait for publisher lease so PublishSnapshot's AllocAppTXID
	// has a seeded counter.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h, _, err := objstore.LoadHEAD(ctx, bucket)
		if err == nil && h.Publisher != nil && h.Baseline != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := node.PublishSnapshot(ctx); err != nil {
		_ = node.Close()
		t.Fatalf("PublishSnapshot: %v", err)
	}
	// One more commit + PublishSnapshot. The first PublishSnapshot
	// pinned its read tx before snapshotter wrote parent_app_txid,
	// so its meta-baseline may not carry the stamp. The second
	// PublishSnapshot's snapshot tx happens after the first one's
	// stamp landed.
	if err := node.Exec(`INSERT INTO notes VALUES (99, 'x')`); err != nil {
		_ = node.Close()
		t.Fatalf("INSERT tail: %v", err)
	}
	if err := node.PublishSnapshot(ctx); err != nil {
		_ = node.Close()
		t.Fatalf("PublishSnapshot 2: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close source: %v", err)
	}
}

func TestPrepare_NoHEAD_ReturnsNil(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "shared.db")
	m, err := lazyrestore.Prepare(context.Background(), dst, be, "")
	if err != nil {
		t.Fatalf("Prepare on empty bucket: %v", err)
	}
	if m != nil {
		t.Fatalf("Prepare on empty bucket: manifest = %+v, want nil", m)
	}
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Prepare on empty bucket left dst behind: %v", err)
	}
}

func TestPrepare_RoundTrip(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	srcDB := filepath.Join(t.TempDir(), "src.db")
	publishSourceBucket(t, be, srcDB)

	dstDir := t.TempDir()
	dst := filepath.Join(dstDir, "shared.db")
	manifest, err := lazyrestore.Prepare(context.Background(), dst, be, "")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if manifest == nil {
		t.Fatalf("Prepare returned nil manifest from populated bucket")
	}
	if manifest.PageSize == 0 || manifest.CommitPages == 0 || manifest.PinnedTXID == 0 {
		t.Fatalf("manifest fields zero: %+v", manifest)
	}
	if len(manifest.Pages) == 0 {
		t.Fatalf("manifest has empty pages map")
	}

	// metadata.db must exist with adopted identity.
	metaPath := filepath.Join(dstDir, "shared.db-syzy", "metadata.db")
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("metadata.db missing: %v", err)
	}
	// shared.db exists with the expected size; page 1 is non-hole;
	// every other page is a hole (no SEEK_DATA hits past page 1).
	st, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat dst: %v", err)
	}
	wantSize := int64(manifest.CommitPages) * int64(manifest.PageSize)
	if st.Size() != wantSize {
		t.Fatalf("shared.db size = %d, want %d", st.Size(), wantSize)
	}
	f, err := os.Open(dst)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	// SEEK_DATA from offset 0 should return 0 (page 1 is data).
	const SEEK_DATA = 3
	const SEEK_HOLE = 4
	pos, err := f.Seek(0, SEEK_DATA)
	if err != nil || pos != 0 {
		t.Fatalf("SEEK_DATA from 0: pos=%d err=%v", pos, err)
	}
	// SEEK_HOLE from end of page 1 should be at or before page 1's end
	// (page 1 is the only fault we forced; everything past it is hole).
	pageSize := int64(manifest.PageSize)
	hole, err := f.Seek(pageSize, SEEK_HOLE)
	if err != nil {
		t.Fatalf("SEEK_HOLE from %d: %v", pageSize, err)
	}
	if hole > pageSize {
		t.Errorf("expected hole at or before %d, got %d", pageSize, hole)
	}

	// FetchPage of lock page returns zeros of length PageSize.
	// Only meaningful when the DB is large enough to contain the
	// lock page (PENDING_BYTE / pageSize + 1 pages). Tiny test DBs
	// have a lock pgno far past commit; for those, FetchPage on
	// any pgno >= lockPgno is correctly ErrPgnoOutOfRange.
	lockPgno := ltx.LockPgno(manifest.PageSize)
	if lockPgno <= manifest.CommitPages {
		got, err := manifest.FetchPage(context.Background(), be, lockPgno)
		if err != nil {
			t.Fatalf("FetchPage lock: %v", err)
		}
		if uint32(len(got)) != manifest.PageSize {
			t.Errorf("FetchPage lock: len=%d, want %d", len(got), manifest.PageSize)
		}
		for i, b := range got {
			if b != 0 {
				t.Errorf("FetchPage lock: byte %d = %d, want 0", i, b)
				break
			}
		}
	}

	// FetchPage of out-of-range pgno returns ErrPgnoOutOfRange.
	if _, err := manifest.FetchPage(context.Background(), be, 0); !errors.Is(err, lazyrestore.ErrPgnoOutOfRange) {
		t.Errorf("FetchPage pgno=0: %v, want ErrPgnoOutOfRange", err)
	}
	if _, err := manifest.FetchPage(context.Background(), be, manifest.CommitPages+1); !errors.Is(err, lazyrestore.ErrPgnoOutOfRange) {
		t.Errorf("FetchPage pgno>commit: %v, want ErrPgnoOutOfRange", err)
	}

	// FetchPage of page 1: bytes start with the SQLite magic.
	page1, err := manifest.FetchPage(context.Background(), be, 1)
	if err != nil {
		t.Fatalf("FetchPage 1: %v", err)
	}
	if string(page1[0:16]) != "SQLite format 3\x00" {
		t.Errorf("page 1 magic mismatch: got %q", page1[0:16])
	}

	// In-range absent → ErrPageMissing. Clone the manifest first so
	// we don't violate the "immutable after construction" contract
	// on the live one.
	corrupt := &lazyrestore.Manifest{
		PageSize:    manifest.PageSize,
		CommitPages: manifest.CommitPages,
		PinnedTXID:  manifest.PinnedTXID,
		Pages:       make(map[uint32]lazyrestore.Page, len(manifest.Pages)),
	}
	for p, loc := range manifest.Pages {
		corrupt.Pages[p] = loc
	}
	delPgno := uint32(0)
	for p := range corrupt.Pages {
		if p > delPgno && p <= corrupt.CommitPages && p != lockPgno {
			delPgno = p
		}
	}
	if delPgno == 0 {
		t.Fatalf("could not pick page to drop; manifest had no in-range entries")
	}
	delete(corrupt.Pages, delPgno)
	if _, err := corrupt.FetchPage(context.Background(), be, delPgno); !errors.Is(err, lazyrestore.ErrPageMissing) {
		t.Errorf("FetchPage after deleting page %d: %v, want ErrPageMissing", delPgno, err)
	}
}

// ErrParentTXIDUnstamped is now a corruption-only safety net —
// the snapshotter stamps parent_app_txid on every tx (including the
// final tx during Close), so a HEAD with baseline+metabaseline but no
// stamp isn't reachable through normal operation. Reproducing the
// state requires surgical edits to the in-bucket meta baseline LTX,
// which is more cost than value for a defensive check.

func TestPrepare_RefusesExistingDst(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "shared.db")
	if err := os.WriteFile(dst, []byte("preexisting"), 0o644); err != nil {
		t.Fatalf("seed dst: %v", err)
	}
	_, err = lazyrestore.Prepare(context.Background(), dst, be, "")
	if err == nil {
		t.Fatalf("Prepare: nil error, want refusal")
	}
}

func TestPrepare_ManifestRoundTrip(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	srcDB := filepath.Join(t.TempDir(), "src.db")
	publishSourceBucket(t, be, srcDB)

	dst := filepath.Join(t.TempDir(), "shared.db")
	m, err := lazyrestore.Prepare(context.Background(), dst, be, "")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := m.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	back, err := lazyrestore.LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if back.PageSize != m.PageSize || back.CommitPages != m.CommitPages || back.PinnedTXID != m.PinnedTXID {
		t.Fatalf("scalar mismatch: got %+v want %+v", back, m)
	}
	if len(back.Pages) != len(m.Pages) {
		t.Fatalf("Pages len mismatch: got %d want %d", len(back.Pages), len(m.Pages))
	}
	for p, want := range m.Pages {
		got, ok := back.Pages[p]
		if !ok {
			t.Fatalf("page %d missing after round-trip", p)
		}
		if got != want {
			t.Fatalf("page %d differs: got %+v want %+v", p, got, want)
		}
	}
}

func TestLoadManifest_LegacyV2TypeNames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.Write([]byte("SYZL")); err != nil {
		t.Fatalf("write magic: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint32(2)); err != nil {
		t.Fatalf("write version: %v", err)
	}
	want := legacyLazyManifest{
		PageSize:    4096,
		CommitPages: 2,
		PinnedTXID:  7,
		Pages: map[uint32]legacyLazyPage{
			1: {Key: "app/db/0009/base.ltx", Offset: 100, Size: 4096},
			2: {Key: "app/db/0000/delta.ltx", Offset: 200, Size: 512},
		},
	}
	if err := gob.NewEncoder(f).Encode(&want); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got, err := lazyrestore.LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if got.PageSize != want.PageSize || got.CommitPages != want.CommitPages || got.PinnedTXID != want.PinnedTXID {
		t.Fatalf("scalars = %+v, want %+v", got, want)
	}
	for pgno, page := range want.Pages {
		if gotPage := got.Pages[pgno]; gotPage.Key != page.Key || gotPage.Offset != page.Offset || gotPage.Size != page.Size {
			t.Fatalf("page %d = %+v, want %+v", pgno, gotPage, page)
		}
	}
}

func TestLoadManifest_BadMagic(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := os.WriteFile(path, []byte("NOTAGOOD"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := lazyrestore.LoadManifest(path); err == nil {
		t.Fatalf("LoadManifest on bad-magic file: nil error")
	}
}
