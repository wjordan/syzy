//go:build linux

package lazyrestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/superfly/ltx"
	"golang.org/x/sys/unix"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/physicalrestore"
)

// Lazy-mode sentinel errors. Callers fall back to full restore on
// these; any other error from Prepare is fatal for the attempt.
var (
	// ErrParentTXIDUnstamped reports that metadata.db's
	// meta.parent_app_txid row is absent or zero. Lazy bootstrap
	// would create the cache frontier > pin invariant violation
	// described at internal/nodestate/cache.go's MissingRangesUpTo
	// early return — silent data loss. Caller falls back to full restore
	// Restore (which materializes both files to their tips).
	ErrParentTXIDUnstamped = errors.New("lazyrestore: parent_app_txid unstamped in metadata.db")

	// ErrPageSizeUnaligned reports that the SQLite page size in
	// the baseline LTX is smaller than the backing filesystem's
	// block size. Bitmap rebuild uses SEEK_DATA, which reports
	// presence at block granularity; with page < block, one fetched
	// page would falsely advertise neighboring pages as present.
	// Caller falls back to full restore.
	ErrPageSizeUnaligned = errors.New("lazyrestore: SQLite page size < filesystem block size")

	// ErrPgnoOutOfRange is returned by Manifest.FetchPage when
	// pgno is outside [1, CommitPages]. The FUSE layer surfaces this
	// as EIO so SQLite reports a clean I/O error instead of treating
	// zeros past EOF as data.
	ErrPgnoOutOfRange = errors.New("lazyrestore: page number out of range")

	// ErrPageMissing is returned by Manifest.FetchPage when an
	// in-range pgno is absent from the merged page index. Signals
	// LTX-chain truncation, publisher bug, or index corruption — fail
	// loudly rather than masking as a zero page that SQLite would
	// interpret as structurally empty.
	ErrPageMissing = errors.New("lazyrestore: page absent from manifest")
)

// lazyManifestVersion is the on-disk format tag for serialized
// Manifest sidecars. Bump on any incompatible field change.
//
// v2 (current): Page.Key carries the caller-supplied object prefix so the
// bucket used for preparation may be narrower than the bucket later used for
// page fetches.
const lazyManifestVersion uint32 = 2

// lazyManifestMagic prefixes every serialized manifest so a
// truncated or unrelated file is rejected with a clear error
// instead of producing a malformed gob deserialization.
var lazyManifestMagic = [4]byte{'S', 'Y', 'Z', 'L'}

// Manifest is the result of a lazy bootstrap. Keys in Pages
// are qualified with the keyPrefix passed to Prepare /
// buildPageMap; FetchPage resolves them against a bucket view
// in which `keyPrefix + relative-key` is the full object path.
// keyPrefix may be empty when FetchPage uses the same bucket passed to Prepare.
//
// Immutable after construction. FetchPage reads Pages with no
// synchronization; callers that need to simulate corruption
// (e.g. drop an entry to provoke ErrPageMissing) MUST clone the
// manifest first. Mutating a live manifest while another goroutine
// is faulting pages is a data race.
type Manifest struct {
	// PageSize is the SQLite page size in bytes (typ. 4096).
	PageSize uint32
	// CommitPages is the database size in pages, taken from the
	// baseline LTX header's Commit field. The sparse backing file
	// is truncated to CommitPages * PageSize at bootstrap.
	CommitPages uint32
	// PinnedTXID is the app-stream TXID that the merged page map
	// resolves at — equal to metadata.db's parent_app_txid at the
	// moment of bootstrap.
	PinnedTXID uint64
	// Pages maps SQLite page number → location of that page's
	// encoded bytes in one of the LTX objects. Keys are qualified
	// with the keyPrefix passed at build time (see Manifest doc).
	Pages map[uint32]Page
}

// Page locates one SQLite page's encoded bytes inside one LTX
// object. Key is qualified by the manifest's keyPrefix (see
// Manifest doc).
type Page struct {
	Key    string // LTX object key, qualified by manifest keyPrefix
	Offset int64  // byte offset in the object
	Size   int64  // encoded length (page header + data)
}

// Prepare initializes a fresh syzy database at dstPath in lazy
// mode. metadata.db is fully restored and identity-adopted; the
// app.db (at dstPath itself) is created as a sparse file of the
// correct size with page 1 pre-faulted. The returned manifest can
// drive on-demand page fetches via FetchPage.
//
// bucket is the view used during preparation. keyPrefix is prepended to every
// stored page key; use it when FetchPage will receive a broader bucket view,
// or pass "" when preparation and page fetches use the same bucket.
//
// Returns (nil, nil) when the bucket has no baseline yet (fresh
// cluster). Returns ErrParentTXIDUnstamped when metadata's
// parent_app_txid is absent/zero — caller should fall back to
// full restore. Returns ErrPageSizeUnaligned when the SQLite
// page size is smaller than the backing filesystem's block size.
// Other errors are fatal for the attempt.
//
// Refuses if dstPath or its metadata dir already exists; the
// caller is responsible for moving any existing files aside.
func Prepare(ctx context.Context, dstPath string, bucket objectstore.Bucket, keyPrefix string) (*Manifest, error) {
	if dstPath == "" {
		return nil, errors.New("lazyrestore: Prepare: empty dstPath")
	}
	if bucket == nil {
		return nil, errors.New("lazyrestore: Prepare: nil bucket")
	}
	metaDir := layout.MetaDir(dstPath)
	if _, err := os.Stat(dstPath); err == nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: %s already exists", dstPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("lazyrestore: Prepare: stat %s: %w", dstPath, err)
	}
	if _, err := os.Stat(metaDir); err == nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: %s already exists", metaDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("lazyrestore: Prepare: stat %s: %w", metaDir, err)
	}

	head, _, err := objstore.LoadHEAD(ctx, bucket)
	if err != nil {
		if errors.Is(err, objstore.ErrNoHEAD) {
			return nil, nil
		}
		return nil, fmt.Errorf("lazyrestore: Prepare: load HEAD: %w", err)
	}
	if head.Baseline == nil {
		return nil, nil
	}
	if head.MetaBaseline == nil {
		// Lazy preparation needs the metadata LTX stream and its
		// parent_app_txid pin. Full restore handles buckets without it.
		return nil, fmt.Errorf("lazyrestore: Prepare: HEAD has no metadata baseline: %w", ErrParentTXIDUnstamped)
	}

	// Stage everything in a sibling .tmp dir on the same filesystem
	// as the final destination so the closing rename is atomic.
	// MkdirTemp under $TMPDIR could land on a different mount and
	// fail with EXDEV on rename.
	parentDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(parentDir, 0o700); err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: mkdir parent: %w", err)
	}
	tmpAppPath := dstPath + ".tmp"
	tmpMetaDir := metaDir + ".tmp"
	if err := os.RemoveAll(tmpMetaDir); err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: clear stage meta dir: %w", err)
	}
	if err := os.Remove(tmpAppPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("lazyrestore: Prepare: clear stage app file: %w", err)
	}
	if err := os.MkdirAll(tmpMetaDir, 0o755); err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: mkdir stage meta dir: %w", err)
	}
	ok := false
	defer func() {
		if ok {
			return
		}
		_ = os.RemoveAll(tmpMetaDir)
		_ = os.Remove(tmpAppPath)
	}()

	// 1. Read baseline header so we know the page size before any
	//    further work. Speculative 100-byte GET; we can re-fetch
	//    the trailer separately.
	pageSize, commit, err := readLTXHeader(ctx, bucket, head.Baseline.LTXRef.Key)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: read baseline header: %w", err)
	}
	if !ltx.IsValidPageSize(pageSize) {
		return nil, fmt.Errorf("lazyrestore: Prepare: invalid page size %d", pageSize)
	}
	if commit == 0 {
		return nil, errors.New("lazyrestore: Prepare: baseline commit pages is zero")
	}

	// 2. Page-size vs filesystem-block-size alignment. Stat the
	//    staged tmp meta dir — it exists by now and shares a
	//    filesystem with dstPath.
	var stfs unix.Statfs_t
	if err := unix.Statfs(tmpMetaDir, &stfs); err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: statfs %s: %w", tmpMetaDir, err)
	}
	if int64(pageSize) < stfs.Bsize {
		return nil, fmt.Errorf("lazyrestore: Prepare: page_size=%d fs_block_size=%d: %w",
			pageSize, stfs.Bsize, ErrPageSizeUnaligned)
	}

	// 3. Restore metadata.db fully (baseline + L0/L1 chain through
	//    its tip). Reuses the full-restore helper.
	stagedMetaDB := filepath.Join(tmpMetaDir, "metadata.db")
	if err := physicalrestore.MaterializeStream(ctx, bucket, objstore.MetadataPrefix, head.MetaBaseline, stagedMetaDB, 0); err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: restore metadata: %w", err)
	}

	// 4. Read parent_app_txid from staged metadata.db. Absent/zero
	//    → fail with ErrParentTXIDUnstamped so caller falls
	//    back to full restore (which materializes both files to their
	//    tips). The unsafe HEAD.Baseline.TXID fallback would cause
	//    silent data loss: cache frontier seeded from metadata is
	//    ahead of any pin below the metadata tip, so the broker's
	//    MissingRangesUpTo returns nil and the gap-fill planner
	//    never asks for the missing seqs.
	parentAppTX, err := physicalrestore.ParentAppTXID(stagedMetaDB)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: read parent_app_txid: %w", err)
	}
	if parentAppTX == 0 {
		return nil, ErrParentTXIDUnstamped
	}
	pinnedTXID := parentAppTX

	// 5. Build merged page index from baseline + selected L0/L1
	//    chain through pinnedTXID. Keys are qualified with
	//    keyPrefix so the manifest is resolvable against the later fetch
	//    bucket. commit is re-resolved to the merged
	//    state's page count (the chain may have grown/shrunk the
	//    database past the baseline header's value).
	pages, commit, err := buildPageMap(ctx, bucket, keyPrefix, head.Baseline, pinnedTXID, pageSize, commit)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: build page map: %w", err)
	}

	// 6. Identity adoption on the staged metadata.db, matching
	//    clone.Adopt's contract (mint origin, AdoptClone, pre-
	//    create origins/<newOrigin>/journal/). Skipping this would
	//    leave the source's node_id, sender_seq, intent, and
	//    snapshot_markers in the new node.
	newOrigin, err := layout.MintOrigin()
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: mint origin: %w", err)
	}
	now := nowHLC()
	sc, err := metadata.Open(stagedMetaDB)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: open staged metadata: %w", err)
	}
	if err := sc.AdoptClone(newOrigin, now); err != nil {
		_ = sc.Close()
		return nil, fmt.Errorf("lazyrestore: Prepare: AdoptClone: %w", err)
	}
	if err := sc.Close(); err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: close staged metadata: %w", err)
	}
	if err := os.MkdirAll(layout.OriginJournalDirIn(tmpMetaDir, newOrigin), 0o755); err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: mkdir new origin journal: %w", err)
	}

	// 7. Create the sparse app.db file at tmpAppPath with the
	//    correct size. os.Truncate sets size without allocating
	//    blocks — reads of holes return zeros, which is the
	//    behavior we want for unfaulted pages until SEEK_DATA can
	//    distinguish them from real data.
	// 8. Pre-fault page 1 (SQLite header). Without this, sqlite.Open
	//    reads zero bytes for the header and treats the file as
	//    empty, then writes its own fresh header → metadata loss.
	//    The manifest carries keyPrefix-qualified keys; pre-fault
	//    strips the prefix because it reads via the same bucket
	//    buildPageMap used.
	if err := writeSparseAppFile(ctx, bucket, keyPrefix, pages, tmpAppPath, pageSize, commit); err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: %w", err)
	}

	manifest := &Manifest{
		PageSize:    pageSize,
		CommitPages: commit,
		PinnedTXID:  pinnedTXID,
		Pages:       pages,
	}

	// 9. Publish the prepared pair. Metadata moves first; if the app-file
	//    rename fails in-process, remove metadata again. A process crash between
	//    the two renames leaves a detectable partial preparation which the next
	//    call refuses rather than silently treating as valid.
	if err := os.Rename(tmpMetaDir, metaDir); err != nil {
		return nil, fmt.Errorf("lazyrestore: Prepare: rename metadata: %w", err)
	}
	if err := os.Rename(tmpAppPath, dstPath); err != nil {
		_ = os.RemoveAll(metaDir)
		return nil, fmt.Errorf("lazyrestore: Prepare: rename app file (metadata reverted): %w", err)
	}
	ok = true
	return manifest, nil
}

// FetchPage downloads and decodes a single page from this manifest's
// pinned LTX chain. Behavior on absence:
//
//   - pgno == ltx.LockPgno(m.PageSize): returns a zero-filled page
//     of length m.PageSize. Baselines deliberately skip the SQLite
//     lock page (internal/ltxstream/encoder.go), and SQLite expects
//     zeros there per the PENDING_BYTE protocol.
//   - pgno < 1 or pgno > m.CommitPages: ErrPgnoOutOfRange.
//   - pgno in [1, CommitPages] absent from m.Pages: ErrPageMissing
//     (integrity error; signals truncated LTX chain / index corruption).
//
// The returned slice has exactly m.PageSize bytes on success.
func (m *Manifest) FetchPage(ctx context.Context, bucket objectstore.Bucket, pgno uint32) ([]byte, error) {
	if m == nil {
		return nil, errors.New("lazyrestore: Manifest.FetchPage on nil")
	}
	if bucket == nil {
		return nil, errors.New("lazyrestore: Manifest.FetchPage: nil bucket")
	}
	return fetchPageBytes(ctx, bucket, "", m.Pages, m.PageSize, pgno, m.CommitPages)
}

// Save writes the manifest to path atomically (write to .tmp, fsync,
// rename).
func (m *Manifest) Save(path string) (err error) {
	if m == nil {
		return errors.New("lazyrestore: Manifest.Save on nil")
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("lazyrestore: save manifest: %w", err)
		}
	}()

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	// On any failure past this point, drop the partial tmp file.
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	if _, err = f.Write(lazyManifestMagic[:]); err != nil {
		return err
	}
	var versionBuf [4]byte
	binary.BigEndian.PutUint32(versionBuf[:], lazyManifestVersion)
	if _, err = f.Write(versionBuf[:]); err != nil {
		return err
	}
	if err = gob.NewEncoder(f).Encode(m); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}

// LoadManifest reads a manifest previously written by Save.
// Returns a parse error (not a sentinel) on any malformed/truncated
// content — callers should treat that as fatal for the prepared database.
func LoadManifest(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: load manifest: %w", err)
	}
	defer f.Close()
	var head [8]byte
	if _, err := io.ReadFull(f, head[:]); err != nil {
		return nil, fmt.Errorf("lazyrestore: load manifest: read header: %w", err)
	}
	if [4]byte(head[:4]) != lazyManifestMagic {
		return nil, fmt.Errorf("lazyrestore: load manifest: bad magic %x", head[:4])
	}
	ver := binary.BigEndian.Uint32(head[4:])
	if ver != lazyManifestVersion {
		return nil, fmt.Errorf("lazyrestore: load manifest: unsupported version %d (want %d)", ver, lazyManifestVersion)
	}
	var m Manifest
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("lazyrestore: load manifest: decode: %w", err)
	}
	if m.PageSize == 0 || m.CommitPages == 0 || m.Pages == nil {
		return nil, errors.New("lazyrestore: load manifest: zero/nil required field")
	}
	return &m, nil
}

// readLTXHeader fetches the first HeaderSize bytes of an LTX object
// and returns (page size, commit pages).
func readLTXHeader(ctx context.Context, bucket objectstore.Bucket, key string) (pageSize uint32, commit uint32, err error) {
	rc, err := bucket.GetRange(ctx, key, 0, int64(ltx.HeaderSize))
	if err != nil {
		return 0, 0, fmt.Errorf("get header range for %s: %w", key, err)
	}
	defer rc.Close()
	buf := make([]byte, ltx.HeaderSize)
	if _, err := io.ReadFull(rc, buf); err != nil {
		return 0, 0, fmt.Errorf("read header bytes for %s: %w", key, err)
	}
	var hdr ltx.Header
	if err := hdr.UnmarshalBinary(buf); err != nil {
		return 0, 0, fmt.Errorf("decode header for %s: %w", key, err)
	}
	return hdr.PageSize, hdr.Commit, nil
}

// buildPageMap returns the merged pgno → location map: start
// from the baseline at level 0009, overlay the L0/L1 chain through
// pinnedTXID. Newest LTX wins per pgno; only the chosen entries are
// recorded (not the entire history).
//
// bucket is the view used for object-store reads. keyPrefix is prepended to
// every Page.Key so the manifest can later resolve against a broader bucket.
// Pass "" to keep keys bucket-relative.
//
// The returned commit is the merged state's database size in pages:
// the Commit of the newest applied chain LTX, or the baseline's when
// no chain applies. Callers MUST size the sparse file and manifest
// from this value, not the baseline header's — a chain that grew (or
// shrank) the database otherwise yields a file whose size contradicts
// the merged page 1's header, which SQLite reports as a malformed
// database image.
func buildPageMap(
	ctx context.Context,
	bucket objectstore.Bucket,
	keyPrefix string,
	baseline *objstore.Baseline,
	pinnedTXID uint64,
	pageSize uint32,
	baselineCommit uint32,
) (map[uint32]Page, uint32, error) {
	// Per-LTX page index from the baseline.
	pages := map[uint32]Page{}
	if err := overlayLTXIndex(ctx, bucket, baseline.LTXRef.Key, pages); err != nil {
		return nil, 0, fmt.Errorf("overlay baseline %s: %w", baseline.LTXRef.Key, err)
	}
	commit := baselineCommit
	// Chain: only L0/L1 LTXes with MinTXID > baseline.TXID and
	// MaxTXID <= pinnedTXID. selectLTXChain orders them so that
	// applying newest-wins yields the same merged state the full restore
	// path would build by sequential apply.
	if pinnedTXID > baseline.TXID {
		l0, err := objstore.ListLTX(ctx, bucket, objstore.DBPrefix, objstore.L0Level)
		if err != nil {
			return nil, 0, fmt.Errorf("list L0: %w", err)
		}
		l1, err := objstore.ListLTX(ctx, bucket, objstore.DBPrefix, objstore.L1Level)
		if err != nil {
			return nil, 0, fmt.Errorf("list L1: %w", err)
		}
		chain := physicalrestore.SelectLTXChain(l0, l1, baseline.TXID, pinnedTXID)
		for _, e := range chain {
			if err := overlayLTXIndex(ctx, bucket, e.Key, pages); err != nil {
				return nil, 0, fmt.Errorf("overlay %s: %w", e.Key, err)
			}
		}
		// The newest chain element's header carries the merged
		// state's page count (chain is sorted ascending).
		if len(chain) > 0 {
			last := chain[len(chain)-1]
			_, chainCommit, err := readLTXHeader(ctx, bucket, last.Key)
			if err != nil {
				return nil, 0, fmt.Errorf("read chain tip header %s: %w", last.Key, err)
			}
			if chainCommit == 0 {
				return nil, 0, fmt.Errorf("chain tip %s has zero commit", last.Key)
			}
			commit = chainCommit
		}
	}
	// A shrinking chain (VACUUM/truncate) leaves stale entries above
	// the merged commit; drop them so the manifest never points past
	// the file end.
	for pgno := range pages {
		if pgno > commit {
			delete(pages, pgno)
		}
	}
	// Sanity-check coverage. Every in-range pgno except the lock
	// page must be present; missing entries here would surface as
	// ErrPageMissing on FetchPage later, but it's better to fail
	// loudly at bootstrap.
	lockPgno := ltx.LockPgno(pageSize)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if pgno == lockPgno {
			continue
		}
		if _, ok := pages[pgno]; !ok {
			return nil, 0, fmt.Errorf("merged page index missing pgno %d (commit=%d, baseline=%s)",
				pgno, commit, baseline.LTXRef.Key)
		}
	}
	// Qualify every key with keyPrefix. Done at the end so the
	// internal overlay/merge loop stays simple (bucket-relative
	// keys throughout). Empty prefix is a no-op.
	if keyPrefix != "" {
		for pgno, loc := range pages {
			loc.Key = keyPrefix + loc.Key
			pages[pgno] = loc
		}
	}
	return pages, commit, nil
}

// overlayLTXIndex fetches key's page index trailer, decodes it, and
// merges the entries into out (newest-wins; caller orders calls).
//
// Strategy: speculative tail read for the index + trailer, fall
// back to a full GET if the tail isn't large enough. The trailer's
// final 8 bytes (after the index) carry the index size; everything
// before that within the tail is the index itself.
func overlayLTXIndex(ctx context.Context, bucket objectstore.Bucket, key string, out map[uint32]Page) error {
	// Need at least the trailer + a few bytes of index sentinel.
	// Start with a generous tail; LTX page index is one uvarint
	// triple per page so even 64K covers ~10K pages of index.
	const initialTail = 64 * 1024
	tailLen := int64(initialTail)
	info, err := bucket.Stat(ctx, key)
	if err == nil && info.Size > 0 && info.Size < tailLen {
		tailLen = info.Size
	}
	for {
		idx, err := readLTXPageIndex(ctx, bucket, key, tailLen)
		if err == nil {
			for pgno, elem := range idx {
				out[pgno] = Page{
					Key:    key,
					Offset: elem.Offset,
					Size:   elem.Size,
				}
			}
			return nil
		}
		// If the tail wasn't big enough to cover the index, try
		// the full object.
		if errors.Is(err, errTailTooSmall) && info.Size > tailLen {
			tailLen = info.Size
			continue
		}
		return err
	}
}

var errTailTooSmall = errors.New("ltx tail too small to cover index")

// readLTXPageIndex pulls the last tailLen bytes of the LTX object
// at key, decodes the page index trailer, and returns the parsed
// index. Returns errTailTooSmall if the tail starts mid-index;
// caller should retry with the full object.
//
// Layout of the tail bytes from end:
//
//	[ ... page index payload ... | indexSize (8B BE) | trailer (16B) ]
//
// DecodePageIndex uses MinTXID/MaxTXID only to populate fields we
// don't carry into Page, so zero placeholders are fine.
func readLTXPageIndex(ctx context.Context, bucket objectstore.Bucket, key string, tailLen int64) (map[uint32]ltx.PageIndexElem, error) {
	rc, err := bucket.GetRange(ctx, key, -tailLen, tailLen)
	if err != nil {
		return nil, fmt.Errorf("get range %s: %w", key, err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, fmt.Errorf("read range %s: %w", key, err)
	}
	if int64(len(body)) < int64(ltx.TrailerSize)+8 {
		return nil, errTailTooSmall
	}
	sizeOff := len(body) - ltx.TrailerSize - 8
	indexSize := binary.BigEndian.Uint64(body[sizeOff : sizeOff+8])
	if indexSize > uint64(sizeOff) {
		// Tail doesn't contain the full index; caller should
		// re-fetch with a larger window (typically the entire
		// object).
		return nil, errTailTooSmall
	}
	indexStart := sizeOff - int(indexSize)
	// bytes.Reader satisfies io.Reader + io.ByteReader, which is
	// the input ltx.DecodePageIndex consumes (varints + a trailing
	// BigEndian uint64).
	idx, err := ltx.DecodePageIndex(bytes.NewReader(body[indexStart:]), 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("decode page index %s: %w", key, err)
	}
	return idx, nil
}

// fetchPageBytes does the actual range-GET + decode for one page.
// stripPrefix is removed from loc.Key before reading via bucket —
// used by Prepare's page-1 pre-fault to consume keyPrefix-qualified manifest
// entries through the preparation bucket.
// Manifest.FetchPage passes "" (the caller's bucket already
// matches qualified keys).
func fetchPageBytes(
	ctx context.Context,
	bucket objectstore.Bucket,
	stripPrefix string,
	pages map[uint32]Page,
	pageSize uint32,
	pgno uint32,
	commit uint32,
) ([]byte, error) {
	if pgno < 1 || pgno > commit {
		return nil, fmt.Errorf("%w: pgno=%d commit=%d", ErrPgnoOutOfRange, pgno, commit)
	}
	if pgno == ltx.LockPgno(pageSize) {
		return make([]byte, pageSize), nil
	}
	loc, ok := pages[pgno]
	if !ok {
		return nil, fmt.Errorf("%w: pgno=%d", ErrPageMissing, pgno)
	}
	readKey := loc.Key
	if stripPrefix != "" {
		if len(readKey) < len(stripPrefix) || readKey[:len(stripPrefix)] != stripPrefix {
			return nil, fmt.Errorf("fetch page %d: key %q missing expected prefix %q", pgno, readKey, stripPrefix)
		}
		readKey = readKey[len(stripPrefix):]
	}
	rc, err := bucket.GetRange(ctx, readKey, loc.Offset, loc.Size)
	if err != nil {
		return nil, fmt.Errorf("fetch page %d from %s @%d+%d: %w", pgno, readKey, loc.Offset, loc.Size, err)
	}
	raw, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, fmt.Errorf("read page %d from %s: %w", pgno, readKey, err)
	}
	hdr, data, err := ltx.DecodePageData(raw)
	if err != nil {
		return nil, fmt.Errorf("decode page %d from %s: %w", pgno, readKey, err)
	}
	if hdr.Pgno != pgno {
		return nil, fmt.Errorf("decoded pgno mismatch for %s @%d: got %d, want %d", readKey, loc.Offset, hdr.Pgno, pgno)
	}
	if uint32(len(data)) != pageSize {
		return nil, fmt.Errorf("decoded page %d wrong size: got %d, want %d", pgno, len(data), pageSize)
	}
	return data, nil
}

// nowHLC mirrors clone.nowHLC: wall-clock millisecond as an HLC
// with logical=0. metadata.AdoptClone uses this to ensure hlc_last
// can't regress relative to the host's clock.
func nowHLC() crdt.Clock { return crdt.Clock{WallTime: time.Now().UnixMilli(), Logical: 0} }

// writeSparseAppFile creates dstPath as a sparse file of
// commit*pageSize bytes, pre-faults page 1 (the SQLite header), and
// fsyncs. Used by Prepare as its single side-effecting step on
// the app file. stripPrefix is passed through to fetchPageBytes so
// keyPrefix-qualified manifest entries resolve via the preparation bucket;
// pass "" when manifest keys are bucket-relative.
func writeSparseAppFile(ctx context.Context, bucket objectstore.Bucket, stripPrefix string, pages map[uint32]Page, dstPath string, pageSize, commit uint32) (err error) {
	f, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create app file: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close app file: %w", cerr)
		}
	}()
	if err := f.Truncate(int64(commit) * int64(pageSize)); err != nil {
		return fmt.Errorf("truncate app file: %w", err)
	}
	page1Bytes, err := fetchPageBytes(ctx, bucket, stripPrefix, pages, pageSize, 1, commit)
	if err != nil {
		return fmt.Errorf("fetch page 1: %w", err)
	}
	if _, err := f.WriteAt(page1Bytes, 0); err != nil {
		return fmt.Errorf("write page 1: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync app file: %w", err)
	}
	return nil
}
