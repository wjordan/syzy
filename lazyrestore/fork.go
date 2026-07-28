//go:build linux

package lazyrestore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/superfly/ltx"
	"golang.org/x/sys/unix"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/physicalrestore"
	syzy "github.com/wjordan/syzy/sqlite"
)

const ManifestFilename = ".lazy-manifest.bin"

// ForkConfig parameterizes Fork. SourcePrefix scopes Bucket to the source's
// object-store layout. SourceNode is optional: when set, Fork publishes a
// snapshot before reading HEAD; otherwise it forks the latest durable state.
//
// DestinationDir is populated from scratch and must not already exist.
// DatabaseName is the database filename within it. Fork always mints a fresh
// origin and cluster identity for the destination.
type ForkConfig struct {
	SourcePrefix string
	Bucket       objectstore.Bucket
	SourceNode   *syzy.Node

	DestinationDir string
	DatabaseName   string
}

// Fork prepares a destination directory from the source's published baseline.
// It contains the sparse database, adopted metadata, a fresh-origin journal,
// and ManifestFilename. The directory does not exist on failure.
//
// It returns nil when the source has no published state; otherwise the returned
// manifest carries the pinned TXID and source-qualified page keys. The
// destination directory is not created on failure.
//
// Fork does not start a syzy.Node against the destination.
func Fork(ctx context.Context, cfg ForkConfig) (*Manifest, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	dstOrigin, err := layout.MintOrigin()
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: mint origin: %w", err)
	}
	var dstClusterID crdt.ClusterID
	if _, err := rand.Read(dstClusterID[:]); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: mint cluster_id: %w", err)
	}

	// Reads use a source-scoped view; manifest keys retain SourcePrefix so
	// FetchPage can resolve them against the original bucket.
	srcBucket := objectstore.Prefixed(cfg.Bucket, cfg.SourcePrefix)

	// Pin the source. PublishSnapshot is a synchronous push that
	// stamps parent_app_txid into the source's metadata.db at the
	// boundary we read. Without SourceNode, use the durable HEAD as-is.
	if cfg.SourceNode != nil {
		if err := cfg.SourceNode.PublishSnapshot(ctx); err != nil {
			return nil, fmt.Errorf("lazyrestore: Fork: PublishSnapshot: %w", err)
		}
	}

	head, _, err := objstore.LoadHEAD(ctx, srcBucket)
	if err != nil {
		if errors.Is(err, objstore.ErrNoHEAD) {
			return nil, nil
		}
		return nil, fmt.Errorf("lazyrestore: Fork: load HEAD: %w", err)
	}
	if head.Baseline == nil || head.MetaBaseline == nil {
		// No published persistence yet; nothing to inherit.
		return nil, nil
	}

	// Stage the destination as a sibling .tmp directory so the
	// closing rename(2) is atomic. Refuse if either path already
	// exists; the caller is responsible for prior cleanup.
	if _, err := os.Stat(cfg.DestinationDir); err == nil {
		return nil, fmt.Errorf("lazyrestore: Fork: %s already exists", cfg.DestinationDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("lazyrestore: Fork: stat dst: %w", err)
	}
	parentDir := filepath.Dir(cfg.DestinationDir)
	if err := os.MkdirAll(parentDir, 0o700); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: mkdir parent: %w", err)
	}
	tmpSlotDir := cfg.DestinationDir + ".tmp"
	if err := os.RemoveAll(tmpSlotDir); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: clear stage: %w", err)
	}
	if err := os.MkdirAll(tmpSlotDir, 0o700); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: mkdir stage: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(tmpSlotDir)
		}
	}()

	tmpSharedDB := filepath.Join(tmpSlotDir, cfg.DatabaseName)
	tmpMetaDir := layout.MetaDir(tmpSharedDB)
	if err := os.MkdirAll(tmpMetaDir, 0o755); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: mkdir meta dir: %w", err)
	}

	// Read the source baseline header to learn page size + commit.
	pageSize, commit, err := readLTXHeader(ctx, srcBucket, head.Baseline.LTXRef.Key)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: read baseline header: %w", err)
	}
	if !ltx.IsValidPageSize(pageSize) {
		return nil, fmt.Errorf("lazyrestore: Fork: invalid page size %d", pageSize)
	}
	if commit == 0 {
		return nil, errors.New("lazyrestore: Fork: baseline commit pages is zero")
	}

	// Page-size vs filesystem-block-size alignment: bitmap rebuild
	// uses SEEK_DATA at block granularity. Same constraint as
	// Prepare.
	var stfs unix.Statfs_t
	if err := unix.Statfs(tmpMetaDir, &stfs); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: statfs %s: %w", tmpMetaDir, err)
	}
	if int64(pageSize) < stfs.Bsize {
		return nil, fmt.Errorf("lazyrestore: Fork: page_size=%d fs_block_size=%d: %w",
			pageSize, stfs.Bsize, ErrPageSizeUnaligned)
	}

	// Restore the source metadata stream into the staged slot. The
	// resulting metadata.db carries the source's catalog, row
	// clocks, applied_gaps, and parent_app_txid stamp.
	stagedMetaDB := filepath.Join(tmpMetaDir, "metadata.db")
	if err := physicalrestore.MaterializeStream(ctx, srcBucket, objstore.MetadataPrefix, head.MetaBaseline, stagedMetaDB, 0); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: restore metadata: %w", err)
	}

	pinnedTXID, err := physicalrestore.ParentAppTXID(stagedMetaDB)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: read parent_app_txid: %w", err)
	}
	if pinnedTXID == 0 {
		return nil, ErrParentTXIDUnstamped
	}

	// AdoptFork the staged metadata: new cluster_id + origin, fresh
	// schema-log namespace, wiped peer frontiers, preserved
	// catalog/row_clock/parent_app_txid.
	sc, err := metadata.Open(stagedMetaDB)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: open staged metadata: %w", err)
	}
	if err := sc.AdoptFork(dstOrigin, dstClusterID, nowHLC()); err != nil {
		_ = sc.Close()
		return nil, fmt.Errorf("lazyrestore: Fork: AdoptFork: %w", err)
	}
	if err := sc.Close(); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: close staged metadata: %w", err)
	}
	if err := os.MkdirAll(layout.OriginJournalDirIn(tmpMetaDir, dstOrigin), 0o755); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: mkdir new origin journal: %w", err)
	}

	// Build merged page map from the source's prefix at the pinned
	// TXID. Keys are qualified with SourcePrefix so FetchPage against
	// Bucket resolves them. commit
	// is re-resolved to the merged state's page count (the chain may
	// have grown/shrunk the database past the baseline header's value).
	pages, commit, err := buildPageMap(ctx, srcBucket, cfg.SourcePrefix, head.Baseline, pinnedTXID, pageSize, commit)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: build page map: %w", err)
	}

	// Create the sparse database. writeSparseAppFile strips the
	// SourcePrefix from manifest keys before reading because it
	// operates through the prefixed srcBucket.
	if err := writeSparseAppFile(ctx, srcBucket, cfg.SourcePrefix, pages, tmpSharedDB, pageSize, commit); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: %w", err)
	}

	manifest := &Manifest{
		PageSize:    pageSize,
		CommitPages: commit,
		PinnedTXID:  pinnedTXID,
		Pages:       pages,
	}
	if err := manifest.Save(filepath.Join(tmpSlotDir, ManifestFilename)); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: save manifest: %w", err)
	}

	// Atomic rename: stage → DestinationDir.
	if err := os.Rename(tmpSlotDir, cfg.DestinationDir); err != nil {
		return nil, fmt.Errorf("lazyrestore: Fork: rename slot: %w", err)
	}
	ok = true
	return manifest, nil
}

func (cfg ForkConfig) validate() error {
	if cfg.Bucket == nil {
		return errors.New("lazyrestore: Fork: Bucket required")
	}
	if cfg.SourcePrefix == "" {
		return errors.New("lazyrestore: Fork: SourcePrefix required")
	}
	if cfg.DestinationDir == "" {
		return errors.New("lazyrestore: Fork: DestinationDir required")
	}
	if cfg.DatabaseName == "" {
		return errors.New("lazyrestore: Fork: DatabaseName required")
	}
	if filepath.Base(cfg.DatabaseName) != cfg.DatabaseName || cfg.DatabaseName == "." {
		return errors.New("lazyrestore: Fork: DatabaseName must be a filename")
	}
	return nil
}
