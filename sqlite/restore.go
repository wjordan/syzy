package sqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/clone"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/physicalrestore"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
	"github.com/wjordan/syzy/transport"
)

// ErrNoSources is returned by Restore when called with no source URLs.
var ErrNoSources = errors.New("syzy: Restore: no sources provided")

// Restore initializes a fresh syzy database at dstPath by adopting a
// clone bundle from the first reachable source. Sources are tried in
// the given order; the first to produce a usable bundle wins.
//
// Recognized URL schemes (matches `cmd/syzy clone <src>`):
//
//   - tcp://host:port
//     Dial a running daemon's bundle endpoint. Live; the source
//     produces a writer-barrier-pinned bundle.
//
//   - s3://bucket/prefix?region=…&endpoint=…
//     Pull the HEAD-pinned snapshot from object storage. Falls
//     through (treated as "no usable bundle") when HEAD has
//     Snapshot:nil — the bucket is in cluster_id-beacon-only state
//     with nothing to restore yet.
//
//   - file:///abs/path
//     Pull from a local FileBackend (testing/dev).
//
// On success returns nil. With no sources returns ErrNoSources. When
// every source fails, returns errors.Join of the per-source failures.
//
// Refuses if dstPath or its metadata dir already exists; the caller is
// responsible for moving any existing files aside.
//
// Credentials: AWS calls use the SDK default credential chain (env,
// shared config, IAM role, IMDSv2). URLs must not embed credentials.
func Restore(ctx context.Context, dstPath string, sources ...string) error {
	return RestoreWith(ctx, dstPath, RestoreOptions{}, sources...)
}

// RestoreOptions extends Restore for callers that plug in their own
// transport.
type RestoreOptions struct {
	// Fetchers maps additional URL schemes to bundle fetchers, letting
	// a custom transport serve clone-from-peer through its own dial
	// path (its serve side registers via transport.BundleSource). The
	// built-in schemes (tcp, unix, s3, file) are always available; an
	// entry for one of those overrides the built-in.
	Fetchers map[string]transport.BundleFetcher
}

// RestoreWith is Restore with explicit options.
func RestoreWith(ctx context.Context, dstPath string, opts RestoreOptions, sources ...string) error {
	if len(sources) == 0 {
		return ErrNoSources
	}
	var errs []error
	for _, src := range sources {
		if err := restoreOne(ctx, dstPath, src, opts); err == nil {
			return nil
		} else {
			errs = append(errs, fmt.Errorf("%s: %w", src, err))
		}
	}
	return errors.Join(errs...)
}

// restoreOne dispatches by URL scheme and pipes a single bundle into
// clone.Adopt at dstPath.
func restoreOne(ctx context.Context, dstPath, src string, opts RestoreOptions) error {
	r, cleanup, err := openRestoreSource(ctx, src, opts)
	if err != nil {
		return err
	}
	defer cleanup()
	if _, err := clone.Adopt(r, dstPath); err != nil {
		return fmt.Errorf("adopt: %w", err)
	}
	return nil
}

// RestoreFromBucket is Restore for callers that already hold a
// objectstore.Bucket (e.g. a per-app prefixed bucket). It runs
// the same algorithm openObjectSource uses for s3:// and file://
// URLs but skips URL parsing. Returns (nil) on success.
//
// Returns ErrNoBaseline when the bucket has no HEAD baseline yet —
// callers (including lazyrestore's full-restore fallback) can
// treat this as "fresh cluster, run the empty-DB path."
func RestoreFromBucket(ctx context.Context, dstPath string, bucket objectstore.Bucket) error {
	if dstPath == "" {
		return errors.New("syzy: RestoreFromBucket: empty dstPath")
	}
	if bucket == nil {
		return errors.New("syzy: RestoreFromBucket: nil bucket")
	}
	// Bound + retry every object-store call this restore makes (HEAD, list,
	// chain tip, baseline, frames) so one hung connection cannot wedge the open.
	bucket = physicalrestore.NewBoundedBucket(bucket)
	tStart := time.Now()
	head, _, err := objstore.LoadHEAD(ctx, bucket)
	if err != nil {
		if errors.Is(err, objstore.ErrNoHEAD) {
			return ErrNoBaseline
		}
		return fmt.Errorf("syzy: RestoreFromBucket: load HEAD: %w", err)
	}
	if head.Baseline == nil {
		return ErrNoBaseline
	}
	headDur := time.Since(tStart)
	defer func() {
		slog.Info("syzy restore: from bucket complete",
			"head_dur", headDur.Round(time.Millisecond), "total_dur", time.Since(tStart).Round(time.Millisecond))
	}()
	tmpDir, err := os.MkdirTemp(filepath.Dir(dstPath), ".syzy-restore-*")
	if err != nil {
		return fmt.Errorf("syzy: RestoreFromBucket: mkdir tmp: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	stagedApp := filepath.Join(tmpDir, "app.db")
	stagedMeta := filepath.Join(tmpDir, "metadata.db")
	if err := restoreFromBucket(ctx, bucket, head, stagedApp, stagedMeta); err != nil {
		return fmt.Errorf("syzy: RestoreFromBucket: %w", err)
	}
	pr, pw := io.Pipe()
	bundleErr := make(chan error, 1)
	go func() {
		err := clone.WriteBundleFromFiles(pw, stagedMeta, stagedApp)
		_ = pw.CloseWithError(err)
		bundleErr <- err
	}()
	_, adoptErr := clone.Adopt(pr, dstPath)
	_ = pr.Close()
	if err := <-bundleErr; err != nil {
		return fmt.Errorf("syzy: RestoreFromBucket: bundle: %w", err)
	}
	if adoptErr != nil {
		return fmt.Errorf("syzy: RestoreFromBucket: adopt: %w", adoptErr)
	}
	// Detect a row_clock <-> app.db divergence in the materialized result
	// (e.g. a metadata baseline anchored ahead of the app stream, or a
	// holed db chain). Read-only and best-effort: a positive count means
	// this node would serve CRDT metadata claiming rows its tables lack.
	// Logged, not fatal — restore still produced the best point available.
	if res, cerr := CheckRestoreConsistency(dstPath, MetadataPathFor(dstPath)); cerr != nil {
		slog.Warn("syzy restore: consistency check skipped", "err", cerr)
	} else if res.Orphans > 0 {
		slog.Warn("syzy restore: orphan rows detected (metadata claims rows app.db lacks)",
			"orphans", res.Orphans, "per_table", res.PerTable)
	}
	return nil
}

// ErrNoBaseline is returned by RestoreFromBucket when the bucket
// has no HEAD baseline yet (fresh cluster). Callers should fall
// through to the ordinary sqlite.Open path on an empty directory.
var ErrNoBaseline = errors.New("syzy: bucket has no baseline yet")

// openRestoreSource resolves src to an io.Reader yielding a clone
// bundle, paired with a cleanup func the caller must invoke.
//
// tcp:// and unix: URLs are routed through the built-in mesh bundle
// protocol (per docs/TRANSPORT.md); a missing "?topic=" query defaults
// to DefaultTopic, matching single-database daemons. opts.Fetchers
// entries add or override schemes.
func openRestoreSource(ctx context.Context, src string, opts RestoreOptions) (io.Reader, func(), error) {
	u, err := url.Parse(src)
	if err != nil {
		return nil, func() {}, fmt.Errorf("parse url: %w", err)
	}
	if f, ok := opts.Fetchers[u.Scheme]; ok {
		return openFetcherSource(ctx, src, f)
	}
	switch u.Scheme {
	case "tcp", "unix":
		if u.Query().Get("topic") == "" {
			q := u.Query()
			q.Set("topic", DefaultTopic)
			u.RawQuery = q.Encode()
			src = u.String()
		}
		return openFetcherSource(ctx, src, tcpmesh.FetchBundle)
	case "s3", "file":
		return openObjectSource(ctx, src)
	default:
		return nil, func() {}, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
}

// openFetcherSource streams fetch(src) through a pipe, returning the
// read side plus a cleanup that cancels the dial.
func openFetcherSource(ctx context.Context, src string, fetch transport.BundleFetcher) (io.Reader, func(), error) {
	pr, pw := io.Pipe()
	dialCtx, cancel := context.WithCancel(ctx)
	go func() {
		err := fetch(dialCtx, src, pw)
		_ = pw.CloseWithError(err)
	}()
	return pr, func() {
		cancel()
		_ = pr.Close()
	}, nil
}

// openObjectSource builds an objectstore.Bucket from src, fetches HEAD,
// downloads + decompresses both files into a temp dir, and pipes a
// synthesized clone bundle through. HEAD with Snapshot:nil falls
// through as "nothing to restore here yet" — caller's fallback chain
// moves to the next source.
//
// Algorithm (matches the spec in internal/objstore/layout.go):
//
//  1. Read HEAD → coupled app and metadata baselines.
//  2. Restore metadata to its tip and read its parent_app_txid.
//  3. Restore app.db through that parent_app_txid.
//  4. Synthesize a clone bundle and feed it to the existing clone.Adopt
//     path (which handles identity reset).
func openObjectSource(ctx context.Context, src string) (io.Reader, func(), error) {
	be, err := objectstore.Open(ctx, src)
	if err != nil {
		return nil, func() {}, fmt.Errorf("backend: %w", err)
	}
	loadCtx, cancel := context.WithCancel(ctx)
	head, _, err := objstore.LoadHEAD(loadCtx, be)
	if err != nil {
		cancel()
		if errors.Is(err, objstore.ErrNoHEAD) {
			return nil, func() {}, fmt.Errorf("no HEAD at %s", src)
		}
		return nil, func() {}, fmt.Errorf("load HEAD: %w", err)
	}
	if head.Baseline == nil {
		cancel()
		return nil, func() {}, fmt.Errorf("HEAD at %s is beacon-only (no baseline yet)", src)
	}
	tmpDir, err := os.MkdirTemp("", "syzy-restore-*")
	if err != nil {
		cancel()
		return nil, func() {}, err
	}
	cleanup := func() {
		cancel()
		_ = os.RemoveAll(tmpDir)
	}
	stagedApp := filepath.Join(tmpDir, "app.db")
	stagedMeta := filepath.Join(tmpDir, "metadata.db")

	if err := restoreFromBucket(loadCtx, be, head, stagedApp, stagedMeta); err != nil {
		cleanup()
		return nil, func() {}, err
	}

	pr, pw := io.Pipe()
	go func() {
		err := clone.WriteBundleFromFiles(pw, stagedMeta, stagedApp)
		_ = pw.CloseWithError(err)
	}()
	return pr, func() {
		cleanup()
		_ = pr.Close()
	}, nil
}

// restoreFromBucket implements the algorithm above against materialized
// paths stagedApp / stagedMeta. Split out for testability.
//
// Both streams (db/ and metadata/) are LTX-shipped: download
// HEAD.Baseline + apply db/<L0|L1>/ chain → app.db; download
// HEAD.MetaBaseline + apply metadata/<L0|L1>/ chain → metadata.db.
func restoreFromBucket(
	ctx context.Context,
	be objectstore.Bucket,
	head *objstore.HEAD,
	stagedApp, stagedMeta string,
) error {
	// Restore metadata FIRST: it carries parent_app_txid, the app-side TXID the
	// row_clock/CRDT state is coupled at. Capping the app (db) stream to it makes
	// the restored pair a coherent snapshot — app.db never lands ahead of the
	// metadata that indexes it — matching lazyrestore.Prepare and
	// quieting CheckRestoreConsistency's false orphans (an app stream that ran
	// past the meta pin, e.g. with a later deletion, looks short of the older
	// clocks). A genuine hole in the app chain still under-restores and is still
	// reported, since the cap can only trim, never conjure missing frames.
	var capTXID uint64
	if head.MetaBaseline == nil {
		return ErrNoBaseline
	}
	if err := physicalrestore.MaterializeStream(ctx, be, objstore.MetadataPrefix, head.MetaBaseline, stagedMeta, 0); err != nil {
		return fmt.Errorf("meta: %w", err)
	}
	tx, err := physicalrestore.ParentAppTXID(stagedMeta)
	if err != nil {
		return fmt.Errorf("read parent_app_txid: %w", err)
	}
	capTXID = tx
	if err := physicalrestore.MaterializeStream(ctx, be, objstore.DBPrefix, head.Baseline, stagedApp, capTXID); err != nil {
		return fmt.Errorf("app: %w", err)
	}
	// Structural verification before the staged pair can be adopted: a
	// malformed image (bucket bitrot, a chain hole the geometry check
	// can't see) must fail the restore here, not poison the node with a
	// local database that every subsequent open rejects.
	if err := verifyMaterializedDB(stagedApp); err != nil {
		return fmt.Errorf("app: %w", err)
	}
	if err := verifyMaterializedDB(stagedMeta); err != nil {
		return fmt.Errorf("meta: %w", err)
	}
	return nil
}

// verifyMaterializedDB runs SQLite's quick_check over a materialized
// database and fails on any structural fault. quick_check walks every
// page of every btree (skipping only index-content cross-checks), so a
// truncated, holed, or torn image is caught while still staged.
func verifyMaterializedDB(path string) error {
	conn, err := sqlitebridge.Open(path, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		return fmt.Errorf("verify %s: open: %w", filepath.Base(path), err)
	}
	defer conn.Close()
	stmt, _, err := conn.Prepare(`PRAGMA quick_check(1)`)
	if err != nil {
		return fmt.Errorf("verify %s: %w", filepath.Base(path), err)
	}
	defer stmt.Finalize()
	if hasRow, err := stmt.Step(); err != nil {
		return fmt.Errorf("verify %s: %w", filepath.Base(path), err)
	} else if !hasRow {
		return fmt.Errorf("verify %s: quick_check returned no rows", filepath.Base(path))
	}
	if res := stmt.ColumnText(0); res != "ok" {
		return fmt.Errorf("verify %s: quick_check: %s", filepath.Base(path), res)
	}
	return nil
}
