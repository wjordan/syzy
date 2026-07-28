//go:build linux

package lazyrestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

// requireFUSE skips the test if /dev/fuse isn't accessible. Lazy
// mount tests need a working FUSE userspace; CI without fuse3 or
// sandboxed environments without CAP_SYS_ADMIN should report
// skipped, not failed.
func requireFUSE(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("lazy-fetch tests require /dev/fuse: %v", err)
	}
}

// publishedBucket spins up a source syzy.Node, commits data, fires
// PublishSnapshot twice so parent_app_txid is stamped into
// metadata.db, closes the node, then runs Prepare into a fresh
// backingDir. Returns the bucket, the manifest, and backingDir for
// the test to mount.
func publishedBucket(t *testing.T) (objectstore.Bucket, *Manifest, string) {
	t.Helper()
	bucketDir := t.TempDir()
	be, err := objectstore.OpenFS(bucketDir)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	srcDB := filepath.Join(t.TempDir(), "src.db")
	ctx := context.Background()
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          srcDB,
		ObjectBackend: be,
		SchemaLog:     schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open src: %v", err)
	}
	if err := node.Exec(`CREATE TABLE notes (id INT PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
		_ = node.Close()
		t.Fatalf("CREATE: %v", err)
	}
	for i := 1; i <= 16; i++ {
		stmt := fmt.Sprintf(`INSERT INTO notes VALUES (%d, 'r%02d')`, i, i)
		if err := node.Exec(stmt); err != nil {
			_ = node.Close()
			t.Fatalf("INSERT: %v", err)
		}
	}
	// PublishSnapshot's AllocAppTXID blocks until the publisher
	// has seeded its counter from MaxLTXTXID(db/0000/), so no
	// HEAD-polling loop is needed here.
	if err := node.PublishSnapshot(ctx); err != nil {
		_ = node.Close()
		t.Fatalf("PublishSnapshot 1: %v", err)
	}
	if err := node.Exec(`INSERT INTO notes VALUES (99, 'tail')`); err != nil {
		_ = node.Close()
		t.Fatalf("INSERT tail: %v", err)
	}
	if err := node.PublishSnapshot(ctx); err != nil {
		_ = node.Close()
		t.Fatalf("PublishSnapshot 2: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close src: %v", err)
	}

	backingDir := filepath.Join(t.TempDir(), "lazy-backing")
	backingDB := filepath.Join(backingDir, "shared.db")
	if err := os.MkdirAll(backingDir, 0o700); err != nil {
		t.Fatalf("mkdir backing: %v", err)
	}
	manifest, err := Prepare(ctx, backingDB, be, "")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if manifest == nil {
		t.Fatalf("Prepare: nil manifest from populated bucket")
	}
	return be, manifest, backingDir
}

// mountOrSkip wraps NewMount so tests in unprivileged
// environments skip cleanly instead of erroring out on permissions.
func mountOrSkip(t *testing.T, cfg MountConfig) *Mount {
	t.Helper()
	m, err := NewMount(context.Background(), cfg)
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") ||
			strings.Contains(err.Error(), "fusermount") ||
			strings.Contains(err.Error(), "permission denied") {
			t.Skipf("FUSE mount not permitted: %v", err)
		}
		t.Fatalf("Mount: %v", err)
	}
	return m
}

func TestMount_RoundTrip(t *testing.T) {
	requireFUSE(t)
	bucket, manifest, backingDir := publishedBucket(t)

	mountPoint := filepath.Join(t.TempDir(), "mnt")
	m := mountOrSkip(t, MountConfig{
		MountPoint:  mountPoint,
		BackingPath: filepath.Join(backingDir, "shared.db"),
		Bucket:      bucket,
		Manifest:    manifest,
	})
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Logf("Mount.Close: %v", err)
		}
	})

	// Read shared.db through the FUSE mount. Bytes must equal
	// what a full restore would produce against the same bucket.
	full := filepath.Join(t.TempDir(), "full.db")
	if err := syzy.RestoreFromBucket(context.Background(), full, bucket); err != nil {
		t.Fatalf("RestoreFromBucket full: %v", err)
	}
	fullBytes, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("read full: %v", err)
	}
	mountedBytes, err := os.ReadFile(filepath.Join(mountPoint, "shared.db"))
	if err != nil {
		t.Fatalf("read through mount: %v", err)
	}
	if len(fullBytes) != len(mountedBytes) {
		t.Fatalf("size mismatch: full=%d mounted=%d", len(fullBytes), len(mountedBytes))
	}
	for i := range fullBytes {
		if fullBytes[i] != mountedBytes[i] {
			t.Fatalf("byte %d differs: full=%d mounted=%d", i, fullBytes[i], mountedBytes[i])
		}
	}

	// After the full-file read every in-range page must be present
	// in the bitmap (tolerate one page slack for the lock page when
	// it falls inside [1, commit]).
	if got, want := m.pagesPresent(), manifest.CommitPages; got+1 < want {
		t.Errorf("pages present after full read = %d, want >= %d", got, want-1)
	}
}

func TestMount_BitmapRebuildOnRemount(t *testing.T) {
	requireFUSE(t)
	bucket, manifest, backingDir := publishedBucket(t)

	mountPoint := filepath.Join(t.TempDir(), "mnt")
	// First mount: fault a few pages, then close.
	{
		m := mountOrSkip(t, MountConfig{
			MountPoint:  mountPoint,
			BackingPath: filepath.Join(backingDir, "shared.db"),
			Bucket:      bucket,
			Manifest:    manifest,
		})
		f, err := os.Open(filepath.Join(mountPoint, "shared.db"))
		if err != nil {
			_ = m.Close()
			t.Fatalf("open mounted: %v", err)
		}
		buf := make([]byte, int(manifest.PageSize)*3)
		if _, err := f.ReadAt(buf, 0); err != nil {
			_ = f.Close()
			_ = m.Close()
			t.Fatalf("ReadAt: %v", err)
		}
		_ = f.Close()
		if err := m.Close(); err != nil {
			t.Fatalf("Close mount 1: %v", err)
		}
	}
	// Second mount: newPageBitmapFromFile should re-discover the
	// pages that were faulted in the first mount via SEEK_DATA on
	// the backing file. pages present should be > 0 without any
	// FUSE_READ having driven new fetches.
	m2 := mountOrSkip(t, MountConfig{
		MountPoint:  mountPoint,
		BackingPath: filepath.Join(backingDir, "shared.db"),
		Bucket:      bucket,
		Manifest:    manifest,
	})
	t.Cleanup(func() { _ = m2.Close() })
	if got := m2.pagesPresent(); got == 0 {
		t.Errorf("remount: pages present = 0, expected > 0 from SEEK_DATA rebuild")
	}
}

// TestBitmap_NewFromFile_AllSparse covers the corner case codex
// flagged in review: a backing file that is entirely sparse must
// produce an all-zero bitmap (every page reported absent), and the
// SEEK_DATA walk must exit cleanly on the first ENXIO without
// touching out-of-range pgno math.
func TestBitmap_NewFromFile_AllSparse(t *testing.T) {
	const pageSize = 4096
	const pages = 32
	path := filepath.Join(t.TempDir(), "sparse.db")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.Truncate(int64(pageSize) * int64(pages)); err != nil {
		_ = f.Close()
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { f.Close() })

	b, err := newPageBitmapFromFile(int(f.Fd()), pages, pageSize)
	if err != nil {
		t.Fatalf("newPageBitmapFromFile: %v", err)
	}
	if got := b.presentCount(); got != 0 {
		t.Errorf("all-sparse PresentCount = %d, want 0", got)
	}
	for pgno := uint32(1); pgno <= pages; pgno++ {
		if b.isSet(pgno) {
			t.Errorf("pgno %d marked present in all-sparse file", pgno)
		}
	}
}

func TestBitmap_BasicOps(t *testing.T) {
	b := newPageBitmap(200)
	if b.isSet(1) {
		t.Fatalf("freshly-allocated bit 1 should be clear")
	}
	if !b.trySet(1) {
		t.Fatalf("TrySet 1 should succeed on clear bit")
	}
	if !b.isSet(1) {
		t.Fatalf("after TrySet: bit 1 should be set")
	}
	if b.trySet(1) {
		t.Fatalf("TrySet on already-set bit should return false")
	}
	if b.presentCount() != 1 {
		t.Fatalf("PresentCount = %d, want 1", b.presentCount())
	}
	// Out-of-range: no-op, no panic.
	if b.trySet(0) || b.isSet(0) {
		t.Errorf("out-of-range pgno 0 should be no-op")
	}
	if b.trySet(201) || b.isSet(201) {
		t.Errorf("out-of-range pgno 201 should be no-op")
	}
}
