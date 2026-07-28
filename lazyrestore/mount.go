//go:build linux

package lazyrestore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/wjordan/objectstore"
)

// Mount is a running page-faulting FUSE filesystem. It exposes the backing
// directory at a mount point and hydrates the configured database from its
// manifest.
//
// Close only after every consumer has released its database handles;
// otherwise the kernel may reject the unmount with EBUSY.
type Mount struct {
	mountPoint    string
	databaseName  string
	bucket        objectstore.Bucket
	manifest      *Manifest
	requesterID   string
	cloneProvider CloneProvider

	// backingFD is shared across every per-Open lazyFile so the
	// faulting protocol stays consistent: a fault from one open
	// pwrites to this fd, sets the bitmap bit, and a concurrent
	// open immediately sees the page as present. Closed at unmount.
	backingFD int

	// bitmap tracks "page bytes present in the sparse backing file";
	// cleanBitmap tracks "those bytes still match the manifest entry
	// for this page." Sibling-clone needs both: present-but-dirty is
	// not a safe clone source. Mutations protected by writeMu so
	// readers seeing a "clean" bit get bytes that haven't already
	// been overwritten by a concurrent local write.
	bitmap      *pageBitmap
	cleanBitmap *pageBitmap

	// writeMu guards the (clear cleanBitmap → pwrite → set bitmap)
	// sequence on the source side. Sibling clone holds the read
	// lock for its predicate-then-ioctl window so a concurrent
	// local write cannot clear cleanBitmap and dirty the page
	// after the predicate said it was clean.
	writeMu sync.RWMutex

	server *fuse.Server
}

// MountConfig captures what Mount needs at construction.
type MountConfig struct {
	// MountPoint is where the FUSE filesystem appears. Created if missing.
	MountPoint string
	// BackingPath is the host-only sparse database created by Prepare.
	// Its parent directory is exposed through the mount; all sibling files
	// retain loopback behavior.
	BackingPath string
	// Bucket is the objectstore the manifest's keys resolve against.
	Bucket objectstore.Bucket
	// Manifest is the Manifest produced by Prepare
	// (or LoadManifest on warm restart).
	Manifest *Manifest

	// RequesterID lets the clone provider skip the requesting mount. Empty
	// disables sibling cloning.
	RequesterID string

	// CloneProvider, when non-nil, is consulted before object-store
	// fetch on every page fault. nil falls through to the cold
	// object-store path.
	CloneProvider CloneProvider
}

// NewMount sets up the FUSE filesystem and returns once the kernel
// has acknowledged the mount. Caller owns Close.
func NewMount(ctx context.Context, cfg MountConfig) (*Mount, error) {
	if cfg.MountPoint == "" {
		return nil, errors.New("lazyrestore: MountPoint required")
	}
	if cfg.BackingPath == "" {
		return nil, errors.New("lazyrestore: BackingPath required")
	}
	if cfg.Bucket == nil {
		return nil, errors.New("lazyrestore: Bucket required")
	}
	if cfg.Manifest == nil {
		return nil, errors.New("lazyrestore: Manifest required")
	}
	if cfg.Manifest.PageSize == 0 || cfg.Manifest.CommitPages == 0 {
		return nil, errors.New("lazyrestore: manifest missing page size or commit pages")
	}
	if err := os.MkdirAll(cfg.MountPoint, 0o700); err != nil {
		return nil, fmt.Errorf("lazyrestore: mkdir mount point: %w", err)
	}

	backingDir := filepath.Dir(cfg.BackingPath)
	databaseName := filepath.Base(cfg.BackingPath)
	fd, err := syscall.Open(cfg.BackingPath, syscall.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("lazyrestore: open backing %s: %w", cfg.BackingPath, err)
	}
	bitmap, err := newPageBitmapFromFile(fd, cfg.Manifest.CommitPages, cfg.Manifest.PageSize)
	if err != nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("lazyrestore: seed bitmap: %w", err)
	}
	// cleanBitmap seeds empty on warm restart: a sparse extent may
	// hold dirty bytes from a prior run while the immutable manifest
	// still points to inherited LTX entries. Until pages refault or
	// get cloned again, a restarted mount is not a clean clone source.
	cleanBitmap := newPageBitmap(cfg.Manifest.CommitPages)

	m := &Mount{
		mountPoint:    cfg.MountPoint,
		databaseName:  databaseName,
		bucket:        cfg.Bucket,
		manifest:      cfg.Manifest,
		requesterID:   cfg.RequesterID,
		cloneProvider: cfg.CloneProvider,
		backingFD:     fd,
		bitmap:        bitmap,
		cleanBitmap:   cleanBitmap,
	}

	// Seed device id so LoopbackRoot's idFromStat composes inodes
	// the way the loopback impl expects (matches NewLoopbackRoot).
	var st syscall.Stat_t
	if err := syscall.Stat(backingDir, &st); err != nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("lazyrestore: stat backing dir: %w", err)
	}
	root := &lazyRoot{mount: m}
	loopRoot := &fs.LoopbackRoot{
		Path:    backingDir,
		Dev:     uint64(st.Dev),
		NewNode: root.newChild,
	}
	rootEmbedder := loopRoot.NewNode(loopRoot, nil, "", &st)
	loopRoot.RootNode = rootEmbedder

	// FUSE options: keep timeouts conservative. The database size is
	// fixed at bootstrap, so attr caching is safe; the other entries
	// inherit loopback semantics where short timeouts are correct.
	oneMinute := time.Minute
	fsOpts := &fs.Options{
		EntryTimeout: &oneMinute,
		AttrTimeout:  &oneMinute,
		MountOptions: fuse.MountOptions{
			AllowOther:   true,
			Options:      []string{"default_permissions"},
			MaxWrite:     1 << 20,
			MaxReadAhead: 128 * 1024,
			// Skip fork+exec of fusermount3 when the process has
			// CAP_SYS_ADMIN.
			DirectMount: true,
		},
	}
	srv, err := fs.Mount(cfg.MountPoint, rootEmbedder, fsOpts)
	if err != nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("lazyrestore: fs.Mount: %w", err)
	}
	// go-fuse opens /dev/fuse WITHOUT O_CLOEXEC. A child forked while this
	// mount lives inherits the fd; if the child outlives this process, the
	// FUSE connection stays alive-but-unserved and the mountpoint HANGS
	// (never ENOTCONN) for every future opener. Close the hole immediately.
	cloexecFuseFDs()
	// Loopback files other than the configured database may be mmap'd by this
	// process. Without
	// kernel FUSE passthrough (kernel >= 6.9 + CAP_SYS_ADMIN, auto-negotiated
	// by go-fuse for loopback handles) those faults are served by this
	// process's own FUSE goroutines, and a fault outstanding when the GC stops
	// the world deadlocks the entire process. Probe with a real registration
	// (the kernel init flag alone misses the missing-capability case) and
	// surface the degraded mode loudly.
	if id, errno := srv.RegisterBackingFd(&fuse.BackingMap{Fd: int32(fd)}); errno != 0 {
		slog.Warn("lazyrestore: kernel FUSE passthrough unavailable; in-process mmap of loopback files risks GC stop-the-world deadlock",
			"mountPoint", cfg.MountPoint, "errno", errno)
	} else {
		_ = srv.UnregisterBackingFd(id)
	}
	m.server = srv
	return m, nil
}

// Close unmounts the FUSE filesystem, drains the server loop, and
// releases the backing fd. Caller must have closed every database consumer;
// an active open returns EBUSY from the kernel unmount and leaves
// the mount in a half-detached state.
//
// Unmount() returns once the kernel mount table is detached, but
// the go-fuse Server.loop goroutine keeps reading from /dev/fuse
// until that fd hits ENODEV. Without Wait() the loop survives Close
// and shows up later as a leaked goroutine (and under -race + GC
// pressure it can deadlock against the next mount in the same
// process).
func (m *Mount) Close() error {
	if m == nil {
		return nil
	}
	var firstErr error
	if m.server != nil {
		if err := m.server.Unmount(); err != nil {
			// EBUSY: an opener we don't control still holds a file
			// inside the mount. A FUSE request from that opener
			// (flush at its exit, say) against our about-to-die
			// server would hang in the kernel forever — including
			// the unkillable-zombie variant where OUR OWN process
			// exits with such an fd still open. Abort the
			// connection: in-flight and future requests fail with
			// ECONNABORTED instead of waiting on a dead server.
			if aerr := m.Abort(); aerr != nil && firstErr == nil {
				firstErr = fmt.Errorf("lazyrestore: unmount: %v; abort: %w", err, aerr)
			} else if firstErr == nil {
				firstErr = fmt.Errorf("lazyrestore: unmount (connection aborted): %w", err)
			}
			_ = m.server.Unmount()
		}
		// Bound the drain: the loop normally exits as soon as
		// /dev/fuse hits ENODEV, but if the kernel still has the
		// connection (unmount raced a new opener), force-abort
		// rather than hang Close.
		done := make(chan struct{})
		srv := m.server
		go func() { srv.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = m.Abort()
			<-done
		}
		m.server = nil
	}
	if m.backingFD > 0 {
		if err := syscall.Close(m.backingFD); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("lazyrestore: close backing fd: %w", err)
		}
		m.backingFD = 0
	}
	return firstErr
}

// Abort force-aborts the kernel FUSE connection backing this mount
// via /sys/fs/fuse/connections/<dev-minor>/abort. Every in-flight and
// future request on the mount fails with ECONNABORTED — exactly what
// a stuck opener needs when the in-process server is gone. The
// connection id is the device minor of the mounted filesystem.
func (m *Mount) Abort() error {
	var st syscall.Stat_t
	if err := syscall.Stat(m.mountPoint, &st); err != nil {
		return fmt.Errorf("lazyrestore: stat mountpoint: %w", err)
	}
	minor := (st.Dev & 0xff) | ((st.Dev >> 12) & ^uint64(0xff))
	path := fmt.Sprintf("/sys/fs/fuse/connections/%d/abort", minor)
	if err := os.WriteFile(path, []byte("1"), 0); err != nil {
		return fmt.Errorf("lazyrestore: abort fuse connection: %w", err)
	}
	return nil
}

func (m *Mount) pagesPresent() uint32 { return m.bitmap.presentCount() }

// ensurePagesPresent populates every page covering [off, off+length)
// from the LTX chain into the backing fd. Used by both Read and
// Write handlers before they touch the backing file. Concurrent
// callers race CAS-cleanly: duplicate work, no corruption.
func (m *Mount) ensurePagesPresent(ctx context.Context, off, length int64) error {
	if length <= 0 {
		return nil
	}
	ps := int64(m.manifest.PageSize)
	loPg := uint32(off/ps) + 1
	hiByte := off + length - 1
	hiPg := uint32(hiByte/ps) + 1
	if loPg < 1 {
		loPg = 1
	}
	commit := m.manifest.CommitPages
	if hiPg > commit {
		hiPg = commit
	}
	for pgno := loPg; pgno <= hiPg; pgno++ {
		if m.bitmap.isSet(pgno) {
			continue
		}
		if err := m.faultPage(ctx, pgno); err != nil {
			return err
		}
	}
	return nil
}

// faultPage fetches a single page and pwrites it to the backing
// fd, then atomically marks both bitmap and cleanBitmap. Safe
// under concurrent callers fetching the same page: the CAS resolves
// the race, and pwrite is naturally idempotent for byte-equal data.
// Tries sibling clone first when a CloneProvider is configured; on
// clone success the page is also clean (the source proved it).
func (m *Mount) faultPage(ctx context.Context, pgno uint32) error {
	loc, ok := m.manifest.Pages[pgno]
	if !ok {
		// Out-of-band pgno or lock page: let FetchPage produce the
		// correct error / zero-fill via its existing rules.
		return m.faultFromObject(ctx, pgno)
	}
	if m.cloneProvider != nil {
		off := int64(pgno-1) * int64(m.manifest.PageSize)
		cloned, err := m.cloneProvider.TryClonePage(m.requesterID, loc, pgno, m.backingFD, off, int64(m.manifest.PageSize))
		if err != nil {
			// Clone is best-effort; fall through to object fetch.
			cloned = false
		}
		if cloned {
			m.bitmap.trySet(pgno)
			m.cleanBitmap.trySet(pgno)
			return nil
		}
	}
	return m.faultFromObject(ctx, pgno)
}

func (m *Mount) faultFromObject(ctx context.Context, pgno uint32) error {
	data, err := m.manifest.FetchPage(ctx, m.bucket, pgno)
	if err != nil {
		return fmt.Errorf("fault pgno %d: %w", pgno, err)
	}
	off := int64(pgno-1) * int64(m.manifest.PageSize)
	// pwrite ignores fd offset; safe across concurrent opens.
	if err := pwriteAll(m.backingFD, data, off); err != nil {
		return fmt.Errorf("pwrite pgno %d @ %d: %w", pgno, off, err)
	}
	m.bitmap.trySet(pgno)      // may already be set if a peer raced; benign.
	m.cleanBitmap.trySet(pgno) // freshly fetched bytes match the manifest entry.
	return nil
}

// pwriteAll keeps issuing Pwrite until len(data) bytes are durably
// committed at off or the kernel returns an error. Critical: callers
// publish presence (bitmap.Set / TrySet) only after this returns nil
// — a partial pwrite that promoted the bit would let a concurrent
// reader serve stale zeros from the uncovered tail.
func pwriteAll(fd int, data []byte, off int64) error {
	for len(data) > 0 {
		n, err := syscall.Pwrite(fd, data, off)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return err
		}
		if n <= 0 {
			return syscall.EIO
		}
		data = data[n:]
		off += int64(n)
	}
	return nil
}

// lazyRoot owns the NewNode hook plugged into LoopbackRoot. It
// holds only the Mount pointer so newChild can stamp it into
// database-specific lazyNodes; everything else stays on LoopbackRoot.
type lazyRoot struct {
	mount *Mount
}

// newChild is plugged into LoopbackRoot.NewNode. Children of root
// named for the configured database get a lazyNode that overrides Open;
// everything else gets the default LoopbackNode and behaves as passthrough.
func (r *lazyRoot) newChild(root *fs.LoopbackRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
	if parent != nil && name == r.mount.databaseName {
		// Verify parent is the root by checking it has no parent
		// of its own (root has no parents recorded in go-fuse's
		// embedded Inode).
		if isRootInode(parent) {
			return &lazyNode{
				LoopbackNode: fs.LoopbackNode{RootData: root},
				mount:        r.mount,
			}
		}
	}
	return &fs.LoopbackNode{RootData: root}
}

// isRootInode returns true when n is the FUSE mount's root inode.
// go-fuse's Inode lookup walks Path(nil) returning "" for the root.
func isRootInode(n *fs.Inode) bool {
	return n.Path(nil) == ""
}

// lazyNode is the special-case loopback node for the configured database. It
// inherits all directory-irrelevant methods from LoopbackNode (we
// only care about Open / Getattr / Setattr-size); the loopback
// implementations handle them correctly when the underlying file
// exists with the right size on disk.
type lazyNode struct {
	fs.LoopbackNode
	mount *Mount
}

// Open hands out a lazyFile that wraps the mount-level shared fd.
// We don't dup the fd: every open shares one backing handle. pread
// and pwrite both carry their own offsets, so concurrent opens stay
// safe without per-FH locking; bitmap CAS resolves any fault races.
func (n *lazyNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	_ = flags // accepted but ignored; faulting+pwrite happens on the shared fd
	return &lazyFile{mount: n.mount}, 0, 0
}

// lazyFile is the FUSE file handle for the configured database. Reads fault
// missing pages then return the requested bytes; writes fault any
// partial-overwrite pages, pwrite, and update the bitmap.
type lazyFile struct {
	mount *Mount
}

var (
	_ fs.FileHandle   = (*lazyFile)(nil)
	_ fs.FileReader   = (*lazyFile)(nil)
	_ fs.FileWriter   = (*lazyFile)(nil)
	_ fs.FileFsyncer  = (*lazyFile)(nil)
	_ fs.FileFlusher  = (*lazyFile)(nil)
	_ fs.FileReleaser = (*lazyFile)(nil)
)

func (lf *lazyFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if err := lf.mount.ensurePagesPresent(ctx, off, int64(len(dest))); err != nil {
		return nil, syscall.EIO
	}
	// pread from the backing fd into dest. Could use ReadResultFd
	// for zero-copy but FUSE_DIRECT_IO is rare here and the small
	// buffer copy keeps the code simple.
	n, err := syscall.Pread(lf.mount.backingFD, dest, off)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (lf *lazyFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if len(data) == 0 {
		return 0, 0
	}
	ps := int64(lf.mount.manifest.PageSize)
	loPg := uint32(off/ps) + 1
	hiByte := off + int64(len(data)) - 1
	hiPg := uint32(hiByte/ps) + 1
	commit := lf.mount.manifest.CommitPages
	// For partial-overwrite pages (the write doesn't cover the
	// whole page), fault the page in first so the unwritten bytes
	// come from the LTX chain, not stale zeros. SQLite writes are
	// page-aligned in steady state, so this only fires at the
	// edges and on the WAL's frame headers (which live in
	// the WAL rather than the database file anyway).
	startPartial := off%ps != 0
	endPartial := (off+int64(len(data)))%ps != 0
	if startPartial && loPg <= commit && !lf.mount.bitmap.isSet(loPg) {
		if err := lf.mount.faultPage(ctx, loPg); err != nil {
			return 0, syscall.EIO
		}
	}
	if endPartial && hiPg <= commit && hiPg != loPg && !lf.mount.bitmap.isSet(hiPg) {
		if err := lf.mount.faultPage(ctx, hiPg); err != nil {
			return 0, syscall.EIO
		}
	}
	// Exclusive write lock pairs with the read lock TryClonePage
	// takes around its predicate-and-ioctl: a sibling can never see
	// "clean" while the local write is in flight.
	lf.mount.writeMu.Lock()
	defer lf.mount.writeMu.Unlock()
	// Clear cleanBitmap for every page this write touches BEFORE
	// the pwrite. Any sibling that checked clean before this point
	// is fine (their bytes are the pre-write image); any sibling
	// that checks after sees not-clean and declines to clone.
	for pgno := loPg; pgno <= hiPg; pgno++ {
		if pgno > commit {
			break
		}
		lf.mount.cleanBitmap.clear(pgno)
	}
	// pwriteAll: a short pwrite that promoted bitmap bits would let
	// a peer read stale zeros from the uncovered tail.
	if err := pwriteAll(lf.mount.backingFD, data, off); err != nil {
		return 0, fs.ToErrno(err)
	}
	// Mark every page wholly covered by the write as present (the
	// write IS the data — no fetch needed for the unwritten parts
	// of partial-cover pages because we already faulted them above).
	for pgno := loPg; pgno <= hiPg; pgno++ {
		if pgno > commit {
			break
		}
		lf.mount.bitmap.set(pgno)
	}
	return uint32(len(data)), 0
}

func (lf *lazyFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	_ = flags
	if err := syscall.Fsync(lf.mount.backingFD); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}

func (lf *lazyFile) Flush(ctx context.Context) syscall.Errno {
	// Loopback's Flush dup+close pattern doesn't apply here (we
	// share one fd across opens). Return success; the actual
	// durability happens at Fsync.
	return 0
}

func (lf *lazyFile) Release(ctx context.Context) syscall.Errno {
	// Nothing per-open to release; the shared backing fd is owned
	// by Mount and closed in Mount.Close.
	return 0
}
