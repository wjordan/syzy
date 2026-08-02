// Package clone implements the bootstrap-by-bytes path for a syzy
// node: produce a "bundle" stream from one node's app.db + metadata,
// land it at a fresh path on another node, and rewrite identity so the
// new node runs as a distinct origin without colliding with the source.
//
// The wire format is the same bytes whether streamed over TCP or read
// from a file. Stream and Adopt are pure io functions so the file and
// network paths share one code path; tests run end-to-end through a
// bytes.Buffer.
package clone

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/sqlitebridge"
)

// Magic bytes at the head of every bundle: "SYZB" v1.
var bundleMagic = [...]byte{'S', 'Y', 'Z', 'B'}

const bundleVersion byte = 0x01

// File names inside a bundle. Two entries, in this order. The receiver
// validates names; mismatches abort.
const (
	nameMetaDB = "metadata.db"
	nameAppDB  = "app.db"
)

// backupPageStep is how many pages backup_step copies per call. Tuned
// so each step runs for tens of microseconds — short enough to release
// the source's writer lock between steps under load.
const backupPageStep = 256

// MaxFileBytes caps an individual file inside a bundle at 64 GiB. A
// safety net against malformed streams; real databases that bump up
// against this should re-evaluate the bundle workflow.
const MaxFileBytes uint64 = 64 << 30

// Stream writes a bundle of (metadata.db, app.db) for the syzy node at
// srcAppDB to w. The two databases are read through SQLite's online
// backup API, so concurrent writers on the source are unblocked
// between page-batches.
//
// Stream does not coordinate with a running daemon; callers that want
// a snapshot consistent with all currently-committed app.db rows must
// flush the in-memory CRDT cache to the metadata before invoking. The
// daemon's Snapshotter.SnapshotOnce is the right primitive there.
func Stream(w io.Writer, srcAppDB string) error {
	bw := bufio.NewWriter(w)
	if _, err := bw.Write(bundleMagic[:]); err != nil {
		return fmt.Errorf("clone: write magic: %w", err)
	}
	if err := bw.WriteByte(bundleVersion); err != nil {
		return fmt.Errorf("clone: write version: %w", err)
	}
	if err := streamFile(bw, nameMetaDB, layout.MetaDB(srcAppDB)); err != nil {
		return err
	}
	if err := streamFile(bw, nameAppDB, srcAppDB); err != nil {
		return err
	}
	return bw.Flush()
}

// streamFile copies one SQLite database from srcPath to w, framed as
// (name_len, name, payload_len, payload). The body is produced by
// running sqlite3_backup against srcPath into a fresh temp file, then
// streaming the temp file's bytes. The temp file is removed on return.
//
// Used by the offline path (Stream) where the source is quiescent;
// the live path uses PinSnapshots+PinnedBundle.Stream instead.
func streamFile(w io.Writer, name, srcPath string) error {
	tmp, err := os.CreateTemp("", "syzy-clone-*.db")
	if err != nil {
		return fmt.Errorf("clone: create tmp for %s: %w", name, err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)

	if err := backupTo(tmpPath, srcPath); err != nil {
		return fmt.Errorf("clone: backup %s: %w", name, err)
	}
	return writeFileFrame(w, name, tmpPath)
}

// writeFileFrame writes a (name_len, name, payload_len, payload) frame
// to w where payload is the bytes of the file at tmpPath. Both Stream
// (offline) and PinnedBundle.Stream (live) use this once their
// respective backup phase has populated tmpPath.
func writeFileFrame(w io.Writer, name, tmpPath string) error {
	info, err := os.Stat(tmpPath)
	if err != nil {
		return fmt.Errorf("clone: stat %s tmp: %w", name, err)
	}
	if uint64(info.Size()) > MaxFileBytes {
		return fmt.Errorf("clone: %s exceeds MaxFileBytes (%d > %d)", name, info.Size(), MaxFileBytes)
	}
	if err := writeName(w, name); err != nil {
		return err
	}
	if err := writeUvarint(w, uint64(info.Size())); err != nil {
		return fmt.Errorf("clone: write %s len: %w", name, err)
	}
	src, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("clone: reopen %s tmp: %w", name, err)
	}
	defer src.Close()
	if n, err := io.Copy(w, src); err != nil {
		return fmt.Errorf("clone: stream %s after %d bytes: %w", name, n, err)
	}
	return nil
}

// BackupTo copies srcPath into a fresh database at dstPath via the
// online backup API. dstPath must not exist; the file is created with
// the source's page size and exact byte content. Useful for offline
// snapshot publishing when the source daemon is stopped.
func BackupTo(dstPath, srcPath string) error { return backupTo(dstPath, srcPath) }

// WriteBundleFromFiles emits a bundle stream (the same wire format
// Stream and PinnedBundle.Stream produce) from already-staged
// metadata.db and app.db files. Used by the snapshot-restore path,
// where files have been downloaded and decompressed beforehand.
func WriteBundleFromFiles(w io.Writer, metaPath, appPath string) error {
	bw := bufio.NewWriter(w)
	if _, err := bw.Write(bundleMagic[:]); err != nil {
		return err
	}
	if err := bw.WriteByte(bundleVersion); err != nil {
		return err
	}
	if err := writeFileFrame(bw, nameMetaDB, metaPath); err != nil {
		return err
	}
	if err := writeFileFrame(bw, nameAppDB, appPath); err != nil {
		return err
	}
	return bw.Flush()
}

// backupTo copies srcPath into a fresh database at dstPath via the
// online backup API. dstPath must not exist; the file is created with
// the source's page size and exact byte content.
func backupTo(dstPath, srcPath string) error {
	src, err := sqlitebridge.Open(srcPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		return fmt.Errorf("open src %q: %w", srcPath, err)
	}
	defer src.Close()
	dst, err := sqlitebridge.Open(dstPath, 0)
	if err != nil {
		return fmt.Errorf("open dst %q: %w", dstPath, err)
	}
	defer dst.Close()

	bk, err := sqlitebridge.BackupInit(dst, "main", src, "main")
	if err != nil {
		return fmt.Errorf("backup_init: %w", err)
	}
	for {
		err := bk.Step(backupPageStep)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = bk.Finish()
			return fmt.Errorf("backup_step: %w", err)
		}
	}
	if err := bk.Finish(); err != nil {
		return fmt.Errorf("backup_finish: %w", err)
	}
	return nil
}

// pinnedBackup holds an sqlite3_backup and its staged destination. SQLite's
// backup API does not retain a source snapshot between backup_step calls: a
// source write can restart the backup and appear in later steps. The caller
// must therefore finish both backups before releasing its writer barrier.
//
// startPinnedBackup and finish must both run inside the writer barrier.
type pinnedBackup struct {
	name    string
	src     *sqlitebridge.Conn
	dst     *sqlitebridge.Conn
	bk      *sqlitebridge.Backup
	tmpPath string
}

func startPinnedBackup(name, srcPath string) (*pinnedBackup, error) {
	tmp, err := os.CreateTemp("", "syzy-clone-*.db")
	if err != nil {
		return nil, fmt.Errorf("clone: create tmp for %s: %w", name, err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	pb := &pinnedBackup{name: name, tmpPath: tmpPath}
	ok := false
	defer func() {
		if !ok {
			pb.close()
		}
	}()

	pb.src, err = sqlitebridge.Open(srcPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		return nil, fmt.Errorf("clone: open %s src: %w", name, err)
	}
	pb.dst, err = sqlitebridge.Open(tmpPath, 0)
	if err != nil {
		return nil, fmt.Errorf("clone: open %s dst: %w", name, err)
	}
	pb.bk, err = sqlitebridge.BackupInit(pb.dst, "main", pb.src, "main")
	if err != nil {
		return nil, fmt.Errorf("clone: backup_init %s: %w", name, err)
	}
	// Copy one page before returning the initialized handle. PinSnapshots
	// immediately drains the rest while its caller still holds the writer
	// barrier. EOF on a tiny database is fine; finish will be a no-op.
	if err := pb.bk.Step(1); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("clone: backup_step(1) %s: %w", name, err)
	}
	ok = true
	return pb, nil
}

func (pb *pinnedBackup) finish() (string, error) {
	if pb == nil || pb.bk == nil {
		if pb != nil {
			return pb.tmpPath, nil
		}
		return "", errors.New("clone: pinnedBackup.finish on nil")
	}
	for {
		err := pb.bk.Step(backupPageStep)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = pb.bk.Finish()
			pb.bk = nil
			return "", fmt.Errorf("clone: backup_step %s: %w", pb.name, err)
		}
	}
	if err := pb.bk.Finish(); err != nil {
		pb.bk = nil
		return "", fmt.Errorf("clone: backup_finish %s: %w", pb.name, err)
	}
	pb.bk = nil
	if err := pb.dst.Close(); err != nil {
		return "", fmt.Errorf("clone: close %s dst: %w", pb.name, err)
	}
	pb.dst = nil
	if err := pb.src.Close(); err != nil {
		return "", fmt.Errorf("clone: close %s src: %w", pb.name, err)
	}
	pb.src = nil
	return pb.tmpPath, nil
}

func (pb *pinnedBackup) close() {
	if pb == nil {
		return
	}
	if pb.bk != nil {
		_ = pb.bk.Finish()
		pb.bk = nil
	}
	if pb.dst != nil {
		_ = pb.dst.Close()
		pb.dst = nil
	}
	if pb.src != nil {
		_ = pb.src.Close()
		pb.src = nil
	}
	if pb.tmpPath != "" {
		_ = os.Remove(pb.tmpPath)
		pb.tmpPath = ""
	}
}

// PinnedBundle holds the two staged backup snapshots that make up one
// consistent bundle: metadata.db (CRDT metadata) and app.db (user data).
// Both are copied to completion at the same logical commit boundary while
// the caller holds its writer barrier across PinSnapshots.
//
// Construct with PinSnapshots inside the barrier. Stream only frames the
// already-staged files, so the barrier may be released before it runs. Close
// releases resources without producing wire output (use on error paths).
type PinnedBundle struct {
	cluster *pinnedBackup
	app     *pinnedBackup
}

// PinSnapshots copies metadata.db and app.db at srcAppDB to staged files.
// Caller must hold a WAL writer barrier across this call (see
// syzy.Node.ServeBundle for the orchestrator) so both completed copies reflect
// the same logical commit boundary. Copying only an initial page here and
// draining later is unsafe: sqlite3_backup follows source writes between
// backup_step calls instead of retaining the first step's snapshot.
//
// On error, no resources are leaked.
func PinSnapshots(srcAppDB string) (*PinnedBundle, error) {
	pb := &PinnedBundle{}
	ok := false
	defer func() {
		if !ok {
			pb.Close()
		}
	}()

	var err error
	pb.cluster, err = startPinnedBackup(nameMetaDB, layout.MetaDB(srcAppDB))
	if err != nil {
		return nil, err
	}
	pb.app, err = startPinnedBackup(nameAppDB, srcAppDB)
	if err != nil {
		return nil, err
	}
	// Drain both backups before returning to the caller, which releases the
	// writer barrier immediately after this function. finish is idempotent;
	// Files later returns these staged paths without touching the sources.
	if _, err = pb.cluster.finish(); err != nil {
		return nil, err
	}
	if _, err = pb.app.finish(); err != nil {
		return nil, err
	}
	ok = true
	return pb, nil
}

// Files returns the staged temp file paths for the metadata.db and app.db
// copies. The caller must invoke Close on the PinnedBundle after consuming the
// files (typically via defer); Close removes the temp files.
//
// Callable exactly once; consumes the pinned backup state. After
// Files returns, Stream cannot be called on the same PinnedBundle.
func (pb *PinnedBundle) Files() (metaPath, appPath string, err error) {
	if pb == nil || pb.cluster == nil || pb.app == nil {
		return "", "", errors.New("clone: PinnedBundle.Files after Close or Stream")
	}
	metaPath, err = pb.cluster.finish()
	if err != nil {
		return "", "", err
	}
	appPath, err = pb.app.finish()
	if err != nil {
		return "", "", err
	}
	return metaPath, appPath, nil
}

// Stream writes the two staged backups in the bundle wire format to w.
// Frees all held SQLite resources before
// returning (success or error). Callable exactly once.
func (pb *PinnedBundle) Stream(w io.Writer) error {
	if pb == nil || pb.cluster == nil || pb.app == nil {
		return errors.New("clone: PinnedBundle.Stream after Close")
	}
	defer pb.Close()

	bw := bufio.NewWriter(w)
	if _, err := bw.Write(bundleMagic[:]); err != nil {
		return fmt.Errorf("clone: write magic: %w", err)
	}
	if err := bw.WriteByte(bundleVersion); err != nil {
		return fmt.Errorf("clone: write version: %w", err)
	}

	metaPath, appPath, err := pb.Files()
	if err != nil {
		return err
	}

	if err := writeFileFrame(bw, nameMetaDB, metaPath); err != nil {
		return err
	}
	if err := writeFileFrame(bw, nameAppDB, appPath); err != nil {
		return err
	}
	return bw.Flush()
}

// Close releases backup handles, source/destination connections, and
// removes any temp files. Idempotent. Safe to call after Stream
// (which always frees resources internally) and on error paths.
func (pb *PinnedBundle) Close() {
	if pb == nil {
		return
	}
	if pb.cluster != nil {
		pb.cluster.close()
		pb.cluster = nil
	}
	if pb.app != nil {
		pb.app.close()
		pb.app = nil
	}
}

// Adopt reads a bundle from r and materializes it at dstAppDB, then
// rewrites identity (fresh node_id, reset sender_seq, etc.) via
// metadata.AdoptClone. Refuses if dstAppDB or its metadata dir already
// exists; the caller is responsible for moving any existing files
// aside before invoking.
//
// The newly-minted origin id is returned for logging.
func Adopt(r io.Reader, dstAppDB string) (crdt.Origin, error) {
	if dstAppDB == "" {
		return 0, errors.New("clone: Adopt: empty dstAppDB")
	}
	metaDir := layout.MetaDir(dstAppDB)
	if _, err := os.Stat(dstAppDB); err == nil {
		return 0, fmt.Errorf("clone: %s already exists; move it aside before cloning", dstAppDB)
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("clone: stat %s: %w", dstAppDB, err)
	}
	if _, err := os.Stat(metaDir); err == nil {
		return 0, fmt.Errorf("clone: %s already exists; move it aside before cloning", metaDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("clone: stat %s: %w", metaDir, err)
	}

	br := bufio.NewReader(r)
	if err := readMagic(br); err != nil {
		return 0, err
	}

	// Stage everything alongside the final paths so the rename is a
	// same-filesystem move (atomic) and a partial clone leaves visible
	// `.tmp` cruft the operator can rm.
	stageMeta := metaDir + ".tmp"
	stageApp := dstAppDB + ".tmp"
	if err := os.RemoveAll(stageMeta); err != nil {
		return 0, fmt.Errorf("clone: clear stage %s: %w", stageMeta, err)
	}
	if err := os.Remove(stageApp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("clone: clear stage %s: %w", stageApp, err)
	}
	if err := os.MkdirAll(stageMeta, 0o755); err != nil {
		return 0, fmt.Errorf("clone: mkdir stage metadata: %w", err)
	}
	cleanupOK := false
	defer func() {
		if cleanupOK {
			return
		}
		_ = os.RemoveAll(stageMeta)
		_ = os.Remove(stageApp)
	}()

	// Two files, in declared order: metadata.db then app.db.
	if err := readNamedFile(br, nameMetaDB, filepath.Join(stageMeta, "metadata.db")); err != nil {
		return 0, err
	}
	if err := readNamedFile(br, nameAppDB, stageApp); err != nil {
		return 0, err
	}

	newOrigin, err := layout.MintOrigin()
	if err != nil {
		return 0, fmt.Errorf("clone: mint origin: %w", err)
	}
	now := nowHLC()
	sc, err := metadata.Open(filepath.Join(stageMeta, "metadata.db"))
	if err != nil {
		return 0, fmt.Errorf("clone: open staged metadata: %w", err)
	}
	if err := sc.AdoptClone(newOrigin, now); err != nil {
		_ = sc.Close()
		return 0, fmt.Errorf("clone: AdoptClone: %w", err)
	}
	if err := sc.Close(); err != nil {
		return 0, fmt.Errorf("clone: close staged metadata: %w", err)
	}

	// Pre-create the new origin's directory + empty journal under the
	// staged metadata. Without this, sqlite.Open's layout.Acquire(pinned=0)
	// finds an empty origins/ dir and mints a *different* random origin,
	// silently desyncing from the sender_seq + frontier rows AdoptClone
	// just seeded. With the dir present, Acquire's Recycle path picks
	// our origin up cleanly.
	if err := os.MkdirAll(layout.OriginJournalDirIn(stageMeta, newOrigin), 0o755); err != nil {
		return 0, fmt.Errorf("clone: mkdir new origin dir: %w", err)
	}

	// Meta dir first: an interrupted clone that leaves the dir but
	// not the app.db lets the operator retry without cleanup, since
	// the next Adopt sees an existing metadata dir and refuses.
	if err := os.Rename(stageMeta, metaDir); err != nil {
		return 0, fmt.Errorf("clone: rename metadata: %w", err)
	}
	if err := os.Rename(stageApp, dstAppDB); err != nil {
		_ = os.RemoveAll(metaDir)
		return 0, fmt.Errorf("clone: rename app.db (metadata dir reverted): %w", err)
	}
	cleanupOK = true
	return newOrigin, nil
}

// readNamedFile reads one (name, payload) frame from r and writes
// payload to dstPath. The name must match expected; otherwise the
// frame is malformed for our schema.
func readNamedFile(r *bufio.Reader, expected, dstPath string) error {
	name, err := readName(r)
	if err != nil {
		return err
	}
	if name != expected {
		return fmt.Errorf("clone: bundle entry %q != %q", name, expected)
	}
	n, err := readUvarint(r)
	if err != nil {
		return fmt.Errorf("clone: read %s len: %w", expected, err)
	}
	if n > MaxFileBytes {
		return fmt.Errorf("clone: %s len %d exceeds max %d", expected, n, MaxFileBytes)
	}
	f, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("clone: create %s: %w", dstPath, err)
	}
	defer f.Close()
	if got, err := io.CopyN(f, r, int64(n)); err != nil {
		return fmt.Errorf("clone: copy %s after %d/%d: %w", expected, got, n, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("clone: fsync %s: %w", dstPath, err)
	}
	return nil
}

func readMagic(r *bufio.Reader) error {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return fmt.Errorf("clone: read magic: %w", err)
	}
	if [4]byte(head[:4]) != bundleMagic {
		return fmt.Errorf("clone: bad magic %q (want %q)", head[:4], bundleMagic[:])
	}
	if head[4] != bundleVersion {
		return fmt.Errorf("clone: unknown bundle version %d (want %d)", head[4], bundleVersion)
	}
	return nil
}

func writeName(w io.Writer, name string) error {
	if err := writeUvarint(w, uint64(len(name))); err != nil {
		return fmt.Errorf("clone: write name_len: %w", err)
	}
	if _, err := io.WriteString(w, name); err != nil {
		return fmt.Errorf("clone: write name: %w", err)
	}
	return nil
}

func readName(r *bufio.Reader) (string, error) {
	n, err := readUvarint(r)
	if err != nil {
		return "", fmt.Errorf("clone: read name_len: %w", err)
	}
	if n > 256 {
		return "", fmt.Errorf("clone: name_len %d implausible", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("clone: read name: %w", err)
	}
	return string(buf), nil
}

func writeUvarint(w io.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := w.Write(buf[:n])
	return err
}

func readUvarint(r *bufio.Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

// nowHLC returns the current wall-clock millisecond as an HLC with
// logical=0. Used to ensure the adopted metadata's hlc_last cannot
// regress relative to the current host's clock.
func nowHLC() crdt.Clock {
	return crdt.Clock{WallTime: time.Now().UnixMilli(), Logical: 0}
}
