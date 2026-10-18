package mirror

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// EmptyMirrorPlan is evidence for one narrowly classified offline repair.
// Directory and segment identities are retained privately for revalidation.
type EmptyMirrorPlan struct {
	Directory string `json:"directory"`
	Origin    uint64 `json:"origin"`
	Size      int64  `json:"bytes"`
	SHA256    string `json:"sha256"`
	dirInfo   os.FileInfo
	fileInfo  os.FileInfo
}

// InspectEmptyMirror never opens a database or invokes journal recovery.
func InspectEmptyMirror(root string, origin crdt.Origin) (EmptyMirrorPlan, error) {
	var plan EmptyMirrorPlan
	if origin == 0 {
		return plan, fmt.Errorf("origin must be nonzero")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return plan, err
	}
	real, err := filepath.EvalSymlinks(root)
	if err != nil || real != root {
		return plan, fmt.Errorf("mirror root must exist without symlink components: %s", root)
	}
	dir := filepath.Join(root, fmt.Sprintf("origin_%d", origin))
	dirInfo, err := os.Lstat(dir)
	if err != nil {
		return plan, err
	}
	if !dirInfo.IsDir() {
		return plan, fmt.Errorf("origin is not a real directory: %s", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return plan, err
	}
	if len(entries) != 1 || entries[0].Name() != "seg-00000000.bin" {
		return plan, fmt.Errorf("repair requires exactly one first segment and no other entries: %s", dir)
	}
	path := filepath.Join(dir, entries[0].Name())
	info, err := os.Lstat(path)
	if err != nil {
		return plan, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return plan, fmt.Errorf("segment must be a regular file without symlinks")
	}
	inspected, hash, err := journal.InspectZeroSegment(path)
	if err != nil {
		return plan, err
	}
	currentDir, err := os.Lstat(dir)
	if err != nil || !os.SameFile(dirInfo, currentDir) || !dirInfo.ModTime().Equal(currentDir.ModTime()) ||
		!os.SameFile(info, inspected) || !info.ModTime().Equal(inspected.ModTime()) {
		return plan, fmt.Errorf("mirror changed during inspection")
	}
	return EmptyMirrorPlan{dir, uint64(origin), inspected.Size(), hash, dirInfo, inspected}, nil
}

// QuarantineEmptyMirror requires the caller to hold the owning daemon lock and
// stop every writer. It preserves the entire origin directory and an audit
// manifest; no database state, frontier, gap, or marker is edited. In particular
// it never installs donor records, which recovery could mistake for local DML.
func QuarantineEmptyMirror(plan EmptyMirrorPlan) (string, error) {
	return quarantineEmptyMirror(plan, os.Rename, syncRepairDir)
}

func quarantineEmptyMirror(plan EmptyMirrorPlan, rename func(string, string) error, syncDir func(string) error) (string, error) {
	root := filepath.Dir(plan.Directory)
	if filepath.Base(root) != "mirror" || plan.dirInfo == nil || plan.fileInfo == nil {
		return "", fmt.Errorf("not an inspected mirror repair plan")
	}
	current, err := InspectEmptyMirror(root, crdt.Origin(plan.Origin))
	if err != nil {
		return "", err
	}
	if current.Directory != plan.Directory || current.Size != plan.Size || current.SHA256 != plan.SHA256 ||
		!os.SameFile(current.dirInfo, plan.dirInfo) || !os.SameFile(current.fileInfo, plan.fileInfo) ||
		!current.fileInfo.ModTime().Equal(plan.fileInfo.ModTime()) {
		return "", fmt.Errorf("mirror changed since inspection")
	}
	recovery, err := os.MkdirTemp(filepath.Dir(root), "mirror-recovery-")
	if err != nil {
		return "", err
	}
	evidence := filepath.Join(recovery, "original")
	manifest, err := json.MarshalIndent(struct {
		Plan       EmptyMirrorPlan `json:"inspection"`
		Evidence   string          `json:"evidence"`
		PreparedAt time.Time       `json:"prepared_at"`
	}{plan, evidence, time.Now().UTC()}, "", "  ")
	if err != nil {
		return recovery, err
	}
	f, err := os.OpenFile(filepath.Join(recovery, "manifest.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return recovery, err
	}
	manifest = append(manifest, '\n')
	n, writeErr := f.Write(manifest)
	if writeErr == nil && n != len(manifest) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return recovery, writeErr
	}
	if closeErr != nil {
		return recovery, closeErr
	}
	if err := syncDir(recovery); err != nil {
		return recovery, err
	}
	if err := syncDir(filepath.Dir(recovery)); err != nil {
		return recovery, err
	}
	if err := rename(plan.Directory, evidence); err != nil {
		return recovery, err
	}
	// A failure from here may mean the move completed. Always retain and report
	// the evidence path, allowing inspection rather than an unsafe blind retry.
	if err := syncDir(recovery); err != nil {
		return recovery, err
	}
	if err := syncDir(root); err != nil {
		return recovery, err
	}
	return recovery, nil
}

func syncRepairDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
