//go:build linux

package lazyrestore

import (
	"fmt"
	"syscall"
	"unsafe"
)

// CloneProvider satisfies the source-of-pages-via-FICLONERANGE
// contract for a Mount. A host can implement it by scanning its
// live mount set; tests can stub it directly. nil disables sibling
// clone and pages always come from object storage.
type CloneProvider interface {
	// TryClonePage attempts a reflink of one page from any live
	// mount whose manifest entry, presence bit, and clean bit match
	// loc/pgno. requesterID lets the implementation skip the
	// requester when scanning. dstFD/dstOff/length identify
	// the destination range in the requester's backing file.
	//
	// Returns (true, nil) on a successful FICLONERANGE.
	// (false, nil) when no eligible source exists or reflink isn't
	// supported on the filesystem — caller falls back to
	// object-store fetch. (false, err) for unexpected failures the
	// caller may surface or log.
	//
	// Implementations must hold the source mount's writeMu read
	// lock for the duration of the predicate check + ioctl so a
	// concurrent local write cannot dirty the page between the
	// "clean" verdict and the byte copy.
	TryClonePage(requesterID string, loc Page, pgno uint32, dstFD int, dstOff int64, length int64) (bool, error)
}

// CanCloneFrom reports whether this Mount is a valid source for a
// FICLONERANGE of pgno against loc. Predicate-only: caller wraps
// the predicate + ioctl in writeMu.RLock to prevent a write from
// invalidating the verdict between the check and the syscall.
func (m *Mount) CanCloneFrom(pgno uint32, loc Page) bool {
	if m == nil {
		return false
	}
	if pgno < 1 || pgno > m.manifest.CommitPages {
		return false
	}
	own, ok := m.manifest.Pages[pgno]
	if !ok || own != loc {
		return false
	}
	return m.bitmap.isSet(pgno) && m.cleanBitmap.isSet(pgno)
}

// CloneTo holds the source mount's writeMu read lock and, if
// CanCloneFrom still holds, issues FICLONERANGE from this mount's
// backing fd into dstFD at dstOff for length bytes. Returns
// (true, nil) on success.
//
// Used by the registry's CloneProvider implementation.
func (m *Mount) CloneTo(pgno uint32, loc Page, dstFD int, dstOff int64, length int64) (bool, error) {
	if m == nil {
		return false, nil
	}
	m.writeMu.RLock()
	defer m.writeMu.RUnlock()
	if !m.CanCloneFrom(pgno, loc) {
		return false, nil
	}
	srcOff := int64(pgno-1) * int64(m.manifest.PageSize)
	if err := ficloneRange(m.backingFD, srcOff, dstFD, dstOff, uint64(length)); err != nil {
		// EOPNOTSUPP / EXDEV / EINVAL from filesystems without
		// reflink support are expected and benign — caller falls
		// back to object-store fetch. Other errors are real.
		if isExpectedCloneFailure(err) {
			return false, nil
		}
		return false, fmt.Errorf("ficlonerange src=%d dst=%d+%d len=%d: %w", srcOff, dstFD, dstOff, length, err)
	}
	return true, nil
}

// fileCloneRange is the struct ioctl(2) FICLONERANGE consumes. Layout
// matches Linux's struct file_clone_range from <linux/fs.h>:
//
//	__s64 src_fd;
//	__u64 src_offset;
//	__u64 src_length;
//	__u64 dest_offset;
type fileCloneRange struct {
	SrcFD     int64
	SrcOffset uint64
	SrcLength uint64
	DstOffset uint64
}

// FICLONERANGE: _IOW('X', 13, struct file_clone_range). _IOW is
// computed from the size of the parameter struct (32 bytes).
const ficloneRangeOp = 0x4020940d

func ficloneRange(srcFD int, srcOff int64, dstFD int, dstOff int64, length uint64) error {
	arg := fileCloneRange{
		SrcFD:     int64(srcFD),
		SrcOffset: uint64(srcOff),
		SrcLength: length,
		DstOffset: uint64(dstOff),
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(dstFD), uintptr(ficloneRangeOp), uintptr(unsafe.Pointer(&arg)))
	if errno != 0 {
		return errno
	}
	return nil
}

// isExpectedCloneFailure reports the kernel errnos a non-reflink-
// capable filesystem returns. Caller treats these as "no clone
// available; use object fetch" rather than fatal errors.
func isExpectedCloneFailure(err error) bool {
	// EOPNOTSUPP and ENOTSUP are the same errno on Linux; list one.
	switch err {
	case syscall.EOPNOTSUPP, syscall.EXDEV, syscall.EINVAL, syscall.ENOTTY:
		return true
	}
	return false
}
