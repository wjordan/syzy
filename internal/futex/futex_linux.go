//go:build linux

// Package futex exposes Wait/WakeAll on a shared uint32 via the Linux
// futex(2) syscall. The journal uses it to publish record availability
// across processes that mmap the same segment (extension writer →
// daemon drainer). Futexes are per-kernel: cross-VM waiters need a
// wake transport (see wake) instead. Non-Linux builds degrade to a
// sleep-poll stub with Supported = false.
package futex

import (
	"errors"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// fuseSuperMagic is statfs f_type for every FUSE-based filesystem,
// including virtiofs (fs/fuse/inode.c pins both the superblock magic and
// the statfs reply to FUSE_SUPER_MAGIC).
const fuseSuperMagic = 0x65735546

// FileEligible reports whether futex(2) on a MAP_SHARED mapping of f is
// useful and safe. FUSE-backed files (including virtiofs, where the mapping
// may be DAX) are excluded on both counts: futexes are per-kernel, so a
// wake can never reach a waiter across the guest/host boundary — the
// bounded wait is already the real mechanism there — and get_futex_key's
// page pin on a DAX mapping races fuse invalidation (kernel WARNs); frozen
// across a VM snapshot it restarts against a dead superblock and can wedge
// the guest. Callers fall back to sleep-polling. A statfs failure reads as
// eligible so ordinary local filesystems keep the futex fast path.
func FileEligible(f *os.File) bool {
	var st syscall.Statfs_t
	if err := syscall.Fstatfs(int(f.Fd()), &st); err != nil {
		return true
	}
	return st.Type != fuseSuperMagic
}

// PathEligible is FileEligible for a path (used before any fd is held,
// e.g. a journal directory whose segment mmaps share the filesystem).
func PathEligible(path string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return true
	}
	return st.Type != fuseSuperMagic
}

const (
	opWait = 0
	opWake = 1
)

var ErrTimeout = errors.New("futex: wait timed out")

func Wait(addr *uint32, expected uint32, timeout time.Duration) error {
	var tsPtr unsafe.Pointer
	if timeout > 0 {
		ts := syscall.NsecToTimespec(int64(timeout))
		tsPtr = unsafe.Pointer(&ts)
	}
	_, _, errno := syscall.Syscall6(
		syscall.SYS_FUTEX,
		uintptr(unsafe.Pointer(addr)),
		uintptr(opWait),
		uintptr(expected),
		uintptr(tsPtr),
		0, 0,
	)
	switch errno {
	case 0, syscall.EAGAIN, syscall.EINTR:
		return nil
	case syscall.ETIMEDOUT:
		return ErrTimeout
	default:
		return errno
	}
}

func WakeAll(addr *uint32) (int, error) {
	n, _, errno := syscall.Syscall6(
		syscall.SYS_FUTEX,
		uintptr(unsafe.Pointer(addr)),
		uintptr(opWake),
		uintptr(int32(^uint32(0)>>1)),
		0, 0, 0,
	)
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

const Supported = true
