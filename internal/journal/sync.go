package journal

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// SyncMode controls whether Sync() flushes the active segment to
// disk via msync. SyncOff leaves trailing-record durability to the
// kernel page cache (host-crash window open); SyncOn closes the
// window at the cost of one msync per Append (and an fdatasync +
// directory fsync on segment rotation). See ARCHITECTURE.md
// "Host-Level Desync".
type SyncMode int

const (
	SyncOff SyncMode = iota
	SyncOn
)

func (m SyncMode) String() string {
	if m == SyncOn {
		return "on"
	}
	return "off"
}

// msyncRange flushes the page-aligned mmap range covered by b to disk
// using msync(MS_SYNC). The slice must be backed by an mmap whose base
// address is page-aligned (true for any mmap with offset 0).
//
// unix.Msync rather than a raw syscall: darwin routes system calls
// through libSystem and does not define the msync syscall number at
// all, so the raw form is Linux-only by construction.
func msyncRange(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	if err := unix.Msync(b, unix.MS_SYNC); err != nil {
		return fmt.Errorf("msync: %w", err)
	}
	return nil
}
