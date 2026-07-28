package journal

import (
	"os"

	"golang.org/x/sys/unix"
)

// fdatasyncFile flushes f's contents and size to durable storage.
//
// darwin has no fdatasync, and its fsync(2) deliberately returns once
// the data reaches the drive — not once the drive has committed it —
// so a power loss can still lose an fsync'd write. F_FULLFSYNC is the
// call that waits for the drive to flush its own cache, making this
// match the Linux path's guarantee rather than quietly weakening the
// journal's durability on one platform.
//
// It is the slower of the two, but this runs on segment creation and
// rotation, not per record: the per-record durability path is msync
// over the mapping. Paying for correctness here is not measurable in
// append throughput.
func fdatasyncFile(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	return err
}
