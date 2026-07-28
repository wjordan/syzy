package journal

import (
	"os"
	"syscall"
)

// fdatasyncFile flushes f's contents and size to durable storage,
// without the extra metadata (times) an fsync would also push.
func fdatasyncFile(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
