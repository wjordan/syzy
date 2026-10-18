package journal

import (
	"os"
	"syscall"
)

func openInspectionFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NOATIME|syscall.O_NONBLOCK, 0)
}
