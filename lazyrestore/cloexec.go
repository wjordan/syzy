//go:build linux

package lazyrestore

import (
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// cloexecFuseFDs sets close-on-exec on every /dev/fuse fd this process
// holds. go-fuse's mount path opens the device without O_CLOEXEC
// (hanwen/go-fuse fuse/mount_linux.go), so without this every child
// spawned while a mount lives inherits the connection fd — and a child
// that outlives us keeps the mount undead: alive at the kernel, served by
// nobody, hanging every walker. The fork-vs-sweep race window is a few
// microseconds after fs.Mount returns; callers invoke this immediately
// after mounting, before the mount is announced to anything that spawns.
func cloexecFuseFDs() {
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return
	}
	for _, e := range ents {
		fd, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil || target != "/dev/fuse" {
			continue
		}
		unix.CloseOnExec(fd)
	}
}
