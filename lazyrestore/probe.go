//go:build linux

package lazyrestore

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	passthroughOnce sync.Once
	passthroughOK   bool
)

// PassthroughAvailable reports whether kernel FUSE passthrough works
// on this host (kernel >= 6.9 with CONFIG_FUSE_PASSTHROUGH +
// CAP_SYS_ADMIN), probed once with a throwaway loopback mount and a
// real backing-fd registration — the same negotiation Mount performs;
// the kernel init flag alone misses the missing-capability case.
//
// Without passthrough a Mount degrades to serving loopback faults
// from this process's own FUSE goroutines, and a fault outstanding
// when the GC stops the world can deadlock the process. Out-of-process
// consumers do not create that cycle; in-process mmap consumers should require
// passthrough.
func PassthroughAvailable() bool {
	passthroughOnce.Do(func() { passthroughOK = probePassthrough() })
	return passthroughOK
}

func probePassthrough() bool {
	dir, err := os.MkdirTemp("", "syzy-lazyrestore-fuse-probe-")
	if err != nil {
		return false
	}
	defer os.RemoveAll(dir)
	mnt := filepath.Join(dir, "mnt")
	back := filepath.Join(dir, "back")
	if os.Mkdir(mnt, 0o700) != nil || os.Mkdir(back, 0o700) != nil {
		return false
	}
	f, err := os.CreateTemp(back, "backing-")
	if err != nil {
		return false
	}
	defer f.Close()

	root, err := fs.NewLoopbackRoot(back)
	if err != nil {
		return false
	}
	srv, err := fs.Mount(mnt, root, &fs.Options{
		MountOptions: fuse.MountOptions{DirectMount: true},
	})
	if err != nil {
		return false
	}
	cloexecFuseFDs() // go-fuse opens /dev/fuse without O_CLOEXEC; see cloexec.go
	defer func() {
		_ = srv.Unmount()
		srv.Wait()
	}()
	id, errno := srv.RegisterBackingFd(&fuse.BackingMap{Fd: int32(f.Fd())})
	if errno != 0 {
		return false
	}
	_ = srv.UnregisterBackingFd(id)
	return true
}
