//go:build !linux

package futex

import (
	"errors"
	"os"
	"time"
)

var (
	ErrTimeout             = errors.New("futex: wait timed out")
	ErrPlatformUnsupported = errors.New("futex: wait not supported on this platform")
)

func Wait(addr *uint32, expected uint32, timeout time.Duration) error {
	if timeout <= 0 {
		return ErrPlatformUnsupported
	}
	time.Sleep(timeout)
	return ErrTimeout
}

func WakeAll(addr *uint32) (int, error) {
	return 0, nil
}

// FileEligible / PathEligible: no futex support at all off-Linux, so no
// mapping is eligible; callers use their sleep-poll paths.
func FileEligible(f *os.File) bool  { return false }
func PathEligible(path string) bool { return false }

const Supported = false
