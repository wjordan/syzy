package pgwire

import (
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

// removeStaleSocket removes path if nothing is listening on it. A
// sidecar that crashed leaves the socket file behind and net.Listen
// would fail with EADDRINUSE even though no process holds it; blindly
// unlinking instead would let a second sidecar steal a live endpoint
// out from under the first. Dialing distinguishes the two.
func removeStaleSocket(path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	conn, err := net.DialTimeout("unix", path, 250*time.Millisecond)
	if err == nil {
		conn.Close()
		return fmt.Errorf("pgwire: %s is already served by a live process", path)
	}
	return os.Remove(path)
}
