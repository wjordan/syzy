package vsock

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// DialAddr parses a SYZY_WAKE_VSOCK-style spec and returns a dial
// function suitable for NewWaker.
//
// Supported specs:
//
//	unix:/run/syzy/<vmID>/vsock.sock_7849     AF_UNIX (tests + the
//	                                          host-side Unix socket
//	                                          cloud-hypervisor exposes
//	                                          per guest vsock port)
//
//	vsock:2:7849                              AF_VSOCK (CID:port; the
//	                                          guest-userspace path,
//	                                          CID=2 is host)
//
// The returned dial function is called by NewWaker each time it
// needs to (re)connect.
func DialAddr(spec string) (func() (net.Conn, error), error) {
	if spec == "" {
		return nil, errors.New("vsock: empty dial spec")
	}
	switch {
	case strings.HasPrefix(spec, "unix:"):
		path := strings.TrimPrefix(spec, "unix:")
		if path == "" {
			return nil, errors.New("vsock: empty unix path")
		}
		return func() (net.Conn, error) {
			return net.DialTimeout("unix", path, 2*time.Second)
		}, nil
	case strings.HasPrefix(spec, "vsock:"):
		rest := strings.TrimPrefix(spec, "vsock:")
		parts := strings.Split(rest, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("vsock: expected vsock:CID:PORT, got %q", spec)
		}
		cid64, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("vsock: bad CID %q: %w", parts[0], err)
		}
		port64, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("vsock: bad port %q: %w", parts[1], err)
		}
		cid := uint32(cid64)
		port := uint32(port64)
		return func() (net.Conn, error) {
			return dialAFVsock(cid, port)
		}, nil
	default:
		return nil, fmt.Errorf("vsock: unrecognized scheme in %q (want unix: or vsock:)", spec)
	}
}
