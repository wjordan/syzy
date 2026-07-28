//go:build !linux

package vsock

import (
	"errors"
	"net"
)

func dialAFVsock(_, _ uint32) (net.Conn, error) {
	return nil, errors.New("vsock: AF_VSOCK only supported on Linux")
}
