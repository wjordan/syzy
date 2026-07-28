//go:build linux

package vsock

import (
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

type vsockAddr struct {
	cid  uint32
	port uint32
}

func (a vsockAddr) Network() string { return "vsock" }
func (a vsockAddr) String() string  { return fmt.Sprintf("vsock(%d:%d)", a.cid, a.port) }

// fdConn implements net.Conn directly because net.FileConn does not recognize
// AF_VSOCK. The descriptor is nonblocking so os.File integrates with Go's
// poller and supports the deadlines required by stream RPC.
type fdConn struct {
	file   *os.File
	local  vsockAddr
	remote vsockAddr
}

func (c *fdConn) Read(p []byte) (int, error)         { return c.file.Read(p) }
func (c *fdConn) Write(p []byte) (int, error)        { return c.file.Write(p) }
func (c *fdConn) Close() error                       { return c.file.Close() }
func (c *fdConn) LocalAddr() net.Addr                { return c.local }
func (c *fdConn) RemoteAddr() net.Addr               { return c.remote }
func (c *fdConn) SetDeadline(t time.Time) error      { return c.file.SetDeadline(t) }
func (c *fdConn) SetReadDeadline(t time.Time) error  { return c.file.SetReadDeadline(t) }
func (c *fdConn) SetWriteDeadline(t time.Time) error { return c.file.SetWriteDeadline(t) }

func newFDConn(fd int, remote vsockAddr) (*fdConn, error) {
	if err := unix.SetNonblock(fd, true); err != nil {
		return nil, err
	}
	local := vsockAddr{cid: unix.VMADDR_CID_ANY}
	if sa, err := unix.Getsockname(fd); err == nil {
		if vm, ok := sa.(*unix.SockaddrVM); ok {
			local = vsockAddr{cid: vm.CID, port: vm.Port}
		}
	}
	return &fdConn{
		file:   os.NewFile(uintptr(fd), fmt.Sprintf("vsock-%d-%d", remote.cid, remote.port)),
		local:  local,
		remote: remote,
	}, nil
}

// dialAFVsock opens an AF_VSOCK stream socket and connects to (cid, port).
func dialAFVsock(cid, port uint32) (net.Conn, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("vsock socket: %w", err)
	}
	for {
		err = unix.Connect(fd, &unix.SockaddrVM{CID: cid, Port: port})
		if err != unix.EINTR {
			break
		}
	}
	if err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("vsock connect cid=%d port=%d: %w", cid, port, err)
	}
	conn, err := newFDConn(fd, vsockAddr{cid: cid, port: port})
	if err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("vsock connection: %w", err)
	}
	return conn, nil
}
