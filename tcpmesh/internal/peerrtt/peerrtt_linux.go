//go:build linux

package peerrtt

import (
	"crypto/tls"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

// PeerRTT reads the kernel's smoothed RTT and last-data-recv age
// for c. Returns zero values when c is not a TCP socket (Unix
// socket, etc.), when the syscall fails, or when the kernel hasn't
// computed a sample yet.
func PeerRTT(c net.Conn) (rtt, rttVar, lastRecv time.Duration) {
	tcpConn := tcpConnFrom(c)
	if tcpConn == nil {
		return 0, 0, 0
	}
	raw, err := tcpConn.SyscallConn()
	if err != nil {
		return 0, 0, 0
	}
	var info *unix.TCPInfo
	var infoErr error
	if cerr := raw.Control(func(fd uintptr) {
		info, infoErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	}); cerr != nil || infoErr != nil || info == nil {
		return 0, 0, 0
	}
	return time.Duration(info.Rtt) * time.Microsecond,
		time.Duration(info.Rttvar) * time.Microsecond,
		time.Duration(info.Last_data_recv) * time.Millisecond
}

// tcpConnFrom unwraps a *tls.Conn to its underlying *net.TCPConn
// when possible. Returns nil for Unix sockets and other non-TCP
// conns.
func tcpConnFrom(c net.Conn) *net.TCPConn {
	switch v := c.(type) {
	case *net.TCPConn:
		return v
	case *tls.Conn:
		if inner, ok := v.NetConn().(*net.TCPConn); ok {
			return inner
		}
	}
	return nil
}
