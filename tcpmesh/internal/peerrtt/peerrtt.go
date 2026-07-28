//go:build !linux

// Package peerrtt reads kernel-level TCP smoothed-RTT samples for
// a net.Conn. Used by transport/mux for the
// PeerStat reports the broker consumes to pick the lowest-RTT
// outbound peer.
//
// Non-Linux build: returns zeros (RTT unavailable). The Linux
// build calls getsockopt(TCP_INFO).
package peerrtt

import (
	"net"
	"time"
)

// PeerRTT returns the kernel's smoothed RTT, RTT variance, and
// last-data-recv age for c. Zero values mean "RTT unavailable" —
// not a TCP socket, syscall failed, or no sample yet.
func PeerRTT(_ net.Conn) (rtt, rttVar, lastRecv time.Duration) {
	return 0, 0, 0
}
