package tcpmesh

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"time"

	"github.com/wjordan/syzy/tcpmesh/internal/catchupwire"
	"github.com/wjordan/syzy/transport"
)

// Wire format for op 0x01 (catchup):
//
//	request body (after op + topic prefix): see catchupwire.Read/Write
//
//	response:
//	  byte    status (0x00 OK; non-zero per the status table —
//	                  server closes immediately)
//	  if OK:
//	    zero or more frames of { u32 BE len, len bytes payload },
//	    terminated by clean EOF
//
// Payload bytes are the canonical Changeset wire format.

// serveCatchup dispatches a catchup request to the channel's
// registered CatchupSource. Called from serveOneShot after the
// topic prefix resolved c.
func serveCatchup(c *Channel, conn net.Conn) {
	req, err := catchupwire.Read(conn)
	if err != nil {
		_ = writeStatus(conn, StatusBadRequest)
		return
	}
	c.mu.Lock()
	src := c.catchupSrc
	c.mu.Unlock()
	if src == nil {
		_ = writeStatus(conn, StatusNoHandler)
		return
	}
	// Request consumed; clear handshake deadline so long scans
	// aren't tripped.
	_ = conn.SetReadDeadline(time.Time{})
	if err := writeStatus(conn, StatusOK); err != nil {
		return
	}
	// Stop the serve when the mesh or this channel closes.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-c.mesh.done:
			cancel()
		case <-c.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	write := func(payload []byte) error {
		var hdr [4]byte
		if uint32(len(payload)) > MaxFrameSize {
			return fmt.Errorf("tcpmesh: catchup payload %d exceeds MaxFrameSize", len(payload))
		}
		_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
		if _, err := conn.Write(hdr[:]); err != nil {
			return err
		}
		_, err := conn.Write(payload)
		return err
	}
	// Status byte already 0x00 OK; mid-stream errors close the conn
	// after partial frames. The catchup chain's idempotency
	// recovers via the next round.
	_ = src.Serve(ctx, req, write)
}

// PeerGapFiller is a transport.GapFiller that pulls missing
// ranges directly from a connected peer. It is per-Channel, so peer
// selection naturally respects topic membership (Channel.PeerStats
// filters to topic-advertising peers); each peer is dialed at its
// gossip address, which serves every one-shot op. Compose with
// s3fetch.Source via gapfillerchain so the broker tries peer-pull
// first and falls back to object storage only when needed.
type PeerGapFiller struct {
	// Channel scopes the gap-fill to one topic.
	Channel *Channel
	// Timeout bounds the total wall-clock cost of a Fetch call
	// across every peer attempt. 0 → 10s.
	Timeout time.Duration
}

// Fetch picks the lowest-RTT topic-advertising peer and asks it for
// ranges. On dial or transport error (including non-zero status),
// falls through to the next peer; the broker's runFetchRound moves
// on to the next GapFiller in the chain once all peers are
// exhausted. The broker dedupes by (origin, seq), so re-requesting
// the same ranges next round is free.
func (p *PeerGapFiller) Fetch(ctx context.Context, ranges []transport.Range, apply transport.ApplyFunc) error {
	if p.Channel == nil {
		return errors.New("tcpmesh: PeerGapFiller.Channel required")
	}
	if len(ranges) == 0 {
		return nil
	}
	stats := p.Channel.PeerStats()
	if len(stats) == 0 {
		return errors.New("tcpmesh: no outbound peers for catchup")
	}
	sort.SliceStable(stats, func(i, j int) bool {
		ri, rj := stats[i].RTT, stats[j].RTT
		// Peers with a zero (un-sampled) RTT sort last so we
		// exhaust peers with real signals first.
		if (ri == 0) != (rj == 0) {
			return ri != 0
		}
		return ri < rj
	})
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	deadline := time.Now().Add(timeout)
	tlsCfg := p.Channel.mesh.cfg.TLSConfig
	topic := p.Channel.topic

	var firstErr error
	for _, st := range stats {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		// Wrap apply to count delivered frames. A clean EOF with
		// zero frames means the peer didn't have any of the
		// requested ranges; fall through to the next-best peer
		// rather than returning success and stalling until the
		// broker's next round.
		var delivered int
		wrapped := func(ctx context.Context, payload []byte) error {
			delivered++
			return apply(ctx, payload)
		}
		err := fetchCatchupFromPeer(ctx, st.Addr, topic, tlsCfg, remaining, ranges, wrapped)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if delivered == 0 {
			// Peer accepted the request but produced nothing.
			// Record a synthetic error so the caller doesn't see a
			// spurious nil when no peer delivered anything.
			// Typed so callers expecting emptiness (the unserveable-range
			// probe) can tell it from a substantive failure.
			if firstErr == nil {
				firstErr = fmt.Errorf("tcpmesh: catchup %s: peer delivered 0 frames: %w", st.Addr, transport.ErrUnfilled)
			}
			continue
		}
		return nil
	}
	if firstErr == nil {
		return errors.New("tcpmesh: catchup timed out before any peer answered")
	}
	return firstErr
}

// fetchCatchupFromPeer dials a single peer, sends the catchup op +
// topic prefix + request body, reads the status byte, and routes
// each response frame through apply.
func fetchCatchupFromPeer(
	ctx context.Context,
	addr, topic string,
	tlsCfg *tls.Config,
	timeout time.Duration,
	ranges []transport.Range,
	apply transport.ApplyFunc,
) error {
	conn, err := dialOp(ctx, addr, topic, opCatchupRequest, tlsCfg, timeout, func(conn net.Conn) error {
		return catchupwire.Write(conn, transport.CatchupRequest{Ranges: ranges})
	})
	if err != nil {
		return err
	}
	defer conn.Close()
	var hdr [4]byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := io.ReadFull(conn, hdr[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // partial close after some frames is fine
			}
			return fmt.Errorf("tcpmesh: read catchup frame header: %w", err)
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n == 0 {
			continue
		}
		if n > MaxFrameSize {
			return fmt.Errorf("tcpmesh: catchup frame %d exceeds MaxFrameSize", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return fmt.Errorf("tcpmesh: read catchup payload: %w", err)
		}
		if err := apply(ctx, payload); err != nil {
			return err
		}
	}
}

var _ transport.GapFiller = (*PeerGapFiller)(nil)
