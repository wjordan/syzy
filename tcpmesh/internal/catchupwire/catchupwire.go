// Package catchupwire holds the wire codec for tcpmesh's catchup
// request body. The format is hand-rolled big-endian binary,
// matching crdt/codec.go's style.
//
// Wire (request body, after any caller-supplied op + topic prefix):
//
//	u32 BE   nRanges
//	nRanges × { u64 BE origin, u64 BE lo, u64 BE hi }   // hi=0 → open-ended
//	u32 BE   maxRecords    (0 = unbounded)
//	u64 BE   maxBytes      (0 = unbounded)
//
// The response stream is the caller's concern; this package only
// covers the request body shape.
package catchupwire

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

// MaxRanges caps the number of ranges in a single catchup request.
// Bounded so a malformed/abusive peer can't make the server
// allocate unbounded request memory.
const MaxRanges = 1024

const rangeBytes = 24 // 8 origin + 8 lo + 8 hi

// Read parses a catchup request body from r. Errors are returned
// verbatim; callers that need a transport-prefixed error wrap.
func Read(r io.Reader) (transport.CatchupRequest, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return transport.CatchupRequest{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxRanges {
		return transport.CatchupRequest{}, fmt.Errorf("catchup request: %d ranges exceeds cap %d", n, MaxRanges)
	}
	body := make([]byte, int(n)*rangeBytes+4+8)
	if _, err := io.ReadFull(r, body); err != nil {
		return transport.CatchupRequest{}, err
	}
	req := transport.CatchupRequest{Ranges: make([]transport.Range, n)}
	off := 0
	for i := range req.Ranges {
		req.Ranges[i].Origin = crdt.Origin(binary.BigEndian.Uint64(body[off:]))
		off += 8
		req.Ranges[i].Lo = crdt.Seq(binary.BigEndian.Uint64(body[off:]))
		off += 8
		req.Ranges[i].Hi = crdt.Seq(binary.BigEndian.Uint64(body[off:]))
		off += 8
	}
	req.MaxRecords = binary.BigEndian.Uint32(body[off:])
	off += 4
	req.MaxBytes = binary.BigEndian.Uint64(body[off:])
	return req, nil
}

// Write serializes req onto w in a single allocation, sized so the
// server can read it in two ReadFulls (4-byte count, then a
// fixed-size body).
func Write(w io.Writer, req transport.CatchupRequest) error {
	if len(req.Ranges) > MaxRanges {
		return fmt.Errorf("catchup request: %d ranges exceeds cap %d", len(req.Ranges), MaxRanges)
	}
	body := make([]byte, 4+len(req.Ranges)*rangeBytes+4+8)
	binary.BigEndian.PutUint32(body[0:], uint32(len(req.Ranges)))
	off := 4
	for _, r := range req.Ranges {
		binary.BigEndian.PutUint64(body[off:], uint64(r.Origin))
		off += 8
		binary.BigEndian.PutUint64(body[off:], uint64(r.Lo))
		off += 8
		binary.BigEndian.PutUint64(body[off:], uint64(r.Hi))
		off += 8
	}
	binary.BigEndian.PutUint32(body[off:], req.MaxRecords)
	off += 4
	binary.BigEndian.PutUint64(body[off:], req.MaxBytes)
	_, err := w.Write(body)
	return err
}
