package mirror

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/transport"
)

// catchupHeaderLen is the byte count of the changeset wire-format
// prefix the serve path inspects to filter by (origin, seq) without
// decoding the full record. Layout (see crdt/codec.go):
//
//	byte 0       version
//	bytes 1..9   origin (BE u64)
//	bytes 9..17  seq    (BE u64)
const catchupHeaderLen = 17

// Serve implements transport.CatchupSource. For every (origin range)
// in req, iterate origin's mirror journal, parse the wire-format
// (origin, seq) prefix of each payload, and stream matching payloads
// to write. Stops on:
//
//   - ctx cancellation,
//   - first write error,
//   - MaxRecords / MaxBytes cap hit (both treated as 0 = unbounded),
//   - all journals scanned to the live tail.
//
// Records inside a per-origin mirror journal are written in apply
// order, not necessarily seq order — the broker accepts out-of-order
// arrivals (see internal/nodestate.Cache.MarkApplied). Serve therefore
// filters every scanned record and does NOT break early on the first
// seq > r.Hi.
//
// It does, however, skip whole segments below the request: a per-origin
// seek index (segSpan, built lazily by ensureIndex and kept current on
// append) records each segment's max seq, so startOffset begins
// iteration at the lowest segment that can hold a seq >= r.Lo. This
// keeps catchup cost proportional to the requested delta rather than to
// total retained history — the old Iterate(0) scanned every record ever
// mirrored for the origin. Skipping is safe because a segment is bypassed
// only when its max seq is below r.Lo; everything from the start segment
// to the live tail is still scanned, so out-of-order stragglers in later
// segments are found.
//
// Payloads are copied before being passed to write so the byte slice
// returned to the caller doesn't alias the journal's mmap-backed
// buffer. The journal's segment-retention design (RetainAfter only
// removes segments strictly older than the snapshot marker) keeps the
// segment containing the freshest records alive for the iterator's
// lifetime; older segments are protected as long as the catchup
// request's lo seq is past the GC frontier.
func (m *Manager) Serve(ctx context.Context, req transport.CatchupRequest, write func(payload []byte) error) error {
	if write == nil {
		return errors.New("mirror: Serve requires non-nil write")
	}
	var (
		recordsSent uint32
		bytesSent   uint64
		stats       ServeStats
	)
	defer func() {
		stats.RecordsSent = int(recordsSent)
		stats.BytesSent = bytesSent
		m.recordServeStats(stats)
		// A serve that streamed records is a real peer catchup; log it at
		// Info so the catchup cost (scan vs skip) is visible in prod when
		// validating the seek + GC. Empty probes stay at Debug.
		lvl := slog.LevelDebug
		if recordsSent > 0 {
			lvl = slog.LevelInfo
		}
		m.log.Log(ctx, lvl, "mirror: served catchup",
			"ranges", len(req.Ranges),
			"records_scanned", stats.RecordsScanned,
			"records_sent", stats.RecordsSent,
			"bytes_sent", stats.BytesSent,
			"segments_total", stats.SegmentsTotal,
			"segments_skipped", stats.SegmentsSkipped,
		)
	}()
	for _, r := range req.Ranges {
		if err := ctx.Err(); err != nil {
			return err
		}
		h, ok := m.lookupHandle(r.Origin)
		if !ok {
			continue // we have nothing for this origin
		}
		// Seek past whole segments below r.Lo instead of scanning the
		// journal from offset 0. An out-of-range or empty index yields
		// ok=false, meaning no segment can satisfy the request.
		h.ensureIndex()
		start, hit, total, skipped := h.startOffset(r.Lo)
		stats.SegmentsTotal += total
		stats.SegmentsSkipped += skipped
		if !hit {
			continue
		}
		it := h.j.Iterate(start)
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			rec, _, err := it.Next()
			if err == io.EOF || errors.Is(err, journal.ErrPending) {
				break
			}
			if err != nil {
				return err
			}
			stats.RecordsScanned++
			if rec.Kind != journal.KindMirror {
				continue
			}
			if len(rec.Payload) < catchupHeaderLen {
				continue
			}
			payloadOrigin := crdt.Origin(binary.BigEndian.Uint64(rec.Payload[1:9]))
			if payloadOrigin != r.Origin {
				// Defensive: the per-origin journal should only hold
				// payloads for its own origin, but a future refactor
				// could share journals. Re-check the wire prefix.
				continue
			}
			seq := crdt.Seq(binary.BigEndian.Uint64(rec.Payload[9:17]))
			if seq < r.Lo {
				continue
			}
			if !r.OpenEnded() && seq > r.Hi {
				// Out of range — but records aren't seq-sorted, so
				// keep scanning rather than break.
				continue
			}
			if req.MaxBytes > 0 && bytesSent+uint64(len(rec.Payload)) > req.MaxBytes && recordsSent > 0 {
				// Honour the cap before the write so the response
				// never exceeds the client's stated bound. We allow
				// the first record through unconditionally so a
				// large single payload still gets a chance to ship
				// (clients can retry with a higher cap if needed).
				return nil
			}
			cp := make([]byte, len(rec.Payload))
			copy(cp, rec.Payload)
			if err := write(cp); err != nil {
				return err
			}
			recordsSent++
			bytesSent += uint64(len(cp))
			if req.MaxRecords > 0 && recordsSent >= req.MaxRecords {
				return nil
			}
		}
	}
	return nil
}

var _ transport.CatchupSource = (*Manager)(nil)
