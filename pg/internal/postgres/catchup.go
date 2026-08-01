package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/transport"
)

// CatchupSource returns this engine's transport.CatchupSource: the seam a
// transport's CatchupRegistrar binds so peers asking for missed (origin, seq)
// ranges over the catchup endpoint reach this node's byte stores. The
// returned source routes per-origin:
//
//   - cfg.Origin (own bytes) -> the self-log journal
//   - any other origin       -> the mirror.Manager journals (cfg.Mirror)
//
// A node thus serves any (origin, seq) it has ever produced (self-log) or
// applied (mirror) — parity with the SQLite mirror.Manager which already
// implements transport.CatchupSource. Returns nil if neither store is wired
// (a zero-config Engine has no catchup bytes to offer).
func (e *Engine) CatchupSource() transport.CatchupSource {
	if e.selfLog == nil && e.cfg.Mirror == nil {
		return nil
	}
	src := &catchupSource{
		selfOrigin: e.cfg.Origin,
		selfLog:    e.selfLog,
	}
	// Nil-check the concrete *mirror.Manager directly. Routing it through an
	// interface-typed adapter would defeat the check: a nil *mirror.Manager
	// boxed into an interface is itself non-nil, so the guard would pass and
	// leave src.mirror a method value bound to a nil receiver — a panic on the
	// first foreign-origin request (the self-log-only config is supported).
	if e.cfg.Mirror != nil {
		src.mirror = e.cfg.Mirror.Serve
	}
	return src
}

type serveFn func(ctx context.Context, req transport.CatchupRequest, write func(payload []byte) error) error

type catchupSource struct {
	selfOrigin crdt.Origin
	selfLog    *journal.Journal
	mirror     serveFn
}

var _ transport.CatchupSource = (*catchupSource)(nil)

// Serve fans the request across its byte stores: own-origin ranges from the
// self-log, the rest from the mirror. The per-request caps (MaxRecords /
// MaxBytes) are a single response budget — what the self-log sends is
// subtracted from what the mirror may send — so the response never exceeds the
// client's stated bound (applying the full cap to each store could return ~2x).
func (s *catchupSource) Serve(ctx context.Context, req transport.CatchupRequest, write func(payload []byte) error) error {
	if write == nil {
		return errors.New("postgres: Serve requires non-nil write")
	}
	var ownRanges, otherRanges []transport.Range
	for _, r := range req.Ranges {
		if r.Origin == s.selfOrigin {
			ownRanges = append(ownRanges, r)
		} else {
			otherRanges = append(otherRanges, r)
		}
	}
	var sentRecords uint32
	var sentBytes uint64
	if len(ownRanges) > 0 && s.selfLog != nil {
		var err error
		sentRecords, sentBytes, err = serveSelfLog(ctx, s.selfLog, s.selfOrigin, ownRanges, req.MaxRecords, req.MaxBytes, write)
		if err != nil {
			return err
		}
	}
	if len(otherRanges) > 0 && s.mirror != nil {
		// Subtract the self-log's contribution so the caps bound the whole
		// response, not each store independently. A cap already met by the
		// self-log ends the round here (the client re-requests the rest).
		maxRecords := req.MaxRecords
		if maxRecords > 0 {
			if sentRecords >= maxRecords {
				return nil
			}
			maxRecords -= sentRecords
		}
		maxBytes := req.MaxBytes
		if maxBytes > 0 {
			if sentBytes >= maxBytes {
				return nil
			}
			maxBytes -= sentBytes
		}
		return s.mirror(ctx, transport.CatchupRequest{
			Ranges:     otherRanges,
			MaxRecords: maxRecords,
			MaxBytes:   maxBytes,
		}, write)
	}
	return nil
}

// Self-log entries carry an 8-byte commit-LSN prefix (selflog.go) before the
// canonical changeset wire bytes. The wire prefix itself is
// version(1) + origin(8 BE) + seq(8 BE), so to read the seq for filtering we
// peek at bytes 9..17 of the LSN-stripped payload (changesetWirePrefixLen).
const selfLogLSNPrefixLen = 8
const changesetWirePrefixLen = 17

// serveSelfLog walks the self-log, strips each entry's LSN prefix, and
// forwards the recovered wire bytes whose (origin, seq) header matches any
// requested range. Entries with a foreign origin (defensive — the self-log
// only holds own commits) or non-KindLocalDML kinds are skipped. It returns
// the records/bytes it wrote so the caller can charge them against a shared
// cap budget. The loop is a full scan filtered by range (no early break): it
// stays general for multi-range requests and relies on the caps for an upper
// bound; catchup is infrequent relative to the apply path.
func serveSelfLog(
	ctx context.Context,
	j *journal.Journal,
	selfOrigin crdt.Origin,
	ranges []transport.Range,
	maxRecords uint32,
	maxBytes uint64,
	write func(payload []byte) error,
) (uint32, uint64, error) {
	var sentRecords uint32
	var sentBytes uint64
	it := j.Iterate(0)
	for {
		if err := ctx.Err(); err != nil {
			return sentRecords, sentBytes, err
		}
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
			return sentRecords, sentBytes, nil
		}
		if err != nil {
			return sentRecords, sentBytes, err
		}
		if rec.Aborted() || rec.Kind != journal.KindLocalDML {
			continue
		}
		if len(rec.Payload) < selfLogLSNPrefixLen+changesetWirePrefixLen {
			continue
		}
		cs := rec.Payload[selfLogLSNPrefixLen:]
		origin := crdt.Origin(binary.BigEndian.Uint64(cs[1:9]))
		if origin != selfOrigin {
			continue
		}
		seq := crdt.Seq(binary.BigEndian.Uint64(cs[9:17]))
		matched := false
		for _, r := range ranges {
			if r.Contains(seq) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if maxBytes > 0 && sentBytes+uint64(len(cs)) > maxBytes && sentRecords > 0 {
			return sentRecords, sentBytes, nil
		}
		cp := make([]byte, len(cs))
		copy(cp, cs)
		if err := write(cp); err != nil {
			return sentRecords, sentBytes, err
		}
		sentRecords++
		sentBytes += uint64(len(cp))
		if maxRecords > 0 && sentRecords >= maxRecords {
			return sentRecords, sentBytes, nil
		}
	}
}
