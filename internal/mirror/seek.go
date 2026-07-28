package mirror

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// ServeStats summarizes one Serve call. recordsScanned counts journal
// records the iterator walked; recordsSent counts those streamed to the
// caller; segmentsSkipped counts whole segments the seek index let Serve
// bypass without iterating. The gap between scanned and (segmentsTotal's
// worth of) records is the win: a from-zero scan would set
// recordsScanned to the full journal length.
type ServeStats struct {
	RecordsScanned  int
	RecordsSent     int
	BytesSent       uint64
	SegmentsTotal   int
	SegmentsSkipped int
}

// LastServeStats returns the counters from the most recent Serve call.
// Intended for tests and the R5 observability log; concurrent Serves
// race for the slot, so callers wanting per-call numbers must serialize.
func (m *Manager) LastServeStats() ServeStats {
	m.statMu.Lock()
	defer m.statMu.Unlock()
	return m.lastStats
}

func (m *Manager) recordServeStats(s ServeStats) {
	m.statMu.Lock()
	m.lastStats = s
	m.statMu.Unlock()
}

// lookupHandle returns the origin's handle (not just its journal) so
// Serve can consult the per-segment seek index. Like LookupJournal it
// does NOT create a handle for an unknown origin.
func (m *Manager) lookupHandle(origin crdt.Origin) (*originHandle, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.journals[origin]
	return h, ok
}

// indexAppend folds a freshly appended record into the seek index. Called
// by the writer goroutine after each journal.Append; off is where the
// record landed, payload carries the (origin, seq) wire prefix.
func (h *originHandle) indexAppend(off journal.Offset, payload []byte) {
	seq, ok := payloadSeq(payload)
	if !ok {
		return
	}
	h.idxMu.Lock()
	h.indexLocked(off, seq)
	h.idxMu.Unlock()
}

// indexLocked merges one (offset, seq) into the per-segment span. maxSeq
// only grows; firstOff only shrinks — so the index never under-reports a
// segment's seq ceiling, which is what makes the skip in startOffset safe.
func (h *originHandle) indexLocked(off journal.Offset, seq uint64) {
	seg := off.Seg()
	sp := h.segIndex[seg]
	if sp == nil {
		h.segIndex[seg] = &segSpan{firstOff: off, maxSeq: seq}
		return
	}
	if off < sp.firstOff {
		sp.firstOff = off
	}
	if seq > sp.maxSeq {
		sp.maxSeq = seq
	}
}

// ensureIndex builds the seek index from the on-disk journal exactly
// once. Pre-existing records (written before this process opened the
// journal) are only covered here; live appends self-index via
// indexAppend. Both paths merge idempotently, so a record seen by both
// is harmless. The scan takes idxMu per record rather than for its whole
// duration, so it never blocks the writer goroutine for long.
func (h *originHandle) ensureIndex() {
	h.idxOnce.Do(func() {
		it := h.j.Iterate(0)
		for {
			rec, off, err := it.Next()
			if err == io.EOF || errors.Is(err, journal.ErrPending) {
				return
			}
			if err != nil {
				// A scan error leaves the index partially built; that only
				// costs extra scanning on a later Serve, never correctness,
				// because startOffset falls back to a full scan when it
				// can't prove a segment is below Lo.
				return
			}
			if rec.Kind != journal.KindMirror {
				continue
			}
			seq, ok := payloadSeq(rec.Payload)
			if !ok {
				continue
			}
			h.idxMu.Lock()
			h.indexLocked(off, seq)
			h.idxMu.Unlock()
		}
	})
}

// startOffset returns the offset Serve should begin iterating at to cover
// every record with seq >= lo, plus the segment counts for stats. It
// picks the lowest-numbered segment whose maxSeq >= lo: every earlier
// segment has maxSeq < lo and so holds nothing the request wants, while
// every later segment is still scanned (records arrive out of seq order,
// so a straggler with seq >= lo may sit in a higher segment). When no
// segment qualifies, ok is false and Serve streams nothing for the range.
func (h *originHandle) startOffset(lo crdt.Seq) (off journal.Offset, ok bool, total, skipped int) {
	h.idxMu.Lock()
	defer h.idxMu.Unlock()
	total = len(h.segIndex)
	var (
		best    journal.Offset
		bestSeg uint32
	)
	for seg, sp := range h.segIndex {
		if sp.maxSeq < uint64(lo) {
			continue
		}
		if !ok || sp.firstOff < best {
			best, bestSeg, ok = sp.firstOff, seg, true
		}
	}
	if !ok {
		return 0, false, total, 0
	}
	for seg := range h.segIndex {
		if seg < bestSeg {
			skipped++
		}
	}
	return best, true, total, skipped
}

// payloadSeq extracts the CRDT seq from a mirror wire payload's prefix.
// Returns false for a payload too short to carry the (version, origin,
// seq) header.
func payloadSeq(payload []byte) (uint64, bool) {
	if len(payload) < catchupHeaderLen {
		return 0, false
	}
	return binary.BigEndian.Uint64(payload[9:17]), true
}
