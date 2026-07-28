package mirror

import (
	"sort"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// RetainSealed drops an origin's journal segments whose every record is
// already durable in object storage: a segment is droppable when its seek-
// index maxSeq ceiling is <= sealedTip (the origin's sealed epoch tip, e.g.
// from s3fetch.DiscoverTips). Peers asking for the truncated range fall back
// to the object store via their gap-filler chain, so nothing is stranded —
// this is the same GC-safety predicate the SQLite node uses (object-store
// seal, never mere peer liveness).
//
// Truncation is segment-granular (journal.RetainAfter) and always keeps the
// newest segment (the writer's active tail). Unknown origins and journals
// with no index are no-ops.
func (m *Manager) RetainSealed(origin crdt.Origin, sealedTip crdt.Seq) error {
	h, ok := m.lookupHandle(origin)
	if !ok {
		return nil
	}
	h.ensureIndex()

	h.idxMu.Lock()
	segs := make([]uint32, 0, len(h.segIndex))
	for seg := range h.segIndex {
		segs = append(segs, seg)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	// Walk ascending: the cutoff is the first segment we must keep — the
	// first whose maxSeq exceeds the sealed tip, or (all sealed) the newest
	// segment. Every segment below the cutoff holds only records with
	// seq <= sealedTip (maxSeq is a never-under-reporting ceiling), all of
	// which the bucket already serves.
	var cut uint32
	found := false
	for i, seg := range segs {
		if uint64(sealedTip) < h.segIndex[seg].maxSeq || i == len(segs)-1 {
			cut, found = seg, true
			break
		}
	}
	h.idxMu.Unlock()
	if !found || cut == 0 {
		return nil
	}

	if err := h.j.RetainAfter(journal.SegmentStart(cut)); err != nil {
		return err
	}
	// Drop index spans for the removed segments so a later Serve's
	// startOffset doesn't point at a deleted file.
	h.idxMu.Lock()
	for seg := range h.segIndex {
		if seg < cut {
			delete(h.segIndex, seg)
		}
	}
	h.idxMu.Unlock()
	return nil
}
