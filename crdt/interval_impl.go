package crdt

import "slices"

// NewIntervalMap returns an empty IntervalMap. Callers create one
// lazily — most rows have no entries and the table-level row stays
// absent in blob_range_clock.
func NewIntervalMap() IntervalMap { return &intervalMap{} }

// intervalMap is the reference implementation: entries sorted by
// Range.Start, non-overlapping, with adjacent same-Stamp entries
// coalesced (run-coalescing invariant).
//
// scratchOut and scratchWon are reused across Apply calls so the hot
// path doesn't re-allocate a fresh []IntervalEntry / []ByteRange every
// time. The returned won slice aliases scratchWon and is only valid
// until the next Apply call on this map.
type intervalMap struct {
	entries    []IntervalEntry
	scratchOut []IntervalEntry
	scratchWon []ByteRange
}

func (m *intervalMap) IsEmpty() bool { return len(m.entries) == 0 }

func (m *intervalMap) Entries() []IntervalEntry { return m.entries }

func (m *intervalMap) At(off uint64) Stamp {
	// First entry whose Range.Start > off; predecessor may contain off.
	i, _ := slices.BinarySearchFunc(m.entries, off, func(e IntervalEntry, off uint64) int {
		if e.Range.Start <= off {
			return -1
		}
		return 1
	})
	if i == 0 {
		return Stamp{}
	}
	e := m.entries[i-1]
	if off < e.Range.End {
		return e.Stamp
	}
	return Stamp{}
}

// Apply integrates the write of c over [start, end). Returns the won
// sub-ranges — only those bytes need to be written through to the blob
// itself.
//
// Algorithm:
//
//  1. Copy entries strictly before [start, end).
//  2. For each entry overlapping [start, end), split into pre/overlap/
//     post regions; LWW the overlap against c; preserve pre and post.
//  3. Fill any gap inside [start, end) at c iff c dominates baseline.
//  4. Copy remaining entries strictly after.
//  5. Coalesce adjacent same-Stamp entries.
func (m *intervalMap) Apply(start, end uint64, c, baseline Stamp) []ByteRange {
	if start >= end {
		return nil
	}
	out := m.scratchOut[:0]
	won := m.scratchWon[:0]

	// claim writes a c-stamped entry over [lo, hi) and records the won
	// range; emit() preserves an existing-entry segment unchanged.
	claim := func(lo, hi uint64) {
		won = append(won, ByteRange{Start: lo, End: hi})
		out = append(out, IntervalEntry{Range: ByteRange{Start: lo, End: hi}, Stamp: c})
	}
	emit := func(lo, hi uint64, s Stamp) {
		out = append(out, IntervalEntry{Range: ByteRange{Start: lo, End: hi}, Stamp: s})
	}

	i := 0
	for i < len(m.entries) && m.entries[i].Range.End <= start {
		out = append(out, m.entries[i])
		i++
	}

	cur := start
	for i < len(m.entries) && m.entries[i].Range.Start < end {
		e := m.entries[i]

		// Pre-region of e (entirely below our range, preserved as-is).
		if e.Range.Start < cur {
			emit(e.Range.Start, cur, e.Stamp)
		}

		// Gap before e inside our range — c wins iff it dominates baseline.
		gapEnd := min(e.Range.Start, end)
		if cur < gapEnd {
			if c.Dominates(baseline) {
				claim(cur, gapEnd)
			}
			cur = gapEnd
		}

		// Overlap with e — c wins iff it dominates e.Stamp.
		ovEnd := min(e.Range.End, end)
		if cur < ovEnd {
			if c.Dominates(e.Stamp) {
				claim(cur, ovEnd)
			} else {
				emit(cur, ovEnd, e.Stamp)
			}
			cur = ovEnd
		}

		// Post-region of e (entirely above our range, preserved as-is).
		if e.Range.End > end {
			emit(end, e.Range.End, e.Stamp)
		}
		i++
	}

	// Trailing gap past all overlapping entries.
	if cur < end && c.Dominates(baseline) {
		claim(cur, end)
	}

	out = append(out, m.entries[i:]...)
	// Swap backings: new entries take what we just built; the old
	// entries slice becomes next call's scratchOut (truncated, capacity
	// retained). scratchWon retains its grown capacity for next call.
	newEntries := coalesce(out)
	m.scratchOut = m.entries[:0]
	m.entries = newEntries
	m.scratchWon = won
	return won
}

func (m *intervalMap) Prune(floor Stamp) {
	out := m.entries[:0]
	for _, e := range m.entries {
		// Keep iff e.Stamp strictly dominates floor.
		if e.Stamp.Dominates(floor) {
			out = append(out, e)
		}
	}
	m.entries = out
}

func (m *intervalMap) Clip(maxEnd uint64) {
	out := m.entries[:0]
	for _, e := range m.entries {
		if e.Range.Start >= maxEnd {
			continue
		}
		if e.Range.End > maxEnd {
			e.Range.End = maxEnd
		}
		out = append(out, e)
	}
	m.entries = out
}

// coalesce merges adjacent entries with the same Stamp and contiguous
// ranges. Run-coalescing invariant (cf. the Yjs Item-merge pattern).
func coalesce(entries []IntervalEntry) []IntervalEntry {
	if len(entries) <= 1 {
		return entries
	}
	out := entries[:1]
	for _, e := range entries[1:] {
		last := &out[len(out)-1]
		if last.Range.End == e.Range.Start && last.Stamp.Equal(e.Stamp) {
			last.Range.End = e.Range.End
		} else {
			out = append(out, e)
		}
	}
	return out
}
