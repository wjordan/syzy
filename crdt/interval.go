package crdt

// IntervalMap is the byte-range layer's CRDT primitive: per-row,
// per-blob-column, mapping disjoint byte intervals to the Stamp of the
// patch that wrote them. See BLOB_PATCH.md for the full algorithm and
// the Layers section of CRDT.md for the byte-range layer's (vis, ar)
// form.
//
// Invariants:
//
//   - Stored entries are sorted by Start, non-overlapping, and (under
//     the run-coalescing invariant) byte-contiguous neighbours with
//     equal Stamps are merged into one entry.
//   - Every entry strictly Dominates the effective parent Stamp at
//     construction time. Apply enforces this.
type IntervalMap interface {
	// At returns the effective Stamp at byte offset off. If no entry
	// covers off, returns the zero Stamp (caller falls through to the
	// parent layer per RowState.EffectiveStamp).
	At(off uint64) Stamp

	// Apply integrates a write of Stamp c over [start, end), with the
	// caller's claimed parent baseline. It returns the sub-ranges where
	// c won — only those bytes need to be written through to the blob
	// itself. Existing entries that c does not strictly dominate are
	// preserved; gaps where c does not strictly dominate baseline are
	// not added.
	Apply(start, end uint64, c, baseline Stamp) []ByteRange

	// Prune drops every entry with Stamp <= floor. Called after the
	// parent row/cell Stamp advances past a stable horizon.
	Prune(floor Stamp)

	// Clip drops entries with Start >= maxEnd and truncates entries
	// with End > maxEnd to End = maxEnd.
	Clip(maxEnd uint64)

	// IsEmpty reports whether the map holds no entries. When true, the
	// caller deletes the blob_range_clock row entirely.
	IsEmpty() bool

	// Entries returns the underlying entries in ascending Start order
	// for inspection / serialization. Caller must not mutate.
	Entries() []IntervalEntry
}

// IntervalEntry is one (range, stamp) pair in an IntervalMap.
type IntervalEntry struct {
	Range ByteRange
	Stamp Stamp
}

// Equal reports whether e and o have the same range and stamp.
func (e IntervalEntry) Equal(o IntervalEntry) bool {
	return e.Range == o.Range && e.Stamp.Equal(o.Stamp)
}
