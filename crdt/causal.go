package crdt

import (
	"fmt"
	"slices"
	"sort"
)

// SeqRange and SeqSet back the per-origin applied frontier's
// out-of-order exception set (nodestate.Cache). The frontier itself
// only grows — invariant (4) in CRDT.md — enforced by
// nodestate.Cache.MarkApplied.

// SeqRange is an inclusive [Lo, Hi] window of Seqs from one Origin.
// Lo > Hi is treated as the empty range.
type SeqRange struct {
	Lo, Hi Seq
}

// Empty reports whether r covers no Seqs.
func (r SeqRange) Empty() bool { return r.Lo > r.Hi }

// Contains reports whether s ∈ [Lo, Hi].
func (r SeqRange) Contains(s Seq) bool { return s >= r.Lo && s <= r.Hi }

// String formats r as "[Lo,Hi]".
func (r SeqRange) String() string {
	if r.Empty() {
		return "[]"
	}
	return fmt.Sprintf("[%d,%d]", r.Lo, r.Hi)
}

// SeqSet is a sparse set of Seqs, stored as sorted, non-overlapping,
// non-adjacent inclusive ranges. The zero value is the empty set.
//
// Used for the applied frontier's out-of-order exception set
// (nodestate.Cache: received Dots above each origin's contiguous head)
// and for gap-fill planning (internal/antientropy,
// internal/gapfillerchain).
type SeqSet struct {
	// ranges is sorted by Lo, with no two ranges adjacent or overlapping
	// (Add maintains the invariant by coalescing).
	ranges []SeqRange
}

// IsEmpty reports whether s holds no Seqs.
func (s SeqSet) IsEmpty() bool { return len(s.ranges) == 0 }

// Contains reports whether v is in the set.
func (s SeqSet) Contains(v Seq) bool {
	// First range with Lo > v; predecessor is the candidate container.
	i := sort.Search(len(s.ranges), func(i int) bool {
		return s.ranges[i].Lo > v
	})
	return i > 0 && v <= s.ranges[i-1].Hi
}

// Add inserts v into the set, coalescing with neighbours. No-op if
// already present.
func (s *SeqSet) Add(v Seq) {
	// i = first range with Lo > v; ranges[i-1] is the predecessor.
	i := sort.Search(len(s.ranges), func(i int) bool {
		return s.ranges[i].Lo > v
	})

	prevAdj := false
	if i > 0 {
		if v <= s.ranges[i-1].Hi {
			return // already present
		}
		prevAdj = s.ranges[i-1].Hi+1 == v
	}
	nextAdj := i < len(s.ranges) && v+1 == s.ranges[i].Lo

	switch {
	case prevAdj && nextAdj:
		s.ranges[i-1].Hi = s.ranges[i].Hi
		s.ranges = slices.Delete(s.ranges, i, i+1)
	case prevAdj:
		s.ranges[i-1].Hi = v
	case nextAdj:
		s.ranges[i].Lo = v
	default:
		s.ranges = slices.Insert(s.ranges, i, SeqRange{Lo: v, Hi: v})
	}
}

// PromoteContiguous returns the highest Seq forming a contiguous prefix
// from above+1 onward, removing those Seqs from the set. If no such
// prefix exists, returns above unchanged. Used by
// nodestate.Cache.MarkApplied to promote out-of-order receives into
// the contiguous frontier head.
func (s *SeqSet) PromoteContiguous(above Seq) Seq {
	if len(s.ranges) == 0 {
		return above
	}
	first := s.ranges[0]
	if first.Lo != above+1 {
		return above
	}
	// First range is contiguous with `above`; absorb it.
	newAbove := first.Hi
	s.ranges = s.ranges[1:]
	return newAbove
}

// Ranges returns the underlying ranges in ascending order. The caller
// must not mutate the returned slice.
func (s SeqSet) Ranges() []SeqRange { return s.ranges }

// Clone returns a deep copy of s.
func (s SeqSet) Clone() SeqSet {
	return SeqSet{ranges: slices.Clone(s.ranges)}
}
