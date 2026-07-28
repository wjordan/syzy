// Package gapfillerchain composes ordered transport.GapFillers so the
// broker's runFetchRound can try the fast path (peer-pull) before the
// durable fallback (object storage). The chain tracks which seqs each
// filler delivered and only forwards still-missing sub-ranges to the
// next filler, keeping unnecessary work off the fallback path while
// still letting the broker's apply path dedupe duplicates.
package gapfillerchain

import (
	"context"
	"encoding/binary"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

// New returns a GapFiller that calls each filler in order, narrowing
// the request to the still-missing sub-ranges after each step. Nil
// fillers are skipped so callers can pass `New(maybePeer, maybeS3)`
// without pre-filtering. Returns nil when no non-nil filler is given.
func New(fillers ...transport.GapFiller) transport.GapFiller {
	out := make([]transport.GapFiller, 0, len(fillers))
	for _, f := range fillers {
		if f != nil {
			out = append(out, f)
		}
	}
	switch len(out) {
	case 0:
		return nil
	case 1:
		return out[0]
	}
	return &chain{fillers: out}
}

type chain struct {
	fillers []transport.GapFiller
}

// Fetch tries each filler in order. After every filler returns it
// subtracts the seqs that filler delivered from the request, passing
// the remaining sub-ranges (if any) to the next filler. Errors from
// individual fillers don't abort the chain — the next filler may
// succeed where the previous one failed. The last filler's error is
// returned only if at least one range is still unfilled after every
// filler has been tried.
func (c *chain) Fetch(ctx context.Context, ranges []transport.Range, apply transport.ApplyFunc) error {
	if len(ranges) == 0 {
		return nil
	}
	seen := map[crdt.Origin]*crdt.SeqSet{}
	wrappedApply := func(applyCtx context.Context, payload []byte) error {
		// Honour cancel between records so a long fetch round (many
		// payloads from one filler) doesn't outlive a Close that's
		// waiting on this goroutine's wg. Mid-cgo inside apply
		// itself is caught by sqlite3_interrupt from broker.Close.
		if err := applyCtx.Err(); err != nil {
			return err
		}
		if err := apply(applyCtx, payload); err != nil {
			// Don't mark as seen: a bad/corrupt payload from filler N
			// must not starve filler N+1's chance to deliver the
			// real bytes.
			return err
		}
		if origin, seq, ok := parseDot(payload); ok {
			s, exists := seen[origin]
			if !exists {
				s = &crdt.SeqSet{}
				seen[origin] = s
			}
			s.Add(seq)
		}
		return nil
	}
	var lastErr error
	current := ranges
	for _, f := range c.fillers {
		if len(current) == 0 {
			return nil
		}
		err := f.Fetch(ctx, current, wrappedApply)
		if err != nil {
			lastErr = err
		}
		current = remaining(current, seen)
	}
	if len(current) == 0 {
		return nil
	}
	return lastErr
}

// parseDot extracts (origin, seq) from a canonical Changeset wire
// payload. Returns ok=false on a short or malformed buffer; the broker
// would have rejected such payloads in apply anyway, so a quiet skip is
// safe here.
func parseDot(payload []byte) (crdt.Origin, crdt.Seq, bool) {
	if len(payload) < 17 {
		return 0, 0, false
	}
	origin := crdt.Origin(binary.BigEndian.Uint64(payload[1:9]))
	seq := crdt.Seq(binary.BigEndian.Uint64(payload[9:17]))
	return origin, seq, true
}

// remaining returns the sub-ranges of in not covered by seen. Used to
// hand the next filler only the seqs we haven't yet seen. Open-ended
// ranges (Hi=0) are clipped past the highest contiguous seen seq from
// Lo; if there is no contiguous prefix, the range is forwarded as-is
// since the next filler still needs to satisfy it.
func remaining(in []transport.Range, seen map[crdt.Origin]*crdt.SeqSet) []transport.Range {
	var out []transport.Range
	for _, r := range in {
		set, ok := seen[r.Origin]
		if !ok || set.IsEmpty() {
			out = append(out, r)
			continue
		}
		out = append(out, subtractRange(r, set)...)
	}
	return out
}

// subtractRange returns the sub-ranges of r not covered by set.
//
// For closed ranges [Lo, Hi]: walk set's ranges and emit the gaps.
// For open-ended ranges (Hi=0): advance Lo past the contiguous
// prefix starting at Lo; if a gap exists at Lo, emit it as the new
// open-ended range with Lo shifted forward and otherwise keep the
// range open at its current Lo.
func subtractRange(r transport.Range, set *crdt.SeqSet) []transport.Range {
	if r.OpenEnded() {
		// Shift Lo past any contiguous coverage at the bottom of the
		// open-ended range. We can't claim the range is fully covered
		// (it has no upper bound), so emit one open-ended remainder.
		for _, sr := range set.Ranges() {
			if sr.Lo > r.Lo {
				break
			}
			if sr.Hi >= r.Lo {
				r.Lo = sr.Hi + 1
			}
		}
		return []transport.Range{r}
	}
	var out []transport.Range
	lo := r.Lo
	for _, sr := range set.Ranges() {
		if sr.Hi < lo {
			continue
		}
		if sr.Lo > r.Hi {
			break
		}
		if sr.Lo > lo {
			out = append(out, transport.Range{Origin: r.Origin, Lo: lo, Hi: sr.Lo - 1})
		}
		if sr.Hi >= lo {
			lo = sr.Hi + 1
		}
		if lo > r.Hi {
			return out
		}
	}
	if lo <= r.Hi {
		out = append(out, transport.Range{Origin: r.Origin, Lo: lo, Hi: r.Hi})
	}
	return out
}
