package crdt

import "fmt"

// Origin identifies a replica's local-write epoch. Producer rotates the
// origin on unclean restart so post-recovery writes cannot reuse a Seq
// already visible under the previous origin.
//
// Restricted to [0, 2^63): the wire format reserves bit 63 zero. Allocators
// must mask the high bit at generation time.
type Origin uint64

// Seq is a per-origin monotonic counter. The pair (Origin, Seq) is a Dot.
type Seq uint64

// Dot is the identity of a single replication event (Preguiça et al.,
// Dotted Version Vectors, 2010). Producer-enforced: invariant (1) (Dot
// uniqueness) and invariant (2) (per-origin Clock monotonicity).
// Spec: CRDT.md.
type Dot struct {
	Origin Origin
	Seq    Seq
}

// String renders a Dot as "origin/seq" in decimal. Cheap and unambiguous;
// useful for logs and error messages.
func (d Dot) String() string {
	return fmt.Sprintf("%d/%d", d.Origin, d.Seq)
}

// Clock is a Hybrid Logical Clock value: 47-bit physical milliseconds in
// the high bits of WallTime, 16-bit logical counter in the low bits, with
// bit 63 reserved zero. Logical is exposed as a separate field so callers
// don't need bit-twiddling for the common case.
//
// Implementation mirrors CockroachDB pkg/util/hlc. There is intentionally
// no Synthetic field — CRDB removed it for soundness; Syzy never adds it.
type Clock struct {
	WallTime int64 // unix epoch milliseconds
	Logical  int32 // tiebreaker for equal wall times
}

// Less reports whether c < other under strict lex on (WallTime, Logical).
func (c Clock) Less(other Clock) bool {
	if c.WallTime != other.WallTime {
		return c.WallTime < other.WallTime
	}
	return c.Logical < other.Logical
}

// Equal reports whether c and other carry the same WallTime and Logical.
func (c Clock) Equal(other Clock) bool {
	return c.WallTime == other.WallTime && c.Logical == other.Logical
}

// IsZero reports whether c is the zero value (used as the implicit Stamp
// for never-existed RowState).
func (c Clock) IsZero() bool {
	return c.WallTime == 0 && c.Logical == 0
}

// Forward returns max(c, other). Pure; does not mutate the receiver.
func (c Clock) Forward(other Clock) Clock {
	if c.Less(other) {
		return other
	}
	return c
}

// clockMaxWall is the largest WallTime that fits in the packed wire
// form (47 bits, ≈ 4458 years from Unix epoch).
const clockMaxWall = int64(1<<47) - 1

// clockMaxLogical is the largest Logical value that fits in the packed
// wire form (16 bits).
const clockMaxLogical = int32(1<<16) - 1

// Pack returns the 8-byte wire form: 47-bit WallTime in the high bits
// + 16-bit Logical in the low bits, with bit 63 reserved zero. Panics
// on out-of-range inputs, which would only occur from buggy callers.
func (c Clock) Pack() uint64 {
	if c.WallTime < 0 || c.WallTime > clockMaxWall {
		panic("crdt: Clock.WallTime out of range for wire pack")
	}
	if c.Logical < 0 || c.Logical > clockMaxLogical {
		panic("crdt: Clock.Logical out of range for wire pack")
	}
	return (uint64(c.WallTime) << 16) | uint64(c.Logical)
}

// UnpackClock reverses Pack: extracts WallTime and Logical from the
// 8-byte wire form. Bit 63 is ignored (reserved zero).
func UnpackClock(v uint64) Clock {
	return Clock{
		WallTime: int64((v >> 16) & uint64(clockMaxWall)),
		Logical:  int32(v & uint64(uint32(clockMaxLogical))),
	}
}

// Stamp is the LWW arbitration key: a Clock paired with the Origin that
// produced it. Total order on (Clock.WallTime, Clock.Logical, Origin)
// by construction — invariant (5) in CRDT.md.
//
// Stamp is the lattice key from Almeida et al. δ-state §7.1
// (LexPair<Clock, Origin>); its Dominates relation makes the per-cell
// layer a register CRDT.
type Stamp struct {
	Clock
	Origin Origin
}

// Dominates reports whether a strictly dominates b: a > b in lex order on
// (WallTime, Logical, Origin). Equal Stamps return false.
func (a Stamp) Dominates(b Stamp) bool {
	if a.WallTime != b.WallTime {
		return a.WallTime > b.WallTime
	}
	if a.Logical != b.Logical {
		return a.Logical > b.Logical
	}
	return a.Origin > b.Origin
}

// Equal reports whether a and b are byte-identical Stamps.
func (a Stamp) Equal(b Stamp) bool {
	return a.Clock.Equal(b.Clock) && a.Origin == b.Origin
}

// IsZero reports whether s is the zero value.
func (s Stamp) IsZero() bool {
	return s.Clock.IsZero() && s.Origin == 0
}

// String renders a Stamp as "wall.logical@origin"; matches the lex order.
func (s Stamp) String() string {
	return fmt.Sprintf("%d.%d@%d", s.WallTime, s.Logical, s.Origin)
}
