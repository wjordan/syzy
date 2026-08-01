package postgres

import (
	"math"
	"testing"
)

// TestIDSlice checks the partition math at the boundaries — no Postgres needed.
// The max ordinal must not wrap (uint16 ordinal+1) and every slice must stay a
// positive bigint, with adjacent slices exactly contiguous and disjoint.
func TestIDSlice(t *testing.T) {
	const maxBigint = uint64(math.MaxInt64) // 2^63-1

	for _, ord := range []uint16{1, 2, 1000, 65534, 65535} {
		lo, hi := idSlice(ord)
		if lo > maxBigint || hi > maxBigint {
			t.Fatalf("ordinal %d: slice [%d,%d] exceeds max bigint %d", ord, lo, hi, maxBigint)
		}
		if lo == 0 {
			t.Fatalf("ordinal %d: lo is 0 (collides with int>0 checks / unpartitioned ids)", ord)
		}
		if hi <= lo {
			t.Fatalf("ordinal %d: empty/inverted slice [%d,%d]", ord, lo, hi)
		}
		// Contiguous, disjoint with the next ordinal.
		if ord < 65535 {
			nlo, _ := idSlice(ord + 1)
			if nlo != hi+1 {
				t.Fatalf("ordinal %d→%d not contiguous: hi=%d nextLo=%d", ord, ord+1, hi, nlo)
			}
		}
	}

	// The documented max ordinal fills the id space up to the bigint ceiling.
	if _, hi := idSlice(65535); hi != maxBigint {
		t.Fatalf("ordinal 65535 hi = %d, want %d (max bigint)", hi, maxBigint)
	}
}
