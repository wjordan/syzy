package crdt

// TableID is a 16-byte stable identifier for a replicated table. Names
// are presentation metadata; IDs survive RENAME and uniquely identify a
// table-generation across a DROP/CREATE cycle.
type TableID [16]byte

// ColumnID is a 16-byte stable identifier for a column. Names survive
// RENAME COLUMN; IDs persist tombstoned beyond DROP COLUMN so late DML
// can be deterministically ignored.
type ColumnID [16]byte

// KeyID is a 16-byte stable identifier for a key (PK or unique key) in
// a table. The all-zero value (PKKeyID in the metadata package) denotes
// the primary key tuple; every other value identifies one unique
// constraint or unique index. See docs/SCHEMA.md#stable-catalog-identity.
type KeyID [16]byte

// PKBlob is the canonical encoded primary key blob. Encoding rules and
// rejected types are specified in docs/PROTOCOL.md#value-encoding.
type PKBlob []byte

// ByteRange is a half-open byte interval [Start, End) used by the
// per-byte-range layer. End == Start denotes the empty range.
type ByteRange struct {
	Start, End uint64
}

// Empty reports whether r covers no bytes.
func (r ByteRange) Empty() bool { return r.End <= r.Start }

// Length returns r's length in bytes (0 if empty).
func (r ByteRange) Length() uint64 {
	if r.Empty() {
		return 0
	}
	return r.End - r.Start
}

// Overlaps reports whether r and other share any bytes.
func (r ByteRange) Overlaps(other ByteRange) bool {
	return r.Start < other.End && other.Start < r.End
}

// RowState is the per-row CRDT state for a single (TableID, PKBlob).
// Stored in the metadata; reconstructed on apply from row_clock,
// cell_clock, and blob_range_clock rows.
//
// CL is the cr-sqlite causal length (parity = liveness). Base is the
// row's baseline LWW Stamp at this CL. Cells and Ranges are sparse
// overrides scoped to (CL, Base); bumping CL on resurrection implicitly
// tombstones prior-generation overrides.
type RowState struct {
	CL     uint64
	Base   Stamp
	Cells  map[ColumnID]Stamp
	Ranges map[ColumnID]IntervalMap
}

// CL is the cr-sqlite causal length. Per-pk and monotonically
// non-decreasing — invariant (7).
//
// Parity convention:
//
//	CL == 0       row never existed at this pk.
//	CL is odd     row alive at generation (CL+1)/2.
//	CL is even    row tombstoned at generation CL/2.
//
// Producer transitions:
//
//	INSERT on never-existed-or-tombstoned-locally → max(CL_observed) + 1
//	UPDATE on live                                → CL unchanged
//	DELETE on live                                → CL + 1
//
// LWW between two writes on the same pk is strict lex on (CL, Stamp):
// higher CL wins; ties broken by Stamp.Dominates. CL replaces the
// "tombstones win unconditionally" carve-out from earlier designs.
// Per-cell and per-byte-range overrides are scoped to the row's current
// CL value; bumping CL on resurrection implicitly tombstones
// prior-generation overrides without explicit GC.

// IsNeverExisted reports whether r represents a pk that has never had
// any write applied (CL == 0).
func (r RowState) IsNeverExisted() bool { return r.CL == 0 }

// IsLive reports whether r currently represents a live row at its
// generation (CL is odd).
func (r RowState) IsLive() bool { return r.CL > 0 && r.CL%2 == 1 }

// IsTombstoned reports whether r is currently a tombstone (CL is even
// and non-zero).
func (r RowState) IsTombstoned() bool { return r.CL > 0 && r.CL%2 == 0 }

// DominatedBy reports whether (incCL, inc) wins LWW against r's
// (CL, Base): strictly greater CL wins; tied CL goes to Stamp.Dominates.
// Spec: CRDT.md#causal-length-cl.
func (r RowState) DominatedBy(incCL uint64, inc Stamp) bool {
	if incCL != r.CL {
		return incCL > r.CL
	}
	return inc.Dominates(r.Base)
}

// NextLiveCL returns the smallest odd CL strictly greater than r.CL —
// the value an INSERT (or resurrecting INSERT) must take. Producer-side
// helper.
func (r RowState) NextLiveCL() uint64 {
	if r.IsLive() {
		// Already live; an INSERT on a live row is an UPDATE under
		// SQLite UPSERT semantics and does not bump CL.
		return r.CL
	}
	// CL is 0 or even; next odd is CL + 1.
	return r.CL + 1
}

// NextTombCL returns the smallest even CL strictly greater than r.CL —
// the value a DELETE on a live row must take. Calling on a non-live row
// returns r.CL unchanged (DELETE is a no-op on a tombstone or never-
// existed row at the producer; receivers handle missing-row DELETEs by
// recording a tombstone).
func (r RowState) NextTombCL() uint64 {
	if !r.IsLive() {
		return r.CL
	}
	return r.CL + 1
}

// EffectiveStamp returns the LWW Stamp governing reads of (col, rng).
// Fall-through order per CRDT.md#layer-composition:
//
//  1. Ranges[col].At(rng.Start) if set and the IntervalMap covers rng.
//  2. Cells[col] if a sparse override exists.
//  3. Base — the row's baseline.
//
// rng with Empty() == true is treated as "no range constraint" — the
// fall-through skips the Ranges layer and goes straight to Cells/Base.
//
// On a never-existed row (CL == 0) the returned Stamp is the zero
// value, which is dominated by every real write.
func (r RowState) EffectiveStamp(col ColumnID, rng ByteRange) Stamp {
	if !rng.Empty() {
		if im, ok := r.Ranges[col]; ok && im != nil {
			return im.At(rng.Start)
		}
	}
	if cs, ok := r.Cells[col]; ok {
		return cs
	}
	return r.Base
}
