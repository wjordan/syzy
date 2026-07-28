package broker

import (
	"fmt"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
)

// Cell-group apply: tables with default_clock_group='cell' arbitrate
// UPDATEs per column (CRDT.md F_cell) instead of per row. Concurrent
// updates to disjoint columns of one row merge; same-column writes
// resolve by stamp. Row liveness stays row-level: Insert/Delete bump
// CL and arbitrate on (CL, Base) exactly as row-group tables do, and a
// CL bump tombstones the prior generation's cell overrides.
//
// Opportunistic collapse keeps cell_clock sparse: a winning update
// that covers every live non-PK column re-absorbs the row into its
// baseline (Base=stamp, cells cleared), so rows only carry cell
// overrides while they have outstanding partial-column writes.

// cellApplyOutcome is applyCellUpdate's result: at most one row-clock
// update (collapse or generation advance), the per-column winner
// stamps, whether DML ran, and the blob_range_clock side effects when
// the DML routed through reconciliation.
type cellApplyOutcome struct {
	rowUpdates  []rowClockUpdate
	cellUpdates []cellClockUpdate
	applied     bool

	blobReconciled bool
	blobCols       []metadata.BlobRangeClockEntry
}

// applyCellUpdate arbitrates one Update against a cell-group table and
// runs the winning columns' DML. Caller has already checked
// rs.DominatedBy(upd.CL, stamp) — i.e. upd.CL >= rs.CL, with stamp
// winning the tie at equal CL against Base (per-column refinement
// happens here).
func (b *Broker) applyCellUpdate(tab *catalog.Table, upd crdt.Update, rs crdt.RowState, stamp crdt.Stamp) (cellApplyOutcome, error) {
	var out cellApplyOutcome
	if err := validateCellCounterValues(tab, upd); err != nil {
		return out, err
	}
	newGen := upd.CL > rs.CL

	// Per-column gate. A new generation (writer saw a resurrection we
	// haven't) wins every carried column; within the current
	// generation each column gates on its effective stamp so
	// concurrent disjoint-column updates merge. Counter contributions
	// (FormatDelta) carry no stamp and always land — they sum instead
	// of arbitrating (CRDT.md F_counter).
	winners := upd.Changed
	if !newGen {
		winners = make([]crdt.ColValue, 0, len(upd.Changed))
		for _, v := range upd.Changed {
			if v.Format == crdt.FormatDelta ||
				stamp.Dominates(rs.EffectiveStamp(v.Column, crdt.ByteRange{})) {
				winners = append(winners, v)
			}
		}
		if len(winners) == 0 {
			return out, nil
		}
	} else if cellUpdateHasCounter(upd) {
		// This update establishes the new generation locally: the
		// physical row still holds prior-generation content, so its
		// counter contributions apply absolutely (the generation's
		// opening value), not additively on top of stale bytes. Later
		// same-generation contributions — including the resurrecting
		// Insert's image — add on top.
		winners = make([]crdt.ColValue, len(upd.Changed))
		for i, v := range upd.Changed {
			if v.Format == crdt.FormatDelta {
				v.Format = crdt.FormatText
			}
			winners[i] = v
		}
	}
	reduced := crdt.Update{Table: upd.Table, PK: upd.PK, CL: upd.CL, Changed: winners}

	arbRec, stolen, err := b.arbitrate(tab, reduced, stamp)
	if err != nil {
		return out, fmt.Errorf("broker: unique arbitration: %w", err)
	}

	// DML for the winning columns (possibly loser-nulled by
	// arbitration). Route through blob reconciliation when the row has
	// active per-byte overrides, mirroring the row-group path.
	if b.cfg.Meta != nil && b.tableHasBlobClock(upd.Table) {
		existing, gerr := b.cfg.Meta.GetBlobRangeClock(upd.Table, upd.PK)
		if gerr != nil {
			return out, fmt.Errorf("broker: load blob_range_clock: %w", gerr)
		}
		if len(existing) > 0 {
			cols, rerr := b.applyDMLReconciled(tab, arbRec, rs, stamp, existing)
			if rerr != nil {
				return out, rerr
			}
			out.blobReconciled = true
			out.blobCols = cols
		}
	}
	if !out.blobReconciled {
		updRec, ok := arbRec.(crdt.Update)
		if !ok {
			return out, fmt.Errorf("broker: cell arbitration returned %T; want Update", arbRec)
		}
		matched, err := b.applyUpdate(tab, updRec)
		if err != nil {
			return out, err
		}
		if matched == 0 {
			// The update outran the row's INSERT (cross-origin delivery
			// is not causally gated): no physical row exists, yet the
			// row clock we are about to write claims it live. A silent
			// 0-row UPDATE would lose the row permanently — the later
			// Insert normalizes to a same-CL update whose UPDATE also
			// matches nothing. Materialize instead: INSERT from PK +
			// the winning columns via the partial path so SQLite fills
			// declared defaults, and the later Insert's per-column
			// arbitration fills what this update didn't carry (Base
			// stays zero / cell stamps cover only carried columns). A
			// NOT NULL column with no default surfaces as a constraint
			// error, rolls back the transaction, and routes to the
			// quarantine/retry machinery.
			//
			// Counter contributions are safe here: bindColValue ignores
			// Format, so a FormatDelta value seeds the new row with its
			// raw delta exactly once, and the generation's later
			// contributions (including the real Insert's image, which
			// asCellUpdate normalizes to deltas) still merge additively.
			ins := crdt.Insert{Table: updRec.Table, PK: updRec.PK, CL: updRec.CL, Image: updRec.Changed}
			if err := b.applyInsertPartial(tab, ins); err != nil {
				return out, err
			}
		}
	}
	out.applied = true
	out.cellUpdates = append(out.cellUpdates, stolen...)

	if tab.CoversAllNonPK(winners) {
		// Opportunistic collapse: the write defines every live column,
		// so the row is uniformly stamped — absorb into the baseline
		// and drop the overrides instead of writing one per column.
		out.rowUpdates = append(out.rowUpdates, rowClockUpdate{
			table: upd.Table, pk: upd.PK,
			state:      crdt.RowState{CL: upd.CL, Base: stamp},
			clearCells: true,
		})
		return out, nil
	}
	if newGen {
		// Generation advance without the resurrecting Insert's full
		// image: carried columns get overrides at the update's stamp;
		// Base stays zero so the (lower-stamped) Insert still wins the
		// columns this update didn't carry when it arrives.
		out.rowUpdates = append(out.rowUpdates, rowClockUpdate{
			table: upd.Table, pk: upd.PK,
			state:      crdt.RowState{CL: upd.CL},
			clearCells: true,
		})
	}
	for _, v := range winners {
		if col, ok := tab.ColumnByID(v.Column); ok && col.Counter() {
			// Counter cells carry no stamps — nothing to advance.
			continue
		}
		out.cellUpdates = append(out.cellUpdates, cellClockUpdate{
			table: upd.Table, pk: upd.PK, col: v.Column, stamp: stamp,
		})
	}
	return out, nil
}

// cellUpdateHasCounter reports whether upd carries any counter
// contribution (FormatDelta value — set by materialize for update
// deltas and by Table.AsCellUpdate for same-generation counter
// images).
func cellUpdateHasCounter(upd crdt.Update) bool {
	for _, v := range upd.Changed {
		if v.Format == crdt.FormatDelta {
			return true
		}
	}
	return false
}
