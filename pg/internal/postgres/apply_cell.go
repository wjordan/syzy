package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/nodestate"
)

// Cell-group apply (§8): a table in the cell clock group arbitrates UPDATEs
// per column instead of per row, so concurrent writes to disjoint columns of
// one row merge and same-column writes still resolve by stamp. Row liveness
// stays row-level — Insert and Delete bump the causal length and arbitrate on
// (CL, Base) exactly as a row-group table does, and a CL bump tombstones the
// prior generation's per-column overrides.
//
// This mirrors internal/broker's applyCellUpdate for the Postgres engine; the
// per-column arbitration rules themselves are not restated — they come from the
// shared core (crdt.AsCellUpdate, crdt.CoversAllNonPK, RowState.EffectiveStamp),
// so the two engines cannot drift on which record wins which column.

// cellOutcome is applyCellUpdate's result: whether DML ran, the row clock to
// publish (if any), and the per-column overrides to publish after commit.
type cellOutcome struct {
	applied    bool
	rowUpdate  *rowClockWrite
	cellStamps []cellStamp
	// winnerCols is the column set the winning record arbitrated for, stashed
	// with the winner image so a losing local fold repairs exactly these columns.
	winnerCols map[crdt.ColumnID]struct{}
	// lost is the columns this record carried that lost their per-column gate —
	// the values discarded on the inbound side, for the conflict log (§9).
	lost []crdt.ColValue
}

// rowClockWrite is one row's post-commit clock advance: the state to publish
// and whether the prior generation's per-column overrides go with it.
type rowClockWrite struct {
	tid        crdt.TableID
	pk         crdt.PKBlob
	state      crdt.RowState
	clearCells bool
}

// applyCellUpdate arbitrates one Update against a cell-group table and runs the
// winning columns' DML inside the apply transaction. The caller has already
// admitted the record on causal length (and, for register-only records, on the
// row-level stamp); the per-column refinement happens here.
//
// certified is the applied marker's verdict (§8): this changeset's counter
// contributions have already landed once. It reaches here because a redelivered
// INSERT is normalized by crdt.AsCellUpdate — which re-tags the image's counter
// columns as contributions — so the row-level certified renderer never sees it.
// Summing them again is exactly what the marker exists to prevent, but the
// record must still be able to recreate a row that is no longer physically
// there, so the counters are rendered insert-if-absent rather than dropped.
func (a *applier) applyCellUpdate(ctx context.Context, tx pgx.Tx, cache *nodestate.Cache,
	ti *tableInfo, upd crdt.Update, rs crdt.RowState, stamp crdt.Stamp, genBumped, certified bool) (cellOutcome, error) {
	var out cellOutcome
	if err := validateCounterValues(ti, upd.Changed); err != nil {
		return out, err
	}
	newGen := upd.CL > rs.CL
	if len(upd.Changed) == 0 {
		if !newGen {
			return out, nil
		}
		// A redelivered counter-only update whose sole contribution the applied
		// marker stripped. It carries nothing left to write, but the committed
		// transaction it certifies DID advance this row to a new generation:
		// dropping that advance would leave the row clock a generation behind,
		// and the resurrecting Insert would then arrive as a row-level write and
		// overwrite the contribution that already landed.
		out.applied = true
		out.rowUpdate = &rowClockWrite{tid: upd.Table, pk: upd.PK,
			state: crdt.RowState{CL: upd.CL}, clearCells: true}
		return out, nil
	}

	// Per-column gate. A new generation (the writer saw a resurrection we have
	// not) wins every carried column; within the current generation each column
	// gates on its own effective stamp so concurrent disjoint-column updates
	// merge. Counter contributions carry no stamp and always land — they sum
	// instead of arbitrating (CRDT.md F_counter).
	winners := upd.Changed
	switch {
	case !newGen:
		winners = make([]crdt.ColValue, 0, len(upd.Changed))
		for _, v := range upd.Changed {
			if v.Format == crdt.FormatDelta ||
				stamp.Dominates(rs.EffectiveStamp(v.Column, crdt.ByteRange{})) {
				winners = append(winners, v)
				continue
			}
			out.lost = append(out.lost, v)
		}
		if len(winners) == 0 {
			return out, nil
		}
	case updateHasCounter(upd):
		// This update establishes the new generation locally: the physical row
		// still holds prior-generation content, so its contributions apply
		// absolutely (the generation's opening value) rather than summing onto
		// stale bytes. Later same-generation contributions still add.
		winners = make([]crdt.ColValue, len(upd.Changed))
		for i, v := range upd.Changed {
			if v.Format == crdt.FormatDelta {
				v.Format = crdt.FormatText
			}
			winners[i] = v
		}
	}

	arb, stolen, err := arbitrateUnique(ctx, tx, cache, ti,
		crdt.Update{Table: upd.Table, PK: upd.PK, CL: upd.CL, Changed: winners}, stamp, genBumped)
	if err != nil {
		return out, err
	}
	out.cellStamps = append(out.cellStamps, stolen...)
	arbUpd, ok := arb.(crdt.Update)
	if !ok {
		return out, fmt.Errorf("postgres: cell arbitration returned %T; want Update", arb)
	}
	if len(arbUpd.Changed) == 0 {
		return out, nil // arbitration dropped every column; row unchanged
	}
	w := renderUpsert(ti, cellImage(ti, upd.PK, arbUpd.Changed), certified)
	if err := execRowWrite(ctx, tx, w); err != nil {
		return out, fmt.Errorf("apply cell update %s: %w", ti.name, err)
	}
	out.applied = true
	out.winnerCols = make(map[crdt.ColumnID]struct{}, len(arbUpd.Changed))
	for _, v := range arbUpd.Changed {
		out.winnerCols[v.Column] = struct{}{}
	}

	switch {
	case crdt.CoversAllNonPK(ti, winners):
		// Opportunistic collapse: the write defines every column, so the row is
		// uniformly stamped — absorb it into the baseline and drop the overrides
		// instead of writing one per column.
		out.rowUpdate = &rowClockWrite{tid: upd.Table, pk: upd.PK,
			state: crdt.RowState{CL: upd.CL, Base: stamp}, clearCells: true}
		return out, nil
	case newGen:
		// Generation advance without the resurrecting Insert's full image: the
		// carried columns get overrides at this stamp, and Base stays zero so the
		// (lower-stamped) Insert still wins the columns this update did not carry.
		out.rowUpdate = &rowClockWrite{tid: upd.Table, pk: upd.PK,
			state: crdt.RowState{CL: upd.CL}, clearCells: true}
	}
	for _, v := range winners {
		if c := ti.colByID(v.Column); c != nil && c.counter {
			continue // counter cells carry no stamps — nothing to advance
		}
		out.cellStamps = append(out.cellStamps, cellStamp{tid: upd.Table, pk: upd.PK, col: v.Column, stamp: stamp})
	}
	return out, nil
}

// cellImage prefixes the arbitrated columns with the row's PK values: a cell
// update carries only the columns it changed, but the upsert that lands it
// still has to name the row it belongs to.
func cellImage(ti *tableInfo, pk crdt.PKBlob, changed []crdt.ColValue) []crdt.ColValue {
	vals := decodePKBlobTyped(pk)
	image := make([]crdt.ColValue, 0, len(ti.pk)+len(changed))
	for i, c := range ti.pk {
		if i >= len(vals) {
			break
		}
		cv := vals[i]
		cv.Column = c.cid
		image = append(image, cv)
	}
	return append(image, changed...)
}

// updateHasCounter reports whether upd carries any counter contribution.
func updateHasCounter(upd crdt.Update) bool {
	for _, v := range upd.Changed {
		if v.Format == crdt.FormatDelta {
			return true
		}
	}
	return false
}

// appliedMarkerTable is the exactly-once ledger counter applies write inside
// their own transaction (sql/counter.sql).
const appliedMarkerTable = "public.syzy_applied"

// appliedMarkerPresent reports whether (origin, seq) was certified by a prior
// committed apply — read inside the apply transaction so the answer cannot
// change under it.
func appliedMarkerPresent(ctx context.Context, tx pgx.Tx, dot crdt.Dot) (bool, error) {
	var present bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+appliedMarkerTable+` WHERE origin = $1 AND seq = $2)`,
		int64(dot.Origin), int64(dot.Seq)).Scan(&present); err != nil {
		return false, fmt.Errorf("postgres: applied marker probe: %w", err)
	}
	return present, nil
}

// writeAppliedMarker records (origin, seq) inside the apply transaction — as
// durable as the counter contributions it certifies — and prunes the entries
// the persisted sidecar frontier already covers.
func writeAppliedMarker(ctx context.Context, tx pgx.Tx, cache *nodestate.Cache, dot crdt.Dot) error {
	if _, err := tx.Exec(ctx, `INSERT INTO `+appliedMarkerTable+` (origin, seq) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		int64(dot.Origin), int64(dot.Seq)); err != nil {
		return fmt.Errorf("postgres: write applied marker: %w", err)
	}
	if bound := nodestate.MarkerPruneBound(cache.PersistedFrontierBound(dot.Origin), dot.Seq); bound > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM `+appliedMarkerTable+` WHERE origin = $1 AND seq <= $2`,
			int64(dot.Origin), int64(bound)); err != nil {
			return fmt.Errorf("postgres: prune applied markers: %w", err)
		}
	}
	return nil
}
