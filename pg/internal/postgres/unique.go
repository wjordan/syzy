package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/nodestate"
)

// cellStamp is a deferred cell-clock mutation the arbitration schedules, applied
// to the Cache AFTER the apply tx commits, so a rolled-back apply
// leaves no clock behind. clear=false: set cell_clock[tid,pk,col] := stamp (a
// steal records the loser's ceded stamp). clear=true: delete the override (a
// winning key write drops a now-stale prior override so the column falls back to
// the row's fresh baseline).
type cellStamp struct {
	tid   crdt.TableID
	pk    crdt.PKBlob
	col   crdt.ColumnID
	stamp crdt.Stamp
	clear bool
}

// arbitrateUnique runs the §5 loser-null UNIQUE arbitration (DDL.md#unique-keys)
// for record rec (Insert/Update) writing pk=P, INSIDE the apply tx: findUniqueOwner
// sees rows written earlier in this same changeset and the loser-null UPDATEs stage
// on that tx. It returns rec with its column writes possibly rewritten — a cede
// nulls R's own key writes; a cell-LWW loss drops them (keeps the current value) —
// and the cell-clock writes the steal path scheduled on loser rows (the caller
// applies those to the Cache post-commit).
//
// PG is row-LWW, unlike SQLite (cell-grouped key columns + a standard cell-LWW
// pass), so this also runs the cell-LWW pass for key columns itself: a write that
// loses to a prior steal's cell_clock on its OWN row's key column is dropped,
// protecting a stolen-NULL from a stale overwrite — the case the contention pass
// alone (which only consults the CURRENT owner) does not cover.
// genBumped marks rec as advancing the row to a new generation (a recreate): the
// per-key-column cell-LWW pass is then skipped, since any cell override is from
// the prior generation and PutRowState's CL-bump clears it post-commit.
func arbitrateUnique(ctx context.Context, tx pgx.Tx, cache *nodestate.Cache, ti *tableInfo, rec crdt.Record, stamp crdt.Stamp, genBumped bool) (crdt.Record, []cellStamp, error) {
	if len(ti.uniqueKeys) == 0 {
		return rec, nil, nil
	}
	switch r := rec.(type) {
	case crdt.Insert:
		writes, stolen, err := arbitrateImage(ctx, tx, cache, ti, r.PK, r.Image, stamp, true, genBumped)
		if err != nil {
			return rec, nil, err
		}
		if writes != nil {
			r.Image = writes
			rec = r
		}
		return rec, stolen, nil
	case crdt.Update:
		writes, stolen, err := arbitrateImage(ctx, tx, cache, ti, r.PK, r.Changed, stamp, false, genBumped)
		if err != nil {
			return rec, nil, err
		}
		if writes != nil {
			r.Changed = writes
			rec = r
		}
		return rec, stolen, nil
	}
	return rec, nil, nil
}

// arbitrateImage is the shared body for an Insert (full=true: writes is a full
// row image) and an Update (full=false: writes is the partial Changed list). It
// returns a non-nil replacement slice iff any column was dropped or nulled.
func arbitrateImage(ctx context.Context, tx pgx.Tx, cache *nodestate.Cache, ti *tableInfo, pk crdt.PKBlob, writes []crdt.ColValue, stamp crdt.Stamp, full, genBumped bool) ([]crdt.ColValue, []cellStamp, error) {
	writesMap := make(map[crdt.ColumnID]crdt.ColValue, len(writes))
	for _, v := range writes {
		writesMap[v.Column] = v
	}
	rowState := cache.RowState(ti.tid, pk)

	// stolen carries every post-commit cell-clock mutation: steal overrides on
	// loser rows (below) and clears on this row's winning key columns (next).
	var stolen []cellStamp

	// (b) Cell-LWW pass for key columns. A write to a key column that loses to a
	// prior steal's cell_clock on THIS row is dropped — keep the current value
	// (removing it from writesMap also makes keyColumnValues read the live value,
	// so a stolen-NULL column nulls the whole tuple). A key column that WINS but
	// carries a now-stale cell override has that override cleared post-commit, so
	// its effective stamp follows the row's fresh baseline. Skipped on a generation
	// bump (recreate): the prior generation's overrides are about to be cleared.
	dropped := map[crdt.ColumnID]struct{}{}
	if !genBumped {
		for _, key := range ti.uniqueKeys {
			if key.coordinated {
				continue // reservation-free CP key: never cell-arbitrated
			}
			for _, col := range key.cols {
				if _, ok := writesMap[col.cid]; !ok {
					continue
				}
				if _, done := dropped[col.cid]; done {
					continue
				}
				if rowState.EffectiveStamp(col.cid, crdt.ByteRange{}).Dominates(stamp) {
					dropped[col.cid] = struct{}{}
					delete(writesMap, col.cid)
				} else if _, hasCell := rowState.Cells[col.cid]; hasCell {
					stolen = append(stolen, cellStamp{tid: ti.tid, pk: pk, col: col.cid, clear: true})
				}
			}
		}
	}

	// loserNulls collects key columns the cede path wants nulled in R's writes,
	// applied once at the end so multiple keys sharing a column converge.
	loserNulls := map[crdt.ColumnID]struct{}{}
	for _, key := range ti.uniqueKeys {
		if key.coordinated {
			// The leaseholder gate + every node's physical index guarantee
			// exclusivity by construction; there is never a loser to null.
			continue
		}
		kvals, touched, err := keyColumnValues(ctx, tx, ti, key, pk, writesMap, loserNulls)
		if err != nil {
			return nil, nil, err
		}
		if !touched || hasNullCV(kvals) {
			continue
		}
		qPK, found, err := findUniqueOwner(ctx, tx, ti, key, kvals)
		if err != nil {
			return nil, nil, err
		}
		if !found || bytes.Equal(qPK, pk) {
			continue
		}
		qState := cache.RowState(ti.tid, qPK)
		// R wins iff its stamp dominates Q's effective stamp on the key tuple
		// (max over c ∈ K) — the latest moment Q asserted ownership of v.
		if stamp.Dominates(maxEffectiveStampPG(qState, key)) {
			if err := nullKeyColumns(ctx, tx, ti, key, qPK); err != nil {
				return nil, nil, err
			}
			for _, col := range key.cols {
				stolen = append(stolen, cellStamp{tid: ti.tid, pk: qPK, col: col.cid, stamp: stamp})
			}
		} else {
			for _, col := range key.cols {
				loserNulls[col.cid] = struct{}{}
			}
		}
	}
	if len(dropped) == 0 && len(loserNulls) == 0 {
		return nil, stolen, nil
	}
	return rewriteWrites(writes, dropped, loserNulls, full), stolen, nil
}

// keyColumnValues returns (kvals, touched). kvals are the key's column values in
// declared order, drawing from writes and (for a column not written — an Update
// column not in Changed, or one dropped by the cell-LWW pass) the live row.
// touched is true iff at least one key column is in writes: only then can R's
// write change the tuple. Key columns are never PK columns (PK is NOT NULL,
// rejected at capture), so the PK never contributes to a unique tuple.
//
// loserNulls force-NULL the listed columns (an earlier key's cede in this same
// record), keeping within-record propagation consistent.
func keyColumnValues(ctx context.Context, tx pgx.Tx, ti *tableInfo, key *uniqueKey, pk crdt.PKBlob, writesMap map[crdt.ColumnID]crdt.ColValue, loserNulls map[crdt.ColumnID]struct{}) ([]crdt.ColValue, bool, error) {
	kvals := make([]crdt.ColValue, len(key.cols))
	touched := false
	var live map[crdt.ColumnID]crdt.ColValue
	for i, col := range key.cols {
		if _, ok := loserNulls[col.cid]; ok {
			kvals[i] = crdt.ColValue{Column: col.cid, TypeTag: crdt.ColNull}
			continue
		}
		if v, ok := writesMap[col.cid]; ok {
			kvals[i] = v
			touched = true
			continue
		}
		// Not written: read the current (live) value of every key column once.
		if live == nil {
			lvals, err := readKeyColumns(ctx, tx, ti, key, pk)
			if err != nil {
				return nil, false, err
			}
			live = lvals
		}
		if v, ok := live[col.cid]; ok {
			kvals[i] = v
		} else {
			kvals[i] = crdt.ColValue{Column: col.cid, TypeTag: crdt.ColNull}
		}
	}
	return kvals, touched, nil
}

// readKeyColumns SELECTs a row's key-column values (as text) by PK against the
// apply tx, so it reflects rows written earlier in this changeset.
func readKeyColumns(ctx context.Context, tx pgx.Tx, ti *tableInfo, key *uniqueKey, pk crdt.PKBlob) (map[crdt.ColumnID]crdt.ColValue, error) {
	cols := make([]string, len(key.cols))
	for i, col := range key.cols {
		cols[i] = quoteIdent(col.name) + "::text"
	}
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1",
		strings.Join(cols, ", "), tableRef(ti), pkWhere(ti, pk))
	dst := make([]*string, len(key.cols))
	scan := make([]any, len(key.cols))
	for i := range dst {
		scan[i] = &dst[i]
	}
	out := map[crdt.ColumnID]crdt.ColValue{}
	if err := tx.QueryRow(ctx, sql).Scan(scan...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, nil
		}
		return nil, fmt.Errorf("unique read %s: %w", ti.name, err)
	}
	for i, col := range key.cols {
		if dst[i] == nil {
			out[col.cid] = crdt.ColValue{Column: col.cid, TypeTag: crdt.ColNull}
			continue
		}
		cv, err := encodeColValue(col.cid, col.typeName, []byte(*dst[i]))
		if err != nil {
			return nil, fmt.Errorf("unique read %s.%s: %w", ti.name, col.name, err)
		}
		out[col.cid] = cv
	}
	return out, nil
}

// findUniqueOwner returns the canonical PKBlob of the existing row whose key
// columns equal kvals (the originator's physical UNIQUE index — or, on a
// follower, the row data alone — guarantees at most one). PK columns are read
// ::text and re-encoded typed to rebuild the same canonical pkBlob capture
// produced, so the result keys the Cache for maxEffectiveStampPG.
func findUniqueOwner(ctx context.Context, tx pgx.Tx, ti *tableInfo, key *uniqueKey, kvals []crdt.ColValue) (crdt.PKBlob, bool, error) {
	preds := make([]string, len(key.cols))
	for i, col := range key.cols {
		preds[i] = fmt.Sprintf("%s = %s", quoteIdent(col.name), literal(kvals[i], col.typeName))
	}
	sel := make([]string, len(ti.pk))
	for i, c := range ti.pk {
		sel[i] = quoteIdent(c.name) + "::text"
	}
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1",
		strings.Join(sel, ", "), tableRef(ti), strings.Join(preds, " AND "))
	vals := make([]string, len(ti.pk))
	scan := make([]any, len(ti.pk))
	for i := range vals {
		scan[i] = &vals[i]
	}
	if err := tx.QueryRow(ctx, sql).Scan(scan...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("unique owner %s: %w", ti.name, err)
	}
	pkCVs := make([]crdt.ColValue, len(vals))
	for i, v := range vals {
		cv, err := encodeColValue(ti.pk[i].cid, ti.pk[i].typeName, []byte(v))
		if err != nil {
			return nil, false, fmt.Errorf("unique owner %s pk: %w", ti.name, err)
		}
		pkCVs[i] = cv
	}
	return pkBlobTyped(pkCVs), true, nil
}

// nullKeyColumns stages UPDATE <table> SET <k cols> = NULL WHERE pk = qPK on the
// apply tx — the steal that cedes v away from the losing owner Q.
func nullKeyColumns(ctx context.Context, tx pgx.Tx, ti *tableInfo, key *uniqueKey, qPK crdt.PKBlob) error {
	sets := make([]string, len(key.cols))
	for i, col := range key.cols {
		sets[i] = quoteIdent(col.name) + " = NULL"
	}
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		tableRef(ti), strings.Join(sets, ", "), pkWhere(ti, qPK))
	_, err := tx.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("unique steal %s: %w", ti.name, err)
	}
	return nil
}

// maxEffectiveStampPG is s_Q = MAX over c ∈ K of effective_stamp(Q, c): the
// latest moment Q asserted the key tuple. effective_stamp falls through
// cell_clock[Q,c] → row baseline (CRDT.md#layer-composition).
func maxEffectiveStampPG(rs crdt.RowState, key *uniqueKey) crdt.Stamp {
	out := rs.Base
	for _, col := range key.cols {
		if s := rs.EffectiveStamp(col.cid, crdt.ByteRange{}); s.Dominates(out) {
			out = s
		}
	}
	return out
}

// rewriteWrites produces R's final column writes: dropped columns (cell-LWW
// losses) are removed entirely (keep the current value); loserNulls columns
// (cede losses) are written as NULL. For a full Insert image a loserNull column
// missing from writes is appended as an explicit NULL.
func rewriteWrites(writes []crdt.ColValue, dropped, loserNulls map[crdt.ColumnID]struct{}, full bool) []crdt.ColValue {
	out := make([]crdt.ColValue, 0, len(writes))
	seen := map[crdt.ColumnID]struct{}{}
	for _, v := range writes {
		if _, ok := dropped[v.Column]; ok {
			continue
		}
		seen[v.Column] = struct{}{}
		if _, ok := loserNulls[v.Column]; ok {
			out = append(out, crdt.ColValue{Column: v.Column, TypeTag: crdt.ColNull})
		} else {
			out = append(out, v)
		}
	}
	if full {
		for id := range loserNulls {
			if _, ok := seen[id]; ok {
				continue
			}
			if _, ok := dropped[id]; ok {
				continue
			}
			out = append(out, crdt.ColValue{Column: id, TypeTag: crdt.ColNull})
		}
	}
	return out
}

func hasNullCV(vals []crdt.ColValue) bool {
	for _, v := range vals {
		if v.TypeTag == crdt.ColNull {
			return true
		}
	}
	return false
}
