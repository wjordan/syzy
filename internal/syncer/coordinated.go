package syncer

import (
	"bytes"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/unique"
)

// coordTouched is the net effect of one transaction's preupdate fires on
// a single (table, rowid) row, reduced to what coordinated-uniqueness
// reservation needs: the pre-transaction OLD image and the final NEW
// image. firstOld/lastNew alias into the touch buffer.
type coordTouched struct {
	tab      *catalog.Table
	firstOld []crdt.ColValue // OLD values of the first fire; nil for an INSERT-first row
	lastNew  []crdt.ColValue // NEW values of the last fire; nil if the row ends deleted
}

// CoordinatedClaims scans a transaction's touch buffer and returns the
// reservation claims it implies: `reserves` are the coordinated key
// values the transaction's rows now hold (claimed before commit), and
// `releases` are the values those rows vacated (freed after commit).
//
// It nets each row's fires first, so same-transaction value churn
// (insert-then-delete, A→B→A) collapses and never touches the registry.
// Only tables with active coordinated keys contribute; NULL key tuples
// are skipped. touchBuf and the returned claims' Value/Owner bytes alias
// into touchBuf — copy if they must outlive it.
func CoordinatedClaims(cat *catalog.Catalog, touchBuf []byte) (reserves, releases []unique.Claim, err error) {
	recs, err := parseJournal(touchBuf, nil)
	if err != nil {
		return nil, nil, err
	}

	// Net-fold per (table, rowid). The slice is tiny (one txn's touched
	// rows), so a linear scan beats a map.
	type slot struct {
		table []byte
		rowid int64
		t     coordTouched
	}
	var slots []slot
	find := func(table []byte, rowid int64) int {
		for i := range slots {
			if slots[i].rowid == rowid && bytes.Equal(slots[i].table, table) {
				return i
			}
		}
		return -1
	}

	mainName := []byte("main")
	for i := range recs {
		r := &recs[i]
		// blob_write / blob_intent fires carry no whole-value image; a
		// coordinated key is whole-value, so they never bear on it.
		if r.Op != sqliteInsert && r.Op != sqliteUpdate && r.Op != sqliteDelete {
			continue
		}
		if !bytes.Equal(r.DBName, mainName) {
			continue
		}
		tab, ok := cat.TableBytes(r.Table)
		if !ok || len(tab.UniqueKeys) == 0 {
			continue
		}
		hasCoord := false
		for _, uk := range tab.UniqueKeys {
			if uk.Coordinated {
				hasCoord = true
				break
			}
		}
		if !hasCoord {
			continue
		}

		var rowid int64
		var firstOld, newVals []crdt.ColValue
		switch r.Op {
		case sqliteInsert:
			rowid = r.NewRowID
			newVals = r.Values
		case sqliteUpdate:
			rowid = r.OldRowID
			firstOld = r.Values
			newVals = r.NewValues
		case sqliteDelete:
			rowid = r.OldRowID
			firstOld = r.Values
		}

		idx := find(r.Table, rowid)
		if idx < 0 {
			slots = append(slots, slot{table: r.Table, rowid: rowid, t: coordTouched{
				tab: tab, firstOld: firstOld, lastNew: newVals,
			}})
		} else {
			// Keep the first OLD (pre-txn image); take the latest NEW.
			slots[idx].t.lastNew = newVals
		}
	}

	for i := range slots {
		t := &slots[i].t
		if err := appendClaims(t, &reserves, &releases); err != nil {
			return nil, nil, err
		}
	}
	netSameTxnTransfers(reserves, &releases)
	return reserves, releases, nil
}

// TouchedTables returns the distinct main-database table names a
// transaction's touch buffer mutated via INSERT/UPDATE/DELETE fires
// (blob fires excluded). Names alias into touchBuf.
func TouchedTables(touchBuf []byte) ([][]byte, error) {
	recs, err := parseJournal(touchBuf, nil)
	if err != nil {
		return nil, err
	}
	mainName := []byte("main")
	var out [][]byte
	for i := range recs {
		r := &recs[i]
		if r.Op != sqliteInsert && r.Op != sqliteUpdate && r.Op != sqliteDelete {
			continue
		}
		if !bytes.Equal(r.DBName, mainName) {
			continue
		}
		seen := false
		for _, t := range out {
			if bytes.Equal(t, r.Table) {
				seen = true
				break
			}
		}
		if !seen {
			out = append(out, r.Table)
		}
	}
	return out, nil
}

// netSameTxnTransfers turns a value released by one row and reserved by a
// different row *in the same transaction* into a transfer rather than a
// free-then-claim. The releases apply post-commit but the reserve runs
// pre-commit, so without this the reserve would conflict with the still-
// held releaser (the headline case: soft-delete one row and insert another
// with the same value, atomically). Carrying the releaser's PK as the
// reserve's Prev lets the leaseholder transfer ownership, and the matching
// release is dropped (the value moves, it is not freed). This is safe
// across replicas because a transaction is one atomic changeset — a
// receiver never observes the releaser and the claimant both live, so the
// quarantine that guards *cross*-transaction reuse is not needed here.
func netSameTxnTransfers(reserves []unique.Claim, releases *[]unique.Claim) {
	rel := *releases
	if len(reserves) == 0 || len(rel) == 0 {
		return
	}
	relByValue := make(map[string]int, len(rel)) // Table||Key||Value -> release index
	for i := range rel {
		relByValue[valueKey(rel[i])] = i
	}
	consumed := make(map[int]bool)
	for r := range reserves {
		if len(reserves[r].Prev) != 0 {
			continue // already a same-row PK-change transfer
		}
		ri, ok := relByValue[valueKey(reserves[r])]
		if !ok || consumed[ri] || bytes.Equal(rel[ri].Owner, reserves[r].Owner) {
			continue
		}
		reserves[r].Prev = append([]byte(nil), rel[ri].Owner...)
		consumed[ri] = true
	}
	if len(consumed) == 0 {
		return
	}
	kept := rel[:0]
	for i := range rel {
		if !consumed[i] {
			kept = append(kept, rel[i])
		}
	}
	*releases = kept
}

// valueKey identifies a claim's reserved value (table, key, value) ignoring
// the owner — the granularity at which a same-transaction transfer matches
// a release to a reserve.
func valueKey(c unique.Claim) string {
	b := make([]byte, 0, 16+16+len(c.Value))
	b = append(b, c.Table[:]...)
	b = append(b, c.Key[:]...)
	b = append(b, c.Value...)
	return string(b)
}

// appendClaims emits, for each active coordinated key on the row's table,
// a reserve for the value the row now holds (if it participates) and a
// release for the value the row vacated (if it previously participated).
// "Participates" generalizes the non-NULL gate: a total key participates
// when its key tuple is non-NULL; a partial key additionally requires the
// index predicate to hold for the row image. Both images are full rows, so
// a predicate column the statement never touched is still present.
func appendClaims(t *coordTouched, reserves, releases *[]unique.Claim) error {
	var newPK, oldPK crdt.PKBlob
	var err error
	if t.lastNew != nil {
		if newPK, err = t.tab.EncodePKFromSlice(nil, t.lastNew); err != nil {
			return err
		}
	}
	if t.firstOld != nil {
		if oldPK, err = t.tab.EncodePKFromSlice(nil, t.firstOld); err != nil {
			return err
		}
	}

	// colIndex maps ColumnID → position in the t.tab.Columns-order image,
	// built lazily and only when a predicate needs it.
	var idxByID map[crdt.ColumnID]int
	colIndex := func() map[crdt.ColumnID]int {
		if idxByID == nil {
			idxByID = make(map[crdt.ColumnID]int, len(t.tab.Columns))
			for i, c := range t.tab.Columns {
				idxByID[c.ID] = i
			}
		}
		return idxByID
	}
	participates := func(uk catalog.UniqueKey, image []crdt.ColValue, keyNull bool) bool {
		if image == nil || keyNull {
			return false // row absent, or key tuple NULL (never reserved)
		}
		if uk.Predicate.Root == nil {
			return true // total key: every non-NULL row participates
		}
		idx := colIndex()
		return uk.Predicate.Eval(func(id crdt.ColumnID) crdt.ColValue {
			if i, ok := idx[id]; ok && i < len(image) {
				return image[i]
			}
			return crdt.ColValue{TypeTag: crdt.ColNull}
		})
	}

	for _, uk := range t.tab.UniqueKeys {
		if !uk.Coordinated {
			continue
		}
		var newVal, oldVal []byte
		var newNull, oldNull bool
		if t.lastNew != nil {
			if newVal, newNull, err = t.tab.EncodeKeyFromSlice(uk, t.lastNew); err != nil {
				return err
			}
		}
		if t.firstOld != nil {
			if oldVal, oldNull, err = t.tab.EncodeKeyFromSlice(uk, t.firstOld); err != nil {
				return err
			}
		}
		newPart := participates(uk, t.lastNew, newNull)
		oldPart := participates(uk, t.firstOld, oldNull)

		// Reserve the value the row now holds. Prev carries the old PK so a
		// PK-changing update that keeps the same participating value
		// transfers it from the old row to the new instead of conflicting
		// with itself.
		if newPart {
			c := unique.Claim{
				Table: [16]byte(t.tab.ID), Key: [16]byte(uk.KeyID),
				Value: newVal, Owner: newPK,
			}
			if oldPart && bytes.Equal(oldVal, newVal) && !bytes.Equal(oldPK, newPK) {
				c.Prev = oldPK
			}
			*reserves = append(*reserves, c)
		}
		// Release the value the row vacated — it participated before but no
		// longer does (deleted, predicate flipped false, or value changed).
		// An unchanged, still-participating value is left held (or, on a PK
		// change, transferred by the reserve above).
		if oldPart && (!newPart || !bytes.Equal(oldVal, newVal)) {
			*releases = append(*releases, unique.Claim{
				Table: [16]byte(t.tab.ID), Key: [16]byte(uk.KeyID),
				Value: oldVal, Owner: oldPK,
			})
		}
	}
	return nil
}
