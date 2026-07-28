package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// arbitrate runs the loser-null UNIQUE-key algorithm from
// sqlite/docs/DDL.md#unique-keys for one record. Returns a possibly-rewritten record
// (the loser path nulls R's key-column writes) and a list of
// cellClockUpdates for any Q rows whose key columns we stole and nulled
// in app.db.
//
// The spec's `cell_clock[Q, c ∈ K] := R.stamp` write keeps Q's
// row-baseline (`row_clock[Q]`) untouched; only the K columns of Q gain
// per-cell overrides at R.stamp. This is what gives non-K writes to Q
// landing concurrently with R the right LWW semantics — they compare
// against Q's original baseline, not against R's stamp.
func (b *Broker) arbitrate(tab *catalog.Table, rec crdt.Record, stamp crdt.Stamp) (crdt.Record, []cellClockUpdate, error) {
	if len(tab.UniqueKeys) == 0 {
		return rec, nil, nil
	}
	switch r := rec.(type) {
	case crdt.Insert:
		newImage, stolen, err := b.arbitrateImage(tab, r.PK, r.Image, stamp, true)
		if err != nil {
			return rec, nil, err
		}
		if newImage != nil {
			r.Image = newImage
			rec = r
		}
		return rec, stolen, nil
	case crdt.Update:
		newChanged, stolen, err := b.arbitrateImage(tab, r.PK, r.Changed, stamp, false)
		if err != nil {
			return rec, nil, err
		}
		if newChanged != nil {
			r.Changed = newChanged
			rec = r
		}
		return rec, stolen, nil
	}
	return rec, nil, nil
}

// arbitrateImage runs arbitration for an Insert (full=true: image covers
// every column) or Update (full=false: writes is the partial Changed
// list). Returns a non-nil replacement slice if a loser-rewrite touched
// any column.
func (b *Broker) arbitrateImage(tab *catalog.Table, pk crdt.PKBlob, writes []crdt.ColValue, stamp crdt.Stamp, full bool) ([]crdt.ColValue, []cellClockUpdate, error) {
	pkVals, err := pkValueMap(tab, pk)
	if err != nil {
		return nil, nil, err
	}
	writesMap := make(map[crdt.ColumnID]crdt.ColValue, len(writes))
	for _, v := range writes {
		writesMap[v.Column] = v
	}
	// loserNulls collects column IDs the loser path wants nulled in
	// writes. We apply them once at the end so multiple unique keys
	// sharing a column converge.
	loserNulls := map[crdt.ColumnID]struct{}{}
	var stolen []cellClockUpdate
	for _, key := range tab.UniqueKeys {
		if key.Coordinated {
			// CP key: uniqueness comes from the pre-commit reservation
			// gate alone. The eventual loser-null algorithm must not run
			// here — it would ignore the key's partial predicate and
			// false-match a soft-deleted row, then try to NULL a NOT NULL
			// key column, wedging apply in quarantine.
			//
			// There is deliberately no second line of defense on apply:
			// the key is syzy_key metadata only on every node (the
			// originator normalizes its physical index away at DDL time),
			// so no index rejects a duplicate and this skip means no
			// arbitration converges one. An out-of-gate duplicate (rows
			// predating the key on a partitioned node) is detected by the
			// leaseholder's enumeration, which fences the value and
			// surfaces it via Node.CoordinatedDuplicates for manual
			// repair — see sqlite/docs/OPERATIONS.md.
			continue
		}
		// Determine R's K-column tuple. Loser nulls from earlier keys
		// in this same record propagate (they'd null v before we even
		// look it up).
		kvals, touched, err := b.keyColumnValues(tab, key, pkVals, writesMap, loserNulls, full)
		if err != nil {
			return nil, nil, err
		}
		if !touched {
			continue
		}
		if hasNull(kvals) {
			continue
		}
		qPK, found, err := b.findUniqueOwner(tab, key, kvals)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			continue
		}
		if bytes.Equal(qPK, pk) {
			continue
		}
		qState := b.cfg.Cache.RowState(tab.ID, qPK)
		// Per sqlite/docs/DDL.md#unique-keys, R wins iff stamp dominates Q's
		// effective stamp on the key tuple (max over c ∈ K).
		if stamp.Dominates(maxEffectiveStamp(qState, key)) {
			if err := b.nullKeyColumns(tab, key, qPK); err != nil {
				return nil, nil, err
			}
			for _, col := range key.Columns {
				stolen = append(stolen, cellClockUpdate{
					table: tab.ID,
					pk:    qPK,
					col:   col.ID,
					stamp: stamp,
				})
			}
		} else {
			for _, col := range key.Columns {
				loserNulls[col.ID] = struct{}{}
			}
		}
	}
	if len(loserNulls) == 0 {
		return nil, stolen, nil
	}
	out := rewriteWritesNull(tab, writes, loserNulls, full)
	return out, stolen, nil
}

// maxEffectiveStamp returns the maximum effective Stamp across the
// key's columns — the moment at which the row most-recently asserted
// ownership of the K tuple. Spec: sqlite/docs/DDL.md
// `s_Q = MAX over c ∈ K of effective_stamp(Q, c)`.
func maxEffectiveStamp(rs crdt.RowState, key catalog.UniqueKey) crdt.Stamp {
	out := rs.Base
	for _, col := range key.Columns {
		if s := rs.EffectiveStamp(col.ID, crdt.ByteRange{}); s.Dominates(out) {
			out = s
		}
	}
	return out
}

// keyColumnValues returns (kvals, touched). kvals are the K columns'
// values in declared order, drawing from PK, writes, and (for Update
// records on a K column not in Changed) the live SQLite row. touched
// is false iff none of K's columns appears in writes (Update only —
// for Insert, the image is full so every column is touched).
//
// loserNulls force-NULL the listed columns when constructing kvals,
// so the within-record propagation in arbitrateImage stays consistent.
func (b *Broker) keyColumnValues(
	tab *catalog.Table,
	key catalog.UniqueKey,
	pkVals map[crdt.ColumnID]crdt.ColValue,
	writesMap map[crdt.ColumnID]crdt.ColValue,
	loserNulls map[crdt.ColumnID]struct{},
	full bool,
) ([]crdt.ColValue, bool, error) {
	// Update fast path: when no K column appears in writes the caller
	// skips arbitration for this key entirely, so don't pay the live-row
	// SELECT just to build kvals that will be discarded.
	if !full {
		touched := false
		for _, col := range key.Columns {
			if _, isLoser := loserNulls[col.ID]; isLoser {
				continue
			}
			if _, ok := writesMap[col.ID]; ok {
				touched = true
				break
			}
		}
		if !touched {
			return nil, false, nil
		}
	}
	kvals := make([]crdt.ColValue, len(key.Columns))
	touched := false
	var liveByID map[crdt.ColumnID]crdt.ColValue
	for i, col := range key.Columns {
		if _, ok := loserNulls[col.ID]; ok {
			kvals[i] = crdt.ColValue{Column: col.ID, TypeTag: crdt.ColNull}
			continue
		}
		if v, ok := writesMap[col.ID]; ok {
			kvals[i] = v
			touched = true
			continue
		}
		if v, ok := pkVals[col.ID]; ok {
			kvals[i] = v
			continue
		}
		if full {
			// Insert image is supposed to be full; treat absence as NULL.
			kvals[i] = crdt.ColValue{Column: col.ID, TypeTag: crdt.ColNull}
			continue
		}
		// Update: need the live row's value for this K column.
		if liveByID == nil {
			lvals, err := b.readKeyColumns(tab, key, pkVals)
			if err != nil {
				return nil, false, err
			}
			liveByID = lvals
		}
		if v, ok := liveByID[col.ID]; ok {
			kvals[i] = v
		} else {
			kvals[i] = crdt.ColValue{Column: col.ID, TypeTag: crdt.ColNull}
		}
	}
	return kvals, touched, nil
}

// readKeyColumns runs a SELECT against the apply conn for the K columns
// of an existing row identified by pkVals. Used by Update arbitration
// when only some K columns are in r.Changed.
func (b *Broker) readKeyColumns(tab *catalog.Table, key catalog.UniqueKey, pkVals map[crdt.ColumnID]crdt.ColValue) (map[crdt.ColumnID]crdt.ColValue, error) {
	stmt, err := b.uniqReadStmt(tab, key)
	if err != nil {
		return nil, err
	}
	if err := stmt.Reset(); err != nil {
		return nil, err
	}
	// Reset on every exit: a cached SELECT abandoned at SQLITE_ROW keeps an
	// implicit read transaction open on AppApply. That pinned snapshot goes
	// stale on the producer's next commit, after which every BEGIN IMMEDIATE
	// on this connection fails SQLITE_BUSY_SNAPSHOT ("database is locked")
	// until the statement is reset — permanently wedging inbound apply.
	defer func() { _ = stmt.Reset() }()
	for i, pkCol := range tab.PK {
		v, ok := pkVals[pkCol.ID]
		if !ok {
			return nil, fmt.Errorf("broker: unique read missing PK col %q", pkCol.Name)
		}
		if err := bindColValue(stmt, i+1, v); err != nil {
			return nil, err
		}
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	out := map[crdt.ColumnID]crdt.ColValue{}
	if !hasRow {
		return out, nil
	}
	for i, col := range key.Columns {
		out[col.ID] = readColValueAt(stmt, i, col.ID)
	}
	return out, nil
}

// findUniqueOwner looks up the existing PK that owns (key.Columns =
// kvals). Returns (qPK, true, nil) on a hit; SQLite's UNIQUE index
// guarantees at most one such row.
func (b *Broker) findUniqueOwner(tab *catalog.Table, key catalog.UniqueKey, kvals []crdt.ColValue) (crdt.PKBlob, bool, error) {
	stmt, err := b.uniqSelectStmt(tab, key)
	if err != nil {
		return nil, false, err
	}
	if err := stmt.Reset(); err != nil {
		return nil, false, err
	}
	// Reset on every exit (incl. the EncodePK error path): see
	// readKeyColumns — an abandoned SQLITE_ROW statement pins a read
	// snapshot on AppApply and wedges every later BEGIN IMMEDIATE.
	defer func() { _ = stmt.Reset() }()
	for i, v := range kvals {
		if err := bindColValue(stmt, i+1, v); err != nil {
			return nil, false, err
		}
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, false, err
	}
	if !hasRow {
		return nil, false, nil
	}
	pkByID := make(map[crdt.ColumnID]crdt.ColValue, len(tab.PK))
	for i, pkCol := range tab.PK {
		pkByID[pkCol.ID] = readColValueAt(stmt, i, pkCol.ID)
	}
	pk, err := tab.EncodePK(pkByID)
	if err != nil {
		return nil, false, err
	}
	return pk, true, nil
}

// nullKeyColumns issues UPDATE tab SET <k cols> = NULL WHERE pk = qPK
// using the cached statement for (tab, key).
func (b *Broker) nullKeyColumns(tab *catalog.Table, key catalog.UniqueKey, qPK crdt.PKBlob) error {
	stmt, err := b.uniqNullStmt(tab, key)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	pkByID, err := tab.DecodePK(qPK)
	if err != nil {
		return err
	}
	for i, pkCol := range tab.PK {
		v, ok := pkByID[pkCol.ID]
		if !ok {
			return fmt.Errorf("broker: nullKeyColumns missing PK col %q", pkCol.Name)
		}
		if err := bindColValue(stmt, i+1, v); err != nil {
			return err
		}
	}
	_, err = stmt.Step()
	return err
}

func (b *Broker) uniqSelectStmt(tab *catalog.Table, key catalog.UniqueKey) (*sqlitebridge.Stmt, error) {
	b.stmtsMu.Lock()
	defer b.stmtsMu.Unlock()
	if b.uniqSelectStmts == nil {
		b.uniqSelectStmts = map[uniqStmtKey]*sqlitebridge.Stmt{}
	}
	k := uniqStmtKey{table: tab.ID, key: key.KeyID}
	if s, ok := b.uniqSelectStmts[k]; ok {
		return s, nil
	}
	pkCols := make([]string, len(tab.PK))
	for i, pk := range tab.PK {
		pkCols[i] = sqlitebridge.QuoteIdent(pk.Name)
	}
	wheres := make([]string, len(key.Columns))
	for i, col := range key.Columns {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(col.Name))
	}
	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s LIMIT 1`,
		strings.Join(pkCols, ", "),
		sqlitebridge.QuoteIdent(tab.Name),
		strings.Join(wheres, " AND "),
	)
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return nil, err
	}
	b.uniqSelectStmts[k] = stmt
	return stmt, nil
}

func (b *Broker) uniqNullStmt(tab *catalog.Table, key catalog.UniqueKey) (*sqlitebridge.Stmt, error) {
	b.stmtsMu.Lock()
	defer b.stmtsMu.Unlock()
	if b.uniqNullStmts == nil {
		b.uniqNullStmts = map[uniqStmtKey]*sqlitebridge.Stmt{}
	}
	k := uniqStmtKey{table: tab.ID, key: key.KeyID}
	if s, ok := b.uniqNullStmts[k]; ok {
		return s, nil
	}
	sets := make([]string, len(key.Columns))
	for i, col := range key.Columns {
		sets[i] = fmt.Sprintf("%s = NULL", sqlitebridge.QuoteIdent(col.Name))
	}
	wheres := make([]string, len(tab.PK))
	for i, pk := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(pk.Name))
	}
	sql := fmt.Sprintf(`UPDATE %s SET %s WHERE %s`,
		sqlitebridge.QuoteIdent(tab.Name),
		strings.Join(sets, ", "),
		strings.Join(wheres, " AND "),
	)
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return nil, err
	}
	b.uniqNullStmts[k] = stmt
	return stmt, nil
}

func (b *Broker) uniqReadStmt(tab *catalog.Table, key catalog.UniqueKey) (*sqlitebridge.Stmt, error) {
	b.stmtsMu.Lock()
	defer b.stmtsMu.Unlock()
	if b.uniqReadStmts == nil {
		b.uniqReadStmts = map[uniqStmtKey]*sqlitebridge.Stmt{}
	}
	k := uniqStmtKey{table: tab.ID, key: key.KeyID}
	if s, ok := b.uniqReadStmts[k]; ok {
		return s, nil
	}
	cols := make([]string, len(key.Columns))
	for i, col := range key.Columns {
		cols[i] = sqlitebridge.QuoteIdent(col.Name)
	}
	wheres := make([]string, len(tab.PK))
	for i, pk := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(pk.Name))
	}
	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s LIMIT 1`,
		strings.Join(cols, ", "),
		sqlitebridge.QuoteIdent(tab.Name),
		strings.Join(wheres, " AND "),
	)
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return nil, err
	}
	b.uniqReadStmts[k] = stmt
	return stmt, nil
}

// uniqStmtKey keys the per-(table, unique-key) prepared statement caches.
type uniqStmtKey struct {
	table crdt.TableID
	key   crdt.KeyID
}

// pkValueMap returns the map of PK column ID → ColValue parsed from pk.
func pkValueMap(tab *catalog.Table, pk crdt.PKBlob) (map[crdt.ColumnID]crdt.ColValue, error) {
	return tab.DecodePK(pk)
}

func hasNull(vals []crdt.ColValue) bool {
	for _, v := range vals {
		if v.TypeTag == crdt.ColNull {
			return true
		}
	}
	return false
}

// rewriteWritesNull returns a copy of writes with the listed column IDs
// nulled. For Insert (full=true) every K column should appear in writes;
// for Update (full=false) only K columns actually in writes are rewritten.
func rewriteWritesNull(tab *catalog.Table, writes []crdt.ColValue, nullSet map[crdt.ColumnID]struct{}, full bool) []crdt.ColValue {
	out := make([]crdt.ColValue, 0, len(writes)+len(nullSet))
	seen := map[crdt.ColumnID]struct{}{}
	for _, v := range writes {
		if _, ok := nullSet[v.Column]; ok {
			out = append(out, crdt.ColValue{Column: v.Column, TypeTag: crdt.ColNull})
		} else {
			out = append(out, v)
		}
		seen[v.Column] = struct{}{}
	}
	if full {
		// An Insert image is full; if a key column was missing from
		// writes (shouldn't happen by spec), append an explicit NULL so
		// the loser-null semantics still hold downstream.
		for id := range nullSet {
			if _, ok := seen[id]; ok {
				continue
			}
			out = append(out, crdt.ColValue{Column: id, TypeTag: crdt.ColNull})
		}
	}
	return out
}

// readColValueAt copies the i-th SQLite result column into a crdt.ColValue
// tagged with colID. Unknown SQLite types degrade to NULL — never observed
// in practice (SQLite's storage classes are exhaustive).
func readColValueAt(stmt *sqlitebridge.Stmt, i int, colID crdt.ColumnID) crdt.ColValue {
	switch stmt.ColumnType(i) {
	case sqlitebridge.ColumnNull:
		return crdt.ColValue{Column: colID, TypeTag: crdt.ColNull}
	case sqlitebridge.ColumnInt:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(stmt.ColumnInt64(i)))
		out := make([]byte, 8)
		copy(out, b[:])
		return crdt.ColValue{Column: colID, TypeTag: crdt.ColInt, Bytes: out}
	case sqlitebridge.ColumnReal:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(stmt.ColumnFloat64(i)))
		out := make([]byte, 8)
		copy(out, b[:])
		return crdt.ColValue{Column: colID, TypeTag: crdt.ColReal, Bytes: out}
	case sqlitebridge.ColumnText:
		return crdt.ColValue{Column: colID, TypeTag: crdt.ColText, Bytes: []byte(stmt.ColumnText(i))}
	case sqlitebridge.ColumnBlob:
		return crdt.ColValue{Column: colID, TypeTag: crdt.ColBlob, Bytes: stmt.ColumnBlob(i)}
	}
	return crdt.ColValue{Column: colID, TypeTag: crdt.ColNull}
}
