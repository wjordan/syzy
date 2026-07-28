package broker

import (
	"fmt"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// applyDMLReconciled handles a full Insert/Update for a row that has
// active blob_range_clock entries, per BLOB_PATCH.md "insert/update
// With Active blob_range_clock". Returns the per-column entries to
// persist (empty → caller deletes the row's blob_range_clock entry).
func (b *Broker) applyDMLReconciled(tab *catalog.Table, rec crdt.Record,
	rs crdt.RowState, newStamp crdt.Stamp,
	existing []metadata.BlobRangeClockEntry,
) ([]metadata.BlobRangeClockEntry, error) {
	maps := metadata.LoadIntervalMaps(existing)

	var (
		blobValues    []crdt.ColValue
		nonBlobValues []crdt.ColValue
		pk            crdt.PKBlob
		isInsert      bool
	)
	switch r := rec.(type) {
	case crdt.Insert:
		isInsert = true
		pk = r.PK
		for _, v := range r.Image {
			if v.TypeTag == crdt.ColBlob {
				blobValues = append(blobValues, v)
			} else {
				nonBlobValues = append(nonBlobValues, v)
			}
		}
	case crdt.Update:
		pk = r.PK
		for _, v := range r.Changed {
			if v.TypeTag == crdt.ColBlob {
				blobValues = append(blobValues, v)
			} else {
				nonBlobValues = append(nonBlobValues, v)
			}
		}
	default:
		return nil, fmt.Errorf("applyDMLReconciled: unsupported record %T", rec)
	}

	if isInsert {
		if err := b.upsertNonBlobInsert(tab, pk, nonBlobValues); err != nil {
			return nil, fmt.Errorf("upsert non-blob: %w", err)
		}
	} else if len(nonBlobValues) > 0 {
		if _, err := b.execRowUpdate(tab, pk, nonBlobValues); err != nil {
			return nil, fmt.Errorf("update non-blob: %w", err)
		}
	}

	if len(blobValues) == 0 {
		// Untouched columns aren't reconciled per spec; existing maps
		// survive.
		return metadata.EntriesFromMaps(maps), nil
	}

	rowid, ok, err := b.lookupRowid(tab, pk)
	if err != nil {
		return nil, fmt.Errorf("lookup rowid: %w", err)
	}
	if !ok {
		// Row vanished post non-blob UPSERT (constraint, etc.) —
		// convergent skip.
		return metadata.EntriesFromMaps(maps), nil
	}

	for _, v := range blobValues {
		col, ok := tab.ColumnByID(v.Column)
		if !ok {
			continue
		}
		m, hasMap := maps[v.Column]
		if !hasMap {
			m = crdt.NewIntervalMap()
			maps[v.Column] = m
		}
		if err := b.reconcileBlobColumn(tab, col, pk, rowid, v, m, rs.EffectiveStamp(v.Column, crdt.ByteRange{}), newStamp); err != nil {
			return nil, fmt.Errorf("reconcile blob column %q: %w", col.Name, err)
		}
	}

	return metadata.EntriesFromMaps(maps), nil
}

// reconcileBlobColumn mutates the on-disk blob and the column's
// IntervalMap per the BLOB_PATCH.md reconciliation algorithm.
func (b *Broker) reconcileBlobColumn(tab *catalog.Table, col catalog.Column,
	pk crdt.PKBlob, rowid int64, v crdt.ColValue, m crdt.IntervalMap,
	rcCol, newStamp crdt.Stamp,
) error {
	newLen := uint64(len(v.Bytes))

	// Surviving entries strictly dominate the incoming stamp; their
	// per-byte ranges are protected from the full-DML write.
	surviving := make([]crdt.IntervalEntry, 0, len(m.Entries()))
	var maxSurvEnd uint64
	for _, e := range m.Entries() {
		if e.Stamp.Dominates(newStamp) {
			surviving = append(surviving, e)
			if e.Range.End > maxSurvEnd {
				maxSurvEnd = e.Range.End
			}
		}
	}
	effectiveLen := newLen
	if maxSurvEnd > effectiveLen {
		effectiveLen = maxSurvEnd
	}

	curLen, err := b.blobColumnLen(tab, col, pk)
	if err != nil {
		return fmt.Errorf("read blob length: %w", err)
	}
	switch {
	case curLen > effectiveLen:
		if err := b.shrinkBlobColumn(tab, col, pk, effectiveLen); err != nil {
			return fmt.Errorf("shrink blob: %w", err)
		}
	case curLen < effectiveLen:
		if err := b.ensureBlobLen(tab, col, pk, effectiveLen); err != nil {
			return fmt.Errorf("extend blob: %w", err)
		}
	}
	m.Clip(effectiveLen)

	if effectiveLen == 0 && newLen == 0 {
		m.Prune(newStamp)
		return nil
	}
	bh, err := b.cfg.AppApply.OpenBlob("main", tab.Name, col.Name, rowid, true)
	if err != nil {
		return fmt.Errorf("open blob: %w", err)
	}
	defer bh.Close()
	// Zero-fill gaps in [new_len, effective_len) not covered by
	// surviving so substr-shrunk and ensureBlobLen-extended replicas
	// agree byte-for-byte.
	if effectiveLen > newLen {
		if err := zeroFillUncovered(bh, surviving, newLen, effectiveLen); err != nil {
			return fmt.Errorf("zero-fill: %w", err)
		}
	}
	won := m.Apply(0, newLen, newStamp, rcCol)
	for _, w := range won {
		if err := bh.Write(v.Bytes[w.Start:w.End], int(w.Start)); err != nil {
			return fmt.Errorf("blob_write %d..%d: %w", w.Start, w.End, err)
		}
	}
	m.Prune(newStamp)
	return nil
}

// zeroFillUncovered writes zeros to bytes in [lo, hi) not covered by
// surviving (sorted by Start, non-overlapping).
func zeroFillUncovered(bh *sqlitebridge.Blob, surviving []crdt.IntervalEntry, lo, hi uint64) error {
	if lo >= hi {
		return nil
	}
	cur := lo
	for _, e := range surviving {
		s, x := e.Range.Start, e.Range.End
		if x <= lo {
			continue
		}
		if s >= hi {
			break
		}
		if s < lo {
			s = lo
		}
		if x > hi {
			x = hi
		}
		if cur < s {
			if err := writeZeros(bh, cur, s); err != nil {
				return err
			}
		}
		if x > cur {
			cur = x
		}
	}
	if cur < hi {
		return writeZeros(bh, cur, hi)
	}
	return nil
}

func writeZeros(bh *sqlitebridge.Blob, lo, hi uint64) error {
	const chunk = 4096
	var zeros [chunk]byte
	off := lo
	for off < hi {
		n := hi - off
		if n > chunk {
			n = chunk
		}
		if err := bh.Write(zeros[:n], int(off)); err != nil {
			return fmt.Errorf("blob_write zeros %d..%d: %w", off, off+n, err)
		}
		off += n
	}
	return nil
}

// upsertNonBlobInsert does a full UPSERT including PK + non-blob image
// values, omitting blob columns so any surviving per-byte overrides
// stay intact for reconcileBlobColumn to apply against.
func (b *Broker) upsertNonBlobInsert(tab *catalog.Table, pk crdt.PKBlob, nonBlobValues []crdt.ColValue) error {
	pkVals, err := tab.DecodePK(pk)
	if err != nil {
		return err
	}
	nonBlobByID := make(map[crdt.ColumnID]crdt.ColValue, len(nonBlobValues))
	for _, v := range nonBlobValues {
		nonBlobByID[v.Column] = v
	}
	blobCols, err := b.blobColumns(tab)
	if err != nil {
		return err
	}
	var (
		cols, placeholders, updates []string
		binds                       []crdt.ColValue
	)
	for _, c := range tab.Columns {
		quoted := sqlitebridge.QuoteIdent(c.Name)
		if c.PKPos > 0 {
			v, ok := pkVals[c.ID]
			if !ok {
				return fmt.Errorf("upsertNonBlobInsert: missing PK col %q", c.Name)
			}
			cols = append(cols, quoted)
			placeholders = append(placeholders, "?")
			binds = append(binds, v)
			continue
		}
		if blobCols[c.ID] {
			cols = append(cols, quoted)
			placeholders = append(placeholders, "x''")
			continue
		}
		v, ok := nonBlobByID[c.ID]
		if !ok {
			continue
		}
		cols = append(cols, quoted)
		placeholders = append(placeholders, "?")
		updates = append(updates, fmt.Sprintf("%s = excluded.%s", quoted, quoted))
		binds = append(binds, v)
	}
	pkNames := make([]string, len(tab.PK))
	for i, p := range tab.PK {
		pkNames[i] = sqlitebridge.QuoteIdent(p.Name)
	}
	var sql string
	if len(updates) == 0 {
		sql = fmt.Sprintf(`INSERT OR IGNORE INTO %s (%s) VALUES (%s)`,
			sqlitebridge.QuoteIdent(tab.Name),
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)
	} else {
		sql = fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s`,
			sqlitebridge.QuoteIdent(tab.Name),
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(pkNames, ", "),
			strings.Join(updates, ", "),
		)
	}
	return execBound(b.cfg.AppApply, sql, binds)
}

// execRowUpdate builds and runs `UPDATE tab SET <cols> WHERE <pk>` for
// values. FormatDelta values become `col = col + ?` counter sums; the
// rest overwrite. Returns how many rows the UPDATE matched: -1 when no
// DML ran (nothing to set — every carried column empty or unknown to
// the catalog); 0 means the SQL executed but the PK found no physical
// row, which the cell-group path uses to detect an update that outran
// its row's INSERT. Shared by applyUpdate (row-level path) and the blob
// reconcile path (which passes the non-blob subset of an Update's
// Changed list).
func (b *Broker) execRowUpdate(tab *catalog.Table, pk crdt.PKBlob, values []crdt.ColValue) (matched int64, err error) {
	// Fold counter contributions into absolute writes with checked
	// arithmetic first (apply_counter.go); when the row is missing the
	// deltas stay in place and the UPDATE's 0-row result drives the
	// caller's materialization path.
	values, err = b.resolveCounterDeltas(tab, pk, values)
	if err != nil {
		return 0, err
	}
	sets := make([]string, 0, len(values))
	binds := make([]crdt.ColValue, 0, len(values)+len(tab.PK))
	for _, v := range values {
		col, ok := tab.ColumnByID(v.Column)
		if !ok {
			continue
		}
		quoted := sqlitebridge.QuoteIdent(col.Name)
		if v.Format == crdt.FormatDelta {
			// Counter contribution: sum into the current cell value.
			sets = append(sets, fmt.Sprintf("%s = %s + ?", quoted, quoted))
		} else {
			sets = append(sets, fmt.Sprintf("%s = ?", quoted))
		}
		binds = append(binds, v)
	}
	if len(sets) == 0 {
		return -1, nil
	}
	wheres := make([]string, len(tab.PK))
	for i, p := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(p.Name))
	}
	if err := tab.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
		binds = append(binds, v)
		return nil
	}); err != nil {
		return 0, err
	}
	sql := fmt.Sprintf(`UPDATE %s SET %s WHERE %s`,
		sqlitebridge.QuoteIdent(tab.Name),
		strings.Join(sets, ", "),
		strings.Join(wheres, " AND "),
	)
	if err := execBound(b.cfg.AppApply, sql, binds); err != nil {
		return 0, err
	}
	return b.cfg.AppApply.Changes(), nil
}

// blobColumnLen returns COALESCE(length(<col>), 0) for the row keyed by pk.
// Returns 0 with no error when the row is missing — caller reads
// effective_len consistently.
func (b *Broker) blobColumnLen(tab *catalog.Table, col catalog.Column, pk crdt.PKBlob) (uint64, error) {
	wheres := make([]string, len(tab.PK))
	for i, p := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(p.Name))
	}
	sql := fmt.Sprintf(`SELECT COALESCE(length(%s), 0) FROM %s WHERE %s`,
		sqlitebridge.QuoteIdent(col.Name),
		sqlitebridge.QuoteIdent(tab.Name),
		strings.Join(wheres, " AND "),
	)
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Finalize()
	pos := 0
	if err := tab.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
		pos++
		return bindColValue(stmt, pos, v)
	}); err != nil {
		return 0, err
	}
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		return 0, err
	}
	return uint64(stmt.ColumnInt64(0)), nil
}

// shrinkBlobColumn truncates the column to n bytes via substr. SQLite
// substr on a NULL blob returns NULL, but reconciliation only invokes
// shrink when curLen > effectiveLen, which implies a non-NULL column.
func (b *Broker) shrinkBlobColumn(tab *catalog.Table, col catalog.Column, pk crdt.PKBlob, n uint64) error {
	wheres := make([]string, len(tab.PK))
	for i, p := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(p.Name))
	}
	quotedCol := sqlitebridge.QuoteIdent(col.Name)
	sql := fmt.Sprintf(`UPDATE %s SET %s = substr(%s, 1, ?) WHERE %s`,
		sqlitebridge.QuoteIdent(tab.Name),
		quotedCol, quotedCol,
		strings.Join(wheres, " AND "),
	)
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, int64(n)); err != nil {
		return err
	}
	pos := 1
	if err := tab.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
		pos++
		return bindColValue(stmt, pos, v)
	}); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}
