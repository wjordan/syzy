package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// errCounterApply marks deterministic, payload-specific counter apply
// failures — a wire value that violates the counter contract, or int64
// overflow of a summation. Like SQLite constraint failures, these would
// otherwise pin the origin's frontier forever, so applyPayload routes
// them to quarantine. (Overflow can even clear on retry: quarantined
// changesets re-apply after later deltas move the cell back in range.)
var errCounterApply = errors.New("broker: counter apply")

// Counter columns (sqlite/docs/DDL.md#counter-columns, CRDT.md F_counter) merge by
// summation: an Update ships a FormatDelta contribution applied as
// `col = col + ?`, and a same-generation Insert image adds instead of
// overwriting. Summation is not idempotent, so exactly-once delivery is
// load-bearing. The in-memory frontier gives it in steady state; the
// crash window between app.db COMMIT and mirror-journal append — which
// registers cover by idempotent re-apply — is closed by the applied
// marker below: a row in the app-db-internal _syzy_applied table
// written inside the same transaction as the DML, hence exactly as
// durable as the counter effects it certifies. On (re)delivery of a
// marked seq the counter contributions are stripped and the remaining
// idempotent effects re-apply as usual.

const appliedMarkerTable = "_syzy_applied"

// recordCounterContribution reports whether rec carries a
// non-idempotent counter effect: a FormatDelta Update value, or a
// counter-column Insert image (which adds when it lands on a live row
// at the same CL).
func recordCounterContribution(tab *catalog.Table, rec crdt.Record) bool {
	switch r := rec.(type) {
	case crdt.Update:
		for _, v := range r.Changed {
			if v.Format == crdt.FormatDelta {
				return true
			}
		}
	case crdt.Insert:
		if !tab.HasCounters() {
			return false
		}
		for _, v := range r.Image {
			if col, ok := tab.ColumnByID(v.Column); ok && col.Counter() {
				return true
			}
		}
	}
	return false
}

// counterBearing reports whether any record in records carries a
// counter contribution. Tables missing from the catalog are ignored
// (the apply loop errors on them independently).
func (b *Broker) counterBearing(records []crdt.Record) bool {
	for _, rec := range records {
		tab, ok := b.cfg.Catalog.TableByID(rec.Header().Table)
		if !ok {
			continue
		}
		if recordCounterContribution(tab, rec) {
			return true
		}
	}
	return false
}

// stripCounterContribution returns rec with its counter contribution
// removed: FormatDelta Update values dropped, counter columns omitted
// from an Insert image (the partial-image UPSERT then leaves the
// committed value untouched). Used when the applied marker certifies
// the contribution already landed. An Update that becomes empty is
// kept — applyCellUpdate no-ops it.
func (b *Broker) stripCounterContribution(rec crdt.Record) crdt.Record {
	tab, ok := b.cfg.Catalog.TableByID(rec.Header().Table)
	if !ok {
		return rec
	}
	switch r := rec.(type) {
	case crdt.Update:
		kept := make([]crdt.ColValue, 0, len(r.Changed))
		for _, v := range r.Changed {
			if v.Format != crdt.FormatDelta {
				kept = append(kept, v)
			}
		}
		r.Changed = kept
		return r
	case crdt.Insert:
		if !tab.HasCounters() {
			return rec
		}
		kept := make([]crdt.ColValue, 0, len(r.Image))
		for _, v := range r.Image {
			if col, ok := tab.ColumnByID(v.Column); ok && col.Counter() {
				continue
			}
			kept = append(kept, v)
		}
		r.Image = kept
		return r
	}
	return rec
}

// stripCounterContributions maps stripCounterContribution over records.
func (b *Broker) stripCounterContributions(records []crdt.Record) []crdt.Record {
	out := make([]crdt.Record, len(records))
	for i, rec := range records {
		out[i] = b.stripCounterContribution(rec)
	}
	return out
}

// validateCellCounterValues rejects cell-update values that violate the
// counter wire contract before any of them can bypass stamp arbitration
// or run SQL arithmetic: FormatDelta is only valid on a declared counter
// column and only as an 8-byte ColInt, and a counter column only ever
// carries FormatDelta (materialize ships every counter update as a
// delta; Table.AsCellUpdate marks valid same-generation images). A
// violating value means a buggy or hostile peer — fail deterministically
// into quarantine rather than diverge silently.
func validateCellCounterValues(tab *catalog.Table, upd crdt.Update) error {
	for _, v := range upd.Changed {
		col, ok := tab.ColumnByID(v.Column)
		if !ok {
			continue // dropped column; skipped downstream
		}
		if v.Format == crdt.FormatDelta {
			if !col.Counter() || v.TypeTag != crdt.ColInt || len(v.Bytes) != 8 {
				return fmt.Errorf("%w: %s.%s carries FormatDelta but is not an integer counter cell (counter=%v, tag=%d, %d bytes)",
					errCounterApply, tab.Name, col.Name, col.Counter(), v.TypeTag, len(v.Bytes))
			}
		} else if col.Counter() {
			return fmt.Errorf("%w: %s.%s is a counter column but carries an absolute value (format=%d, tag=%d); counter updates ship FormatDelta contributions",
				errCounterApply, tab.Name, col.Name, v.Format, v.TypeTag)
		}
	}
	return nil
}

// validateInsertCounterImage rejects insert images on counter tables
// whose counter columns are not plain 8-byte integers (or that carry a
// wire-level FormatDelta, which materialize never emits on images).
func validateInsertCounterImage(tab *catalog.Table, ins crdt.Insert) error {
	if !tab.HasCounters() {
		return nil
	}
	for _, v := range ins.Image {
		if v.Format == crdt.FormatDelta {
			return fmt.Errorf("%w: %s insert image carries a FormatDelta value; images ship absolute values", errCounterApply, tab.Name)
		}
		if col, ok := tab.ColumnByID(v.Column); ok && col.Counter() {
			if v.TypeTag != crdt.ColInt || len(v.Bytes) != 8 {
				return fmt.Errorf("%w: %s.%s counter image is not an 8-byte integer (tag=%d, %d bytes)",
					errCounterApply, tab.Name, col.Name, v.TypeTag, len(v.Bytes))
			}
		}
	}
	return nil
}

// counterMergeUpdate converts a generation-establishing Insert whose
// physical row already exists into the update shape that merges counter
// columns additively. This is the undrained-local-commit window (a
// local INSERT committed to app.db but its row-clock advance is still
// queued behind the drain) or an adopted pre-existing row: the physical
// content is a same-generation contribution, so an absolute image would
// erase it here while every peer sums both (CRDT.md F_counter).
// Register columns keep the image's absolute values and Base still
// lands at the insert's stamp; ReassertLocal re-asserts any register
// the local commit dominates and strips counters — already summed.
func counterMergeUpdate(tab *catalog.Table, ins crdt.Insert) crdt.Update {
	changed := make([]crdt.ColValue, 0, len(ins.Image))
	for _, v := range ins.Image {
		col, ok := tab.ColumnByID(v.Column)
		if ok && col.PKPos > 0 {
			continue
		}
		if ok && col.Counter() {
			v.Format = crdt.FormatDelta
		}
		changed = append(changed, v)
	}
	return crdt.Update{Table: ins.Table, PK: ins.PK, CL: ins.CL, Changed: changed}
}

// rowExists reports whether a physical row for pk is present, probed
// inside the apply transaction. SELECT 1 (not rowid) so WITHOUT ROWID
// tables work.
func (b *Broker) rowExists(tab *catalog.Table, pk crdt.PKBlob) (bool, error) {
	wheres := make([]string, len(tab.PK))
	for i, p := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(p.Name))
	}
	stmt, _, err := b.cfg.AppApply.Prepare(fmt.Sprintf(`SELECT 1 FROM %s WHERE %s`,
		sqlitebridge.QuoteIdent(tab.Name), strings.Join(wheres, " AND ")))
	if err != nil {
		return false, err
	}
	defer stmt.Finalize()
	pos := 0
	if err := tab.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
		pos++
		return bindColValue(stmt, pos, v)
	}); err != nil {
		return false, err
	}
	return stmt.Step()
}

// resolveCounterDeltas reads the current values of values' FormatDelta
// columns inside the apply transaction and folds each contribution in
// with checked int64 addition, returning the values with those entries
// rewritten as absolute writes. SQLite's + silently promotes to REAL on
// int64 overflow — an order-dependent (non-convergent) result — so the
// arithmetic runs in Go and overflow fails deterministically into
// quarantine. A missing row returns values unchanged: the caller's
// UPDATE then matches 0 rows and the cell path materializes the row
// with the raw contribution as the generation's opening value.
func (b *Broker) resolveCounterDeltas(tab *catalog.Table, pk crdt.PKBlob, values []crdt.ColValue) ([]crdt.ColValue, error) {
	var idx []int
	var names []string
	for i, v := range values {
		if v.Format != crdt.FormatDelta {
			continue
		}
		col, ok := tab.ColumnByID(v.Column)
		if !ok {
			continue
		}
		idx = append(idx, i)
		names = append(names, sqlitebridge.QuoteIdent(col.Name))
	}
	if len(idx) == 0 {
		return values, nil
	}
	wheres := make([]string, len(tab.PK))
	for i, p := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(p.Name))
	}
	stmt, _, err := b.cfg.AppApply.Prepare(fmt.Sprintf(`SELECT %s FROM %s WHERE %s`,
		strings.Join(names, ", "), sqlitebridge.QuoteIdent(tab.Name), strings.Join(wheres, " AND ")))
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	pos := 0
	if err := tab.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
		pos++
		return bindColValue(stmt, pos, v)
	}); err != nil {
		return nil, err
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return values, nil
	}
	out := make([]crdt.ColValue, len(values))
	copy(out, values)
	for j, i := range idx {
		if t := stmt.ColumnType(j); t != sqlitebridge.ColumnInt {
			return nil, fmt.Errorf("%w: %s column %s holds a non-INTEGER value (storage class %d); cannot sum a contribution into it",
				errCounterApply, tab.Name, names[j], t)
		}
		cur := stmt.ColumnInt64(j)
		d := int64(binary.BigEndian.Uint64(values[i].Bytes))
		sum := cur + d
		if (d > 0 && sum < cur) || (d < 0 && sum > cur) {
			return nil, fmt.Errorf("%w: %s column %s overflows int64 (%d %+d)",
				errCounterApply, tab.Name, names[j], cur, d)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(sum))
		out[i] = crdt.ColValue{Column: values[i].Column, TypeTag: crdt.ColInt, Bytes: buf}
	}
	return out, nil
}

// ensureAppliedMarkerTable lazily creates _syzy_applied on the apply
// connection. Created only when a counter-bearing changeset first
// applies, so counter-free databases never grow the table. Local-only
// bookkeeping: absent from the replicated catalog, never journaled.
func (b *Broker) ensureAppliedMarkerTable() error {
	if b.markerTableReady {
		return nil
	}
	if err := b.cfg.AppApply.Exec(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (origin INTEGER NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY (origin, seq)) WITHOUT ROWID`,
		appliedMarkerTable)); err != nil {
		return fmt.Errorf("broker: create %s: %w", appliedMarkerTable, err)
	}
	b.markerTableReady = true
	return nil
}

// appliedMarkerPresent reports whether (origin, seq) was certified by a
// prior committed apply. Cheap: sticky-false until the marker table has
// ever been created on this connection's database.
func (b *Broker) appliedMarkerPresent(origin crdt.Origin, seq crdt.Seq) (bool, error) {
	if !b.markerTableReady {
		exists, err := sqlitebridge.ObjectExists(b.cfg.AppApply, "table", appliedMarkerTable)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
		b.markerTableReady = true
	}
	stmt, _, err := b.cfg.AppApply.Prepare(fmt.Sprintf(
		`SELECT 1 FROM %s WHERE origin = ? AND seq = ?`, appliedMarkerTable))
	if err != nil {
		return false, err
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, int64(origin)); err != nil {
		return false, err
	}
	if err := stmt.BindInt64(2, int64(seq)); err != nil {
		return false, err
	}
	return stmt.Step()
}

// writeAppliedMarker inserts the (origin, seq) marker and prunes
// entries the persisted snapshot frontier already covers. Runs inside
// the apply transaction so the marker commits atomically with the
// counter DML it certifies.
func (b *Broker) writeAppliedMarker(origin crdt.Origin, seq crdt.Seq) error {
	if err := b.ensureAppliedMarkerTable(); err != nil {
		return err
	}
	if err := b.execMarker(
		fmt.Sprintf(`INSERT OR IGNORE INTO %s (origin, seq) VALUES (?, ?)`, appliedMarkerTable),
		origin, seq); err != nil {
		return err
	}
	if bound := b.cfg.Cache.PersistedFrontierBound(origin); bound > 0 {
		return b.execMarker(
			fmt.Sprintf(`DELETE FROM %s WHERE origin = ? AND seq <= ?`, appliedMarkerTable),
			origin, crdt.Seq(bound))
	}
	return nil
}

// execMarker prepares sql, binds (origin, seq), steps, and finalizes.
// The three marker statements (probe SELECT, INSERT, prune DELETE) stay
// one-shot rather than cached like the per-table DML: they run only on
// counter-bearing applies and only against the fixed-shape internal
// _syzy_applied table, so caching would add finalize/reload bookkeeping
// (finalizeCachedStmts) without shape-invalidation ever needing it.
func (b *Broker) execMarker(sql string, origin crdt.Origin, seq crdt.Seq) error {
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, int64(origin)); err != nil {
		return err
	}
	if err := stmt.BindInt64(2, int64(seq)); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}
