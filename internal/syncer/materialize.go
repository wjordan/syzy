package syncer

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/internal/syzylog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// touched accumulates per-(table, rowid) preupdate observations across
// one in-progress transaction. SQLite preupdate may fire multiple times
// for the same row when a transaction touches it more than once
// (e.g., INSERT then UPDATE, or INSERT then DELETE collapsed). We
// derive the net effect from the first and last observations.
//
// firstOld/lastNew alias into the touch-journal buffer; the buffer
// must outlive evidence build (true on the wal_hook hot path).
type touched struct {
	tableID  crdt.TableID
	tab      *catalog.Table
	firstOp  int             // op of the first preupdate fire
	firstOld []crdt.ColValue // first-fire OLD values (UPDATE/DELETE only); nil for INSERT
	lastOp   int             // op of the most recent preupdate fire
	lastNew  []crdt.ColValue // most recent NEW values (INSERT/UPDATE); nil for DELETE
}

// dedupSmallN sizes the inline-array fast path for the dedup pass.
const dedupSmallN = 8

// blobWriteEntry tracks one (table, rowid, col) blob_write deduplicated
// across multiple preupdate fires in one txn. We keep the *earliest*
// OLD bytes — the only valid diff baseline against the post-commit
// NEW (the pre-txn image of the blob column). lastNewVals is unused
// here; the sink reads NEW post-commit via sqlite3_blob_open.
type blobWriteEntry struct {
	tableID crdt.TableID
	tab     *catalog.Table
	rowid   int64
	pkBytes []byte
	col     int32 // declared-column index of the blob being written
	colID   crdt.ColumnID
	oldBlob []byte // earliest OLD bytes for the column
}

// blobIntentEntry tracks one (table, rowid, col) covered by one or more
// SYZY_OP_BLOB_INTENT records in the same txn. Unlike blobWriteEntry,
// no OLD bytes are captured — the wrapper recorded only (offset, length)
// per write. The materializer reads NEW bytes from the post-commit DB
// for the union of recorded ranges.
type blobIntentEntry struct {
	tableID crdt.TableID
	tab     *catalog.Table
	rowid   int64
	col     int32
	colID   crdt.ColumnID
	colName string
	ranges  []intentRange // appended in fire order; coalesced at materialize time
}

type intentRange struct{ off, end uint64 }

// dedupSlot pairs an interned table name with a rowid for the inline
// dedup search. The first preupdate fire to touch a (table, rowid)
// initializes its `t`; subsequent fires update it.
type dedupSlot struct {
	table []byte // aliases into the touch-journal buf
	rowid int64
	t     touched
}

// buildRecordEvidence parses the touch journal and appends per-row
// evidence onto s.evidence. With the C-side touch journal capturing
// both OLD and NEW values for UPDATE (in addition to the existing
// INSERT-NEW and DELETE-OLD captures), no app.db reads are needed at
// any stage for ordinary DML. blob_patch evidence requires reading
// post-commit NEW bytes via s.blobRead (a read-only app conn) when
// configured.
//
// epoch is the record's capture-time schema stamp (schema_seq+1; 0 =
// pre-stamp record). Journal values are positional — captured in the
// table's physical column order with no ColumnIDs — so a record that
// outlived a schema migration in the journal (lagging or wedged drain)
// must be decoded under the layout it was captured against, or a
// dropped column's value silently lands in whichever column inherited
// its position. captureTable resolves that layout; everything below
// zips against it.
//
// On return, s.evidence contains the new entries (zero or more).
// Caller can compare the pre/post length to detect "nothing replicated."
func (s *MetaSink) buildRecordEvidence(touchBuf []byte, epoch uint64) error {
	var err error
	s.journalRecs, err = parseJournal(touchBuf, s.journalRecs[:0])
	if err != nil {
		return err
	}
	var stackArr [dedupSmallN]dedupSlot
	slots := stackArr[:0]

	findSlot := func(table []byte, rowid int64) int {
		for i := range slots {
			if slots[i].rowid == rowid && bytes.Equal(slots[i].table, table) {
				return i
			}
		}
		return -1
	}

	// blobSlots holds per-(table, rowid, col) earliest-OLD entries for
	// blob_write fires. Kept separate from the DML dedupe so the
	// "drop blob_patch when full DML covers the row" rule (BLOB_PATCH.md
	// step 2) is a simple post-pass intersection.
	var blobSlots []blobWriteEntry
	findBlobSlot := func(table crdt.TableID, rowid int64, col int32) int {
		for i := range blobSlots {
			if blobSlots[i].rowid == rowid && blobSlots[i].col == col && blobSlots[i].tableID == table {
				return i
			}
		}
		return -1
	}

	// intentSlots holds per-(table, rowid, col) entries for
	// SYZY_OP_BLOB_INTENT fires (Syzy-owned blob writes). Same coverage
	// rule as blobSlots: dropped if the row is also touched by full DML
	// in the same txn.
	var intentSlots []blobIntentEntry
	findIntentSlot := func(table crdt.TableID, rowid int64, col int32) int {
		for i := range intentSlots {
			if intentSlots[i].rowid == rowid && intentSlots[i].col == col && intentSlots[i].tableID == table {
				return i
			}
		}
		return -1
	}

	mainName := []byte("main")
	for i := range s.journalRecs {
		r := &s.journalRecs[i]
		if !bytes.Equal(r.DBName, mainName) {
			continue
		}
		tab, ok := s.captureTable(r.Table, epoch)
		if !ok {
			continue
		}

		if r.Op == syzyBlobIntent {
			rowid := r.OldRowID
			colName := string(r.BlobColName)
			colIdx := -1
			for i, c := range tab.Columns {
				if c.Name == colName {
					colIdx = i
					break
				}
			}
			if colIdx < 0 {
				return fmt.Errorf("syzyBlobIntent col %q not in table %q", colName, tab.Name)
			}
			rng := intentRange{off: r.BlobOffset, end: r.BlobOffset + uint64(r.BlobLen)}
			if rng.off >= rng.end {
				continue
			}
			idx := findIntentSlot(tab.ID, rowid, int32(colIdx))
			if idx < 0 {
				intentSlots = append(intentSlots, blobIntentEntry{
					tableID: tab.ID, tab: tab,
					rowid:   rowid,
					col:     int32(colIdx),
					colID:   tab.Columns[colIdx].ID,
					colName: colName,
					ranges:  []intentRange{rng},
				})
			} else {
				intentSlots[idx].ranges = append(intentSlots[idx].ranges, rng)
			}
			continue
		}

		if r.Op == syzyBlobWrite {
			// preupdate_blobwrite fires before the in-progress
			// sqlite3_blob_write completes; r.Values are OLD.
			rowid := r.OldRowID
			col := r.BlobCol
			if int(col) < 0 || int(col) >= len(tab.Columns) {
				return fmt.Errorf("syzyBlobWrite col=%d out of range for table %q (ncol=%d)",
					col, tab.Name, len(tab.Columns))
			}
			assignColumnIDs(tab, r.Values)
			pk, err := tab.EncodePKFromSlice(nil, r.Values)
			if err != nil {
				return fmt.Errorf("blob_write pk encode for table %q: %w", tab.Name, err)
			}
			oldBytes := []byte(nil)
			if int(col) < len(r.Values) && r.Values[col].TypeTag == crdt.ColBlob {
				oldBytes = r.Values[col].Bytes
			}
			idx := findBlobSlot(tab.ID, rowid, col)
			if idx < 0 {
				blobSlots = append(blobSlots, blobWriteEntry{
					tableID: tab.ID, tab: tab,
					rowid: rowid, pkBytes: pk,
					col: col, colID: tab.Columns[col].ID,
					oldBlob: oldBytes,
				})
			}
			// Otherwise keep the earliest OLD: do nothing (later fires
			// race against an already-mutated baseline).
			continue
		}

		var rowid int64
		switch r.Op {
		case sqliteInsert:
			rowid = r.NewRowID
		default:
			rowid = r.OldRowID
		}
		var newVals []crdt.ColValue
		switch r.Op {
		case sqliteInsert:
			newVals = r.Values
		case sqliteUpdate:
			newVals = r.NewValues
		}
		var firstOld []crdt.ColValue
		if r.Op != sqliteInsert {
			firstOld = r.Values
		}
		assignColumnIDs(tab, firstOld)
		assignColumnIDs(tab, newVals)

		idx := findSlot(r.Table, rowid)
		if idx < 0 {
			slots = append(slots, dedupSlot{
				table: r.Table,
				rowid: rowid,
				t: touched{
					tableID: tab.ID, tab: tab,
					firstOp: r.Op, firstOld: firstOld,
					lastOp: r.Op, lastNew: newVals,
				},
			})
		} else {
			t := &slots[idx].t
			t.lastOp = r.Op
			if newVals != nil {
				t.lastNew = newVals
			}
			if r.Op == sqliteDelete {
				t.lastNew = nil
			}
		}
	}
	for i := range slots {
		ev, ok, err := evidenceForTouched(&slots[i].t)
		if err != nil {
			return fmt.Errorf("table %q rowid=%d: %w", slots[i].table, slots[i].rowid, err)
		}
		if ok {
			s.evidence = append(s.evidence, ev)
		}
	}

	// blob_patch evidence: drop entries whose row was also touched by
	// full DML in the same txn (the DML record carries the post-write
	// state). For surviving entries, read NEW bytes via s.blobRead and
	// diff into ranges.
	for i := range blobSlots {
		bs := &blobSlots[i]
		if rowFullyCoveredByDML(slots, bs.tab.Name, bs.rowid) {
			continue
		}
		if s.blobRead == nil {
			// No read conn configured — drop silently. The producer
			// will log this once at startup if blob_patch is expected.
			continue
		}
		ev, err := s.evidenceForBlobWrite(bs)
		if err != nil {
			return fmt.Errorf("blob_write table %q rowid=%d col=%d: %w",
				bs.tab.Name, bs.rowid, bs.col, err)
		}
		if ev != nil {
			s.evidence = append(s.evidence, *ev)
		}
	}
	for i := range intentSlots {
		bs := &intentSlots[i]
		if rowFullyCoveredByDML(slots, bs.tab.Name, bs.rowid) {
			continue
		}
		if s.blobRead == nil {
			continue
		}
		ev, err := s.evidenceForBlobIntent(bs)
		if err != nil {
			return fmt.Errorf("blob_intent table %q rowid=%d col=%q: %w",
				bs.tab.Name, bs.rowid, bs.colName, err)
		}
		if ev != nil {
			s.evidence = append(s.evidence, *ev)
		}
	}
	return nil
}

// rowFullyCoveredByDML returns true if the dedupe slots include a DML
// record whose net effect is non-trivial for the same (table, rowid).
// Used to suppress blob_patch entries the DML record's payload already
// carries.
func rowFullyCoveredByDML(slots []dedupSlot, tableName string, rowid int64) bool {
	tn := []byte(tableName)
	for i := range slots {
		if slots[i].rowid == rowid && bytes.Equal(slots[i].table, tn) {
			return true
		}
	}
	return false
}

// evidenceForBlobIntent reads NEW bytes for the row's coalesced intent
// ranges via s.blobRead and emits a BlobPatch evidence. PK is read from
// the row by rowid (the intent record carries no values; the wrapper
// recorded only the byte range). Returns nil when every range is empty
// after clamping to the post-commit blob length.
func (s *MetaSink) evidenceForBlobIntent(e *blobIntentEntry) (*recordEvidence, error) {
	merged := coalesceIntentRanges(e.ranges)
	if len(merged) == 0 {
		return nil, nil
	}
	pk, err := readRowPK(s.blobRead, e.tab, e.rowid)
	if err != nil {
		return nil, fmt.Errorf("read PK: %w", err)
	}
	col := e.tab.Columns[e.col]
	bh, err := s.blobRead.OpenBlob("main", e.tab.Name, col.Name, e.rowid, false)
	if err != nil {
		return nil, fmt.Errorf("open NEW blob: %w", err)
	}
	defer bh.Close()
	blobLen := uint64(bh.Bytes())
	var ranges []crdt.BlobPatchRange
	for _, r := range merged {
		end := r.end
		if end > blobLen {
			end = blobLen
		}
		if r.off >= end {
			continue
		}
		buf := make([]byte, end-r.off)
		if err := bh.Read(buf, int(r.off)); err != nil {
			return nil, fmt.Errorf("read NEW range [%d..%d): %w", r.off, end, err)
		}
		ranges = append(ranges, crdt.BlobPatchRange{Offset: r.off, Bytes: buf})
	}
	if len(ranges) == 0 {
		return nil, nil
	}
	return &recordEvidence{
		op:         evOpBlobPatch,
		tableID:    e.tableID,
		newPK:      pk,
		blobCol:    e.colID,
		blobRanges: ranges,
	}, nil
}

// coalesceIntentRanges sorts and merges overlapping/adjacent ranges so
// the post-commit blob_read pass issues at most one read per disjoint
// region of the column.
func coalesceIntentRanges(in []intentRange) []intentRange {
	if len(in) <= 1 {
		return in
	}
	cp := make([]intentRange, len(in))
	copy(cp, in)
	sort.Slice(cp, func(i, j int) bool { return cp[i].off < cp[j].off })
	out := cp[:1]
	for _, r := range cp[1:] {
		last := &out[len(out)-1]
		if r.off <= last.end {
			if r.end > last.end {
				last.end = r.end
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

// readRowPK queries (pk_cols...) from tab WHERE rowid = ? and encodes
// the result into a canonical PKBlob.
func readRowPK(c *sqlitebridge.Conn, tab *catalog.Table, rowid int64) (crdt.PKBlob, error) {
	if len(tab.PK) == 0 {
		return nil, fmt.Errorf("table %q has no primary key", tab.Name)
	}
	colNames := make([]string, len(tab.PK))
	for i, p := range tab.PK {
		colNames[i] = sqlitebridge.QuoteIdent(p.Name)
	}
	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE rowid = ?`,
		strings.Join(colNames, ", "), sqlitebridge.QuoteIdent(tab.Name))
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, rowid); err != nil {
		return nil, err
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, fmt.Errorf("rowid %d not in table %q", rowid, tab.Name)
	}
	cols := make([]crdt.ColValue, len(tab.Columns))
	for i, c := range tab.Columns {
		cols[i] = crdt.ColValue{TypeTag: crdt.ColNull, Column: c.ID}
	}
	for i, pkCol := range tab.PK {
		idx := -1
		for j, c := range tab.Columns {
			if c.ID == pkCol.ID {
				idx = j
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("PK col %q not in t.Columns", pkCol.Name)
		}
		cols[idx] = stmtColumnValue(stmt, i, tab.Columns[idx].ID)
	}
	return tab.EncodePKFromSlice(nil, cols)
}

func stmtColumnValue(stmt *sqlitebridge.Stmt, col int, id crdt.ColumnID) crdt.ColValue {
	switch stmt.ColumnType(col) {
	case sqlitebridge.ColumnInt:
		b := make([]byte, 8)
		v := uint64(stmt.ColumnInt64(col))
		for i := 7; i >= 0; i-- {
			b[i] = byte(v)
			v >>= 8
		}
		return crdt.ColValue{TypeTag: crdt.ColInt, Bytes: b, Column: id}
	case sqlitebridge.ColumnReal:
		b := make([]byte, 8)
		bits := math.Float64bits(stmt.ColumnFloat64(col))
		for i := 7; i >= 0; i-- {
			b[i] = byte(bits)
			bits >>= 8
		}
		return crdt.ColValue{TypeTag: crdt.ColReal, Bytes: b, Column: id}
	case sqlitebridge.ColumnText:
		return crdt.ColValue{TypeTag: crdt.ColText, Bytes: []byte(stmt.ColumnText(col)), Column: id}
	case sqlitebridge.ColumnBlob:
		return crdt.ColValue{TypeTag: crdt.ColBlob, Bytes: append([]byte(nil), stmt.ColumnBlob(col)...), Column: id}
	default:
		return crdt.ColValue{TypeTag: crdt.ColNull, Column: id}
	}
}

// evidenceForBlobWrite reads NEW bytes for the row's blob column via
// s.blobRead (a read-only app conn) and diffs OLD vs NEW into a
// minimal sequence of contiguous ranges. Returns nil when the diff is
// empty (no-op blob_write — bytes equal).
func (s *MetaSink) evidenceForBlobWrite(e *blobWriteEntry) (*recordEvidence, error) {
	col := e.tab.Columns[e.col]
	bh, err := s.blobRead.OpenBlob("main", e.tab.Name, col.Name, e.rowid, false)
	if err != nil {
		return nil, fmt.Errorf("open NEW blob: %w", err)
	}
	defer bh.Close()
	newLen := bh.Bytes()
	newBytes := make([]byte, newLen)
	if newLen > 0 {
		if err := bh.Read(newBytes, 0); err != nil {
			return nil, fmt.Errorf("read NEW blob: %w", err)
		}
	}
	ranges := diffBlobRanges(e.oldBlob, newBytes)
	if len(ranges) == 0 {
		return nil, nil
	}
	return &recordEvidence{
		op:         evOpBlobPatch,
		tableID:    e.tableID,
		newPK:      crdt.PKBlob(e.pkBytes),
		blobCol:    e.colID,
		blobRanges: ranges,
	}, nil
}

// diffBlobRanges returns the minimal set of contiguous (offset, bytes)
// ranges where new differs from old. If new extends past old, the
// trailing bytes form one trailing range. If new is shorter than old,
// the truncation isn't expressible as a blob_patch — receivers infer
// length from the row update; for now we only patch the in-common
// prefix and leave length divergence to a future full-row update.
func diffBlobRanges(old, newB []byte) []crdt.BlobPatchRange {
	var ranges []crdt.BlobPatchRange
	common := len(old)
	if len(newB) < common {
		common = len(newB)
	}
	i := 0
	for i < common {
		if old[i] == newB[i] {
			i++
			continue
		}
		j := i + 1
		for j < common && old[j] != newB[j] {
			j++
		}
		ranges = append(ranges, crdt.BlobPatchRange{
			Offset: uint64(i),
			Bytes:  append([]byte(nil), newB[i:j]...),
		})
		i = j
	}
	if len(newB) > common {
		ranges = append(ranges, crdt.BlobPatchRange{
			Offset: uint64(common),
			Bytes:  append([]byte(nil), newB[common:]...),
		})
	}
	return ranges
}

// evidenceForTouched derives the net effect of one transaction's
// preupdate observations against a single (table, rowid) row. Returns
// (ev, true, nil) for a replicable record, (zero, false, nil) for a
// no-op (e.g., INSERT-then-DELETE collapse, or UPDATE with no changed
// columns), and (zero, false, err) on error.
//
// Slice ownership: t.lastNew / t.firstOld are slice headers into the
// parser's reusable journalRecs scratch — the very next parseJournal
// call (for the next journal record in this drain batch) overwrites
// those backing arrays. Each returned evidence therefore takes its own
// copy of the slice header for image / changed; the underlying
// ColValue.Bytes still alias into the stable journal mmap and don't
// need copying.
func evidenceForTouched(t *touched) (recordEvidence, bool, error) {
	switch t.firstOp {
	case sqliteInsert:
		if t.lastOp == sqliteDelete {
			return recordEvidence{}, false, nil
		}
		if t.lastNew == nil {
			return recordEvidence{}, false, fmt.Errorf("INSERT first-touch but no NEW values captured")
		}
		pk, err := t.tab.EncodePKFromSlice(nil, t.lastNew)
		if err != nil {
			return recordEvidence{}, false, err
		}
		return recordEvidence{
			op:      evOpInsert,
			tableID: t.tableID,
			newPK:   pk,
			image:   cloneColValues(t.lastNew),
		}, true, nil

	case sqliteDelete:
		if t.firstOld == nil {
			return recordEvidence{}, false, fmt.Errorf("DELETE first-touch but no OLD values captured")
		}
		oldPK, err := t.tab.EncodePKFromSlice(nil, t.firstOld)
		if err != nil {
			return recordEvidence{}, false, err
		}
		if t.lastOp == sqliteDelete {
			return recordEvidence{
				op:      evOpDelete,
				tableID: t.tableID,
				oldPK:   oldPK,
			}, true, nil
		}
		newPK, err := t.tab.EncodePKFromSlice(nil, t.lastNew)
		if err != nil {
			return recordEvidence{}, false, err
		}
		return recordEvidence{
			op:      evOpUpdatePKChange,
			tableID: t.tableID,
			oldPK:   oldPK,
			newPK:   newPK,
			image:   cloneColValues(t.lastNew),
		}, true, nil

	case sqliteUpdate:
		if t.firstOld == nil {
			return recordEvidence{}, false, fmt.Errorf("UPDATE first-touch but no OLD values captured")
		}
		oldPK, err := t.tab.EncodePKFromSlice(nil, t.firstOld)
		if err != nil {
			return recordEvidence{}, false, err
		}
		if t.lastOp == sqliteDelete {
			return recordEvidence{
				op:      evOpDelete,
				tableID: t.tableID,
				oldPK:   oldPK,
			}, true, nil
		}
		if t.lastNew == nil {
			return recordEvidence{}, false, fmt.Errorf("UPDATE last-touch missing NEW values")
		}
		newPK, err := t.tab.EncodePKFromSlice(nil, t.lastNew)
		if err != nil {
			return recordEvidence{}, false, err
		}
		if !bytes.Equal(oldPK, newPK) {
			return recordEvidence{
				op:      evOpUpdatePKChange,
				tableID: t.tableID,
				oldPK:   oldPK,
				newPK:   newPK,
				image:   cloneColValues(t.lastNew),
			}, true, nil
		}
		changed := changedColumnsSlice(t.tab, t.firstOld, t.lastNew)
		if len(changed) == 0 {
			return recordEvidence{}, false, nil
		}
		// The payload unit must match the arbitration unit.
		//
		// Row group (whole-row LWW): ship the full non-PK post-image,
		// not just the diff — receivers skip a losing record wholesale
		// and a winning record must define every column. A partial
		// diff applied over divergent receiver bases breaks SEC.
		//
		// Cell group (per-column LWW): ship the diff — receivers
		// arbitrate each carried column independently, so unchanged
		// columns must NOT be carried (they'd stomp concurrent
		// disjoint-column writes at this record's stamp).
		if !t.tab.CellGroup() {
			changed = fullImageSlice(t.tab, t.lastNew)
		} else if t.tab.HasCounters() {
			// Counter columns ship the signed adjustment NEW − OLD
			// (FormatDelta), not the absolute value — receivers sum it
			// (CRDT.md F_counter), so concurrent increments merge
			// without loss.
			if err := counterDeltasInPlace(t.tab, t.firstOld, t.lastNew, changed); err != nil {
				return recordEvidence{}, false, err
			}
		}
		return recordEvidence{
			op:      evOpUpdate,
			tableID: t.tableID,
			newPK:   newPK,
			changed: changed,
		}, true, nil
	}
	return recordEvidence{}, false, fmt.Errorf("unknown first-touch op %d", t.firstOp)
}

// captureTable resolves the catalog view a journal record's positional
// values were captured under. epoch is the record's stamp: 0 for a
// pre-stamp record (decode under the current layout, the historical
// behavior), else schema_seq+1 (Producer.schemaEpoch — offset so a
// genesis-schema capture at seq 0 is distinguishable from no stamp).
// When history is unreconstructable (legacy tombstones), fall back to
// the current layout and warn once per table.
func (s *MetaSink) captureTable(name []byte, epoch uint64) (*catalog.Table, bool) {
	tab, ok := s.cat.TableBytes(name)
	if !ok {
		return nil, false
	}
	if epoch == 0 {
		return tab, true
	}
	at, ok := s.cat.TableAtSeq(tab, epoch-1)
	if !ok {
		if s.warnedHistory == nil {
			s.warnedHistory = map[string]struct{}{}
		}
		if _, warned := s.warnedHistory[tab.Name]; !warned {
			s.warnedHistory[tab.Name] = struct{}{}
			syzylog.Printf("syncer: table %q: no reconstructable layout at schema_seq %d (legacy drop tombstone); decoding under current layout — verify column values by name",
				tab.Name, epoch-1)
		}
		return tab, true
	}
	return at, true
}

// assignColumnIDs fills the Column field of each entry in vals from
// the catalog table's column positions. parseJournal preserves column
// order from preupdate but doesn't know about ColumnIDs.
func assignColumnIDs(t *catalog.Table, vals []crdt.ColValue) {
	for i := range vals {
		if i >= len(t.Columns) {
			break
		}
		vals[i].Column = t.Columns[i].ID
	}
}

// cloneColValues returns a freshly-allocated copy of the slice header
// for cols. The ColValue.Bytes still alias into the caller's source —
// detaching only the slice of struct values, since that's the part the
// parser scratch reuses across journal records in one drain batch.
func cloneColValues(cols []crdt.ColValue) []crdt.ColValue {
	if len(cols) == 0 {
		return nil
	}
	out := make([]crdt.ColValue, len(cols))
	copy(out, cols)
	return out
}

// fullImageSlice returns every non-PK column's NEW value in t.Columns
// order. Each returned ColValue carries Column = t.Columns[i].ID.
func fullImageSlice(t *catalog.Table, new []crdt.ColValue) []crdt.ColValue {
	out := make([]crdt.ColValue, 0, len(t.Columns))
	for i, c := range t.Columns {
		if c.PKPos > 0 {
			continue
		}
		if i >= len(new) {
			break
		}
		v := new[i]
		v.Column = c.ID
		out = append(out, v)
	}
	return out
}

// changedColumnsSlice diffs old and new (both in t.Columns order) and
// returns only the non-PK columns whose values differ. Each returned
// ColValue carries Column = t.Columns[i].ID.
func changedColumnsSlice(t *catalog.Table, old, new []crdt.ColValue) []crdt.ColValue {
	out := make([]crdt.ColValue, 0, len(t.Columns))
	for i, c := range t.Columns {
		if c.PKPos > 0 {
			continue
		}
		if i >= len(old) || i >= len(new) {
			break
		}
		if crdt.ColValueEqual(old[i], new[i]) {
			continue
		}
		v := new[i]
		v.Column = c.ID
		out = append(out, v)
	}
	return out
}

// counterDeltasInPlace rewrites each counter column's entry in changed
// (values from changedColumnsSlice, aliasing new) into a FormatDelta
// contribution: 8-byte big-endian int64 of NEW − OLD. old and new are in
// t.Columns order. A non-integer value in a counter column is a hard
// error — admission (sqlite/docs/DDL.md#counter-columns) makes it unreachable
// without deliberate type abuse, and a loud stop beats applying an
// arithmetically meaningless delta.
func counterDeltasInPlace(t *catalog.Table, old, new []crdt.ColValue, changed []crdt.ColValue) error {
	for i, c := range t.Columns {
		if !c.Counter() || i >= len(old) || i >= len(new) {
			continue
		}
		ci := -1
		for j := range changed {
			if changed[j].Column == c.ID {
				ci = j
				break
			}
		}
		if ci < 0 {
			continue // column unchanged this transaction
		}
		// Shared with every other engine's counter producer: a
		// non-integer cell or a wrapped difference is a hard stop, since
		// a wrong contribution would apply on every node.
		delta, err := crdt.CounterDelta(old[i], new[i])
		if err != nil {
			return fmt.Errorf("syncer: counter column %q.%q: %w", t.Name, c.Name, err)
		}
		delta.Column = c.ID
		changed[ci] = delta
	}
	return nil
}

const (
	sqliteInsert   = 18
	sqliteUpdate   = 23
	sqliteDelete   = 9
	syzyBlobWrite  = 5
	syzyBlobIntent = 6
)

