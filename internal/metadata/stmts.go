package metadata

import (
	"fmt"

	"github.com/wjordan/syzy/sqlitebridge"
)

// stmts holds prepared *Stmt for every hot-path accessor on a Store's
// Conn. Statements are prepared once at Open (after the schema has been
// applied) and finalized once at Close. Callers reset+bind+step instead
// of paying Prepare/Finalize per call — sqlite3_prepare_v3 is by far the
// hottest cgo crossing in the inner loop, so caching here is the single
// largest perf win.
//
// All access is serialized through Store.mu (Tx inherits the lock from
// its enclosing WithTx), so no per-stmt locking is required.
type stmts struct {
	putRowClock     *sqlitebridge.Stmt
	advanceFrontier *sqlitebridge.Stmt
	getRowClock     *sqlitebridge.Stmt
	getMeta         *sqlitebridge.Stmt
	setMeta         *sqlitebridge.Stmt
	deleteMeta      *sqlitebridge.Stmt
	listIntents     *sqlitebridge.Stmt
	clearAllIntents *sqlitebridge.Stmt
	frontierFor     *sqlitebridge.Stmt
	frontierAll     *sqlitebridge.Stmt
	deleteFrontier  *sqlitebridge.Stmt
	getSenderSeqs   *sqlitebridge.Stmt
	putSenderSeq    *sqlitebridge.Stmt
	begin           *sqlitebridge.Stmt
	commit          *sqlitebridge.Stmt
	rollback        *sqlitebridge.Stmt
	// DDL catalog statements. Lazy-prepared on first use because most
	// connections never touch them on the hot path.
	upsertTable            *sqlitebridge.Stmt
	setClockGroup          *sqlitebridge.Stmt
	upsertColumn           *sqlitebridge.Stmt
	renameColumn           *sqlitebridge.Stmt
	renameTable            *sqlitebridge.Stmt
	dropColumn             *sqlitebridge.Stmt
	upsertKey              *sqlitebridge.Stmt
	appendSchemaEvent      *sqlitebridge.Stmt
	insertSynthTrigger     *sqlitebridge.Stmt
	deleteSynthTrigger     *sqlitebridge.Stmt
	getBlobRangeClock      *sqlitebridge.Stmt
	putBlobRangeClock      *sqlitebridge.Stmt
	deleteBlobRangeClock   *sqlitebridge.Stmt
	hasAnyBlobRangeClock   *sqlitebridge.Stmt
	getCellClocksForRow    *sqlitebridge.Stmt
	putCellClock           *sqlitebridge.Stmt
	deleteCellClock        *sqlitebridge.Stmt
	deleteCellClocksForRow *sqlitebridge.Stmt
}

// Cached SQL. Each is parameterized so the same prepared statement
// serves every call.

const putRowClockSQL = `
INSERT INTO row_clock (table_id, pk_blob, cl, base_hlc, base_origin)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(table_id, pk_blob) DO UPDATE SET
	cl = excluded.cl,
	base_hlc = excluded.base_hlc,
	base_origin = excluded.base_origin`

const advanceFrontierSQL = `
INSERT INTO frontier (origin, last_seq, last_hlc) VALUES (?, ?, ?)
ON CONFLICT(origin) DO UPDATE SET
	last_seq = excluded.last_seq,
	last_hlc = excluded.last_hlc`

const getRowClockSQL = `
SELECT cl, base_hlc, base_origin FROM row_clock
WHERE table_id = ? AND pk_blob = ?`

const getMetaSQL = `SELECT value FROM meta WHERE key = ?`

const setMetaSQL = `
INSERT INTO meta (key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`

const deleteMetaSQL = `DELETE FROM meta WHERE key = ?`

// Origin-scoped intent slots live under "intent:<origin-hex>" in meta.
// The half-open key range ['intent:', 'intent;') covers exactly that
// prefix (';' is the codepoint after ':') and stays index-friendly.
const listIntentsSQL = `
SELECT key, value FROM meta WHERE key >= 'intent:' AND key < 'intent;' ORDER BY key`

const clearAllIntentsSQL = `
DELETE FROM meta WHERE key >= 'intent:' AND key < 'intent;'`

const frontierForSQL = `SELECT last_seq, last_hlc FROM frontier WHERE origin = ?`

const frontierAllSQL = `SELECT origin, last_seq, last_hlc FROM frontier`

const deleteFrontierSQL = `DELETE FROM frontier WHERE origin = ?`

const getSenderSeqsSQL = `SELECT origin, next_seq FROM sender_seq`

const putSenderSeqSQL = `
INSERT INTO sender_seq (origin, next_seq) VALUES (?, ?)
ON CONFLICT(origin) DO UPDATE SET next_seq = excluded.next_seq`

const beginSQL = `BEGIN IMMEDIATE`
const commitSQL = `COMMIT`
const rollbackSQL = `ROLLBACK`

const getBlobRangeClockSQL = `SELECT intervals FROM blob_range_clock
WHERE table_id = ? AND pk_blob = ?`

const putBlobRangeClockSQL = `
INSERT INTO blob_range_clock (table_id, pk_blob, intervals) VALUES (?, ?, ?)
ON CONFLICT(table_id, pk_blob) DO UPDATE SET intervals = excluded.intervals`

const deleteBlobRangeClockSQL = `DELETE FROM blob_range_clock
WHERE table_id = ? AND pk_blob = ?`

const hasAnyBlobRangeClockSQL = `SELECT 1 FROM blob_range_clock
WHERE table_id = ? LIMIT 1`

const getCellClocksForRowSQL = `SELECT column_id, hlc, hlc_origin FROM cell_clock
WHERE table_id = ? AND pk_blob = ?`

const putCellClockSQL = `
INSERT INTO cell_clock (table_id, pk_blob, column_id, hlc, hlc_origin)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(table_id, pk_blob, column_id) DO UPDATE SET
	hlc = excluded.hlc,
	hlc_origin = excluded.hlc_origin`

const deleteCellClockSQL = `DELETE FROM cell_clock
WHERE table_id = ? AND pk_blob = ? AND column_id = ?`

const deleteCellClocksForRowSQL = `DELETE FROM cell_clock
WHERE table_id = ? AND pk_blob = ?`

// prepareStmts compiles every cached statement. Must be called after the
// schema has been applied (sqlite3_prepare_v3 resolves table names eagerly).
func (s *Store) prepareStmts() error {
	st := &stmts{}
	for _, p := range []struct {
		sql string
		dst **sqlitebridge.Stmt
	}{
		{putRowClockSQL, &st.putRowClock},
		{advanceFrontierSQL, &st.advanceFrontier},
		{getRowClockSQL, &st.getRowClock},
		{getMetaSQL, &st.getMeta},
		{setMetaSQL, &st.setMeta},
		{deleteMetaSQL, &st.deleteMeta},
		{listIntentsSQL, &st.listIntents},
		{clearAllIntentsSQL, &st.clearAllIntents},
		{frontierForSQL, &st.frontierFor},
		{frontierAllSQL, &st.frontierAll},
		{deleteFrontierSQL, &st.deleteFrontier},
		{getSenderSeqsSQL, &st.getSenderSeqs},
		{putSenderSeqSQL, &st.putSenderSeq},
		{beginSQL, &st.begin},
		{commitSQL, &st.commit},
		{rollbackSQL, &st.rollback},
		{getBlobRangeClockSQL, &st.getBlobRangeClock},
		{putBlobRangeClockSQL, &st.putBlobRangeClock},
		{deleteBlobRangeClockSQL, &st.deleteBlobRangeClock},
		{hasAnyBlobRangeClockSQL, &st.hasAnyBlobRangeClock},
		{getCellClocksForRowSQL, &st.getCellClocksForRow},
		{putCellClockSQL, &st.putCellClock},
		{deleteCellClockSQL, &st.deleteCellClock},
		{deleteCellClocksForRowSQL, &st.deleteCellClocksForRow},
	} {
		stmt, _, err := s.conn.Prepare(p.sql)
		if err != nil {
			st.finalize()
			return fmt.Errorf("metadata: prepare statement: %w", err)
		}
		*p.dst = stmt
	}
	s.stmts = st
	return nil
}

// finalize releases every cached statement. Safe to call on a nil receiver.
func (st *stmts) finalize() {
	if st == nil {
		return
	}
	for _, s := range []**sqlitebridge.Stmt{
		&st.putRowClock,
		&st.advanceFrontier,
		&st.getRowClock,
		&st.getMeta,
		&st.setMeta,
		&st.deleteMeta,
		&st.listIntents,
		&st.clearAllIntents,
		&st.frontierFor,
		&st.frontierAll,
		&st.deleteFrontier,
		&st.getSenderSeqs,
		&st.putSenderSeq,
		&st.begin,
		&st.commit,
		&st.rollback,
		&st.upsertTable,
		&st.setClockGroup,
		&st.upsertColumn,
		&st.renameColumn,
		&st.renameTable,
		&st.upsertKey,
		&st.appendSchemaEvent,
		&st.insertSynthTrigger,
		&st.deleteSynthTrigger,
		&st.getBlobRangeClock,
		&st.putBlobRangeClock,
		&st.deleteBlobRangeClock,
		&st.hasAnyBlobRangeClock,
		&st.getCellClocksForRow,
		&st.putCellClock,
		&st.deleteCellClock,
		&st.deleteCellClocksForRow,
	} {
		if *s != nil {
			_ = (*s).Finalize()
			*s = nil
		}
	}
}
