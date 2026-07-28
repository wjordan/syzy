package metadata

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// PKKeyID is the reserved syzy_key.key_id value identifying the primary
// key tuple. All-zero by spec (see sqlite/docs/DDL.md#metadata-catalog).
var PKKeyID = crdt.KeyID{}

// State strings stored in syzy_table.state, syzy_column.state, syzy_key.state.
const (
	StateActive  = "active"
	StateDropped = "dropped"
)

// ClockGroup strings stored in syzy_table.default_clock_group and
// syzy_column.clock_group. Tables carry 'row' or 'cell'
// (sqlite/docs/DDL.md#sparse-clock-groups); 'counter' is column-only — a declared
// counter column whose concurrent writes merge by summation
// (sqlite/docs/DDL.md#counter-columns, CRDT.md F_counter).
const (
	ClockGroupRow     = "row"
	ClockGroupCell    = "cell"
	ClockGroupCounter = "counter"
)

// CounterType reports whether a declared column type carries the
// COUNTER token (e.g. "INTEGER COUNTER"), the DDL surface that makes a
// column's clock_group 'counter' from birth (sqlite/docs/DDL.md#counter-columns).
func CounterType(declType string) bool {
	for _, tok := range strings.Fields(declType) {
		if strings.EqualFold(tok, "COUNTER") {
			return true
		}
	}
	return false
}

// IntAffinityType reports whether a declared type has SQLite INTEGER
// affinity (contains "INT", per SQLite's affinity rule 1). Counter
// columns require it.
func IntAffinityType(declType string) bool {
	return strings.Contains(strings.ToUpper(declType), "INT")
}

// ApplyState strings stored in syzy_schema_event.apply_state.
//
// ApplyStateApplied is the steady-state value: the SQLite-side DDL and
// the metadata-catalog rows both reflect the event.
//
// ApplyStateFailedLocal is a recovery marker, not a normal runtime state.
// The receiver-side catchup loop never writes it: under the current
// invariant, a failed SQLite apply leaves schema_seq un-advanced and no
// schema_event row at all, so the next tick retries cleanly. The marker
// remains for two transitional/edge cases:
//
//   - Rows persisted by older broker binaries that advanced schema_seq
//     even when applyCatalogStructural failed. Broker startup runs
//     drainFailedLocalSchemaEvents to reconcile these.
//   - The originator's resolveLocalDDL: when metadata-side UPSERTs
//     diverge from already-committed SQLite DDL (disk full / corruption),
//     the row is written so a later drain can heal the divergence.
const (
	ApplyStateApplied     = "applied"
	ApplyStateFailedLocal = "failed_local"
)

// TableEntry is one row of syzy_table.
type TableEntry struct {
	ID                crdt.TableID
	Name              string
	State             string
	DefaultClockGroup string
	CreateSeq         uint64
	DropSeq           uint64 // 0 ⇒ NULL on disk
}

// ColumnEntry is one row of syzy_column.
type ColumnEntry struct {
	TableID    crdt.TableID
	ColumnID   crdt.ColumnID
	Name       string
	Ordinal    int
	State      string
	ClockGroup string
	// Collation is the column's text collating sequence (crdt.CollBinary
	// default). Carried in the catalog model so admission can compile a
	// partial predicate and validate unique-key members without re-reading
	// the schema.
	Collation crdt.Collation
	CreateSeq uint64
	DropSeq   uint64 // 0 ⇒ NULL on disk
}

// KeyEntry is one row of syzy_key. PK columns live at KeyID = PKKeyID.
type KeyEntry struct {
	TableID     crdt.TableID
	KeyID       crdt.KeyID
	ColumnID    crdt.ColumnID
	Ordinal     int
	State       string
	Coordinated bool // CP unique key (NOT NULL UNIQUE); always false for the PK
	// Predicate is the compiled partial-index WHERE clause
	// (crdt.EncodeUniquePredicate bytes); nil/empty for a total key.
	// Stored on every member row of the key, identical across ordinals.
	Predicate []byte
	CreateSeq uint64
	DropSeq   uint64 // 0 ⇒ NULL on disk
}

// SchemaEventEntry is one row of syzy_schema_event.
type SchemaEventEntry struct {
	SchemaSeq   uint64
	ParentSeq   uint64
	CatalogOp   []byte
	RawSQL      string
	AppliedAtUs int64
	ApplyState  string
}

const (
	upsertTableSQL = `
INSERT INTO syzy_table (table_id, name, state, default_clock_group, create_seq, drop_seq)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(table_id) DO UPDATE SET
  name = excluded.name,
  state = excluded.state,
  default_clock_group = excluded.default_clock_group,
  create_seq = excluded.create_seq,
  drop_seq = excluded.drop_seq`

	upsertColumnSQL = `
INSERT INTO syzy_column (table_id, column_id, name, ordinal, state, clock_group, collation, create_seq, drop_seq)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(table_id, column_id) DO UPDATE SET
  name = excluded.name,
  ordinal = excluded.ordinal,
  state = excluded.state,
  clock_group = excluded.clock_group,
  collation = excluded.collation,
  create_seq = excluded.create_seq,
  drop_seq = excluded.drop_seq`

	// renameColumnSQL changes only the name; see RenameColumn for why a rename
	// must not touch ordinal/clock_group/state/create_seq.
	renameColumnSQL = `UPDATE syzy_column SET name = ? WHERE table_id = ? AND column_id = ?`

	// renameTableSQL changes only the name — a rename must not touch
	// default_clock_group/state/create_seq (an UpsertTable here would
	// silently reset a cell-group table to row-group).
	renameTableSQL = `UPDATE syzy_table SET name = ? WHERE table_id = ?`

	// dropColumnSQL tombstones in place: name/ordinal/create_seq survive
	// so historical layouts stay reconstructable (see DropColumn).
	dropColumnSQL = `UPDATE syzy_column SET state = ?, drop_seq = ? WHERE table_id = ? AND column_id = ?`

	upsertKeySQL = `
INSERT INTO syzy_key (table_id, key_id, column_id, ordinal, state, coordinated, predicate, create_seq, drop_seq)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(table_id, key_id, ordinal) DO UPDATE SET
  column_id = excluded.column_id,
  state = excluded.state,
  coordinated = excluded.coordinated,
  predicate = excluded.predicate,
  create_seq = excluded.create_seq,
  drop_seq = excluded.drop_seq`

	// INSERT OR IGNORE: schema_seq is the PK, and the producer's
	// wal_hook + the broker's catch-up loop can race to insert the
	// same row. The earlier writer wins, the later one no-ops. Both
	// callsites pre-decode the same authoritative bytes, so the row
	// they would have written is identical.
	appendSchemaEventSQL = `
INSERT OR IGNORE INTO syzy_schema_event (schema_seq, parent_seq, catalog_op, raw_sql, applied_at_us, apply_state)
VALUES (?, ?, ?, ?, ?, ?)`

	listTablesSQL  = `SELECT table_id, name, state, default_clock_group, create_seq, drop_seq FROM syzy_table`
	listColumnsSQL = `SELECT table_id, column_id, name, ordinal, state, clock_group, collation, create_seq, drop_seq FROM syzy_column`
	listKeysSQL    = `SELECT table_id, key_id, column_id, ordinal, state, coordinated, predicate, create_seq, drop_seq FROM syzy_key`

	insertSynthTriggerSQL = `INSERT OR IGNORE INTO syzy_synth_trigger (child_table_id, trigger_name, parent_table) VALUES (?, ?, ?)`
	deleteSynthTriggerSQL = `DELETE FROM syzy_synth_trigger WHERE child_table_id = ?`
	listSynthTriggerSQL   = `SELECT trigger_name, parent_table FROM syzy_synth_trigger WHERE child_table_id = ? ORDER BY trigger_name`

	readFailedLocalEventsSQL = `
SELECT schema_seq, parent_seq, catalog_op, raw_sql, applied_at_us
FROM syzy_schema_event WHERE apply_state = 'failed_local' ORDER BY schema_seq`

	readAppliedEventsSQL = `
SELECT schema_seq, parent_seq, catalog_op, raw_sql, applied_at_us
FROM syzy_schema_event WHERE apply_state = 'applied' ORDER BY schema_seq`

	markSchemaEventAppliedSQL = `
UPDATE syzy_schema_event SET apply_state = 'applied' WHERE schema_seq = ?`
)

// SynthTriggerEntry is one row of syzy_synth_trigger.
type SynthTriggerEntry struct {
	ChildTableID crdt.TableID
	TriggerName  string
	ParentTable  string
}

// InsertSynthTrigger records a synthesized cascade trigger that
// belongs to the given child table. Idempotent — repeated insertions
// of the same (child_table_id, trigger_name) no-op.
func (tx *Tx) InsertSynthTrigger(e SynthTriggerEntry) error {
	stmt, err := tx.cachedStmt(&tx.stmts.insertSynthTrigger, insertSynthTriggerSQL)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindBlob(1, bind16(e.ChildTableID[:])); err != nil {
		return err
	}
	if err := stmt.BindText(2, e.TriggerName); err != nil {
		return err
	}
	if err := stmt.BindText(3, e.ParentTable); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

// DeleteSynthTriggersForTable removes every syzy_synth_trigger row
// owned by the given child_table_id. Used by the producer when a
// DROP TABLE on the child has been resolved so the bookkeeping doesn't
// outlive the table.
func (tx *Tx) DeleteSynthTriggersForTable(tid crdt.TableID) error {
	stmt, err := tx.cachedStmt(&tx.stmts.deleteSynthTrigger, deleteSynthTriggerSQL)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindBlob(1, bind16(tid[:])); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

// ListSynthTriggersForTable returns the synthesized triggers owned by
// child_table_id, ordered by trigger name. Used by the producer's
// DROP TABLE path to enumerate matching DROP TRIGGER ops.
func (s *Store) ListSynthTriggersForTable(tid crdt.TableID) ([]SynthTriggerEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt, _, err := s.conn.Prepare(listSynthTriggerSQL)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, bind16(tid[:])); err != nil {
		return nil, err
	}
	var out []SynthTriggerEntry
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		out = append(out, SynthTriggerEntry{
			ChildTableID: tid,
			TriggerName:  stmt.ColumnText(0),
			ParentTable:  stmt.ColumnText(1),
		})
	}
	return out, nil
}

// UpsertTable writes a syzy_table row inside an open WithTx.
func (tx *Tx) UpsertTable(e TableEntry) error {
	stmt, err := tx.cachedStmt(&tx.stmts.upsertTable, upsertTableSQL)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	id := bind16(e.ID[:])
	if err := stmt.BindBlob(1, id); err != nil {
		return err
	}
	if err := stmt.BindText(2, e.Name); err != nil {
		return err
	}
	if err := stmt.BindText(3, e.State); err != nil {
		return err
	}
	if err := stmt.BindText(4, e.DefaultClockGroup); err != nil {
		return err
	}
	if err := stmt.BindInt64(5, int64(e.CreateSeq)); err != nil {
		return err
	}
	if e.DropSeq == 0 {
		if err := stmt.BindNull(6); err != nil {
			return err
		}
	} else {
		if err := stmt.BindInt64(6, int64(e.DropSeq)); err != nil {
			return err
		}
	}
	_, err = stmt.Step()
	return err
}

// SetDefaultClockGroup updates one table's default_clock_group inside
// an open WithTx. Used by the OpSetClockGroup schema-event apply.
func (tx *Tx) SetDefaultClockGroup(tid crdt.TableID, group string) error {
	stmt, err := tx.cachedStmt(&tx.stmts.setClockGroup,
		`UPDATE syzy_table SET default_clock_group = ? WHERE table_id = ?`)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindText(1, group); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, bind16(tid[:])); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

// UpsertColumn writes a syzy_column row inside an open WithTx.
func (tx *Tx) UpsertColumn(e ColumnEntry) error {
	stmt, err := tx.cachedStmt(&tx.stmts.upsertColumn, upsertColumnSQL)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	tid := bind16(e.TableID[:])
	cid := bind16(e.ColumnID[:])
	if err := stmt.BindBlob(1, tid); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, cid); err != nil {
		return err
	}
	if err := stmt.BindText(3, e.Name); err != nil {
		return err
	}
	if err := stmt.BindInt64(4, int64(e.Ordinal)); err != nil {
		return err
	}
	if err := stmt.BindText(5, e.State); err != nil {
		return err
	}
	if err := stmt.BindText(6, e.ClockGroup); err != nil {
		return err
	}
	if err := stmt.BindInt64(7, int64(e.Collation)); err != nil {
		return err
	}
	if err := stmt.BindInt64(8, int64(e.CreateSeq)); err != nil {
		return err
	}
	if e.DropSeq == 0 {
		if err := stmt.BindNull(9); err != nil {
			return err
		}
	} else {
		if err := stmt.BindInt64(9, int64(e.DropSeq)); err != nil {
			return err
		}
	}
	_, err = stmt.Step()
	return err
}

// RenameColumn updates only a column's name, preserving its ordinal, clock
// group, state, and create_seq. ApplyCatalogOp uses this for OpRenameColumn so
// a rename does not reset the ordinal (which would corrupt column order and
// break attnum-stable rebinding on restart).
func (tx *Tx) RenameColumn(tid crdt.TableID, cid crdt.ColumnID, name string) error {
	stmt, err := tx.cachedStmt(&tx.stmts.renameColumn, renameColumnSQL)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindText(1, name); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, bind16(tid[:])); err != nil {
		return err
	}
	if err := stmt.BindBlob(3, bind16(cid[:])); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

// RenameTable updates only a table's name, preserving its
// default_clock_group, state, and create_seq. ApplyCatalogOp uses this
// for OpRenameTable so a rename does not silently reset a cell-group
// table to row-group.
func (tx *Tx) RenameTable(tid crdt.TableID, name string) error {
	stmt, err := tx.cachedStmt(&tx.stmts.renameTable, renameTableSQL)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindText(1, name); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, bind16(tid[:])); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

// DropColumn tombstones a syzy_column row in place, preserving its
// name, ordinal, and create_seq — the fields historical layout
// reconstruction (catalog.TableAtSeq) and structural reconciliation
// need after the column is gone. Returns whether a row was updated;
// callers fall back to an upsert when the row never existed locally.
func (tx *Tx) DropColumn(tid crdt.TableID, cid crdt.ColumnID, dropSeq uint64) (bool, error) {
	stmt, err := tx.cachedStmt(&tx.stmts.dropColumn, dropColumnSQL)
	if err != nil {
		return false, err
	}
	if err := stmt.Reset(); err != nil {
		return false, err
	}
	if err := stmt.BindText(1, StateDropped); err != nil {
		return false, err
	}
	if err := stmt.BindInt64(2, int64(dropSeq)); err != nil {
		return false, err
	}
	if err := stmt.BindBlob(3, bind16(tid[:])); err != nil {
		return false, err
	}
	if err := stmt.BindBlob(4, bind16(cid[:])); err != nil {
		return false, err
	}
	if _, err := stmt.Step(); err != nil {
		return false, err
	}
	return tx.conn.Changes() > 0, nil
}

// UpsertKey writes a syzy_key row inside an open WithTx.
func (tx *Tx) UpsertKey(e KeyEntry) error {
	stmt, err := tx.cachedStmt(&tx.stmts.upsertKey, upsertKeySQL)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	tid := bind16(e.TableID[:])
	kid := bind16(e.KeyID[:])
	cid := bind16(e.ColumnID[:])
	if err := stmt.BindBlob(1, tid); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, kid); err != nil {
		return err
	}
	if err := stmt.BindBlob(3, cid); err != nil {
		return err
	}
	if err := stmt.BindInt64(4, int64(e.Ordinal)); err != nil {
		return err
	}
	if err := stmt.BindText(5, e.State); err != nil {
		return err
	}
	if err := stmt.BindInt64(6, boolToInt64(e.Coordinated)); err != nil {
		return err
	}
	if len(e.Predicate) == 0 {
		if err := stmt.BindNull(7); err != nil {
			return err
		}
	} else {
		if err := stmt.BindBlob(7, e.Predicate); err != nil {
			return err
		}
	}
	if err := stmt.BindInt64(8, int64(e.CreateSeq)); err != nil {
		return err
	}
	if e.DropSeq == 0 {
		if err := stmt.BindNull(9); err != nil {
			return err
		}
	} else {
		if err := stmt.BindInt64(9, int64(e.DropSeq)); err != nil {
			return err
		}
	}
	_, err = stmt.Step()
	return err
}

// boolToInt64 maps a Go bool to the 0/1 integer stored in STRICT tables.
func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// AppendSchemaEvent inserts a new syzy_schema_event row inside an open
// WithTx. The schema_seq must not already exist; the catalog is
// append-only.
func (tx *Tx) AppendSchemaEvent(e SchemaEventEntry) error {
	stmt, err := tx.cachedStmt(&tx.stmts.appendSchemaEvent, appendSchemaEventSQL)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindInt64(1, int64(e.SchemaSeq)); err != nil {
		return err
	}
	if err := stmt.BindInt64(2, int64(e.ParentSeq)); err != nil {
		return err
	}
	if err := stmt.BindBlob(3, e.CatalogOp); err != nil {
		return err
	}
	if e.RawSQL == "" {
		if err := stmt.BindNull(4); err != nil {
			return err
		}
	} else {
		if err := stmt.BindText(4, e.RawSQL); err != nil {
			return err
		}
	}
	if err := stmt.BindInt64(5, e.AppliedAtUs); err != nil {
		return err
	}
	if err := stmt.BindText(6, e.ApplyState); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

// ReadFailedLocalSchemaEvents returns every syzy_schema_event row whose
// apply_state is 'failed_local', in schema_seq order. Used once at broker
// startup by the drain pass that re-attempts these events.
//
// failed_local rows can originate from two places: (a) old broker binaries
// that advanced schema_seq even when the receiver-side SQLite DDL apply
// failed (the bug this drain repairs); (b) the originator path's rare
// "metadata UPSERTs diverged from already-committed SQLite DDL" case in
// producer/ddl_resolve. Both shapes are self-healing once applyCatalog-
// Structural is re-run idempotently against the current SQLite state.
func (s *Store) ReadFailedLocalSchemaEvents() ([]SchemaEventEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt, _, err := s.conn.Prepare(readFailedLocalEventsSQL)
	if err != nil {
		return nil, fmt.Errorf("metadata: prepare read_failed_local: %w", err)
	}
	defer stmt.Finalize()
	var out []SchemaEventEntry
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		e := SchemaEventEntry{
			SchemaSeq:   uint64(stmt.ColumnInt64(0)),
			ParentSeq:   uint64(stmt.ColumnInt64(1)),
			CatalogOp:   append([]byte(nil), stmt.ColumnBlob(2)...),
			AppliedAtUs: stmt.ColumnInt64(4),
			ApplyState:  ApplyStateFailedLocal,
		}
		if !stmt.ColumnIsNull(3) {
			e.RawSQL = stmt.ColumnText(3)
		}
		out = append(out, e)
	}
	return out, nil
}

// ReadAppliedSchemaEvents returns every syzy_schema_event row whose
// apply_state is 'applied', in schema_seq order. Used by the broker's
// on-open reconciliation to re-check that each applied DDL's structural
// effect is actually present in app.db: the metadata catalog can record
// a DDL as applied while app.db lacks it (a two-stream restore that
// advanced the metadata stream past the data stream across the DDL).
func (s *Store) ReadAppliedSchemaEvents() ([]SchemaEventEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt, _, err := s.conn.Prepare(readAppliedEventsSQL)
	if err != nil {
		return nil, fmt.Errorf("metadata: prepare read_applied: %w", err)
	}
	defer stmt.Finalize()
	var out []SchemaEventEntry
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		e := SchemaEventEntry{
			SchemaSeq:   uint64(stmt.ColumnInt64(0)),
			ParentSeq:   uint64(stmt.ColumnInt64(1)),
			CatalogOp:   append([]byte(nil), stmt.ColumnBlob(2)...),
			AppliedAtUs: stmt.ColumnInt64(4),
			ApplyState:  ApplyStateApplied,
		}
		if !stmt.ColumnIsNull(3) {
			e.RawSQL = stmt.ColumnText(3)
		}
		out = append(out, e)
	}
	return out, nil
}

// MarkSchemaEventApplied flips a failed_local row to 'applied' once the
// drain pass has successfully reconciled it. Tx-scoped so the flip can
// share a transaction with the metadata catalog upserts the reapply
// performs.
func (tx *Tx) MarkSchemaEventApplied(schemaSeq uint64) error {
	stmt, _, err := tx.conn.Prepare(markSchemaEventAppliedSQL)
	if err != nil {
		return fmt.Errorf("metadata: prepare mark_applied: %w", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, int64(schemaSeq)); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

// CatalogSnapshot is the union of all syzy_table/column/key rows. Loaded
// at producer startup to seed the in-memory catalog. Tables, Columns,
// and Keys are returned in primary-key order so callers don't re-sort.
type CatalogSnapshot struct {
	Tables  []TableEntry
	Columns []ColumnEntry
	Keys    []KeyEntry
}

// LoadCatalogSnapshot reads the entire DDL catalog into memory. Used by
// internal/catalog at producer startup to build the in-memory ID
// mapping.
func (s *Store) LoadCatalogSnapshot() (*CatalogSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := &CatalogSnapshot{}

	tablesStmt, _, err := s.conn.Prepare(listTablesSQL)
	if err != nil {
		return nil, fmt.Errorf("metadata: prepare list_tables: %w", err)
	}
	defer tablesStmt.Finalize()
	for {
		hasRow, err := tablesStmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		var id crdt.TableID
		if err := readBlobInto(tablesStmt, 0, id[:]); err != nil {
			return nil, err
		}
		entry := TableEntry{
			ID:                id,
			Name:              tablesStmt.ColumnText(1),
			State:             tablesStmt.ColumnText(2),
			DefaultClockGroup: tablesStmt.ColumnText(3),
			CreateSeq:         uint64(tablesStmt.ColumnInt64(4)),
		}
		if !tablesStmt.ColumnIsNull(5) {
			entry.DropSeq = uint64(tablesStmt.ColumnInt64(5))
		}
		snap.Tables = append(snap.Tables, entry)
	}

	colsStmt, _, err := s.conn.Prepare(listColumnsSQL)
	if err != nil {
		return nil, fmt.Errorf("metadata: prepare list_columns: %w", err)
	}
	defer colsStmt.Finalize()
	for {
		hasRow, err := colsStmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		var tid crdt.TableID
		var cid crdt.ColumnID
		if err := readBlobInto(colsStmt, 0, tid[:]); err != nil {
			return nil, err
		}
		if err := readBlobInto(colsStmt, 1, cid[:]); err != nil {
			return nil, err
		}
		entry := ColumnEntry{
			TableID:    tid,
			ColumnID:   cid,
			Name:       colsStmt.ColumnText(2),
			Ordinal:    int(colsStmt.ColumnInt64(3)),
			State:      colsStmt.ColumnText(4),
			ClockGroup: colsStmt.ColumnText(5),
			Collation:  crdt.Collation(colsStmt.ColumnInt64(6)),
			CreateSeq:  uint64(colsStmt.ColumnInt64(7)),
		}
		if !colsStmt.ColumnIsNull(8) {
			entry.DropSeq = uint64(colsStmt.ColumnInt64(8))
		}
		snap.Columns = append(snap.Columns, entry)
	}

	keysStmt, _, err := s.conn.Prepare(listKeysSQL)
	if err != nil {
		return nil, fmt.Errorf("metadata: prepare list_keys: %w", err)
	}
	defer keysStmt.Finalize()
	for {
		hasRow, err := keysStmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		var tid crdt.TableID
		var kid crdt.KeyID
		var cid crdt.ColumnID
		if err := readBlobInto(keysStmt, 0, tid[:]); err != nil {
			return nil, err
		}
		if err := readBlobInto(keysStmt, 1, kid[:]); err != nil {
			return nil, err
		}
		if err := readBlobInto(keysStmt, 2, cid[:]); err != nil {
			return nil, err
		}
		entry := KeyEntry{
			TableID:     tid,
			KeyID:       kid,
			ColumnID:    cid,
			Ordinal:     int(keysStmt.ColumnInt64(3)),
			State:       keysStmt.ColumnText(4),
			Coordinated: keysStmt.ColumnInt64(5) != 0,
			CreateSeq:   uint64(keysStmt.ColumnInt64(7)),
		}
		if !keysStmt.ColumnIsNull(6) {
			entry.Predicate = append([]byte(nil), keysStmt.ColumnBlob(6)...)
		}
		if !keysStmt.ColumnIsNull(8) {
			entry.DropSeq = uint64(keysStmt.ColumnInt64(8))
		}
		snap.Keys = append(snap.Keys, entry)
	}

	return snap, nil
}

// bind16 copies a 16-byte ID into a fresh heap-allocated slice that
// contains only bytes and no surrounding pointers. Cgo's pointer-check
// walks the allocation containing a Go pointer passed to C; if the
// caller's slice points into a stack frame or struct that holds Go
// pointers (e.g. a TableEntry with string fields), the check rejects
// the call. Copying into a fresh []byte allocation isolates the bytes.
func bind16(b []byte) []byte {
	out := make([]byte, 16)
	copy(out, b)
	return out
}

// readBlobInto copies the column blob at i into dst. Errors if the column
// width != len(dst), since every blob ID is fixed-width by spec.
func readBlobInto(stmt *sqlitebridge.Stmt, i int, dst []byte) error {
	b := stmt.ColumnBlob(i)
	if len(b) != len(dst) {
		return fmt.Errorf("metadata: column %d width = %d; want %d", i, len(b), len(dst))
	}
	copy(dst, b)
	return nil
}

// cachedStmt lazily prepares and caches a per-tx-shared *Stmt slot. The
// catalog/schema-event statements are cold (a few per DDL apply) — caching
// is cheap and avoids re-preparation across consecutive DDLs.
func (tx *Tx) cachedStmt(slot **sqlitebridge.Stmt, sql string) (*sqlitebridge.Stmt, error) {
	if slot == nil {
		return nil, errors.New("metadata: nil stmt slot")
	}
	if *slot != nil {
		return *slot, nil
	}
	stmt, _, err := tx.conn.Prepare(sql)
	if err != nil {
		return nil, fmt.Errorf("metadata: prepare cached stmt: %w", err)
	}
	*slot = stmt
	return stmt, nil
}
