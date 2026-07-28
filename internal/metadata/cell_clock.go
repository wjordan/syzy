package metadata

import (
	"fmt"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// CellClockEntry is one (column, Stamp) override for a single row. Sparse
// per spec: written only when a column needs independent ordering
// (UNIQUE arbitration loser-null, blob_patch interaction). Effective
// Stamp falls through cell_clock → row_clock baseline.
type CellClockEntry struct {
	Column crdt.ColumnID
	Stamp  crdt.Stamp
}

// FullCellClockEntry is a single row from the cell_clock table, used by
// recovery to rehydrate the in-memory cache.
type FullCellClockEntry struct {
	Table  crdt.TableID
	PK     crdt.PKBlob
	Column crdt.ColumnID
	Stamp  crdt.Stamp
}

// GetCellClocks returns every cell_clock override for one row. Empty
// slice when the row has no overrides.
func (s *Store) GetCellClocks(table crdt.TableID, pk crdt.PKBlob) ([]CellClockEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return getCellClocks(s.stmts.getCellClocksForRow, table, pk)
}

// PutCellClock upserts one (table, pk, column) override at stamp.
func (tx *Tx) PutCellClock(table crdt.TableID, pk crdt.PKBlob, col crdt.ColumnID, stamp crdt.Stamp) error {
	return putCellClock(tx.stmts.putCellClock, table, pk, col, stamp)
}

// DeleteCellClock drops a single (table, pk, column) override.
func (tx *Tx) DeleteCellClock(table crdt.TableID, pk crdt.PKBlob, col crdt.ColumnID) error {
	return deleteCellClock(tx.stmts.deleteCellClock, table, pk, col)
}

// DeleteCellClocksForRow drops every override for one row. Called on CL
// bumps (resurrection / tombstone) to honor the implicit-tombstone rule
// for prior-generation overrides (CRDT.md#causal-length-cl).
func (tx *Tx) DeleteCellClocksForRow(table crdt.TableID, pk crdt.PKBlob) error {
	return deleteCellClocksForRow(tx.stmts.deleteCellClocksForRow, table, pk)
}

// AllCellClocks scans the entire cell_clock table. Used by recovery to
// rehydrate the cache. One-shot; allocates a single slice.
func (s *Store) AllCellClocks() ([]FullCellClockEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt, _, err := s.conn.Prepare(`SELECT table_id, pk_blob, column_id, hlc, hlc_origin FROM cell_clock`)
	if err != nil {
		return nil, fmt.Errorf("metadata: prepare AllCellClocks: %w", err)
	}
	defer stmt.Finalize()
	var out []FullCellClockEntry
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return out, nil
		}
		var table crdt.TableID
		copy(table[:], stmt.ColumnBlob(0))
		pkBytes := stmt.ColumnBlob(1)
		pk := make(crdt.PKBlob, len(pkBytes))
		copy(pk, pkBytes)
		col, stamp := readCellAt(stmt, 2, 3, 4)
		out = append(out, FullCellClockEntry{Table: table, PK: pk, Column: col, Stamp: stamp})
	}
}

func getCellClocks(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob) ([]CellClockEntry, error) {
	if _, err := bindCellKey(stmt, table, pk, nil); err != nil {
		return nil, err
	}
	var out []CellClockEntry
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return out, nil
		}
		col, stamp := readCellAt(stmt, 0, 1, 2)
		out = append(out, CellClockEntry{Column: col, Stamp: stamp})
	}
}

// readCellAt decodes a (column_id, packed-hlc, origin) triple at the
// given column indices into a (ColumnID, Stamp).
func readCellAt(stmt *sqlitebridge.Stmt, colIdx, hlcIdx, originIdx int) (crdt.ColumnID, crdt.Stamp) {
	var col crdt.ColumnID
	copy(col[:], stmt.ColumnBlob(colIdx))
	stamp := crdt.Stamp{
		Clock:  crdt.UnpackClock(uint64(stmt.ColumnInt64(hlcIdx))),
		Origin: crdt.Origin(stmt.ColumnInt64(originIdx)),
	}
	return col, stamp
}

// bindCellKey resets stmt and binds (table, pk[, col]) to the leading
// parameters. col is optional: pass nil to bind only (table, pk) for
// the row-level statements. Returns the next 1-based parameter index.
func bindCellKey(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob, col *crdt.ColumnID) (int, error) {
	if err := stmt.Reset(); err != nil {
		return 0, err
	}
	if err := stmt.BindBlob(1, table[:]); err != nil {
		return 0, err
	}
	if err := stmt.BindBlob(2, pk); err != nil {
		return 0, err
	}
	if col == nil {
		return 3, nil
	}
	if err := stmt.BindBlob(3, col[:]); err != nil {
		return 0, err
	}
	return 4, nil
}

func putCellClock(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob, col crdt.ColumnID, stamp crdt.Stamp) error {
	next, err := bindCellKey(stmt, table, pk, &col)
	if err != nil {
		return err
	}
	if err := stmt.BindInt64(next, int64(stamp.Pack())); err != nil {
		return err
	}
	if err := stmt.BindInt64(next+1, int64(stamp.Origin)); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

func deleteCellClock(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob, col crdt.ColumnID) error {
	if _, err := bindCellKey(stmt, table, pk, &col); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}

func deleteCellClocksForRow(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob) error {
	if _, err := bindCellKey(stmt, table, pk, nil); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}
