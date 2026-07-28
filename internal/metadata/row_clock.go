package metadata

import (
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// RowClockEntry is the per-row CL + base stamp persisted in the row_clock
// table. Sparse cell/range overrides are deferred to a future package.
type RowClockEntry struct {
	CL   uint64
	Base crdt.Stamp
}

// GetRowClock returns the row's CL+base. ok=false → never-existed (caller
// uses crdt.RowState{} as the implicit baseline).
func (s *Store) GetRowClock(table crdt.TableID, pk crdt.PKBlob) (RowClockEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return getRowClock(s.stmts.getRowClock, table, pk)
}

// PutRowClock upserts the row's CL+base.
func (s *Store) PutRowClock(table crdt.TableID, pk crdt.PKBlob, e RowClockEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return putRowClock(s.stmts.putRowClock, table, pk, e)
}

// PutRowClock upserts the row's CL+base inside an open WithTx — see
// Store.PutRowClock.
func (tx *Tx) PutRowClock(table crdt.TableID, pk crdt.PKBlob, e RowClockEntry) error {
	return putRowClock(tx.stmts.putRowClock, table, pk, e)
}

func getRowClock(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob) (RowClockEntry, bool, error) {
	if err := stmt.Reset(); err != nil {
		return RowClockEntry{}, false, err
	}
	if err := stmt.BindBlob(1, table[:]); err != nil {
		return RowClockEntry{}, false, err
	}
	if err := stmt.BindBlob(2, pk); err != nil {
		return RowClockEntry{}, false, err
	}
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		return RowClockEntry{}, hasRow, err
	}
	entry := RowClockEntry{
		CL: uint64(stmt.ColumnInt64(0)),
		Base: crdt.Stamp{
			Clock:  crdt.UnpackClock(uint64(stmt.ColumnInt64(1))),
			Origin: crdt.Origin(stmt.ColumnInt64(2)),
		},
	}
	if _, err := stmt.Step(); err != nil {
		return RowClockEntry{}, false, err
	}
	return entry, true, nil
}

func putRowClock(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob, e RowClockEntry) error {
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindBlob(1, table[:]); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, pk); err != nil {
		return err
	}
	if err := stmt.BindInt64(3, int64(e.CL)); err != nil {
		return err
	}
	if err := stmt.BindInt64(4, int64(e.Base.Pack())); err != nil {
		return err
	}
	if err := stmt.BindInt64(5, int64(e.Base.Origin)); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}
