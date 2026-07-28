package metadata

import (
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// FrontierEntry is one origin's contiguous-applied head plus the HLC of
// the last finalized changeset under that head.
type FrontierEntry struct {
	LastSeq crdt.Seq
	LastHLC crdt.Clock
}

// Frontier returns the full per-origin map. Empty if no origins seen.
func (s *Store) Frontier() (map[crdt.Origin]FrontierEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt := s.stmts.frontierAll
	if err := stmt.Reset(); err != nil {
		return nil, err
	}
	out := map[crdt.Origin]FrontierEntry{}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return out, nil
		}
		out[crdt.Origin(stmt.ColumnInt64(0))] = FrontierEntry{
			LastSeq: crdt.Seq(stmt.ColumnInt64(1)),
			LastHLC: crdt.UnpackClock(uint64(stmt.ColumnInt64(2))),
		}
	}
}

// FrontierFor returns one origin's entry, or zero/false if absent.
func (s *Store) FrontierFor(origin crdt.Origin) (FrontierEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt := s.stmts.frontierFor
	if err := stmt.Reset(); err != nil {
		return FrontierEntry{}, false, err
	}
	if err := stmt.BindInt64(1, int64(origin)); err != nil {
		return FrontierEntry{}, false, err
	}
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		return FrontierEntry{}, hasRow, err
	}
	entry := FrontierEntry{
		LastSeq: crdt.Seq(stmt.ColumnInt64(0)),
		LastHLC: crdt.UnpackClock(uint64(stmt.ColumnInt64(1))),
	}
	if _, err := stmt.Step(); err != nil {
		return FrontierEntry{}, false, err
	}
	return entry, true, nil
}

// AdvanceFrontier upserts (origin, last_seq, last_hlc). Caller is
// responsible for ensuring last_seq is monotonically non-decreasing.
func (s *Store) AdvanceFrontier(origin crdt.Origin, lastSeq crdt.Seq, lastHLC crdt.Clock) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return advanceFrontier(s.stmts.advanceFrontier, origin, lastSeq, lastHLC)
}

// AdvanceFrontier upserts (origin, last_seq, last_hlc) inside an open
// WithTx — see Store.AdvanceFrontier.
func (tx *Tx) AdvanceFrontier(origin crdt.Origin, lastSeq crdt.Seq, lastHLC crdt.Clock) error {
	return advanceFrontier(tx.stmts.advanceFrontier, origin, lastSeq, lastHLC)
}

// DeleteFrontier removes an origin's frontier row inside an open WithTx.
// Used by origin GC to forget a dead, cluster-wide-durable origin so the
// frontier stops tracking it (and the published baseline stops carrying
// it to new nodes). Deleting an absent origin is a no-op.
func (tx *Tx) DeleteFrontier(origin crdt.Origin) error {
	return deleteFrontier(tx.stmts.deleteFrontier, origin)
}

func deleteFrontier(stmt *sqlitebridge.Stmt, origin crdt.Origin) error {
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindInt64(1, int64(origin)); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}

func advanceFrontier(stmt *sqlitebridge.Stmt, origin crdt.Origin, lastSeq crdt.Seq, lastHLC crdt.Clock) error {
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindInt64(1, int64(origin)); err != nil {
		return err
	}
	if err := stmt.BindInt64(2, int64(lastSeq)); err != nil {
		return err
	}
	if err := stmt.BindInt64(3, int64(lastHLC.Pack())); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}
