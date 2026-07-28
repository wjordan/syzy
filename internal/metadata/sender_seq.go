package metadata

import (
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

// SenderSeqs returns the per-origin next-to-allocate sequence map. A
// daemon draining N origins gets back N entries (one per origin it
// has ever drained). Empty result on a fresh metadata.
func (s *Store) SenderSeqs() (map[crdt.Origin]crdt.Seq, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt := s.stmts.getSenderSeqs
	if err := stmt.Reset(); err != nil {
		return nil, err
	}
	out := map[crdt.Origin]crdt.Seq{}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, fmt.Errorf("metadata: sender_seqs scan: %w", err)
		}
		if !hasRow {
			return out, nil
		}
		o := crdt.Origin(uint64(stmt.ColumnInt64(0)))
		seq := crdt.Seq(uint64(stmt.ColumnInt64(1)))
		out[o] = seq
	}
}

// PutSenderSeq upserts (origin, next_seq) inside an open WithTx. The
// snapshotter calls this once per dirty origin per snapshot.
func (tx *Tx) PutSenderSeq(origin crdt.Origin, next crdt.Seq) error {
	stmt := tx.stmts.putSenderSeq
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindInt64(1, int64(uint64(origin))); err != nil {
		return err
	}
	if err := stmt.BindInt64(2, int64(uint64(next))); err != nil {
		return err
	}
	if _, err := stmt.Step(); err != nil {
		return fmt.Errorf("metadata: put sender_seq: %w", err)
	}
	return nil
}
