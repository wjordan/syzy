package metadata

import (
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

// QuarantineEntry is one inbound changeset the broker advanced past after a
// deterministic constraint failure, retained for deferred re-apply. Payload is
// the raw wire changeset; Err is the apply error string at quarantine time.
type QuarantineEntry struct {
	Origin   crdt.Origin
	Seq      crdt.Seq
	Payload  []byte
	Err      string
	Attempts int64
}

// QuarantineStats is the poll-friendly aggregate over apply_quarantine:
// resident entry count, the oldest entry's first_seen (µs; 0 when empty),
// and the highest attempt count. Cheap (one aggregate query, no payload
// copies) — intended for InboundHealth.
type QuarantineStats struct {
	Resident    int
	OldestUs    int64
	MaxAttempts int64
}

// QuarantineStats aggregates the resident quarantine without copying
// payloads.
func (s *Store) QuarantineStats() (QuarantineStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return QuarantineStats{}, ErrClosed
	}
	stmt, _, err := s.conn.Prepare(`SELECT COUNT(*), COALESCE(MIN(first_seen), 0), COALESCE(MAX(attempts), 0) FROM apply_quarantine`)
	if err != nil {
		return QuarantineStats{}, fmt.Errorf("metadata: prepare quarantine_stats: %w", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		return QuarantineStats{}, fmt.Errorf("metadata: quarantine_stats: %w", err)
	}
	return QuarantineStats{
		Resident:    int(stmt.ColumnInt64(0)),
		OldestUs:    stmt.ColumnInt64(1),
		MaxAttempts: stmt.ColumnInt64(2),
	}, nil
}

const putQuarantineSQL = `
INSERT INTO apply_quarantine (origin, seq, payload, err, first_seen, attempts)
VALUES (?, ?, ?, ?, ?, 0)
ON CONFLICT(origin, seq) DO UPDATE SET err = excluded.err`

// PutQuarantine records (or refreshes) a quarantined changeset. Idempotent on
// (origin, seq): a repeat keeps the original first_seen/attempts and only
// updates the error string.
func (s *Store) PutQuarantine(origin crdt.Origin, seq crdt.Seq, payload []byte, applyErr string, firstSeenUs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return ErrClosed
	}
	stmt, _, err := s.conn.Prepare(putQuarantineSQL)
	if err != nil {
		return fmt.Errorf("metadata: prepare put_quarantine: %w", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, int64(origin)); err != nil {
		return err
	}
	if err := stmt.BindInt64(2, int64(seq)); err != nil {
		return err
	}
	if err := stmt.BindBlob(3, payload); err != nil {
		return err
	}
	if err := stmt.BindText(4, applyErr); err != nil {
		return err
	}
	if err := stmt.BindInt64(5, firstSeenUs); err != nil {
		return err
	}
	if _, err := stmt.Step(); err != nil {
		return fmt.Errorf("metadata: put_quarantine: %w", err)
	}
	return nil
}

// DeleteQuarantine removes a quarantined entry (after a successful re-apply).
func (s *Store) DeleteQuarantine(origin crdt.Origin, seq crdt.Seq) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return ErrClosed
	}
	stmt, _, err := s.conn.Prepare(`DELETE FROM apply_quarantine WHERE origin = ? AND seq = ?`)
	if err != nil {
		return fmt.Errorf("metadata: prepare delete_quarantine: %w", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, int64(origin)); err != nil {
		return err
	}
	if err := stmt.BindInt64(2, int64(seq)); err != nil {
		return err
	}
	if _, err := stmt.Step(); err != nil {
		return fmt.Errorf("metadata: delete_quarantine: %w", err)
	}
	return nil
}

// BumpQuarantineAttempt increments the retry counter for an entry that still
// fails (diagnostic only).
func (s *Store) BumpQuarantineAttempt(origin crdt.Origin, seq crdt.Seq) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return ErrClosed
	}
	stmt, _, err := s.conn.Prepare(`UPDATE apply_quarantine SET attempts = attempts + 1 WHERE origin = ? AND seq = ?`)
	if err != nil {
		return fmt.Errorf("metadata: prepare bump_quarantine: %w", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, int64(origin)); err != nil {
		return err
	}
	if err := stmt.BindInt64(2, int64(seq)); err != nil {
		return err
	}
	if _, err := stmt.Step(); err != nil {
		return fmt.Errorf("metadata: bump_quarantine: %w", err)
	}
	return nil
}

// CountQuarantineByOrigin returns the number of resident quarantine entries for
// origin. The broker caps this to bound damage: a flood of constraint failures
// for one origin signals likely real corruption (not an isolated cross-origin
// gap), at which point it stops advancing and hard-blocks instead.
func (s *Store) CountQuarantineByOrigin(origin crdt.Origin) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return 0, ErrClosed
	}
	stmt, _, err := s.conn.Prepare(`SELECT COUNT(*) FROM apply_quarantine WHERE origin = ?`)
	if err != nil {
		return 0, fmt.Errorf("metadata: prepare count_quarantine: %w", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, int64(origin)); err != nil {
		return 0, err
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return 0, fmt.Errorf("metadata: count_quarantine: %w", err)
	}
	if !hasRow {
		return 0, nil
	}
	return int(stmt.ColumnInt64(0)), nil
}

// ListQuarantine returns every resident quarantine entry, oldest first, for the
// deferred re-apply drain.
func (s *Store) ListQuarantine() ([]QuarantineEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil, ErrClosed
	}
	stmt, _, err := s.conn.Prepare(`SELECT origin, seq, payload, err, attempts FROM apply_quarantine ORDER BY first_seen`)
	if err != nil {
		return nil, fmt.Errorf("metadata: prepare list_quarantine: %w", err)
	}
	defer stmt.Finalize()
	var out []QuarantineEntry
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, fmt.Errorf("metadata: list_quarantine: %w", err)
		}
		if !hasRow {
			break
		}
		out = append(out, QuarantineEntry{
			Origin:   crdt.Origin(uint64(stmt.ColumnInt64(0))),
			Seq:      crdt.Seq(uint64(stmt.ColumnInt64(1))),
			Payload:  append([]byte(nil), stmt.ColumnBlob(2)...),
			Err:      stmt.ColumnText(3),
			Attempts: stmt.ColumnInt64(4),
		})
	}
	return out, nil
}
