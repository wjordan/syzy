package metadata

import (
	"encoding/binary"
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

// AdoptClone resets the identity-bearing fields of a metadata that was
// just imported wholesale from a peer's clone stream, so a producer
// opening it next will run as newOrigin without colliding with the
// source's own ongoing operation.
//
// What survives the import:
//
//   - cluster_id, schema_seq, the schema catalog (syzy_table/_column/_key/
//     _schema_event/_synth_trigger), row_clock, and frontier rows for
//     every origin the source had observed. These are the receiver's
//     starting view of cluster history.
//
// What this method rewrites:
//
//   - node_id           — set to newOrigin.
//   - sender_seq        — wiped; one fresh row (newOrigin, 1).
//   - clean_shutdown    — true (next start is treated as clean).
//   - hlc_last          — pulled forward to atLeastHLC if it would
//     regress otherwise; never moves backward.
//   - frontier[newOrigin] — seeded as (last_seq=0, last_hlc=hlc_last)
//     so the producer's monotonicity invariant holds at first commit.
//   - meta.intent       — cleared (any source-side intent is theirs).
//   - meta.snapshot_markers — cleared (those are byte offsets into the
//     source's per-origin journals; we have fresh empty journals).
//
// applied_gaps deliberately survives: the source already saw those
// remote seqs, the row state is in the cloned app.db, and we don't want
// peer re-broadcasts to redundantly re-apply them.
func (s *Store) AdoptClone(newOrigin crdt.Origin, atLeastHLC crdt.Clock) error {
	if newOrigin == 0 {
		return fmt.Errorf("metadata: AdoptClone: newOrigin must be non-zero")
	}
	return s.WithTx(func(tx *Tx) error {
		if err := tx.execNoArgs(`DELETE FROM sender_seq`); err != nil {
			return fmt.Errorf("metadata: AdoptClone: clear sender_seq: %w", err)
		}
		if err := tx.PutSenderSeq(newOrigin, 1); err != nil {
			return fmt.Errorf("metadata: AdoptClone: seed sender_seq: %w", err)
		}
		if err := tx.putUint64(keyNodeID, uint64(newOrigin)); err != nil {
			return fmt.Errorf("metadata: AdoptClone: set node_id: %w", err)
		}
		if err := tx.SetMeta(keyCleanShutdown, []byte{1}); err != nil {
			return fmt.Errorf("metadata: AdoptClone: set clean_shutdown: %w", err)
		}
		hlc, err := tx.getUint64(keyHLCLast)
		if err != nil {
			return fmt.Errorf("metadata: AdoptClone: read hlc_last: %w", err)
		}
		merged := crdt.UnpackClock(hlc).Forward(atLeastHLC)
		if err := tx.SetHLCLast(merged); err != nil {
			return fmt.Errorf("metadata: AdoptClone: set hlc_last: %w", err)
		}
		if err := tx.AdvanceFrontier(newOrigin, 0, merged); err != nil {
			return fmt.Errorf("metadata: AdoptClone: seed frontier: %w", err)
		}
		if err := tx.ClearAllIntents(); err != nil {
			return fmt.Errorf("metadata: AdoptClone: clear intent: %w", err)
		}
		if err := tx.deleteMeta(keySnapshotMarkers); err != nil {
			return fmt.Errorf("metadata: AdoptClone: clear snapshot_markers: %w", err)
		}
		return nil
	})
}

// getUint64 reads an 8-byte meta value, returning 0 when absent. Used
// inside WithTx where the public Store.getUint64 would re-acquire the
// already-held mutex.
func (tx *Tx) getUint64(key string) (uint64, error) {
	v, ok, err := getMeta(tx.stmts.getMeta, key)
	if err != nil || !ok {
		return 0, err
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("metadata: %s wrong width: got %d, want 8", key, len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

// deleteMeta removes a key inside an open WithTx.
func (tx *Tx) deleteMeta(key string) error {
	stmt := tx.stmts.deleteMeta
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindText(1, key); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}

// execNoArgs runs a parameterless statement on the metadata conn. Used
// by AdoptClone for DELETE FROM sender_seq; not exposed for general
// use because Tx callers should prefer prepared-statement helpers.
func (tx *Tx) execNoArgs(sql string) error {
	return tx.conn.Exec(sql)
}
