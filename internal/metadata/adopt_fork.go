package metadata

import (
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

// AdoptFork resets identity- and mesh-bearing state on a metadata that
// was just staged from a parent app's published baseline, so the
// destination opens as a fresh cluster with a fresh schema-log
// namespace while keeping the parent's materialized rows + catalog.
//
// AdoptFork differs from AdoptClone in two ways: (1) it accepts a new
// cluster_id (a fork joins a new mesh; a clone keeps the source's), and
// (2) it wipes anything that depends on the source cluster's history —
// the full frontier, applied_gaps, and syzy_schema_event — because the
// fork's local schema log restarts at seq 0 with no peer origins.
//
// What survives the staged baseline:
//
//   - syzy_table/_column/_key/_synth_trigger, row_clock, cell_clock,
//     blob_range_clock — the current materialized catalog and per-row
//     clocks. Inherited rows need their CRDT/LWW baseline.
//   - meta.parent_app_txid — preserved as a recovery anchor pointing
//     into the source app's object prefix.
//   - meta.replicate_underscore — preserved so the fork inherits the
//     parent's producer.ReplicateUnderscoreTables setting. A fork of a
//     PocketBase-style template that opted underscore tables into
//     replication continues to replicate them.
//   - schema_version — set by Open; not touched.
//
// What this method rewrites:
//
//   - cluster_id            — set to newClusterID.
//   - node_id               — set to newOrigin.
//   - sender_seq            — wiped; one fresh row (newOrigin, 1).
//   - frontier              — wiped; one fresh row (newOrigin, 0, hlc).
//   - applied_gaps          — cleared (parent-cluster gap bookkeeping
//     does not transfer to a new mesh).
//   - syzy_schema_event     — wiped (schema sequence restarts; PK
//     collisions would otherwise be inevitable).
//   - meta.schema_seq       — reset to 0 (fresh schema-log namespace).
//   - meta.intent           — cleared (source-side DDL intent is theirs).
//   - meta.snapshot_markers — cleared (source journal byte offsets are
//     not valid against the fork's fresh empty journals).
//   - meta.clean_shutdown   — set true (staged metadata opens cleanly).
//   - meta.hlc_last         — Forward(atLeastHLC); never regresses.
//
// Caller must run AdoptFork on the staged metadata.db before sqlite.Open
// touches it: sqlite.Open trusts persisted metadata for identity, so the
// fork's identity must be in place at first Open.
func (s *Store) AdoptFork(newOrigin crdt.Origin, newClusterID crdt.ClusterID, atLeastHLC crdt.Clock) error {
	if newOrigin == 0 {
		return fmt.Errorf("metadata: AdoptFork: newOrigin must be non-zero")
	}
	var zero crdt.ClusterID
	if newClusterID == zero {
		return fmt.Errorf("metadata: AdoptFork: newClusterID must be non-zero")
	}
	return s.WithTx(func(tx *Tx) error {
		if err := tx.SetMeta(keyClusterID, newClusterID[:]); err != nil {
			return fmt.Errorf("metadata: AdoptFork: set cluster_id: %w", err)
		}
		if err := tx.putUint64(keyNodeID, uint64(newOrigin)); err != nil {
			return fmt.Errorf("metadata: AdoptFork: set node_id: %w", err)
		}
		if err := tx.execNoArgs(`DELETE FROM sender_seq`); err != nil {
			return fmt.Errorf("metadata: AdoptFork: clear sender_seq: %w", err)
		}
		if err := tx.PutSenderSeq(newOrigin, 1); err != nil {
			return fmt.Errorf("metadata: AdoptFork: seed sender_seq: %w", err)
		}
		if err := tx.execNoArgs(`DELETE FROM frontier`); err != nil {
			return fmt.Errorf("metadata: AdoptFork: clear frontier: %w", err)
		}
		if err := tx.execNoArgs(`DELETE FROM syzy_schema_event`); err != nil {
			return fmt.Errorf("metadata: AdoptFork: clear syzy_schema_event: %w", err)
		}
		if err := tx.putUint64(keySchemaSeq, 0); err != nil {
			return fmt.Errorf("metadata: AdoptFork: reset schema_seq: %w", err)
		}
		if err := tx.deleteMeta(keyAppliedGaps); err != nil {
			return fmt.Errorf("metadata: AdoptFork: clear applied_gaps: %w", err)
		}
		if err := tx.deleteMeta(keySnapshotMarkers); err != nil {
			return fmt.Errorf("metadata: AdoptFork: clear snapshot_markers: %w", err)
		}
		if err := tx.ClearAllIntents(); err != nil {
			return fmt.Errorf("metadata: AdoptFork: clear intent: %w", err)
		}
		if err := tx.SetMeta(keyCleanShutdown, []byte{1}); err != nil {
			return fmt.Errorf("metadata: AdoptFork: set clean_shutdown: %w", err)
		}
		hlc, err := tx.getUint64(keyHLCLast)
		if err != nil {
			return fmt.Errorf("metadata: AdoptFork: read hlc_last: %w", err)
		}
		merged := crdt.UnpackClock(hlc).Forward(atLeastHLC)
		if err := tx.SetHLCLast(merged); err != nil {
			return fmt.Errorf("metadata: AdoptFork: set hlc_last: %w", err)
		}
		if err := tx.AdvanceFrontier(newOrigin, 0, merged); err != nil {
			return fmt.Errorf("metadata: AdoptFork: seed frontier: %w", err)
		}
		return nil
	})
}
