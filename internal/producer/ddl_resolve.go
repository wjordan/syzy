package producer

import (
	"bytes"
	"context"
	"fmt"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// resolveLocalDDL applies a durable LocalDDL intent to the metadata
// catalog inside one transaction: idempotent apply_catalog_op +
// syzy_schema_event UPSERT + meta.schema_seq advance + meta.intent
// clear. Used by wal_hook (originator's live path) and by producer
// startup recovery.
//
// On the originator's live path the SQLite mutation has already been
// executed by the user statement; the catalog is just being brought
// in line with the post-state. On startup recovery the intent's
// CatalogOp is verified against the schema log before being applied
// idempotently: apply_catalog_op checks the live SQLite structural
// state before issuing any DDL.
func resolveLocalDDL(app *sqlitebridge.Conn, sc *metadata.Store, cat *catalog.Catalog,
	schemaLog schemalog.Log, origin crdt.Origin,
	intent metadata.LocalDDLIntent, nowMicros int64) error {
	op, err := crdt.DecodeCatalogOp(intent.CatalogOp)
	if err != nil {
		return fmt.Errorf("DecodeCatalogOp: %w", err)
	}
	// Check whether the broker's catch-up loop already applied this
	// schema_seq from the schema log. If so, all the catalog mutations
	// + schema_event row + meta.schema_seq advance are already in
	// place; we just need to clear the intent.
	currentSeq, _, err := sc.GetSchemaSeq()
	if err != nil {
		return fmt.Errorf("read schema_seq: %w", err)
	}
	if currentSeq >= intent.SchemaSeq {
		if err := sc.ClearOriginIntent(origin); err != nil {
			return err
		}
		return cat.RebuildWithPKDefaults(app)
	}
	// Trust-but-verify: an intent is a valid resolution input only if
	// its event actually committed to the schema log. Admission writes
	// the intent BEFORE Append on the autocommit path, so a crash (or
	// a failed clear after a rejected Append) can strand an intent
	// whose event never landed — resolving it would advance schema_seq
	// past the log head, an unrepairable fork. A different producer
	// may also have won intent.SchemaSeq, in which case our statement
	// never executed and the intent is dead weight. nil schemaLog (no
	// log configured in this process) keeps the legacy trust path.
	if schemaLog != nil {
		verified := false
		if intent.SchemaSeq > 0 {
			events, err := schemaLog.Read(context.Background(), intent.SchemaSeq-1, 1)
			if err != nil {
				return fmt.Errorf("verify intent seq=%d: %w", intent.SchemaSeq, err)
			}
			verified = len(events) > 0 && events[0].SchemaSeq == intent.SchemaSeq &&
				bytes.Equal(events[0].CatalogOp, intent.CatalogOp)
		}
		if !verified {
			return sc.ClearOriginIntent(origin)
		}
	}
	applyState := metadata.ApplyStateApplied
	if err := sc.WithTx(func(tx *metadata.Tx) error {
		// Re-check under the tx: a stale-intent takeover by the
		// broker's catch-up (or another tick of it) may have applied
		// this event — and later ones — since the read above. Writing
		// intent.SchemaSeq now would REGRESS schema_seq.
		if cur, _, err := tx.GetSchemaSeq(); err != nil {
			return err
		} else if cur >= intent.SchemaSeq {
			return tx.ClearOriginIntent(origin)
		}
		if err := tx.ApplyCatalogOp(op, intent.SchemaSeq); err != nil {
			applyState = metadata.ApplyStateFailedLocal
			// Originator-only divergence: the user's DDL has already
			// committed in SQLite (this is the wal_hook / startup-recovery
			// finalize step, not the inbound apply). We can't roll back
			// SQLite, so we still advance schema_seq + clear the intent,
			// but mark the row failed_local so the broker's startup
			// drainFailedLocalSchemaEvents pass re-runs applyCatalogOp
			// idempotently next start. In normal operation these UPSERTs
			// don't fail; this is a disk-full / corruption escape hatch.
		}
		if err := tx.AppendSchemaEvent(metadata.SchemaEventEntry{
			SchemaSeq:   intent.SchemaSeq,
			ParentSeq:   intent.ParentSeq,
			CatalogOp:   intent.CatalogOp,
			RawSQL:      intent.RawSQL,
			AppliedAtUs: nowMicros,
			ApplyState:  applyState,
		}); err != nil {
			return err
		}
		if err := tx.SetMeta("schema_seq", packUint64(intent.SchemaSeq)); err != nil {
			return err
		}
		return tx.ClearOriginIntent(origin)
	}); err != nil {
		return err
	}
	return cat.RebuildWithPKDefaults(app)
}

// packUint64 returns the 8-byte big-endian encoding used by metadata
// uint64 meta keys.
func packUint64(v uint64) []byte {
	var b [8]byte
	for i := 7; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
	return b[:]
}
