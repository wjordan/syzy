package metadata

import (
	"errors"
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

// ApplyCatalogOp upserts the metadata catalog rows (syzy_table / syzy_column /
// syzy_key / syzy_synth_trigger) that reflect op's post-state, at schemaSeq.
// It is the replication-catalog half of applying a schema event: the physical
// structural SQLite mutation is performed by the caller, and
// this records the durable catalog so the in-memory ID map can be rebuilt on
// restart. Idempotent — rerunning against an already-applied op lands on the
// same rows.
//
// Shared by the SQLite producer (resolveLocalDDL, the originator finalize)
// and the broker catch-up loop so the two paths cannot drift.
func (tx *Tx) ApplyCatalogOp(op crdt.CatalogOp, schemaSeq uint64) error {
	switch op.Kind {
	case crdt.OpCreateTable:
		return catApplyCreateTable(tx, op, schemaSeq)
	case crdt.OpAddColumn:
		return catApplyAddColumn(tx, op, schemaSeq)
	case crdt.OpRenameTable:
		return catApplyRenameTable(tx, op, schemaSeq)
	case crdt.OpRenameColumn:
		return catApplyRenameColumn(tx, op, schemaSeq)
	case crdt.OpDropColumn:
		return catApplyDropColumn(tx, op, schemaSeq)
	case crdt.OpDropTable:
		if err := catApplyDropTable(tx, op, schemaSeq); err != nil {
			return err
		}
		return tx.DeleteSynthTriggersForTable(op.TableID)
	case crdt.OpAddUniqueKey:
		return catApplyAddUniqueKey(tx, op, schemaSeq)
	case crdt.OpDropUniqueKey:
		return catApplyDropUniqueKey(tx, op, schemaSeq)
	case crdt.OpAlterColumn:
		// The attributes an ALTER COLUMN carries (type, default, NOT NULL) are
		// engine-local; this catalog records a column's identity, ordinal, clock
		// group and collation, none of which the op moves.
		return nil
	case crdt.OpCreateIndex, crdt.OpDropIndex,
		crdt.OpCreateView, crdt.OpDropView,
		crdt.OpCreateVirtualTable, crdt.OpDropVirtualTable:
		// Opaque-SQL forms have no catalog rows to upsert; the structural
		// mutation is the entire effect.
		return nil
	case crdt.OpCreateTrigger:
		if op.TableID != (crdt.TableID{}) {
			return tx.InsertSynthTrigger(SynthTriggerEntry{
				ChildTableID: op.TableID,
				TriggerName:  op.ObjectName,
			})
		}
		return nil
	case crdt.OpSetClockGroup:
		return tx.SetDefaultClockGroup(op.TableID, op.ClockGroup)
	case crdt.OpDropTrigger:
		// Synth triggers are tracked as a group keyed by child_table_id; the
		// clean-up lives on the OpDropTable branch (synth triggers always ship
		// in a child-drop bundle). Standalone user-written triggers have no
		// metadata bookkeeping.
		return nil
	case crdt.OpBundle:
		for _, sub := range op.SubOps {
			if err := tx.ApplyCatalogOp(sub, schemaSeq); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("apply_catalog_op: unsupported kind %v", op.Kind)
}

func catApplyCreateTable(tx *Tx, op crdt.CatalogOp, schemaSeq uint64) error {
	// A counter column's contributions are per-column payloads; the
	// table must arbitrate per cell for them to merge (admission
	// guarantees the pairing, this keeps the derivation deterministic
	// on every receiver).
	group := ClockGroupRow
	for _, c := range op.Columns {
		if c.ClockGroup == ClockGroupCounter {
			group = ClockGroupCell
			break
		}
	}
	if err := tx.UpsertTable(TableEntry{
		ID: op.TableID, Name: op.TableName, State: StateActive,
		DefaultClockGroup: group, CreateSeq: schemaSeq,
	}); err != nil {
		return err
	}
	for _, c := range op.Columns {
		group := c.ClockGroup
		if group == "" {
			group = ClockGroupRow
		}
		if err := tx.UpsertColumn(ColumnEntry{
			TableID: op.TableID, ColumnID: c.ID, Name: c.Name,
			Ordinal: c.Ordinal, State: StateActive,
			ClockGroup: group, Collation: c.Collation, CreateSeq: schemaSeq,
		}); err != nil {
			return err
		}
	}
	for _, k := range op.Keys {
		for i, m := range k.Members {
			ord := m.Ordinal
			if ord == 0 && i > 0 {
				ord = i
			}
			if err := tx.UpsertKey(KeyEntry{
				TableID: op.TableID, KeyID: k.KeyID, ColumnID: m.ColumnID,
				Ordinal: ord, State: StateActive,
				Coordinated: k.Coordinated, Predicate: keyPredicateBytes(k),
				CreateSeq: schemaSeq,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func catApplyAddColumn(tx *Tx, op crdt.CatalogOp, schemaSeq uint64) error {
	if len(op.Columns) != 1 {
		return errors.New("ADD COLUMN: expected 1 column")
	}
	c := op.Columns[0]
	group := c.ClockGroup
	if group == "" {
		group = ClockGroupRow
	}
	return tx.UpsertColumn(ColumnEntry{
		TableID: op.TableID, ColumnID: c.ID, Name: c.Name,
		Ordinal: c.Ordinal, State: StateActive,
		ClockGroup: group, Collation: c.Collation, CreateSeq: schemaSeq,
	})
}

func catApplyRenameTable(tx *Tx, op crdt.CatalogOp, _ uint64) error {
	// Name-only update — an UpsertTable here would reset
	// default_clock_group and create_seq.
	return tx.RenameTable(op.TableID, op.TableName)
}

func catApplyRenameColumn(tx *Tx, op crdt.CatalogOp, _ uint64) error {
	// Name-only update — preserving ordinal keeps it == attnum-1 for rebinding.
	return tx.RenameColumn(op.TableID, op.ColumnID, op.ColumnName)
}

func catApplyDropColumn(tx *Tx, op crdt.CatalogOp, schemaSeq uint64) error {
	// Tombstone in place: name/ordinal/create_seq must survive the drop
	// so capture-time layouts (TableAtSeq) and structural reconciliation
	// can still resolve the column. The upsert fallback only fires when
	// the row is missing entirely (a replayed drop on a node that folded
	// away the add) and records the degenerate tombstone shape.
	changed, err := tx.DropColumn(op.TableID, op.ColumnID, schemaSeq)
	if err != nil || changed {
		return err
	}
	return tx.UpsertColumn(ColumnEntry{
		TableID: op.TableID, ColumnID: op.ColumnID, Name: "",
		State: StateDropped, ClockGroup: ClockGroupRow,
		CreateSeq: 0, DropSeq: schemaSeq,
	})
}

func catApplyDropTable(tx *Tx, op crdt.CatalogOp, schemaSeq uint64) error {
	return tx.UpsertTable(TableEntry{
		ID: op.TableID, Name: "", State: StateDropped,
		DefaultClockGroup: ClockGroupRow,
		CreateSeq:         0, DropSeq: schemaSeq,
	})
}

func catApplyAddUniqueKey(tx *Tx, op crdt.CatalogOp, schemaSeq uint64) error {
	if len(op.Keys) != 1 {
		return errors.New("AddUniqueKey: expected one key")
	}
	for _, m := range op.Keys[0].Members {
		if err := tx.UpsertKey(KeyEntry{
			TableID: op.TableID, KeyID: op.KeyID, ColumnID: m.ColumnID,
			Ordinal: m.Ordinal, State: StateActive,
			Coordinated: op.Keys[0].Coordinated, Predicate: keyPredicateBytes(op.Keys[0]),
			CreateSeq: schemaSeq,
		}); err != nil {
			return err
		}
	}
	return nil
}

// keyPredicateBytes encodes a partial unique key's predicate for metadata
// storage, returning nil for a total key (stored as NULL).
func keyPredicateBytes(k crdt.CatalogKey) []byte {
	if k.Predicate.Root == nil {
		return nil
	}
	return crdt.EncodeUniquePredicate(k.Predicate)
}

func catApplyDropUniqueKey(tx *Tx, op crdt.CatalogOp, schemaSeq uint64) error {
	// Member columns aren't on the wire for the drop; mark the synthetic key
	// row at ordinal 0 dropped — the active-state filter suppresses it.
	return tx.UpsertKey(KeyEntry{
		TableID: op.TableID, KeyID: op.KeyID,
		ColumnID: crdt.ColumnID{},
		Ordinal:  0, State: StateDropped,
		CreateSeq: 0, DropSeq: schemaSeq,
	})
}
