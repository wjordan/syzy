package broker

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// catalogOpToSQL renders the SQLite DDL needed to bring AppApply
// in line with op. For typed forms (CREATE/ALTER TABLE) the SQL is
// reconstructed from the catalog op fields; for opaque forms (views,
// virtual tables, indexes) the originator's RawSQL replays directly.
//
// Returns "" for ops that have no SQLite-side effect (currently none —
// every op kind either typed-rebuilds SQL or replays RawSQL).
func catalogOpToSQL(op crdt.CatalogOp, cat catalog.TableResolver) (string, error) {
	switch op.Kind {
	case crdt.OpCreateTable:
		return renderCreateTable(op)
	case crdt.OpAddColumn:
		if len(op.Columns) != 1 {
			return "", errors.New("ADD COLUMN: need exactly one column")
		}
		// op.TableName is not on the wire for OpAddColumn (encoding only
		// carries TableID + Columns); resolve the table name via the
		// local catalog the same way DROP / RENAME COLUMN do below.
		tab, ok := cat.TableByID(op.TableID)
		if !ok {
			return "", fmt.Errorf("ADD COLUMN: table id %x not in local catalog", op.TableID)
		}
		colDef, err := renderColumnDef(op.Columns[0])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
			sqlitebridge.QuoteIdent(tab.Name), colDef), nil
	case crdt.OpRenameTable:
		// We need the old name; cat must already have the table.
		tab, ok := cat.TableByID(op.TableID)
		if !ok {
			return "", fmt.Errorf("RENAME: table id %x not in local catalog", op.TableID)
		}
		return fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			sqlitebridge.QuoteIdent(tab.Name),
			sqlitebridge.QuoteIdent(op.TableName)), nil
	case crdt.OpRenameColumn:
		tab, ok := cat.TableByID(op.TableID)
		if !ok {
			return "", fmt.Errorf("RENAME COLUMN: table id %x not in catalog", op.TableID)
		}
		col, ok := tab.ColumnByID(op.ColumnID)
		if !ok {
			return "", fmt.Errorf("RENAME COLUMN: column id %x not in catalog", op.ColumnID)
		}
		return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
			sqlitebridge.QuoteIdent(tab.Name),
			sqlitebridge.QuoteIdent(col.Name),
			sqlitebridge.QuoteIdent(op.ColumnName)), nil
	case crdt.OpDropColumn:
		tab, ok := cat.TableByID(op.TableID)
		if !ok {
			return "", fmt.Errorf("DROP COLUMN: table id %x not in catalog", op.TableID)
		}
		col, ok := tab.ColumnByID(op.ColumnID)
		if !ok {
			return "", fmt.Errorf("DROP COLUMN: column id %x not in catalog", op.ColumnID)
		}
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
			sqlitebridge.QuoteIdent(tab.Name),
			sqlitebridge.QuoteIdent(col.Name)), nil
	case crdt.OpDropTable:
		tab, ok := cat.TableByID(op.TableID)
		if !ok {
			return "", nil // already absent locally; idempotent skip
		}
		return fmt.Sprintf("DROP TABLE IF EXISTS %s",
			sqlitebridge.QuoteIdent(tab.Name)), nil
	case crdt.OpAddUniqueKey, crdt.OpDropUniqueKey:
		// CREATE TABLE bundles a typed PRIMARY KEY into the table-
		// definition; standalone CREATE UNIQUE INDEX replays as raw
		// SQL since reconstructing the index name from the typed op
		// requires extra plumbing. The apply path's loser-null
		// arbitration consults syzy_key directly, not SQLite's index
		// shape, so RawSQL replay is sufficient.
		return op.RawSQL, nil
	case crdt.OpCreateIndex, crdt.OpDropIndex,
		crdt.OpCreateView, crdt.OpDropView,
		crdt.OpCreateVirtualTable, crdt.OpDropVirtualTable,
		crdt.OpCreateTrigger, crdt.OpDropTrigger:
		return op.RawSQL, nil
	case crdt.OpSetClockGroup:
		// Pure catalog-metadata mutation; no SQLite-side effect.
		return "", nil
	case crdt.OpBundle:
		// Bundles are decomposed by applyCatalogStructural; this entry
		// is unreachable in normal flow but included for completeness.
		return "", errors.New("catalogOpToSQL: OpBundle should be applied sub-op by sub-op")
	}
	return "", fmt.Errorf("catalogOpToSQL: unsupported kind %v", op.Kind)
}

func renderCreateTable(op crdt.CatalogOp) (string, error) {
	var b strings.Builder
	// IF NOT EXISTS: the originator's broker catch-up loop can race
	// with wal_hook's schema_seq advance, briefly trying to re-apply
	// an event the writer already executed. The freshSeq guard in
	// schemaCatchupLoop usually catches this, but the small window
	// between SchemaLog.Read and GetSchemaSeq is enough that an
	// idempotent rendering is the simpler defense.
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(sqlitebridge.QuoteIdent(op.TableName))
	b.WriteString(" (")
	for i, c := range op.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		colDef, err := renderColumnDef(c)
		if err != nil {
			return "", err
		}
		b.WriteString(colDef)
	}
	// Always emit table-level PRIMARY KEY. Replicated tables reject
	// rowid-alias INTEGER PRIMARY KEY auto-allocation, so column-level
	// PK declarations have no advantage and the table-level form is
	// uniform across single and multi-column PKs.
	pk := pkColumnsFromOp(op)
	if len(pk) > 0 {
		b.WriteString(", PRIMARY KEY (")
		for i, name := range pk {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(sqlitebridge.QuoteIdent(name))
		}
		b.WriteString(")")
	}
	b.WriteString(")")
	if op.WithoutRowid {
		b.WriteString(" WITHOUT ROWID")
	}
	return b.String(), nil
}

func renderColumnDef(c crdt.CatalogColumn) (string, error) {
	var b strings.Builder
	b.WriteString(sqlitebridge.QuoteIdent(c.Name))
	if c.Type != "" {
		b.WriteString(" ")
		b.WriteString(c.Type)
	}
	if c.NotNull {
		b.WriteString(" NOT NULL")
	}
	if c.Default != "" {
		b.WriteString(" DEFAULT ")
		b.WriteString(c.Default)
	}
	// Reproduce the origin's declared collation so text comparisons,
	// ordering, and unique semantics match across replicas. BINARY is the
	// default and omitted.
	if name := c.Collation.Name(); name != "" {
		b.WriteString(" COLLATE ")
		b.WriteString(name)
	}
	return b.String(), nil
}

// opAlreadyAppliedInSQLite reports whether op's desired SQLite-side
// post-state is already in place. When true, applyCatalogStructural can
// skip the DDL and proceed straight to the metadata-side writes — this
// closes the crash window where a prior attempt committed SQLite but
// failed (or crashed) before the metadata tx, leaving the receiver
// stuck retrying a non-idempotent ALTER forever.
//
// Name-level checks only. Shape mismatches (e.g. an out-of-band ALTER
// that added the column with a different declared type) are treated as
// "applied" — we don't reconcile shape here; this is for syzy's own
// retry idempotency, not for repairing operator-introduced drift.
func opAlreadyAppliedInSQLite(op crdt.CatalogOp, app *sqlitebridge.Conn, cat catalog.TableResolver) (bool, error) {
	switch op.Kind {
	case crdt.OpCreateTable:
		// Resolve by identity, not by the name the op literally carried.
		// A later op — a sub-op of the same bundle, or a later schema
		// event — may have renamed or dropped this table since, and a
		// replay that judged the create by its original name would call
		// it missing and resurrect an intermediate object no other node
		// has (ReconcileSchemaToSQLite runs on every open).
		if tab, ok := cat.TableByID(op.TableID); ok {
			if tab.Dropped() {
				return true, nil
			}
			return sqlitebridge.ObjectExists(app, "table", tab.Name)
		}
		return sqlitebridge.ObjectExists(app, "table", op.TableName)

	case crdt.OpDropTable:
		tab, ok := cat.TableByID(op.TableID)
		if !ok {
			return true, nil
		}
		exists, err := sqlitebridge.ObjectExists(app, "table", tab.Name)
		return !exists, err

	case crdt.OpAddColumn:
		if len(op.Columns) != 1 {
			return false, nil
		}
		tab, ok := cat.TableByID(op.TableID)
		if !ok {
			return false, nil
		}
		return sqlitebridge.ColumnExists(app, tab.Name, op.Columns[0].Name)

	case crdt.OpDropColumn:
		tab, ok := cat.TableByID(op.TableID)
		if !ok {
			return true, nil
		}
		// The catalog has already tombstoned the column by the time this
		// precheck runs, so resolve its name through the history — the
		// active-only lookup would read every applied-in-catalog drop as
		// "already applied" and a restored pre-drop app.db would keep the
		// column forever. A legacy tombstone without a name is
		// unresolvable; treat as applied (the pre-history behavior).
		col, ok := tab.HistoricColumnByID(op.ColumnID)
		if !ok || col.Name == "" {
			return true, nil
		}
		exists, err := sqlitebridge.ColumnExists(app, tab.Name, col.Name)
		return !exists, err

	case crdt.OpRenameTable:
		return sqlitebridge.ObjectExists(app, "table", op.TableName)

	case crdt.OpRenameColumn:
		tab, ok := cat.TableByID(op.TableID)
		if !ok {
			return false, nil
		}
		return sqlitebridge.ColumnExists(app, tab.Name, op.ColumnName)

	case crdt.OpAddUniqueKey, crdt.OpDropUniqueKey:
		// UNIQUE arbitration goes through syzy_key (consulted by the
		// apply path), not through a SQLite-side UNIQUE INDEX, so the
		// receiver has no SQLite-side state to reconcile. Skip the
		// RawSQL replay rather than try to render a brittle idempotency
		// check for the originator's literal CREATE UNIQUE INDEX text.
		return true, nil

	case crdt.OpSetClockGroup:
		// Pure catalog-metadata mutation; no SQLite-side state.
		return true, nil

	case crdt.OpCreateIndex:
		return sqlitebridge.ObjectExists(app, "index", op.ObjectName)
	case crdt.OpDropIndex:
		exists, err := sqlitebridge.ObjectExists(app, "index", op.ObjectName)
		return !exists, err

	case crdt.OpCreateView:
		return sqlitebridge.ObjectExists(app, "view", op.ObjectName)
	case crdt.OpDropView:
		exists, err := sqlitebridge.ObjectExists(app, "view", op.ObjectName)
		return !exists, err

	case crdt.OpCreateVirtualTable:
		return sqlitebridge.ObjectExists(app, "table", op.ObjectName)
	case crdt.OpDropVirtualTable:
		exists, err := sqlitebridge.ObjectExists(app, "table", op.ObjectName)
		return !exists, err

	case crdt.OpCreateTrigger:
		return sqlitebridge.ObjectExists(app, "trigger", op.ObjectName)
	case crdt.OpDropTrigger:
		exists, err := sqlitebridge.ObjectExists(app, "trigger", op.ObjectName)
		return !exists, err

	case crdt.OpBundle:
		// applyCatalogStructural recurses into each sub-op, which runs
		// its own precheck. Returning false here keeps the SAVEPOINT/
		// RELEASE bookkeeping in place so a fresh-failure mid-bundle
		// still rolls back partial structural state.
		return false, nil
	}
	return false, fmt.Errorf("opAlreadyAppliedInSQLite: unsupported kind %v", op.Kind)
}

// structuralEffectMissing reports whether op's SQLite-side post-state is
// absent from app.db, i.e. a re-apply would actually change the schema. It
// is the inverse of opAlreadyAppliedInSQLite, with bundles folded over their
// sub-ops (a bundle is "missing" if any sub-op's effect is absent). Used by
// ReconcileSchemaToSQLite to decide whether an applied schema event needs
// repair (and to avoid logging a repair on a healthy node).
//
// A bundle's sub-ops are prechecked against the state the bundle ends in —
// the same view applyCatalogStructural uses — so an intermediate object a
// later sub-op renamed or dropped away does not read as missing forever.
func structuralEffectMissing(op crdt.CatalogOp, app *sqlitebridge.Conn, cat *catalog.Catalog) (bool, error) {
	if op.Kind == crdt.OpBundle {
		final := bundleFinalState(cat, op.SubOps)
		for _, sub := range op.SubOps {
			applied, err := opAlreadyAppliedInSQLite(sub, app, final)
			if err != nil {
				return false, err
			}
			if !applied {
				return true, nil
			}
		}
		return false, nil
	}
	applied, err := opAlreadyAppliedInSQLite(op, app, cat)
	if err != nil {
		return false, err
	}
	return !applied, nil
}

func pkColumnsFromOp(op crdt.CatalogOp) []string {
	var pk []string
	for _, k := range op.Keys {
		if k.KeyID != (crdt.KeyID{}) {
			continue
		}
		// Members are in ordinal order as encoded.
		for _, m := range k.Members {
			for _, c := range op.Columns {
				if c.ID == m.ColumnID {
					pk = append(pk, c.Name)
					break
				}
			}
		}
	}
	return pk
}
