package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	catalogpkg "github.com/wjordan/syzy/internal/sqlitecatalog"
)

// buildCatalogOps turns one committed transaction's DDL intent descriptors (§6)
// into typed CatalogOps, built from pg_catalog — never parsed from the audit
// SQL. It allocates a fresh stable ID for each created object (recording the
// OID⇄ID mapping in cat) and resolves existing/dropped objects through that
// map. Intents arrive in command (ordinal) order.
//
// CREATE TABLE … PRIMARY KEY emits the implicit PK index as its own
// ddl_command_end event; that index is already expressed in the table op's
// Keys, so the builder folds it (skips the intent) rather than emitting a stray
// CreateIndex.
//
// It mutates cat, so it runs only on the single orchestrator/capture goroutine
// (the increment-1 single-writer model). Increment C covers CREATE TABLE and
// DROP TABLE; other command tags return a clean admission error — the supported
// surface widens in later increments.
func buildCatalogOps(ctx context.Context, conn *pgx.Conn, cat *catalog, intents []ddlIntent) ([]crdt.CatalogOp, error) {
	var ops []crdt.CatalogOp
	for _, in := range intents {
		switch {
		case in.isDrop:
			if op, ok := buildDropOp(cat, in); ok {
				ops = append(ops, op)
			}
		case in.commandTag == "CREATE TABLE":
			if cat.byOID[in.objid] != nil {
				// Already created + appended: a post-recovery re-delivery of the
				// intent rows (the originator crashed between append and prune; the
				// table was rebound by the startup catch-up). Re-running would
				// AllocTableID a divergent id and re-append, so skip.
				continue
			}
			op, err := buildCreateTableOp(ctx, conn, cat, in)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		case in.commandTag == "ALTER TABLE":
			altered, err := buildAlterTableOps(ctx, conn, cat, in)
			if err != nil {
				return nil, err
			}
			ops = append(ops, altered...)
		case in.commandTag == "CREATE INDEX":
			// The implicit PK index is folded into its table's OpCreateTable; a
			// standalone CREATE UNIQUE INDEX is a §5 unique key (typed, arbitrated);
			// any other user secondary index ships as an opaque-SQL OpCreateIndex.
			primary, unique, relOID, err := indexClass(ctx, conn, in.objid)
			if err != nil {
				return nil, err
			}
			if primary {
				continue
			}
			if unique {
				ti := cat.byOID[relOID]
				if ti == nil {
					continue // unique index on an untracked table; skip like its DML
				}
				uqOps, err := diffUniqueKeys(ctx, conn, ti, cat.coordUnique)
				if err != nil {
					return nil, err
				}
				ops = append(ops, uqOps...)
				continue
			}
			idxOp, err := buildCreateIndexOp(ctx, conn, in.objid)
			if err != nil {
				return nil, err
			}
			ops = append(ops, idxOp)
		case in.commandTag == "CREATE VIEW":
			viewOp, err := buildCreateViewOp(ctx, conn, in)
			if err != nil {
				return nil, err
			}
			ops = append(ops, viewOp)
		case in.commandTag == "CREATE MATERIALIZED VIEW":
			// A matview materializes a node-local query result (like CREATE TABLE AS)
			// and cannot replicate; reject rather than ship a snapshot peers can't
			// reproduce.
			return nil, unsupportedDDLf("postgres: MATERIALIZED VIEW materializes a node-local query result and cannot replicate")
		case in.objectType == "sequence":
			// A serial/bigserial column's owned sequence is created (and OWNED BY'd)
			// as its own ddl_command_end event; it is folded into the table's
			// OpCreateTable — the column ships as a serial pseudo-type a follower
			// re-creates locally. A standalone user CREATE SEQUENCE is a later
			// increment.
			owned, err := isOwnedSequence(ctx, conn, in.objid)
			if err != nil {
				return nil, err
			}
			if owned {
				continue
			}
			return nil, unsupportedDDLf("postgres: DDL %q not yet supported (increment C)", in.commandTag)
		case in.commandTag == "CREATE TRIGGER", in.objectType == "trigger":
			// User triggers are intentionally not replicated. A trigger's EFFECTS
			// already converge — the originator's trigger fires, its row changes are
			// captured as DML and replicated, and applied writes run under replica
			// role so the trigger does NOT re-fire (no double-apply). Replicating the
			// trigger DEFINITION would also require replicating its function (not yet
			// supported) and rejecting ENABLE ALWAYS/REPLICA triggers (which WOULD
			// fire on applied DML and diverge). Until that lands, keep the trigger
			// local to the node that created it.
			return nil, unsupportedDDLf("postgres: user trigger %q is not replicated (keep it local; its row effects already replicate as DML)", in.objectIdentity)
		case in.commandTag == "CREATE FUNCTION", in.commandTag == "CREATE PROCEDURE", in.objectType == "function", in.objectType == "procedure":
			// Functions/procedures are not replicated: their determinism, language,
			// and side-effects are not yet modeled, and a function referenced by a
			// replicated DEFAULT/GENERATED expression must exist identically on every
			// node. Reject rather than risk a default that evaluates differently.
			return nil, unsupportedDDLf("postgres: %q is not replicated (function determinism/side-effects not yet modeled)", in.commandTag)
		default:
			return nil, unsupportedDDLf("postgres: DDL %q not yet supported (increment C)", in.commandTag)
		}
	}
	return ops, nil
}

// buildCreateTableOp introspects the just-created table by OID and builds the
// typed OpCreateTable, allocating a fresh stable TableID/ColumnIDs and recording
// the table in the OID⇄ID map.
//
// Build-from-live-catalog constraint: capture is post-commit, so this reads the
// *current* catalog, not the catalog as of the create txn's commit. If a later
// DDL txn drops or alters the table within the same capture-lag window, the
// introspection here sees the post-mutation state (or no rows at all). The live
// path keeps capture within ~one txn of head, and the DDL lease + ddl_command_start
// gate (increment E) make this exact — a node's own pending DDL is appended
// before its next DDL txn proceeds. DropTable does not read the catalog (it
// resolves the prior mapping), so only the create path is timing-sensitive.
func buildCreateTableOp(ctx context.Context, conn *pgx.Conn, cat *catalog, in ddlIntent) (crdt.CatalogOp, error) {
	var schema, name string
	if err := conn.QueryRow(ctx, `
		SELECT ns.nspname, c.relname
		FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace
		WHERE c.oid = $1`, in.objid).Scan(&schema, &name); err != nil {
		return crdt.CatalogOp{}, fmt.Errorf("create table oid %d: %w", in.objid, err)
	}
	pgcols, err := introspectColumns(ctx, conn, in.objid)
	if err != nil {
		return crdt.CatalogOp{}, err
	}
	if err := rejectUnreplicable(schema, name, pgcols); err != nil {
		return crdt.CatalogOp{}, err
	}

	ti := &tableInfo{schema: schema, name: name, oid: in.objid, tid: catalogpkg.AllocTableID(), byName: map[string]*colInfo{}}
	var cols []crdt.CatalogColumn
	var pks []pkEntry
	for _, pc := range pgcols {
		cid := catalogpkg.AllocColumnID()
		cols = append(cols, pc.catalogColumn(cid, pc.attnum-1))
		ci := pc.colInfo(cid)
		ti.cols = append(ti.cols, ci)
		ti.byName[ci.name] = ci
		if pc.pkpos > 0 {
			pks = append(pks, pkEntry{pos: pc.pkpos, ci: ci})
		}
	}
	if len(pks) == 0 {
		return crdt.CatalogOp{}, unsupportedDDLf("postgres: CREATE TABLE %s.%s: replicated tables require a PRIMARY KEY", schema, name)
	}
	pkMembers := buildPK(ti, pks)
	keys := []crdt.CatalogKey{{KeyID: crdt.KeyID{}, Members: pkMembers}}
	// Inline UNIQUE constraints declared at CREATE TABLE (§5): each ships as a
	// distinct non-PK key in op.Keys; a follower binds it in-catalog only.
	uqKeys, err := captureUniqueKeys(ctx, conn, ti, cat.coordUnique)
	if err != nil {
		return crdt.CatalogOp{}, err
	}
	keys = append(keys, uqKeys...)
	cat.addTable(ti)
	return crdt.CatalogOp{
		Kind:      crdt.OpCreateTable,
		TableID:   ti.tid,
		TableName: name,
		Columns:   cols,
		Keys:      keys,
	}, nil
}

// buildAlterTableOps reconstructs what one ALTER TABLE command changed by
// diffing the table's current pg_attribute (and name) against the prior
// tableInfo — risk #2: ddl_command_end reports only objid + command_tag, never
// the sub-object, so the change must be recovered from the catalog. attnum is
// the rename-stable diff key: the same attnum under a new name is a RENAME
// COLUMN (the stable id is preserved); a vanished attnum is a DROP; a new attnum
// is an ADD. One ALTER can yield several ops (e.g. ADD COLUMN a, b); they are
// emitted rename→drop→add so a follower applies them without transient name or
// ordinal conflicts.
//
// Any column attribute change other than add/drop/rename (ALTER TYPE, SET/DROP
// DEFAULT/NOT NULL, GENERATED) is not yet expressible as a typed op, so it is
// rejected here with a clean error rather than silently dropped (which would
// diverge). Because capture is post-commit the ALTER has already committed
// locally, so this halts capture (schema-unhealthy) — the proper admission-time
// rejection (the ddl_command_start gate RAISEs before commit) is increment G.
//
// Beyond columns + relname this also diffs the table's non-PK unique keys (§5,
// diffUniqueKeys) so ADD/DROP CONSTRAINT UNIQUE replicates as OpAddUniqueKey /
// OpDropUniqueKey. A CHECK / FOREIGN KEY constraint changes no column, no name,
// and no unique key, so it still produces no op and no error (silently not
// replicated); closing that needs increment G's admission gate (RAISE on
// unsupported forms before commit).
func buildAlterTableOps(ctx context.Context, conn *pgx.Conn, cat *catalog, in ddlIntent) ([]crdt.CatalogOp, error) {
	ti := cat.byOID[in.objid]
	if ti == nil {
		// Untracked table: the event triggers fire for DDL on every table (the
		// publication is FOR ALL TABLES), so skip it exactly as capture skips DML
		// on a table not in the catalog — erroring would halt capture on an
		// unrelated table's ALTER.
		return nil, nil
	}
	var ops []crdt.CatalogOp

	// Table rename: same OID, new relname.
	var relname string
	if err := conn.QueryRow(ctx, `SELECT relname FROM pg_class WHERE oid = $1`, in.objid).Scan(&relname); err != nil {
		return nil, fmt.Errorf("postgres: ALTER TABLE oid %d: %w", in.objid, err)
	}
	if relname != ti.name {
		ops = append(ops, crdt.CatalogOp{Kind: crdt.OpRenameTable, TableID: ti.tid, TableName: relname})
		ti.name = relname
	}

	// Column diff, keyed by attnum. The post-ALTER column set is rebuilt fresh
	// (cols + byName) at the end rather than mutated in place, so a rename onto a
	// just-dropped column's name (e.g. "DROP a; RENAME b TO a") cannot corrupt
	// byName by ordering.
	pgcols, err := introspectColumns(ctx, conn, in.objid)
	if err != nil {
		return nil, err
	}
	prior := make(map[int]*colInfo, len(ti.cols))
	for _, ci := range ti.cols {
		prior[ci.attnum] = ci
	}
	live := make(map[int]bool, len(pgcols))

	var renames, adds []crdt.CatalogOp
	var newCols []*colInfo
	for _, pc := range pgcols {
		live[pc.attnum] = true
		ci, existed := prior[pc.attnum]
		if !existed {
			// An added serial/identity column mints a value PER EXISTING ROW from
			// the local sequence at ADD time; each node would generate different
			// values (its own partitioned sequence), so the new column diverges.
			// Reject it (CREATE TABLE is safe — no pre-existing rows).
			if pc.serialType != "" || pc.identity != 0 {
				return nil, unsupportedDDLf("postgres: ALTER TABLE %s: ADD COLUMN %q is auto-increment (serial/identity); it mints divergent per-node values for existing rows and is not supported", ti.name, pc.name)
			}
			cid := catalogpkg.AllocColumnID()
			newCols = append(newCols, pc.colInfo(cid))
			adds = append(adds, crdt.CatalogOp{
				Kind:    crdt.OpAddColumn,
				TableID: ti.tid,
				Columns: []crdt.CatalogColumn{pc.catalogColumn(cid, pc.attnum-1)},
			})
			continue
		}
		if pc.attrsDiffer(ci) {
			return nil, unsupportedDDLf("postgres: ALTER TABLE %s: column %q attribute change (type/default/not-null/generated/pk) is not yet representable as a typed op (increment G)", ti.name, pc.name)
		}
		if ci.name != pc.name {
			renames = append(renames, crdt.CatalogOp{
				Kind: crdt.OpRenameColumn, TableID: ti.tid, ColumnID: ci.cid, ColumnName: pc.name,
			})
			ci.name = pc.name // the shared pointer also updates ti.pk
		}
		newCols = append(newCols, ci)
	}

	// DROP COLUMN: a prior attnum no longer live.
	var drops []crdt.CatalogOp
	for _, ci := range ti.cols {
		if live[ci.attnum] {
			continue
		}
		if ci.isPK {
			return nil, unsupportedDDLf("postgres: ALTER TABLE %s: dropping PRIMARY KEY column %q changes row identity and is not supported", ti.name, ci.name)
		}
		drops = append(drops, crdt.CatalogOp{Kind: crdt.OpDropColumn, TableID: ti.tid, ColumnID: ci.cid})
	}

	// Commit the post-ALTER column set and rebuild the name index from it.
	ti.cols = newCols
	ti.byName = make(map[string]*colInfo, len(newCols))
	for _, ci := range newCols {
		ti.byName[ci.name] = ci
	}

	// Unique-key diff (§5): introspect the table's live non-PK unique indexes
	// and reconcile against the cached set. Runs after the column rebuild so a
	// key on a column added in the same ALTER resolves. Emitted after column
	// adds for the same reason.
	uqOps, err := diffUniqueKeys(ctx, conn, ti, cat.coordUnique)
	if err != nil {
		return nil, err
	}

	ops = append(ops, renames...)
	ops = append(ops, drops...)
	ops = append(ops, adds...)
	ops = append(ops, uqOps...)
	return ops, nil
}

// buildDropOp resolves a dropped object's stable ID through the OID⇄ID map (the
// object is already gone from pg_catalog) and trims it from the map. ok is false
// for an object that was never tracked (e.g. a non-replicated table), which the
// caller skips. Increment C handles DROP TABLE; other dropped object types are
// not yet emitted.
func buildDropOp(cat *catalog, in ddlIntent) (crdt.CatalogOp, bool) {
	switch in.objectType {
	case "table":
		ti := cat.byOID[in.objid]
		if ti == nil {
			return crdt.CatalogOp{}, false
		}
		cat.dropTable(ti)
		return crdt.CatalogOp{Kind: crdt.OpDropTable, TableID: ti.tid, TableName: ti.name}, true
	case "index":
		// A DROP INDEX on a tracked UNIQUE index drops the §5 key it backs (the
		// follower holds the key in-catalog, no physical index, so OpDropIndex
		// would no-op there and leave a stale key — a divergence). Map the index
		// OID back to its key. A constraint-backed index is dropped with its
		// constraint (the ALTER diff handles that), so this matches the rare
		// standalone CREATE UNIQUE INDEX case. Any other index is opaque by name,
		// replayed as DROP INDEX IF EXISTS.
		if ti, uk := cat.uniqueKeyByIndexOID(in.objid); uk != nil {
			ti.removeUniqueKey(uk.keyID)
			return crdt.CatalogOp{Kind: crdt.OpDropUniqueKey, TableID: ti.tid, KeyID: uk.keyID}, true
		}
		return crdt.CatalogOp{Kind: crdt.OpDropIndex, ObjectName: in.objectIdentity}, true
	case "view":
		return crdt.CatalogOp{Kind: crdt.OpDropView, ObjectName: in.objectIdentity}, true
	}
	return crdt.CatalogOp{}, false
}

// buildCreateViewOp builds an opaque-SQL OpCreateView from the live view
// definition (pg_get_viewdef), replayed on followers as CREATE OR REPLACE VIEW
// (idempotent). A view is a pure projection — no stored rows, no convergence
// concern — and the schema log's ordering guarantees its underlying tables are
// created on the follower first. (Materialized views are rejected in the
// dispatch above: their snapshot is node-local data.)
func buildCreateViewOp(ctx context.Context, conn *pgx.Conn, in ddlIntent) (crdt.CatalogOp, error) {
	var schema, name, def string
	if err := conn.QueryRow(ctx, `
		SELECT ns.nspname, c.relname, pg_get_viewdef(c.oid)
		FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace
		WHERE c.oid = $1 AND c.relkind = 'v'`, in.objid).Scan(&schema, &name, &def); err != nil {
		return crdt.CatalogOp{}, fmt.Errorf("view def oid %d: %w", in.objid, err)
	}
	qual := quoteIdent(schema) + "." + quoteIdent(name)
	return crdt.CatalogOp{Kind: crdt.OpCreateView, ObjectName: qual, RawSQL: "CREATE OR REPLACE VIEW " + qual + " AS " + def}, nil
}

// buildCreateIndexOp builds an opaque-SQL OpCreateIndex from the live index
// definition (pg_get_indexdef — canonical, fully schema-qualified), replayed
// verbatim on followers. Only non-unique secondary indexes reach here; UNIQUE
// indexes are routed to the §5 unique-key path by the buildCatalogOps dispatch.
func buildCreateIndexOp(ctx context.Context, conn *pgx.Conn, oid uint32) (crdt.CatalogOp, error) {
	var def, name string
	if err := conn.QueryRow(ctx, `
		SELECT pg_get_indexdef(i.indexrelid), c.relname
		FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE i.indexrelid = $1`, oid).Scan(&def, &name); err != nil {
		return crdt.CatalogOp{}, fmt.Errorf("index def oid %d: %w", oid, err)
	}
	return crdt.CatalogOp{Kind: crdt.OpCreateIndex, ObjectName: name, RawSQL: def}, nil
}

// isOwnedSequence reports whether the sequence OID is owned by a table column —
// the serial/bigserial pattern (pg_depend deptype 'a', auto) or a GENERATED … AS
// IDENTITY sequence (deptype 'i', internal) — rather than a standalone user
// CREATE SEQUENCE. Either is folded into the table's OpCreateTable. The owning
// dependency exists post-commit (when capture introspects), so this is decided
// the same way regardless of the order the implicit sequence/table/ownership
// commands fired.
func isOwnedSequence(ctx context.Context, conn *pgx.Conn, oid uint32) (bool, error) {
	var owned bool
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1 FROM pg_depend
		  WHERE classid = 'pg_class'::regclass AND objid = $1
		    AND refclassid = 'pg_class'::regclass AND deptype IN ('a', 'i')
		)`, oid).Scan(&owned); err != nil {
		return false, fmt.Errorf("sequence oid %d: %w", oid, err)
	}
	return owned, nil
}

// indexClass reports an index's PK / unique status and its owning table OID in
// one query, so the CREATE INDEX dispatch can fold the PK index, route a UNIQUE
// index to the §5 unique-key path, and ship anything else as opaque SQL.
func indexClass(ctx context.Context, conn *pgx.Conn, oid uint32) (primary, unique bool, relOID uint32, err error) {
	if err = conn.QueryRow(ctx, `
		SELECT indisprimary, indisunique, indrelid FROM pg_index WHERE indexrelid = $1`,
		oid).Scan(&primary, &unique, &relOID); err != nil {
		return false, false, 0, fmt.Errorf("index oid %d: %w", oid, err)
	}
	return primary, unique, relOID, nil
}

// liveUniqueKey is one of ti's live non-PK unique indexes, resolved to its
// member columns (tuple order) and the backing index OID.
type liveUniqueKey struct {
	cols        []*colInfo
	indexOID    uint32
	coordinated bool
}

// liveUniqueKeyCols introspects ti's live non-PK unique indexes and resolves each
// to its member colInfos in index (tuple) order. Shapes loser-null arbitration
// cannot converge are rejected (errUnsupportedDDL) rather than silently filtered —
// a silent skip would leave the originator's physical constraint with no
// replicated counterpart, diverging (or 23505-ing on apply):
//   - partial (WHERE) or expression unique index — not a plain column tuple, and
//     a predicate's truth can differ across replicas;
//   - NULLS NOT DISTINCT — PG enforces NULL=NULL physically, but arbitration skips
//     NULL tuples, so the originator would 23505 on values followers accept;
//   - a NOT NULL member in a key that is not fully NOT NULL — a loser converges
//     by nulling its key columns, which a NOT NULL column forbids. A key whose
//     members are ALL NOT NULL is instead **coordinated** when the engine has
//     coordination enabled (coordOK): reserved before commit, physically
//     indexed on no node, never loser-nulled (coordinated.go). Without
//     coordination it stays rejected.
func liveUniqueKeyCols(ctx context.Context, conn *pgx.Conn, ti *tableInfo, coordOK bool) ([]liveUniqueKey, error) {
	byAttnum := make(map[int]*colInfo, len(ti.cols))
	for _, ci := range ti.cols {
		byAttnum[ci.attnum] = ci
	}
	rows, err := conn.Query(ctx, `
		SELECT i.indexrelid,
		       bool_or(i.indpred IS NOT NULL) AS partial,
		       bool_or(i.indexprs IS NOT NULL) AS hasexpr,
		       bool_or(i.indnullsnotdistinct) AS nulls_not_distinct,
		       array_agg(k.attnum::int ORDER BY k.ord) AS attnums
		FROM pg_index i
		JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
		WHERE i.indrelid = $1 AND i.indisunique AND NOT i.indisprimary
		GROUP BY i.indexrelid
		ORDER BY i.indexrelid`, ti.oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []liveUniqueKey
	for rows.Next() {
		var indexOID uint32
		var partial, hasExpr, nullsNotDistinct bool
		var attnums []int32
		if err := rows.Scan(&indexOID, &partial, &hasExpr, &nullsNotDistinct, &attnums); err != nil {
			return nil, err
		}
		switch {
		case partial:
			return nil, unsupportedDDLf("postgres: %s: partial UNIQUE index (WHERE) cannot replicate — predicate truth varies across replicas", ti.name)
		case hasExpr:
			return nil, unsupportedDDLf("postgres: %s: expression UNIQUE index cannot replicate — not a plain column tuple", ti.name)
		case nullsNotDistinct:
			return nil, unsupportedDDLf("postgres: %s: UNIQUE NULLS NOT DISTINCT is not supported — arbitration treats NULL tuples as non-colliding", ti.name)
		}
		cols := make([]*colInfo, 0, len(attnums))
		notNull := 0
		for _, an := range attnums {
			ci := byAttnum[int(an)]
			if ci == nil {
				return nil, unsupportedDDLf("postgres: %s: UNIQUE key references column attnum %d not in the catalog", ti.name, an)
			}
			if ci.notNull {
				notNull++
			}
			cols = append(cols, ci)
		}
		coordinated := false
		switch {
		case notNull == len(cols) && coordOK:
			coordinated = true // CP key: reserved before commit, indexed nowhere
		case notNull == len(cols):
			return nil, unsupportedDDLf("postgres: %s: NOT NULL UNIQUE requires coordinated uniqueness (run with an object-store bucket so the cluster's key registry is available)", ti.name)
		case notNull > 0:
			return nil, unsupportedDDLf("postgres: %s: UNIQUE mixing NOT NULL and nullable members has no convergent loser state and is rejected", ti.name)
		}
		out = append(out, liveUniqueKey{cols: cols, indexOID: indexOID, coordinated: coordinated})
	}
	return out, rows.Err()
}

// liveUniqueIndexOIDs maps each live non-PK unique index's member-column
// signature to its index OID — used at restore to rebind uniqueKey.indexOID
// (which the engine-neutral metadata catalog does not store) so a later DROP
// INDEX on the originator still resolves to its key. Best-effort: it skips the
// shape checks (the keys were already accepted at capture) and returns an empty
// map on a follower, which has no physical unique index.
func liveUniqueIndexOIDs(ctx context.Context, conn *pgx.Conn, ti *tableInfo) (map[string]uint32, error) {
	byAttnum := make(map[int]*colInfo, len(ti.cols))
	for _, ci := range ti.cols {
		byAttnum[ci.attnum] = ci
	}
	rows, err := conn.Query(ctx, `
		SELECT i.indexrelid, array_agg(k.attnum::int ORDER BY k.ord)
		FROM pg_index i
		JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
		WHERE i.indrelid = $1 AND i.indisunique AND NOT i.indisprimary
		  AND i.indpred IS NULL AND i.indexprs IS NULL
		GROUP BY i.indexrelid`, ti.oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]uint32{}
	for rows.Next() {
		var indexOID uint32
		var attnums []int32
		if err := rows.Scan(&indexOID, &attnums); err != nil {
			return nil, err
		}
		cols := make([]*colInfo, 0, len(attnums))
		ok := true
		for _, an := range attnums {
			ci := byAttnum[int(an)]
			if ci == nil {
				ok = false
				break
			}
			cols = append(cols, ci)
		}
		if ok {
			out[uniqueKeySig(cols)] = indexOID
		}
	}
	return out, rows.Err()
}

// captureUniqueKeys allocates a fresh KeyID for each of ti's live non-PK unique
// keys, recording them in ti.uniqueKeys and returning the matching CatalogKeys
// (for an OpCreateTable's Keys list). Used only at table-create time.
func captureUniqueKeys(ctx context.Context, conn *pgx.Conn, ti *tableInfo, coordOK bool) ([]crdt.CatalogKey, error) {
	live, err := liveUniqueKeyCols(ctx, conn, ti, coordOK)
	if err != nil {
		return nil, err
	}
	var keys []crdt.CatalogKey
	for _, lk := range live {
		kid := allocKeyID()
		keys = append(keys, crdt.CatalogKey{KeyID: kid, Members: keyMembers(lk.cols), Coordinated: lk.coordinated})
		ti.uniqueKeys = append(ti.uniqueKeys, &uniqueKey{keyID: kid, cols: lk.cols, indexOID: lk.indexOID, coordinated: lk.coordinated})
	}
	return keys, nil
}

// diffUniqueKeys reconciles ti's live non-PK unique keys against the cached set,
// emitting OpAddUniqueKey for a key present live but not cached (allocating a
// fresh KeyID) and OpDropUniqueKey for one cached but no longer live. Keys are
// matched by their member-column signature (rename-stable: signatures are over
// the stable ColumnIDs, not names). It mutates ti.uniqueKeys to the live set, so
// a re-delivered intent (post-crash) is an idempotent no-op. Shared by the ALTER
// diff and the CREATE UNIQUE INDEX path.
func diffUniqueKeys(ctx context.Context, conn *pgx.Conn, ti *tableInfo, coordOK bool) ([]crdt.CatalogOp, error) {
	live, err := liveUniqueKeyCols(ctx, conn, ti, coordOK)
	if err != nil {
		return nil, err
	}
	liveSigs := make(map[string]struct{}, len(live))
	for _, lk := range live {
		liveSigs[uniqueKeySig(lk.cols)] = struct{}{}
	}
	have := make(map[string]struct{}, len(ti.uniqueKeys))
	var ops []crdt.CatalogOp
	var kept []*uniqueKey
	for _, uk := range ti.uniqueKeys {
		sig := uniqueKeySig(uk.cols)
		if _, ok := liveSigs[sig]; ok {
			have[sig] = struct{}{}
			kept = append(kept, uk)
			continue
		}
		ops = append(ops, crdt.CatalogOp{Kind: crdt.OpDropUniqueKey, TableID: ti.tid, KeyID: uk.keyID})
	}
	for _, lk := range live {
		sig := uniqueKeySig(lk.cols)
		if _, ok := have[sig]; ok {
			continue
		}
		have[sig] = struct{}{}
		kid := allocKeyID()
		ops = append(ops, crdt.CatalogOp{
			Kind: crdt.OpAddUniqueKey, TableID: ti.tid, KeyID: kid,
			Keys: []crdt.CatalogKey{{KeyID: kid, Members: keyMembers(lk.cols), Coordinated: lk.coordinated}},
		})
		kept = append(kept, &uniqueKey{keyID: kid, cols: lk.cols, indexOID: lk.indexOID, coordinated: lk.coordinated})
	}
	ti.uniqueKeys = kept
	return ops, nil
}

// keyMembers renders member columns as CatalogKeyMembers in tuple order.
func keyMembers(cols []*colInfo) []crdt.CatalogKeyMember {
	m := make([]crdt.CatalogKeyMember, len(cols))
	for i, ci := range cols {
		m[i] = crdt.CatalogKeyMember{ColumnID: ci.cid, Ordinal: i}
	}
	return m
}

// uniqueKeySig is the order-sensitive signature of a key's member columns over
// their stable ColumnIDs — the comparison key for diffUniqueKeys.
func uniqueKeySig(cols []*colInfo) string {
	var b strings.Builder
	for _, ci := range cols {
		b.Write(ci.cid[:])
	}
	return b.String()
}

// allocKeyID returns a fresh non-zero KeyID (the all-zero KeyID is reserved for
// the PK). KeyID and ColumnID are both 16-byte, so a column id reuse is safe.
func allocKeyID() crdt.KeyID {
	for {
		col := catalogpkg.AllocColumnID()
		var id crdt.KeyID
		copy(id[:], col[:])
		if id != (crdt.KeyID{}) {
			return id
		}
	}
}

// pkEntry pairs a PK column with its 1-based key position (pg_index.indkey
// ordinality), so the PK can be ordered by key position before it is committed
// to ti.pk or shipped as CatalogKeyMembers.
type pkEntry struct {
	pos int
	ci  *colInfo
}

// buildPK orders pks by key position (not attnum) and appends them to ti.pk in
// that order, returning the matching CatalogKeyMembers. Key order — not attnum
// order — is canonical so the PKBlob byte order agrees on the originator and
// every follower (which reconstructs ti.pk from Keys.Members.Ordinal) even when
// the PK is declared out of attnum order. Shared by the bootstrap introspect
// and the DDL create-table builder so both produce an identical PK.
func buildPK(ti *tableInfo, pks []pkEntry) []crdt.CatalogKeyMember {
	sort.Slice(pks, func(i, j int) bool { return pks[i].pos < pks[j].pos })
	members := make([]crdt.CatalogKeyMember, len(pks))
	for i, p := range pks {
		ti.pk = append(ti.pk, p.ci)
		members[i] = crdt.CatalogKeyMember{ColumnID: p.ci.cid, Ordinal: p.pos - 1}
	}
	return members
}

// pgColumn is one row of a relation's replicated column state, read from
// pg_catalog. It is the source both for the CatalogColumn shipped in an op and
// for the colInfo cached in the OID⇄ID map.
type pgColumn struct {
	attnum     int
	name       string
	typeName   string // format_type(atttypid, atttypmod)
	notNull    bool
	def        string // pg_get_expr(adbin) default, "" for none
	generated  bool
	pkpos      int    // 1-based PK key position, 0 if not in the PK
	serialType string // "bigserial"/"serial"/"smallserial" if backed by an OWNED sequence; else ""
	identity   uint8  // attidentity: 0 (none), 'a' (GENERATED ALWAYS), 'd' (GENERATED BY DEFAULT)
}

// introspectColumns reads a relation's live, non-dropped user columns (attnum
// order) by OID. Shared by introspectCatalog (bootstrap), buildCreateTableOp,
// and the ALTER diff so all three see identical column shape.
func introspectColumns(ctx context.Context, conn *pgx.Conn, oid uint32) ([]pgColumn, error) {
	rows, err := conn.Query(ctx, `
		SELECT a.attnum, a.attname, format_type(a.atttypid, a.atttypmod),
		       a.attnotnull,
		       COALESCE(pg_get_expr(d.adbin, d.adrelid), '') AS default_expr,
		       a.attgenerated <> '' AS generated,
		       COALESCE(k.pkpos, 0) AS pkpos,
		       EXISTS (
		         SELECT 1 FROM pg_depend dep
		         JOIN pg_class s ON s.oid = dep.objid AND s.relkind = 'S'
		         WHERE dep.refobjid = a.attrelid AND dep.refobjsubid = a.attnum
		           AND dep.deptype = 'a'
		       ) AS owns_seq,
		       CASE WHEN a.attidentity IN ('a','d') THEN a.attidentity::text ELSE '' END AS identity
		FROM pg_attribute a
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		LEFT JOIN (
		  SELECT i.indrelid, x.attnum, x.ord AS pkpos
		  FROM pg_index i, unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
		  WHERE i.indisprimary
		) k ON k.indrelid = a.attrelid AND k.attnum = a.attnum
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pgColumn
	for rows.Next() {
		var c pgColumn
		var ownsSeq bool
		var ident string
		if err := rows.Scan(&c.attnum, &c.name, &c.typeName, &c.notNull, &c.def, &c.generated, &c.pkpos, &ownsSeq, &ident); err != nil {
			return nil, err
		}
		c.serialType = serialTypeFor(c.typeName, ownsSeq && strings.HasPrefix(c.def, "nextval("))
		if ident != "" {
			c.identity = ident[0]
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// serialTypeFor maps a serial integer column to the pseudo-type a follower
// re-creates so it gets its own owned sequence to partition. "serial" means BOTH
// a nextval default AND an OWNED sequence (deptype 'a', the serial/bigserial
// pattern — not IDENTITY, whose dependency is 'i'): a column that merely owns a
// sequence (CREATE SEQUENCE … OWNED BY) with no nextval default is NOT serial,
// and a follower must not be handed a default the originator lacks. A serial on a
// non-integer column (exotic) is left as a plain column with its captured default.
// rejectUnreplicable rejects a column shape that cannot converge in multi-master
// regardless of how the table was created: a GENERATED ALWAYS AS IDENTITY column
// that is NOT the primary key. Its value is minted from this node's own
// (partitioned) sequence on every insert and a GENERATED ALWAYS column cannot be
// UPDATEd, so two nodes inserting the same PK concurrently each keep their own
// locally-minted value — a silent divergence apply cannot repair. (A PK identity
// converges by row identity; a serial / BY DEFAULT identity is updatable, so the
// LWW winner's value is written on conflict.)
func rejectUnreplicable(schema, name string, pgcols []pgColumn) error {
	for _, pc := range pgcols {
		if pc.identity == 'a' && pc.pkpos == 0 {
			return unsupportedDDLf("postgres: %s.%s: column %q is GENERATED ALWAYS AS IDENTITY on a non-primary-key column; its values are minted per node and cannot converge in multi-master", schema, name, pc.name)
		}
	}
	return nil
}

func serialTypeFor(typeName string, serial bool) string {
	if !serial {
		return ""
	}
	switch typeName {
	case "bigint":
		return "bigserial"
	case "integer":
		return "serial"
	case "smallint":
		return "smallserial"
	default:
		return ""
	}
}

// catalogColumn renders the column as a CatalogColumn with the given allocated
// id and declared ordinal.
func (pc pgColumn) catalogColumn(id crdt.ColumnID, ordinal int) crdt.CatalogColumn {
	// A serial column ships as its pseudo-type (bigserial/…) with no default: the
	// pseudo-type implies the owned sequence + nextval default, which a follower
	// re-creates locally (its own sequence, then partitioned to its ordinal). The
	// captured nextval default names the originator's sequence and must not cross.
	typeName, def := pc.typeName, pc.def
	if pc.serialType != "" {
		typeName, def = pc.serialType, ""
	}
	// An identity column travels as its DDL syntax appended to the declared
	// type — the engine-neutral CatalogColumn has no identity attribute, and
	// like the serial pseudo-type this is auto-increment shape riding Type.
	// pgColumnType passes the composite through verbatim, so a follower's
	// CREATE re-creates the identity (its own internal sequence, then
	// partitioned to its ordinal) and its introspection recovers attidentity
	// locally for the OVERRIDING SYSTEM VALUE apply path.
	switch pc.identity {
	case 'a':
		typeName += " GENERATED ALWAYS AS IDENTITY"
	case 'd':
		typeName += " GENERATED BY DEFAULT AS IDENTITY"
	}
	return crdt.CatalogColumn{
		ID:         id,
		Name:       pc.name,
		Ordinal:    ordinal,
		Type:       typeName,
		NotNull:    pc.notNull,
		Default:    def,
		IsPK:       pc.pkpos > 0,
		PKPos:      pc.pkpos,
		ClockGroup: metadata.ClockGroupRow,
		Generated:  pc.generated,
	}
}

// colInfo caches the column in the OID⇄ID map under the given id.
func (pc pgColumn) colInfo(id crdt.ColumnID) *colInfo {
	return &colInfo{
		name:      pc.name,
		typeName:  pc.typeName,
		cid:       id,
		isPK:      pc.pkpos > 0,
		attnum:    pc.attnum,
		notNull:   pc.notNull,
		def:       pc.def,
		generated: pc.generated,
		identity:  pc.identity,
	}
}

// attrsDiffer reports whether the live column carries a replicated attribute
// (type, not-null, default, generated, PK membership) that differs from the
// cached ci — i.e. a change beyond a pure rename that the ALTER diff cannot yet
// express as a typed op.
func (pc pgColumn) attrsDiffer(ci *colInfo) bool {
	return pc.typeName != ci.typeName ||
		pc.notNull != ci.notNull ||
		pc.def != ci.def ||
		pc.generated != ci.generated ||
		pc.identity != ci.identity ||
		(pc.pkpos > 0) != ci.isPK
}
