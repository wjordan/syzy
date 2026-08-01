package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// applyCatalogOp executes the structural change described by op on conn — the
// apply session (session_replication_role=replica + an origin tag), so the
// executed DDL fires no ENABLE ORIGIN intent trigger and its commit carries an
// origin the capture slot's origin='none' filter drops — and updates the OID⇄ID
// map so this node resolves its local relation OID to the cluster's allocated
// stable id. It is the inverse of buildCatalogOps: the originator allocates the
// id and ships it inside the op; every follower replays the op and binds that
// same id to its own local OID. Runs only on the single orchestrator goroutine.
//
// ordinal is this node's id slice (§6); a follower partitions a freshly-applied
// serial/bigserial PK sequence to it (0 disables, matching the bootstrap path).
//
// Supported operations include table and column changes, indexes, views,
// unique keys, clock groups, bundles, and follower-side sequence partitioning.
func applyCatalogOp(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp, ordinal uint16) error {
	// Partial (WHERE-predicate) unique keys are a SQLite-originated shape this
	// engine cannot enforce (its arbitration is over total keys). Refusing the
	// op halts schema-unhealthy — silently recording the key would let local
	// writes break the cluster's uniqueness guarantee. Coordinated total keys
	// ARE enforced (reserve-before-commit, coordinated.go) — but only when
	// this node runs with coordination enabled.
	for _, k := range op.Keys {
		if k.Predicate.Root != nil {
			return unsupportedDDLf("postgres: %s: partial unique key cannot be enforced by this engine", op.TableName)
		}
		if k.Coordinated && !cat.coordUnique {
			return unsupportedDDLf("postgres: %s: coordinated unique key requires this node to run with coordinated uniqueness enabled", op.TableName)
		}
	}
	switch op.Kind {
	case crdt.OpBundle:
		for _, sub := range op.SubOps {
			if err := applyCatalogOp(ctx, conn, cat, sub, ordinal); err != nil {
				return err
			}
		}
		return nil
	case crdt.OpCreateTable:
		return applyCreateTable(ctx, conn, cat, op, ordinal)
	case crdt.OpAddColumn:
		return applyAddColumn(ctx, conn, cat, op)
	case crdt.OpAlterColumn:
		return applyAlterColumn(ctx, conn, cat, op)
	case crdt.OpDropColumn:
		return applyDropColumn(ctx, conn, cat, op)
	case crdt.OpRenameColumn:
		return applyRenameColumn(ctx, conn, cat, op)
	case crdt.OpRenameTable:
		return applyRenameTable(ctx, conn, cat, op)
	case crdt.OpDropTable:
		return applyDropTable(ctx, conn, cat, op)
	case crdt.OpCreateIndex:
		return applyCreateIndex(ctx, conn, op)
	case crdt.OpDropIndex:
		return execDDLApply(ctx, conn, "DROP INDEX IF EXISTS "+op.ObjectName)
	case crdt.OpCreateView:
		return execDDLApply(ctx, conn, op.RawSQL) // CREATE OR REPLACE VIEW — idempotent
	case crdt.OpDropView:
		return execDDLApply(ctx, conn, "DROP VIEW IF EXISTS "+op.ObjectName)
	case crdt.OpAddUniqueKey:
		return applyAddUniqueKey(ctx, conn, cat, op)
	case crdt.OpDropUniqueKey:
		return applyDropUniqueKey(ctx, conn, cat, op)
	case crdt.OpSetClockGroup:
		return applySetClockGroup(ctx, conn, cat, op)
	default:
		return fmt.Errorf("postgres: apply catalog op: unsupported kind %v", op.Kind)
	}
}

// applyCreateIndex replays an opaque-SQL OpCreateIndex (a non-unique secondary
// index built from pg_get_indexdef). pg_get_indexdef emits no IF NOT EXISTS, so
// inject it: a crash-mid-batch replay — or a follower that already built the
// index — is then an idempotent no-op.
func applyCreateIndex(ctx context.Context, conn *pgx.Conn, op crdt.CatalogOp) error {
	sql := op.RawSQL
	const prefix = "CREATE INDEX "
	if strings.HasPrefix(sql, prefix) {
		sql = "CREATE INDEX IF NOT EXISTS " + sql[len(prefix):]
	}
	return execDDLApply(ctx, conn, sql)
}

// appliedSchema is the schema replicated DDL lands in. The neutral CatalogOp
// carries only the bare table name (cross-engine; SQLite has no schemas), so
// the adapter applies to public — matching the bootstrap convention. Multi-
// schema replication is a later concern.
const appliedSchema = "public"

func applyCreateTable(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp, ordinal uint16) error {
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(quoteIdent(appliedSchema) + "." + quoteIdent(op.TableName))
	b.WriteString(" (")
	for i, c := range op.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(renderPGColumnDef(c))
	}
	if pk := pkColumnNames(op); len(pk) > 0 {
		b.WriteString(", PRIMARY KEY (")
		for i, n := range pk {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteIdent(n))
		}
		b.WriteString(")")
	}
	b.WriteString(")")
	if err := execDDLApply(ctx, conn, b.String()); err != nil {
		return fmt.Errorf("apply create table %s: %w", op.TableName, err)
	}

	// Bind the op's allocated ids to this node's freshly-assigned OID/attnums.
	oid, err := relationOID(ctx, conn, appliedSchema, op.TableName)
	if err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `SELECT public.syzy_install_truncate_guard($1)`, oid); err != nil {
		return fmt.Errorf("apply create table %s: install truncate guard: %w", op.TableName, err)
	}
	pgcols, err := introspectColumns(ctx, conn, oid)
	if err != nil {
		return err
	}
	cidByName := make(map[string]crdt.ColumnID, len(op.Columns))
	for _, c := range op.Columns {
		cidByName[c.Name] = c.ID
	}
	ti := &tableInfo{schema: appliedSchema, name: op.TableName, oid: oid, tid: op.TableID, byName: map[string]*colInfo{}}
	for _, pc := range pgcols {
		cid, ok := cidByName[pc.name]
		if !ok {
			return fmt.Errorf("apply create table %s: column %q is not in the op", op.TableName, pc.name)
		}
		ci := pc.colInfo(cid)
		ti.cols = append(ti.cols, ci)
		ti.byName[pc.name] = ci
	}
	for _, n := range pkColumnNames(op) {
		ti.pk = append(ti.pk, ti.byName[n])
	}
	// Bind any inline UNIQUE keys (§5). Eventual keys bind in-catalog only — no
	// physical follower constraint (arbitration is the sole convergence
	// mechanism on apply). Coordinated keys are physically enforced on every
	// node (index + gate triggers, coordinated.go).
	coordinated := false
	for _, k := range op.Keys {
		if k.KeyID == (crdt.KeyID{}) {
			continue // PK, bound above via ti.pk
		}
		if err := bindUniqueKey(ti, k.KeyID, k.Members, k.Coordinated); err != nil {
			return fmt.Errorf("apply create table %s: %w", op.TableName, err)
		}
		coordinated = coordinated || k.Coordinated
	}
	// A counter column implies the cell clock group; set the REPLICA IDENTITY it
	// runs on before the table can take a single row, so no write is ever
	// captured under whole-row merge. A cell table with no counters gets its own
	// OpSetClockGroup right after this op.
	if ti.hasCounters() {
		if err := setReplicaIdentity(ctx, conn, ti, metadata.ClockGroupCell); err != nil {
			return err
		}
		ti.clockGroup = metadata.ClockGroupCell
	}
	cat.addTable(ti)
	if coordinated {
		if err := ensureCoordinated(ctx, conn, cat, ti); err != nil {
			return fmt.Errorf("apply create table %s: %w", op.TableName, err)
		}
	}

	// Partition this follower's freshly-created bigint PK sequence to its node
	// slice, exactly as bootstrap partitionSequences does for pre-created tables.
	// Correctness rests on ordinal 0 being reserved: the originator's sequence
	// stays in the unpartitioned [1, 2^47) low range, which no node's slice
	// (ordinal<<47, ordinal>=1) ever reaches — so the originator never collides
	// with a follower's locally-minted ids. The sequence is pristine here (the
	// table was just created and no local insert has run), so RESTART is safe.
	if ordinal != 0 {
		lo, hi := idSlice(ordinal)
		if err := partitionTable(ctx, conn, ti, lo, hi, true); err != nil {
			return fmt.Errorf("partition %s: %w", op.TableName, err)
		}
	}
	return nil
}

// applySetClockGroup switches a table's merge unit. The clock group IS the
// table's REPLICA IDENTITY on this engine (§8): FULL logs the old tuple every
// UPDATE, which is what per-column capture diffs against, so applying the op
// installs the capability and records the rule in one step. Idempotent.
func applySetClockGroup(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp) error {
	ti := cat.byID[op.TableID]
	if ti == nil {
		return fmt.Errorf("apply set clock group: table id %x not in catalog", op.TableID)
	}
	group := op.ClockGroup
	if group != metadata.ClockGroupCell {
		group = metadata.ClockGroupRow
	}
	if group == metadata.ClockGroupRow && ti.hasCounters() {
		return unsupportedDDLf("postgres: %s: counter columns merge per column and cannot leave the cell clock group", ti.name)
	}
	if err := setReplicaIdentity(ctx, conn, ti, group); err != nil {
		return err
	}
	ti.clockGroup = group
	return nil
}

// setReplicaIdentity installs the REPLICA IDENTITY the clock group runs on.
func setReplicaIdentity(ctx context.Context, conn *pgx.Conn, ti *tableInfo, group string) error {
	if err := execDDLApply(ctx, conn, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY %s",
		tableRef(ti), replIdentClause(group))); err != nil {
		return fmt.Errorf("apply clock group %s on %s: %w", group, ti.name, err)
	}
	return nil
}

func applyAddColumn(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp) error {
	ti := cat.byID[op.TableID]
	if ti == nil {
		return fmt.Errorf("apply add column: table id %x not in catalog", op.TableID)
	}
	if len(op.Columns) != 1 {
		return fmt.Errorf("apply add column %s: expected 1 column, got %d", ti.name, len(op.Columns))
	}
	c := op.Columns[0]
	if ti.colByID(c.ID) != nil {
		return nil // already bound (idempotent replay-from-floor on restart)
	}
	// IF NOT EXISTS so a replay after a crash mid-batch (the column committed but
	// schemaSeq not yet advanced past this event) re-runs cleanly.
	if err := execDDLApply(ctx, conn, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", tableRef(ti), renderPGColumnDef(c))); err != nil {
		return fmt.Errorf("apply add column %s.%s: %w", ti.name, c.Name, err)
	}
	// Bind from what Postgres actually created, not from what the op declared.
	// The two differ: a serial ships as the CREATE-shaped pseudo-type with an
	// empty Default, while the column it produces here holds nextval(...) — so
	// copying the op's fields leaves this node's catalog describing a column that
	// does not exist, and disagreeing with the originator about the same one.
	// Only the stable ID and the counter role come from the op; the rest is the
	// same introspection CREATE TABLE binds through.
	local, err := introspectColumns(ctx, conn, ti.oid)
	if err != nil {
		return fmt.Errorf("apply add column %s.%s: %w", ti.name, c.Name, err)
	}
	var ci *colInfo
	for _, pc := range local {
		if pc.name == c.Name {
			ci = pc.colInfo(c.ID)
			break
		}
	}
	if ci == nil {
		return fmt.Errorf("apply add column %s.%s: column absent after ALTER", ti.name, c.Name)
	}
	counter := c.ClockGroup == metadata.ClockGroupCounter
	if counter {
		ci.counter = true
		ci.typeName = counterTypeName
	}
	ti.cols = append(ti.cols, ci)
	ti.byName[c.Name] = ci
	if counter && !ti.cellGroup() {
		// The added counter declares the cell clock group for the whole table,
		// exactly as the originator's admission gate did when the ALTER ran there.
		if err := setReplicaIdentity(ctx, conn, ti, metadata.ClockGroupCell); err != nil {
			return err
		}
		ti.clockGroup = metadata.ClockGroupCell
	}
	return nil
}

// applyAlterColumn brings one column to the whole attribute state the op
// carries. The op is a desired state, not a delta, so this compares against the
// cached column and issues only the ALTERs whose effect is missing — a replay
// after a crash mid-batch is a no-op. Only relaxations reach here
// (classifyColumnChange), so no statement can fail on existing rows.
func applyAlterColumn(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp) error {
	ti := cat.byID[op.TableID]
	if ti == nil {
		return fmt.Errorf("apply alter column: table id %x not in catalog", op.TableID)
	}
	if len(op.Columns) != 1 {
		return fmt.Errorf("apply alter column %s: expected 1 column, got %d", ti.name, len(op.Columns))
	}
	c := op.Columns[0]
	ci := ti.colByID(c.ID)
	if ci == nil {
		return fmt.Errorf("apply alter column %s: column id %x not in table", ti.name, c.ID)
	}
	col := quoteIdent(ci.name)
	typ := pgColumnType(c.Type)
	var stmts []string
	if ci.typeName != typ {
		// Defense in depth against the two shapes the origin classifier refuses
		// to emit: a serial pseudo-type and an embedded IDENTITY clause are
		// CREATE-only spellings, so rendering either into ALTER COLUMN … TYPE
		// would fail on every follower forever. Halt with the reason instead.
		if isAutoIncrementType(typ) {
			return fmt.Errorf("apply alter column %s.%s: op carries auto-increment type %q, which cannot be spelled in ALTER COLUMN … TYPE", ti.name, ci.name, typ)
		}
		stmts = append(stmts, fmt.Sprintf("ALTER COLUMN %s TYPE %s", col, typ))
	}
	if c.NotNull && !ci.notNull {
		// A restriction cannot replicate; the origin rejects it, and an op that
		// carries one anyway must not be recorded as applied.
		return fmt.Errorf("apply alter column %s.%s: op sets NOT NULL, which cannot replicate", ti.name, ci.name)
	}
	if ci.def != c.Default {
		if c.Default == "" {
			stmts = append(stmts, "ALTER COLUMN "+col+" DROP DEFAULT")
		} else {
			stmts = append(stmts, "ALTER COLUMN "+col+" SET DEFAULT "+c.Default)
		}
	}
	if ci.notNull && !c.NotNull {
		stmts = append(stmts, "ALTER COLUMN "+col+" DROP NOT NULL")
	}
	for _, s := range stmts {
		if err := execDDLApply(ctx, conn, "ALTER TABLE "+tableRef(ti)+" "+s); err != nil {
			return fmt.Errorf("apply alter column %s.%s: %w", ti.name, ci.name, err)
		}
	}
	ci.setAttrs(typ, c.Default, c.NotNull)
	// A coordinated key's reservation path encodes values through the column's
	// type, so its decoupled snapshot has to see the new one.
	if cat.coordIdx != nil {
		for _, uk := range ti.uniqueKeys {
			if uk.coordinated {
				return ensureCoordinated(ctx, conn, cat, ti)
			}
		}
	}
	return nil
}

func applyDropColumn(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp) error {
	ti := cat.byID[op.TableID]
	if ti == nil {
		return fmt.Errorf("apply drop column: table id %x not in catalog", op.TableID)
	}
	ci := ti.colByID(op.ColumnID)
	if ci == nil {
		return nil // already absent; idempotent
	}
	if err := execDDLApply(ctx, conn, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableRef(ti), quoteIdent(ci.name))); err != nil {
		return fmt.Errorf("apply drop column %s.%s: %w", ti.name, ci.name, err)
	}
	ti.removeColumn(ci)
	return nil
}

func applyRenameColumn(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp) error {
	ti := cat.byID[op.TableID]
	if ti == nil {
		return fmt.Errorf("apply rename column: table id %x not in catalog", op.TableID)
	}
	ci := ti.colByID(op.ColumnID)
	if ci == nil {
		return fmt.Errorf("apply rename column: column id %x not in table %s", op.ColumnID, ti.name)
	}
	if ci.name == op.ColumnName {
		return nil // already renamed; idempotent
	}
	if err := execDDLApply(ctx, conn, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		tableRef(ti), quoteIdent(ci.name), quoteIdent(op.ColumnName))); err != nil {
		return fmt.Errorf("apply rename column %s.%s: %w", ti.name, ci.name, err)
	}
	delete(ti.byName, ci.name)
	ci.name = op.ColumnName
	ti.byName[ci.name] = ci
	return nil
}

func applyRenameTable(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp) error {
	ti := cat.byID[op.TableID]
	if ti == nil {
		return fmt.Errorf("apply rename table: table id %x not in catalog", op.TableID)
	}
	if ti.name == op.TableName {
		return nil // already renamed; idempotent
	}
	if err := execDDLApply(ctx, conn, fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
		tableRef(ti), quoteIdent(op.TableName))); err != nil {
		return fmt.Errorf("apply rename table %s: %w", ti.name, err)
	}
	ti.name = op.TableName
	return nil
}

// applyAddUniqueKey binds a replicated unique key into the follower's catalog.
// Followers hold no physical UNIQUE constraint (§5): the loser-null arbitration
// on the apply path is the sole convergence mechanism, and a physical constraint
// would 23505 the apply txn before arbitration could null the loser.
func applyAddUniqueKey(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp) error {
	ti := cat.byID[op.TableID]
	if ti == nil {
		return fmt.Errorf("apply add unique key: table id %x not in catalog", op.TableID)
	}
	if len(op.Keys) != 1 {
		return fmt.Errorf("apply add unique key %s: expected 1 key, got %d", ti.name, len(op.Keys))
	}
	if err := bindUniqueKey(ti, op.KeyID, op.Keys[0].Members, op.Keys[0].Coordinated); err != nil {
		return err
	}
	if op.Keys[0].Coordinated {
		return ensureCoordinated(ctx, conn, cat, ti)
	}
	return nil
}

func applyDropUniqueKey(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp) error {
	ti := cat.byID[op.TableID]
	if ti == nil {
		return nil // table already gone; idempotent
	}
	var wasCoordinated bool
	for _, uk := range ti.uniqueKeys {
		if uk.keyID == op.KeyID {
			wasCoordinated = uk.coordinated
		}
	}
	ti.removeUniqueKey(op.KeyID) // no-op if the key is already gone (idempotent replay)
	if wasCoordinated {
		// Reconcile rather than drop-by-name: the key is gone from ti, so
		// this uninstalls its accumulating trigger (and the table's
		// reservation trigger once no coordinated key is left).
		return ensureCoordinated(ctx, conn, cat, ti)
	}
	return nil
}

// bindUniqueKey resolves a key's member ColumnIDs to the table's colInfos (in
// ordinal order) and appends it to ti.uniqueKeys. Idempotent: a re-applied key
// (replay-from-floor on restart) is a no-op.
func bindUniqueKey(ti *tableInfo, keyID crdt.KeyID, members []crdt.CatalogKeyMember, coordinated bool) error {
	for _, uk := range ti.uniqueKeys {
		if uk.keyID == keyID {
			return nil
		}
	}
	ms := append([]crdt.CatalogKeyMember(nil), members...)
	sort.Slice(ms, func(i, j int) bool { return ms[i].Ordinal < ms[j].Ordinal })
	cols := make([]*colInfo, 0, len(ms))
	for _, m := range ms {
		ci := ti.colByID(m.ColumnID)
		if ci == nil {
			return fmt.Errorf("unique key on %s: column %x not in table", ti.name, m.ColumnID)
		}
		cols = append(cols, ci)
	}
	ti.uniqueKeys = append(ti.uniqueKeys, &uniqueKey{keyID: keyID, cols: cols, coordinated: coordinated})
	return nil
}

func applyDropTable(ctx context.Context, conn *pgx.Conn, cat *catalog, op crdt.CatalogOp) error {
	ti := cat.byID[op.TableID]
	if ti == nil {
		return nil // already absent; idempotent
	}
	if err := execDDLApply(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableRef(ti))); err != nil {
		return fmt.Errorf("apply drop table %s: %w", ti.name, err)
	}
	cat.dropTable(ti)
	return nil
}

// execDDLApply runs one applied DDL statement on the apply session. Loopback is
// handled by the session's replica role: the ddl_command_end / sql_drop intent
// triggers are ENABLE ORIGIN, so they are silent under replica — applied DDL
// writes no intent rows and is never re-captured. Sequence partitioning for a
// follower's applied CREATE TABLE is done in Go (applyCreateTable →
// partitionTable), not by an in-DB trigger, so no syzy.internal guard is needed.
func execDDLApply(ctx context.Context, conn *pgx.Conn, sql string) error {
	_, err := conn.Exec(ctx, sql)
	return err
}

// pgColumnType maps a catalog op's declared column type to the local Postgres
// type. A Postgres-originated op carries format_type() output, which passes
// through untouched; a SQLite-originated op carries SQLite's declared type
// names, mapped here to the cross-engine profile's Postgres shapes (§13):
// INTEGER is 64-bit in SQLite so it lands as bigint, BLOB as bytea, REAL (an
// 8-byte float there) as double precision. An exact-match table — free-form
// SQLite type names outside the profile fail the CREATE loudly
// (schema-unhealthy) rather than guess.
// isAutoIncrementType reports whether a rendered column type carries
// auto-increment shape — a serial pseudo-type or the IDENTITY clause
// catalogColumn appends. Both are CREATE-only spellings.
func isAutoIncrementType(typ string) bool {
	switch typ {
	case "smallserial", "serial", "bigserial":
		return true
	}
	return strings.Contains(typ, " AS IDENTITY")
}

func pgColumnType(declared string) string {
	switch strings.ToUpper(declared) {
	case "":
		return "text" // SQLite typeless: the profile's text class
	case "INTEGER", "INT":
		return "bigint"
	case "TEXT", "CLOB":
		return "text"
	case "BLOB":
		return "bytea"
	case "REAL", "DOUBLE", "FLOAT":
		return "double precision"
	case "BOOLEAN", "BOOL":
		// SQLite stores booleans as 0/1 integers (ColInt on the wire); a real
		// Postgres boolean column would re-capture them as boolean→ColInt but
		// accept text 'true' from SQLite's flexible typing under a different
		// class tag — bigint keeps the class stable per column. The profile's
		// answer to flexible typing generally is SQLite STRICT tables (§13).
		return "bigint"
	default:
		return declared
	}
}

// renderPGColumnDef renders a column for CREATE TABLE / ADD COLUMN. Type is the
// originator's format_type() string (passed through) or a SQLite declared type
// (mapped — see pgColumnType). An identity column arrives with its GENERATED …
// AS IDENTITY clause embedded in Type (catalogColumn), so the composite passes
// through pgColumnType verbatim and needs no case here.
func renderPGColumnDef(c crdt.CatalogColumn) string {
	var b strings.Builder
	b.WriteString(quoteIdent(c.Name))
	b.WriteString(" ")
	if c.ClockGroup == metadata.ClockGroupCounter {
		// The counter declaration rides the column type on every engine; here it
		// is the syzy_counter domain, whatever integer type the originator used
		// to express it (sql/counter.sql).
		b.WriteString(counterTypeName)
	} else {
		b.WriteString(pgColumnType(c.Type))
	}
	if c.NotNull {
		b.WriteString(" NOT NULL")
	}
	switch {
	case c.Generated && c.Default != "":
		b.WriteString(" GENERATED ALWAYS AS (")
		b.WriteString(c.Default)
		b.WriteString(") STORED")
	case c.Default != "":
		b.WriteString(" DEFAULT ")
		b.WriteString(c.Default)
	}
	return b.String()
}

// pkColumnNames returns the PK member column names in key order (the PK key is
// the one with the all-zero KeyID).
func pkColumnNames(op crdt.CatalogOp) []string {
	byID := make(map[crdt.ColumnID]string, len(op.Columns))
	for _, c := range op.Columns {
		byID[c.ID] = c.Name
	}
	var names []string
	for _, k := range op.Keys {
		if k.KeyID != (crdt.KeyID{}) {
			continue
		}
		members := append([]crdt.CatalogKeyMember(nil), k.Members...)
		sort.Slice(members, func(i, j int) bool { return members[i].Ordinal < members[j].Ordinal })
		for _, m := range members {
			names = append(names, byID[m.ColumnID])
		}
	}
	return names
}

func relationOID(ctx context.Context, conn *pgx.Conn, schema, name string) (uint32, error) {
	var oid uint32
	if err := conn.QueryRow(ctx, `
		SELECT c.oid FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace
		WHERE ns.nspname = $1 AND c.relname = $2`, schema, name).Scan(&oid); err != nil {
		return 0, fmt.Errorf("resolve oid %s.%s: %w", schema, name, err)
	}
	return oid, nil
}
