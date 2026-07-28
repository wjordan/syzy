// Admission rules that keep every write to a coordinated (NOT NULL
// UNIQUE) key table on the pre-commit reservation path. The gate is the
// only enforcement point — no replica holds a physical UNIQUE index for
// a coordinated key — so any write channel that bypasses claims capture
// (trigger bodies, which run at preupdate depth > 0 and re-execute on
// every replica) must be rejected at DDL time, in both directions:
// no new trigger may write a coordinated table, and no coordinated key
// may be created on a table an existing trigger writes.

package producer

import (
	"bytes"
	"fmt"
	"strings"

	rsql "github.com/rqlite/sql"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// triggerWriteTargets parses a CREATE TRIGGER statement and returns the
// table names its body INSERTs or UPDATEs (REPLACE and upserts parse as
// InsertStatement). DELETE-only bodies return nothing: a deleted row's
// vacated value is observed and freed by the leaseholder's derivation,
// so deletes cannot violate uniqueness.
func triggerWriteTargets(sql string) ([]string, error) {
	stmt, err := rsql.NewParser(strings.NewReader(sql)).ParseStatement()
	if err != nil {
		return nil, fmt.Errorf("parse trigger: %w", err)
	}
	ct, ok := stmt.(*rsql.CreateTriggerStatement)
	if !ok {
		return nil, fmt.Errorf("not a CREATE TRIGGER statement")
	}
	var out []string
	for _, body := range ct.Body {
		switch s := body.(type) {
		case *rsql.InsertStatement:
			if s.Table != nil {
				out = append(out, s.Table.Name)
			}
		case *rsql.UpdateStatement:
			if s.Table != nil && s.Table.Name != nil {
				out = append(out, s.Table.Name.Name)
			}
		}
	}
	return out, nil
}

// tableHasCoordinatedKey reports whether tab carries any active
// coordinated key.
func tableHasCoordinatedKey(tab *catalog.Table) bool {
	for _, uk := range tab.UniqueKeys {
		if uk.Coordinated {
			return true
		}
	}
	return false
}

// opHasCoordinatedKey reports whether a CREATE TABLE op declares a
// coordinated key.
func opHasCoordinatedKey(op crdt.CatalogOp) bool {
	for _, k := range op.Keys {
		if k.Coordinated {
			return true
		}
	}
	return false
}

// hasCounterColumn reports whether any declared column is a COUNTER
// column (which forces the table into the cell clock group at apply).
func hasCounterColumn(cols []crdt.CatalogColumn) bool {
	for _, c := range cols {
		if c.ClockGroup == metadata.ClockGroupCounter {
			return true
		}
	}
	return false
}

// fkUpdatesChild reports whether any declared FK action synthesizes a
// cascade trigger that UPDATEs the child table: SET NULL / SET DEFAULT
// on delete, and every writing action on update (CASCADE, SET NULL, SET
// DEFAULT all rewrite the child's referencing column). ON DELETE CASCADE
// only deletes child rows, which the reservation derivation self-heals,
// so it doesn't count.
func fkUpdatesChild(fks []parsedFK) bool {
	for _, fk := range fks {
		if fk.OnDelete == fkSetNull || fk.OnDelete == fkSetDefault || isCascadeAction(fk.OnUpdate) {
			return true
		}
	}
	return false
}

// rejectTriggerOnCoordinated fails a CREATE TRIGGER whose body writes a
// coordinated-key table. Matching is case-insensitive in both of its
// resolution paths — SQLite folds identifiers, so `INSERT INTO Users`
// and `INSERT INTO users` name one table and one gate.
func (d *ddlAdmission) rejectTriggerOnCoordinated(sql string) error {
	targets, err := triggerWriteTargets(sql)
	if err != nil {
		return fmt.Errorf("CREATE TRIGGER: %w", err)
	}
	for _, name := range targets {
		if d.namesCoordinatedTable(name) {
			return fmt.Errorf("CREATE TRIGGER: body writes table %q, which has a coordinated (NOT NULL UNIQUE) key; trigger writes bypass the reservation gate", name)
		}
	}
	return nil
}

// namesCoordinatedTable reports whether name resolves to a table with an
// active coordinated key, under SQLite's case-insensitive identifier
// rules. Two passes, because neither covers the other's ground: the
// fold-scan reads the committed catalog and needs nothing from the
// physical schema, and the name lookups resolve through the transaction
// overlay and through the spelling SQLite itself stores (which is how a
// table created earlier in this very transaction is seen).
func (d *ddlAdmission) namesCoordinatedTable(name string) bool {
	for _, tab := range d.cat.Tables() {
		if strings.EqualFold(tab.Name, name) && tableHasCoordinatedKey(tab) {
			return true
		}
	}
	for _, n := range []string{name, d.canonicalTableName(name)} {
		if tab, ok := d.lookupTable(n); ok && tableHasCoordinatedKey(tab) {
			return true
		}
	}
	return false
}

// canonicalTableName folds name to the spelling sqlite_master stores,
// using SQLite's own NOCASE comparison; empty when it names no table or
// the read fails. Reads the writer connection so DDL earlier in the open
// transaction is visible; safe here because no CREATE TRIGGER carries a
// cascade bundle (see rejectCoordinatedKeyIfTriggerTarget for why that
// matters).
func (d *ddlAdmission) canonicalTableName(name string) string {
	stmt, _, err := d.app.Prepare(
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? COLLATE NOCASE`)
	if err != nil {
		return ""
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, name); err != nil {
		return ""
	}
	if hasRow, err := stmt.Step(); err != nil || !hasRow {
		return ""
	}
	return stmt.ColumnText(0)
}

// rejectUnverifiableTriggerOnCoordinated fails closed on a CREATE
// TRIGGER the DDL parser could not read while any coordinated key
// exists. classifyDDL passes an unparsable statement through to SQLite
// unadmitted (SQLite accepts syntax rqlite/sql does not — bracket-quoted
// identifiers, for one), which for a trigger body would install an
// ungated write channel into a coordinated table. Nothing downstream can
// catch that: the key has no physical index anywhere. Databases with no
// coordinated key are unaffected.
func (d *ddlAdmission) rejectUnverifiableTriggerOnCoordinated(p parsedDDL) error {
	if !p.Unparsable || !d.cat.HasCoordinatedKeys() || !looksLikeCreateTrigger(p.RawSQL) {
		return nil
	}
	return fmt.Errorf("CREATE TRIGGER: statement could not be parsed, so its body cannot be checked against the coordinated (NOT NULL UNIQUE) keys in this database; rewrite it using standard SQL identifier quoting")
}

// looksLikeCreateTrigger reports whether sql's leading keywords are a
// CREATE ... TRIGGER form, ignoring TEMP/TEMPORARY and OR REPLACE.
func looksLikeCreateTrigger(sql string) bool {
	f := strings.Fields(sql)
	if len(f) == 0 || !strings.EqualFold(f[0], "CREATE") {
		return false
	}
	for _, w := range f[1:min(len(f), 5)] {
		if strings.EqualFold(w, "TRIGGER") {
			return true
		}
	}
	return false
}

// rejectCoordinatedKeyIfTriggerTarget fails coordinated-key creation on
// a table some existing trigger's body writes. Scans every trigger in
// sqlite_master and fails closed on one the parser cannot read — an
// unverifiable trigger must not silently coexist with the key.
//
// The scan runs on the HELPER connection, never the writer: a writer-
// side read at trace time pins the writer's WAL snapshot while its
// outer statement is active, and the cascade-bundle path then writes
// through the helper before the statement body runs — the stale
// snapshot would fail the body's write upgrade with SQLITE_BUSY.
// Admission requires the helper whenever a coordinated key is involved
// (see the call sites), so d.helper is non-nil here.
func (d *ddlAdmission) rejectCoordinatedKeyIfTriggerTarget(table string) error {
	return CoordinatedTriggerConflict(d.helper, table)
}

// CoordinatedTriggerConflict returns a non-nil error when some trigger in
// conn's schema INSERTs or UPDATEs table, or when a trigger cannot be
// parsed to prove that it does not. Callers use it to keep a coordinated
// key and an ungated write channel from coexisting: admission before
// creating the key, and open-time normalization before stripping the
// index a pre-normalization database still relies on.
func CoordinatedTriggerConflict(conn *sqlitebridge.Conn, table string) error {
	stmt, _, err := conn.Prepare(`SELECT name, sql FROM sqlite_master WHERE type='trigger' AND sql IS NOT NULL`)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		name, sql := stmt.ColumnText(0), stmt.ColumnText(1)
		targets, err := triggerWriteTargets(sql)
		if err != nil {
			return fmt.Errorf("existing trigger %q: %v — cannot verify it does not write %q", name, err, table)
		}
		for _, tgt := range targets {
			if strings.EqualFold(tgt, table) {
				return fmt.Errorf("existing trigger %q writes table %q; trigger writes bypass the reservation gate — drop the trigger first", name, table)
			}
		}
	}
}

// rejectCoordinatedColumnShadow fails a plain CREATE INDEX whose column
// tuple is exactly a *total* (unfiltered) coordinated key's members.
// DROP INDEX is classified as key removal by column tuple + predicate;
// a plain index carries no predicate, so it matches a total key exactly
// as the key's own downgraded index does, and dropping either would
// silently remove the key. Refusing the second index keeps that
// classification unambiguous by construction — and the tuple is
// redundant wherever the key kept a backing index.
func rejectCoordinatedColumnShadow(tab *catalog.Table, cols []string, desc string) error {
	for _, uk := range tab.UniqueKeys {
		if !uk.Coordinated || uk.Predicate.Root != nil {
			continue
		}
		if matchesAnySet(cols, [][]string{columnNames(uk)}) {
			return fmt.Errorf("%s: table %q has a coordinated (NOT NULL UNIQUE) key on exactly column(s) %v; a second index over them would make DROP INDEX ambiguous (it removes the key by column match) — index a different column tuple", desc, tab.Name, cols)
		}
	}
	return nil
}

// unfilteredIndexOnColumns returns the name of an existing index on
// table whose column tuple equals cols and which carries no WHERE
// clause, skipping the index named exclude. Empty when none matches.
func unfilteredIndexOnColumns(conn *sqlitebridge.Conn, table string, cols []string, exclude string) (string, error) {
	stmt, _, err := conn.Prepare(`SELECT name FROM pragma_index_list(?) WHERE origin = 'c' AND partial = 0`)
	if err != nil {
		return "", err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, table); err != nil {
		return "", err
	}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return "", err
		}
		if !hasRow {
			return "", nil
		}
		name := stmt.ColumnText(0)
		if strings.EqualFold(name, exclude) {
			continue
		}
		got, err := indexColumnNames(conn, name)
		if err != nil {
			return "", err
		}
		if got != nil && matchesAnySet(got, [][]string{cols}) {
			return name, nil
		}
	}
}

// coordinatedKeyRemovalOp classifies a DROP INDEX as coordinated-key
// removal when the index matches an active coordinated key on its table:
// same column list (ordered, case-insensitive) AND same partial
// predicate. Both halves are required — a table may carry several
// coordinated keys over one column tuple as long as their predicates
// differ (per-tenant soft-delete scopes, say), and a plain lookup index
// carries no predicate at all, so a column-only match would remove an
// arbitrary one of them. Ambiguity is resolved toward keeping the key:
// no unique match means the plain OpDropIndex path, which drops the
// index alone. ok=false also covers an index unrelated to any key, or
// one that doesn't exist (SQLite then fails the statement itself).
func (d *ddlAdmission) coordinatedKeyRemovalOp(p parsedDDL) (crdt.CatalogOp, bool, error) {
	stmt, _, err := d.app.Prepare(`SELECT tbl_name FROM sqlite_master WHERE type = 'index' AND name = ?`)
	if err != nil {
		return crdt.CatalogOp{}, false, err
	}
	tblName, found := "", false
	err = func() error {
		defer stmt.Finalize()
		if err := stmt.BindText(1, p.Name); err != nil {
			return err
		}
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			tblName, found = stmt.ColumnText(0), true
		}
		return nil
	}()
	if err != nil || !found {
		return crdt.CatalogOp{}, false, err
	}
	tab, ok := d.lookupTable(tblName)
	if !ok {
		return crdt.CatalogOp{}, false, nil
	}
	names, err := indexColumnNames(d.app, p.Name)
	if err != nil || names == nil {
		return crdt.CatalogOp{}, false, err
	}
	var byColumns []catalog.UniqueKey
	for _, uk := range tab.UniqueKeys {
		if uk.Coordinated && matchesAnySet(names, [][]string{columnNames(uk)}) {
			byColumns = append(byColumns, uk)
		}
	}
	if len(byColumns) == 0 {
		return crdt.CatalogOp{}, false, nil
	}
	pred, ok, err := d.indexPredicate(tab, p.Name)
	if err != nil {
		return crdt.CatalogOp{}, false, err
	}
	if !ok {
		// Predicate unreadable. Safe only when the columns already name
		// exactly one key — otherwise keep every key and drop the index.
		if len(byColumns) != 1 {
			return crdt.CatalogOp{}, false, nil
		}
		return dropKeyOp(tab, byColumns[0], p), true, nil
	}
	want := crdt.EncodeUniquePredicate(pred)
	for _, uk := range byColumns {
		if bytes.Equal(crdt.EncodeUniquePredicate(uk.Predicate), want) {
			return dropKeyOp(tab, uk, p), true, nil
		}
	}
	return crdt.CatalogOp{}, false, nil
}

func columnNames(uk catalog.UniqueKey) []string {
	out := make([]string, len(uk.Columns))
	for i, c := range uk.Columns {
		out[i] = c.Name
	}
	return out
}

func dropKeyOp(tab *catalog.Table, uk catalog.UniqueKey, p parsedDDL) crdt.CatalogOp {
	return crdt.CatalogOp{
		Kind: crdt.OpDropUniqueKey, TableID: tab.ID, KeyID: uk.KeyID,
		ObjectName: p.Name, RawSQL: p.RawSQL,
	}
}

// indexPredicate compiles the WHERE clause of a named index into the
// same form a key's Predicate carries, so the two can be compared. A
// total (unfiltered) index yields the zero predicate. ok=false means the
// stored SQL could not be read or compiled — the caller must not assume
// either answer from that.
func (d *ddlAdmission) indexPredicate(tab *catalog.Table, index string) (crdt.UniquePredicate, bool, error) {
	sql, err := indexMasterSQL(d.app, index)
	if err != nil || sql == "" {
		return crdt.UniquePredicate{}, false, err
	}
	stmt, perr := rsql.NewParser(strings.NewReader(sql)).ParseStatement()
	if perr != nil {
		return crdt.UniquePredicate{}, false, nil
	}
	ci, isIndex := stmt.(*rsql.CreateIndexStatement)
	if !isIndex {
		return crdt.UniquePredicate{}, false, nil
	}
	if !ci.Where.IsValid() {
		return crdt.UniquePredicate{}, true, nil
	}
	pred, cerr := compilePartialPredicate(ci.WhereExpr, d.app, tab)
	if cerr != nil {
		return crdt.UniquePredicate{}, false, nil
	}
	return pred, true, nil
}

func indexMasterSQL(conn *sqlitebridge.Conn, name string) (string, error) {
	stmt, _, err := conn.Prepare(`SELECT COALESCE(sql, '') FROM sqlite_master WHERE type = 'index' AND name = ?`)
	if err != nil {
		return "", err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, name); err != nil {
		return "", err
	}
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		return "", err
	}
	return stmt.ColumnText(0), nil
}

// pendingCoordinatedKeyTables returns the tables gaining a coordinated
// key in the open transaction's pending DDL. Bundles never reach the
// pending set (both stash paths exclude them), so only the two key-
// bearing op kinds appear.
func (d *ddlAdmission) pendingCoordinatedKeyTables() map[string]bool {
	if d.txnDDL == nil {
		return nil
	}
	var out map[string]bool
	created := map[crdt.TableID]string{}
	for _, po := range d.txnDDL.ops {
		op := po.op
		switch op.Kind {
		case crdt.OpCreateTable:
			created[op.TableID] = op.TableName
			if !opHasCoordinatedKey(op) {
				continue
			}
		case crdt.OpAddUniqueKey:
			if !opHasCoordinatedKey(op) {
				continue
			}
		default:
			continue
		}
		name := op.TableName
		if name == "" {
			if n, ok := created[op.TableID]; ok {
				name = n
			} else if tab, ok := d.cat.TableByID(op.TableID); ok {
				name = tab.Name
			} else {
				continue
			}
		}
		if out == nil {
			out = map[string]bool{}
		}
		out[name] = true
	}
	return out
}
