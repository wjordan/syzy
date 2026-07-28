// Physical normalization for coordinated (NOT NULL UNIQUE) keys: no
// node may hold a SQLite UNIQUE index enforcing one. The reservation
// gate is the only enforcement point; a native index on any node would
// arbitrate on the apply path and wedge a legitimate same-transaction
// transfer (docs/CRDT.md#unique-keys). Receivers never materialize
// unique indexes; the originator's own index — the inline-constraint
// autoindex or the executed CREATE UNIQUE INDEX — is normalized away
// here, between the DDL's commit and its catalog resolution, so the
// key is never active while the index still enforces.
//
// Two shapes, by pragma_index_list origin:
//   - 'c' (named CREATE UNIQUE INDEX): downgraded in place to a plain
//     index with the same name and definition.
//   - 'u' (inline UNIQUE autoindex): the autoindex cannot be dropped, so
//     the table is rebuilt from its own definition with the matching
//     UNIQUE constraints stripped (rows copied, other indexes and
//     triggers re-created). At birth the table is empty and the rebuild
//     is trivial; at open-time convergence it is the one-time migration
//     for tables created before normalization existed.
//
// Only indexes whose column list matches a coordinated key's members are
// touched; eventual (nullable) unique indexes keep native enforcement.

package producer

import (
	"fmt"
	"strings"

	rsql "github.com/rqlite/sql"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// coordinatedKeyTargets maps table name → coordinated-key member-name
// sets declared by op (bundles walked). OpCreateTable resolves names
// from the op's own columns; OpAddUniqueKey from the committed catalog
// (the table pre-exists, only the key is new).
func coordinatedKeyTargets(op crdt.CatalogOp, cat *catalog.Catalog) map[string][][]string {
	out := map[string][][]string{}
	var walk func(crdt.CatalogOp)
	walk = func(op crdt.CatalogOp) {
		switch op.Kind {
		case crdt.OpBundle:
			for _, sub := range op.SubOps {
				walk(sub)
			}
		case crdt.OpCreateTable:
			byID := map[crdt.ColumnID]string{}
			for _, c := range op.Columns {
				byID[c.ID] = c.Name
			}
			for _, k := range op.Keys {
				if !k.Coordinated {
					continue
				}
				names := make([]string, len(k.Members))
				for i, m := range k.Members {
					names[i] = byID[m.ColumnID]
				}
				out[op.TableName] = append(out[op.TableName], names)
			}
		case crdt.OpAddUniqueKey:
			tab, ok := cat.TableByID(op.TableID)
			if !ok {
				return
			}
			for _, k := range op.Keys {
				if !k.Coordinated {
					continue
				}
				names := make([]string, len(k.Members))
				for i, m := range k.Members {
					c, ok := tab.ColumnByID(m.ColumnID)
					if !ok {
						return
					}
					names[i] = c.Name
				}
				out[tab.Name] = append(out[tab.Name], names)
			}
		}
	}
	walk(op)
	return out
}

// CoordinatedMemberSets returns the member-column name sets of tab's
// active coordinated keys — the sets NormalizeCoordinatedIndexes must
// keep free of physical UNIQUE enforcement.
func CoordinatedMemberSets(tab *catalog.Table) [][]string {
	var sets [][]string
	for _, uk := range tab.UniqueKeys {
		if !uk.Coordinated {
			continue
		}
		names := make([]string, len(uk.Columns))
		for i, c := range uk.Columns {
			names[i] = c.Name
		}
		sets = append(sets, names)
	}
	return sets
}

// NormalizeCoordinatedIndexes removes physical UNIQUE enforcement of the
// given member sets on table: named unique indexes are downgraded to
// plain indexes (same name), inline-constraint autoindexes trigger a
// table rebuild with those constraints stripped. Idempotent — reports
// changed=false when nothing matched. conn must be a hook-free
// connection (never the producer's writer).
//
// Verified, not assumed: whatever path ran, the function re-reads the
// physical schema before returning success and errors if any matching
// UNIQUE enforcement survived. Both rewrite paths re-render SQL through
// a parser that does not cover every shape SQLite accepts, so a strip
// that silently no-ops is possible; reporting success on one would leave
// the key active behind an index that arbitrates on the apply path,
// which is the single failure this whole file exists to prevent.
func NormalizeCoordinatedIndexes(conn *sqlitebridge.Conn, table string, sets [][]string) (changed bool, err error) {
	if len(sets) == 0 {
		return false, nil
	}
	changed, err = normalizeOnce(conn, table, sets)
	if err != nil {
		return changed, err
	}
	if !changed {
		return false, nil
	}
	left, err := matchingUniqueIndexes(conn, table, sets)
	if err != nil {
		return changed, err
	}
	if len(left) > 0 {
		return changed, fmt.Errorf(
			"table %q: UNIQUE enforcement still present on coordinated key column(s) after normalization (index %q); the constraint's SQL form is not one this build can rewrite",
			table, left[0].name)
	}
	return changed, nil
}

// normalizeOnce is the rewrite half: one pass over the matching UNIQUE
// indexes, rebuilding the table for an inline constraint or downgrading
// named indexes in place. Its caller owns the verification.
func normalizeOnce(conn *sqlitebridge.Conn, table string, sets [][]string) (bool, error) {
	matched, err := matchingUniqueIndexes(conn, table, sets)
	if err != nil {
		return false, err
	}
	var downgrades []physIndex
	rebuild := false
	for _, ix := range matched {
		if ix.origin == "u" {
			rebuild = true
		} else {
			downgrades = append(downgrades, ix)
		}
	}
	if rebuild {
		// The rebuild re-creates the table's named indexes itself,
		// downgrading any that match — one pass covers both shapes.
		return true, rebuildTableStripped(conn, table, sets)
	}
	if len(downgrades) == 0 {
		return false, nil
	}
	err = inTxn(conn, func() error {
		for _, ix := range downgrades {
			plain, err := plainIndexSQL(ix.sql)
			if err != nil {
				return fmt.Errorf("index %q: %w", ix.name, err)
			}
			if err := conn.Exec("DROP INDEX " + sqlitebridge.QuoteIdent(ix.name)); err != nil {
				return err
			}
			if err := conn.Exec(plain); err != nil {
				return err
			}
		}
		return nil
	})
	return err == nil, err
}

type physIndex struct{ name, origin, sql string }

// matchingUniqueIndexes lists table's UNIQUE indexes whose column tuple
// equals one of sets — the physical enforcement a coordinated key must
// not have. Doubles as the post-normalization verification read.
func matchingUniqueIndexes(conn *sqlitebridge.Conn, table string, sets [][]string) ([]physIndex, error) {
	idxs, err := uniqueIndexesOn(conn, table)
	if err != nil {
		return nil, err
	}
	var out []physIndex
	for _, ix := range idxs {
		names, err := indexColumnNames(conn, ix.name)
		if err != nil {
			return nil, err
		}
		if names != nil && matchesAnySet(names, sets) {
			out = append(out, ix)
		}
	}
	return out, nil
}

// uniqueIndexesOn lists table's unique indexes of origin 'c' (named) or
// 'u' (inline-constraint autoindex), with the named ones' SQL.
func uniqueIndexesOn(conn *sqlitebridge.Conn, table string) ([]physIndex, error) {
	stmt, _, err := conn.Prepare(`SELECT il.name, il.origin, COALESCE(m.sql, '')
FROM pragma_index_list(?) il
LEFT JOIN sqlite_master m ON m.name = il.name AND m.type = 'index'
WHERE il."unique" = 1 AND il.origin IN ('c', 'u')`)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, table); err != nil {
		return nil, err
	}
	var out []physIndex
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return out, nil
		}
		out = append(out, physIndex{
			name:   stmt.ColumnText(0),
			origin: stmt.ColumnText(1),
			sql:    stmt.ColumnText(2),
		})
	}
}

// matchesAnySet reports whether names equals one of sets (ordered,
// case-insensitive — SQLite identifier semantics).
func matchesAnySet(names []string, sets [][]string) bool {
	for _, set := range sets {
		if len(set) != len(names) {
			continue
		}
		same := true
		for i := range set {
			if !strings.EqualFold(set[i], names[i]) {
				same = false
				break
			}
		}
		if same {
			return true
		}
	}
	return false
}

// plainIndexSQL re-renders a CREATE UNIQUE INDEX statement without
// UNIQUE, preserving name, columns, and any partial-index predicate.
func plainIndexSQL(sql string) (string, error) {
	stmt, err := rsql.NewParser(strings.NewReader(sql)).ParseStatement()
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	ci, ok := stmt.(*rsql.CreateIndexStatement)
	if !ok {
		return "", fmt.Errorf("not a CREATE INDEX statement")
	}
	ci.Unique = rsql.Pos{}
	return ci.String(), nil
}

// rebuildTableStripped rebuilds table from its own sqlite_master
// definition with the UNIQUE constraints matching sets removed, copying
// all rows and re-creating the table's named indexes (matching unique
// ones downgraded) and triggers. Runs as one transaction; the
// intermediate states are invisible to other connections.
func rebuildTableStripped(conn *sqlitebridge.Conn, table string, sets [][]string) error {
	origSQL, err := masterSQL(conn, "table", table)
	if err != nil {
		return err
	}
	createSQL, err := strippedCreateTableSQL(origSQL, normalizeTmpName, sets)
	if err != nil {
		return fmt.Errorf("strip UNIQUE from %q: %w", table, err)
	}
	assoc, err := associatedObjectSQL(conn, table)
	if err != nil {
		return err
	}
	cols, err := storedColumnList(conn, table)
	if err != nil {
		return err
	}
	// The copy must not fire FK actions or violate enforcement while the
	// table is swapped out, and the swap-back needs legacy rename
	// semantics (see below). Both are connection-level and outlive the
	// transaction, so both are restored on every exit path — conn is the
	// long-lived producer helper, not a scratch connection.
	restore, err := setPragmas(conn, map[string]string{
		"foreign_keys":       "OFF",
		"legacy_alter_table": "ON",
	})
	if err != nil {
		return err
	}
	defer restore()
	qt, qtmp := sqlitebridge.QuoteIdent(table), sqlitebridge.QuoteIdent(normalizeTmpName)
	return inTxn(conn, func() error {
		// The tmp name is never dropped first. The rebuild is one
		// transaction, so it cannot leave a stale one behind — anything
		// sitting there is the application's, and CREATE fails on it
		// rather than deleting it.
		steps := []string{
			createSQL,
			fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", qtmp, cols, cols, qt),
			"DROP TABLE " + qt,
			// legacy_alter_table for the swap-back: modern ALTER re-parses
			// every trigger, and a trigger on ANOTHER table referencing
			// this one (e.g. a synth cascade trigger's DELETE FROM child)
			// errors while the table is transiently absent. Nothing
			// references the tmp name, so no rewriting is needed.
			fmt.Sprintf("ALTER TABLE %s RENAME TO %s", qtmp, qt),
		}
		for _, s := range steps {
			if err := conn.Exec(s); err != nil {
				return fmt.Errorf("%s: %w", firstWords(s, 3), err)
			}
		}
		for _, objSQL := range assoc {
			sql := objSQL
			if plain, matched, err := downgradeIfMatching(sql, sets); err != nil {
				return err
			} else if matched {
				sql = plain
			}
			if err := conn.Exec(sql); err != nil {
				return fmt.Errorf("re-create %s: %w", firstWords(sql, 4), err)
			}
		}
		return nil
	})
}

// normalizeTmpName is the transient table name used mid-rebuild. The
// underscore prefix keeps it out of the replicated namespace even if it
// ever leaks (it cannot: the rebuild is a single transaction). Nothing
// reserves the name, so the rebuild must never assume a table under it
// is its own — see rebuildTableStripped.
const normalizeTmpName = "_syzy_normalize_new"

// strippedCreateTableSQL parses a CREATE TABLE and re-renders it under
// newName with the UNIQUE constraints matching sets removed — both
// column-level (UNIQUE on a single matching column) and table-level
// (UNIQUE(...) over a matching tuple). rsql's renderer omits table
// options, so WITHOUT ROWID / STRICT are re-appended from the parse.
func strippedCreateTableSQL(sql, newName string, sets [][]string) (string, error) {
	stmt, err := rsql.NewParser(strings.NewReader(sql)).ParseStatement()
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	ct, ok := stmt.(*rsql.CreateTableStatement)
	if !ok {
		return "", fmt.Errorf("not a CREATE TABLE statement")
	}
	for _, col := range ct.Columns {
		kept := col.Constraints[:0]
		for _, c := range col.Constraints {
			if u, ok := c.(*rsql.UniqueConstraint); ok && len(u.Columns) == 0 &&
				matchesAnySet([]string{col.Name.Name}, sets) {
				continue
			}
			kept = append(kept, c)
		}
		col.Constraints = kept
	}
	keptTab := ct.Constraints[:0]
	for _, c := range ct.Constraints {
		if u, ok := c.(*rsql.UniqueConstraint); ok {
			if names := plainIndexedColumnNames(u.Columns); names != nil && matchesAnySet(names, sets) {
				continue
			}
		}
		keptTab = append(keptTab, c)
	}
	ct.Constraints = keptTab
	ct.Name = &rsql.Ident{Name: newName}
	out := ct.String()
	var opts []string
	if ct.Without.IsValid() {
		opts = append(opts, "WITHOUT ROWID")
	}
	if ct.Strict.IsValid() {
		opts = append(opts, "STRICT")
	}
	if len(opts) > 0 {
		out += " " + strings.Join(opts, ", ")
	}
	return out, nil
}

// plainIndexedColumnNames extracts plain column names from an indexed-
// column list; nil if any entry is an expression (expression indexes
// can never back a coordinated key).
func plainIndexedColumnNames(cols []*rsql.IndexedColumn) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		id, ok := c.X.(*rsql.Ident)
		if !ok {
			return nil
		}
		names[i] = id.Name
	}
	return names
}

// downgradeIfMatching re-renders sql without UNIQUE when it is a CREATE
// UNIQUE INDEX over columns matching sets; matched=false leaves other
// statements (triggers, plain or eventual-unique indexes) untouched.
func downgradeIfMatching(sql string, sets [][]string) (string, bool, error) {
	stmt, err := rsql.NewParser(strings.NewReader(sql)).ParseStatement()
	if err != nil {
		// Unparsable associated object: re-create verbatim rather than
		// fail the rebuild; it cannot be a UNIQUE index admission minted.
		return "", false, nil
	}
	ci, ok := stmt.(*rsql.CreateIndexStatement)
	if !ok || !ci.Unique.IsValid() {
		return "", false, nil
	}
	names := plainIndexedColumnNames(ci.Columns)
	if names == nil || !matchesAnySet(names, sets) {
		return "", false, nil
	}
	ci.Unique = rsql.Pos{}
	return ci.String(), true, nil
}

// associatedObjectSQL returns the SQL of table's named indexes and
// triggers (autoindexes have NULL sql and are excluded), in creation
// order.
func associatedObjectSQL(conn *sqlitebridge.Conn, table string) ([]string, error) {
	stmt, _, err := conn.Prepare(
		`SELECT sql FROM sqlite_master WHERE tbl_name = ? AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY rowid`)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, table); err != nil {
		return nil, err
	}
	var out []string
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return out, nil
		}
		out = append(out, stmt.ColumnText(0))
	}
}

// storedColumnList returns table's stored (non-generated, non-hidden)
// columns as a quoted, comma-joined list for the rebuild's copy.
func storedColumnList(conn *sqlitebridge.Conn, table string) (string, error) {
	stmt, _, err := conn.Prepare(`SELECT name FROM pragma_table_xinfo(?) WHERE hidden = 0 ORDER BY cid`)
	if err != nil {
		return "", err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, table); err != nil {
		return "", err
	}
	var cols []string
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return "", err
		}
		if !hasRow {
			break
		}
		cols = append(cols, sqlitebridge.QuoteIdent(stmt.ColumnText(0)))
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("table %q has no stored columns", table)
	}
	return strings.Join(cols, ", "), nil
}

// masterSQL returns the sqlite_master SQL of one object.
func masterSQL(conn *sqlitebridge.Conn, typ, name string) (string, error) {
	stmt, _, err := conn.Prepare(`SELECT sql FROM sqlite_master WHERE type = ? AND name = ?`)
	if err != nil {
		return "", err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, typ); err != nil {
		return "", err
	}
	if err := stmt.BindText(2, name); err != nil {
		return "", err
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return "", err
	}
	if !hasRow {
		return "", fmt.Errorf("%s %q not in sqlite_master", typ, name)
	}
	return stmt.ColumnText(0), nil
}

// setPragmas applies name→value connection pragmas, returning a func
// that puts each back to the value it read beforehand. Pragmas are not
// transactional, so a rollback does not undo them; the caller must. A
// partial failure restores what it already set before returning.
func setPragmas(conn *sqlitebridge.Conn, want map[string]string) (func(), error) {
	prior := map[string]string{}
	restore := func() {
		for name, cur := range prior {
			_ = conn.Exec("PRAGMA " + name + " = " + cur)
		}
	}
	for name, val := range want {
		cur, err := readPragma(conn, name)
		if err != nil {
			restore()
			return nil, err
		}
		if err := conn.Exec("PRAGMA " + name + " = " + val); err != nil {
			restore()
			return nil, err
		}
		prior[name] = cur
	}
	return restore, nil
}

func readPragma(conn *sqlitebridge.Conn, name string) (string, error) {
	stmt, _, err := conn.Prepare("PRAGMA " + name)
	if err != nil {
		return "", err
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil {
		return "", err
	}
	if !hasRow {
		return "", fmt.Errorf("PRAGMA %s returned no row", name)
	}
	return stmt.ColumnText(0), nil
}

// inTxn runs fn inside BEGIN IMMEDIATE … COMMIT on conn, rolling back
// on error.
func inTxn(conn *sqlitebridge.Conn, fn func() error) error {
	if err := conn.Exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	if err := fn(); err != nil {
		_ = conn.Exec("ROLLBACK")
		return err
	}
	return conn.Exec("COMMIT")
}

// firstWords returns the first n whitespace-separated words of s, for
// terse error context.
func firstWords(s string, n int) string {
	f := strings.Fields(s)
	if len(f) > n {
		f = f[:n]
	}
	return strings.Join(f, " ")
}
