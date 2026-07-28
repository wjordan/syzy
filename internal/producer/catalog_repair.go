package producer

// Catalog repair: drop metadata unique keys the SQLite schema does not
// back — duplicates (legacy pre-guard admissions; a duplicate total key
// permanently reserves soft-deleted values) and orphans (DROP INDEX has
// no catalog effect on the SQLite engine). The desired key set is
// recomputed with the same helpers admission uses, so a healthy catalog
// is a no-op. Node-local and deterministic (same schema + same rows +
// KeyID-based winner ⇒ same result on every replica); reservations under
// a dropped key become row-less at the leaseholder's next enumerate and
// exit through its release hold.
//
// Coordinated keys are metadata-authoritative: no node holds a physical
// UNIQUE index for one (receivers never did; the originator normalizes
// its own away — see normalizeCoordinatedIndexes), so physical backing
// says nothing about their validity and they are never orphan-dropped
// here. Their removal path is the replicated key-removal op emitted for
// a matching DROP INDEX. Repair still collapses exact coordinated
// duplicates, and still heals the legacy poison class: predicate-less
// coordinated keys shadowing a partial coordinated key on the same
// member columns (pre-guard `CREATE UNIQUE INDEX IF NOT EXISTS` replays
// that lost the predicate). Admission rejects new keys of that shape,
// so the heuristic can't misfire on a legitimately-created key.

import (
	"bytes"
	"fmt"
	"log/slog"
	"sort"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// RepairStats summarizes one repair pass.
type RepairStats struct {
	Dropped int // keys marked dropped (duplicates + orphans)
}

// existingKey is one active non-PK key group assembled from syzy_key rows.
type existingKey struct {
	id   crdt.KeyID
	rows []metadata.KeyEntry // sorted by ordinal
}

// RepairUniqueKeys drops active non-PK unique keys the SQLite schema does
// not back. Call before the unique leaseholder starts enumerating, and
// after schema skew has been reconciled — on a node whose app.db is
// behind its catalog, a not-yet-applied index would misread as an
// orphaned key. Idempotent; a healthy catalog is a no-op.
func RepairUniqueKeys(app *sqlitebridge.Conn, cat *catalog.Catalog, meta *metadata.Store, log *slog.Logger) (RepairStats, error) {
	var stats RepairStats
	snap, err := meta.LoadCatalogSnapshot()
	if err != nil {
		return stats, fmt.Errorf("catalog repair: load snapshot: %w", err)
	}
	seq, _, err := meta.GetSchemaSeq()
	if err != nil {
		return stats, fmt.Errorf("catalog repair: schema seq: %w", err)
	}

	// Group active non-PK key rows by (table, key). A single dropped
	// marker row drops the whole key (mirrors catalog.Reload).
	type tableKey struct {
		t crdt.TableID
		k crdt.KeyID
	}
	groups := map[tableKey][]metadata.KeyEntry{}
	droppedKeys := map[tableKey]struct{}{}
	for _, ke := range snap.Keys {
		if ke.KeyID == metadata.PKKeyID {
			continue
		}
		tk := tableKey{ke.TableID, ke.KeyID}
		if ke.State == metadata.StateDropped {
			droppedKeys[tk] = struct{}{}
			continue
		}
		groups[tk] = append(groups[tk], ke)
	}
	for tk := range droppedKeys {
		delete(groups, tk)
	}

	var drops []crdt.KeyID
	var dropsTable []crdt.TableID

	for _, tab := range cat.Tables() {
		desired, err := desiredUniqueKeys(app, tab)
		if err != nil {
			// Conservative: leave this table's keys untouched. The usual
			// cause is schema skew this node hasn't healed; the next open
			// retries.
			log.Warn("catalog repair: skipping table; cannot derive desired keys",
				"table", tab.Name, "err", err)
			continue
		}
		// Assemble this table's existing keys and bucket them by signature.
		existingBySig := map[string][]existingKey{}
		// predicatedMembers marks the member tuples of coordinated keys
		// that carry a predicate — the shadow test for the poison rule.
		predicatedMembers := map[string]bool{}
		for tk, rows := range groups {
			if tk.t != tab.ID {
				continue
			}
			sortKeyRowsByOrdinal(rows)
			sig := existingSig(rows)
			existingBySig[sig] = append(existingBySig[sig], existingKey{id: tk.k, rows: rows})
			if rows[0].Coordinated && rowsHavePredicate(rows) {
				predicatedMembers[memberSig(rows)] = true
			}
		}

		for sig, keys := range existingBySig {
			rows := keys[0].rows
			coordinated := rows[0].Coordinated
			// Coordinated keys have no physical backing by design, so
			// `desired` cannot vouch for them; they are kept unless they are
			// exact duplicates or the legacy predicate-less poison shadowed
			// by a partial key on the same members.
			poison := coordinated && !rowsHavePredicate(rows) && predicatedMembers[memberSig(rows)]
			wanted := (coordinated && !poison) || desired[sig]
			// Winner selection uses KeyID bytes only: KeyIDs come from
			// replicated events, so they are identical on every replica —
			// unlike e.g. create_seq, which is node-local.
			sort.Slice(keys, func(i, j int) bool {
				return bytes.Compare(keys[i].id[:], keys[j].id[:]) < 0
			})
			start := 0
			if wanted {
				start = 1 // keep the winner
			}
			for _, k := range keys[start:] {
				reason := "orphan"
				switch {
				case wanted:
					reason = "duplicate"
				case poison:
					reason = "predicate-less shadow of a partial key"
				}
				log.Warn("catalog repair: dropping unique key",
					"table", tab.Name, "key_id", fmt.Sprintf("%x", k.id[:]), "reason", reason)
				drops = append(drops, k.id)
				dropsTable = append(dropsTable, tab.ID)
			}
		}
	}

	if len(drops) == 0 {
		return stats, nil
	}
	err = meta.WithTx(func(tx *metadata.Tx) error {
		for i, id := range drops {
			// Same shape as catApplyDropUniqueKey: one dropped marker row
			// at ordinal 0; the active-state filter suppresses the rest.
			if err := tx.UpsertKey(metadata.KeyEntry{
				TableID: dropsTable[i], KeyID: id,
				Ordinal: 0, State: metadata.StateDropped,
				CreateSeq: 0, DropSeq: seq,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return stats, fmt.Errorf("catalog repair: write: %w", err)
	}
	stats.Dropped = len(drops)
	if err := cat.Reload(); err != nil {
		return stats, fmt.Errorf("catalog repair: reload: %w", err)
	}
	return stats, nil
}

// desiredUniqueKeys derives, as key signatures, the unique keys admission
// would produce for tab's current SQLite schema: unique indexes (origin
// 'c') and UNIQUE constraints (origin 'u'); the PK is excluded (origin
// 'pk' / PKKeyID). Indexes admission would reject (expression members,
// non-BINARY members, eventual partial) are skipped — they never made a
// key.
func desiredUniqueKeys(app *sqlitebridge.Conn, tab *catalog.Table) (map[string]bool, error) {
	// pragma_index_list on a missing table yields zero rows, NOT an error —
	// which would read as "no indexes" and orphan-drop every key on a table
	// app.db doesn't have yet (schema skew). Fail the table instead.
	exists, err := sqlitebridge.ObjectExists(app, "table", tab.Name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("table %q not in sqlite_master", tab.Name)
	}
	type idx struct {
		name    string
		partial bool
		sql     string
	}
	stmt, _, err := app.Prepare(`SELECT il.name, il.partial, COALESCE(m.sql, '')
FROM pragma_index_list(?) il
LEFT JOIN sqlite_master m ON m.name = il.name AND m.type = 'index'
WHERE il."unique" = 1 AND il.origin IN ('c', 'u')`)
	if err != nil {
		return nil, err
	}
	var idxs []idx
	if err := func() error {
		defer stmt.Finalize()
		if err := stmt.BindText(1, tab.Name); err != nil {
			return err
		}
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			idxs = append(idxs, idx{
				name:    stmt.ColumnText(0),
				partial: stmt.ColumnInt64(1) != 0,
				sql:     stmt.ColumnText(2),
			})
		}
	}(); err != nil {
		return nil, err
	}

	out := map[string]bool{}
	for _, ix := range idxs {
		names, err := indexColumnNames(app, ix.name)
		if err != nil {
			return nil, err
		}
		if names == nil {
			continue // expression/rowid member: not admissible
		}
		// validateUniqueColumnsOnTable / compilePartialPredicate mix semantic
		// rejection with operational failures (pragma Prepare/Step errors); a
		// transient error misread as "not admissible" would orphan-drop a
		// healthy key here and re-synthesize it under a divergent KeyID on
		// the next open. Any error fails the whole table — conservative: a
		// table with one truly non-admissible unique index (which never
		// minted a key) goes unrepaired rather than mis-repaired.
		coordinated, err := validateUniqueColumnsOnTable(app, tab, names)
		if err != nil {
			return nil, fmt.Errorf("validate index %q: %w", ix.name, err)
		}
		var predicate []byte
		if ix.partial {
			if !coordinated {
				continue // eventual partial: not admissible
			}
			p, err := classifyDDL(ix.sql)
			if err != nil {
				return nil, fmt.Errorf("parse partial index %q: %w", ix.name, err)
			}
			if p.Kind != ddlCreateUniqueIndex || p.WherePred == nil {
				return nil, fmt.Errorf("parse partial index %q: unexpected statement shape", ix.name)
			}
			pred, err := compilePartialPredicate(p.WherePred, app, tab)
			if err != nil {
				return nil, fmt.Errorf("compile predicate of index %q: %w", ix.name, err)
			}
			predicate = crdt.EncodeUniquePredicate(pred)
		}
		members, err := keyMembersFromColumnNames(tab, names)
		if err != nil {
			return nil, err
		}
		if err := validateBinaryUniqueMembers(members, func(id crdt.ColumnID) (crdt.Collation, bool) {
			c, ok := tab.ColumnByID(id)
			return c.Collation, ok
		}, ix.name); err != nil {
			continue // not admissible
		}
		out[keySig(members, coordinated, predicate)] = true
	}
	return out, nil
}

// indexColumnNames returns the key columns of an index in seqno order,
// or nil if any member is an expression or rowid (name IS NULL).
func indexColumnNames(app *sqlitebridge.Conn, index string) ([]string, error) {
	stmt, _, err := app.Prepare(
		`SELECT name, name IS NULL FROM pragma_index_info(?) ORDER BY seqno`)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, index); err != nil {
		return nil, err
	}
	var names []string
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return names, nil
		}
		if stmt.ColumnInt64(1) != 0 {
			return nil, nil
		}
		names = append(names, stmt.ColumnText(0))
	}
}

// keySig is the matching signature of a key: member count, member
// ColumnIDs in key order, the coordinated flag, and the encoded
// predicate. The count delimits the fixed-size members from the
// variable-length predicate tail.
func keySig(members []crdt.CatalogKeyMember, coordinated bool, pred []byte) string {
	var b bytes.Buffer
	b.WriteByte(byte(len(members)))
	for _, m := range members {
		b.Write(m.ColumnID[:])
	}
	if coordinated {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
	b.Write(pred)
	return b.String()
}

// existingSig computes the signature of an assembled key group. The
// stored predicate is normalized through a decode/encode round-trip so
// byte comparison is against the current encoder; an undecodable
// predicate yields a signature matching nothing (the key reads as an
// orphan and is dropped).
func existingSig(rows []metadata.KeyEntry) string {
	members := make([]crdt.CatalogKeyMember, len(rows))
	for i, r := range rows {
		members[i] = crdt.CatalogKeyMember{ColumnID: r.ColumnID, Ordinal: r.Ordinal}
	}
	pred := rows[0].Predicate
	if len(pred) > 0 {
		if p, err := crdt.DecodeUniquePredicate(pred); err != nil {
			pred = []byte("\x00undecodable")
		} else if p.Root == nil {
			pred = nil // explicitly-encoded empty predicate ≡ total key
		} else {
			pred = crdt.EncodeUniquePredicate(p)
		}
	} else {
		pred = nil
	}
	return keySig(members, rows[0].Coordinated, pred)
}

// memberSig is the member-columns-only prefix of keySig: count byte plus
// ColumnIDs in ordinal order. Two keys share it iff they constrain the
// same column tuple, regardless of mode or predicate.
func memberSig(rows []metadata.KeyEntry) string {
	var b bytes.Buffer
	b.WriteByte(byte(len(rows)))
	for _, r := range rows {
		b.Write(r.ColumnID[:])
	}
	return b.String()
}

// rowsHavePredicate reports whether an assembled key group carries a
// decodable, non-empty predicate (an explicitly-encoded empty predicate
// is a total key; an undecodable one matches nothing, like existingSig).
func rowsHavePredicate(rows []metadata.KeyEntry) bool {
	if len(rows[0].Predicate) == 0 {
		return false
	}
	p, err := crdt.DecodeUniquePredicate(rows[0].Predicate)
	return err == nil && p.Root != nil
}

func sortKeyRowsByOrdinal(rows []metadata.KeyEntry) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Ordinal < rows[j].Ordinal })
}
