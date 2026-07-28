package sqlite

import (
	"fmt"
	"math"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/unique"
)

// enumerateCoordinatedClaims snapshots the local replica for the
// leaseholder: every active coordinated key identity — including keys
// with no participating rows, which must still become servable — plus
// each key's live (value → owning row) claims. Full replication makes
// the local rows authoritative, so any node can derive the reservation
// state this way every maintenance tick. The Value/Owner encodings match
// the producer's commit-time reserve path exactly
// (catalog.EncodeKeyFromSlice / EncodePKFromSlice), so reservations made
// by writers and the derived taken-set agree byte-for-byte.
//
// conn must be a dedicated read connection — the leaseholder's maintenance
// goroutine is its only user, never shared with the broker's apply conn.
func enumerateCoordinatedClaims(cat *catalog.Catalog, conn *sqlitebridge.Conn) (unique.Snapshot, error) {
	var snap unique.Snapshot
	for _, tab := range cat.Tables() {
		for _, uk := range tab.UniqueKeys {
			if !uk.Coordinated {
				continue
			}
			snap.Keys = append(snap.Keys, unique.KeyRef{
				Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID),
			})
			tabClaims, err := enumerateKeyClaims(conn, tab, uk)
			if err != nil {
				return unique.Snapshot{}, err
			}
			snap.Claims = append(snap.Claims, tabClaims...)
		}
	}
	return snap, nil
}

func enumerateKeyClaims(conn *sqlitebridge.Conn, tab *catalog.Table, uk catalog.UniqueKey) ([]unique.Claim, error) {
	// Select the union of PK and key columns; place each into a positional
	// row image (t.Columns order) for the canonical encoders.
	idxByID := make(map[crdt.ColumnID]int, len(tab.Columns))
	for i, c := range tab.Columns {
		idxByID[c.ID] = i
	}
	type sel struct {
		idx  int
		id   crdt.ColumnID
		name string
	}
	var selected []sel
	seen := map[crdt.ColumnID]struct{}{}
	add := func(c catalog.Column) {
		if _, dup := seen[c.ID]; dup {
			return
		}
		seen[c.ID] = struct{}{}
		selected = append(selected, sel{idx: idxByID[c.ID], id: c.ID, name: c.Name})
	}
	for _, c := range tab.PK {
		add(c)
	}
	for _, c := range uk.Columns {
		add(c)
	}

	cols := make([]string, len(selected))
	var notNull []string
	keyIDs := map[crdt.ColumnID]struct{}{}
	for _, c := range uk.Columns {
		keyIDs[c.ID] = struct{}{}
	}
	for i, s := range selected {
		cols[i] = sqlitebridge.QuoteIdent(s.name)
		if _, isKey := keyIDs[s.id]; isKey {
			notNull = append(notNull, sqlitebridge.QuoteIdent(s.name)+" IS NOT NULL")
		}
	}
	where := strings.Join(notNull, " AND ")
	// Partial key: AND the index predicate so the rebuilt taken-set holds
	// exactly the participating rows. Rendered against current column names
	// (the predicate stores ColumnIDs, so it survives renames).
	if uk.Predicate.Root != nil {
		missing := false
		predSQL, err := uk.Predicate.SQL(func(id crdt.ColumnID) string {
			c, ok := tab.ColumnByID(id)
			if !ok {
				missing = true
				return "NULL"
			}
			return sqlitebridge.QuoteIdent(c.Name)
		})
		if err != nil {
			return nil, fmt.Errorf("unique enumerate %q: render predicate: %w", tab.Name, err)
		}
		if missing {
			return nil, fmt.Errorf("unique enumerate %q: partial predicate references a column absent from the catalog", tab.Name)
		}
		where += " AND " + predSQL
	}
	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s`,
		strings.Join(cols, ", "), sqlitebridge.QuoteIdent(tab.Name), where)

	stmt, _, err := conn.Prepare(sql)
	if err != nil {
		return nil, fmt.Errorf("unique enumerate %q: prepare: %w", tab.Name, err)
	}
	defer stmt.Finalize()

	var out []unique.Claim
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, fmt.Errorf("unique enumerate %q: step: %w", tab.Name, err)
		}
		if !hasRow {
			break
		}
		image := make([]crdt.ColValue, len(tab.Columns))
		for i, c := range tab.Columns {
			image[i] = crdt.ColValue{TypeTag: crdt.ColNull, Column: c.ID}
		}
		for i, s := range selected {
			image[s.idx] = colValueFromStmt(stmt, i, s.id)
		}
		pk, err := tab.EncodePKFromSlice(nil, image)
		if err != nil {
			return nil, fmt.Errorf("unique enumerate %q: pk encode: %w", tab.Name, err)
		}
		value, hasNull, err := tab.EncodeKeyFromSlice(uk, image)
		if err != nil {
			return nil, fmt.Errorf("unique enumerate %q: key encode: %w", tab.Name, err)
		}
		if hasNull {
			continue // defensive: WHERE already excludes NULL tuples
		}
		out = append(out, unique.Claim{
			Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID),
			Value: value, Owner: pk,
		})
	}
	return out, nil
}

// colValueFromStmt reads column i of stmt into a crdt.ColValue whose Bytes
// match the producer's capture encoding (8-byte BE int/real, raw
// text/blob), so reserve and rebuild produce identical reservation values.
func colValueFromStmt(stmt *sqlitebridge.Stmt, i int, id crdt.ColumnID) crdt.ColValue {
	switch stmt.ColumnType(i) {
	case sqlitebridge.ColumnInt:
		b := make([]byte, 8)
		v := uint64(stmt.ColumnInt64(i))
		for k := 7; k >= 0; k-- {
			b[k] = byte(v)
			v >>= 8
		}
		return crdt.ColValue{TypeTag: crdt.ColInt, Bytes: b, Column: id}
	case sqlitebridge.ColumnReal:
		b := make([]byte, 8)
		bits := math.Float64bits(stmt.ColumnFloat64(i))
		for k := 7; k >= 0; k-- {
			b[k] = byte(bits)
			bits >>= 8
		}
		return crdt.ColValue{TypeTag: crdt.ColReal, Bytes: b, Column: id}
	case sqlitebridge.ColumnText:
		return crdt.ColValue{TypeTag: crdt.ColText, Bytes: []byte(stmt.ColumnText(i)), Column: id}
	case sqlitebridge.ColumnBlob:
		return crdt.ColValue{TypeTag: crdt.ColBlob, Bytes: append([]byte(nil), stmt.ColumnBlob(i)...), Column: id}
	default:
		return crdt.ColValue{TypeTag: crdt.ColNull, Column: id}
	}
}
