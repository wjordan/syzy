package broker

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/sqlitebridge"
)

// buildUniqueUpdate builds an inbound Update touching ONLY the non-unique
// column n — the shape that makes arbitration read the live slug via the
// cached readKeyColumns SELECT.
func buildUniqueUpdate(t testing.TB, a *uniqueApplier, dot crdt.Dot, stamp crdt.Stamp, idVal []byte, name string) *crdt.Changeset {
	t.Helper()
	tab := a.tab
	idCol := tab.PK[0].ID
	pk, err := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{
		idCol: blobCol(idCol, idVal),
	})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	nCol, _ := tab.Column("n")
	rec := crdt.Update{
		Table: tab.ID, PK: pk, CL: 1,
		Changed: []crdt.ColValue{textCol(nCol.ID, name)},
	}
	cs, err := crdt.Build(dot, stamp, nil, testCluster, []crdt.Record{rec})
	if err != nil {
		t.Fatalf("crdt.Build: %v", err)
	}
	return cs
}

// TestUniquePartialUpdate_NoPendingStmtWedge is the regression test for the
// prod inbound-apply freeze: applying an Update that does not touch the
// unique column runs readKeyColumns, whose cached `SELECT ... LIMIT 1` was
// stepped to SQLITE_ROW and abandoned. The pending statement pins a read
// snapshot on AppApply; after any other connection advances the WAL, every
// subsequent BEGIN IMMEDIATE on AppApply fails SQLITE_BUSY (BUSY_SNAPSHOT,
// "database is locked") forever — applyPayloadWithRetry then retries one
// payload for hours and inbound apply is frozen until process restart.
func TestUniquePartialUpdate_NoPendingStmtWedge(t *testing.T) {
	t.Parallel()
	a := newUniqueApplier(t, 1)
	// Add a composite UNIQUE(slug, n) key: an Update touching only n then
	// needs the live slug value, which is exactly the readKeyColumns path.
	// (The single-column UNIQUE(slug) key short-circuits when slug is not
	// in Changed and never reaches the live read.)
	slugCol, _ := a.tab.Column("slug")
	nCol, _ := a.tab.Column("n")
	keyID2 := crdt.KeyID{0x02}
	if err := a.br.cfg.Meta.WithTx(func(tx *metadata.Tx) error {
		if err := tx.UpsertKey(metadata.KeyEntry{
			TableID: a.tab.ID, KeyID: keyID2, ColumnID: slugCol.ID,
			Ordinal: 0, State: metadata.StateActive, CreateSeq: 0,
		}); err != nil {
			return err
		}
		return tx.UpsertKey(metadata.KeyEntry{
			TableID: a.tab.ID, KeyID: keyID2, ColumnID: nCol.ID,
			Ordinal: 1, State: metadata.StateActive, CreateSeq: 0,
		})
	}); err != nil {
		t.Fatalf("UpsertKey composite: %v", err)
	}
	if err := a.cat.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	a.tab, _ = a.cat.Table("u")

	remote := crdt.Origin(2)
	clk := func(w int64) crdt.Clock { return crdt.Clock{WallTime: w} }
	stamp := func(w int64) crdt.Stamp { return crdt.Stamp{Origin: remote, Clock: clk(w)} }

	// Seed the row via a normal inbound insert.
	ins := buildUniqueInsert(t, a.tab, crdt.Dot{Origin: remote, Seq: 1}, stamp(100),
		[]byte{0xAA}, "slug-1", "n-1")
	if err := a.br.applyPayloadCache(ins, ins.Encoded(), false); err != nil {
		t.Fatalf("apply insert: %v", err)
	}

	// Inbound partial update: Changed = {n} only. Arbitration reads the
	// live slug via readKeyColumns. With the bug the SELECT stays pending
	// at SQLITE_ROW after the apply commits.
	upd := buildUniqueUpdate(t, a, crdt.Dot{Origin: remote, Seq: 2}, stamp(200), []byte{0xAA}, "n-2")
	if err := a.br.applyPayloadCache(upd, upd.Encoded(), false); err != nil {
		t.Fatalf("apply partial update: %v", err)
	}

	// Another connection (the producer in prod) advances the WAL, making
	// the pinned snapshot stale.
	dbPath := dbFileOf(t, a.app)
	w, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer w.Close()
	if err := w.Exec(`INSERT INTO u (id, slug, n) VALUES (x'BB', 'slug-b', 'n-b')`); err != nil {
		t.Fatalf("writer insert: %v", err)
	}

	// The next inbound apply must not fail "database is locked".
	upd3 := buildUniqueUpdate(t, a, crdt.Dot{Origin: remote, Seq: 3}, stamp(300), []byte{0xAA}, "n-3")
	if err := a.br.applyPayloadCache(upd3, upd3.Encoded(), false); err != nil {
		if strings.Contains(err.Error(), "database is locked") {
			t.Fatalf("INBOUND APPLY WEDGED by pending readKeyColumns stmt: %v", err)
		}
		t.Fatalf("apply seq=3: %v", err)
	}
}

// dbFileOf resolves the main database file path of an open connection.
func dbFileOf(t testing.TB, c *sqlitebridge.Conn) string {
	t.Helper()
	stmt, _, err := c.Prepare(`SELECT file FROM pragma_database_list WHERE name='main'`)
	if err != nil {
		t.Fatalf("pragma_database_list: %v", err)
	}
	defer stmt.Finalize()
	has, err := stmt.Step()
	if err != nil || !has {
		t.Fatalf("database_list step: has=%v err=%v", has, err)
	}
	return stmt.ColumnText(0)
}

var _ = context.Background
var _ = filepath.Join
