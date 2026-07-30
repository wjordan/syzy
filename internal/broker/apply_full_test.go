package broker

import (
	"fmt"
	"strings"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

func pragmaInt64(t testing.TB, conn *sqlitebridge.Conn, name string) int64 {
	t.Helper()
	stmt, _, err := conn.Prepare("PRAGMA " + name)
	if err != nil {
		t.Fatalf("prepare PRAGMA %s: %v", name, err)
	}
	defer stmt.Finalize()
	if row, err := stmt.Step(); err != nil || !row {
		t.Fatalf("step PRAGMA %s: row=%v err=%v", name, row, err)
	}
	return stmt.ColumnInt64(0)
}

func TestApplyFullRepairsTransactionState(t *testing.T) {
	t.Parallel()
	f := newApplierSchema(t, 1, nil,
		`CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT NOT NULL)`)
	remote := crdt.Origin(7)
	stamp := func(seq crdt.Seq) crdt.Stamp {
		return crdt.Stamp{Origin: remote, Clock: crdt.Clock{WallTime: int64(seq)}}
	}

	// Prevent app.db from allocating another page, then apply a row large enough
	// to require one. SQLite reports SQLITE_FULL and may auto-roll back the
	// transaction.
	pages := pragmaInt64(t, f.app, "page_count")
	if err := f.app.Exec(fmt.Sprintf("PRAGMA max_page_count = %d", pages)); err != nil {
		t.Fatalf("cap database pages: %v", err)
	}
	full := buildInsert(t, f.tab, crdt.Dot{Origin: remote, Seq: 1}, stamp(1), 1,
		[]byte{0x01}, strings.Repeat("x", 256<<10))
	err := f.br.applyPayloadCache(full, full.Encoded(), false)
	if !sqlitebridge.IsCode(err, sqlitebridge.ResultFull) {
		t.Fatalf("apply error = %v; want SQLITE_FULL", err)
	}
	if !retryableApplyError(err) {
		t.Fatalf("SQLITE_FULL must be retained for retry: %v", err)
	}
	if !f.app.InAutocommit() {
		t.Fatal("apply connection left in a transaction after SQLITE_FULL")
	}

	if err := f.app.Exec("PRAGMA max_page_count = 1073741823"); err != nil {
		t.Fatalf("restore database page cap: %v", err)
	}

	// Exercise another failed transaction after the auto-rollback. This was the
	// delayed poison point: a cached ROLLBACK could return its previous error
	// from Reset without executing, leaving this transaction open.
	badBase := buildInsert(t, f.tab, crdt.Dot{Origin: remote, Seq: 2}, stamp(2), 1,
		[]byte{0x02}, "placeholder")
	badInsert := badBase.Records[0].(crdt.Insert)
	nCol, _ := f.tab.Column("n")
	badInsert.Image = []crdt.ColValue{{Column: nCol.ID, TypeTag: crdt.ColNull}}
	bad, err := crdt.Build(badBase.Dot, badBase.Stamp, nil, testCluster,
		[]crdt.Record{badInsert})
	if err != nil {
		t.Fatalf("build bad row: %v", err)
	}
	if err := f.br.applyPayloadCache(bad, bad.Encoded(), false); err == nil {
		t.Fatal("missing NOT NULL value unexpectedly applied")
	}
	if !f.app.InAutocommit() {
		t.Fatal("apply connection left in a transaction after constraint failure")
	}

	good := buildInsert(t, f.tab, crdt.Dot{Origin: remote, Seq: 3}, stamp(3), 1,
		[]byte{0x03}, "healthy")
	if err := f.br.applyPayloadCache(good, good.Encoded(), false); err != nil {
		t.Fatalf("apply after recovered disk space: %v", err)
	}
}
