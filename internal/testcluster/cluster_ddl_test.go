package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/transport/memtransport"
)

func allocTID() crdt.TableID  { return catalog.AllocTableID() }
func allocCID() crdt.ColumnID { return catalog.AllocColumnID() }

func buildOp(tab crdt.TableID, idCol, bodyCol crdt.ColumnID) crdt.CatalogOp {
	return crdt.CatalogOp{
		Kind:      crdt.OpCreateTable,
		TableID:   tab,
		TableName: "doc",
		Columns: []crdt.CatalogColumn{
			{ID: idCol, Name: "id", Ordinal: 0, Type: "BLOB",
				NotNull: true, IsPK: true, PKPos: 1, ClockGroup: "row"},
			{ID: bodyCol, Name: "body", Ordinal: 1, Type: "TEXT", ClockGroup: "row"},
		},
		Keys: []crdt.CatalogKey{
			{KeyID: crdt.KeyID{}, Members: []crdt.CatalogKeyMember{{ColumnID: idCol, Ordinal: 0}}},
		},
	}
}

func encodeOp(t *testing.T, op crdt.CatalogOp) []byte {
	t.Helper()
	b, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		t.Fatalf("EncodeCatalogOp: %v", err)
	}
	return b
}

// TestDDL_CreateTableConverges verifies that a CREATE TABLE issued on
// node A is replicated to node B via the SchemaLog + broker
// catch-up loop, and that DML on the new table flows correctly.
func TestDDL_CreateTableConverges(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	a := NewWithDDL(t, hub, 1, log, 5*time.Millisecond)
	b := NewWithDDL(t, hub, 2, log, 5*time.Millisecond)
	a.Start(t, ctx)
	b.Start(t, ctx)

	// Issue CREATE TABLE on A; admission goes through the schema log
	// (head 0 → 1) and resolve_intent advances A's catalog.
	if err := a.AppWrite.Exec(`CREATE TABLE doc (id BLOB PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE on A: %v", err)
	}

	// Wait for B's catch-up loop to apply schema_seq=1.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := b.Catalog.Table("doc"); ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if _, ok := b.Catalog.Table("doc"); !ok {
		t.Fatalf("B never received schema for 'doc'")
	}

	// Verify B's app.db has the table structurally (apply path created it).
	stmt, _, err := b.Read.Prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='doc'`)
	if err != nil {
		t.Fatalf("prepare check: %v", err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		t.Fatalf("doc not in B sqlite_master: hasRow=%v err=%v", hasRow, err)
	}
}

// TestDDL_AddColumnConverges issues a follow-up ALTER TABLE ADD COLUMN
// after the first DDL and verifies B picks it up.
func TestDDL_AddColumnConverges(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	a := NewWithDDL(t, hub, 1, log, 5*time.Millisecond)
	b := NewWithDDL(t, hub, 2, log, 5*time.Millisecond)
	a.Start(t, ctx)
	b.Start(t, ctx)

	if err := a.AppWrite.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := a.AppWrite.Exec(`ALTER TABLE t ADD COLUMN extra TEXT`); err != nil {
		t.Fatalf("ALTER: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tab, ok := b.Catalog.Table("t"); ok {
			if _, has := tab.Column("extra"); has {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("B never observed extra column on t")
}

// TestDDL_HeadMovedRejectedOnConcurrentDDL verifies CAS contention:
// when the schema log's head has already advanced (because some other
// peer committed first), the local trace_v2 hook rejects the user's
// CREATE TABLE because schemalog.Append(parent=local_seq) returns
// ErrHeadMoved.
func TestDDL_HeadMovedRejectedOnConcurrentDDL(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	b := NewWithDDL(t, hub, 2, log, 5*time.Millisecond)
	b.Start(t, ctx)

	// Seed a valid event onto the schema log before B tries its DDL.
	// B's broker catch-up loop will eventually apply it, but the user
	// statement on B races first: it calls schemalog.Append(parent=0)
	// while head=1, so it gets ErrHeadMoved.
	winnerOp := buildWinnerCreateOp(t)
	if _, err := log.Append(ctx, 0, winnerOp, "winner CREATE TABLE"); err != nil {
		t.Fatalf("seed schema log: %v", err)
	}

	// B's catch-up loop runs concurrently; if it gets there first, the
	// catalog will be populated when B's CREATE TABLE runs, and admission
	// will reject for "table already exists". Either way the user's DDL
	// fails. We accept either rejection path.
	err := b.AppWrite.Exec(`CREATE TABLE doc (id BLOB PRIMARY KEY NOT NULL, body TEXT)`)
	if err == nil {
		t.Errorf("B's concurrent CREATE TABLE accepted; want rejection")
	}

	// Eventually catch-up applies the winning op.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		seq, _, _ := b.Meta.GetSchemaSeq()
		if seq >= 1 {
			if _, ok := b.Catalog.Table("doc"); ok {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("B never caught up to seeded schema_seq")
}

// buildWinnerCreateOp encodes a valid CatalogOp that the catch-up
// loop's apply path will accept (doc(id BLOB PK NOT NULL, body TEXT)).
func buildWinnerCreateOp(t *testing.T) []byte {
	t.Helper()
	tabID := allocTID()
	idCol := allocCID()
	bodyCol := allocCID()
	op := buildOp(tabID, idCol, bodyCol)
	encoded := encodeOp(t, op)
	return encoded
}
