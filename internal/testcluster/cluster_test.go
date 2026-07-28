package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/transport/memtransport"
)

const eventSchema = `CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`

// TestDMLConvergence drives a real INSERT/UPDATE/DELETE on Node A's app.db
// and asserts B's state converges via the live transport. No fixed sleeps;
// WaitApplied is signal-driven via broker.OnApplied.
func TestDMLConvergence(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(t, hub, 1, eventSchema, 0)
	b := NewWithCache(t, hub, 2, eventSchema, 0)
	a.Start(t, ctx)
	b.Start(t, ctx)

	if err := a.AppWrite.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'hello')`); err != nil {
		t.Fatalf("A INSERT: %v", err)
	}
	b.WaitApplied(t, a.Origin, 1, time.Second)
	if got := readN(t, b.Read, []byte{0x01}); got != "hello" {
		t.Fatalf("after INSERT: B.event.n = %q; want hello", got)
	}

	if err := a.AppWrite.Exec(`UPDATE event SET n = 'world' WHERE id = x'01'`); err != nil {
		t.Fatalf("A UPDATE: %v", err)
	}
	b.WaitApplied(t, a.Origin, 2, time.Second)
	if got := readN(t, b.Read, []byte{0x01}); got != "world" {
		t.Fatalf("after UPDATE: B.event.n = %q; want world", got)
	}

	if err := a.AppWrite.Exec(`DELETE FROM event WHERE id = x'01'`); err != nil {
		t.Fatalf("A DELETE: %v", err)
	}
	b.WaitApplied(t, a.Origin, 3, time.Second)
	if rowExists(t, b.Read, []byte{0x01}) {
		t.Fatalf("after DELETE: row still present on B")
	}

	// Frontier and row_clock convergence on B mirror the dot/state from A.
	// State lives in B.Cache; the snapshotter would persist later.
	bf, ok := b.Cache.FrontierFor(a.Origin)
	if !ok || bf.LastSeq != 3 {
		t.Errorf("B.Cache frontier for A = %+v ok=%v; want LastSeq=3", bf, ok)
	}
	tab, _ := b.Catalog.Table("event")
	idCol := tab.PK[0].ID
	pk, _ := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{
		idCol: {Column: idCol, TypeTag: crdt.ColBlob, Bytes: []byte{0x01}},
	})
	rs := b.Cache.RowState(tab.ID, pk)
	// Post-INSERT CL=1 (live), post-DELETE CL=2 (tomb).
	if rs.CL != 2 {
		t.Errorf("B.Cache row_clock.CL = %d; want 2", rs.CL)
	}
}

func TestPrimaryKeyUpdateConverges(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(t, hub, 1, eventSchema, 0)
	b := NewWithCache(t, hub, 2, eventSchema, 0)
	a.Start(t, ctx)
	b.Start(t, ctx)

	if err := a.AppWrite.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'hello')`); err != nil {
		t.Fatalf("A INSERT: %v", err)
	}
	b.WaitApplied(t, a.Origin, 1, time.Second)

	if err := a.AppWrite.Exec(`UPDATE event SET id = x'02', n = 'moved' WHERE id = x'01'`); err != nil {
		t.Fatalf("A PK UPDATE: %v", err)
	}
	b.WaitApplied(t, a.Origin, 2, time.Second)

	if rowExists(t, b.Read, []byte{0x01}) {
		t.Fatalf("old PK still present on B after PK update")
	}
	if got := readN(t, b.Read, []byte{0x02}); got != "moved" {
		t.Fatalf("new PK value on B = %q; want moved", got)
	}
}

// TestWaitAppliedTimesOutWhenStarved is a negative test: if the producer
// never publishes, WaitApplied must respect its deadline rather than
// hang. Uses a Node detached from any hub so no transport delivers.
func TestWaitAppliedTimesOutWhenStarved(t *testing.T) {
	n := NewWithCache(t, nil, 1, eventSchema, 0)

	tFake := &fakeT{}
	n.WaitApplied(tFake, crdt.Origin(99), 1, 50*time.Millisecond)
	if !tFake.failed {
		t.Errorf("WaitApplied did not fail on timeout")
	}
}

// fakeT records that Fatalf was called without aborting the surrounding
// test. Sufficient because WaitApplied calls only Helper and Fatalf.
type fakeT struct {
	testing.TB
	failed bool
}

func (f *fakeT) Helper()                                   {}
func (f *fakeT) Fatalf(format string, args ...interface{}) { f.failed = true }

func readN(t testing.TB, c *sqlitebridge.Conn, id []byte) string {
	t.Helper()
	stmt, _, err := c.Prepare(`SELECT n FROM event WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !hasRow {
		return ""
	}
	return stmt.ColumnText(0)
}

func rowExists(t testing.TB, c *sqlitebridge.Conn, id []byte) bool {
	t.Helper()
	stmt, _, err := c.Prepare(`SELECT 1 FROM event WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	return hasRow
}
