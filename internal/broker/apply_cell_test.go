package broker

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// cellApplier builds an applierFixture whose `event` table uses the
// cell clock group, with a second non-PK column so partial updates are
// expressible: event(id PK, n TEXT, m TEXT).
func cellApplier(t *testing.T) *applierFixture {
	t.Helper()
	return cellApplierSchema(t,
		`CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT, m TEXT)`)
}

// cellApplierSchema is cellApplier over a caller-supplied `event`
// schema (must declare table event).
func cellApplierSchema(t *testing.T, schema string) *applierFixture {
	t.Helper()
	f := newApplierSchema(t, 1, nil, schema)
	// Flip the table to the cell group (what OpSetClockGroup does on a
	// live node).
	if err := f.sc.WithTx(func(tx *metadata.Tx) error {
		return tx.SetDefaultClockGroup(f.tab.ID, metadata.ClockGroupCell)
	}); err != nil {
		t.Fatalf("set clock group: %v", err)
	}
	if err := f.cat.Reload(); err != nil {
		t.Fatalf("catalog reload: %v", err)
	}
	tab, ok := f.cat.Table("event")
	if !ok || !tab.CellGroup() {
		t.Fatalf("event table not cell-group after flip")
	}
	f.tab = tab
	return f
}

func cellUpdateCS(t *testing.T, f *applierFixture, dot crdt.Dot, stamp crdt.Stamp, idVal []byte, cols map[string]string) *crdt.Changeset {
	t.Helper()
	idCol := f.tab.PK[0].ID
	pk, err := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, idVal)})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	var changed []crdt.ColValue
	for name, val := range cols {
		c, ok := f.tab.Column(name)
		if !ok {
			t.Fatalf("column %q missing", name)
		}
		changed = append(changed, textCol(c.ID, val))
	}
	upd := crdt.Update{Table: f.tab.ID, PK: pk, CL: 1, Changed: changed}
	cs, err := crdt.Build(dot, stamp, nil, testCluster, []crdt.Record{upd})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return cs
}

// readColsOK is readCols that reports row absence instead of failing.
func readColsOK(t *testing.T, f *applierFixture, id []byte) (n, m string, ok bool) {
	t.Helper()
	stmt, _, err := f.app.Prepare(`SELECT n, m FROM event WHERE id = ?`)
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
		return "", "", false
	}
	return stmt.ColumnText(0), stmt.ColumnText(1), true
}

func readCols(t *testing.T, f *applierFixture, id []byte) (string, string) {
	t.Helper()
	stmt, _, err := f.app.Prepare(`SELECT n, m FROM event WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	ok, err := stmt.Step()
	if err != nil || !ok {
		t.Fatalf("Step: ok=%v err=%v", ok, err)
	}
	return stmt.ColumnText(0), stmt.ColumnText(1)
}

func seedCellRow(t *testing.T, f *applierFixture, src crdt.Origin) crdt.PKBlob {
	t.Helper()
	s0 := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: src}
	nCol, _ := f.tab.Column("n")
	mCol, _ := f.tab.Column("m")
	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x01})})
	ins := crdt.Insert{Table: f.tab.ID, PK: pk, CL: 1, Image: []crdt.ColValue{
		textCol(nCol.ID, "n0"), textCol(mCol.ID, "m0"),
	}}
	cs, err := crdt.Build(crdt.Dot{Origin: src, Seq: 1}, s0, nil, testCluster, []crdt.Record{ins})
	if err != nil {
		t.Fatalf("Build insert: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}
	return pk
}

// TestCellGroupDisjointColumnsMerge: two concurrent writers update
// different columns of one row; both edits survive regardless of
// delivery order, and the merged rows are identical.
func TestCellGroupDisjointColumnsMerge(t *testing.T) {
	t.Parallel()
	for _, swapped := range []bool{false, true} {
		f := cellApplier(t)
		seedCellRow(t, f, 7)

		// Writer A (origin 8) updates n at wall 200; writer B (origin
		// 9) updates m at wall 300. Concurrent (neither saw the other).
		a := cellUpdateCS(t, f, crdt.Dot{Origin: 8, Seq: 1},
			crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 8},
			[]byte{0x01}, map[string]string{"n": "nA"})
		bcs := cellUpdateCS(t, f, crdt.Dot{Origin: 9, Seq: 1},
			crdt.Stamp{Clock: crdt.Clock{WallTime: 300}, Origin: 9},
			[]byte{0x01}, map[string]string{"m": "mB"})
		first, second := a, bcs
		if swapped {
			first, second = bcs, a
		}
		if err := f.br.applyPayload(context.Background(), first.Encoded()); err != nil {
			t.Fatalf("apply first: %v", err)
		}
		if err := f.br.applyPayload(context.Background(), second.Encoded()); err != nil {
			t.Fatalf("apply second: %v", err)
		}
		n, m := readCols(t, f, []byte{0x01})
		if n != "nA" || m != "mB" {
			t.Fatalf("swapped=%v: row = (%q, %q); want (nA, mB) — disjoint edits must merge", swapped, n, m)
		}
	}
}

// TestCellGroupSameColumnLWW: concurrent writes to the same column
// resolve by stamp in either delivery order.
func TestCellGroupSameColumnLWW(t *testing.T) {
	t.Parallel()
	for _, swapped := range []bool{false, true} {
		f := cellApplier(t)
		seedCellRow(t, f, 7)

		lo := cellUpdateCS(t, f, crdt.Dot{Origin: 8, Seq: 1},
			crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 8},
			[]byte{0x01}, map[string]string{"n": "lo"})
		hi := cellUpdateCS(t, f, crdt.Dot{Origin: 9, Seq: 1},
			crdt.Stamp{Clock: crdt.Clock{WallTime: 300}, Origin: 9},
			[]byte{0x01}, map[string]string{"n": "hi"})
		first, second := lo, hi
		if swapped {
			first, second = hi, lo
		}
		if err := f.br.applyPayload(context.Background(), first.Encoded()); err != nil {
			t.Fatalf("apply first: %v", err)
		}
		if err := f.br.applyPayload(context.Background(), second.Encoded()); err != nil {
			t.Fatalf("apply second: %v", err)
		}
		if n, _ := readCols(t, f, []byte{0x01}); n != "hi" {
			t.Fatalf("swapped=%v: n = %q; want hi", swapped, n)
		}
	}
}

// TestCellGroupCollapseOnFullCoverage: a full-coverage winning update
// re-absorbs the row into its baseline — no outstanding cell
// overrides remain, and the row clock base advances to the stamp.
func TestCellGroupCollapseOnFullCoverage(t *testing.T) {
	t.Parallel()
	f := cellApplier(t)
	pk := seedCellRow(t, f, 7)

	// Partial update leaves an outstanding override on n.
	partial := cellUpdateCS(t, f, crdt.Dot{Origin: 8, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 8},
		[]byte{0x01}, map[string]string{"n": "nA"})
	if err := f.br.applyPayload(context.Background(), partial.Encoded()); err != nil {
		t.Fatalf("apply partial: %v", err)
	}
	nCol, _ := f.tab.Column("n")
	if _, ok := f.cache.CellStamp(f.tab.ID, pk, nCol.ID); !ok {
		t.Fatalf("partial update should leave a cell override on n")
	}

	// Full-coverage update at a higher stamp collapses the row.
	fullStamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 400}, Origin: 9}
	full := cellUpdateCS(t, f, crdt.Dot{Origin: 9, Seq: 1}, fullStamp,
		[]byte{0x01}, map[string]string{"n": "nF", "m": "mF"})
	if err := f.br.applyPayload(context.Background(), full.Encoded()); err != nil {
		t.Fatalf("apply full: %v", err)
	}
	n, m := readCols(t, f, []byte{0x01})
	if n != "nF" || m != "mF" {
		t.Fatalf("row = (%q, %q); want (nF, mF)", n, m)
	}
	rs := f.cache.RowState(f.tab.ID, pk)
	if rs.Base != fullStamp {
		t.Fatalf("base = %+v; want %+v (collapse must absorb)", rs.Base, fullStamp)
	}
	if len(rs.Cells) != 0 {
		t.Fatalf("cells = %v; want none after collapse", rs.Cells)
	}
}

// TestCellGroupPartialLosesOnlyLosingColumns: an update straddling a
// newer cell override applies its winning column and skips the losing
// one.
func TestCellGroupPartialLosesOnlyLosingColumns(t *testing.T) {
	t.Parallel()
	f := cellApplier(t)
	seedCellRow(t, f, 7)

	newer := cellUpdateCS(t, f, crdt.Dot{Origin: 8, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: 8},
		[]byte{0x01}, map[string]string{"n": "nNew"})
	if err := f.br.applyPayload(context.Background(), newer.Encoded()); err != nil {
		t.Fatalf("apply newer: %v", err)
	}
	// Straddler: older than n's override, newer than m's baseline.
	straddle := cellUpdateCS(t, f, crdt.Dot{Origin: 9, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 300}, Origin: 9},
		[]byte{0x01}, map[string]string{"n": "nMid", "m": "mMid"})
	if err := f.br.applyPayload(context.Background(), straddle.Encoded()); err != nil {
		t.Fatalf("apply straddle: %v", err)
	}
	n, m := readCols(t, f, []byte{0x01})
	if n != "nNew" || m != "mMid" {
		t.Fatalf("row = (%q, %q); want (nNew, mMid)", n, m)
	}
}

// TestCellGroupUpdateOutrunsInsert: cross-origin delivery is not
// causally gated, so an Update at CL=1 can land before the Insert that
// created the row anywhere existed locally. The update must
// materialize the row (PK + carried columns, defaults elsewhere) so
// the later Insert's per-column arbitration can fill the columns the
// update didn't carry. Without materialization both DMLs are 0-row
// UPDATEs and the row is silently lost while RowState claims live.
func TestCellGroupUpdateOutrunsInsert(t *testing.T) {
	t.Parallel()
	f := cellApplier(t)
	id := []byte{0x02}

	upd := cellUpdateCS(t, f, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8),
		id, map[string]string{"n": "nU"})
	// Deliver the update twice: the second is a frontier-idempotent
	// redelivery and must not disturb anything.
	if err := f.br.applyPayload(context.Background(), upd.Encoded()); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), upd.Encoded()); err != nil {
		t.Fatalf("redeliver update: %v", err)
	}
	if n, _, ok := readColsOK(t, f, id); !ok {
		t.Fatalf("row missing after update-before-insert: RowState claims live but no physical row was materialized")
	} else if n != "nU" {
		t.Fatalf("n = %q; want nU", n)
	}

	// The generation's Insert arrives (causally earlier, lower stamp):
	// n loses to the update's newer cell stamp, m fills from the image.
	nCol, _ := f.tab.Column("n")
	mCol, _ := f.tab.Column("m")
	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, id)})
	ins := crdt.Insert{Table: f.tab.ID, PK: pk, CL: 1, Image: []crdt.ColValue{
		textCol(nCol.ID, "n0"), textCol(mCol.ID, "m0"),
	}}
	cs, err := crdt.Build(crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), nil, testCluster, []crdt.Record{ins})
	if err != nil {
		t.Fatalf("Build insert: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}
	n, m, ok := readColsOK(t, f, id)
	if !ok {
		t.Fatalf("row missing after insert: silently lost")
	}
	if n != "nU" || m != "m0" {
		t.Fatalf("row = (%q, %q); want (nU, m0) — merged update + insert", n, m)
	}
}

// TestCellGroupFullCoverageUpdateOutrunsInsert: the collapse variant —
// a full-coverage update outrunning the insert absorbs the row into
// its baseline; the lower-stamped Insert then loses every column and
// the row must equal the update's image.
func TestCellGroupFullCoverageUpdateOutrunsInsert(t *testing.T) {
	t.Parallel()
	f := cellApplier(t)
	id := []byte{0x03}

	upd := cellUpdateCS(t, f, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8),
		id, map[string]string{"n": "nU", "m": "mU"})
	if err := f.br.applyPayload(context.Background(), upd.Encoded()); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	nCol, _ := f.tab.Column("n")
	mCol, _ := f.tab.Column("m")
	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, id)})
	ins := crdt.Insert{Table: f.tab.ID, PK: pk, CL: 1, Image: []crdt.ColValue{
		textCol(nCol.ID, "n0"), textCol(mCol.ID, "m0"),
	}}
	cs, err := crdt.Build(crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), nil, testCluster, []crdt.Record{ins})
	if err != nil {
		t.Fatalf("Build insert: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}
	n, m, ok := readColsOK(t, f, id)
	if !ok {
		t.Fatalf("row missing: silently lost")
	}
	if n != "nU" || m != "mU" {
		t.Fatalf("row = (%q, %q); want (nU, mU) — collapse baseline dominates the insert", n, m)
	}
}

// TestCellGroupUpdateOutrunsInsertNotNull: when the update can't
// materialize the row (a NOT NULL column with no default that it
// doesn't carry), the failure must surface as a constraint error and
// route to quarantine — never a silent 0-row UPDATE. Once the Insert
// lands, the quarantine retry merges the update.
func TestCellGroupUpdateOutrunsInsertNotNull(t *testing.T) {
	t.Parallel()
	f := cellApplierSchema(t,
		`CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT, m TEXT, req TEXT NOT NULL)`)
	id := []byte{0x04}

	upd := cellUpdateCS(t, f, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8),
		id, map[string]string{"n": "nU"})
	if err := f.br.applyPayload(context.Background(), upd.Encoded()); err != nil {
		t.Fatalf("poison update should quarantine (nil), got: %v", err)
	}
	q, err := f.sc.ListQuarantine()
	if err != nil {
		t.Fatalf("ListQuarantine: %v", err)
	}
	if len(q) != 1 {
		t.Fatalf("quarantine = %+v; want one entry — the unmaterializable update must not silently succeed", q)
	}
	if _, _, ok := readColsOK(t, f, id); ok {
		t.Fatalf("row exists after quarantined update; want absent")
	}

	nCol, _ := f.tab.Column("n")
	mCol, _ := f.tab.Column("m")
	reqCol, _ := f.tab.Column("req")
	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, id)})
	ins := crdt.Insert{Table: f.tab.ID, PK: pk, CL: 1, Image: []crdt.ColValue{
		textCol(nCol.ID, "n0"), textCol(mCol.ID, "m0"), textCol(reqCol.ID, "r0"),
	}}
	cs, err := crdt.Build(crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), nil, testCluster, []crdt.Record{ins})
	if err != nil {
		t.Fatalf("Build insert: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}
	f.br.RetryQuarantined(context.Background())
	q, err = f.sc.ListQuarantine()
	if err != nil {
		t.Fatalf("ListQuarantine after retry: %v", err)
	}
	if len(q) != 0 {
		t.Fatalf("quarantine after retry = %+v; want empty", q)
	}
	n, m, ok := readColsOK(t, f, id)
	if !ok {
		t.Fatalf("row missing after retry")
	}
	if n != "nU" || m != "m0" {
		t.Fatalf("row = (%q, %q); want (nU, m0) — retried update must merge", n, m)
	}
}

// TestRowGroupUpdateOutrunsInsertQuarantines: the row-group variant of
// the update-before-insert hole. Materializing from the update would
// diverge here (no per-column merge lets the late Insert fill in), so a
// 0-row row-group UPDATE fails deterministically into quarantine; the
// retry converges once the Insert lands, because row-group updates
// carry the full post-image.
func TestRowGroupUpdateOutrunsInsertQuarantines(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, nil) // event(id, n) — row-group by default
	nCol, _ := f.tab.Column("n")
	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x01})})

	insCS := buildInsert(t, f.tab, crdt.Dot{Origin: 7, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: 7}, 1, []byte{0x01}, "base")
	upd := crdt.Update{Table: f.tab.ID, PK: pk, CL: 1,
		Changed: []crdt.ColValue{textCol(nCol.ID, "updated")}} // full non-PK post-image
	updCS, err := crdt.Build(crdt.Dot{Origin: 8, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 8}, nil, testCluster, []crdt.Record{upd})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Update outruns the insert: quarantined (nil error), frontier
	// advances, and crucially NO row clock lands — the rollback keeps
	// the later insert eligible.
	if err := f.br.applyPayload(context.Background(), updCS.Encoded()); err != nil {
		t.Fatalf("outrun update must quarantine, not error: %v", err)
	}
	if !f.cache.IsAppliedRemote(8, 1) {
		t.Fatalf("quarantine must advance the frontier")
	}
	if rs := f.cache.RowState(f.tab.ID, pk); rs.CL != 0 {
		t.Fatalf("row clock advanced to CL=%d despite rollback; the insert would be dropped forever", rs.CL)
	}

	// Insert lands, then the quarantined update re-applies and wins.
	if err := f.br.applyPayload(context.Background(), insCS.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}
	f.br.RetryQuarantined(context.Background())
	if got := readNCol(t, f.app, []byte{0x01}); got != "updated" {
		t.Fatalf("n = %q; want updated — quarantined update must converge after the insert lands", got)
	}

	// Parity: the in-order fixture ends in the same state.
	f2 := newApplier(t, 1, nil)
	for _, cs := range []*crdt.Changeset{insCS, updCS} {
		if err := f2.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("in-order apply: %v", err)
		}
	}
	if got := readNCol(t, f2.app, []byte{0x01}); got != "updated" {
		t.Fatalf("in-order n = %q; want updated", got)
	}
}
