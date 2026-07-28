package broker

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/nodestate"
)

// Mirror-journal tail replay for cell-group tables: RecoverMirror must
// reconstruct the same clock state the live apply path built — a
// same-CL partial update is a per-column cell_clock override, NOT a
// row Base advance. An inflated Base makes every column the record
// never carried over-strict after restart, silently dropping a
// late-delivered legitimate winner (BUGS.md "RecoverMirror inflates
// the row Base from cell-group partial updates").

// recMirrorSrc serves one origin's journal to nodestate.RecoverMirror.
type recMirrorSrc struct {
	orig crdt.Origin
	j    *journal.Journal
}

func (s *recMirrorSrc) Origins() []crdt.Origin                        { return []crdt.Origin{s.orig} }
func (s *recMirrorSrc) Journal(crdt.Origin) (*journal.Journal, error) { return s.j, nil }

func recStamp(wall int64, o crdt.Origin) crdt.Stamp {
	return crdt.Stamp{Clock: crdt.Clock{WallTime: wall}, Origin: o}
}

// recCellPK encodes the fixture table's single-column blob PK.
func recCellPK(t *testing.T, f *applierFixture, id []byte) crdt.PKBlob {
	t.Helper()
	idCol := f.tab.PK[0].ID
	pk, err := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, id)})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	return pk
}

// recCellInsert builds an Insert changeset carrying both non-PK
// columns (n, m) of the cellApplier fixture table.
func recCellInsert(t *testing.T, f *applierFixture, dot crdt.Dot, stamp crdt.Stamp, cl uint64, id []byte, n, m string) *crdt.Changeset {
	t.Helper()
	nCol, _ := f.tab.Column("n")
	mCol, _ := f.tab.Column("m")
	ins := crdt.Insert{Table: f.tab.ID, PK: recCellPK(t, f, id), CL: cl, Image: []crdt.ColValue{
		textCol(nCol.ID, n), textCol(mCol.ID, m),
	}}
	cs, err := crdt.Build(dot, stamp, nil, testCluster, []crdt.Record{ins})
	if err != nil {
		t.Fatalf("Build insert: %v", err)
	}
	return cs
}

// recCellUpdate builds an Update changeset at an explicit CL for a
// subset of columns.
func recCellUpdate(t *testing.T, f *applierFixture, dot crdt.Dot, stamp crdt.Stamp, cl uint64, id []byte, cols map[string]string) *crdt.Changeset {
	t.Helper()
	var changed []crdt.ColValue
	for name, val := range cols {
		c, ok := f.tab.Column(name)
		if !ok {
			t.Fatalf("column %q missing", name)
		}
		changed = append(changed, textCol(c.ID, val))
	}
	upd := crdt.Update{Table: f.tab.ID, PK: recCellPK(t, f, id), CL: cl, Changed: changed}
	cs, err := crdt.Build(dot, stamp, nil, testCluster, []crdt.Record{upd})
	if err != nil {
		t.Fatalf("Build update: %v", err)
	}
	return cs
}

// recRecover simulates a restart whose metadata snapshot is seeded by
// seed and whose mirror-journal tail holds every changeset in tail
// (already-applied seqs are skipped via the seeded frontier, exactly
// like a real marker-aligned replay). Returns the recovered cache.
func recRecover(t *testing.T, f *applierFixture, seed func(*nodestate.Cache), tail ...*crdt.Changeset) *nodestate.Cache {
	t.Helper()
	j, err := journal.Open(t.TempDir(), 64*1024, journal.SyncOff)
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })
	var orig crdt.Origin
	for _, cs := range tail {
		orig = cs.Dot.Origin
		if _, _, err := j.Append(journal.KindMirror, 0, uint64(cs.Dot.Origin), cs.Encoded()); err != nil {
			t.Fatalf("Append mirror: %v", err)
		}
	}
	rec := nodestate.New(1)
	seed(rec)
	if _, err := nodestate.RecoverMirror(rec, &recMirrorSrc{orig: orig, j: j}, f.cat); err != nil {
		t.Fatalf("RecoverMirror: %v", err)
	}
	return rec
}

// restartedBroker builds a second Broker over the fixture's app.db /
// metadata / catalog with the recovered cache — the post-restart node.
func restartedBroker(t *testing.T, f *applierFixture, cache *nodestate.Cache) *Broker {
	t.Helper()
	br, err := New(Config{AppApply: f.app, Meta: f.sc, Catalog: f.cat, Cache: cache})
	if err != nil {
		t.Fatalf("broker.New (restart): %v", err)
	}
	return br
}

// TestRecoverMirrorCellGroupSameCLPartialUpdate: the tail holds a
// same-CL partial update (only column n). Replay must record it as a
// cell stamp on n and leave Base at the snapshot's value; a
// late-delivered legitimate winner for column m (stamp between Base
// and the partial update's) must still apply after restart.
func TestRecoverMirrorCellGroupSameCLPartialUpdate(t *testing.T) {
	t.Parallel()
	f := cellApplier(t)
	src := crdt.Origin(7)
	id := []byte{0x01}
	pk := recCellPK(t, f, id)
	s0 := recStamp(100, src) // insert
	s1 := recStamp(500, src) // partial update of n

	insCS := recCellInsert(t, f, crdt.Dot{Origin: src, Seq: 1}, s0, 1, id, "n0", "m0")
	updCS := recCellUpdate(t, f, crdt.Dot{Origin: src, Seq: 2}, s1, 1, id, map[string]string{"n": "n1"})

	// Pre-restart live history: both land through the production apply
	// path (app.db now holds n1/m0; live cache: Base=s0, Cells{n:s1}).
	for _, cs := range []*crdt.Changeset{insCS, updCS} {
		if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("live apply: %v", err)
		}
	}
	liveRS := f.cache.RowState(f.tab.ID, pk)

	// Restart: the snapshot covered only seq 1; seq 2 is journal tail.
	rec := recRecover(t, f, func(c *nodestate.Cache) {
		c.PutRowState(f.tab.ID, pk, crdt.RowState{CL: 1, Base: s0})
		c.MarkApplied(src, 1, s0.Clock)
	}, insCS, updCS)

	// Clock parity with the live path.
	rs := rec.RowState(f.tab.ID, pk)
	if rs.Base != liveRS.Base {
		t.Errorf("recovered Base = %+v, want live path's %+v (partial update must not inflate Base)", rs.Base, liveRS.Base)
	}
	nCol, _ := f.tab.Column("n")
	if got, ok := rec.CellStamp(f.tab.ID, pk, nCol.ID); !ok || got != s1 {
		t.Errorf("recovered cell stamp for n = %+v ok=%v, want %+v (carried column's stamp must replay)", got, ok, s1)
	}

	// Consequence: a legitimately-winning write for the OTHER column
	// (m at wall 300: above Base s0, concurrent-below the n-only s1)
	// must apply on the restarted node, as it would have live.
	br2 := restartedBroker(t, f, rec)
	late := recCellUpdate(t, f, crdt.Dot{Origin: 9, Seq: 1}, recStamp(300, 9), 1, id, map[string]string{"m": "m2"})
	if err := br2.applyPayload(context.Background(), late.Encoded()); err != nil {
		t.Fatalf("late apply: %v", err)
	}
	if n, m := readCols(t, f, id); n != "n1" || m != "m2" {
		t.Errorf("post-restart row = (%q, %q), want (\"n1\", \"m2\"): inflated Base dropped the legitimate m winner", n, m)
	}
}

// TestRecoverMirrorCellGroupGenerationAdvance: the tail holds a
// partial update at a HIGHER CL (writer saw a resurrection we
// haven't). Live, that lands CL with a zero Base plus a cell stamp on
// the carried column, so the (lower-stamped) resurrecting Insert still
// wins the columns the update didn't carry when it arrives.
func TestRecoverMirrorCellGroupGenerationAdvance(t *testing.T) {
	t.Parallel()
	f := cellApplier(t)
	src := crdt.Origin(7)
	id := []byte{0x02}
	pk := recCellPK(t, f, id)
	s0 := recStamp(100, src)
	s1 := recStamp(500, src) // CL=3 partial update of n

	insCS := recCellInsert(t, f, crdt.Dot{Origin: src, Seq: 1}, s0, 1, id, "n0", "m0")
	updCS := recCellUpdate(t, f, crdt.Dot{Origin: src, Seq: 2}, s1, 3, id, map[string]string{"n": "n1"})
	for _, cs := range []*crdt.Changeset{insCS, updCS} {
		if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("live apply: %v", err)
		}
	}
	liveRS := f.cache.RowState(f.tab.ID, pk)

	rec := recRecover(t, f, func(c *nodestate.Cache) {
		c.PutRowState(f.tab.ID, pk, crdt.RowState{CL: 1, Base: s0})
		c.MarkApplied(src, 1, s0.Clock)
	}, insCS, updCS)

	rs := rec.RowState(f.tab.ID, pk)
	if rs.CL != 3 || rs.Base != liveRS.Base {
		t.Errorf("recovered state = {CL:%d Base:%+v}, want live path's {CL:%d Base:%+v}", rs.CL, rs.Base, liveRS.CL, liveRS.Base)
	}

	// The resurrecting Insert (CL=3, stamp 200 < 500) arrives late: it
	// must win column m (Base is zero for the new generation) while
	// losing n to the cell override.
	br2 := restartedBroker(t, f, rec)
	late := recCellInsert(t, f, crdt.Dot{Origin: 9, Seq: 1}, recStamp(200, 9), 3, id, "nI", "mI")
	if err := br2.applyPayload(context.Background(), late.Encoded()); err != nil {
		t.Fatalf("late apply: %v", err)
	}
	if n, m := readCols(t, f, id); n != "n1" || m != "mI" {
		t.Errorf("post-restart row = (%q, %q), want (\"n1\", \"mI\"): inflated Base dropped the resurrecting Insert's m", n, m)
	}
}

// TestRecoverMirrorCellGroupFullCoverCollapse: a tail update covering
// EVERY non-PK column collapses into the baseline exactly like the
// live path's opportunistic collapse — Base advances to the stamp and
// no cell overrides remain.
func TestRecoverMirrorCellGroupFullCoverCollapse(t *testing.T) {
	t.Parallel()
	f := cellApplier(t)
	src := crdt.Origin(7)
	id := []byte{0x03}
	pk := recCellPK(t, f, id)
	s0 := recStamp(100, src)
	s1 := recStamp(500, src)

	insCS := recCellInsert(t, f, crdt.Dot{Origin: src, Seq: 1}, s0, 1, id, "n0", "m0")
	updCS := recCellUpdate(t, f, crdt.Dot{Origin: src, Seq: 2}, s1, 1, id, map[string]string{"n": "n1", "m": "m1"})

	rec := recRecover(t, f, func(c *nodestate.Cache) {
		c.PutRowState(f.tab.ID, pk, crdt.RowState{CL: 1, Base: s0})
		c.MarkApplied(src, 1, s0.Clock)
	}, insCS, updCS)

	rs := rec.RowState(f.tab.ID, pk)
	if rs.Base != s1 {
		t.Errorf("recovered Base = %+v, want collapsed %+v", rs.Base, s1)
	}
	for _, name := range []string{"n", "m"} {
		col, _ := f.tab.Column(name)
		if got, ok := rec.CellStamp(f.tab.ID, pk, col.ID); ok {
			t.Errorf("recovered cell stamp for %s = %+v, want none (full-cover update collapses)", name, got)
		}
	}
}

// TestRecoverMirrorCounterMixedUpdate: the tail holds a MIXED update —
// a counter contribution plus a register write. Replay must route it
// through the cell-aware path: the register column's cell stamp is
// reconstructed (a later lower-stamp register write must lose exactly
// as it would live), Base stays uninflated, and the counter cell
// carries no stamp while its contribution stays intact in app.db.
func TestRecoverMirrorCounterMixedUpdate(t *testing.T) {
	t.Parallel()
	// A third register column keeps the mixed update PARTIAL — covering
	// every non-PK column would (correctly) collapse Base instead.
	f := counterApplierSchema(t,
		`CREATE TABLE inv (id BLOB PRIMARY KEY NOT NULL, qty INTEGER NOT NULL DEFAULT 0, note TEXT, extra TEXT)`)
	src := crdt.Origin(7)
	id := []byte{0x0B}
	pk := counterPK(t, f, id)
	s0 := recStamp(100, src)
	s1 := recStamp(900, src)
	qtyCol, _ := f.tab.Column("qty")
	noteCol, _ := f.tab.Column("note")

	insCS := counterCS(t, crdt.Dot{Origin: src, Seq: 1}, s0, counterInsert(t, f, 1, id, 100, "seed"))
	mixed := crdt.Update{Table: f.tab.ID, PK: pk, CL: 1, Changed: []crdt.ColValue{
		deltaCol(qtyCol.ID, 7), textCol(noteCol.ID, "high"),
	}}
	mixedCS := counterCS(t, crdt.Dot{Origin: src, Seq: 2}, s1, mixed)
	for _, cs := range []*crdt.Changeset{insCS, mixedCS} {
		if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("live apply: %v", err)
		}
	}

	// Restart: snapshot covers seq 1; the mixed update is journal tail.
	rec := recRecover(t, f, func(c *nodestate.Cache) {
		c.PutRowState(f.tab.ID, pk, crdt.RowState{CL: 1, Base: s0})
		c.MarkApplied(src, 1, s0.Clock)
	}, insCS, mixedCS)

	rs := rec.RowState(f.tab.ID, pk)
	if rs.Base != s0 {
		t.Errorf("recovered Base = %+v, want %+v (mixed update must not inflate Base)", rs.Base, s0)
	}
	if got, ok := rec.CellStamp(f.tab.ID, pk, noteCol.ID); !ok || got != s1 {
		t.Errorf("recovered note cell stamp = %+v ok=%v, want %+v (mixed update's register column must replay)", got, ok, s1)
	}
	if _, ok := rec.CellStamp(f.tab.ID, pk, qtyCol.ID); ok {
		t.Errorf("counter column recovered a cell stamp; counter cells carry none")
	}

	// Consequences on the restarted node: a lower-stamp register write
	// loses (as live); a fresh contribution still sums.
	br2 := restartedBroker(t, f, rec)
	low := crdt.Update{Table: f.tab.ID, PK: pk, CL: 1, Changed: []crdt.ColValue{textCol(noteCol.ID, "low")}}
	inc := counterDelta(t, f, 1, id, 5)
	for i, r := range []crdt.Record{low, inc} {
		cs := counterCS(t, crdt.Dot{Origin: 9, Seq: crdt.Seq(i + 1)}, recStamp(int64(200+i), 9), r)
		if err := br2.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("post-restart apply %d: %v", i, err)
		}
	}
	if got, _ := readQty(t, f, id); got != 112 {
		t.Errorf("qty=%d; want 112 (100+7+5)", got)
	}
	stmt, _, err := f.app.Prepare(`SELECT note FROM inv WHERE id = x'0B'`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if ok, err := stmt.Step(); err != nil || !ok {
		t.Fatalf("Step: ok=%v err=%v", ok, err)
	}
	if note := stmt.ColumnText(0); note != "high" {
		t.Errorf("note=%q; want high — the replayed register stamp must reject the lower-stamp write", note)
	}
}
