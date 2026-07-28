package broker

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
)

// counterApplier builds an applierFixture whose `inv` table has a
// declared counter column: inv(id PK, qty INTEGER COUNTER, note TEXT).
// The schema is seeded without the COUNTER token (the adopt path
// rejects it) and the column's clock_group is flipped in metadata —
// the same rows catApplyCreateTable would have written.
func counterApplier(t *testing.T) *applierFixture {
	return counterApplierSchema(t,
		`CREATE TABLE inv (id BLOB PRIMARY KEY NOT NULL, qty INTEGER NOT NULL DEFAULT 0, note TEXT)`)
}

// counterApplierSchema is counterApplier for an alternate inv shape
// (the table must be named inv with a counter column named qty).
func counterApplierSchema(t *testing.T, schema string) *applierFixture {
	t.Helper()
	f := newApplierSchema(t, 1, nil, schema)
	f.tab, _ = f.cat.Table("inv") // newApplierSchema resolves only "event"
	if f.tab == nil {
		t.Fatalf("inv table missing from catalog")
	}
	qty, ok := f.tab.Column("qty")
	if !ok {
		t.Fatalf("qty column missing")
	}
	if err := f.sc.WithTx(func(tx *metadata.Tx) error {
		if err := tx.SetDefaultClockGroup(f.tab.ID, metadata.ClockGroupCell); err != nil {
			return err
		}
		return tx.UpsertColumn(metadata.ColumnEntry{
			TableID: f.tab.ID, ColumnID: qty.ID, Name: qty.Name,
			Ordinal: qty.Ordinal, State: metadata.StateActive,
			ClockGroup: metadata.ClockGroupCounter,
			Collation:  qty.Collation, CreateSeq: qty.CreateSeq,
		})
	}); err != nil {
		t.Fatalf("declare counter column: %v", err)
	}
	if err := f.cat.Reload(); err != nil {
		t.Fatalf("catalog reload: %v", err)
	}
	tab, ok := f.cat.Table("inv")
	if !ok || !tab.CellGroup() || !tab.HasCounters() {
		t.Fatalf("inv not a cell-group counter table after flip")
	}
	f.tab = tab
	return f
}

func deltaCol(id crdt.ColumnID, d int64) crdt.ColValue {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(d))
	return crdt.ColValue{Column: id, TypeTag: crdt.ColInt, Format: crdt.FormatDelta, Bytes: b[:]}
}

func counterPK(t *testing.T, f *applierFixture, idVal []byte) crdt.PKBlob {
	t.Helper()
	idCol := f.tab.PK[0].ID
	pk, err := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, idVal)})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	return pk
}

// counterCS builds a one-record Changeset from records.
func counterCS(t *testing.T, dot crdt.Dot, stamp crdt.Stamp, recs ...crdt.Record) *crdt.Changeset {
	t.Helper()
	cs, err := crdt.Build(dot, stamp, nil, testCluster, recs)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return cs
}

func counterInsert(t *testing.T, f *applierFixture, cl uint64, idVal []byte, qty int64, note string) crdt.Insert {
	t.Helper()
	qtyCol, _ := f.tab.Column("qty")
	noteCol, _ := f.tab.Column("note")
	return crdt.Insert{Table: f.tab.ID, PK: counterPK(t, f, idVal), CL: cl, Image: []crdt.ColValue{
		intCol(qtyCol.ID, qty), textCol(noteCol.ID, note),
	}}
}

func counterDelta(t *testing.T, f *applierFixture, cl uint64, idVal []byte, d int64) crdt.Update {
	t.Helper()
	qtyCol, _ := f.tab.Column("qty")
	return crdt.Update{Table: f.tab.ID, PK: counterPK(t, f, idVal), CL: cl,
		Changed: []crdt.ColValue{deltaCol(qtyCol.ID, d)}}
}

func readQty(t *testing.T, f *applierFixture, id []byte) (int64, bool) {
	t.Helper()
	stmt, _, err := f.app.Prepare(`SELECT qty FROM inv WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	ok, err := stmt.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !ok {
		return 0, false
	}
	return stmt.ColumnInt64(0), true
}

func applyAll(t *testing.T, f *applierFixture, css ...*crdt.Changeset) {
	t.Helper()
	for _, cs := range css {
		if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("applyPayload: %v", err)
		}
	}
}

// permutations returns every ordering of n indices (n <= 4 in these tests).
func permutations(n int) [][]int {
	if n == 1 {
		return [][]int{{0}}
	}
	var out [][]int
	for _, sub := range permutations(n - 1) {
		for i := 0; i <= len(sub); i++ {
			p := make([]int, 0, n)
			p = append(p, sub[:i]...)
			p = append(p, n-1)
			p = append(p, sub[i:]...)
			out = append(out, p)
		}
	}
	return out
}

// applyPermutation asserts that every causally-valid delivery order of
// css converges to the same qty, and returns it. deps lists (before,
// after) index pairs: an order is skipped unless every `before` lands
// before its `after`. Causal+ delivery (CRDT.md) guarantees a record
// never outruns its transitive Deps, so orders violating deps are
// outside the apply contract (cross-origin dep gating at the transport
// is the applied_gaps-extension follow-up noted in applyPayloadCache).
func applyPermutation(t *testing.T, mk func(t *testing.T) *applierFixture, id []byte, deps [][2]int, css func(f *applierFixture) []*crdt.Changeset) (int64, bool) {
	t.Helper()
	var want int64
	var wantLive bool
	first := true
	tested := 0
	for _, perm := range permutations(len(css(mk(t)))) {
		pos := make([]int, len(perm))
		for at, i := range perm {
			pos[i] = at
		}
		causal := true
		for _, d := range deps {
			if pos[d[0]] > pos[d[1]] {
				causal = false
				break
			}
		}
		if !causal {
			continue
		}
		tested++
		f := mk(t)
		all := css(f)
		for _, i := range perm {
			applyAll(t, f, all[i])
		}
		got, live := readQty(t, f, id)
		if first {
			want, wantLive, first = got, live, false
			continue
		}
		if got != want || live != wantLive {
			t.Fatalf("order %v: qty=(%d,%v); other orders gave (%d,%v) — non-convergent", perm, got, live, want, wantLive)
		}
	}
	if tested < 2 {
		t.Fatalf("deps admit only %d order(s); constraints too tight to test convergence", tested)
	}
	return want, wantLive
}

// restartApplier simulates a crash-restart with a lost in-memory
// frontier: a new Broker over the same app.db and metadata, with a
// fresh (empty) nodestate.Cache.
func restartApplier(t *testing.T, f *applierFixture) *applierFixture {
	t.Helper()
	cache := nodestate.New(1)
	br, err := New(Config{
		AppApply: f.app, Meta: f.sc, Catalog: f.cat, Cache: cache,
	})
	if err != nil {
		t.Fatalf("broker.New (restart): %v", err)
	}
	return &applierFixture{app: f.app, sc: f.sc, cat: f.cat, cache: cache, br: br, tab: f.tab}
}

// TestCounterConcurrentIncrementsMerge: the headline behavior — two
// concurrent increments on one row both survive, in every delivery
// order (the classic additive-counter conflict scenario, upgraded from
// primary-resolved to true multi-master convergence).
func TestCounterConcurrentIncrementsMerge(t *testing.T) {
	t.Parallel()
	id := []byte{0x01}
	got, live := applyPermutation(t, counterApplier, id, [][2]int{{0, 1}, {0, 2}}, func(f *applierFixture) []*crdt.Changeset {
		return []*crdt.Changeset{
			counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, 100, "seed")),
			counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), counterDelta(t, f, 1, id, 30)),
			counterCS(t, crdt.Dot{Origin: 9, Seq: 1}, stampAt(300, 9), counterDelta(t, f, 1, id, -50)),
		}
	})
	if !live || got != 80 {
		t.Fatalf("qty=(%d, live=%v); want (80, true): 100+30-50 with no lost increment", got, live)
	}
}

// TestCounterIncrementSurvivesCollapse: a full-coverage register+counter
// update collapses the row base to a high stamp; a concurrent increment
// with a LOWER stamp must still land (counter contributions gate on CL,
// never on stamps).
func TestCounterIncrementSurvivesCollapse(t *testing.T) {
	t.Parallel()
	id := []byte{0x02}
	got, live := applyPermutation(t, counterApplier, id, [][2]int{{0, 1}, {0, 2}}, func(f *applierFixture) []*crdt.Changeset {
		qtyCol, _ := f.tab.Column("qty")
		noteCol, _ := f.tab.Column("note")
		full := crdt.Update{Table: f.tab.ID, PK: counterPK(t, f, id), CL: 1, Changed: []crdt.ColValue{
			deltaCol(qtyCol.ID, 7), textCol(noteCol.ID, "noteF"),
		}}
		return []*crdt.Changeset{
			counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, 100, "seed")),
			counterCS(t, crdt.Dot{Origin: 9, Seq: 1}, stampAt(900, 9), full),
			counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), counterDelta(t, f, 1, id, 30)),
		}
	})
	if !live || got != 137 {
		t.Fatalf("qty=(%d, live=%v); want (137, true): 100+7+30 — the low-stamp increment must survive the collapse", got, live)
	}
}

// TestCounterConcurrentInsertsSum: two same-CL inserts of one PK merge
// counter columns additively (within a generation the cell is the sum
// of all contributions), while register columns LWW as usual.
func TestCounterConcurrentInsertsSum(t *testing.T) {
	t.Parallel()
	id := []byte{0x03}
	got, live := applyPermutation(t, counterApplier, id, [][2]int{{1, 2}}, func(f *applierFixture) []*crdt.Changeset {
		return []*crdt.Changeset{
			counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, 10, "a")),
			counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), counterInsert(t, f, 1, id, 20, "b")),
			counterCS(t, crdt.Dot{Origin: 9, Seq: 1}, stampAt(300, 9), counterDelta(t, f, 1, id, 5)),
		}
	})
	if !live || got != 35 {
		t.Fatalf("qty=(%d, live=%v); want (35, true): 10+20+5 — concurrent creations' counts merge", got, live)
	}
}

// TestCounterDeleteDominates: a tombstone at a higher CL drops
// same-generation increments in every order — row liveness stays
// row-level.
func TestCounterDeleteDominates(t *testing.T) {
	t.Parallel()
	id := []byte{0x04}
	_, live := applyPermutation(t, counterApplier, id, [][2]int{{0, 1}, {0, 2}}, func(f *applierFixture) []*crdt.Changeset {
		del := crdt.Delete{Table: f.tab.ID, PK: counterPK(t, f, id), CL: 2}
		return []*crdt.Changeset{
			counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, 100, "seed")),
			counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), counterDelta(t, f, 1, id, 30)),
			counterCS(t, crdt.Dot{Origin: 9, Seq: 1}, stampAt(300, 9), del),
		}
	})
	if live {
		t.Fatalf("row still live; the CL-2 tombstone must dominate the generation-1 increment")
	}
}

// TestCounterResurrectionResets: a delete + re-insert opens a new
// generation whose counter restarts from the insert image; a
// same-generation increment adds on top, even when it outruns the
// resurrecting insert (out-of-causal-order delivery).
func TestCounterResurrectionResets(t *testing.T) {
	t.Parallel()
	id := []byte{0x05}
	got, live := applyPermutation(t, counterApplier, id, [][2]int{{0, 1}, {0, 2}, {2, 3}}, func(f *applierFixture) []*crdt.Changeset {
		del := crdt.Delete{Table: f.tab.ID, PK: counterPK(t, f, id), CL: 2}
		return []*crdt.Changeset{
			counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, 100, "seed")),
			counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), del),
			counterCS(t, crdt.Dot{Origin: 8, Seq: 2}, stampAt(300, 8), counterInsert(t, f, 3, id, 7, "re")),
			counterCS(t, crdt.Dot{Origin: 9, Seq: 1}, stampAt(400, 9), counterDelta(t, f, 3, id, 5)),
		}
	})
	if !live || got != 12 {
		t.Fatalf("qty=(%d, live=%v); want (12, true): resurrection resets to 7, +5 adds", got, live)
	}
}

// TestCounterDeltaOutrunsInsert: a delta outrunning the row's creating
// Insert (never-existed PK) must materialize the row seeded from the
// delta exactly once; the Insert's image then merges additively — both
// orders converge with no lost and no double-counted contribution.
func TestCounterDeltaOutrunsInsert(t *testing.T) {
	t.Parallel()
	id := []byte{0x08}
	got, live := applyPermutation(t, counterApplier, id, nil, func(f *applierFixture) []*crdt.Changeset {
		return []*crdt.Changeset{
			counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, 100, "seed")),
			counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), counterDelta(t, f, 1, id, 30)),
		}
	})
	if !live || got != 130 {
		t.Fatalf("qty=(%d, live=%v); want (130, true): the delta seeds the row once and the insert image adds", got, live)
	}
}

// TestCounterRedeliveryFrontierSkip: re-applying the same payload is a
// no-op — the frontier short-circuit is what makes non-idempotent
// summation safe against transport redelivery.
func TestCounterRedeliveryFrontierSkip(t *testing.T) {
	t.Parallel()
	f := counterApplier(t)
	id := []byte{0x06}
	seed := counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, 100, "seed"))
	inc := counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), counterDelta(t, f, 1, id, 30))
	applyAll(t, f, seed, inc, inc, inc)
	if got, _ := readQty(t, f, id); got != 130 {
		t.Fatalf("qty=%d; want 130 — redelivered increment must not double-count", got)
	}
}

// TestCounterAppliedMarkerStripsRedelivery: the crash window. The DML
// (and its applied marker) committed, but the frontier advance was
// lost — simulated by a second broker over the same app.db with a
// fresh cache. Redelivery must strip the counter contribution (no
// double count) while still advancing the new frontier.
func TestCounterAppliedMarkerStripsRedelivery(t *testing.T) {
	t.Parallel()
	f := counterApplier(t)
	id := []byte{0x07}
	seed := counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, 100, "seed"))
	inc := counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), counterDelta(t, f, 1, id, 30))
	applyAll(t, f, seed, inc)
	if got, _ := readQty(t, f, id); got != 130 {
		t.Fatalf("pre-crash qty=%d; want 130", got)
	}

	// "Restart": same app.db (DML + markers durable), frontier lost.
	f2 := restartApplier(t, f)
	applyAll(t, f2, seed, inc)
	if got, _ := readQty(t, f2, id); got != 130 {
		t.Fatalf("post-redelivery qty=%d; want 130 — the applied marker must strip the re-delivered contribution", got)
	}
	if !f2.cache.IsAppliedRemote(8, 1) {
		t.Fatalf("redelivery must still advance the frontier")
	}

	// The stripped redelivery must not damage register columns either.
	stmt, _, err := f2.app.Prepare(`SELECT note FROM inv WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if ok, err := stmt.Step(); err != nil || !ok {
		t.Fatalf("Step: ok=%v err=%v", ok, err)
	}
	if note := stmt.ColumnText(0); note != "seed" {
		t.Fatalf("note=%q; want seed", note)
	}
}

// TestCounterInsertMergesUndrainedLocalRow: codex review finding — a
// local INSERT commits to app.db, and before the drain advances the row
// clock, a concurrent remote same-PK insert arrives. The row clock
// still reads CL=0, so the remote insert takes the CL-bump row-level
// path; an absolute image would erase the local counter contribution
// this node alone carries (every peer sums both). The apply must merge
// counter columns additively; registers take the image and are repaired
// by ReassertLocal at drain if the local commit dominates.
func TestCounterInsertMergesUndrainedLocalRow(t *testing.T) {
	t.Parallel()
	f := counterApplier(t)
	id := []byte{0x08}

	// Local commit, clock advance still queued behind the drain.
	if err := f.app.Exec(`INSERT INTO inv (id, qty, note) VALUES (x'08', 10, 'local')`); err != nil {
		t.Fatalf("local INSERT: %v", err)
	}

	// Concurrent remote insert lands first (row clock at CL=0).
	remote := counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), counterInsert(t, f, 1, id, 20, "remote"))
	applyAll(t, f, remote)
	if got, _ := readQty(t, f, id); got != 30 {
		t.Fatalf("qty=%d; want 30 — the remote insert must merge, not erase, the undrained local contribution", got)
	}

	// Drain: ReassertLocal with the local commit's record (higher
	// stamp). Registers re-assert; the counter contribution is already
	// summed and must not double-count.
	qtyCol, _ := f.tab.Column("qty")
	noteCol, _ := f.tab.Column("note")
	localRec := crdt.Insert{Table: f.tab.ID, PK: counterPK(t, f, id), CL: 1, Image: []crdt.ColValue{
		intCol(qtyCol.ID, 10), textCol(noteCol.ID, "local"),
	}}
	if err := f.br.ReassertLocal([]crdt.Record{localRec}, stampAt(300, 1)); err != nil {
		t.Fatalf("ReassertLocal: %v", err)
	}
	if got, _ := readQty(t, f, id); got != 30 {
		t.Fatalf("post-reassert qty=%d; want 30 — reassert must not double-count", got)
	}
	stmt, _, err := f.app.Prepare(`SELECT note FROM inv WHERE id = x'08'`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if ok, err := stmt.Step(); err != nil || !ok {
		t.Fatalf("Step: ok=%v err=%v", ok, err)
	}
	if note := stmt.ColumnText(0); note != "local" {
		t.Fatalf("note=%q; want local (register reassert)", note)
	}
}

// TestCounterApplyOverflowQuarantines: SQLite's + silently promotes to
// REAL on int64 overflow — order-dependent, non-convergent. The apply
// does checked arithmetic in Go instead: the overflowing contribution
// fails deterministically and is quarantined (frontier advances, value
// and storage class untouched).
func TestCounterApplyOverflowQuarantines(t *testing.T) {
	t.Parallel()
	f := counterApplier(t)
	id := []byte{0x09}
	const nearMax = int64(9223372036854775806) // MaxInt64 - 1
	seed := counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, nearMax, "seed"))
	applyAll(t, f, seed)

	over := counterCS(t, crdt.Dot{Origin: 8, Seq: 1}, stampAt(200, 8), counterDelta(t, f, 1, id, 2))
	if err := f.br.applyPayload(context.Background(), over.Encoded()); err != nil {
		t.Fatalf("overflowing delta must quarantine, not error: %v", err)
	}
	if !f.cache.IsAppliedRemote(8, 1) {
		t.Fatalf("quarantine must advance the frontier past the overflowing seq")
	}
	stmt, _, err := f.app.Prepare(`SELECT qty, typeof(qty) FROM inv WHERE id = x'09'`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if ok, err := stmt.Step(); err != nil || !ok {
		t.Fatalf("Step: ok=%v err=%v", ok, err)
	}
	if got, typ := stmt.ColumnInt64(0), stmt.ColumnText(1); got != nearMax || typ != "integer" {
		t.Fatalf("qty=%d typeof=%s; want %d integer — no REAL promotion, no partial apply", got, typ, nearMax)
	}

	// A compensating decrement brings the cell back in range; the
	// quarantined delta then applies cleanly on retry.
	down := counterCS(t, crdt.Dot{Origin: 9, Seq: 1}, stampAt(300, 9), counterDelta(t, f, 1, id, -100))
	applyAll(t, f, down)
	f.br.RetryQuarantined(context.Background())
	if got, _ := readQty(t, f, id); got != nearMax-100+2 {
		t.Fatalf("qty=%d; want %d — quarantined delta must apply once back in range", got, nearMax-100+2)
	}
}

// TestCounterHostileWireQuarantines: FormatDelta is only honored on
// declared integer counter columns; anything else is a deterministic
// wire-contract failure routed to quarantine — never a stamp-arbitration
// bypass or SQL arithmetic on a register.
func TestCounterHostileWireQuarantines(t *testing.T) {
	t.Parallel()
	f := counterApplier(t)
	id := []byte{0x0A}
	seed := counterCS(t, crdt.Dot{Origin: 7, Seq: 1}, stampAt(100, 7), counterInsert(t, f, 1, id, 100, "seed"))
	applyAll(t, f, seed)
	noteCol, _ := f.tab.Column("note")
	qtyCol, _ := f.tab.Column("qty")

	// (a) FormatDelta aimed at a TEXT register column.
	hostileReg := crdt.Update{Table: f.tab.ID, PK: counterPK(t, f, id), CL: 1,
		Changed: []crdt.ColValue{deltaCol(noteCol.ID, 5)}}
	// (b) absolute value aimed at the counter column.
	hostileAbs := crdt.Update{Table: f.tab.ID, PK: counterPK(t, f, id), CL: 1,
		Changed: []crdt.ColValue{intCol(qtyCol.ID, 0)}}
	for i, rec := range []crdt.Record{hostileReg, hostileAbs} {
		cs := counterCS(t, crdt.Dot{Origin: 8, Seq: crdt.Seq(i + 1)}, stampAt(int64(200+i), 8), rec)
		if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("hostile record %d must quarantine, not error: %v", i, err)
		}
	}
	if got, _ := readQty(t, f, id); got != 100 {
		t.Fatalf("qty=%d; want 100 — hostile records must not touch the row", got)
	}
	stmt, _, err := f.app.Prepare(`SELECT note FROM inv WHERE id = x'0A'`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if ok, err := stmt.Step(); err != nil || !ok {
		t.Fatalf("Step: ok=%v err=%v", ok, err)
	}
	if note := stmt.ColumnText(0); note != "seed" {
		t.Fatalf("note=%q; want seed", note)
	}
}
