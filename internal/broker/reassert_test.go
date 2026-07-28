package broker

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// TestReassertLocalRestoresClobberedCommit pins the commit→drain race:
// a local transaction commits to app.db, and before the drain advances
// the row clock, an inbound changeset (stamped above the clock the
// gate sees, but below the local commit) overwrites the row. The
// drain's ReassertLocal call must re-apply the local content and
// advance the clock so later deliveries gate correctly.
func TestReassertLocalRestoresClobberedCommit(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, nil)
	self := f.cache.Self()
	src := crdt.Origin(7)
	nCol, _ := f.tab.Column("n")
	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x01})})

	// Remote insert establishes the row at stamp 1000.
	s1 := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	cs1 := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 1}, s1, 1, []byte{0x01}, "base")
	if err := f.br.applyPayload(context.Background(), cs1.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}

	// Local commit at stamp 3000: app.db has the row, but the clock
	// advance is still queued behind the drain (simulated by writing
	// app.db directly and NOT touching the cache).
	if err := f.app.Exec(`UPDATE event SET n = 'local' WHERE id = x'01'`); err != nil {
		t.Fatalf("local UPDATE: %v", err)
	}
	localStamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 3000}, Origin: self}
	localRec := crdt.Update{Table: f.tab.ID, PK: pk, CL: 1,
		Changed: []crdt.ColValue{textCol(nCol.ID, "local")}}

	// Inbound update at stamp 2000 passes the LWW gate (clock still at
	// 1000) and clobbers the locally-committed content.
	s2 := crdt.Stamp{Clock: crdt.Clock{WallTime: 2000}, Origin: src}
	upd := crdt.Update{Table: f.tab.ID, PK: pk, CL: 1,
		Changed: []crdt.ColValue{textCol(nCol.ID, "remote")}}
	cs2, err := crdt.Build(crdt.Dot{Origin: src, Seq: 2}, s2, nil, testCluster, []crdt.Record{upd})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs2.Encoded()); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	if got := readNCol(t, f.app, []byte{0x01}); got != "remote" {
		t.Fatalf("pre-reassert n = %q; want remote (clobber happened)", got)
	}

	// The drain materializes the local commit and calls ReassertLocal:
	// the row clock was last advanced by a remote write the local
	// commit dominates, so the DML re-applies and the clock advances.
	if err := f.br.ReassertLocal([]crdt.Record{localRec}, localStamp); err != nil {
		t.Fatalf("ReassertLocal: %v", err)
	}
	if got := readNCol(t, f.app, []byte{0x01}); got != "local" {
		t.Fatalf("post-reassert n = %q; want local", got)
	}
	rs := f.cache.RowState(f.tab.ID, pk)
	if rs.Base != localStamp {
		t.Fatalf("row clock base = %+v; want %+v", rs.Base, localStamp)
	}

	// A straggler stamped between the clobber and the local commit now
	// gates correctly.
	s3 := crdt.Stamp{Clock: crdt.Clock{WallTime: 2500}, Origin: src}
	upd3 := crdt.Update{Table: f.tab.ID, PK: pk, CL: 1,
		Changed: []crdt.ColValue{textCol(nCol.ID, "straggler")}}
	cs3, err := crdt.Build(crdt.Dot{Origin: src, Seq: 3}, s3, nil, testCluster, []crdt.Record{upd3})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs3.Encoded()); err != nil {
		t.Fatalf("apply straggler: %v", err)
	}
	if got := readNCol(t, f.app, []byte{0x01}); got != "local" {
		t.Fatalf("post-straggler n = %q; want local (gate must reject)", got)
	}
}

// TestReassertLocalSkipsOwnChain pins the common single-writer case:
// when the row clock is still on this node's own chain, ReassertLocal
// must not touch app.db (the journal record's effects are already
// there) — observable as the row keeping a divergent value written
// out-of-band.
func TestReassertLocalSkipsOwnChain(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, nil)
	self := f.cache.Self()
	nCol, _ := f.tab.Column("n")
	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x01})})

	// Row clock already on self's chain.
	f.cache.PutRowState(f.tab.ID, pk, crdt.RowState{CL: 1,
		Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: self}})
	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'sentinel')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rec := crdt.Update{Table: f.tab.ID, PK: pk, CL: 1,
		Changed: []crdt.ColValue{textCol(nCol.ID, "drained")}}
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 2000}, Origin: self}
	if err := f.br.ReassertLocal([]crdt.Record{rec}, stamp); err != nil {
		t.Fatalf("ReassertLocal: %v", err)
	}
	if got := readNCol(t, f.app, []byte{0x01}); got != "sentinel" {
		t.Fatalf("n = %q; want sentinel (own-chain commit must not re-apply DML)", got)
	}
}
