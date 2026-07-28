package broker

import (
	"bytes"
	"context"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// TestApplyDMLReconciled_FullUpdateRespectsHigherStampPatch covers the
// reconciliation algorithm in BLOB_PATCH.md "insert/update With Active
// blob_range_clock". Order: low-stamp INSERT, high-stamp blob_patch
// extending into bytes [12,16), then a mid-stamp full UPDATE that
// shrinks body to 8 bytes. The higher-stamp patch must survive the
// shrinking UPDATE — visible blob length stays 16 with [12,16) holding
// the patched bytes.
func TestApplyDMLReconciled_FullUpdateRespectsHigherStampPatch(t *testing.T) {
	t.Parallel()
	f := newBlobApplier(t, 1)
	src := crdt.Origin(7)
	idCol := f.tab.PK[0].ID
	bodyCol, _ := f.tab.Column("body")
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: intCol(idCol, 1)})

	// 1. Low-stamp INSERT: 16 bytes 0xAA.
	stampLow := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: src}
	body0 := bytes.Repeat([]byte{0xAA}, 16)
	cs1, err := crdt.Build(crdt.Dot{Origin: src, Seq: 1}, stampLow, nil, testCluster,
		[]crdt.Record{crdt.Insert{
			Table: f.tab.ID, PK: pk, CL: 1,
			Image: []crdt.ColValue{blobCol(bodyCol.ID, body0)},
		}})
	if err != nil {
		t.Fatalf("Build insert: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs1.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}

	// 2. High-stamp blob_patch on bytes [12,16) → DEADBEEF.
	stampHigh := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	patchBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	cs2, err := crdt.Build(crdt.Dot{Origin: src, Seq: 2}, stampHigh, nil, testCluster,
		[]crdt.Record{crdt.BlobPatch{
			Table: f.tab.ID, PK: pk, CL: 1, Col: bodyCol.ID,
			Ranges: []crdt.BlobPatchRange{{Offset: 12, Bytes: patchBytes}},
		}})
	if err != nil {
		t.Fatalf("Build patch: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs2.Encoded()); err != nil {
		t.Fatalf("apply patch: %v", err)
	}

	// 3. Mid-stamp full UPDATE shrinking body to 8 bytes 0xBB.
	stampMid := crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: src}
	bodyShort := bytes.Repeat([]byte{0xBB}, 8)
	cs3, err := crdt.Build(crdt.Dot{Origin: src, Seq: 3}, stampMid, nil, testCluster,
		[]crdt.Record{crdt.Update{
			Table: f.tab.ID, PK: pk, CL: 1,
			Changed: []crdt.ColValue{blobCol(bodyCol.ID, bodyShort)},
		}})
	if err != nil {
		t.Fatalf("Build update: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs3.Encoded()); err != nil {
		t.Fatalf("apply update: %v", err)
	}

	want := append(append(append([]byte{}, bodyShort...), bytes.Repeat([]byte{0x00}, 4)...), patchBytes...)
	got := readBodyBlob(t, f.app, 1)
	if !bytes.Equal(got, want) {
		t.Errorf("body = % x; want % x (high-stamp patch must survive shrinking UPDATE)", got, want)
	}

	entries, err := f.br.cfg.Meta.GetBlobRangeClock(f.tab.ID, pk)
	if err != nil {
		t.Fatalf("GetBlobRangeClock: %v", err)
	}
	if len(entries) != 1 || entries[0].Column != bodyCol.ID {
		t.Fatalf("entries = %+v; want one entry for body col", entries)
	}
	if got, want := entries[0].Entries, ([]crdt.IntervalEntry{{Range: crdt.ByteRange{Start: 12, End: 16}, Stamp: stampHigh}}); !equalIntervalEntries(got, want) {
		t.Errorf("body intervals = %+v; want %+v", got, want)
	}
}

// TestApplyDMLReconciled_LowerStampUpdateAbsorbedWhenNoSurviving covers
// the fast-collapse case: the row has range_clock entries from a prior
// patch but the incoming full DML's stamp dominates every entry. The
// reconciliation algorithm should absorb the patch and leave no
// blob_range_clock row.
func TestApplyDMLReconciled_FullUpdateAbsorbsLowerStampPatches(t *testing.T) {
	t.Parallel()
	f := newBlobApplier(t, 1)
	src := crdt.Origin(7)
	idCol := f.tab.PK[0].ID
	bodyCol, _ := f.tab.Column("body")
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: intCol(idCol, 1)})

	stampLow := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: src}
	cs1, _ := crdt.Build(crdt.Dot{Origin: src, Seq: 1}, stampLow, nil, testCluster,
		[]crdt.Record{crdt.Insert{
			Table: f.tab.ID, PK: pk, CL: 1,
			Image: []crdt.ColValue{blobCol(bodyCol.ID, bytes.Repeat([]byte{0xAA}, 8))},
		}})
	if err := f.br.applyPayload(context.Background(), cs1.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}

	// Patch at a low-but-strictly-greater stamp.
	stampPatch := crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: src}
	cs2, _ := crdt.Build(crdt.Dot{Origin: src, Seq: 2}, stampPatch, nil, testCluster,
		[]crdt.Record{crdt.BlobPatch{
			Table: f.tab.ID, PK: pk, CL: 1, Col: bodyCol.ID,
			Ranges: []crdt.BlobPatchRange{{Offset: 4, Bytes: []byte{0xCC, 0xCC, 0xCC, 0xCC}}},
		}})
	if err := f.br.applyPayload(context.Background(), cs2.Encoded()); err != nil {
		t.Fatalf("apply patch: %v", err)
	}

	// Full UPDATE at higher stamp — should absorb the patch entry.
	stampHigh := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	body := bytes.Repeat([]byte{0xBB}, 12)
	cs3, _ := crdt.Build(crdt.Dot{Origin: src, Seq: 3}, stampHigh, nil, testCluster,
		[]crdt.Record{crdt.Update{
			Table: f.tab.ID, PK: pk, CL: 1,
			Changed: []crdt.ColValue{blobCol(bodyCol.ID, body)},
		}})
	if err := f.br.applyPayload(context.Background(), cs3.Encoded()); err != nil {
		t.Fatalf("apply update: %v", err)
	}

	got := readBodyBlob(t, f.app, 1)
	if !bytes.Equal(got, body) {
		t.Errorf("body = % x; want % x (full-DML at higher stamp absorbs patch)", got, body)
	}
	entries, err := f.br.cfg.Meta.GetBlobRangeClock(f.tab.ID, pk)
	if err != nil {
		t.Fatalf("GetBlobRangeClock: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("range_clock = %+v; want empty after absorbing dominated patches", entries)
	}
}

func TestApplyDMLReconciled_InsertSeedsMissingBlobNotNull(t *testing.T) {
	t.Parallel()
	f := newBlobApplierWithSchema(t, 1, `CREATE TABLE blobrow (
		id    INTEGER PRIMARY KEY,
		name  TEXT DEFAULT '',
		body  BLOB NOT NULL
	)`)
	src := crdt.Origin(7)
	idCol := f.tab.PK[0].ID
	nameCol, _ := f.tab.Column("name")
	bodyCol, _ := f.tab.Column("body")
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: intCol(idCol, 1)})

	patch := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	csPatch, err := crdt.Build(
		crdt.Dot{Origin: src, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: src},
		nil,
		testCluster,
		[]crdt.Record{crdt.BlobPatch{
			Table: f.tab.ID, PK: pk, CL: 1, Col: bodyCol.ID,
			Ranges: []crdt.BlobPatchRange{{Offset: 0, Bytes: patch}},
		}},
	)
	if err != nil {
		t.Fatalf("Build patch: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), csPatch.Encoded()); err != nil {
		t.Fatalf("apply patch: %v", err)
	}

	csInsert, err := crdt.Build(
		crdt.Dot{Origin: src, Seq: 2},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: src},
		nil,
		testCluster,
		[]crdt.Record{crdt.Insert{
			Table: f.tab.ID, PK: pk, CL: 1,
			Image: []crdt.ColValue{textCol(nameCol.ID, "hello")},
		}},
	)
	if err != nil {
		t.Fatalf("Build insert: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), csInsert.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}
	if got := readBodyBlob(t, f.app, 1); !bytes.Equal(got, patch) {
		t.Errorf("body = % x; want patched bytes % x", got, patch)
	}
}

// TestApplyDMLReconciled_ConvergesOutOfOrder verifies replicas applying
// the same record set in different orders converge byte-for-byte. One
// replica sees [INSERT, UPDATE, PATCH], the other [INSERT, PATCH,
// UPDATE]. Both must end up at the identical body and range_clock.
func TestApplyDMLReconciled_ConvergesOutOfOrder(t *testing.T) {
	t.Parallel()
	a := newBlobApplier(t, 1)
	b := newBlobApplier(t, 2)
	src := crdt.Origin(7)
	idCol := a.tab.PK[0].ID
	bodyCol, _ := a.tab.Column("body")
	pk, _ := a.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: intCol(idCol, 1)})

	stampLow := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: src}
	stampMid := crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: src}
	stampHigh := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}

	body0 := bytes.Repeat([]byte{0xAA}, 16)
	bodyShort := bytes.Repeat([]byte{0xBB}, 8)
	patchBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	csInsert, _ := crdt.Build(crdt.Dot{Origin: src, Seq: 1}, stampLow, nil, testCluster,
		[]crdt.Record{crdt.Insert{
			Table: a.tab.ID, PK: pk, CL: 1,
			Image: []crdt.ColValue{blobCol(bodyCol.ID, body0)},
		}})
	csUpdate, _ := crdt.Build(crdt.Dot{Origin: src, Seq: 3}, stampMid, nil, testCluster,
		[]crdt.Record{crdt.Update{
			Table: a.tab.ID, PK: pk, CL: 1,
			Changed: []crdt.ColValue{blobCol(bodyCol.ID, bodyShort)},
		}})
	csPatch, _ := crdt.Build(crdt.Dot{Origin: src, Seq: 2}, stampHigh, nil, testCluster,
		[]crdt.Record{crdt.BlobPatch{
			Table: a.tab.ID, PK: pk, CL: 1, Col: bodyCol.ID,
			Ranges: []crdt.BlobPatchRange{{Offset: 12, Bytes: patchBytes}},
		}})

	for _, cs := range []*crdt.Changeset{csInsert, csUpdate, csPatch} {
		if err := a.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("apply A: %v", err)
		}
	}
	for _, cs := range []*crdt.Changeset{csInsert, csPatch, csUpdate} {
		if err := b.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
			t.Fatalf("apply B: %v", err)
		}
	}

	bodyA := readBodyBlob(t, a.app, 1)
	bodyB := readBodyBlob(t, b.app, 1)
	if !bytes.Equal(bodyA, bodyB) {
		t.Fatalf("divergence: A=% x B=% x", bodyA, bodyB)
	}
	entA, _ := a.br.cfg.Meta.GetBlobRangeClock(a.tab.ID, pk)
	entB, _ := b.br.cfg.Meta.GetBlobRangeClock(b.tab.ID, pk)
	if len(entA) != len(entB) {
		t.Fatalf("range_clock cardinality: A=%d B=%d", len(entA), len(entB))
	}
	for i := range entA {
		if entA[i].Column != entB[i].Column {
			t.Errorf("entA[%d].Column=%v entB[%d].Column=%v", i, entA[i].Column, i, entB[i].Column)
		}
		if !equalIntervalEntries(entA[i].Entries, entB[i].Entries) {
			t.Errorf("entries[%d]: A=%+v B=%+v", i, entA[i].Entries, entB[i].Entries)
		}
	}
}

func equalIntervalEntries(a, b []crdt.IntervalEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}
