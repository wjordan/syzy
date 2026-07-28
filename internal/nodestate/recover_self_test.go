package nodestate

import (
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// selfLogRecord is one appended self-log entry: the changeset plus the
// source self-journal endOffset it carried in its header.
type selfLogRecord struct {
	cs     *crdt.Changeset
	endOff journal.Offset
}

// writeSelfLog builds a self-log journal exactly as mirror.AppendSelf does:
// KindMirror records whose header hlc field carries the source endOffset and
// whose payload is the pristine wire changeset.
func writeSelfLog(t *testing.T, recs []selfLogRecord) *journal.Journal {
	t.Helper()
	j, err := journal.Open(t.TempDir(), 64*1024, journal.SyncOn)
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })
	for _, r := range recs {
		if _, _, err := j.Append(journal.KindMirror, uint64(r.endOff), uint64(r.cs.Dot.Origin), r.cs.Encoded()); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	return j
}

const recoverSelfOrigin = crdt.Origin(7)

func mkInsert(t *testing.T, seq crdt.Seq, wall int64, table byte, pk byte, cl uint64) *crdt.Changeset {
	t.Helper()
	cs, err := crdt.Build(
		crdt.Dot{Origin: recoverSelfOrigin, Seq: seq},
		crdt.Stamp{Clock: crdt.Clock{WallTime: wall}, Origin: recoverSelfOrigin},
		nil, crdt.ClusterID{0xCC},
		[]crdt.Record{crdt.Insert{
			Table: crdt.TableID{table}, PK: crdt.PKBlob{pk}, CL: cl,
			Image: []crdt.ColValue{{Column: crdt.ColumnID{1}, TypeTag: 1, Bytes: []byte{pk}}},
		}},
	)
	if err != nil {
		t.Fatalf("Build insert: %v", err)
	}
	return cs
}

func mkDelete(t *testing.T, seq crdt.Seq, wall int64, table byte, pk byte, cl uint64) *crdt.Changeset {
	t.Helper()
	cs, err := crdt.Build(
		crdt.Dot{Origin: recoverSelfOrigin, Seq: seq},
		crdt.Stamp{Clock: crdt.Clock{WallTime: wall}, Origin: recoverSelfOrigin},
		nil, crdt.ClusterID{0xCC},
		[]crdt.Record{crdt.Delete{Table: crdt.TableID{table}, PK: crdt.PKBlob{pk}, CL: cl}},
	)
	if err != nil {
		t.Fatalf("Build delete: %v", err)
	}
	return cs
}

// assertRecovered checks the cache reflects the fixed 3-record self-log:
// senderNextSeq past seq 3, marker at the tip endOffset, row A tombstoned at
// CL 2, row B live at CL 1.
func assertRecovered(t *testing.T, cache *Cache) {
	t.Helper()
	if got := cache.SenderNextSeq(recoverSelfOrigin); got != 4 {
		t.Errorf("SenderNextSeq = %d, want 4 (max seq 3 + 1)", got)
	}
	if got := cache.SnapshotMarker(recoverSelfOrigin); got != 300 {
		t.Errorf("SnapshotMarker = %d, want 300 (tip endOffset)", got)
	}
	if got := cache.RowState(crdt.TableID{0x01}, crdt.PKBlob{0xAA}).CL; got != 2 {
		t.Errorf("row A CL = %d, want 2 (delete seq 3 supersedes insert seq 1)", got)
	}
	if got := cache.RowState(crdt.TableID{0x01}, crdt.PKBlob{0xBB}).CL; got != 1 {
		t.Errorf("row B CL = %d, want 1 (insert seq 2)", got)
	}
}

// fixedSelfLog is the shared 3-record fixture: insert A@CL1, insert B@CL1,
// delete A@CL2, with strictly increasing endOffsets 100/200/300.
func fixedSelfLog(t *testing.T) []selfLogRecord {
	return []selfLogRecord{
		{mkInsert(t, 1, 1001, 0x01, 0xAA, 1), 100},
		{mkInsert(t, 2, 1002, 0x01, 0xBB, 1), 200},
		{mkDelete(t, 3, 1003, 0x01, 0xAA, 2), 300},
	}
}

// TestRecoverSelf_RestoresAndIsIdempotent is the core recovery contract:
// replaying the self-log restores senderNextSeq / marker / row_clock from the
// captured wire bytes, and re-running recovery (or replaying against a cache
// the snapshot already advanced past) changes nothing — the marker never
// regresses, no seq is re-derived.
func TestRecoverSelf_RestoresAndIsIdempotent(t *testing.T) {
	j := writeSelfLog(t, fixedSelfLog(t))
	cache := New(recoverSelfOrigin)

	if err := RecoverSelf(cache, j, nil, nil); err != nil {
		t.Fatalf("RecoverSelf: %v", err)
	}
	assertRecovered(t, cache)

	// Second pass: pure idempotency — same journal, same cache.
	if err := RecoverSelf(cache, j, nil, nil); err != nil {
		t.Fatalf("RecoverSelf (2nd): %v", err)
	}
	assertRecovered(t, cache)
}

// TestRecoverSelf_NoOpAgainstCoveringSnapshot: a cache whose loaded snapshot
// already covers the whole self-log recovers to the identical state — the
// dominance/idempotency gates make every record a no-op, and the marker holds.
func TestRecoverSelf_NoOpAgainstCoveringSnapshot(t *testing.T) {
	j := writeSelfLog(t, fixedSelfLog(t))

	// Cache A: recover from empty (the reference).
	ref := New(recoverSelfOrigin)
	if err := RecoverSelf(ref, j, nil, nil); err != nil {
		t.Fatalf("RecoverSelf ref: %v", err)
	}

	// Cache B: pre-seed as if a snapshot already persisted the tip state,
	// then recover. Must not regress marker or row state.
	covered := New(recoverSelfOrigin)
	covered.ObserveSelfSeq(recoverSelfOrigin, 3)
	covered.SetSnapshotMarker(recoverSelfOrigin, 300)
	covered.PutRowState(crdt.TableID{0x01}, crdt.PKBlob{0xAA}, crdt.RowState{CL: 2, Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 1003}, Origin: recoverSelfOrigin}})
	covered.PutRowState(crdt.TableID{0x01}, crdt.PKBlob{0xBB}, crdt.RowState{CL: 1, Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 1002}, Origin: recoverSelfOrigin}})
	if err := RecoverSelf(covered, j, nil, nil); err != nil {
		t.Fatalf("RecoverSelf covered: %v", err)
	}
	assertRecovered(t, covered)
	if covered.SenderNextSeq(recoverSelfOrigin) != ref.SenderNextSeq(recoverSelfOrigin) {
		t.Errorf("covered senderNextSeq %d != ref %d", covered.SenderNextSeq(recoverSelfOrigin), ref.SenderNextSeq(recoverSelfOrigin))
	}
}

func TestRecoverSelf_LegacyRecordTolerated(t *testing.T) {
	j := writeSelfLog(t, []selfLogRecord{
		{mkInsert(t, 1, 1001, 0x01, 0xAA, 1), 0},
		{mkInsert(t, 2, 1002, 0x01, 0xBB, 1), 0},
	})
	cache := New(recoverSelfOrigin)
	cache.ObserveSelfSeq(recoverSelfOrigin, 2)
	cache.SetSnapshotMarker(recoverSelfOrigin, 500)
	if err := RecoverSelf(cache, j, nil, nil); err != nil {
		t.Fatalf("RecoverSelf rejected a pre-self-log record: %v", err)
	}
	if got := cache.SnapshotMarker(recoverSelfOrigin); got != 500 {
		t.Errorf("SnapshotMarker = %d, want 500", got)
	}
}

func TestRecoverSelf_LegacyThenCurrent(t *testing.T) {
	j := writeSelfLog(t, []selfLogRecord{
		{mkInsert(t, 1, 1001, 0x01, 0xAA, 1), 0},
		{mkInsert(t, 2, 1002, 0x01, 0xBB, 1), 200},
		{mkDelete(t, 3, 1003, 0x01, 0xAA, 2), 300},
	})
	cache := New(recoverSelfOrigin)
	if err := RecoverSelf(cache, j, nil, nil); err != nil {
		t.Fatalf("RecoverSelf: %v", err)
	}
	if got := cache.SenderNextSeq(recoverSelfOrigin); got != 4 {
		t.Errorf("SenderNextSeq = %d, want 4", got)
	}
	if got := cache.SnapshotMarker(recoverSelfOrigin); got != 300 {
		t.Errorf("SnapshotMarker = %d, want 300", got)
	}
	if got := cache.RowState(crdt.TableID{0x01}, crdt.PKBlob{0xBB}).CL; got != 1 {
		t.Errorf("row B CL = %d, want 1", got)
	}
}

// TestRecoverSelf_PromotionGapFatal: a persisted senderNextSeq ahead of the
// self-log's coverage means seqs were produced without capture — the
// single-node→replicated promotion footgun. Recovery must fail fast rather
// than resume past uncaptured seqs and wedge peers that can never fetch them.
func TestRecoverSelf_PromotionGapFatal(t *testing.T) {
	j := writeSelfLog(t, []selfLogRecord{
		{mkInsert(t, 1, 1001, 0x01, 0xAA, 1), 100},
		{mkInsert(t, 2, 1002, 0x01, 0xBB, 1), 200},
	})
	cache := New(recoverSelfOrigin)
	// Claim seqs up to 4 (senderNextSeq=5) though the self-log covers only 2.
	cache.ObserveSelfSeq(recoverSelfOrigin, 4)
	if err := RecoverSelf(cache, j, nil, nil); err == nil {
		t.Fatal("RecoverSelf tolerated uncaptured produced seqs; want fatal promotion guard")
	}
}

// TestRecoverSelf_OriginMismatchFatal: a self-log payload whose Dot.Origin is
// not this node's own origin means a mis-provisioned or corrupt log; the
// self-log is our only copy of our own bytes, so this is fatal.
func TestRecoverSelf_OriginMismatchFatal(t *testing.T) {
	other := crdt.Origin(99)
	cs, err := crdt.Build(
		crdt.Dot{Origin: other, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 1001}, Origin: other},
		nil, crdt.ClusterID{0xCC},
		[]crdt.Record{crdt.Delete{Table: crdt.TableID{0x01}, PK: crdt.PKBlob{0xAA}, CL: 2}},
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	j := writeSelfLog(t, []selfLogRecord{{cs, 100}})
	cache := New(recoverSelfOrigin)
	if err := RecoverSelf(cache, j, nil, nil); err == nil {
		t.Fatal("RecoverSelf tolerated a foreign-origin payload; want fatal")
	}
}
