package metadata

import (
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// seedSourceLikeMeta fills sc as if it were the source side of a
// clone: a couple of remote frontiers, an existing local origin with a
// stale sender_seq, snapshot_markers + intent set, and an hlc_last value.
func seedSourceLikeMeta(t *testing.T, sc *Store, srcOrigin crdt.Origin) {
	t.Helper()
	if err := sc.SetClusterID(crdt.ClusterID{0xAB, 0xCD}); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(srcOrigin); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}
	if err := sc.SetHLCLast(crdt.Clock{WallTime: 1_000_000, Logical: 7}); err != nil {
		t.Fatalf("SetHLCLast: %v", err)
	}
	if err := sc.AdvanceFrontier(srcOrigin, 42, crdt.Clock{WallTime: 1_000_000, Logical: 7}); err != nil {
		t.Fatalf("AdvanceFrontier(src): %v", err)
	}
	if err := sc.AdvanceFrontier(crdt.Origin(0x1111), 5, crdt.Clock{WallTime: 999_000, Logical: 0}); err != nil {
		t.Fatalf("AdvanceFrontier(remote): %v", err)
	}
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.PutSenderSeq(srcOrigin, 43)
	}); err != nil {
		t.Fatalf("PutSenderSeq: %v", err)
	}
	if err := sc.SetSnapshotMarkers(map[crdt.Origin]uint64{srcOrigin: 8192}); err != nil {
		t.Fatalf("SetSnapshotMarkers: %v", err)
	}
	if err := sc.SetOriginIntent(srcOrigin, []byte{byte(IntentClone), 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatalf("SetIntent: %v", err)
	}
}

func TestAdoptClone_RewritesIdentityAndPreservesHistory(t *testing.T) {
	dir := t.TempDir()
	sc, err := Open(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sc.Close()

	const srcOrigin = crdt.Origin(0xDEAD_BEEF)
	const newOrigin = crdt.Origin(0x1234_5678)
	seedSourceLikeMeta(t, sc, srcOrigin)

	// AdoptClone with an HLC older than the loaded one — must NOT regress.
	if err := sc.AdoptClone(newOrigin, crdt.Clock{WallTime: 500_000, Logical: 0}); err != nil {
		t.Fatalf("AdoptClone: %v", err)
	}

	// node_id reflects newOrigin.
	if got, ok, err := sc.GetNodeID(); err != nil || !ok || got != newOrigin {
		t.Fatalf("GetNodeID: got=%x ok=%v err=%v want=%x", got, ok, err, newOrigin)
	}
	// clean_shutdown is set.
	if v, ok, err := sc.GetCleanShutdown(); err != nil || !ok || !v {
		t.Fatalf("GetCleanShutdown: got=%v ok=%v err=%v", v, ok, err)
	}
	// hlc_last did not regress.
	if hlc, _, _ := sc.GetHLCLast(); hlc.WallTime != 1_000_000 || hlc.Logical != 7 {
		t.Fatalf("hlc_last regressed: %+v", hlc)
	}
	// sender_seq has exactly one row, for newOrigin, at 1.
	seqs, err := sc.SenderSeqs()
	if err != nil {
		t.Fatalf("SenderSeqs: %v", err)
	}
	if len(seqs) != 1 || seqs[newOrigin] != 1 {
		t.Fatalf("SenderSeqs after adopt: %+v", seqs)
	}
	// frontier preserves the source's prior origins AND adds the new one.
	front, err := sc.Frontier()
	if err != nil {
		t.Fatalf("Frontier: %v", err)
	}
	if front[srcOrigin].LastSeq != 42 {
		t.Fatalf("source frontier dropped: %+v", front[srcOrigin])
	}
	if front[crdt.Origin(0x1111)].LastSeq != 5 {
		t.Fatalf("remote frontier dropped: %+v", front[crdt.Origin(0x1111)])
	}
	if got := front[newOrigin]; got.LastSeq != 0 || got.LastHLC.WallTime != 1_000_000 {
		t.Fatalf("new frontier seed: %+v", got)
	}
	// intent + snapshot_markers cleared.
	if all, _ := sc.ListIntents(); len(all) != 0 {
		t.Fatalf("intents not cleared")
	}
	if m, _ := sc.GetSnapshotMarkers(); len(m) != 0 {
		t.Fatalf("snapshot_markers not cleared: %+v", m)
	}
	// cluster_id preserved.
	if cid, ok, _ := sc.GetClusterID(); !ok || cid != (crdt.ClusterID{0xAB, 0xCD}) {
		t.Fatalf("cluster_id lost: ok=%v %x", ok, cid)
	}
}

func TestAdoptClone_PullsHLCForward(t *testing.T) {
	dir := t.TempDir()
	sc, err := Open(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sc.Close()

	if err := sc.SetHLCLast(crdt.Clock{WallTime: 100, Logical: 0}); err != nil {
		t.Fatalf("SetHLCLast: %v", err)
	}
	if err := sc.AdoptClone(crdt.Origin(7), crdt.Clock{WallTime: 5_000, Logical: 3}); err != nil {
		t.Fatalf("AdoptClone: %v", err)
	}
	hlc, _, _ := sc.GetHLCLast()
	if hlc.WallTime != 5_000 || hlc.Logical != 3 {
		t.Fatalf("hlc not pulled forward: %+v", hlc)
	}
}

func TestAdoptClone_RejectsZeroOrigin(t *testing.T) {
	dir := t.TempDir()
	sc, err := Open(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sc.Close()
	if err := sc.AdoptClone(0, crdt.Clock{}); err == nil {
		t.Fatalf("AdoptClone with zero origin should error")
	}
}
