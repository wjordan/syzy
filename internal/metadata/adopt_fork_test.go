package metadata

import (
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// seedForkSource fills sc as if it were a parent app's metadata at the
// moment of a fork: catalog rows, a couple of frontiers, applied_gaps,
// schema-log events with a non-zero schema_seq, an intent + snapshot
// markers, plus a parent_app_txid that the fork must preserve.
func seedForkSource(t *testing.T, sc *Store, srcOrigin crdt.Origin) {
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
		t.Fatalf("AdvanceFrontier(peer): %v", err)
	}
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.PutSenderSeq(srcOrigin, 43)
	}); err != nil {
		t.Fatalf("PutSenderSeq: %v", err)
	}
	if err := sc.SetSchemaSeq(9); err != nil {
		t.Fatalf("SetSchemaSeq: %v", err)
	}
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.AppendSchemaEvent(SchemaEventEntry{
			SchemaSeq:   1,
			ParentSeq:   0,
			CatalogOp:   []byte{0x01, 0x02},
			RawSQL:      "CREATE TABLE t (x INTEGER)",
			AppliedAtUs: 12345,
			ApplyState:  ApplyStateApplied,
		})
	}); err != nil {
		t.Fatalf("AppendSchemaEvent: %v", err)
	}
	if err := sc.SetSnapshotMarkers(map[crdt.Origin]uint64{srcOrigin: 8192}); err != nil {
		t.Fatalf("SetSnapshotMarkers: %v", err)
	}
	if err := sc.SetAppliedGaps(map[crdt.Origin]crdt.SeqSet{crdt.Origin(0x1111): seqSetWith(2, 3, 4)}); err != nil {
		t.Fatalf("SetAppliedGaps: %v", err)
	}
	if err := sc.SetOriginIntent(srcOrigin, []byte{byte(IntentLocalDDL), 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatalf("SetIntent: %v", err)
	}
	// parent_app_txid is just a meta key; set it directly to simulate
	// what the publisher stamps in the source's metadata.db.
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.putUint64("parent_app_txid", 0xCAFE)
	}); err != nil {
		t.Fatalf("set parent_app_txid: %v", err)
	}
}

func seqSetWith(seqs ...uint64) crdt.SeqSet {
	var ss crdt.SeqSet
	for _, s := range seqs {
		ss.Add(crdt.Seq(s))
	}
	return ss
}

func TestAdoptFork_RewritesIdentityWipesClusterHistory(t *testing.T) {
	dir := t.TempDir()
	sc, err := Open(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sc.Close()

	const srcOrigin = crdt.Origin(0xDEAD_BEEF)
	const newOrigin = crdt.Origin(0x1234_5678)
	newCluster := crdt.ClusterID{0x55, 0x66, 0x77}
	seedForkSource(t, sc, srcOrigin)

	if err := sc.AdoptFork(newOrigin, newCluster, crdt.Clock{WallTime: 500_000, Logical: 0}); err != nil {
		t.Fatalf("AdoptFork: %v", err)
	}

	// Identity rewritten.
	if got, _, _ := sc.GetClusterID(); got != newCluster {
		t.Fatalf("cluster_id: got %x want %x", got, newCluster)
	}
	if got, ok, err := sc.GetNodeID(); err != nil || !ok || got != newOrigin {
		t.Fatalf("GetNodeID: got=%x ok=%v err=%v want=%x", got, ok, err, newOrigin)
	}

	// HLC did not regress.
	if hlc, _, _ := sc.GetHLCLast(); hlc.WallTime != 1_000_000 || hlc.Logical != 7 {
		t.Fatalf("hlc regressed: %+v", hlc)
	}

	// sender_seq has exactly one row, for newOrigin, at 1.
	seqs, err := sc.SenderSeqs()
	if err != nil {
		t.Fatalf("SenderSeqs: %v", err)
	}
	if len(seqs) != 1 || seqs[newOrigin] != 1 {
		t.Fatalf("SenderSeqs: %+v", seqs)
	}

	// frontier wiped except for new origin seed at (0, hlc).
	front, err := sc.Frontier()
	if err != nil {
		t.Fatalf("Frontier: %v", err)
	}
	if len(front) != 1 {
		t.Fatalf("frontier not wiped: %+v", front)
	}
	if got := front[newOrigin]; got.LastSeq != 0 || got.LastHLC.WallTime != 1_000_000 {
		t.Fatalf("new frontier seed: %+v", got)
	}

	// schema-log namespace reset.
	if seq, _, _ := sc.GetSchemaSeq(); seq != 0 {
		t.Fatalf("schema_seq not reset: %d", seq)
	}
	cnt, err := schemaEventCount(sc)
	if err != nil {
		t.Fatalf("schemaEventCount: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("syzy_schema_event not wiped: %d rows", cnt)
	}

	// applied_gaps + snapshot_markers + intent all cleared.
	if g, _ := sc.GetAppliedGaps(); len(g) != 0 {
		t.Fatalf("applied_gaps not cleared: %+v", g)
	}
	if m, _ := sc.GetSnapshotMarkers(); len(m) != 0 {
		t.Fatalf("snapshot_markers not cleared: %+v", m)
	}
	if all, _ := sc.ListIntents(); len(all) != 0 {
		t.Fatalf("intents not cleared")
	}

	// clean_shutdown true.
	if v, ok, err := sc.GetCleanShutdown(); err != nil || !ok || !v {
		t.Fatalf("clean_shutdown: got=%v ok=%v err=%v", v, ok, err)
	}

	// parent_app_txid preserved.
	v, ok, err := sc.GetMeta("parent_app_txid")
	if err != nil || !ok {
		t.Fatalf("parent_app_txid missing: ok=%v err=%v", ok, err)
	}
	if len(v) != 8 {
		t.Fatalf("parent_app_txid wrong width: %d", len(v))
	}
}

// TestAdoptFork_PreservesReplicateUnderscore checks the per-slot
// ReplicateUnderscoreTables flag survives a fork. Set it on a source
// slot, run AdoptFork, then read it back — the destination slot must
// observe the same value so the fork's producer.New uses the parent's
// underscore-replication mode (e.g. a forked PocketBase template keeps
// replicating PB's underscore tables).
func TestAdoptFork_PreservesReplicateUnderscore(t *testing.T) {
	dir := t.TempDir()
	sc, err := Open(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sc.Close()

	const srcOrigin = crdt.Origin(0xDEAD_BEEF)
	const newOrigin = crdt.Origin(0x1234_5678)
	newCluster := crdt.ClusterID{0x55, 0x66, 0x77}
	seedForkSource(t, sc, srcOrigin)

	if err := sc.SetReplicateUnderscoreTables(true); err != nil {
		t.Fatalf("SetReplicateUnderscoreTables: %v", err)
	}

	if err := sc.AdoptFork(newOrigin, newCluster, crdt.Clock{WallTime: 500_000, Logical: 0}); err != nil {
		t.Fatalf("AdoptFork: %v", err)
	}

	got, ok, err := sc.GetReplicateUnderscoreTables()
	if err != nil {
		t.Fatalf("GetReplicateUnderscoreTables: %v", err)
	}
	if !ok {
		t.Fatalf("replicate_underscore not preserved through AdoptFork")
	}
	if !got {
		t.Fatalf("replicate_underscore lost its value: got %v want true", got)
	}
}

func TestAdoptFork_PullsHLCForward(t *testing.T) {
	dir := t.TempDir()
	sc, err := Open(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sc.Close()

	if err := sc.SetHLCLast(crdt.Clock{WallTime: 100, Logical: 0}); err != nil {
		t.Fatalf("SetHLCLast: %v", err)
	}
	if err := sc.AdoptFork(crdt.Origin(7), crdt.ClusterID{0x01}, crdt.Clock{WallTime: 5_000, Logical: 3}); err != nil {
		t.Fatalf("AdoptFork: %v", err)
	}
	hlc, _, _ := sc.GetHLCLast()
	if hlc.WallTime != 5_000 || hlc.Logical != 3 {
		t.Fatalf("hlc not pulled forward: %+v", hlc)
	}
}

func TestAdoptFork_RejectsZeroes(t *testing.T) {
	dir := t.TempDir()
	sc, err := Open(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sc.Close()
	if err := sc.AdoptFork(0, crdt.ClusterID{0x01}, crdt.Clock{}); err == nil {
		t.Fatalf("AdoptFork with zero origin should error")
	}
	if err := sc.AdoptFork(crdt.Origin(1), crdt.ClusterID{}, crdt.Clock{}); err == nil {
		t.Fatalf("AdoptFork with zero cluster_id should error")
	}
}

func schemaEventCount(sc *Store) (int, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	stmt, _, err := sc.conn.Prepare(`SELECT COUNT(*) FROM syzy_schema_event`)
	if err != nil {
		return 0, err
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		return 0, err
	}
	return int(stmt.ColumnInt64(0)), nil
}
