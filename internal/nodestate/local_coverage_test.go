package nodestate

import (
	"reflect"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

func TestRetiredLocalProductionIsCoveredWithoutChangingRemoteReceipts(t *testing.T) {
	t.Parallel()
	c := New(2)
	c.ObserveSelfSeq(7, 500000)
	if got := c.MissingRangesUpTo(7, 500000); len(got) != 0 {
		t.Fatalf("re-fetching local history: %+v", got)
	}
	if c.IsAppliedRemote(7, 500000) {
		t.Fatal("planning must not bypass journal recovery")
	}
	if _, ok := c.FrontierFor(7); ok {
		t.Fatal("planning changed the received frontier")
	}
	c.MarkApplied(7, 500003, crdt.Clock{WallTime: 100})
	want := []crdt.SeqRange{{Lo: 500001, Hi: 500002}, {Lo: 500004, Hi: 500005}}
	if got := c.MissingRangesUpTo(7, 500005); !reflect.DeepEqual(got, want) {
		t.Fatalf("missing ranges %+v, want %+v", got, want)
	}
	if got := c.AppliedTipMap()[7]; got != 500003 {
		t.Fatalf("local production hid received tip: %d", got)
	}
	c.MarkApplied(9, 5, crdt.Clock{WallTime: 100})
	if got := c.MissingRangesUpTo(9, 5); !reflect.DeepEqual(got, []crdt.SeqRange{{Lo: 1, Hi: 4}}) {
		t.Fatalf("remote hole skipped: %+v", got)
	}
}

func TestRetiredLocalProductionStillRecoversJournalClocks(t *testing.T) {
	t.Parallel()
	const old crdt.Origin = 7
	c := New(2)
	c.ObserveSelfSeq(old, 5)
	cs, err := crdt.Build(crdt.Dot{Origin: old, Seq: 5},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: old}, nil, crdt.ClusterID{1},
		[]crdt.Record{crdt.Delete{Table: crdt.TableID{1}, PK: crdt.PKBlob{1}, CL: 2}})
	if err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(t.TempDir(), 65536, journal.SyncOff)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, _, err := j.Append(journal.KindMirror, 0, uint64(old), cs.Encoded()); err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverMirror(c, &fakeMirrorSource{self: 2, orig: old, j: j}, nil); err != nil {
		t.Fatal(err)
	}
	if got := c.RowState(crdt.TableID{1}, crdt.PKBlob{1}); got.CL != 2 || got.Base != cs.Stamp {
		t.Fatalf("local coverage bypassed clock recovery: %+v", got)
	}
	if !c.IsAppliedRemote(old, 5) {
		t.Fatal("journal record was not recovered")
	}
}

func TestRetiredLocalCoverageSurvivesMetadataReload(t *testing.T) {
	t.Parallel()
	meta := newMeta(t)
	old := New(7)
	old.ObserveSelfSeq(7, 500000)
	old.MarkApplied(9, 3, crdt.Clock{WallTime: 100})
	if err := NewSnapshotter(old, meta, SnapshotterConfig{}).SnapshotOnce(); err != nil {
		t.Fatal(err)
	}
	if err := meta.SetNodeID(2); err != nil {
		t.Fatal(err)
	}
	restarted := New(2)
	if err := restarted.LoadFromMeta(meta); err != nil {
		t.Fatal(err)
	}
	if got := restarted.MissingRangesUpTo(7, 500002); !reflect.DeepEqual(got, []crdt.SeqRange{{Lo: 500001, Hi: 500002}}) {
		t.Fatalf("lost retired local coverage: %+v", got)
	}
	if got := restarted.MissingRangesUpTo(9, 3); !reflect.DeepEqual(got, []crdt.SeqRange{{Lo: 1, Hi: 2}}) {
		t.Fatalf("lost remote holes: %+v", got)
	}
	if _, ok := restarted.FrontierFor(7); ok {
		t.Fatal("metadata reload converted local coverage into remote receipts")
	}
	if got := restarted.AppliedTipMap()[7]; got != 500000 {
		t.Fatalf("lost retired producer tip: %d", got)
	}
}
