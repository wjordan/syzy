package nodestate

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

func newMeta(t *testing.T) *metadata.Store {
	t.Helper()
	dir := t.TempDir()
	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	if err := sc.SetClusterID(crdt.ClusterID{1}); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(7); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}
	return sc
}

// TestCacheBasicHLCAndSeq verifies StampHLC monotonicity and self-seq
// allocation under typical commit ordering.
func TestCacheBasicHLCAndSeq(t *testing.T) {
	c := New(7)

	// Even if wall clock retreats, HLC must be strictly monotonic.
	c1 := c.StampHLC(100)
	c2 := c.StampHLC(50)
	c3 := c.StampHLC(101)
	if !c1.Less(c2) || !c2.Less(c3) {
		t.Fatalf("StampHLC not monotonic: c1=%v c2=%v c3=%v", c1, c2, c3)
	}
	if c.HLCLast() != c3 {
		t.Fatalf("HLCLast = %v; want %v", c.HLCLast(), c3)
	}

	// Self-seq starts at 1 and walks forward.
	for i := crdt.Seq(1); i <= 5; i++ {
		got := c.AllocSelfSeq(c.Self())
		if got != i {
			t.Fatalf("AllocSelfSeq #%d = %d; want %d", i, got, i)
		}
	}
}

// TestCacheRemoteApplyAndFrontier checks idempotency, applied_gaps
// promotion, and frontier advancement when applies arrive out of order.
func TestCacheRemoteApplyAndFrontier(t *testing.T) {
	c := New(7)

	apply := func(origin crdt.Origin, seq crdt.Seq, hlcWall int64) {
		c.MarkApplied(origin, seq, crdt.Clock{WallTime: hlcWall})
	}

	// Out of order: 1, 3, 2, 4
	apply(11, 1, 100)
	if f, ok := c.FrontierFor(11); !ok || f.LastSeq != 1 {
		t.Fatalf("frontier after seq=1: %v ok=%v", f, ok)
	}
	apply(11, 3, 102)
	if f, _ := c.FrontierFor(11); f.LastSeq != 1 {
		t.Fatalf("frontier should still be 1 (gap at 2): got %d", f.LastSeq)
	}
	if !c.IsAppliedRemote(11, 3) {
		t.Fatal("seq=3 should be in applied_gaps")
	}
	if c.IsAppliedRemote(11, 2) {
		t.Fatal("seq=2 should NOT be applied yet")
	}
	apply(11, 2, 101)
	if f, _ := c.FrontierFor(11); f.LastSeq != 3 {
		t.Fatalf("frontier should promote to 3: got %d", f.LastSeq)
	}
	apply(11, 4, 103)
	if f, _ := c.FrontierFor(11); f.LastSeq != 4 {
		t.Fatalf("frontier should be 4: got %d", f.LastSeq)
	}
	// Repeat applies — idempotent.
	if !c.IsAppliedRemote(11, 1) || !c.IsAppliedRemote(11, 4) {
		t.Fatal("applied seqs should be reported as applied")
	}
	if c.IsAppliedRemote(7, 1) {
		t.Fatal("self origin must not be reported as remote-applied")
	}
}

// TestRetentionFrontierIgnoresGaps is the Bug A regression: when applies
// arrive out of order (gap at 2, seq 3 landed), the applied tip advances to
// 3 but the contiguous frontier stays at 1. Object-store retention must key
// on the contiguous frontier, never the tip — reclaiming an epoch covering
// [2,3] would strand the range the unfilled prefix (seq 2) still needs.
func TestRetentionFrontierIgnoresGaps(t *testing.T) {
	c := New(7)

	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})
	c.MarkApplied(11, 3, crdt.Clock{WallTime: 102}) // gap at 2

	if got := c.AppliedTipMap()[11]; got != 3 {
		t.Fatalf("AppliedTipMap[11] = %d; want 3 (tip advances over the gap)", got)
	}
	if got := c.RetentionFrontierMap()[11]; got != 1 {
		t.Fatalf("RetentionFrontierMap[11] = %d; want 1 (contiguous head, NOT the tip) — retention would strand [2,3]", got)
	}

	// Filling the gap promotes the contiguous frontier; retention may advance.
	c.MarkApplied(11, 2, crdt.Clock{WallTime: 101})
	if got := c.RetentionFrontierMap()[11]; got != 3 {
		t.Fatalf("RetentionFrontierMap[11] = %d after gap fill; want 3", got)
	}

	// Locally-produced origins (self + drained secondaries, in senderNextSeq)
	// are EXCLUDED from the retention map so their epochs are never swept by
	// local retention — keying them on production would strand peers holding
	// a lower contiguous prefix (the owner-origin wedge). This holds even in
	// the pathological case where such an origin also sits in the frontier
	// (stale self-origin metadata, as observed in prod).
	_ = c.AllocSelfSeq(c.Self()) // senderNextSeq[7] -> 2
	if _, ok := c.RetentionFrontierMap()[c.Self()]; ok {
		t.Fatal("RetentionFrontierMap must exclude the self origin (owner-origin not locally sweepable)")
	}
	// A produced origin that is ALSO in the frontier is still excluded.
	const drained crdt.Origin = 11 // origin 11 was MarkApplied above (frontier=3)
	_ = c.AllocSelfSeq(drained)    // now also in senderNextSeq
	if _, ok := c.RetentionFrontierMap()[drained]; ok {
		t.Fatal("RetentionFrontierMap must exclude a produced origin even when present in the frontier")
	}
}

// TestCacheSnapshotRoundTrip exercises snapshot + load: build state,
// snapshot to metadata, construct a fresh Cache, LoadFromMeta, verify
// state matches.
func TestCacheSnapshotRoundTrip(t *testing.T) {
	sc := newMeta(t)

	c := New(7)
	// Self-side activity.
	_ = c.StampHLC(1000)
	_ = c.AllocSelfSeq(c.Self()) // 1
	_ = c.AllocSelfSeq(c.Self()) // 2

	// Remote applies producing gaps and rowClock entries.
	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})
	c.MarkApplied(11, 3, crdt.Clock{WallTime: 102})
	c.MarkApplied(22, 5, crdt.Clock{WallTime: 200})
	c.PutRowState(crdt.TableID{1}, crdt.PKBlob{0xaa}, crdt.RowState{
		CL:   3,
		Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 102}, Origin: 11},
	})
	c.PutRowState(crdt.TableID{2}, crdt.PKBlob{0xbb, 0xcc}, crdt.RowState{
		CL:   2,
		Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 22},
	})

	snapper := NewSnapshotter(c, sc, SnapshotterConfig{})
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce: %v", err)
	}

	// After ClearDirty, a no-op snapshot run shouldn't write anything;
	// SnapshotOnce returns nil cleanly with empty work.
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce#2: %v", err)
	}

	// Load into a fresh cache and compare.
	c2 := New(7)
	if err := c2.LoadFromMeta(sc); err != nil {
		t.Fatalf("LoadFromMeta: %v", err)
	}
	if got, want := c2.SenderNextSeq(c.Self()), c.SenderNextSeq(c.Self()); got != want {
		t.Errorf("senderNextSeq: got %d want %d", got, want)
	}
	if got, want := c2.HLCLast(), c.HLCLast(); got != want {
		t.Errorf("hlcLast: got %v want %v", got, want)
	}
	if !c2.IsAppliedRemote(11, 1) || !c2.IsAppliedRemote(11, 3) || !c2.IsAppliedRemote(22, 5) {
		t.Errorf("loaded cache missing applied seqs")
	}
	if c2.IsAppliedRemote(11, 2) {
		t.Errorf("loaded cache should not report 11/2 as applied")
	}
	if got, ok := c2.FrontierFor(11); !ok || got.LastSeq != 1 {
		t.Errorf("frontier(11) = %v ok=%v; want LastSeq=1 (gap at 2 keeps it there)", got, ok)
	}
	if got := c2.RowState(crdt.TableID{1}, crdt.PKBlob{0xaa}); got.CL != 3 || got.Base.Origin != 11 {
		t.Errorf("rowState(1,aa) = %+v; want CL=3 origin=11", got)
	}
}

// TestCacheEvictOrigin covers the in-memory mechanics of origin GC: self and
// unknown origins are no-ops, a real origin drops out of the frontier and is
// queued for frontier-row deletion, and its seqs stop reporting as applied.
func TestCacheEvictOrigin(t *testing.T) {
	c := New(7)
	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})
	c.MarkApplied(11, 2, crdt.Clock{WallTime: 101})
	c.MarkApplied(22, 5, crdt.Clock{WallTime: 200})
	if c.FrontierLen() != 2 {
		t.Fatalf("FrontierLen = %d, want 2", c.FrontierLen())
	}
	if c.EvictOrigin(7) {
		t.Error("EvictOrigin(self) must be a no-op")
	}
	if c.EvictOrigin(999) {
		t.Error("EvictOrigin(untracked) must be a no-op")
	}
	if !c.EvictOrigin(11) {
		t.Fatal("EvictOrigin(11) should report an eviction")
	}
	if c.FrontierLen() != 1 {
		t.Errorf("FrontierLen after evict = %d, want 1", c.FrontierLen())
	}
	if _, ok := c.FrontierFor(11); ok {
		t.Error("frontier(11) should be gone after eviction")
	}
	if c.IsAppliedRemote(11, 1) {
		t.Error("evicted origin must not report applied (it is forgotten)")
	}
	if !c.IsAppliedRemote(22, 5) {
		t.Error("origin 22 must be untouched by evicting 11")
	}
	snap := c.SnapshotIncremental()
	if len(snap.Forgotten) != 1 || snap.Forgotten[0] != 11 {
		t.Errorf("snap.Forgotten = %v, want [11]", snap.Forgotten)
	}
	if _, inFront := snap.Frontier[11]; inFront {
		t.Error("evicted origin must not also appear in snap.Frontier")
	}
}

// TestCacheEvictOriginReadmit: a straggler apply from an evicted origin
// re-admits it and cancels the pending frontier-row delete, so the snapshot
// never both advances and deletes the same origin.
func TestCacheEvictOriginReadmit(t *testing.T) {
	c := New(7)
	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})
	if !c.EvictOrigin(11) {
		t.Fatal("EvictOrigin(11)")
	}
	c.MarkApplied(11, 2, crdt.Clock{WallTime: 102}) // straggler re-admits
	snap := c.SnapshotIncremental()
	if len(snap.Forgotten) != 0 {
		t.Errorf("re-admitted origin must not be in Forgotten: %v", snap.Forgotten)
	}
	if _, ok := snap.Frontier[11]; !ok {
		t.Error("re-admitted origin must be back in snap.Frontier")
	}
}

// TestCacheEvictOriginRoundTrip is the durability guarantee: an eviction must
// survive restart. Without the DeleteFrontier persist, AdvanceFrontier's
// upsert would leave a stale row that reloads on the next boot, re-inflating
// the frontier and undoing the GC.
func TestCacheEvictOriginRoundTrip(t *testing.T) {
	sc := newMeta(t)
	c := New(7)
	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})
	c.MarkApplied(22, 2, crdt.Clock{WallTime: 200})
	c.MarkApplied(33, 3, crdt.Clock{WallTime: 300})
	snapper := NewSnapshotter(c, sc, SnapshotterConfig{})
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce: %v", err)
	}
	if !c.EvictOrigin(22) {
		t.Fatal("EvictOrigin(22)")
	}
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce after evict: %v", err)
	}
	c2 := New(7)
	if err := c2.LoadFromMeta(sc); err != nil {
		t.Fatalf("LoadFromMeta: %v", err)
	}
	if _, ok := c2.FrontierFor(22); ok {
		t.Error("evicted origin 22 must not reload from metadata")
	}
	if _, ok := c2.FrontierFor(11); !ok {
		t.Error("origin 11 should survive the eviction of 22")
	}
	if _, ok := c2.FrontierFor(33); !ok {
		t.Error("origin 33 should survive the eviction of 22")
	}
	if c2.FrontierLen() != 2 {
		t.Errorf("reloaded FrontierLen = %d, want 2", c2.FrontierLen())
	}
}

func TestSnapshotHasWorkClearsAfterCleanMetadata(t *testing.T) {
	sc := newMeta(t)
	c := New(7)

	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})
	c.SetSnapshotMarker(11, 123)
	snapper := NewSnapshotter(c, sc, SnapshotterConfig{})

	if snap := c.SnapshotIncremental(); !snapshotHasWork(snap) {
		t.Fatalf("dirty metadata snapshot reported no work: %+v", snap)
	}
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce: %v", err)
	}
	if snap := c.SnapshotIncremental(); snapshotHasWork(snap) {
		t.Fatalf("clean metadata snapshot still reports work: %+v", snap)
	}

	tab := crdt.TableID{1}
	pk := crdt.PKBlob{0x01}
	c.PutRowState(tab, pk, crdt.RowState{CL: 1, Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 7}})
	if snap := c.SnapshotIncremental(); !snapshotHasWork(snap) || snap.MetaDirty || len(snap.ClearedRows) != 0 {
		t.Fatalf("row-only dirty snapshot = %+v; want work without MetaDirty", snap)
	}
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce row-only: %v", err)
	}
	if snap := c.SnapshotIncremental(); snapshotHasWork(snap) {
		t.Fatalf("clean row-only snapshot still reports work: %+v", snap)
	}
}

func TestClearSnapshotDirtyPreservesConcurrentDirtyState(t *testing.T) {
	c := New(7)
	table := crdt.TableID{1}
	pk1 := crdt.PKBlob{0x01}
	pk2 := crdt.PKBlob{0x02}

	st1 := crdt.RowState{
		CL:   1,
		Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: 7},
	}
	c.PutRowState(table, pk1, st1)
	if got := c.AllocSelfSeq(7); got != 1 {
		t.Fatalf("AllocSelfSeq = %d; want 1", got)
	}
	c.MarkApplied(11, 1, crdt.Clock{WallTime: 50})

	// Simulate Snapshotter.SnapshotOnce after it has captured the dirty
	// set but before the metadata transaction commits.
	snap := c.SnapshotIncremental()

	st1Later := crdt.RowState{
		CL:   3,
		Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 7},
	}
	st2 := crdt.RowState{
		CL:   1,
		Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 201}, Origin: 7},
	}
	c.PutRowState(table, pk1, st1Later)
	c.PutRowState(table, pk2, st2)
	if got := c.AllocSelfSeq(7); got != 2 {
		t.Fatalf("AllocSelfSeq after snapshot capture = %d; want 2", got)
	}
	c.MarkApplied(11, 2, crdt.Clock{WallTime: 60})

	c.ClearSnapshotDirty(snap)
	next := c.SnapshotIncremental()

	if got, ok := snapshotRow(next, table, pk1); !ok || got.CL != st1Later.CL || got.Base != st1Later.Base {
		t.Fatalf("late update row = %+v ok=%v; want %+v", got, ok, st1Later)
	}
	if got, ok := snapshotRow(next, table, pk2); !ok || got.CL != st2.CL || got.Base != st2.Base {
		t.Fatalf("late insert row = %+v ok=%v; want %+v", got, ok, st2)
	}
	if got := next.SenderNextSeq[7]; got != 3 {
		t.Fatalf("late sender seq snapshot = %d; want 3", got)
	}
	if f := next.Frontier[11]; f.LastSeq != 2 {
		t.Fatalf("late frontier snapshot = %+v; want LastSeq=2", f)
	}

	c.ClearSnapshotDirty(next)
	final := c.SnapshotIncremental()
	if _, ok := snapshotRow(final, table, pk1); ok {
		t.Fatalf("covered pk1 remained dirty after matching clear")
	}
	if _, ok := snapshotRow(final, table, pk2); ok {
		t.Fatalf("covered pk2 remained dirty after matching clear")
	}
	if _, ok := final.SenderNextSeq[7]; ok {
		t.Fatalf("covered sender seq remained dirty after matching clear")
	}
}

func snapshotRow(s Snapshot, table crdt.TableID, pk crdt.PKBlob) (RowEntry, bool) {
	for _, r := range s.Rows {
		if r.Table == table && bytes.Equal(r.PK, pk) {
			return r, true
		}
	}
	return RowEntry{}, false
}

// TestCacheCellsBasic round-trips a few cell-clock writes through the
// cache and verifies CellStamp lookup and the surfaced dirty set.
func TestCacheCellsBasic(t *testing.T) {
	c := New(7)
	tab := crdt.TableID{1}
	pk := crdt.PKBlob{0x01}
	colA := crdt.ColumnID{0xC1}
	colB := crdt.ColumnID{0xC2}
	stampA := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: 7}
	stampB := crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 7}

	c.PutCellStamp(tab, pk, colA, stampA)
	c.PutCellStamps(tab, pk, map[crdt.ColumnID]crdt.Stamp{colB: stampB})

	if got, ok := c.CellStamp(tab, pk, colA); !ok || got != stampA {
		t.Errorf("CellStamp colA = (%v, %v); want (%v, true)", got, ok, stampA)
	}
	if got, ok := c.CellStamp(tab, pk, colB); !ok || got != stampB {
		t.Errorf("CellStamp colB = (%v, %v); want (%v, true)", got, ok, stampB)
	}

	snap := c.SnapshotIncremental()
	if len(snap.Cells) != 2 {
		t.Errorf("snap.Cells len = %d; want 2", len(snap.Cells))
	}
	for _, ce := range snap.Cells {
		if !ce.Present {
			t.Errorf("cell %x not present in snapshot", ce.Column)
		}
	}

	// Delete one cell; surfaced as Present=false in the next snapshot.
	c.ClearSnapshotDirty(snap)
	c.DeleteCellStamp(tab, pk, colA)
	if _, ok := c.CellStamp(tab, pk, colA); ok {
		t.Errorf("colA still present after delete")
	}
	snap = c.SnapshotIncremental()
	if len(snap.Cells) != 1 || snap.Cells[0].Column != colA || snap.Cells[0].Present {
		t.Errorf("delete snapshot = %+v", snap.Cells)
	}
}

// TestCacheCLBumpClearsCells validates the CL-bump invariant: any
// PutRowState whose CL strictly exceeds the row's current CL drops
// every prior-generation cell_clock override (CRDT.md#causal-length-cl).
// Subsequent reload must observe an empty Cells map.
func TestCacheCLBumpClearsCells(t *testing.T) {
	sc := newMeta(t)
	c := New(7)
	tab := crdt.TableID{1}
	pk := crdt.PKBlob{0x01}
	col := crdt.ColumnID{0xC1}
	stampGen1 := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: 7}

	c.PutRowState(tab, pk, crdt.RowState{CL: 1, Base: stampGen1})
	c.PutCellStamp(tab, pk, col, crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 11})

	snapper := NewSnapshotter(c, sc, SnapshotterConfig{})
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("snap 1: %v", err)
	}

	// Bump CL to 3 (resurrection / new generation). Override is
	// implicitly tombstoned even if the caller accidentally supplies a
	// non-nil Cells map in the replacement RowState.
	c.PutRowState(tab, pk, crdt.RowState{
		CL:    3,
		Base:  crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: 7},
		Cells: map[crdt.ColumnID]crdt.Stamp{col: {Clock: crdt.Clock{WallTime: 300}, Origin: 11}},
	})
	if _, ok := c.CellStamp(tab, pk, col); ok {
		t.Errorf("cell override survived CL bump in cache")
	}
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("snap 2: %v", err)
	}

	c2 := New(7)
	if err := c2.LoadFromMeta(sc); err != nil {
		t.Fatalf("LoadFromMeta: %v", err)
	}
	if _, ok := c2.CellStamp(tab, pk, col); ok {
		t.Errorf("cell override survived CL bump after metadata reload")
	}
}

// TestCacheCellsRoundTripThroughMeta persists cells to the metadata
// and reloads into a fresh cache.
func TestCacheCellsRoundTripThroughMeta(t *testing.T) {
	sc := newMeta(t)
	c := New(7)
	tab := crdt.TableID{1}
	pk := crdt.PKBlob{0x01}
	colA := crdt.ColumnID{0xC1}
	colB := crdt.ColumnID{0xC2}
	stampA := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: 7}
	stampB := crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 11}

	c.PutRowState(tab, pk, crdt.RowState{
		CL:   1,
		Base: crdt.Stamp{Clock: crdt.Clock{WallTime: 50}, Origin: 7},
	})
	c.PutCellStamp(tab, pk, colA, stampA)
	c.PutCellStamp(tab, pk, colB, stampB)

	snapper := NewSnapshotter(c, sc, SnapshotterConfig{})
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce: %v", err)
	}

	c2 := New(7)
	if err := c2.LoadFromMeta(sc); err != nil {
		t.Fatalf("LoadFromMeta: %v", err)
	}
	if got, ok := c2.CellStamp(tab, pk, colA); !ok || got != stampA {
		t.Errorf("loaded colA = (%v, %v); want (%v, true)", got, ok, stampA)
	}
	if got, ok := c2.CellStamp(tab, pk, colB); !ok || got != stampB {
		t.Errorf("loaded colB = (%v, %v); want (%v, true)", got, ok, stampB)
	}

	// Delete colA, snapshot again, reload — colA must be gone.
	c.DeleteCellStamp(tab, pk, colA)
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce after delete: %v", err)
	}
	c3 := New(7)
	if err := c3.LoadFromMeta(sc); err != nil {
		t.Fatalf("LoadFromMeta #3: %v", err)
	}
	if _, ok := c3.CellStamp(tab, pk, colA); ok {
		t.Errorf("colA reappeared after delete + reload")
	}
	if got, ok := c3.CellStamp(tab, pk, colB); !ok || got != stampB {
		t.Errorf("colB lost after colA delete: (%v, %v)", got, ok)
	}

	// ClearCellsForRow drops everything; reload must show no cells.
	c.ClearCellsForRow(tab, pk)
	if err := snapper.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce after clear: %v", err)
	}
	c4 := New(7)
	if err := c4.LoadFromMeta(sc); err != nil {
		t.Fatalf("LoadFromMeta #4: %v", err)
	}
	if _, ok := c4.CellStamp(tab, pk, colB); ok {
		t.Errorf("colB present after ClearCellsForRow + reload")
	}
}

// TestCacheClearCellsForRow drops every cell entry for a row and emits
// a ClearedRow in the next snapshot. Subsequent puts ride on top.
func TestCacheClearCellsForRow(t *testing.T) {
	c := New(7)
	tab := crdt.TableID{1}
	pk := crdt.PKBlob{0x01}
	colA := crdt.ColumnID{0xC1}
	colB := crdt.ColumnID{0xC2}
	stampA := crdt.Stamp{Clock: crdt.Clock{WallTime: 100}, Origin: 7}
	stampB := crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: 7}

	c.PutCellStamp(tab, pk, colA, stampA)
	c.PutCellStamp(tab, pk, colB, stampB)
	c.ClearSnapshotDirty(c.SnapshotIncremental())

	c.ClearCellsForRow(tab, pk)
	if _, ok := c.CellStamp(tab, pk, colA); ok {
		t.Errorf("colA present after clear")
	}
	c.PutCellStamp(tab, pk, colA, stampA)

	snap := c.SnapshotIncremental()
	if len(snap.ClearedRows) != 1 {
		t.Errorf("ClearedRows = %+v; want 1", snap.ClearedRows)
	}
	if len(snap.Cells) != 1 || snap.Cells[0].Column != colA || !snap.Cells[0].Present {
		t.Errorf("snap.Cells after clear+put = %+v", snap.Cells)
	}
}
