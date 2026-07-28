package nodestate

import (
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// stubJournals records which origins gcSegments asked about. Returning
// (nil, nil) tells the snapshotter "no journal — skip", so the test
// avoids needing a real on-disk journal.
type stubJournals struct {
	seen []crdt.Origin
}

func (s *stubJournals) JournalFor(o crdt.Origin) (*journal.Journal, error) {
	s.seen = append(s.seen, o)
	return nil, nil
}

// stubSealer returns predetermined contiguous-sealed values per origin.
type stubSealer struct {
	uploaded map[crdt.Origin]uint64
}

func (s *stubSealer) ContiguousSealedSeq(o uint64) uint64 { return s.uploaded[crdt.Origin(o)] }

// TestSnapshotterGCDrainedSealerGated: drained origins (self plus any
// origin with a SenderNextSeq entry) are pruned only when the sealer
// has uploaded past our contiguous head. Mirror origins are always
// considered for pruning regardless of sealer state.
func TestSnapshotterGCDrainedSealerGated(t *testing.T) {
	sc := newMeta(t)
	c := New(7)

	// Self: 5 seqs allocated → SenderNextSeq=6, ourHead=5.
	for i := 0; i < 5; i++ {
		_ = c.AllocSelfSeq(c.Self())
	}
	// Origin 11: pure mirror (frontier head only).
	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})
	c.MarkApplied(11, 2, crdt.Clock{WallTime: 101})
	// Origin 22: pure mirror.
	c.MarkApplied(22, 1, crdt.Clock{WallTime: 200})

	c.SetSnapshotMarker(7, 999)
	c.SetSnapshotMarker(11, 888)
	c.SetSnapshotMarker(22, 777)

	jp := &stubJournals{}
	// Sealer ahead of self ⇒ self is GC-eligible.
	sl := &stubSealer{uploaded: map[crdt.Origin]uint64{7: 5}}
	snap := NewSnapshotter(c, sc, SnapshotterConfig{
		GC:       true,
		Journals: jp,
		Sealer:   sl,
		Self:     7,
	})
	if err := snap.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce: %v", err)
	}

	saw := map[crdt.Origin]bool{}
	for _, o := range jp.seen {
		saw[o] = true
	}
	if !saw[7] {
		t.Errorf("expected self (7) to be GC-eligible (sealer caught up); seen=%v", jp.seen)
	}
	if !saw[11] || !saw[22] {
		t.Errorf("expected mirror origins to always be GC-eligible; seen=%v", jp.seen)
	}
}

// TestSnapshotterEnableGC: the EnableGC setter (used by sqlite.Open once the
// sealer exists) turns on GC with the same gating as the config path, and
// carries the retention window through.
func TestSnapshotterEnableGC(t *testing.T) {
	sc := newMeta(t)
	c := New(7)
	for i := 0; i < 3; i++ {
		_ = c.AllocSelfSeq(c.Self()) // SenderNextSeq=4, ourHead=3
	}
	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})
	c.SetSnapshotMarker(7, 100)
	c.SetSnapshotMarker(11, 200)

	jp := &stubJournals{}
	sl := &stubSealer{uploaded: map[crdt.Origin]uint64{7: 3}} // caught up

	snap := NewSnapshotter(c, sc, SnapshotterConfig{Self: 7}) // GC off
	snap.EnableGC(jp, sl, 72*time.Hour)
	if !snap.gc || snap.retention != 72*time.Hour {
		t.Fatalf("EnableGC did not set gc/retention: gc=%v retention=%v", snap.gc, snap.retention)
	}
	if err := snap.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce: %v", err)
	}
	saw := map[crdt.Origin]bool{}
	for _, o := range jp.seen {
		saw[o] = true
	}
	if !saw[7] {
		t.Errorf("EnableGC should make self GC-eligible (sealer caught up); seen=%v", jp.seen)
	}
	if !saw[11] {
		t.Errorf("EnableGC should make mirror 11 GC-eligible; seen=%v", jp.seen)
	}
}

// TestSnapshotterGCDrainedBlockedBySealer: when the sealer hasn't yet
// uploaded past our self-head, self-origin GC is blocked. Mirror
// origins still proceed.
func TestSnapshotterGCDrainedBlockedBySealer(t *testing.T) {
	sc := newMeta(t)
	c := New(7)

	for i := 0; i < 5; i++ {
		_ = c.AllocSelfSeq(c.Self())
	}
	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})

	c.SetSnapshotMarker(7, 999)
	c.SetSnapshotMarker(11, 888)

	jp := &stubJournals{}
	// Sealer at 3 < ourHead 5: self blocked, 11 still allowed.
	sl := &stubSealer{uploaded: map[crdt.Origin]uint64{7: 3}}
	snap := NewSnapshotter(c, sc, SnapshotterConfig{
		GC:       true,
		Journals: jp,
		Sealer:   sl,
		Self:     7,
	})
	if err := snap.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce: %v", err)
	}

	saw := map[crdt.Origin]bool{}
	for _, o := range jp.seen {
		saw[o] = true
	}
	if saw[7] {
		t.Errorf("expected self GC blocked by sealer lag; seen=%v", jp.seen)
	}
	if !saw[11] {
		t.Errorf("expected mirror 11 to still be GC-eligible; seen=%v", jp.seen)
	}
}

// TestSnapshotterGCNoSealerSkipsDrained: with no Sealer wired, drained
// origins are never pruned (we don't know if they're durable). Mirror
// origins are still pruned marker-only.
func TestSnapshotterGCNoSealerSkipsDrained(t *testing.T) {
	sc := newMeta(t)
	c := New(7)

	for i := 0; i < 3; i++ {
		_ = c.AllocSelfSeq(c.Self())
	}
	c.MarkApplied(11, 1, crdt.Clock{WallTime: 100})
	c.SetSnapshotMarker(7, 100)
	c.SetSnapshotMarker(11, 200)

	jp := &stubJournals{}
	snap := NewSnapshotter(c, sc, SnapshotterConfig{
		GC:       true,
		Journals: jp,
		Self:     7,
	})

	if err := snap.SnapshotOnce(); err != nil {
		t.Fatalf("SnapshotOnce: %v", err)
	}

	saw := map[crdt.Origin]bool{}
	for _, o := range jp.seen {
		saw[o] = true
	}
	if saw[7] {
		t.Errorf("expected self skipped (no sealer); seen=%v", jp.seen)
	}
	if !saw[11] {
		t.Errorf("expected mirror 11 still GC'd marker-only; seen=%v", jp.seen)
	}
}
