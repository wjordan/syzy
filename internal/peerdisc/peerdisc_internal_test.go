package peerdisc

import (
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
)

// Invariant: with two heartbeats at the same listen address and
// equal LastModified, the most-recently-observed origin must win
// deterministically — independent of Go's map iteration order.
func TestBindingsLockedSeenSeqTiebreak(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := &Discoverer{
		cfg: Config{
			Origin:   crdt.Origin(0xAA),
			Interval: 10 * time.Second,
			Now:      func() time.Time { return now },
		},
		cache: map[string]cachedEntry{},
	}
	// Two origins claim the same listen address (unclean-restart
	// rotation window) with identical fresh LastModified.
	stamp := now.Add(-1 * time.Second)
	d.seenSeq++
	d.cache["peers/"+layout.OriginHex(0xB1)+".json"] = cachedEntry{
		hb:           Heartbeat{Origin: layout.OriginHex(0xB1), Listen: "10.0.0.5:7000"},
		lastModified: stamp,
		seenSeq:      d.seenSeq,
	}
	d.seenSeq++
	d.cache["peers/"+layout.OriginHex(0xB2)+".json"] = cachedEntry{
		hb:           Heartbeat{Origin: layout.OriginHex(0xB2), Listen: "10.0.0.5:7000"},
		lastModified: stamp,
		seenSeq:      d.seenSeq,
	}

	for i := 0; i < 20; i++ {
		b := d.bindingsLocked()
		got, ok := b["10.0.0.5:7000"]
		if !ok {
			t.Fatalf("call %d: no binding for 10.0.0.5:7000", i)
		}
		if got != crdt.Origin(0xB2) {
			t.Fatalf("call %d: binding for 10.0.0.5:7000 = %x, want B2 (most-recently-observed)", i, got)
		}
	}

	// Re-observation of B1 advances its seenSeq past B2's;
	// the tiebreak follows.
	d.seenSeq++
	prev := d.cache["peers/"+layout.OriginHex(0xB1)+".json"]
	prev.seenSeq = d.seenSeq
	d.cache["peers/"+layout.OriginHex(0xB1)+".json"] = prev
	for i := 0; i < 20; i++ {
		b := d.bindingsLocked()
		got := b["10.0.0.5:7000"]
		if got != crdt.Origin(0xB1) {
			t.Fatalf("after B1 refresh, call %d: binding = %x, want B1", i, got)
		}
	}
}

// Same invariant for backends that don't populate LastModified at
// all — both entries tie on time.Time{} so the tiebreak must rely
// on observation order, not the timestamp.
func TestBindingsLockedZeroLastModifiedTiebreak(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := &Discoverer{
		cfg: Config{
			Origin:   crdt.Origin(0xAA),
			Interval: 10 * time.Second,
			Now:      func() time.Time { return now },
		},
		cache: map[string]cachedEntry{},
	}
	d.seenSeq++
	d.cache["peers/"+layout.OriginHex(0xC1)+".json"] = cachedEntry{
		hb:      Heartbeat{Origin: layout.OriginHex(0xC1), Listen: "10.0.0.6:7000"},
		seenSeq: d.seenSeq,
	}
	d.seenSeq++
	d.cache["peers/"+layout.OriginHex(0xC2)+".json"] = cachedEntry{
		hb:      Heartbeat{Origin: layout.OriginHex(0xC2), Listen: "10.0.0.6:7000"},
		seenSeq: d.seenSeq,
	}
	for i := 0; i < 20; i++ {
		got := d.bindingsLocked()["10.0.0.6:7000"]
		if got != crdt.Origin(0xC2) {
			t.Fatalf("call %d: zero-LastModified tiebreak picked %x, want C2", i, got)
		}
	}
}
