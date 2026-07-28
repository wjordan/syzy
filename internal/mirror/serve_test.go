package mirror_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/transport"
)

// payload synthesizes a wire-format Changeset prefix carrying
// (origin, seq). The mirror.Serve path only inspects bytes [1, 17);
// the rest is opaque to it, so a trailing tag suffices to make the
// payloads distinguishable.
func payload(origin crdt.Origin, seq crdt.Seq, tag byte) []byte {
	b := make([]byte, 18)
	b[0] = 1 // wire version
	binary.BigEndian.PutUint64(b[1:9], uint64(origin))
	binary.BigEndian.PutUint64(b[9:17], uint64(seq))
	b[17] = tag
	return b
}

// drainMirror busy-waits for the per-origin writer goroutine to flush
// every queued payload into its journal. mgr.Append is async (chan to
// per-origin writer goroutine); Iterate from offset 0 is the
// authoritative "did it land yet" check.
func drainMirror(t *testing.T, m *mirror.Manager, origin crdt.Origin, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if j, ok := m.LookupJournal(origin); ok {
			it := j.Iterate(0)
			count := 0
			for {
				rec, _, err := it.Next()
				if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
					break
				}
				if err != nil {
					t.Fatalf("Iterate: %v", err)
				}
				if rec.Kind == journal.KindMirror {
					count++
				}
			}
			if count >= want {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("mirror writer never drained %d records for origin %d", want, origin)
}

func newManager(t *testing.T) *mirror.Manager {
	t.Helper()
	mgr, err := mirror.New(mirror.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}

func TestServe_ReturnsOnlyMatchingRange(t *testing.T) {
	mgr := newManager(t)
	for i := crdt.Seq(1); i <= 5; i++ {
		if err := mgr.Append(7, payload(7, i, byte(i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	drainMirror(t, mgr, 7, 5)

	ctx := context.Background()
	var got []crdt.Seq
	err := mgr.Serve(ctx, transport.CatchupRequest{
		Ranges: []transport.Range{{Origin: 7, Lo: 2, Hi: 4}},
	}, func(p []byte) error {
		got = append(got, crdt.Seq(binary.BigEndian.Uint64(p[9:17])))
		return nil
	})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	want := []crdt.Seq{2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("got seqs %v, want %v", got, want)
	}
	for i, s := range got {
		if s != want[i] {
			t.Fatalf("got[%d]=%d, want %d", i, s, want[i])
		}
	}
}

func TestServe_HonorsMaxRecords(t *testing.T) {
	mgr := newManager(t)
	for i := crdt.Seq(1); i <= 10; i++ {
		if err := mgr.Append(7, payload(7, i, 0)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	drainMirror(t, mgr, 7, 10)
	got := 0
	err := mgr.Serve(context.Background(), transport.CatchupRequest{
		Ranges:     []transport.Range{{Origin: 7, Lo: 1, Hi: 10}},
		MaxRecords: 4,
	}, func([]byte) error { got++; return nil })
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if got != 4 {
		t.Fatalf("MaxRecords=4: served %d, want 4", got)
	}
}

func TestServe_UnknownOriginIsNoop(t *testing.T) {
	mgr := newManager(t)
	called := 0
	err := mgr.Serve(context.Background(), transport.CatchupRequest{
		Ranges: []transport.Range{{Origin: 99, Lo: 1, Hi: 5}},
	}, func([]byte) error { called++; return nil })
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if called != 0 {
		t.Fatalf("Serve called write for unknown origin")
	}
	// Unknown origin must NOT auto-create a journal directory.
	if _, ok := mgr.LookupJournal(99); ok {
		t.Fatalf("Serve created a journal handle for unknown origin")
	}
}

func TestServe_OpenEndedRangeStreamsAll(t *testing.T) {
	mgr := newManager(t)
	for i := crdt.Seq(5); i <= 8; i++ {
		if err := mgr.Append(7, payload(7, i, 0)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	drainMirror(t, mgr, 7, 4)
	var seqs []crdt.Seq
	err := mgr.Serve(context.Background(), transport.CatchupRequest{
		Ranges: []transport.Range{{Origin: 7, Lo: 6, Hi: 0}},
	}, func(p []byte) error {
		seqs = append(seqs, crdt.Seq(binary.BigEndian.Uint64(p[9:17])))
		return nil
	})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	want := []crdt.Seq{6, 7, 8}
	if len(seqs) != len(want) {
		t.Fatalf("got %v, want %v", seqs, want)
	}
}

func TestServe_OutOfOrderAppendsAreFound(t *testing.T) {
	// Broker.apply ingests records in causal-arrival order, not seq
	// order. The mirror journal therefore stores seqs unsorted. A
	// request for [1,1] must keep scanning past a seq=2 record at the
	// head to find seq=1 later in the journal.
	mgr := newManager(t)
	for _, s := range []crdt.Seq{2, 1, 3} {
		if err := mgr.Append(7, payload(7, s, byte(s))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	drainMirror(t, mgr, 7, 3)

	var got []crdt.Seq
	err := mgr.Serve(context.Background(), transport.CatchupRequest{
		Ranges: []transport.Range{{Origin: 7, Lo: 1, Hi: 1}},
	}, func(p []byte) error {
		got = append(got, crdt.Seq(binary.BigEndian.Uint64(p[9:17])))
		return nil
	})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("got %v, want [1]", got)
	}
}

// minSegmentSize is the journal's smallest accepted segment size; the
// seek tests use it so a modest record count still spans many segments.
const minSegmentSize = 1088

func TestServe_SkipsSegmentsBelowLo(t *testing.T) {
	// At the minimum segment size, 200 in-order records spread across
	// many segments. A request for Lo=190 must seek past the early
	// segments rather than scan from offset 0.
	mgr, err := mirror.New(mirror.Config{Root: t.TempDir(), SegmentSize: minSegmentSize})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	const n = 200
	for i := crdt.Seq(1); i <= n; i++ {
		if err := mgr.Append(7, payload(7, i, byte(i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	drainMirror(t, mgr, 7, n)

	var got []crdt.Seq
	err = mgr.Serve(context.Background(), transport.CatchupRequest{
		Ranges: []transport.Range{{Origin: 7, Lo: 190, Hi: 0}},
	}, func(p []byte) error {
		got = append(got, crdt.Seq(binary.BigEndian.Uint64(p[9:17])))
		return nil
	})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}

	// Correctness: exactly the tail records 190..200, in order.
	var want []crdt.Seq
	for i := crdt.Seq(190); i <= n; i++ {
		want = append(want, i)
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, s := range got {
		if s != want[i] {
			t.Fatalf("got[%d]=%d, want %d", i, s, want[i])
		}
	}

	// Skip actually happened: many segments formed, most were bypassed,
	// and the scan touched far fewer than all 200 records.
	st := mgr.LastServeStats()
	if st.SegmentsTotal <= 1 {
		t.Fatalf("test did not produce multiple segments (total=%d); raise the record count", st.SegmentsTotal)
	}
	if st.SegmentsSkipped == 0 {
		t.Fatalf("no segments skipped (total=%d): seek index not consulted", st.SegmentsTotal)
	}
	if st.RecordsScanned >= n {
		t.Fatalf("scanned %d records — full scan, no skip", st.RecordsScanned)
	}
}

// TestServe_SkipIndexFindsLateStraggler guards the correctness boundary:
// a record whose seq is in range but which arrived late (so it sits in a
// higher segment than seq order would place it) must still be served —
// the seek may skip segments below Lo but never one that holds a wanted
// seq.
func TestServe_SkipIndexFindsLateStraggler(t *testing.T) {
	mgr, err := mirror.New(mirror.Config{Root: t.TempDir(), SegmentSize: minSegmentSize})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	// Append 1..100 in order (spanning many segments), then a late seq=40
	// that lands in the newest (highest) segment, far from where seq
	// order would place it.
	for i := crdt.Seq(1); i <= 100; i++ {
		if err := mgr.Append(7, payload(7, i, byte(i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := mgr.Append(7, payload(7, 40, 0xFF)); err != nil {
		t.Fatalf("Append straggler: %v", err)
	}
	drainMirror(t, mgr, 7, 101)

	var got []crdt.Seq
	err = mgr.Serve(context.Background(), transport.CatchupRequest{
		Ranges: []transport.Range{{Origin: 7, Lo: 40, Hi: 40}},
	}, func(p []byte) error {
		got = append(got, crdt.Seq(binary.BigEndian.Uint64(p[9:17])))
		return nil
	})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	// Both the in-order seq=40 and the late duplicate must come back.
	if len(got) != 2 {
		t.Fatalf("got %v, want two seq=40 records", got)
	}
	for _, s := range got {
		if s != 40 {
			t.Fatalf("got seq %d, want 40", s)
		}
	}
}

func TestServe_PerOriginIsolation(t *testing.T) {
	mgr := newManager(t)
	if err := mgr.Append(7, payload(7, 1, 0)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := mgr.Append(8, payload(8, 1, 0)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	drainMirror(t, mgr, 7, 1)
	drainMirror(t, mgr, 8, 1)
	var got7, got8 int
	err := mgr.Serve(context.Background(), transport.CatchupRequest{
		Ranges: []transport.Range{{Origin: 7, Lo: 1, Hi: 1}},
	}, func(p []byte) error {
		switch crdt.Origin(binary.BigEndian.Uint64(p[1:9])) {
		case 7:
			got7++
		case 8:
			got8++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if got7 != 1 || got8 != 0 {
		t.Fatalf("isolation: got7=%d got8=%d", got7, got8)
	}
}
