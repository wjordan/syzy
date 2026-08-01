package postgres

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/transport"
)

// synthChangeset builds an empty-records Changeset under the given (origin,
// seq, clusterID), returning its canonical wire bytes. The catchup serve
// path inspects only the wire prefix (version, origin, seq), so an empty
// record set is enough to exercise the routing logic.
func synthChangeset(t *testing.T, origin crdt.Origin, seq crdt.Seq) []byte {
	t.Helper()
	dot := crdt.Dot{Origin: origin, Seq: seq}
	stamp := crdt.Stamp{Origin: origin, Clock: crdt.Clock{WallTime: 1}}
	cs, err := crdt.Build(dot, stamp, nil, crdt.ClusterID{}, nil)
	if err != nil {
		t.Fatalf("crdt.Build: %v", err)
	}
	return cs.Encoded()
}

func TestServeSelfLog_FiltersByRange(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	j, err := journal.Open(dir, 1<<16, journal.SyncOff)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer j.Close()

	const selfOrigin = crdt.Origin(7)
	// Seqs 1..5 of our own origin, in the format the self-log holds:
	// 8-byte LSN prefix + canonical changeset bytes.
	for seq := crdt.Seq(1); seq <= 5; seq++ {
		body := synthChangeset(t, selfOrigin, seq)
		payload := encodeSelfLogPayload(0, body)
		if _, _, err := j.Append(journal.KindLocalDML, 0, uint64(selfOrigin), payload); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	var got []crdt.Seq
	collect := func(p []byte) error {
		cs, err := crdt.Decode(p)
		if err != nil {
			return err
		}
		got = append(got, cs.Dot.Seq)
		return nil
	}
	ranges := []transport.Range{{Origin: selfOrigin, Lo: 2, Hi: 4}}
	if _, _, err := serveSelfLog(context.Background(), j, selfOrigin, ranges, 0, 0, collect); err != nil {
		t.Fatalf("serveSelfLog: %v", err)
	}
	want := []crdt.Seq{2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("got seqs %v, want %v", got, want)
	}
	for i, s := range want {
		if got[i] != s {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestServeSelfLog_SkipsForeignOrigin(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	j, err := journal.Open(dir, 1<<16, journal.SyncOff)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer j.Close()

	const selfOrigin = crdt.Origin(7)
	const peerOrigin = crdt.Origin(8)
	// One own-origin entry, one peer-origin entry mixed in (defensive case).
	for _, e := range []struct {
		o crdt.Origin
		s crdt.Seq
	}{{selfOrigin, 1}, {peerOrigin, 99}, {selfOrigin, 2}} {
		body := synthChangeset(t, e.o, e.s)
		payload := encodeSelfLogPayload(0, body)
		if _, _, err := j.Append(journal.KindLocalDML, 0, uint64(e.o), payload); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	var got []crdt.Seq
	ranges := []transport.Range{{Origin: selfOrigin, Lo: 1, Hi: 0}} // open-ended
	collect := func(p []byte) error {
		cs, err := crdt.Decode(p)
		if err != nil {
			return err
		}
		if cs.Dot.Origin != selfOrigin {
			t.Fatalf("served foreign origin %d via self-log", cs.Dot.Origin)
		}
		got = append(got, cs.Dot.Seq)
		return nil
	}
	if _, _, err := serveSelfLog(context.Background(), j, selfOrigin, ranges, 0, 0, collect); err != nil {
		t.Fatalf("serveSelfLog: %v", err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v, want [1 2]", got)
	}
}

func TestServeSelfLog_RespectsMaxRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	j, err := journal.Open(dir, 1<<16, journal.SyncOff)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer j.Close()

	const selfOrigin = crdt.Origin(7)
	for seq := crdt.Seq(1); seq <= 10; seq++ {
		body := synthChangeset(t, selfOrigin, seq)
		payload := encodeSelfLogPayload(0, body)
		if _, _, err := j.Append(journal.KindLocalDML, 0, uint64(selfOrigin), payload); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	count := 0
	collect := func(p []byte) error { count++; return nil }
	ranges := []transport.Range{{Origin: selfOrigin, Lo: 1, Hi: 0}}
	if _, _, err := serveSelfLog(context.Background(), j, selfOrigin, ranges, 3, 0, collect); err != nil {
		t.Fatalf("serveSelfLog: %v", err)
	}
	if count != 3 {
		t.Fatalf("MaxRecords=3 but served %d", count)
	}
}

func TestCatchupSource_RoutesPerOrigin(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	// Self-log holding own-origin (selfOrigin) entries.
	const selfOrigin = crdt.Origin(7)
	const peerOrigin = crdt.Origin(8)
	selfDir := filepath.Join(tmp, "self")
	selfLog, err := journal.Open(selfDir, 1<<16, journal.SyncOff)
	if err != nil {
		t.Fatalf("open self: %v", err)
	}
	defer selfLog.Close()
	for seq := crdt.Seq(1); seq <= 3; seq++ {
		body := synthChangeset(t, selfOrigin, seq)
		payload := encodeSelfLogPayload(0, body)
		if _, _, err := selfLog.Append(journal.KindLocalDML, 0, uint64(selfOrigin), payload); err != nil {
			t.Fatalf("self append: %v", err)
		}
	}

	// Mirror holding peer-origin entries.
	mirrorDir := filepath.Join(tmp, "mirror")
	mgr, err := mirror.New(mirror.Config{Root: mirrorDir})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	defer mgr.Close()
	for seq := crdt.Seq(1); seq <= 3; seq++ {
		body := synthChangeset(t, peerOrigin, seq)
		if err := mgr.Append(peerOrigin, body); err != nil {
			t.Fatalf("mirror append: %v", err)
		}
	}
	// mirror.Append is async — wait for the writer goroutine to drain.
	mustDrainMirror(t, mgr, peerOrigin, 3)

	src := &catchupSource{
		selfOrigin: selfOrigin,
		selfLog:    selfLog,
		mirror:     mgr.Serve,
	}

	type got struct {
		o crdt.Origin
		s crdt.Seq
	}
	var seen []got
	collect := func(p []byte) error {
		cs, err := crdt.Decode(p)
		if err != nil {
			return err
		}
		seen = append(seen, got{cs.Dot.Origin, cs.Dot.Seq})
		return nil
	}

	req := transport.CatchupRequest{Ranges: []transport.Range{
		{Origin: selfOrigin, Lo: 1, Hi: 0},
		{Origin: peerOrigin, Lo: 1, Hi: 0},
	}}
	if err := src.Serve(context.Background(), req, collect); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	// Expect 3 own + 3 peer, but order is per-source.
	ownCount, peerCount := 0, 0
	for _, g := range seen {
		switch g.o {
		case selfOrigin:
			ownCount++
		case peerOrigin:
			peerCount++
		default:
			t.Fatalf("unexpected origin %d", g.o)
		}
	}
	if ownCount != 3 || peerCount != 3 {
		t.Fatalf("got own=%d peer=%d, want 3/3 (entries=%v)", ownCount, peerCount, seen)
	}
}

// TestCatchupSourceNilMirrorNoPanic is the regression for the typed-nil trap:
// CatchupSource() with a self-log but no Mirror (a supported config) must
// produce a source whose foreign-origin path is a clean no-op, NOT a method
// value bound to a nil *mirror.Manager that panics on the first such request.
func TestCatchupSourceNilMirrorNoPanic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	j, err := journal.Open(dir, 1<<16, journal.SyncOff)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer j.Close()

	e := &Engine{selfLog: j, cfg: Config{Origin: 7}} // Mirror left nil
	src := e.CatchupSource()
	if src == nil {
		t.Fatal("CatchupSource returned nil despite a self-log being present")
	}
	// A foreign-origin range routes to the (absent) mirror; before the fix this
	// invoked (*mirror.Manager)(nil).Serve and panicked.
	req := transport.CatchupRequest{Ranges: []transport.Range{{Origin: 8, Lo: 1, Hi: 0}}}
	if err := src.Serve(context.Background(), req, func([]byte) error { return nil }); err != nil {
		t.Fatalf("Serve with nil mirror: %v", err)
	}
}

// mustDrainMirror waits until the per-origin mirror journal has at least
// minRecords entries, so a test that races a writer goroutine doesn't read
// an empty tail. Bounded retries — fail loudly rather than spin forever.
func mustDrainMirror(t *testing.T, mgr *mirror.Manager, origin crdt.Origin, minRecords int) {
	t.Helper()
	j, err := mgr.Journal(origin)
	if err != nil {
		t.Fatalf("mirror.Journal: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		it := j.Iterate(0)
		count := 0
		for {
			rec, _, err := it.Next()
			if err != nil {
				break
			}
			if rec.Kind == journal.KindMirror {
				count++
			}
		}
		if count >= minRecords {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("mirror did not drain %d records in 2s", minRecords)
}
