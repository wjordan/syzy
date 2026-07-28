package producer

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// captureRecorder is a fake syncer.SelfLog that records the interleaving of
// capture (AppendSelf/SyncSelf) against publish (an OnEncoded listener). It
// can force SyncSelf to fail, exercising the fatal-fsync path.
type captureRecorder struct {
	mu      sync.Mutex
	events  []string
	syncErr error
}

func (r *captureRecorder) AppendSelf(payload []byte, endOffset journal.Offset) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if endOffset == 0 {
		// Mirror the real invariant: a zero endOffset is never captured.
		return errors.New("captureRecorder: zero endOffset")
	}
	r.events = append(r.events, "append")
	return nil
}

func (r *captureRecorder) SyncSelf() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "sync")
	return r.syncErr
}

func (r *captureRecorder) publish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "publish")
}

func (r *captureRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

// TestSelfLog_CaptureBeforePublish is spec §3.2's crux: for a drained batch,
// every changeset is appended to the self-log and the batch is fsync'd
// (SyncSelf) BEFORE any encoded-listener (broadcast/sealer) fires. Nothing
// leaves the process before its bytes are durable.
func TestSelfLog_CaptureBeforePublish(t *testing.T) {
	rec := &captureRecorder{}
	f := setupTBWithConfig(t, Config{SelfLog: rec})
	f.p.OnEncoded(func([]byte) { rec.publish() })

	if err := f.app.Exec(`INSERT INTO event VALUES (x'01', 'a')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	f.waitDrain(t)

	ev := rec.snapshot()
	firstSync := slices.Index(ev, "sync")
	firstPublish := slices.Index(ev, "publish")
	firstAppend := slices.Index(ev, "append")
	if firstAppend < 0 || firstSync < 0 || firstPublish < 0 {
		t.Fatalf("missing events, got %v (want append, sync, publish)", ev)
	}
	if !(firstAppend < firstSync && firstSync < firstPublish) {
		t.Fatalf("capture-before-publish violated: %v (want append < sync < publish)", ev)
	}
	// No publish may precede the batch fsync.
	for i, e := range ev {
		if e == "publish" && i < firstSync {
			t.Fatalf("publish at %d precedes sync at %d: %v", i, firstSync, ev)
		}
	}
}

// TestSelfLog_FatalFsyncPublishesNothing is spec §3.2's fatal path: a SyncSelf
// failure stops the drainer with NO publish, NO marker advance, and NO
// senderNextSeq advance — durability is left to be decided on restart, never
// papered over by publishing un-fsync'd bytes.
func TestSelfLog_FatalFsyncPublishesNothing(t *testing.T) {
	rec := &captureRecorder{syncErr: errors.New("simulated fsync failure")}
	f := setupTBWithConfig(t, Config{SelfLog: rec})
	f.p.OnEncoded(func([]byte) { rec.publish() })

	const self = crdt.Origin(42)
	seqBefore := f.cache.SenderNextSeq(self)
	markerBefore := f.cache.SnapshotMarker(self)

	if err := f.app.Exec(`INSERT INTO event VALUES (x'01', 'a')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// The drainer applies the batch, SyncSelf fails, Apply errors, the
	// drainer exits — WaitForDrain surfaces the dead drainer.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.p.WaitForDrain(ctx); err == nil {
		t.Fatal("WaitForDrain succeeded; want a dead-drainer error after fatal fsync")
	}

	ev := rec.snapshot()
	if slices.Index(ev, "sync") < 0 {
		t.Fatalf("SyncSelf was never attempted: %v", ev)
	}
	if slices.Contains(ev, "publish") {
		t.Fatalf("published after a failed fsync: %v", ev)
	}
	if got := f.cache.SenderNextSeq(self); got != seqBefore {
		t.Errorf("senderNextSeq advanced past a failed fsync: %d -> %d", seqBefore, got)
	}
	if got := f.cache.SnapshotMarker(self); got != markerBefore {
		t.Errorf("snapshot marker advanced past a failed fsync: %d -> %d", markerBefore, got)
	}
}
