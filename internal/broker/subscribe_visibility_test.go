package broker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

// capturingHandler records every emitted slog.Record so a test can assert
// the subscribe loop surfaced a failure (and at which level) instead of
// dying silent.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) count(level slog.Level) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Level == level {
			n++
		}
	}
	return n
}

func withLog(l *slog.Logger) applierOpt { return func(c *Config) { c.Log = l } }

// closedSubTransport models a mux channel/mux that has closed: Subscribe
// returns nil with no error even though the broker ctx is still live.
type closedSubTransport struct{}

func (closedSubTransport) Broadcast(context.Context, []byte) error { return nil }
func (closedSubTransport) Subscribe(context.Context, transport.ApplyFunc) error {
	return nil
}

// oneBadThenBlock delivers a single payload (which fails apply
// non-retryably) on the first Subscribe call, then blocks on later calls so
// the loop re-subscribes exactly once without busy-spinning.
type oneBadThenBlock struct {
	payload []byte
	sent    atomic.Bool
}

func (t *oneBadThenBlock) Broadcast(context.Context, []byte) error { return nil }
func (t *oneBadThenBlock) Subscribe(ctx context.Context, apply transport.ApplyFunc) error {
	if t.sent.CompareAndSwap(false, true) {
		return apply(ctx, t.payload)
	}
	<-ctx.Done()
	return ctx.Err()
}

func waitErr(t *testing.T, br *Broker, want error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := br.LastSubscribeError(); got != nil {
			if want == nil || got == want || got.Error() == want.Error() {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("LastSubscribeError did not become %v within timeout (got %v)", want, br.LastSubscribeError())
}

// TestSubscribeClosedWhileRunningIsVisible: when the transport subscription
// returns cleanly while the broker is still running (mux channel/mux closed
// out from under the loop), the broker must NOT die silent — it records
// errSubscribeClosed and logs at Error. This is the prod failure mode that
// froze live-only rows (host_capacity) with no log output.
func TestSubscribeClosedWhileRunningIsVisible(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{}
	f := newApplier(t, 1, closedSubTransport{}, withLog(slog.New(h)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = f.br.Close() })

	waitErr(t, f.br, errSubscribeClosed)
	if got := h.count(slog.LevelError); got != 1 {
		t.Errorf("error-level log count = %d, want 1 (unexpected-close must be logged once)", got)
	}
}

// TestNonRetryableApplyErrorIsLogged: a dropped, non-retryable inbound apply
// error (here a cluster_id mismatch) is logged at Warn, not just stored in
// LastSubscribeError. A sustained stream of these (a cluster/schema
// divergence during lease churn) otherwise freezes inbound silently.
func TestNonRetryableApplyErrorIsLogged(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{}
	tx := &oneBadThenBlock{}
	f := newApplier(t, 1, tx, withLog(slog.New(h)))

	// Build a changeset stamped with a DIFFERENT cluster_id so applyPayload
	// rejects it non-retryably (the cluster check fires at decode, before any
	// catalog use). Set it on the transport before Start so Subscribe sees it.
	wrongCluster := crdt.ClusterID{0x11, 0x22, 0x33, 0x44}
	idCol := f.tab.PK[0].ID
	pk, err := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x09})})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	nCol, _ := f.tab.Column("n")
	rec := crdt.Insert{Table: f.tab.ID, PK: pk, CL: 1, Image: []crdt.ColValue{textCol(nCol.ID, "x")}}
	cs, err := crdt.Build(crdt.Dot{Origin: 7, Seq: 1},
		crdt.Stamp{Clock: crdt.Clock{WallTime: 1}, Origin: 7}, nil, wrongCluster, []crdt.Record{rec})
	if err != nil {
		t.Fatalf("crdt.Build: %v", err)
	}
	tx.payload = cs.Encoded()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = f.br.Close() })

	waitErr(t, f.br, nil) // any non-nil error recorded
	if got := h.count(slog.LevelWarn); got < 1 {
		t.Errorf("warn-level log count = %d, want >= 1 (dropped apply must be logged)", got)
	}
}
